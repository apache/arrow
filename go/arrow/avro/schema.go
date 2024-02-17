// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package avro reads Avro OCF files and presents the extracted data as records
package avro

import (
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/decimal128"
	"github.com/apache/arrow/go/v16/internal/types"
	avro "github.com/hamba/avro/v2"
)

type schemaNode struct {
	name         string
	parent       *schemaNode
	schema       avro.Schema
	union        bool
	nullable     bool
	childrens    []*schemaNode
	arrowField   arrow.Field
	schemaCache  *avro.SchemaCache
	index, depth int32
}

func newSchemaNode() *schemaNode {
	var schemaCache avro.SchemaCache
	return &schemaNode{name: "", index: -1, schemaCache: &schemaCache}
}

func (node *schemaNode) schemaPath() string {
	var path string
	n := node
	for n.parent != nil {
		path = "." + n.name + path
		n = n.parent
	}
	return path
}

func (node *schemaNode) newChild(n string, s avro.Schema) *schemaNode {
	child := &schemaNode{
		name:        n,
		parent:      node,
		schema:      s,
		schemaCache: node.schemaCache,
		index:       int32(len(node.childrens)),
		depth:       node.depth + 1,
	}
	node.childrens = append(node.childrens, child)
	return child
}
func (node *schemaNode) children() []*schemaNode { return node.childrens }

// func (node *schemaNode) nodeName() string { return node.name }

// ArrowSchemaFromAvro returns a new Arrow schema from an Avro schema
func ArrowSchemaFromAvro(schema avro.Schema) (s *arrow.Schema, err error) {
	defer func() {
		if r := recover(); r != nil {
			s = nil
			switch x := r.(type) {
			case string:
				err = fmt.Errorf("invalid avro schema: %s", x)
			case error:
				err = fmt.Errorf("invalid avro schema: %w", x)
			default:
				err = fmt.Errorf("invalid avro schema: unknown error")
			}
		}
	}()
	n := newSchemaNode()
	n.schema = schema
	c := n.newChild(n.schema.(avro.NamedSchema).Name(), n.schema)
	arrowSchemafromAvro(c)
	var fields []arrow.Field
	for _, g := range c.children() {
		fields = append(fields, g.arrowField)
	}
	s = arrow.NewSchema(fields, nil)
	return s, nil
}

func arrowSchemafromAvro(n *schemaNode) {
	if ns, ok := n.schema.(avro.NamedSchema); ok {
		n.schemaCache.Add(ns.Name(), ns)
	}
	switch st := n.schema.Type(); st {
	case "record":
		iterateFields(n)
	case "enum":
		n.schemaCache.Add(n.schema.(avro.NamedSchema).Name(), n.schema.(*avro.EnumSchema))
		symbols := make(map[string]string)
		for index, symbol := range n.schema.(avro.PropertySchema).(*avro.EnumSchema).Symbols() {
			k := strconv.FormatInt(int64(index), 10)
			symbols[k] = symbol
		}
		var dt arrow.DictionaryType = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
		sl := int64(len(symbols))
		switch {
		case sl <= math.MaxUint8:
			dt.IndexType = arrow.PrimitiveTypes.Uint8
		case sl > math.MaxUint8 && sl <= math.MaxUint16:
			dt.IndexType = arrow.PrimitiveTypes.Uint16
		case sl > math.MaxUint16 && sl <= math.MaxUint32:
			dt.IndexType = arrow.PrimitiveTypes.Uint32
		}
		n.arrowField = buildArrowField(n, &dt, arrow.MetadataFrom(symbols))
	case "array":
		// logical items type
		c := n.newChild(n.name, n.schema.(*avro.ArraySchema).Items())
		if isLogicalSchemaType(n.schema.(*avro.ArraySchema).Items()) {
			avroLogicalToArrowField(c)
		} else {
			arrowSchemafromAvro(c)
		}
		switch c.arrowField.Nullable {
		case true:
			n.arrowField = arrow.Field{Name: n.name, Type: arrow.ListOfField(c.arrowField), Metadata: c.arrowField.Metadata}
		case false:
			n.arrowField = arrow.Field{Name: n.name, Type: arrow.ListOfNonNullable(c.arrowField.Type), Metadata: c.arrowField.Metadata}
		}
	case "map":
		n.schemaCache.Add(n.schema.(*avro.MapSchema).Values().(avro.NamedSchema).Name(), n.schema.(*avro.MapSchema).Values())
		c := n.newChild(n.name, n.schema.(*avro.MapSchema).Values())
		arrowSchemafromAvro(c)
		n.arrowField = buildArrowField(n, arrow.MapOf(arrow.BinaryTypes.String, c.arrowField.Type), c.arrowField.Metadata)
	case "union":
		if n.schema.(*avro.UnionSchema).Nullable() {
			if len(n.schema.(*avro.UnionSchema).Types()) > 1 {
				n.schema = n.schema.(*avro.UnionSchema).Types()[1]
				n.union = true
				n.nullable = true
				arrowSchemafromAvro(n)
			}
		}
	// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
	case "fixed":
		n.schemaCache.Add(n.schema.(avro.NamedSchema).Name(), n.schema.(*avro.FixedSchema))
		if isLogicalSchemaType(n.schema) {
			avroLogicalToArrowField(n)
		} else {
			n.arrowField = buildArrowField(n, &arrow.FixedSizeBinaryType{ByteWidth: n.schema.(*avro.FixedSchema).Size()}, arrow.Metadata{})
		}
	case "string", "bytes", "int", "long":
		if isLogicalSchemaType(n.schema) {
			avroLogicalToArrowField(n)
		} else {
			n.arrowField = buildArrowField(n, avroPrimitiveToArrowType(string(st)), arrow.Metadata{})
		}
	case "float", "double", "boolean":
		n.arrowField = arrow.Field{Name: n.name, Type: avroPrimitiveToArrowType(string(st)), Nullable: n.nullable}
	case "<ref>":
		refSchema := n.schemaCache.Get(string(n.schema.(*avro.RefSchema).Schema().Name()))
		if refSchema == nil {
			panic(fmt.Errorf("could not find schema for '%v' in schema cache - %v", n.schemaPath(), n.schema.(*avro.RefSchema).Schema().Name()))
		}
		n.schema = refSchema
		arrowSchemafromAvro(n)
	case "null":
		n.schemaCache.Add(n.schema.(*avro.MapSchema).Values().(avro.NamedSchema).Name(), &avro.NullSchema{})
		n.nullable = true
		n.arrowField = buildArrowField(n, arrow.Null, arrow.Metadata{})
	}
}

// iterate record Fields()
func iterateFields(n *schemaNode) {
	for _, f := range n.schema.(*avro.RecordSchema).Fields() {
		switch ft := f.Type().(type) {
		// Avro "array" field type
		case *avro.ArraySchema:
			n.schemaCache.Add(f.Name(), ft.Items())
			// logical items type
			c := n.newChild(f.Name(), ft.Items())
			if isLogicalSchemaType(ft.Items()) {
				avroLogicalToArrowField(c)
			} else {
				arrowSchemafromAvro(c)
			}
			switch c.arrowField.Nullable {
			case true:
				c.arrowField = arrow.Field{Name: c.name, Type: arrow.ListOfField(c.arrowField), Metadata: c.arrowField.Metadata}
			case false:
				c.arrowField = arrow.Field{Name: c.name, Type: arrow.ListOfNonNullable(c.arrowField.Type), Metadata: c.arrowField.Metadata}
			}
		// Avro "enum" field type = Arrow dictionary type
		case *avro.EnumSchema:
			n.schemaCache.Add(f.Type().(*avro.EnumSchema).Name(), f.Type())
			c := n.newChild(f.Name(), f.Type())
			symbols := make(map[string]string)
			for index, symbol := range ft.Symbols() {
				k := strconv.FormatInt(int64(index), 10)
				symbols[k] = symbol
			}
			var dt arrow.DictionaryType = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
			sl := len(symbols)
			switch {
			case sl <= math.MaxUint8:
				dt.IndexType = arrow.PrimitiveTypes.Uint8
			case sl > math.MaxUint8 && sl <= math.MaxUint16:
				dt.IndexType = arrow.PrimitiveTypes.Uint16
			case sl > math.MaxUint16 && sl <= math.MaxInt:
				dt.IndexType = arrow.PrimitiveTypes.Uint32
			}
			c.arrowField = buildArrowField(c, &dt, arrow.MetadataFrom(symbols))
		// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
		case *avro.FixedSchema:
			n.schemaCache.Add(f.Name(), f.Type())
			c := n.newChild(f.Name(), f.Type())
			if isLogicalSchemaType(f.Type()) {
				avroLogicalToArrowField(c)
			} else {
				arrowSchemafromAvro(c)
			}
		case *avro.RecordSchema:
			n.schemaCache.Add(f.Name(), f.Type())
			c := n.newChild(f.Name(), f.Type())
			iterateFields(c)
			// Avro "map" field type - KVP with value of one type - keys are strings
		case *avro.MapSchema:
			n.schemaCache.Add(f.Name(), ft.Values())
			c := n.newChild(f.Name(), ft.Values())
			arrowSchemafromAvro(c)
			c.arrowField = buildArrowField(c, arrow.MapOf(arrow.BinaryTypes.String, c.arrowField.Type), c.arrowField.Metadata)
		case *avro.UnionSchema:
			if ft.Nullable() {
				if len(ft.Types()) > 1 {
					n.schemaCache.Add(f.Name(), ft.Types()[1])
					c := n.newChild(f.Name(), ft.Types()[1])
					c.union = true
					c.nullable = true
					arrowSchemafromAvro(c)
				}
			}
		default:
			n.schemaCache.Add(f.Name(), f.Type())
			if isLogicalSchemaType(f.Type()) {
				c := n.newChild(f.Name(), f.Type())
				avroLogicalToArrowField(c)
			} else {
				c := n.newChild(f.Name(), f.Type())
				arrowSchemafromAvro(c)
			}

		}
	}
	var fields []arrow.Field
	for _, child := range n.children() {
		fields = append(fields, child.arrowField)
	}

	namedSchema, ok := isNamedSchema(n.schema)

	var md arrow.Metadata
	if ok && namedSchema != n.name+"_data" && n.union {
		md = arrow.NewMetadata([]string{"typeName"}, []string{namedSchema})
	}
	n.arrowField = buildArrowField(n, arrow.StructOf(fields...), md)
}

func isLogicalSchemaType(s avro.Schema) bool {
	lts, ok := s.(avro.LogicalTypeSchema)
	if !ok {
		return false
	}
	if lts.Logical() != nil {
		return true
	}
	return false
}

func isNamedSchema(s avro.Schema) (string, bool) {
	if ns, ok := s.(avro.NamedSchema); ok {
		return ns.FullName(), ok
	}
	return "", false
}

func buildArrowField(n *schemaNode, t arrow.DataType, m arrow.Metadata) arrow.Field {
	return arrow.Field{
		Name:     n.name,
		Type:     t,
		Metadata: m,
		Nullable: n.nullable,
	}
}

// Avro primitive type.
//
// NOTE: Arrow Binary type is used as a catchall to avoid potential data loss.
func avroPrimitiveToArrowType(avroFieldType string) arrow.DataType {
	switch avroFieldType {
	// int: 32-bit signed integer
	case "int":
		return arrow.PrimitiveTypes.Int32
	// long: 64-bit signed integer
	case "long":
		return arrow.PrimitiveTypes.Int64
	// float: single precision (32-bit) IEEE 754 floating-point number
	case "float":
		return arrow.PrimitiveTypes.Float32
	// double: double precision (64-bit) IEEE 754 floating-point number
	case "double":
		return arrow.PrimitiveTypes.Float64
	// bytes: sequence of 8-bit unsigned bytes
	case "bytes":
		return arrow.BinaryTypes.Binary
	// boolean: a binary value
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	// string: unicode character sequence
	case "string":
		return arrow.BinaryTypes.String
	}
	return nil
}

func avroLogicalToArrowField(n *schemaNode) {
	var dt arrow.DataType
	// Avro logical types
	switch lt := n.schema.(avro.LogicalTypeSchema).Logical(); lt.Type() {
	// The decimal logical type represents an arbitrary-precision signed decimal number of the form unscaled × 10-scale.
	// A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the two’s-complement
	// representation of the unscaled integer value in big-endian byte order. The scale is fixed, and is specified
	// using an attribute.
	//
	// The following attributes are supported:
	// scale, a JSON integer representing the scale (optional). If not specified the scale is 0.
	// precision, a JSON integer representing the (maximum) precision of decimals stored in this type (required).
	case "decimal":
		id := arrow.DECIMAL128
		if lt.(*avro.DecimalLogicalSchema).Precision() > decimal128.MaxPrecision {
			id = arrow.DECIMAL256
		}
		dt, _ = arrow.NewDecimalType(id, int32(lt.(*avro.DecimalLogicalSchema).Precision()), int32(lt.(*avro.DecimalLogicalSchema).Scale()))

		// The uuid logical type represents a random generated universally unique identifier (UUID).
		// A uuid logical type annotates an Avro string. The string has to conform with RFC-4122
	case "uuid":
		dt = types.NewUUIDType()

	// The date logical type represents a date within the calendar, with no reference to a particular
	// time zone or time of day.
	// A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch,
	// 1 January 1970 (ISO calendar).
	case "date":
		dt = arrow.FixedWidthTypes.Date32

	// The time-millis logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one millisecond.
	// A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds
	// after midnight, 00:00:00.000.
	case "time-millis":
		dt = arrow.FixedWidthTypes.Time32ms

	// The time-micros logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one microsecond.
	// A time-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// after midnight, 00:00:00.000000.
	case "time-micros":
		dt = arrow.FixedWidthTypes.Time64us

	// The timestamp-millis logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one millisecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users in
	// their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds
	// from the unix epoch, 1 January 1970 00:00:00.000 UTC.
	case "timestamp-millis":
		dt = arrow.FixedWidthTypes.Timestamp_ms

	// The timestamp-micros logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one microsecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users
	// in their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
	case "timestamp-micros":
		dt = arrow.FixedWidthTypes.Timestamp_us

	// The local-timestamp-millis logical type represents a timestamp in a local timezone, regardless of
	// what specific time zone is considered local, with a precision of one millisecond.
	// A local-timestamp-millis logical type annotates an Avro long, where the long stores the number of
	// milliseconds, from 1 January 1970 00:00:00.000.
	// Note: not implemented in hamba/avro
	// case "local-timestamp-millis":
	// 	dt = &arrow.TimestampType{Unit: arrow.Millisecond}

	// The local-timestamp-micros logical type represents a timestamp in a local timezone, regardless of
	// what specific time zone is considered local, with a precision of one microsecond.
	// A local-timestamp-micros logical type annotates an Avro long, where the long stores the number of
	// microseconds, from 1 January 1970 00:00:00.000000.
	// case "local-timestamp-micros":
	// Note: not implemented in hamba/avro
	// 	dt = &arrow.TimestampType{Unit: arrow.Microsecond}

	// The duration logical type represents an amount of time defined by a number of months, days and milliseconds.
	// This is not equivalent to a number of milliseconds, because, depending on the moment in time from which the
	// duration is measured, the number of days in the month and number of milliseconds in a day may differ. Other
	// standard periods such as years, quarters, hours and minutes can be expressed through these basic periods.

	// A duration logical type annotates Avro fixed type of size 12, which stores three little-endian unsigned integers
	// that represent durations at different granularities of time. The first stores a number in months, the second
	// stores a number in days, and the third stores a number in milliseconds.
	case "duration":
		dt = arrow.FixedWidthTypes.MonthDayNanoInterval
	}
	n.arrowField = buildArrowField(n, dt, arrow.Metadata{})
}

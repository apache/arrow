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

// Package arrjson provides types and functions to encode and decode ARROW types and data
// to and from JSON files.
package arrjson

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/decimal256"
	"github.com/apache/arrow/go/v12/arrow/float16"
	"github.com/apache/arrow/go/v12/arrow/internal/dictutils"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type Schema struct {
	Fields    []FieldWrapper `json:"fields"`
	arrowMeta arrow.Metadata `json:"-"`
	Metadata  []metaKV       `json:"metadata,omitempty"`
}

func (s Schema) MarshalJSON() ([]byte, error) {
	if s.arrowMeta.Len() > 0 {
		s.Metadata = make([]metaKV, 0, s.arrowMeta.Len())
		keys := s.arrowMeta.Keys()
		vals := s.arrowMeta.Values()
		for i := range keys {
			s.Metadata = append(s.Metadata, metaKV{Key: keys[i], Value: vals[i]})
		}
	}
	type alias Schema
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(alias(s))
	return buf.Bytes(), err
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	type Alias Schema
	aux := &struct {
		*Alias
	}{Alias: (*Alias)(s)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var (
		mdkeys = make([]string, 0)
		mdvals = make([]string, 0)
	)

	for _, kv := range s.Metadata {
		mdkeys = append(mdkeys, kv.Key)
		mdvals = append(mdvals, kv.Value)
	}

	if len(s.Metadata) > 0 {
		s.arrowMeta = arrow.NewMetadata(mdkeys, mdvals)
	}
	return nil
}

// FieldWrapper gets used in order to hook into the JSON marshalling and
// unmarshalling without creating an infinite loop when dealing with the
// children fields.
type FieldWrapper struct {
	Field
}

type FieldDict struct {
	ID      int             `json:"id"`
	Type    json.RawMessage `json:"indexType"`
	idxType arrow.DataType  `json:"-"`
	Ordered bool            `json:"isOrdered"`
}

type Field struct {
	Name string `json:"name"`
	// the arrowType will get populated during unmarshalling by processing the
	// Type, and will be used to generate the Type during Marshalling to JSON
	arrowType arrow.DataType `json:"-"`
	// leave this as a json RawMessage in order to partially unmarshal as needed
	// during marshal/unmarshal time so we can determine what the structure is
	// actually expected to be.
	Type       json.RawMessage `json:"type"`
	Nullable   bool            `json:"nullable"`
	Children   []FieldWrapper  `json:"children"`
	arrowMeta  arrow.Metadata  `json:"-"`
	Dictionary *FieldDict      `json:"dictionary,omitempty"`
	Metadata   []metaKV        `json:"metadata,omitempty"`
}

type metaKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func typeToJSON(arrowType arrow.DataType) (json.RawMessage, error) {
	var typ interface{}
	switch dt := arrowType.(type) {
	case *arrow.NullType:
		typ = nameJSON{"null"}
	case *arrow.BooleanType:
		typ = nameJSON{"bool"}
	case *arrow.Int8Type:
		typ = bitWidthJSON{Name: "int", Signed: true, BitWidth: 8}
	case *arrow.Int16Type:
		typ = bitWidthJSON{Name: "int", Signed: true, BitWidth: 16}
	case *arrow.Int32Type:
		typ = bitWidthJSON{Name: "int", Signed: true, BitWidth: 32}
	case *arrow.Int64Type:
		typ = bitWidthJSON{Name: "int", Signed: true, BitWidth: 64}
	case *arrow.Uint8Type:
		typ = bitWidthJSON{Name: "int", Signed: false, BitWidth: 8}
	case *arrow.Uint16Type:
		typ = bitWidthJSON{Name: "int", Signed: false, BitWidth: 16}
	case *arrow.Uint32Type:
		typ = bitWidthJSON{Name: "int", Signed: false, BitWidth: 32}
	case *arrow.Uint64Type:
		typ = bitWidthJSON{Name: "int", Signed: false, BitWidth: 64}
	case *arrow.Float16Type:
		typ = floatJSON{"floatingpoint", "HALF"}
	case *arrow.Float32Type:
		typ = floatJSON{"floatingpoint", "SINGLE"}
	case *arrow.Float64Type:
		typ = floatJSON{"floatingpoint", "DOUBLE"}
	case *arrow.BinaryType:
		typ = nameJSON{"binary"}
	case *arrow.LargeBinaryType:
		typ = nameJSON{"largebinary"}
	case *arrow.StringType:
		typ = nameJSON{"utf8"}
	case *arrow.LargeStringType:
		typ = nameJSON{"largeutf8"}
	case *arrow.Date32Type:
		typ = unitZoneJSON{Name: "date", Unit: "DAY"}
	case *arrow.Date64Type:
		typ = unitZoneJSON{Name: "date", Unit: "MILLISECOND"}
	case *arrow.MonthIntervalType:
		typ = unitZoneJSON{Name: "interval", Unit: "YEAR_MONTH"}
	case *arrow.DayTimeIntervalType:
		typ = unitZoneJSON{Name: "interval", Unit: "DAY_TIME"}
	case *arrow.MonthDayNanoIntervalType:
		typ = unitZoneJSON{Name: "interval", Unit: "MONTH_DAY_NANO"}
	case *arrow.DurationType:
		switch dt.Unit {
		case arrow.Second:
			typ = unitZoneJSON{Name: "duration", Unit: "SECOND"}
		case arrow.Millisecond:
			typ = unitZoneJSON{Name: "duration", Unit: "MILLISECOND"}
		case arrow.Microsecond:
			typ = unitZoneJSON{Name: "duration", Unit: "MICROSECOND"}
		case arrow.Nanosecond:
			typ = unitZoneJSON{Name: "duration", Unit: "NANOSECOND"}
		}
	case *arrow.Time32Type:
		switch dt.Unit {
		case arrow.Second:
			typ = bitWidthJSON{Name: "time", BitWidth: dt.BitWidth(), Unit: "SECOND"}
		case arrow.Millisecond:
			typ = bitWidthJSON{Name: "time", BitWidth: dt.BitWidth(), Unit: "MILLISECOND"}
		}
	case *arrow.Time64Type:
		switch dt.Unit {
		case arrow.Microsecond:
			typ = bitWidthJSON{Name: "time", BitWidth: dt.BitWidth(), Unit: "MICROSECOND"}
		case arrow.Nanosecond:
			typ = bitWidthJSON{Name: "time", BitWidth: dt.BitWidth(), Unit: "NANOSECOND"}
		}
	case *arrow.TimestampType:
		switch dt.Unit {
		case arrow.Second:
			typ = unitZoneJSON{Name: "timestamp", Unit: "SECOND", TimeZone: dt.TimeZone}
		case arrow.Millisecond:
			typ = unitZoneJSON{Name: "timestamp", Unit: "MILLISECOND", TimeZone: dt.TimeZone}
		case arrow.Microsecond:
			typ = unitZoneJSON{Name: "timestamp", Unit: "MICROSECOND", TimeZone: dt.TimeZone}
		case arrow.Nanosecond:
			typ = unitZoneJSON{Name: "timestamp", Unit: "NANOSECOND", TimeZone: dt.TimeZone}
		}
	case *arrow.ListType:
		typ = nameJSON{"list"}
	case *arrow.LargeListType:
		typ = nameJSON{"largelist"}
	case *arrow.MapType:
		typ = mapJSON{Name: "map", KeysSorted: dt.KeysSorted}
	case *arrow.StructType:
		typ = nameJSON{"struct"}
	case *arrow.FixedSizeListType:
		typ = listSizeJSON{"fixedsizelist", dt.Len()}
	case *arrow.FixedSizeBinaryType:
		typ = byteWidthJSON{"fixedsizebinary", dt.ByteWidth}
	case *arrow.Decimal128Type:
		typ = decimalJSON{"decimal", int(dt.Scale), int(dt.Precision), 128}
	case *arrow.Decimal256Type:
		typ = decimalJSON{"decimal", int(dt.Scale), int(dt.Precision), 256}
	case arrow.UnionType:
		typ = unionJSON{"union", dt.Mode().String(), dt.TypeCodes()}
	case *arrow.RunEndEncodedType:
		typ = nameJSON{"runendencoded"}
	default:
		return nil, fmt.Errorf("unknown arrow.DataType %v", arrowType)
	}

	return json.Marshal(typ)
}

func (f FieldWrapper) MarshalJSON() ([]byte, error) {
	// for extension types, add the extension type metadata appropriately
	// and then marshal as normal for the storage type.
	if f.arrowType.ID() == arrow.EXTENSION {
		exType := f.arrowType.(arrow.ExtensionType)

		mdkeys := append(f.arrowMeta.Keys(), ipc.ExtensionTypeKeyName)
		mdvals := append(f.arrowMeta.Values(), exType.ExtensionName())

		serializedData := exType.Serialize()
		if len(serializedData) > 0 {
			mdkeys = append(mdkeys, ipc.ExtensionMetadataKeyName)
			mdvals = append(mdvals, string(serializedData))
		}

		f.arrowMeta = arrow.NewMetadata(mdkeys, mdvals)
		f.arrowType = exType.StorageType()
	}

	var err error
	if f.arrowType.ID() == arrow.DICTIONARY {
		f.arrowType = f.arrowType.(*arrow.DictionaryType).ValueType
		if f.Dictionary.Type, err = typeToJSON(f.Dictionary.idxType); err != nil {
			return nil, err
		}
	}

	if f.Type, err = typeToJSON(f.arrowType); err != nil {
		return nil, err
	}

	// if we have metadata then add the key/value pairs to the json
	if f.arrowMeta.Len() > 0 {
		f.Metadata = make([]metaKV, 0, f.arrowMeta.Len())
		for i := 0; i < f.arrowMeta.Len(); i++ {
			f.Metadata = append(f.Metadata, metaKV{Key: f.arrowMeta.Keys()[i], Value: f.arrowMeta.Values()[i]})
		}
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err = enc.Encode(f.Field)
	return buf.Bytes(), err
}

func typeFromJSON(typ json.RawMessage, children []FieldWrapper) (arrowType arrow.DataType, err error) {
	tmp := nameJSON{}
	if err = json.Unmarshal(typ, &tmp); err != nil {
		return
	}

	switch tmp.Name {
	case "null":
		arrowType = arrow.Null
	case "bool":
		arrowType = arrow.FixedWidthTypes.Boolean
	case "int":
		t := bitWidthJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Signed {
		case true:
			switch t.BitWidth {
			case 8:
				arrowType = arrow.PrimitiveTypes.Int8
			case 16:
				arrowType = arrow.PrimitiveTypes.Int16
			case 32:
				arrowType = arrow.PrimitiveTypes.Int32
			case 64:
				arrowType = arrow.PrimitiveTypes.Int64
			}
		default:
			switch t.BitWidth {
			case 8:
				arrowType = arrow.PrimitiveTypes.Uint8
			case 16:
				arrowType = arrow.PrimitiveTypes.Uint16
			case 32:
				arrowType = arrow.PrimitiveTypes.Uint32
			case 64:
				arrowType = arrow.PrimitiveTypes.Uint64
			}
		}
	case "floatingpoint":
		t := floatJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Precision {
		case "HALF":
			arrowType = arrow.FixedWidthTypes.Float16
		case "SINGLE":
			arrowType = arrow.PrimitiveTypes.Float32
		case "DOUBLE":
			arrowType = arrow.PrimitiveTypes.Float64
		}
	case "binary":
		arrowType = arrow.BinaryTypes.Binary
	case "largebinary":
		arrowType = arrow.BinaryTypes.LargeBinary
	case "utf8":
		arrowType = arrow.BinaryTypes.String
	case "largeutf8":
		arrowType = arrow.BinaryTypes.LargeString
	case "date":
		t := unitZoneJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Unit {
		case "DAY":
			arrowType = arrow.FixedWidthTypes.Date32
		case "MILLISECOND":
			arrowType = arrow.FixedWidthTypes.Date64
		}
	case "time":
		t := bitWidthJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.BitWidth {
		case 32:
			switch t.Unit {
			case "SECOND":
				arrowType = arrow.FixedWidthTypes.Time32s
			case "MILLISECOND":
				arrowType = arrow.FixedWidthTypes.Time32ms
			}
		case 64:
			switch t.Unit {
			case "MICROSECOND":
				arrowType = arrow.FixedWidthTypes.Time64us
			case "NANOSECOND":
				arrowType = arrow.FixedWidthTypes.Time64ns
			}
		}
	case "timestamp":
		t := unitZoneJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		arrowType = &arrow.TimestampType{TimeZone: t.TimeZone}
		switch t.Unit {
		case "SECOND":
			arrowType.(*arrow.TimestampType).Unit = arrow.Second
		case "MILLISECOND":
			arrowType.(*arrow.TimestampType).Unit = arrow.Millisecond
		case "MICROSECOND":
			arrowType.(*arrow.TimestampType).Unit = arrow.Microsecond
		case "NANOSECOND":
			arrowType.(*arrow.TimestampType).Unit = arrow.Nanosecond
		}
	case "list":
		arrowType = arrow.ListOfField(arrow.Field{
			Name:     children[0].Name,
			Type:     children[0].arrowType,
			Metadata: children[0].arrowMeta,
			Nullable: children[0].Nullable,
		})
	case "largelist":
		arrowType = arrow.LargeListOfField(arrow.Field{
			Name:     children[0].Name,
			Type:     children[0].arrowType,
			Metadata: children[0].arrowMeta,
			Nullable: children[0].Nullable,
		})
	case "map":
		t := mapJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		pairType := children[0].arrowType
		arrowType = arrow.MapOf(pairType.(*arrow.StructType).Field(0).Type, pairType.(*arrow.StructType).Field(1).Type)
		arrowType.(*arrow.MapType).KeysSorted = t.KeysSorted
	case "struct":
		arrowType = arrow.StructOf(fieldsFromJSON(children)...)
	case "fixedsizebinary":
		t := byteWidthJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		arrowType = &arrow.FixedSizeBinaryType{ByteWidth: t.ByteWidth}
	case "fixedsizelist":
		t := listSizeJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		arrowType = arrow.FixedSizeListOfField(t.ListSize, arrow.Field{
			Name:     children[0].Name,
			Type:     children[0].arrowType,
			Metadata: children[0].arrowMeta,
			Nullable: children[0].Nullable,
		})
	case "interval":
		t := unitZoneJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Unit {
		case "YEAR_MONTH":
			arrowType = arrow.FixedWidthTypes.MonthInterval
		case "DAY_TIME":
			arrowType = arrow.FixedWidthTypes.DayTimeInterval
		case "MONTH_DAY_NANO":
			arrowType = arrow.FixedWidthTypes.MonthDayNanoInterval
		}
	case "duration":
		t := unitZoneJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Unit {
		case "SECOND":
			arrowType = arrow.FixedWidthTypes.Duration_s
		case "MILLISECOND":
			arrowType = arrow.FixedWidthTypes.Duration_ms
		case "MICROSECOND":
			arrowType = arrow.FixedWidthTypes.Duration_us
		case "NANOSECOND":
			arrowType = arrow.FixedWidthTypes.Duration_ns
		}
	case "decimal":
		t := decimalJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.BitWidth {
		case 256:
			arrowType = &arrow.Decimal256Type{Precision: int32(t.Precision), Scale: int32(t.Scale)}
		case 128, 0: // default to 128 bits when missing
			arrowType = &arrow.Decimal128Type{Precision: int32(t.Precision), Scale: int32(t.Scale)}
		}
	case "union":
		t := unionJSON{}
		if err = json.Unmarshal(typ, &t); err != nil {
			return
		}
		switch t.Mode {
		case "SPARSE":
			arrowType = arrow.SparseUnionOf(fieldsFromJSON(children), t.TypeIDs)
		case "DENSE":
			arrowType = arrow.DenseUnionOf(fieldsFromJSON(children), t.TypeIDs)
		}
	case "runendencoded":
		if len(children) != 2 {
			err = fmt.Errorf("%w: run-end encoded array must have exactly 2 fields, but got %d",
				arrow.ErrInvalid, len(children))
			return
		}
		if children[0].Name != "run_ends" {
			err = fmt.Errorf("%w: first child of run-end encoded array must be called run_ends, but got: %s",
				arrow.ErrInvalid, children[0].Name)
			return
		}
		switch children[0].arrowType.ID() {
		case arrow.INT16, arrow.INT32, arrow.INT64:
		default:
			err = fmt.Errorf("%w: only int16, int32 and int64 type are supported as run ends array, but got: %s",
				arrow.ErrInvalid, children[0].Type)
			return
		}

		if children[0].Nullable {
			err = fmt.Errorf("%w: run ends array cannot be nullable", arrow.ErrInvalid)
			return
		}
		if children[1].Name != "values" {
			err = fmt.Errorf("%w: second child of run-end encoded array must be called values, got: %s",
				arrow.ErrInvalid, children[1].Name)
			return
		}
		arrowType = arrow.RunEndEncodedOf(children[0].arrowType, children[1].arrowType)
	}

	if arrowType == nil {
		err = fmt.Errorf("unhandled type unmarshalling from json: %s", tmp.Name)
	}
	return
}

func (f *FieldWrapper) UnmarshalJSON(data []byte) error {
	var err error
	if err = json.Unmarshal(data, &f.Field); err != nil {
		return err
	}

	if f.arrowType, err = typeFromJSON(f.Type, f.Children); err != nil {
		return err
	}

	if f.Dictionary != nil {
		if f.Dictionary.idxType, err = typeFromJSON(f.Dictionary.Type, nil); err != nil {
			return err
		}
		f.arrowType = &arrow.DictionaryType{IndexType: f.Dictionary.idxType, ValueType: f.arrowType}
	}

	if len(f.Metadata) > 0 { // unmarshal the key/value metadata pairs
		var (
			mdkeys         = make([]string, 0, len(f.Metadata))
			mdvals         = make([]string, 0, len(f.Metadata))
			extKeyIdx  int = -1
			extDataIdx int = -1
		)

		for i, kv := range f.Metadata {
			switch kv.Key {
			case ipc.ExtensionTypeKeyName:
				extKeyIdx = i
			case ipc.ExtensionMetadataKeyName:
				extDataIdx = i
			}
			mdkeys = append(mdkeys, kv.Key)
			mdvals = append(mdvals, kv.Value)
		}

		if extKeyIdx == -1 { // no extension metadata just create the metadata
			f.arrowMeta = arrow.NewMetadata(mdkeys, mdvals)
			return nil
		}

		extType := arrow.GetExtensionType(mdvals[extKeyIdx])
		if extType == nil { // unregistered extension type, just keep the metadata
			f.arrowMeta = arrow.NewMetadata(mdkeys, mdvals)
			return nil
		}

		var extData string
		if extDataIdx > -1 {
			extData = mdvals[extDataIdx]
			// if both extension type and extension type metadata exist
			// filter out both keys
			newkeys := make([]string, 0, len(mdkeys)-2)
			newvals := make([]string, 0, len(mdvals)-2)
			for i := range mdkeys {
				if i != extKeyIdx && i != extDataIdx {
					newkeys = append(newkeys, mdkeys[i])
					newvals = append(newvals, mdvals[i])
				}
			}
			mdkeys = newkeys
			mdvals = newvals
		} else {
			// if only extension type key is present, we can simplify filtering it out
			mdkeys = append(mdkeys[:extKeyIdx], mdkeys[extKeyIdx+1:]...)
			mdvals = append(mdvals[:extKeyIdx], mdvals[extKeyIdx+1:]...)
		}

		if f.arrowType, err = extType.Deserialize(f.arrowType, extData); err != nil {
			return err
		}

		f.arrowMeta = arrow.NewMetadata(mdkeys, mdvals)
	}

	return err
}

// the structs below represent various configurations of the Type
// json block and what fields will be expected. Sometimes there is
// overlap between the same key used with different types, so it's
// easier to partial unmarshal and then use these to ensure correct
// typing.

type nameJSON struct {
	Name string `json:"name"`
}

type listSizeJSON struct {
	Name     string `json:"name"`
	ListSize int32  `json:"listSize,omitempty"`
}

type bitWidthJSON struct {
	Name     string `json:"name"`
	Signed   bool   `json:"isSigned,omitempty"`
	BitWidth int    `json:"bitWidth,omitempty"`
	Unit     string `json:"unit,omitempty"`
}

type floatJSON struct {
	Name      string `json:"name"`
	Precision string `json:"precision,omitempty"`
}

type unitZoneJSON struct {
	Name     string `json:"name"`
	Unit     string `json:"unit,omitempty"`
	TimeZone string `json:"timezone,omitempty"`
}

type decimalJSON struct {
	Name      string `json:"name"`
	Scale     int    `json:"scale,omitempty"`
	Precision int    `json:"precision,omitempty"`
	BitWidth  int    `json:"bitWidth,omitempty"`
}

type byteWidthJSON struct {
	Name      string `json:"name"`
	ByteWidth int    `json:"byteWidth,omitempty"`
}

type mapJSON struct {
	Name       string `json:"name"`
	KeysSorted bool   `json:"keysSorted,omitempty"`
}

type unionJSON struct {
	Name    string                `json:"name"`
	Mode    string                `json:"mode"`
	TypeIDs []arrow.UnionTypeCode `json:"typeIds"`
}

func schemaToJSON(schema *arrow.Schema, mapper *dictutils.Mapper) Schema {
	return Schema{
		Fields:    fieldsToJSON(schema.Fields(), dictutils.NewFieldPos(), mapper),
		arrowMeta: schema.Metadata(),
	}
}

func schemaFromJSON(schema Schema, memo *dictutils.Memo) *arrow.Schema {
	sc := arrow.NewSchema(fieldsFromJSON(schema.Fields), &schema.arrowMeta)
	dictInfoFromJSONFields(schema.Fields, dictutils.NewFieldPos(), memo)
	return sc
}

func dictInfoFromJSONFields(fields []FieldWrapper, pos dictutils.FieldPos, memo *dictutils.Memo) {
	for i, f := range fields {
		dictInfoFromJSON(f, pos.Child(int32(i)), memo)
	}
}

func dictInfoFromJSON(field FieldWrapper, pos dictutils.FieldPos, memo *dictutils.Memo) {
	if field.Dictionary != nil {
		typ := field.arrowType
		if typ.ID() == arrow.EXTENSION {
			typ = typ.(arrow.ExtensionType).StorageType()
		}
		valueType := typ.(*arrow.DictionaryType).ValueType

		if err := memo.Mapper.AddField(int64(field.Dictionary.ID), pos.Path()); err != nil {
			panic(err)
		}
		if err := memo.AddType(int64(field.Dictionary.ID), valueType); err != nil {
			panic(err)
		}
	}
	dictInfoFromJSONFields(field.Children, pos, memo)
}

func fieldsToJSON(fields []arrow.Field, parentPos dictutils.FieldPos, mapper *dictutils.Mapper) []FieldWrapper {
	o := make([]FieldWrapper, len(fields))
	for i, f := range fields {
		pos := parentPos.Child(int32(i))
		o[i] = FieldWrapper{Field{
			Name:      f.Name,
			arrowType: f.Type,
			Nullable:  f.Nullable,
			Children:  []FieldWrapper{},
			arrowMeta: f.Metadata,
		}}
		typ := f.Type
		if typ.ID() == arrow.EXTENSION {
			typ = typ.(arrow.ExtensionType).StorageType()
		}
		if typ.ID() == arrow.DICTIONARY {
			dictType := typ.(*arrow.DictionaryType)
			typ = dictType.ValueType
			dictID, err := mapper.GetFieldID(pos.Path())
			if err != nil {
				panic(err)
			}
			o[i].Dictionary = &FieldDict{
				idxType: dictType.IndexType,
				ID:      int(dictID),
				Ordered: dictType.Ordered,
			}
		}

		if dt, ok := typ.(arrow.NestedType); ok {
			o[i].Children = fieldsToJSON(dt.Fields(), pos, mapper)
		}
	}
	return o
}

func fieldsFromJSON(fields []FieldWrapper) []arrow.Field {
	vs := make([]arrow.Field, len(fields))
	for i, v := range fields {
		vs[i] = fieldFromJSON(v.Field)
	}
	return vs
}

func fieldFromJSON(f Field) arrow.Field {
	return arrow.Field{
		Name:     f.Name,
		Type:     f.arrowType,
		Nullable: f.Nullable,
		Metadata: f.arrowMeta,
	}
}

type Dictionary struct {
	ID   int64  `json:"id"`
	Data Record `json:"data"`
}

func dictionariesFromJSON(mem memory.Allocator, dicts []Dictionary, memo *dictutils.Memo) {
	for _, d := range dicts {
		valueType, exists := memo.Type(d.ID)
		if !exists {
			panic(fmt.Errorf("arrow/json: no corresponding dictionary memo for id=%d", d.ID))
		}

		dict := arrayFromJSON(mem, valueType, d.Data.Columns[0])
		defer dict.Release()
		memo.Add(d.ID, dict)
	}
}

type Record struct {
	Count   int64   `json:"count"`
	Columns []Array `json:"columns"`
}

func recordsFromJSON(mem memory.Allocator, schema *arrow.Schema, recs []Record, memo *dictutils.Memo) []arrow.Record {
	vs := make([]arrow.Record, len(recs))
	for i, rec := range recs {
		vs[i] = recordFromJSON(mem, schema, rec, memo)
	}
	return vs
}

func recordFromJSON(mem memory.Allocator, schema *arrow.Schema, rec Record, memo *dictutils.Memo) arrow.Record {
	arrs := arraysFromJSON(mem, schema, rec.Columns)
	if err := dictutils.ResolveDictionaries(memo, arrs, dictutils.NewFieldPos(), mem); err != nil {
		panic(err)
	}

	cols := make([]arrow.Array, len(arrs))
	for i, d := range arrs {
		cols[i] = array.MakeFromData(d)
		defer d.Release()
		defer cols[i].Release()
	}
	return array.NewRecord(schema, cols, int64(rec.Count))
}

func recordToJSON(rec arrow.Record) Record {
	return Record{
		Count:   rec.NumRows(),
		Columns: arraysToJSON(rec.Schema(), rec.Columns()),
	}
}

type Array struct {
	Name     string                `json:"name"`
	Count    int                   `json:"count"`
	Valids   []int                 `json:"VALIDITY,omitempty"`
	Data     []interface{}         `json:"DATA,omitempty"`
	TypeID   []arrow.UnionTypeCode `json:"TYPE_ID,omitempty"`
	Offset   interface{}           `json:"OFFSET,omitempty"`
	Children []Array               `json:"children,omitempty"`
}

func (a *Array) MarshalJSON() ([]byte, error) {
	type Alias Array
	aux := struct {
		*Alias
		OutOffset interface{} `json:"OFFSET,omitempty"`
	}{Alias: (*Alias)(a), OutOffset: a.Offset}
	return json.Marshal(aux)
}

func (a *Array) UnmarshalJSON(b []byte) (err error) {
	type Alias Array
	aux := &struct {
		*Alias
		RawOffset json.RawMessage `json:"OFFSET,omitempty"`
	}{Alias: (*Alias)(a)}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()

	if err = dec.Decode(&aux); err != nil {
		return
	}

	if len(aux.RawOffset) == 0 {
		return
	}

	var rawOffsets []interface{}
	if err = json.Unmarshal(aux.RawOffset, &rawOffsets); err != nil {
		return
	}

	if len(rawOffsets) == 0 {
		return
	}

	switch rawOffsets[0].(type) {
	case string:
		out := make([]int64, len(rawOffsets))
		for i, o := range rawOffsets {
			out[i], err = strconv.ParseInt(o.(string), 10, 64)
			if err != nil {
				return
			}
		}
		a.Offset = out
	case float64:
		out := make([]int32, len(rawOffsets))
		for i, o := range rawOffsets {
			out[i] = int32(o.(float64))
		}
		a.Offset = out
	}

	return nil
}

func arraysFromJSON(mem memory.Allocator, schema *arrow.Schema, arrs []Array) []arrow.ArrayData {
	o := make([]arrow.ArrayData, len(arrs))
	for i, v := range arrs {
		o[i] = arrayFromJSON(mem, schema.Field(i).Type, v)
	}
	return o
}

func arraysToJSON(schema *arrow.Schema, arrs []arrow.Array) []Array {
	o := make([]Array, len(arrs))
	for i, v := range arrs {
		o[i] = arrayToJSON(schema.Field(i), v)
	}
	return o
}

func validsToBitmap(valids []bool, mem memory.Allocator) *memory.Buffer {
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(len(valids)))))

	wr := bitutil.NewBitmapWriter(buf.Bytes(), 0, len(valids))
	wr.AppendBools(valids)
	wr.Finish()
	return buf
}

func returnNewArrayData(bldr array.Builder) arrow.ArrayData {
	arr := bldr.NewArray()
	defer arr.Release()
	arr.Data().Retain()
	return arr.Data()
}

func arrayFromJSON(mem memory.Allocator, dt arrow.DataType, arr Array) arrow.ArrayData {
	switch dt := dt.(type) {
	case *arrow.NullType:
		return array.NewNull(arr.Count).Data()

	case *arrow.BooleanType:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()
		data := boolsFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Int8Type:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()
		data := i8FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Int16Type:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()
		data := i16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Int32Type:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		data := i32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Int64Type:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		data := i64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Uint8Type:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()
		data := u8FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Uint16Type:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()
		data := u16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Uint32Type:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()
		data := u32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Uint64Type:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()
		data := u64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Float16Type:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()
		data := f16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Float32Type:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()
		data := f32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Float64Type:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()
		data := f64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.StringType:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		data := strFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.LargeStringType:
		bldr := array.NewLargeStringBuilder(mem)
		defer bldr.Release()
		data := strFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.LargeBinaryType:
		bldr := array.NewBinaryBuilder(mem, dt)
		defer bldr.Release()
		data := bytesFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.BinaryType:
		bldr := array.NewBinaryBuilder(mem, dt)
		defer bldr.Release()
		data := bytesFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.ListType:
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.Elem(), arr.Children[0])
		defer elems.Release()

		bitmap := validsToBitmap(valids, mem)
		defer bitmap.Release()

		nulls := arr.Count - bitutil.CountSetBits(bitmap.Bytes(), 0, arr.Count)
		return array.NewData(dt, arr.Count, []*memory.Buffer{bitmap,
			memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(arr.Offset.([]int32)))},
			[]arrow.ArrayData{elems}, nulls, 0)

	case *arrow.LargeListType:
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.Elem(), arr.Children[0])
		defer elems.Release()

		bitmap := validsToBitmap(valids, mem)
		defer bitmap.Release()

		nulls := arr.Count - bitutil.CountSetBits(bitmap.Bytes(), 0, arr.Count)
		return array.NewData(dt, arr.Count, []*memory.Buffer{bitmap,
			memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(arr.Offset.([]int64)))},
			[]arrow.ArrayData{elems}, nulls, 0)

	case *arrow.FixedSizeListType:
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.Elem(), arr.Children[0])
		defer elems.Release()

		bitmap := validsToBitmap(valids, mem)
		defer bitmap.Release()

		nulls := arr.Count - bitutil.CountSetBits(bitmap.Bytes(), 0, arr.Count)
		return array.NewData(dt, arr.Count, []*memory.Buffer{bitmap}, []arrow.ArrayData{elems}, nulls, 0)

	case *arrow.StructType:
		valids := validsFromJSON(arr.Valids)
		bitmap := validsToBitmap(valids, mem)
		defer bitmap.Release()

		nulls := arr.Count - bitutil.CountSetBits(bitmap.Bytes(), 0, arr.Count)

		fields := make([]arrow.ArrayData, len(dt.Fields()))
		for i := range fields {
			child := arrayFromJSON(mem, dt.Field(i).Type, arr.Children[i])
			defer child.Release()
			fields[i] = child
		}

		return array.NewData(dt, arr.Count, []*memory.Buffer{bitmap}, fields, nulls, 0)

	case *arrow.FixedSizeBinaryType:
		bldr := array.NewFixedSizeBinaryBuilder(mem, dt)
		defer bldr.Release()
		strdata := strFromJSON(arr.Data)
		data := make([][]byte, len(strdata))
		for i, v := range strdata {
			if len(v) != 2*dt.ByteWidth {
				panic(fmt.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
			}
			vv, err := hex.DecodeString(v)
			if err != nil {
				panic(err)
			}
			data[i] = vv
		}
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.MapType:
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.ValueType(), arr.Children[0])
		defer elems.Release()

		bitmap := validsToBitmap(valids, mem)
		defer bitmap.Release()

		nulls := arr.Count - bitutil.CountSetBits(bitmap.Bytes(), 0, arr.Count)
		return array.NewData(dt, arr.Count, []*memory.Buffer{bitmap,
			memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(arr.Offset.([]int32)))},
			[]arrow.ArrayData{elems}, nulls, 0)

	case *arrow.Date32Type:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()
		data := date32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Date64Type:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()
		data := date64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Time32Type:
		bldr := array.NewTime32Builder(mem, dt)
		defer bldr.Release()
		data := time32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Time64Type:
		bldr := array.NewTime64Builder(mem, dt)
		defer bldr.Release()
		data := time64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.TimestampType:
		bldr := array.NewTimestampBuilder(mem, dt)
		defer bldr.Release()
		data := timestampFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.MonthIntervalType:
		bldr := array.NewMonthIntervalBuilder(mem)
		defer bldr.Release()
		data := monthintervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.DayTimeIntervalType:
		bldr := array.NewDayTimeIntervalBuilder(mem)
		defer bldr.Release()
		data := daytimeintervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.MonthDayNanoIntervalType:
		bldr := array.NewMonthDayNanoIntervalBuilder(mem)
		defer bldr.Release()
		data := monthDayNanointervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.DurationType:
		bldr := array.NewDurationBuilder(mem, dt)
		defer bldr.Release()
		data := durationFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Decimal128Type:
		bldr := array.NewDecimal128Builder(mem, dt)
		defer bldr.Release()
		data := decimal128FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case *arrow.Decimal256Type:
		bldr := array.NewDecimal256Builder(mem, dt)
		defer bldr.Release()
		data := decimal256FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return returnNewArrayData(bldr)

	case arrow.ExtensionType:
		storage := arrayFromJSON(mem, dt.StorageType(), arr)
		defer storage.Release()
		return array.NewData(dt, storage.Len(), storage.Buffers(), storage.Children(), storage.NullN(), storage.Offset())

	case *arrow.DictionaryType:
		indices := arrayFromJSON(mem, dt.IndexType, arr)
		defer indices.Release()
		return array.NewData(dt, indices.Len(), indices.Buffers(), indices.Children(), indices.NullN(), indices.Offset())

	case *arrow.RunEndEncodedType:
		runEnds := arrayFromJSON(mem, dt.RunEnds(), arr.Children[0])
		defer runEnds.Release()
		values := arrayFromJSON(mem, dt.Encoded(), arr.Children[1])
		defer values.Release()
		return array.NewData(dt, arr.Count, []*memory.Buffer{nil}, []arrow.ArrayData{runEnds, values}, 0, 0)

	case arrow.UnionType:
		fields := make([]arrow.ArrayData, len(dt.Fields()))
		for i, f := range dt.Fields() {
			child := arrayFromJSON(mem, f.Type, arr.Children[i])
			defer child.Release()
			fields[i] = child
		}

		typeIdBuf := memory.NewBufferBytes(arrow.Int8Traits.CastToBytes(arr.TypeID))
		defer typeIdBuf.Release()
		buffers := []*memory.Buffer{nil, typeIdBuf}
		if dt.Mode() == arrow.DenseMode {
			var offsets []byte
			if arr.Offset == nil {
				offsets = []byte{}
			} else {
				offsets = arrow.Int32Traits.CastToBytes(arr.Offset.([]int32))
			}
			offsetBuf := memory.NewBufferBytes(offsets)
			defer offsetBuf.Release()
			buffers = append(buffers, offsetBuf)
		}

		return array.NewData(dt, arr.Count, buffers, fields, 0, 0)

	default:
		panic(fmt.Errorf("unknown data type %v %T", dt, dt))
	}
}

func arrayToJSON(field arrow.Field, arr arrow.Array) Array {
	switch arr := arr.(type) {
	case *array.Null:
		return Array{
			Name:  field.Name,
			Count: arr.Len(),
		}

	case *array.Boolean:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   boolsToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Int8:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   i8ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Int16:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   i16ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Int32:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   i32ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Int64:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   i64ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Uint8:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   u8ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Uint16:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   u16ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Uint32:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   u32ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Uint64:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   u64ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Float16:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   f16ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Float32:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   f32ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Float64:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   f64ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.String:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   strToJSON(arr),
			Valids: validsToJSON(arr),
			Offset: arr.ValueOffsets(),
		}

	case *array.LargeString:
		offsets := arr.ValueOffsets()
		strOffsets := make([]string, len(offsets))
		for i, o := range offsets {
			strOffsets[i] = strconv.FormatInt(o, 10)
		}
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   strToJSON(arr),
			Valids: validsToJSON(arr),
			Offset: strOffsets,
		}

	case *array.Binary:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   bytesToJSON(arr),
			Valids: validsToJSON(arr),
			Offset: arr.ValueOffsets(),
		}

	case *array.LargeBinary:
		offsets := arr.ValueOffsets()
		strOffsets := make([]string, len(offsets))
		for i, o := range offsets {
			strOffsets[i] = strconv.FormatInt(o, 10)
		}
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   bytesToJSON(arr),
			Valids: validsToJSON(arr),
			Offset: strOffsets,
		}

	case *array.List:
		o := Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Valids: validsToJSON(arr),
			Offset: arr.Offsets(),
			Children: []Array{
				arrayToJSON(arrow.Field{Name: "item", Type: arr.DataType().(*arrow.ListType).Elem()}, arr.ListValues()),
			},
		}
		return o

	case *array.LargeList:
		offsets := arr.Offsets()
		strOffsets := make([]string, len(offsets))
		for i, o := range offsets {
			strOffsets[i] = strconv.FormatInt(o, 10)
		}
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Valids: validsToJSON(arr),
			Offset: strOffsets,
			Children: []Array{
				arrayToJSON(arrow.Field{Name: "item", Type: arr.DataType().(*arrow.LargeListType).Elem()}, arr.ListValues()),
			},
		}

	case *array.Map:
		o := Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Valids: validsToJSON(arr),
			Offset: arr.Offsets(),
			Children: []Array{
				arrayToJSON(arrow.Field{Name: "entries", Type: arr.DataType().(*arrow.MapType).ValueType()}, arr.ListValues()),
			},
		}
		return o

	case *array.FixedSizeList:
		o := Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Valids: validsToJSON(arr),
			Children: []Array{
				arrayToJSON(arrow.Field{Name: "", Type: arr.DataType().(*arrow.FixedSizeListType).Elem()}, arr.ListValues()),
			},
		}
		return o

	case *array.Struct:
		dt := arr.DataType().(*arrow.StructType)
		o := Array{
			Name:     field.Name,
			Count:    arr.Len(),
			Valids:   validsToJSON(arr),
			Children: make([]Array, len(dt.Fields())),
		}
		for i := range o.Children {
			o.Children[i] = arrayToJSON(dt.Field(i), arr.Field(i))
		}
		return o

	case *array.FixedSizeBinary:
		dt := arr.DataType().(*arrow.FixedSizeBinaryType)
		o := Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Valids: validsToJSON(arr),
			Data:   make([]interface{}, arr.Len()),
		}
		for i := range o.Data {
			v := []byte(strings.ToUpper(hex.EncodeToString(arr.Value(i))))
			if len(v) != 2*dt.ByteWidth {
				panic(fmt.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
			}
			o.Data[i] = string(v) // re-convert as string to prevent json.Marshal from base64-encoding it.
		}
		return o

	case *array.Date32:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   date32ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Date64:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   date64ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Time32:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   time32ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Time64:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   time64ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Timestamp:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   timestampToJSON(arr),
			Valids: validsToJSON(arr),
		}
	case *array.MonthInterval:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   monthintervalToJSON(arr),
			Valids: validsToJSON(arr),
		}
	case *array.DayTimeInterval:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   daytimeintervalToJSON(arr),
			Valids: validsToJSON(arr),
		}
	case *array.MonthDayNanoInterval:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   monthDayNanointervalToJSON(arr),
			Valids: validsToJSON(arr),
		}
	case *array.Duration:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   durationToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Decimal128:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   decimal128ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case *array.Decimal256:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   decimal256ToJSON(arr),
			Valids: validsToJSON(arr),
		}

	case array.ExtensionArray:
		return arrayToJSON(field, arr.Storage())

	case *array.Dictionary:
		return arrayToJSON(field, arr.Indices())

	case array.Union:
		dt := arr.DataType().(arrow.UnionType)
		o := Array{
			Name:     field.Name,
			Count:    arr.Len(),
			Valids:   validsToJSON(arr),
			TypeID:   arr.RawTypeCodes(),
			Children: make([]Array, len(dt.Fields())),
		}
		if dt.Mode() == arrow.DenseMode {
			o.Offset = arr.(*array.DenseUnion).RawValueOffsets()
		}
		fields := dt.Fields()
		for i := range o.Children {
			o.Children[i] = arrayToJSON(fields[i], arr.Field(i))
		}
		return o

	case *array.RunEndEncoded:
		dt := arr.DataType().(*arrow.RunEndEncodedType)
		fields := dt.Fields()
		runEnds := arr.LogicalRunEndsArray(memory.DefaultAllocator)
		defer runEnds.Release()
		values := arr.LogicalValuesArray()
		defer values.Release()
		return Array{
			Name:  field.Name,
			Count: arr.Len(),
			Children: []Array{
				arrayToJSON(fields[0], runEnds),
				arrayToJSON(fields[1], values),
			},
		}

	default:
		panic(fmt.Errorf("unknown array type %T", arr))
	}
}

func validsFromJSON(vs []int) []bool {
	o := make([]bool, len(vs))
	for i, v := range vs {
		if v > 0 {
			o[i] = true
		}
	}
	return o
}

func validsToJSON(arr arrow.Array) []int {
	o := make([]int, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = 1
		}
	}
	return o
}

func boolsFromJSON(vs []interface{}) []bool {
	o := make([]bool, len(vs))
	for i, v := range vs {
		o[i] = v.(bool)
	}
	return o
}

func boolsToJSON(arr *array.Boolean) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i8FromJSON(vs []interface{}) []int8 {
	o := make([]int8, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = int8(vv)
	}
	return o
}

func i8ToJSON(arr *array.Int8) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i16FromJSON(vs []interface{}) []int16 {
	o := make([]int16, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = int16(vv)
	}
	return o
}

func i16ToJSON(arr *array.Int16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i32FromJSON(vs []interface{}) []int32 {
	o := make([]int32, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = int32(vv)
	}
	return o
}

func i32ToJSON(arr *array.Int32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func i64FromJSON(vs []interface{}) []int64 {
	o := make([]int64, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = vv
	}
	return o
}

func i64ToJSON(arr *array.Int64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatInt(arr.Value(i), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

func u8FromJSON(vs []interface{}) []uint8 {
	o := make([]uint8, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = uint8(vv)
	}
	return o
}

func u8ToJSON(arr *array.Uint8) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u16FromJSON(vs []interface{}) []uint16 {
	o := make([]uint16, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = uint16(vv)
	}
	return o
}

func u16ToJSON(arr *array.Uint16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u32FromJSON(vs []interface{}) []uint32 {
	o := make([]uint32, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = uint32(vv)
	}
	return o
}

func u32ToJSON(arr *array.Uint32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func u64FromJSON(vs []interface{}) []uint64 {
	o := make([]uint64, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseUint(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = vv
	}
	return o
}

func u64ToJSON(arr *array.Uint64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatUint(arr.Value(i), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

func f16FromJSON(vs []interface{}) []float16.Num {
	o := make([]float16.Num, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Float64()
		if err != nil {
			panic(err)
		}
		o[i] = float16.New(float32(vv))
	}
	return o
}

func f16ToJSON(arr *array.Float16) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i).Float32()
	}
	return o
}

func f32FromJSON(vs []interface{}) []float32 {
	o := make([]float32, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Float64()
		if err != nil {
			panic(err)
		}
		o[i] = float32(vv)
	}
	return o
}

func f32ToJSON(arr *array.Float32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func f64FromJSON(vs []interface{}) []float64 {
	o := make([]float64, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Float64()
		if err != nil {
			panic(err)
		}
		o[i] = float64(vv)
	}
	return o
}

func f64ToJSON(arr *array.Float64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func decimal128ToJSON(arr *array.Decimal128) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i).BigInt().String()
	}
	return o
}

func decimal128FromJSON(vs []interface{}) []decimal128.Num {
	var tmp big.Int
	o := make([]decimal128.Num, len(vs))
	for i, v := range vs {
		if err := tmp.UnmarshalJSON([]byte(v.(string))); err != nil {
			panic(fmt.Errorf("could not convert %v (%T) to decimal128: %w", v, v, err))
		}

		o[i] = decimal128.FromBigInt(&tmp)
	}
	return o
}

func decimal256ToJSON(arr *array.Decimal256) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i).BigInt().String()
	}
	return o
}

func decimal256FromJSON(vs []interface{}) []decimal256.Num {
	var tmp big.Int
	o := make([]decimal256.Num, len(vs))
	for i, v := range vs {
		if err := tmp.UnmarshalJSON([]byte(v.(string))); err != nil {
			panic(fmt.Errorf("could not convert %v (%T) to decimal128: %w", v, v, err))
		}

		o[i] = decimal256.FromBigInt(&tmp)
	}
	return o
}

func strFromJSON(vs []interface{}) []string {
	o := make([]string, len(vs))
	for i, v := range vs {
		switch v := v.(type) {
		case string:
			o[i] = v
		case json.Number:
			o[i] = v.String()
		default:
			panic(fmt.Errorf("could not convert %v (%T) to a string", v, v))
		}
	}
	return o
}

type strlike interface {
	arrow.Array
	Value(int) string
}

func strToJSON(arr strlike) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func bytesFromJSON(vs []interface{}) [][]byte {
	o := make([][]byte, len(vs))
	for i, v := range vs {
		var err error
		switch v := v.(type) {
		case string:
			o[i], err = hex.DecodeString(v)
		case json.Number:
			o[i], err = hex.DecodeString(v.String())
		default:
			panic(fmt.Errorf("could not convert %v (%T) to a string", v, v))
		}
		if err != nil {
			panic(fmt.Errorf("could not decode %v: %v", v, err))
		}
	}
	return o
}

type binarylike interface {
	arrow.Array
	Value(int) []byte
}

func bytesToJSON(arr binarylike) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = strings.ToUpper(hex.EncodeToString(arr.Value(i)))
	}
	return o
}

func date32FromJSON(vs []interface{}) []arrow.Date32 {
	o := make([]arrow.Date32, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Date32(vv)
	}
	return o
}

func date32ToJSON(arr *array.Date32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func date64FromJSON(vs []interface{}) []arrow.Date64 {
	o := make([]arrow.Date64, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Date64(vv)
	}
	return o
}

func date64ToJSON(arr *array.Date64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatInt(int64(arr.Value(i)), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

func time32FromJSON(vs []interface{}) []arrow.Time32 {
	o := make([]arrow.Time32, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Time32(vv)
	}
	return o
}

func time32ToJSON(arr *array.Time32) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func time64FromJSON(vs []interface{}) []arrow.Time64 {
	o := make([]arrow.Time64, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Time64(vv)
	}
	return o
}

func time64ToJSON(arr *array.Time64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatInt(int64(arr.Value(i)), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

func timestampFromJSON(vs []interface{}) []arrow.Timestamp {
	o := make([]arrow.Timestamp, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Timestamp(vv)
	}
	return o
}

func timestampToJSON(arr *array.Timestamp) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatInt(int64(arr.Value(i)), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

func monthintervalFromJSON(vs []interface{}) []arrow.MonthInterval {
	o := make([]arrow.MonthInterval, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = arrow.MonthInterval(int32(vv))
	}
	return o
}

func monthintervalToJSON(arr *array.MonthInterval) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = int32(arr.Value(i))
	}
	return o
}

func daytimeintervalFromJSON(vs []interface{}) []arrow.DayTimeInterval {
	o := make([]arrow.DayTimeInterval, len(vs))
	for i, vv := range vs {
		v := vv.(map[string]interface{})
		days, err := v["days"].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		ms, err := v["milliseconds"].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = arrow.DayTimeInterval{Days: int32(days), Milliseconds: int32(ms)}
	}
	return o
}

func daytimeintervalToJSON(arr *array.DayTimeInterval) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func monthDayNanointervalFromJSON(vs []interface{}) []arrow.MonthDayNanoInterval {
	o := make([]arrow.MonthDayNanoInterval, len(vs))
	for i, vv := range vs {
		v := vv.(map[string]interface{})
		months, err := v["months"].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		days, err := v["days"].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		ns, err := v["nanoseconds"].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = arrow.MonthDayNanoInterval{Months: int32(months), Days: int32(days), Nanoseconds: ns}
	}
	return o
}

func monthDayNanointervalToJSON(arr *array.MonthDayNanoInterval) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
	}
	return o
}

func durationFromJSON(vs []interface{}) []arrow.Duration {
	o := make([]arrow.Duration, len(vs))
	for i, v := range vs {
		vv, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = arrow.Duration(vv)
	}
	return o
}

func durationToJSON(arr *array.Duration) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		if arr.IsValid(i) {
			o[i] = strconv.FormatInt(int64(arr.Value(i)), 10)
		} else {
			o[i] = "0"
		}
	}
	return o
}

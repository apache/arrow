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
package arrjson // import "github.com/apache/arrow/go/arrow/internal/arrjson"

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
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

type Field struct {
	Name string `json:"name"`
	// the arrowType will get populated during unmarshalling by processing the
	// Type, and will be used to generate the Type during Marshalling to JSON
	arrowType arrow.DataType `json:"-"`
	// leave this as a json RawMessage in order to partially unmarshal as needed
	// during marshal/unmarshal time so we can determine what the structure is
	// actually expected to be.
	Type      json.RawMessage `json:"type"`
	Nullable  bool            `json:"nullable"`
	Children  []FieldWrapper  `json:"children"`
	arrowMeta arrow.Metadata  `json:"-"`
	Metadata  []metaKV        `json:"metadata,omitempty"`
}

type metaKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

	var typ interface{}
	switch dt := f.arrowType.(type) {
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
	case *arrow.StringType:
		typ = nameJSON{"utf8"}
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
	case *arrow.MapType:
		typ = mapJSON{Name: "map", KeysSorted: dt.KeysSorted}
	case *arrow.StructType:
		typ = nameJSON{"struct"}
	case *arrow.FixedSizeListType:
		typ = listSizeJSON{"fixedsizelist", dt.Len()}
	case *arrow.FixedSizeBinaryType:
		typ = byteWidthJSON{"fixedsizebinary", dt.ByteWidth}
	case *arrow.Decimal128Type:
		typ = decimalJSON{"decimal", int(dt.Scale), int(dt.Precision)}
	default:
		return nil, xerrors.Errorf("unknown arrow.DataType %v", f.arrowType)
	}

	var err error
	if f.Type, err = json.Marshal(typ); err != nil {
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

func (f *FieldWrapper) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &f.Field); err != nil {
		return err
	}

	tmp := nameJSON{}
	if err := json.Unmarshal(f.Type, &tmp); err != nil {
		return err
	}

	switch tmp.Name {
	case "null":
		f.arrowType = arrow.Null
	case "bool":
		f.arrowType = arrow.FixedWidthTypes.Boolean
	case "int":
		t := bitWidthJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.Signed {
		case true:
			switch t.BitWidth {
			case 8:
				f.arrowType = arrow.PrimitiveTypes.Int8
			case 16:
				f.arrowType = arrow.PrimitiveTypes.Int16
			case 32:
				f.arrowType = arrow.PrimitiveTypes.Int32
			case 64:
				f.arrowType = arrow.PrimitiveTypes.Int64
			}
		default:
			switch t.BitWidth {
			case 8:
				f.arrowType = arrow.PrimitiveTypes.Uint8
			case 16:
				f.arrowType = arrow.PrimitiveTypes.Uint16
			case 32:
				f.arrowType = arrow.PrimitiveTypes.Uint32
			case 64:
				f.arrowType = arrow.PrimitiveTypes.Uint64
			}
		}
	case "floatingpoint":
		t := floatJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.Precision {
		case "HALF":
			f.arrowType = arrow.FixedWidthTypes.Float16
		case "SINGLE":
			f.arrowType = arrow.PrimitiveTypes.Float32
		case "DOUBLE":
			f.arrowType = arrow.PrimitiveTypes.Float64
		}
	case "binary":
		f.arrowType = arrow.BinaryTypes.Binary
	case "utf8":
		f.arrowType = arrow.BinaryTypes.String
	case "date":
		t := unitZoneJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.Unit {
		case "DAY":
			f.arrowType = arrow.FixedWidthTypes.Date32
		case "MILLISECOND":
			f.arrowType = arrow.FixedWidthTypes.Date64
		}
	case "time":
		t := bitWidthJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.BitWidth {
		case 32:
			switch t.Unit {
			case "SECOND":
				f.arrowType = arrow.FixedWidthTypes.Time32s
			case "MILLISECOND":
				f.arrowType = arrow.FixedWidthTypes.Time32ms
			}
		case 64:
			switch t.Unit {
			case "MICROSECOND":
				f.arrowType = arrow.FixedWidthTypes.Time64us
			case "NANOSECOND":
				f.arrowType = arrow.FixedWidthTypes.Time64ns
			}
		}
	case "timestamp":
		t := unitZoneJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		f.arrowType = &arrow.TimestampType{TimeZone: t.TimeZone}
		switch t.Unit {
		case "SECOND":
			f.arrowType.(*arrow.TimestampType).Unit = arrow.Second
		case "MILLISECOND":
			f.arrowType.(*arrow.TimestampType).Unit = arrow.Millisecond
		case "MICROSECOND":
			f.arrowType.(*arrow.TimestampType).Unit = arrow.Microsecond
		case "NANOSECOND":
			f.arrowType.(*arrow.TimestampType).Unit = arrow.Nanosecond
		}
	case "list":
		f.arrowType = arrow.ListOf(f.Children[0].arrowType)
		f.arrowType.(*arrow.ListType).Meta = f.Children[0].arrowMeta
		f.arrowType.(*arrow.ListType).NullableElem = f.Children[0].Nullable
	case "map":
		t := mapJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		pairType := f.Children[0].arrowType
		f.arrowType = arrow.MapOf(pairType.(*arrow.StructType).Field(0).Type, pairType.(*arrow.StructType).Field(1).Type)
		f.arrowType.(*arrow.MapType).KeysSorted = t.KeysSorted
	case "struct":
		f.arrowType = arrow.StructOf(fieldsFromJSON(f.Children)...)
	case "fixedsizebinary":
		t := byteWidthJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		f.arrowType = &arrow.FixedSizeBinaryType{ByteWidth: t.ByteWidth}
	case "fixedsizelist":
		t := listSizeJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		f.arrowType = arrow.FixedSizeListOf(t.ListSize, f.Children[0].arrowType)
		f.arrowType.(*arrow.FixedSizeListType).NullableElem = f.Children[0].Nullable
		f.arrowType.(*arrow.FixedSizeListType).Meta = f.Children[0].arrowMeta
	case "interval":
		t := unitZoneJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.Unit {
		case "YEAR_MONTH":
			f.arrowType = arrow.FixedWidthTypes.MonthInterval
		case "DAY_TIME":
			f.arrowType = arrow.FixedWidthTypes.DayTimeInterval
		case "MONTH_DAY_NANO":
			f.arrowType = arrow.FixedWidthTypes.MonthDayNanoInterval
		}
	case "duration":
		t := unitZoneJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		switch t.Unit {
		case "SECOND":
			f.arrowType = arrow.FixedWidthTypes.Duration_s
		case "MILLISECOND":
			f.arrowType = arrow.FixedWidthTypes.Duration_ms
		case "MICROSECOND":
			f.arrowType = arrow.FixedWidthTypes.Duration_us
		case "NANOSECOND":
			f.arrowType = arrow.FixedWidthTypes.Duration_ns
		}
	case "decimal":
		t := decimalJSON{}
		if err := json.Unmarshal(f.Type, &t); err != nil {
			return err
		}
		f.arrowType = &arrow.Decimal128Type{Precision: int32(t.Precision), Scale: int32(t.Scale)}
	}

	if f.arrowType == nil {
		return xerrors.Errorf("unhandled type unmarshalling from json: %s", tmp.Name)
	}

	var err error
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
}

type byteWidthJSON struct {
	Name      string `json:"name"`
	ByteWidth int    `json:"byteWidth,omitempty"`
}

type mapJSON struct {
	Name       string `json:"name"`
	KeysSorted bool   `json:"keysSorted,omitempty"`
}

func schemaToJSON(schema *arrow.Schema) Schema {
	return Schema{
		Fields:    fieldsToJSON(schema.Fields()),
		arrowMeta: schema.Metadata(),
	}
}

func schemaFromJSON(schema Schema) *arrow.Schema {
	return arrow.NewSchema(fieldsFromJSON(schema.Fields), &schema.arrowMeta)
}

func fieldsToJSON(fields []arrow.Field) []FieldWrapper {
	o := make([]FieldWrapper, len(fields))
	for i, f := range fields {
		o[i] = FieldWrapper{Field{
			Name:      f.Name,
			arrowType: f.Type,
			Nullable:  f.Nullable,
			Children:  []FieldWrapper{},
			arrowMeta: f.Metadata,
		}}
		switch dt := f.Type.(type) {
		case *arrow.ListType:
			o[i].Children = fieldsToJSON([]arrow.Field{dt.ElemField()})
		case *arrow.FixedSizeListType:
			o[i].Children = fieldsToJSON([]arrow.Field{dt.ElemField()})
		case *arrow.StructType:
			o[i].Children = fieldsToJSON(dt.Fields())
		case *arrow.MapType:
			o[i].Children = fieldsToJSON([]arrow.Field{dt.ValueField()})
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

type Record struct {
	Count   int64   `json:"count"`
	Columns []Array `json:"columns"`
}

func recordsFromJSON(mem memory.Allocator, schema *arrow.Schema, recs []Record) []array.Record {
	vs := make([]array.Record, len(recs))
	for i, rec := range recs {
		vs[i] = recordFromJSON(mem, schema, rec)
	}
	return vs
}

func recordFromJSON(mem memory.Allocator, schema *arrow.Schema, rec Record) array.Record {
	arrs := arraysFromJSON(mem, schema, rec.Columns)
	defer func() {
		for _, arr := range arrs {
			arr.Release()
		}
	}()
	return array.NewRecord(schema, arrs, int64(rec.Count))
}

func recordToJSON(rec array.Record) Record {
	return Record{
		Count:   rec.NumRows(),
		Columns: arraysToJSON(rec.Schema(), rec.Columns()),
	}
}

type Array struct {
	Name     string        `json:"name"`
	Count    int           `json:"count"`
	Valids   []int         `json:"VALIDITY,omitempty"`
	Data     []interface{} `json:"DATA,omitempty"`
	Offset   []int32       `json:"OFFSET,omitempty"`
	Children []Array       `json:"children,omitempty"`
}

func arraysFromJSON(mem memory.Allocator, schema *arrow.Schema, arrs []Array) []array.Interface {
	o := make([]array.Interface, len(arrs))
	for i, v := range arrs {
		o[i] = arrayFromJSON(mem, schema.Field(i).Type, v)
	}
	return o
}

func arraysToJSON(schema *arrow.Schema, arrs []array.Interface) []Array {
	o := make([]Array, len(arrs))
	for i, v := range arrs {
		o[i] = arrayToJSON(schema.Field(i), v)
	}
	return o
}

func arrayFromJSON(mem memory.Allocator, dt arrow.DataType, arr Array) array.Interface {
	switch dt := dt.(type) {
	case *arrow.NullType:
		return array.NewNull(arr.Count)

	case *arrow.BooleanType:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()
		data := boolsFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int8Type:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()
		data := i8FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int16Type:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()
		data := i16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int32Type:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		data := i32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int64Type:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		data := i64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint8Type:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()
		data := u8FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint16Type:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()
		data := u16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint32Type:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()
		data := u32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint64Type:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()
		data := u64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float16Type:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()
		data := f16FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float32Type:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()
		data := f32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float64Type:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()
		data := f64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.StringType:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		data := strFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.BinaryType:
		bldr := array.NewBinaryBuilder(mem, dt)
		defer bldr.Release()
		data := bytesFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.ListType:
		bldr := array.NewListBuilder(mem, dt.Elem())
		defer bldr.Release()
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.Elem(), arr.Children[0])
		defer elems.Release()
		for i, v := range valids {
			bldr.Append(v)
			beg := int64(arr.Offset[i])
			end := int64(arr.Offset[i+1])
			buildArray(bldr.ValueBuilder(), array.NewSlice(elems, beg, end))
		}
		return bldr.NewArray()

	case *arrow.FixedSizeListType:
		bldr := array.NewFixedSizeListBuilder(mem, dt.Len(), dt.Elem())
		defer bldr.Release()
		valids := validsFromJSON(arr.Valids)
		elems := arrayFromJSON(mem, dt.Elem(), arr.Children[0])
		defer elems.Release()
		size := int64(dt.Len())
		for i, v := range valids {
			bldr.Append(v)
			beg := int64(i) * size
			end := int64(i+1) * size
			buildArray(bldr.ValueBuilder(), array.NewSlice(elems, beg, end))
		}
		return bldr.NewArray()

	case *arrow.StructType:
		bldr := array.NewStructBuilder(mem, dt)
		defer bldr.Release()
		valids := validsFromJSON(arr.Valids)
		fields := make([]array.Interface, len(dt.Fields()))
		for i := range fields {
			fields[i] = arrayFromJSON(mem, dt.Field(i).Type, arr.Children[i])
		}

		bldr.AppendValues(valids)
		for i := range dt.Fields() {
			fbldr := bldr.FieldBuilder(i)
			buildArray(fbldr, fields[i])
		}

		return bldr.NewArray()

	case *arrow.FixedSizeBinaryType:
		bldr := array.NewFixedSizeBinaryBuilder(mem, dt)
		defer bldr.Release()
		strdata := strFromJSON(arr.Data)
		data := make([][]byte, len(strdata))
		for i, v := range strdata {
			if len(v) != 2*dt.ByteWidth {
				panic(xerrors.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
			}
			vv, err := hex.DecodeString(v)
			if err != nil {
				panic(err)
			}
			data[i] = vv
		}
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.MapType:
		bldr := array.NewMapBuilder(mem, dt.KeyType(), dt.ItemType(), dt.KeysSorted)
		defer bldr.Release()
		valids := validsFromJSON(arr.Valids)
		pairs := arrayFromJSON(mem, dt.ValueType(), arr.Children[0])
		defer pairs.Release()
		for i, v := range valids {
			bldr.Append(v)
			beg := int64(arr.Offset[i])
			end := int64(arr.Offset[i+1])
			kb := bldr.KeyBuilder()
			buildArray(kb, array.NewSlice(pairs.(*array.Struct).Field(0), beg, end))
			ib := bldr.ItemBuilder()
			buildArray(ib, array.NewSlice(pairs.(*array.Struct).Field(1), beg, end))
		}
		return bldr.NewArray()

	case *arrow.Date32Type:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()
		data := date32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Date64Type:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()
		data := date64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Time32Type:
		bldr := array.NewTime32Builder(mem, dt)
		defer bldr.Release()
		data := time32FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Time64Type:
		bldr := array.NewTime64Builder(mem, dt)
		defer bldr.Release()
		data := time64FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.TimestampType:
		bldr := array.NewTimestampBuilder(mem, dt)
		defer bldr.Release()
		data := timestampFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.MonthIntervalType:
		bldr := array.NewMonthIntervalBuilder(mem)
		defer bldr.Release()
		data := monthintervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.DayTimeIntervalType:
		bldr := array.NewDayTimeIntervalBuilder(mem)
		defer bldr.Release()
		data := daytimeintervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.MonthDayNanoIntervalType:
		bldr := array.NewMonthDayNanoIntervalBuilder(mem)
		defer bldr.Release()
		data := monthDayNanointervalFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.DurationType:
		bldr := array.NewDurationBuilder(mem, dt)
		defer bldr.Release()
		data := durationFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Decimal128Type:
		bldr := array.NewDecimal128Builder(mem, dt)
		defer bldr.Release()
		data := decimal128FromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case arrow.ExtensionType:
		storage := arrayFromJSON(mem, dt.StorageType(), arr)
		defer storage.Release()
		return array.NewExtensionArrayWithStorage(dt, storage)

	default:
		panic(xerrors.Errorf("unknown data type %v %T", dt, dt))
	}
	panic("impossible")
}

func arrayToJSON(field arrow.Field, arr array.Interface) Array {
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
		}

	case *array.Binary:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   bytesToJSON(arr),
			Valids: validsToJSON(arr),
			Offset: arr.ValueOffsets(),
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
				panic(xerrors.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
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

	case array.ExtensionArray:
		return arrayToJSON(field, arr.Storage())

	default:
		panic(xerrors.Errorf("unknown array type %T", arr))
	}
	panic("impossible")
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

func validsToJSON(arr array.Interface) []int {
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
			panic(xerrors.Errorf("could not convert %v (%T) to decimal128: %w", v, v, err))
		}

		o[i] = decimal128.FromBigInt(&tmp)
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
			panic(xerrors.Errorf("could not convert %v (%T) to a string", v, v))
		}
	}
	return o
}

func strToJSON(arr *array.String) []interface{} {
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
			panic(xerrors.Errorf("could not convert %v (%T) to a string", v, v))
		}
		if err != nil {
			panic(xerrors.Errorf("could not decode %v: %v", v, err))
		}
	}
	return o
}

func bytesToJSON(arr *array.Binary) []interface{} {
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

func buildArray(bldr array.Builder, data array.Interface) {
	defer data.Release()

	switch bldr := bldr.(type) {
	default:
		panic(xerrors.Errorf("unknown builder %T", bldr))

	case *array.BooleanBuilder:
		data := data.(*array.Boolean)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int8Builder:
		data := data.(*array.Int8)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int16Builder:
		data := data.(*array.Int16)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int32Builder:
		data := data.(*array.Int32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int64Builder:
		data := data.(*array.Int64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint8Builder:
		data := data.(*array.Uint8)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint16Builder:
		data := data.(*array.Uint16)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint32Builder:
		data := data.(*array.Uint32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint64Builder:
		data := data.(*array.Uint64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Float32Builder:
		data := data.(*array.Float32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Float64Builder:
		data := data.(*array.Float64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.StringBuilder:
		data := data.(*array.String)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}
	}
}

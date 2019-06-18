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
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pkg/errors"
)

const (
	kData         = "DATA"
	kDays         = "days"
	kDayTime      = "DAY_TIME"
	kDuration     = "duration"
	kMilliseconds = "milliseconds"
	kYearMonth    = "YEAR_MONTH"
)

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Name     string   `json:"name"`
	Type     dataType `json:"type"`
	Nullable bool     `json:"nullable"`
	Children []Field  `json:"children"`
}

type dataType struct {
	Name      string `json:"name"`
	Signed    bool   `json:"isSigned,omitempty"`
	BitWidth  int    `json:"bitWidth,omitempty"`
	Precision string `json:"precision,omitempty"`
	ByteWidth int    `json:"byteWidth,omitempty"`
	ListSize  int32  `json:"listSize,omitempty"`
	Unit      string `json:"unit,omitempty"`
	TimeZone  string `json:"timezone,omitempty"`
	Scale     int    `json:"scale,omitempty"` // for Decimal128
}

func dtypeToJSON(dt arrow.DataType) dataType {
	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return dataType{Name: "bool"}
	case *arrow.Int8Type:
		return dataType{Name: "int", Signed: true, BitWidth: 8}
	case *arrow.Int16Type:
		return dataType{Name: "int", Signed: true, BitWidth: 16}
	case *arrow.Int32Type:
		return dataType{Name: "int", Signed: true, BitWidth: 32}
	case *arrow.Int64Type:
		return dataType{Name: "int", Signed: true, BitWidth: 64}
	case *arrow.Uint8Type:
		return dataType{Name: "int", BitWidth: 8}
	case *arrow.Uint16Type:
		return dataType{Name: "int", BitWidth: 16}
	case *arrow.Uint32Type:
		return dataType{Name: "int", BitWidth: 32}
	case *arrow.Uint64Type:
		return dataType{Name: "int", BitWidth: 64}
	case *arrow.Float16Type:
		return dataType{Name: "floatingpoint", Precision: "HALF"}
	case *arrow.Float32Type:
		return dataType{Name: "floatingpoint", Precision: "SINGLE"}
	case *arrow.Float64Type:
		return dataType{Name: "floatingpoint", Precision: "DOUBLE"}
	case *arrow.BinaryType:
		return dataType{Name: "binary"}
	case *arrow.StringType:
		return dataType{Name: "utf8"}
	case *arrow.Date32Type:
		return dataType{Name: "date", Unit: "DAY"}
	case *arrow.Date64Type:
		return dataType{Name: "date", Unit: "MILLISECOND"}
	case *arrow.Time32Type:
		switch dt.Unit {
		case arrow.Second:
			return dataType{Name: "time", Unit: "SECOND", BitWidth: dt.BitWidth()}
		case arrow.Millisecond:
			return dataType{Name: "time", Unit: "MILLISECOND", BitWidth: dt.BitWidth()}
		}
	case *arrow.Time64Type:
		switch dt.Unit {
		case arrow.Microsecond:
			return dataType{Name: "time", Unit: "MICROSECOND", BitWidth: dt.BitWidth()}
		case arrow.Nanosecond:
			return dataType{Name: "time", Unit: "NANOSECOND", BitWidth: dt.BitWidth()}
		}
	case *arrow.TimestampType:
		switch dt.Unit {
		case arrow.Second:
			return dataType{Name: "timestamp", Unit: "SECOND", TimeZone: dt.TimeZone}
		case arrow.Millisecond:
			return dataType{Name: "timestamp", Unit: "MILLISECOND", TimeZone: dt.TimeZone}
		case arrow.Microsecond:
			return dataType{Name: "timestamp", Unit: "MICROSECOND", TimeZone: dt.TimeZone}
		case arrow.Nanosecond:
			return dataType{Name: "timestamp", Unit: "NANOSECOND", TimeZone: dt.TimeZone}
		}
	case *arrow.MonthIntervalType:
		return dataType{Name: "interval", Unit: "YEAR_MONTH"}
	case *arrow.DayTimeIntervalType:
		return dataType{Name: "interval", Unit: "DAY_TIME"}
	case *arrow.DurationType:
		switch dt.Unit {
		case arrow.Second:
			return dataType{Name: "duration", Unit: "SECOND"}
		case arrow.Millisecond:
			return dataType{Name: "duration", Unit: "MILLISECOND"}
		case arrow.Microsecond:
			return dataType{Name: "duration", Unit: "MICROSECOND"}
		case arrow.Nanosecond:
			return dataType{Name: "duration", Unit: "NANOSECOND"}
		}

	case *arrow.ListType:
		return dataType{Name: "list"}
	case *arrow.StructType:
		return dataType{Name: "struct"}
	case *arrow.FixedSizeListType:
		return dataType{Name: "fixedsizelist", ListSize: dt.Len()}
	case *arrow.FixedSizeBinaryType:
		return dataType{
			Name:      "fixedsizebinary",
			ByteWidth: dt.ByteWidth,
		}
	}
	panic(errors.Errorf("unknown arrow.DataType %v", dt))
}

func dtypeFromJSON(dt dataType, children []Field) arrow.DataType {
	switch dt.Name {
	case "bool":
		return arrow.FixedWidthTypes.Boolean
	case "int":
		switch dt.Signed {
		case true:
			switch dt.BitWidth {
			case 8:
				return arrow.PrimitiveTypes.Int8
			case 16:
				return arrow.PrimitiveTypes.Int16
			case 32:
				return arrow.PrimitiveTypes.Int32
			case 64:
				return arrow.PrimitiveTypes.Int64
			}
		default:
			switch dt.BitWidth {
			case 8:
				return arrow.PrimitiveTypes.Uint8
			case 16:
				return arrow.PrimitiveTypes.Uint16
			case 32:
				return arrow.PrimitiveTypes.Uint32
			case 64:
				return arrow.PrimitiveTypes.Uint64
			}
		}
	case "floatingpoint":
		switch dt.Precision {
		case "HALF":
			return arrow.FixedWidthTypes.Float16
		case "SINGLE":
			return arrow.PrimitiveTypes.Float32
		case "DOUBLE":
			return arrow.PrimitiveTypes.Float64
		}
	case "binary":
		return arrow.BinaryTypes.Binary
	case "utf8":
		return arrow.BinaryTypes.String
	case "date":
		switch dt.Unit {
		case "DAY":
			return arrow.FixedWidthTypes.Date32
		case "MILLISECOND":
			return arrow.FixedWidthTypes.Date64
		}
	case "time":
		switch dt.BitWidth {
		case 32:
			switch dt.Unit {
			case "SECOND":
				return arrow.FixedWidthTypes.Time32s
			case "MILLISECOND":
				return arrow.FixedWidthTypes.Time32ms
			}
		case 64:
			switch dt.Unit {
			case "MICROSECOND":
				return arrow.FixedWidthTypes.Time64us
			case "NANOSECOND":
				return arrow.FixedWidthTypes.Time64ns
			}
		}
	case "timestamp":
		switch dt.Unit {
		case "SECOND":
			return &arrow.TimestampType{TimeZone: dt.TimeZone, Unit: arrow.Second}
		case "MILLISECOND":
			return &arrow.TimestampType{TimeZone: dt.TimeZone, Unit: arrow.Millisecond}
		case "MICROSECOND":
			return &arrow.TimestampType{TimeZone: dt.TimeZone, Unit: arrow.Microsecond}
		case "NANOSECOND":
			return &arrow.TimestampType{TimeZone: dt.TimeZone, Unit: arrow.Nanosecond}
		}
	case "list":
		return arrow.ListOf(dtypeFromJSON(children[0].Type, nil))
	case "struct":
		return arrow.StructOf(fieldsFromJSON(children)...)
	case "fixedsizebinary":
		return &arrow.FixedSizeBinaryType{ByteWidth: dt.ByteWidth}
	case "fixedsizelist":
		return arrow.FixedSizeListOf(dt.ListSize, dtypeFromJSON(children[0].Type, nil))
	case "interval":
		switch dt.Unit {
		case "YEAR_MONTH":
			return arrow.FixedWidthTypes.MonthInterval
		case "DAY_TIME":
			return arrow.FixedWidthTypes.DayTimeInterval
		}
	case "duration":
		switch dt.Unit {
		case "SECOND":
			return arrow.FixedWidthTypes.Duration_s
		case "MILLISECOND":
			return arrow.FixedWidthTypes.Duration_ms
		case "MICROSECOND":
			return arrow.FixedWidthTypes.Duration_us
		case "NANOSECOND":
			return arrow.FixedWidthTypes.Duration_ns
		}
	}
	panic(errors.Errorf("unknown DataType %#v", dt))
}

func schemaToJSON(schema *arrow.Schema) Schema {
	return Schema{
		Fields: fieldsToJSON(schema.Fields()),
	}
}

func schemaFromJSON(schema Schema) *arrow.Schema {
	return arrow.NewSchema(fieldsFromJSON(schema.Fields), nil)
}

func fieldsToJSON(fields []arrow.Field) []Field {
	o := make([]Field, len(fields))
	for i, f := range fields {
		o[i] = Field{
			Name:     f.Name,
			Type:     dtypeToJSON(f.Type),
			Nullable: f.Nullable,
			Children: []Field{},
		}
		switch dt := f.Type.(type) {
		case *arrow.ListType:
			o[i].Children = fieldsToJSON([]arrow.Field{{Name: "item", Type: dt.Elem(), Nullable: f.Nullable}})
		case *arrow.FixedSizeListType:
			o[i].Children = fieldsToJSON([]arrow.Field{{Name: "item", Type: dt.Elem(), Nullable: f.Nullable}})
		case *arrow.StructType:
			o[i].Children = fieldsToJSON(dt.Fields())
		}
	}
	return o
}

func fieldsFromJSON(fields []Field) []arrow.Field {
	vs := make([]arrow.Field, len(fields))
	for i, v := range fields {
		vs[i] = fieldFromJSON(v)
	}
	return vs
}

func fieldFromJSON(f Field) arrow.Field {
	return arrow.Field{
		Name:     f.Name,
		Type:     dtypeFromJSON(f.Type, f.Children),
		Nullable: f.Nullable,
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
			slice := array.NewSlice(elems, beg, end)
			buildArray(bldr.ValueBuilder(), slice)
			slice.Release()
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
			slice := array.NewSlice(elems, beg, end)
			buildArray(bldr.ValueBuilder(), slice)
			slice.Release()
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
			fields[i].Release()
		}

		return bldr.NewArray()

	case *arrow.FixedSizeBinaryType:
		bldr := array.NewFixedSizeBinaryBuilder(mem, dt)
		defer bldr.Release()
		strdata := strFromJSON(arr.Data)
		data := make([][]byte, len(strdata))
		for i, v := range strdata {
			if len(v) != 2*dt.ByteWidth {
				panic(errors.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
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

	case *arrow.DurationType:
		bldr := array.NewDurationBuilder(mem, dt)
		defer bldr.Release()
		data := durationFromJSON(arr.Data)
		valids := validsFromJSON(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	default:
		panic(errors.Errorf("unknown data type %v %T", dt, dt))
	}
	panic("impossible")
}

func arrayToJSON(field arrow.Field, arr array.Interface) Array {
	switch arr := arr.(type) {
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
				panic(errors.Errorf("arrjson: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth))
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
	case *array.Duration:
		return Array{
			Name:   field.Name,
			Count:  arr.Len(),
			Data:   durationToJSON(arr),
			Valids: validsToJSON(arr),
		}

	default:
		panic(errors.Errorf("unknown array type %T", arr))
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
		vv, err := v.(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		o[i] = int64(vv)
	}
	return o
}

func i64ToJSON(arr *array.Int64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
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
		vv, err := strconv.ParseUint(v.(json.Number).String(), 10, 64)
		if err != nil {
			panic(err)
		}
		o[i] = uint64(vv)
	}
	return o
}

func u64ToJSON(arr *array.Uint64) []interface{} {
	o := make([]interface{}, arr.Len())
	for i := range o {
		o[i] = arr.Value(i)
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

func strFromJSON(vs []interface{}) []string {
	o := make([]string, len(vs))
	for i, v := range vs {
		switch v := v.(type) {
		case string:
			o[i] = v
		case json.Number:
			o[i] = v.String()
		default:
			panic(errors.Errorf("could not convert %v (%T) to a string", v, v))
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
			panic(errors.Errorf("could not convert %v (%T) to a string", v, v))
		}
		if err != nil {
			panic(errors.Errorf("could not decode %v: %v", v, err))
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
		vv, err := v.(json.Number).Int64()
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
		o[i] = int64(arr.Value(i))
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
		vv, err := v.(json.Number).Int64()
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
		o[i] = int64(arr.Value(i))
	}
	return o
}

func timestampFromJSON(vs []interface{}) []arrow.Timestamp {
	o := make([]arrow.Timestamp, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
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
		o[i] = int64(arr.Value(i))
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

func durationFromJSON(vs []interface{}) []arrow.Duration {
	o := make([]arrow.Duration, len(vs))
	for i, v := range vs {
		vv, err := v.(json.Number).Int64()
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
		o[i] = arr.Value(i)
	}
	return o
}

func buildArray(bldr array.Builder, data array.Interface) {
	defer data.Release()

	switch bldr := bldr.(type) {
	default:
		panic(errors.Errorf("unknown builder %T", bldr))

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

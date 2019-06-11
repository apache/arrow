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
	Name     string     `json:"name"`
	Type     dataType   `json:"type"`
	Nullable bool       `json:"nullable"`
	Layout   typeLayout `json:"typeLayout"`
	Children []Field    `json:"children,omitempty"`
}

func (f Field) ToArrow() arrow.Field {
	return arrow.Field{
		Name:     f.Name,
		Type:     f.Type.ToArrow(f.Children),
		Nullable: f.Nullable,
	}
}

type dataType struct {
	Name      string `json:"name"`
	Signed    bool   `json:"isSigned,omitempty"`
	BitWidth  int    `json:"bitWidth,omitempty"`
	Precision string `json:"precision,omitempty"`
	ByteWidth int    `json:"byteWidth,omitempty"`
	Unit      string `json:"unit,omitempty"`
}

var dtypes = map[dataType]arrow.DataType{
	{Name: "bool"}:                                    arrow.FixedWidthTypes.Boolean,
	{Name: "int", Signed: true, BitWidth: 8}:          arrow.PrimitiveTypes.Int8,
	{Name: "int", Signed: true, BitWidth: 16}:         arrow.PrimitiveTypes.Int16,
	{Name: "int", Signed: true, BitWidth: 32}:         arrow.PrimitiveTypes.Int32,
	{Name: "int", Signed: true, BitWidth: 64}:         arrow.PrimitiveTypes.Int64,
	{Name: "int", Signed: false, BitWidth: 8}:         arrow.PrimitiveTypes.Uint8,
	{Name: "int", Signed: false, BitWidth: 16}:        arrow.PrimitiveTypes.Uint16,
	{Name: "int", Signed: false, BitWidth: 32}:        arrow.PrimitiveTypes.Uint32,
	{Name: "int", Signed: false, BitWidth: 64}:        arrow.PrimitiveTypes.Uint64,
	{Name: "floatingpoint", Precision: "SINGLE"}:      arrow.PrimitiveTypes.Float32,
	{Name: "floatingpoint", Precision: "DOUBLE"}:      arrow.PrimitiveTypes.Float64,
	{Name: "binary"}:                                  arrow.BinaryTypes.Binary,
	{Name: "utf8"}:                                    arrow.BinaryTypes.String,
	{Name: "date", Unit: "DAY"}:                       arrow.FixedWidthTypes.Date32,
	{Name: "date", Unit: "MILLISECOND"}:               arrow.FixedWidthTypes.Date64,
	{Name: "time", Unit: "SECOND", BitWidth: 32}:      arrow.FixedWidthTypes.Time32s,
	{Name: "time", Unit: "MILLISECOND", BitWidth: 32}: arrow.FixedWidthTypes.Time32ms,
	{Name: "time", Unit: "MICROSECOND", BitWidth: 64}: arrow.FixedWidthTypes.Time64us,
	{Name: "time", Unit: "NANOSECOND", BitWidth: 64}:  arrow.FixedWidthTypes.Time64ns,
	{Name: "timestamp", Unit: "SECOND"}:               &arrow.TimestampType{TimeZone: "UTC", Unit: arrow.Second},
	{Name: "timestamp", Unit: "MILLISECOND"}:          &arrow.TimestampType{TimeZone: "UTC", Unit: arrow.Millisecond},
	{Name: "timestamp", Unit: "MICROSECOND"}:          &arrow.TimestampType{TimeZone: "UTC", Unit: arrow.Microsecond},
	{Name: "timestamp", Unit: "NANOSECOND"}:           &arrow.TimestampType{TimeZone: "UTC", Unit: arrow.Nanosecond},
}

func dataTypeFrom(dt arrow.DataType) dataType {
	for k, v := range dtypes {
		if arrow.TypeEquals(v, dt) {
			return k
		}
	}
	panic(errors.Errorf("unknown arrow.DataType %v", dt))
}

func (dt dataType) ToArrow(children []Field) arrow.DataType {
	v, ok := dtypes[dt]
	if ok {
		return v
	}
	switch dt.Name {
	case "list":
		return arrow.ListOf(children[0].Type.ToArrow(nil))
	case "struct":
		elems := make([]arrow.Field, len(children))
		for i, v := range children {
			elems[i] = v.ToArrow()
		}
		return arrow.StructOf(elems...)
	case "fixedsizebinary":
		return &arrow.FixedSizeBinaryType{ByteWidth: dt.ByteWidth}
	}
	if !ok {
		panic(errors.Errorf("unknown DataType %#v", dt))
	}
	return v
}

type typeLayout struct {
	Vectors []vector `json:"vectors"`
}

type vector struct {
	Type     string `json:"type"`
	Bitwidth int    `json:"typeBitWidth"`
}

func schemaFromArrow(schema *arrow.Schema) Schema {
	return Schema{
		Fields: fieldsFromArrow(schema.Fields()),
	}
}

func newSchemaFrom(schema Schema) *arrow.Schema {
	return arrow.NewSchema(fieldsToArrow(schema.Fields), nil)
}

func fieldsFromArrow(fields []arrow.Field) []Field {
	o := make([]Field, len(fields))
	for i, f := range fields {
		o[i] = Field{
			Name:     f.Name,
			Type:     dataTypeFrom(f.Type),
			Nullable: f.Nullable,
		}
	}
	return o
}

func fieldsToArrow(fields []Field) []arrow.Field {
	vs := make([]arrow.Field, len(fields))
	for i, v := range fields {
		vs[i] = v.ToArrow()
	}
	return vs
}

type Record struct {
	Count   int     `json:"count"`
	Columns []Array `json:"columns"`
}

func newRecordsFrom(mem memory.Allocator, schema *arrow.Schema, recs []Record) []array.Record {
	vs := make([]array.Record, len(recs))
	for i, rec := range recs {
		vs[i] = newRecordFrom(mem, schema, rec)
	}
	return vs
}

func newRecordFrom(mem memory.Allocator, schema *arrow.Schema, rec Record) array.Record {
	arrs := newArraysFrom(mem, schema, rec.Columns)
	defer func() {
		for _, arr := range arrs {
			arr.Release()
		}
	}()
	return array.NewRecord(schema, arrs, int64(rec.Count))
}

type Array struct {
	Name     string        `json:"name"`
	Count    int           `json:"count"`
	Data     []interface{} `json:"DATA,omitempty"`
	Valids   []int         `json:"VALIDITY,omitempty"`
	Offset   []int         `json:"OFFSET,omitempty"`
	Children []Array       `json:"children,omitempty"`
}

func newArraysFrom(mem memory.Allocator, schema *arrow.Schema, arrs []Array) []array.Interface {
	o := make([]array.Interface, len(arrs))
	for i, v := range arrs {
		o[i] = newArrayFrom(mem, schema.Field(i).Type, v)
	}
	return o
}

func newArrayFrom(mem memory.Allocator, dt arrow.DataType, arr Array) array.Interface {
	switch dt := dt.(type) {
	case *arrow.BooleanType:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()
		data := boolsToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int8Type:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()
		data := i8ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int16Type:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()
		data := i16ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int32Type:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		data := i32ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Int64Type:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		data := i64ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint8Type:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()
		data := u8ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint16Type:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()
		data := u16ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint32Type:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()
		data := u32ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Uint64Type:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()
		data := u64ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float16Type:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()
		data := f16ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float32Type:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()
		data := f32ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Float64Type:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()
		data := f64ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.StringType:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		data := strToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.BinaryType:
		bldr := array.NewBinaryBuilder(mem, dt)
		defer bldr.Release()
		data := bytesToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.ListType:
		bldr := array.NewListBuilder(mem, dt.Elem())
		defer bldr.Release()
		valids := validsToArrow(arr.Valids)
		elems := newArrayFrom(mem, dt.Elem(), arr.Children[0])
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

	case *arrow.StructType:
		bldr := array.NewStructBuilder(mem, dt)
		defer bldr.Release()
		valids := validsToArrow(arr.Valids)
		fields := make([]array.Interface, len(dt.Fields()))
		for i := range fields {
			fields[i] = newArrayFrom(mem, dt.Field(i).Type, arr.Children[i])
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
		strdata := strToArrow(arr.Data)
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
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Date32Type:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()
		data := date32ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Date64Type:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()
		data := date64ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Time32Type:
		bldr := array.NewTime32Builder(mem, dt)
		defer bldr.Release()
		data := time32ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.Time64Type:
		bldr := array.NewTime64Builder(mem, dt)
		defer bldr.Release()
		data := time64ToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	case *arrow.TimestampType:
		bldr := array.NewTimestampBuilder(mem, dt)
		defer bldr.Release()
		data := timestampToArrow(arr.Data)
		valids := validsToArrow(arr.Valids)
		bldr.AppendValues(data, valids)
		return bldr.NewArray()

	default:
		panic(errors.Errorf("unknown data type %v %T", dt, dt))
	}
	panic("impossible")
}

func validsToArrow(vs []int) []bool {
	o := make([]bool, len(vs))
	for i, v := range vs {
		if v > 0 {
			o[i] = true
		}
	}
	return o
}

func validsFromArrow(vs []bool) []int {
	o := make([]int, len(vs))
	for i, v := range vs {
		if v {
			o[i] = 1
		}
	}
	return o
}

func boolsToArrow(vs []interface{}) []bool {
	o := make([]bool, len(vs))
	for i, v := range vs {
		o[i] = v.(bool)
	}
	return o
}

func i8ToArrow(vs []interface{}) []int8 {
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

func i16ToArrow(vs []interface{}) []int16 {
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

func i32ToArrow(vs []interface{}) []int32 {
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

func i64ToArrow(vs []interface{}) []int64 {
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

func u8ToArrow(vs []interface{}) []uint8 {
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

func u16ToArrow(vs []interface{}) []uint16 {
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

func u32ToArrow(vs []interface{}) []uint32 {
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

func u64ToArrow(vs []interface{}) []uint64 {
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

func f16ToArrow(vs []interface{}) []float16.Num {
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

func f32ToArrow(vs []interface{}) []float32 {
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

func f64ToArrow(vs []interface{}) []float64 {
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

func strToArrow(vs []interface{}) []string {
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

func bytesToArrow(vs []interface{}) [][]byte {
	o := make([][]byte, len(vs))
	for i, v := range vs {
		switch v := v.(type) {
		case string:
			o[i] = []byte(v)
		case json.Number:
			o[i] = []byte(v.String())
		default:
			panic(errors.Errorf("could not convert %v (%T) to a string", v, v))
		}
	}
	return o
}

func date32ToArrow(vs []interface{}) []arrow.Date32 {
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

func date64ToArrow(vs []interface{}) []arrow.Date64 {
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

func time32ToArrow(vs []interface{}) []arrow.Time32 {
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

func time64ToArrow(vs []interface{}) []arrow.Time64 {
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

func timestampToArrow(vs []interface{}) []arrow.Timestamp {
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

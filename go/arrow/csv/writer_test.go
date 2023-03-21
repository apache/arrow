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

package csv_test

import (
	"bufio"
	"bytes"
	ecsv "encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/csv"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/decimal256"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

const (
	separator = ';'
	nullVal   = "null"
)

func Example_writer() {
	f := new(bytes.Buffer)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2", "str-3", "str-4", "str-5", "str-6", "str-7", "str-8", "str-9"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	w := csv.NewWriter(f, schema, csv.WithComma(';'))
	err := w.Write(rec)
	if err != nil {
		log.Fatal(err)
	}

	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}

	err = w.Error()
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(f, schema, csv.WithComment('#'), csv.WithComma(';'))
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

	// check for reader errors indicating issues converting csv values
	// to the arrow schema types
	err = r.Err()
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// rec[0]["i64"]: [0]
	// rec[0]["f64"]: [0]
	// rec[0]["str"]: ["str-0"]
	// rec[1]["i64"]: [1]
	// rec[1]["f64"]: [1]
	// rec[1]["str"]: ["str-1"]
	// rec[2]["i64"]: [2]
	// rec[2]["f64"]: [2]
	// rec[2]["str"]: ["str-2"]
	// rec[3]["i64"]: [3]
	// rec[3]["f64"]: [3]
	// rec[3]["str"]: ["str-3"]
	// rec[4]["i64"]: [4]
	// rec[4]["f64"]: [4]
	// rec[4]["str"]: ["str-4"]
	// rec[5]["i64"]: [5]
	// rec[5]["f64"]: [5]
	// rec[5]["str"]: ["str-5"]
	// rec[6]["i64"]: [6]
	// rec[6]["f64"]: [6]
	// rec[6]["str"]: ["str-6"]
	// rec[7]["i64"]: [7]
	// rec[7]["f64"]: [7]
	// rec[7]["str"]: ["str-7"]
	// rec[8]["i64"]: [8]
	// rec[8]["f64"]: [8]
	// rec[8]["str"]: ["str-8"]
	// rec[9]["i64"]: [9]
	// rec[9]["f64"]: [9]
	// rec[9]["str"]: ["str-9"]
}

var (
	fullData = [][]string{
		{"bool", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "str", "ts_s", "d32", "d64", "dec128", "dec256", "list(i64)", "binary"},
		{"true", "-1", "-1", "-1", "-1", "0", "0", "0", "0", "0", "0", "str-0", "2014-07-28 15:04:05", "2017-05-18", "2028-04-26", "-123.45", "-123.45", "{1,2,3}", "AAEC"},
		{"false", "0", "0", "0", "0", "1", "1", "1", "1", "0.1", "0.1", "str-1", "2016-09-08 15:04:05", "2022-11-08", "2031-06-28", "0", "0", "{4,5,6}", "AwQF"},
		{"true", "1", "1", "1", "1", "2", "2", "2", "2", "0.2", "0.2", "str-2", "2021-09-18 15:04:05", "2025-08-04", "2034-08-28", "123.45", "123.45", "{7,8,9}", ""},
		{nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal},
	}
	bananaData = [][]string{
		{"bool", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "str", "ts_s", "d32", "d64", "dec128", "dec256", "list(i64)", "binary"},
		{"BANANA", "-1", "-1", "-1", "-1", "0", "0", "0", "0", "0", "0", "str-0", "2014-07-28 15:04:05", "2017-05-18", "2028-04-26", "-123.45", "-123.45", "{1,2,3}", "AAEC"},
		{"MANGO", "0", "0", "0", "0", "1", "1", "1", "1", "0.1", "0.1", "str-1", "2016-09-08 15:04:05", "2022-11-08", "2031-06-28", "0", "0", "{4,5,6}", "AwQF"},
		{"BANANA", "1", "1", "1", "1", "2", "2", "2", "2", "0.2", "0.2", "str-2", "2021-09-18 15:04:05", "2025-08-04", "2034-08-28", "123.45", "123.45", "{7,8,9}", ""},
		{nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal, nullVal},
	}
)

func TestCSVWriter(t *testing.T) {
	tests := []struct {
		name       string
		header     bool
		boolFormat func(bool) string
		data       [][]string
	}{
		{
			name:   "Noheader",
			header: false,
			data:   fullData[1:],
		},
		{
			name:   "header",
			header: true,
			data:   fullData,
		},
		{
			name:   "Header with bool fmt",
			header: true,
			boolFormat: func(b bool) string {
				if b {
					return "BANANA"
				}
				return "MANGO"
			},
			data: bananaData,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCSVWriter(t, test.data, test.header, test.boolFormat)
		})
	}
}

func genTimestamps(unit arrow.TimeUnit) []arrow.Timestamp {
	out := []arrow.Timestamp{}
	for _, input := range []string{"2014-07-28 15:04:05", "2016-09-08 15:04:05", "2021-09-18 15:04:05"} {
		ts, err := arrow.TimestampFromString(input, unit)
		if err != nil {
			panic(fmt.Errorf("could not convert %s to arrow.Timestamp err=%s", input, err))
		}
		out = append(out, ts)
	}
	return out
}

func testCSVWriter(t *testing.T, data [][]string, writeHeader bool, fmtr func(bool) string) {
	f := new(bytes.Buffer)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "ts_s", Type: arrow.FixedWidthTypes.Timestamp_s},
			{Name: "d32", Type: arrow.FixedWidthTypes.Date32},
			{Name: "d64", Type: arrow.FixedWidthTypes.Date64},
			{Name: "dec128", Type: &arrow.Decimal128Type{Precision: 5, Scale: 2}},
			{Name: "dec256", Type: &arrow.Decimal256Type{Precision: 5, Scale: 2}},
			{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "binary", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	b.Field(1).(*array.Int8Builder).AppendValues([]int8{-1, 0, 1}, nil)
	b.Field(2).(*array.Int16Builder).AppendValues([]int16{-1, 0, 1}, nil)
	b.Field(3).(*array.Int32Builder).AppendValues([]int32{-1, 0, 1}, nil)
	b.Field(4).(*array.Int64Builder).AppendValues([]int64{-1, 0, 1}, nil)
	b.Field(5).(*array.Uint8Builder).AppendValues([]uint8{0, 1, 2}, nil)
	b.Field(6).(*array.Uint16Builder).AppendValues([]uint16{0, 1, 2}, nil)
	b.Field(7).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
	b.Field(8).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2}, nil)
	b.Field(9).(*array.Float32Builder).AppendValues([]float32{0.0, 0.1, 0.2}, nil)
	b.Field(10).(*array.Float64Builder).AppendValues([]float64{0.0, 0.1, 0.2}, nil)
	b.Field(11).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2"}, nil)
	b.Field(12).(*array.TimestampBuilder).AppendValues(genTimestamps(arrow.Second), nil)
	b.Field(13).(*array.Date32Builder).AppendValues([]arrow.Date32{17304, 19304, 20304}, nil)
	b.Field(14).(*array.Date64Builder).AppendValues([]arrow.Date64{1840400000000, 1940400000000, 2040400000000}, nil)
	b.Field(15).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-12345), decimal128.FromI64(0), decimal128.FromI64(12345)}, nil)
	b.Field(16).(*array.Decimal256Builder).AppendValues([]decimal256.Num{decimal256.FromI64(-12345), decimal256.FromI64(0), decimal256.FromI64(12345)}, nil)
	listBuilder := b.Field(17).(*array.ListBuilder)
	listBuilderInt64 := listBuilder.ValueBuilder().(*array.Int64Builder)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{1, 2, 3}, nil)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{4, 5, 6}, nil)
	listBuilder.Append(true)
	listBuilderInt64.AppendValues([]int64{7, 8, 9}, nil)
	b.Field(18).(*array.BinaryBuilder).AppendValues([][]byte{{0, 1, 2}, {3, 4, 5}, {}}, nil)

	for _, field := range b.Fields() {
		field.AppendNull()
	}

	rec := b.NewRecord()
	defer rec.Release()

	w := csv.NewWriter(f, schema,
		csv.WithComma(separator),
		csv.WithCRLF(false),
		csv.WithHeader(writeHeader),
		csv.WithNullWriter(nullVal),
		csv.WithBoolWriter(fmtr),
	)
	err := w.Write(rec)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Flush()
	if err != nil {
		t.Fatal(err)
	}

	err = w.Error()
	if err != nil {
		t.Fatal(err)
	}

	bdata, err := expectedOutout(data)
	if err != nil {
		t.Fatal(err)
	}

	if err = matchCSV(bdata.Bytes(), f.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func expectedOutout(data [][]string) (*bytes.Buffer, error) {
	b := bytes.NewBuffer(nil)
	w := ecsv.NewWriter(b)
	w.Comma = separator
	w.UseCRLF = false
	return b, w.WriteAll(data)
}

func matchCSV(expected, test []byte) error {
	expectedScanner := bufio.NewScanner(bytes.NewReader(expected))
	testScanner := bufio.NewScanner(bytes.NewReader(test))
	line := 0
	for expectedScanner.Scan() && testScanner.Scan() {
		if expectedScanner.Text() != testScanner.Text() {
			return fmt.Errorf("expected=%s != test=%s line=%d", expectedScanner.Text(), testScanner.Text(), line)
		}
		line++
	}

	if expectedScanner.Scan() {
		return fmt.Errorf("expected unprocessed:%s", expectedScanner.Text())
	}

	if testScanner.Scan() {
		return fmt.Errorf("test unprocessed:%s", testScanner.Text())
	}

	if err := expectedScanner.Err(); err != nil {
		return err
	}

	return testScanner.Err()
}

func BenchmarkWrite(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "dec128", Type: &arrow.Decimal128Type{Precision: 4, Scale: 3}},
			{Name: "dec128", Type: &arrow.Decimal256Type{Precision: 4, Scale: 3}},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	const N = 1000
	for i := 0; i < N; i++ {
		bldr.Field(0).(*array.BooleanBuilder).Append(i%10 == 0)
		bldr.Field(1).(*array.Int8Builder).Append(int8(i))
		bldr.Field(2).(*array.Int16Builder).Append(int16(i))
		bldr.Field(3).(*array.Int32Builder).Append(int32(i))
		bldr.Field(4).(*array.Int64Builder).Append(int64(i))
		bldr.Field(5).(*array.Uint8Builder).Append(uint8(i))
		bldr.Field(6).(*array.Uint16Builder).Append(uint16(i))
		bldr.Field(7).(*array.Uint32Builder).Append(uint32(i))
		bldr.Field(8).(*array.Uint64Builder).Append(uint64(i))
		bldr.Field(9).(*array.Float32Builder).Append(float32(i))
		bldr.Field(10).(*array.Float64Builder).Append(float64(i))
		bldr.Field(11).(*array.StringBuilder).Append(fmt.Sprintf("str-%d", i))
		bldr.Field(12).(*array.Decimal128Builder).Append(decimal128.FromI64(int64(i)))
		bldr.Field(13).(*array.Decimal256Builder).Append(decimal256.FromI64(int64(i)))
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	w := csv.NewWriter(ioutil.Discard, schema, csv.WithComma(';'), csv.WithCRLF(false))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := w.Write(rec)
		if err != nil {
			b.Fatal(err)
		}
		err = w.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}

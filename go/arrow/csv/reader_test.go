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
	"bytes"
	stdcsv "encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/csv"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Example() {
	f := bytes.NewBufferString(`## a simple set of data: int64;float64;string
0;0;str-0
1;1;str-1
2;2;str-2
3;3;str-3
4;4;str-4
5;5;str-5
6;6;str-6
7;7;str-7
8;8;str-8
9;9;str-9
`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
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
	err := r.Err()
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

func Example_withChunk() {
	f := bytes.NewBufferString(`## a simple set of data: int64;float64;string
0;0;str-0
1;1;str-1
2;2;str-2
3;3;str-3
4;4;str-4
5;5;str-5
6;6;str-6
7;7;str-7
8;8;str-8
9;9;str-9
`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(
		f, schema,
		csv.WithComment('#'), csv.WithComma(';'),
		csv.WithChunk(3),
	)
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

	// Output:
	// rec[0]["i64"]: [0 1 2]
	// rec[0]["f64"]: [0 1 2]
	// rec[0]["str"]: ["str-0" "str-1" "str-2"]
	// rec[1]["i64"]: [3 4 5]
	// rec[1]["f64"]: [3 4 5]
	// rec[1]["str"]: ["str-3" "str-4" "str-5"]
	// rec[2]["i64"]: [6 7 8]
	// rec[2]["f64"]: [6 7 8]
	// rec[2]["str"]: ["str-6" "str-7" "str-8"]
	// rec[3]["i64"]: [9]
	// rec[3]["f64"]: [9]
	// rec[3]["str"]: ["str-9"]
}

func TestCSVReadInvalidFields(t *testing.T) {
	tests := []struct {
		Name          string
		Data          string
		Fields        []arrow.Field
		ExpectedError bool
	}{
		{
			Name: "ValidListInt64",
			Data: "{}",
			Fields: []arrow.Field{
				{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			},
			ExpectedError: false,
		},
		{
			Name: "InvalidListInt64T1",
			Data: "{",
			Fields: []arrow.Field{
				{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			},
			ExpectedError: true,
		},
		{
			Name: "InvalidListInt64T2",
			Data: "}",
			Fields: []arrow.Field{
				{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			},
			ExpectedError: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			f := bytes.NewBufferString(tc.Data)
			schema := arrow.NewSchema(tc.Fields, nil)

			r := csv.NewReader(
				f, schema,
				csv.WithComma(','),
			)
			defer r.Release()
			for r.Next() {
			}
			parseErr := r.Err()
			if tc.ExpectedError && parseErr == nil {
				t.Fatal("Expected error, but none found")
			}
			if !tc.ExpectedError && parseErr != nil {
				t.Fatalf("Not expecting error, but got %v", parseErr)
			}
		})
	}
}

func TestCSVReaderParseError(t *testing.T) {
	f := bytes.NewBufferString(`## a simple set of data: int64;float64;string
0;0;str-0
1;1;str-1
2;2;str-2
3;3;str-3
4;BADDATA;str-4
5;5;str-5
6;6;str-6
7;7;str-7
8;8;str-8
9;9;str-9
`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(
		f, schema,
		csv.WithComment('#'), csv.WithComma(';'),
		csv.WithChunk(3),
	)
	defer r.Release()

	n := 0
	lines := 0
	var rec arrow.Record
	for r.Next() {
		if rec != nil {
			rec.Release()
		}
		rec = r.Record()
		rec.Retain()

		if n == 1 && r.Err() == nil {
			t.Fatal("Expected error on second chunk, but none found")
		}

		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
			lines++
		}
		n++
	}

	if r.Err() == nil {
		t.Fatal("Expected any chunk with error to leave reader in an error state.")
	}

	if got, want := n, 2; got != want {
		t.Fatalf("invalid number of chunks: got=%d, want=%d", got, want)
	}

	if got, want := lines, 6; got != want {
		t.Fatalf("invalid number of lines: got=%d, want=%d", got, want)
	}

	if !rec.Columns()[1].IsNull(1) {
		t.Fatalf("expected bad data to be null, found: %v", rec.Columns()[1].Data())
	}
	rec.Release()
}

func TestCSVReader(t *testing.T) {
	tests := []struct {
		Name             string
		File             string
		Header           bool
		StringsCanBeNull bool
	}{
		{
			Name:   "NoHeader",
			File:   "testdata/types.csv",
			Header: false,
		}, {
			Name:   "Header",
			File:   "testdata/header.csv",
			Header: true,
		},
		{
			Name:             "NoHeader_StringsCanBeNull",
			File:             "testdata/types.csv",
			Header:           false,
			StringsCanBeNull: true,
		}, {
			Name:             "Header_StringsCanBeNull",
			File:             "testdata/header.csv",
			Header:           true,
			StringsCanBeNull: true,
		},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			testCSVReader(t, test.File, test.Header, test.StringsCanBeNull)
		})
	}
}

var defaultNullValues = []string{"", "NULL", "null", "N/A"}

func testCSVReader(t *testing.T, filepath string, withHeader bool, stringsCanBeNull bool) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	raw, err := os.ReadFile(filepath)
	if err != nil {
		t.Fatal(err)
	}

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
			{Name: "f16", Type: arrow.FixedWidthTypes.Float16},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "large_str", Type: arrow.BinaryTypes.LargeString},
			{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
			{Name: "list(i64)", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "large_list(i64)", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int64)},
			{Name: "fixed_size_list(i64)", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int64)},
			{Name: "binary", Type: arrow.BinaryTypes.Binary},
			{Name: "large_binary", Type: arrow.BinaryTypes.LargeBinary},
			{Name: "fixed_size_binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}},
			{Name: "uuid", Type: types.NewUUIDType()},
		},
		nil,
	)
	r := csv.NewReader(bytes.NewReader(raw), schema,
		csv.WithAllocator(mem),
		csv.WithComment('#'), csv.WithComma(';'),
		csv.WithHeader(withHeader),
		csv.WithNullReader(stringsCanBeNull, defaultNullValues...),
	)
	defer r.Release()

	r.Retain()
	r.Release()

	if got, want := r.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%v, want=%v", got, want)
	}

	out := new(bytes.Buffer)
	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Fprintf(out, "rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if got, want := n, 3; got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}

	str1Value := `""`
	str2Value := `"null"`
	if stringsCanBeNull {
		str1Value = array.NullValueStr
		str2Value = array.NullValueStr
	}

	want := fmt.Sprintf(`rec[0]["bool"]: [true]
rec[0]["i8"]: [-1]
rec[0]["i16"]: [-1]
rec[0]["i32"]: [-1]
rec[0]["i64"]: [-1]
rec[0]["u8"]: [1]
rec[0]["u16"]: [1]
rec[0]["u32"]: [1]
rec[0]["u64"]: [1]
rec[0]["f16"]: [1.0996094]
rec[0]["f32"]: [1.1]
rec[0]["f64"]: [1.1]
rec[0]["str"]: ["str-1"]
rec[0]["large_str"]: ["str-1"]
rec[0]["ts"]: [1652054461000]
rec[0]["list(i64)"]: [[1 2 3]]
rec[0]["large_list(i64)"]: [[1 2 3]]
rec[0]["fixed_size_list(i64)"]: [[1 2 3]]
rec[0]["binary"]: ["\x00\x01\x02"]
rec[0]["large_binary"]: ["\x00\x01\x02"]
rec[0]["fixed_size_binary"]: ["\x00\x01\x02"]
rec[0]["uuid"]: ["00000000-0000-0000-0000-000000000001"]
rec[1]["bool"]: [false]
rec[1]["i8"]: [-2]
rec[1]["i16"]: [-2]
rec[1]["i32"]: [-2]
rec[1]["i64"]: [-2]
rec[1]["u8"]: [2]
rec[1]["u16"]: [2]
rec[1]["u32"]: [2]
rec[1]["u64"]: [2]
rec[1]["f16"]: [2.1992188]
rec[1]["f32"]: [2.2]
rec[1]["f64"]: [2.2]
rec[1]["str"]: [%s]
rec[1]["large_str"]: [%s]
rec[1]["ts"]: [1652140799000]
rec[1]["list(i64)"]: [[]]
rec[1]["large_list(i64)"]: [[]]
rec[1]["fixed_size_list(i64)"]: [[4 5 6]]
rec[1]["binary"]: [(null)]
rec[1]["large_binary"]: [(null)]
rec[1]["fixed_size_binary"]: [(null)]
rec[1]["uuid"]: ["00000000-0000-0000-0000-000000000002"]
rec[2]["bool"]: [(null)]
rec[2]["i8"]: [(null)]
rec[2]["i16"]: [(null)]
rec[2]["i32"]: [(null)]
rec[2]["i64"]: [(null)]
rec[2]["u8"]: [(null)]
rec[2]["u16"]: [(null)]
rec[2]["u32"]: [(null)]
rec[2]["u64"]: [(null)]
rec[2]["f16"]: [(null)]
rec[2]["f32"]: [(null)]
rec[2]["f64"]: [(null)]
rec[2]["str"]: [%s]
rec[2]["large_str"]: [%s]
rec[2]["ts"]: [(null)]
rec[2]["list(i64)"]: [(null)]
rec[2]["large_list(i64)"]: [(null)]
rec[2]["fixed_size_list(i64)"]: [(null)]
rec[2]["binary"]: [(null)]
rec[2]["large_binary"]: [(null)]
rec[2]["fixed_size_binary"]: [(null)]
rec[2]["uuid"]: [(null)]
`, str1Value, str1Value, str2Value, str2Value)
	got, want := out.String(), want
	require.Equal(t, want, got)

	if r.Err() != nil {
		t.Fatalf("unexpected error: %v", r.Err())
	}

	// test error modes
	{
		r := csv.NewReader(bytes.NewReader(raw), schema,
			csv.WithAllocator(mem),
			csv.WithComment('#'), csv.WithComma(';'),
			csv.WithHeader(withHeader),
			csv.WithNullReader(stringsCanBeNull),
		)

		r.Next()
		r.Record()

		r.Release()
	}
}

func TestCSVReaderWithChunk(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	raw, err := os.ReadFile("testdata/simple.csv")
	if err != nil {
		t.Fatal(err)
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	for _, tc := range []struct {
		name    string
		opts    []csv.Option
		records int
		want    string
	}{
		{
			name:    "chunk=default",
			opts:    []csv.Option{csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';')},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[0]["f64"]: [0]
rec[0]["str"]: ["str-0"]
rec[1]["i64"]: [1]
rec[1]["f64"]: [1]
rec[1]["str"]: ["str-1"]
rec[2]["i64"]: [2]
rec[2]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[3]["i64"]: [3]
rec[3]["f64"]: [3]
rec[3]["str"]: ["str-3"]
rec[4]["i64"]: [4]
rec[4]["f64"]: [4]
rec[4]["str"]: ["str-4"]
rec[5]["i64"]: [5]
rec[5]["f64"]: [5]
rec[5]["str"]: ["str-5"]
rec[6]["i64"]: [6]
rec[6]["f64"]: [6]
rec[6]["str"]: ["str-6"]
rec[7]["i64"]: [7]
rec[7]["f64"]: [7]
rec[7]["str"]: ["str-7"]
rec[8]["i64"]: [8]
rec[8]["f64"]: [8]
rec[8]["str"]: ["str-8"]
rec[9]["i64"]: [9]
rec[9]["f64"]: [9]
rec[9]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=0",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(0),
			},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[0]["f64"]: [0]
rec[0]["str"]: ["str-0"]
rec[1]["i64"]: [1]
rec[1]["f64"]: [1]
rec[1]["str"]: ["str-1"]
rec[2]["i64"]: [2]
rec[2]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[3]["i64"]: [3]
rec[3]["f64"]: [3]
rec[3]["str"]: ["str-3"]
rec[4]["i64"]: [4]
rec[4]["f64"]: [4]
rec[4]["str"]: ["str-4"]
rec[5]["i64"]: [5]
rec[5]["f64"]: [5]
rec[5]["str"]: ["str-5"]
rec[6]["i64"]: [6]
rec[6]["f64"]: [6]
rec[6]["str"]: ["str-6"]
rec[7]["i64"]: [7]
rec[7]["f64"]: [7]
rec[7]["str"]: ["str-7"]
rec[8]["i64"]: [8]
rec[8]["f64"]: [8]
rec[8]["str"]: ["str-8"]
rec[9]["i64"]: [9]
rec[9]["f64"]: [9]
rec[9]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=1",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(1),
			},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[0]["f64"]: [0]
rec[0]["str"]: ["str-0"]
rec[1]["i64"]: [1]
rec[1]["f64"]: [1]
rec[1]["str"]: ["str-1"]
rec[2]["i64"]: [2]
rec[2]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[3]["i64"]: [3]
rec[3]["f64"]: [3]
rec[3]["str"]: ["str-3"]
rec[4]["i64"]: [4]
rec[4]["f64"]: [4]
rec[4]["str"]: ["str-4"]
rec[5]["i64"]: [5]
rec[5]["f64"]: [5]
rec[5]["str"]: ["str-5"]
rec[6]["i64"]: [6]
rec[6]["f64"]: [6]
rec[6]["str"]: ["str-6"]
rec[7]["i64"]: [7]
rec[7]["f64"]: [7]
rec[7]["str"]: ["str-7"]
rec[8]["i64"]: [8]
rec[8]["f64"]: [8]
rec[8]["str"]: ["str-8"]
rec[9]["i64"]: [9]
rec[9]["f64"]: [9]
rec[9]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=3",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(3),
			},
			records: 4,
			want: `rec[0]["i64"]: [0 1 2]
rec[0]["f64"]: [0 1 2]
rec[0]["str"]: ["str-0" "str-1" "str-2"]
rec[1]["i64"]: [3 4 5]
rec[1]["f64"]: [3 4 5]
rec[1]["str"]: ["str-3" "str-4" "str-5"]
rec[2]["i64"]: [6 7 8]
rec[2]["f64"]: [6 7 8]
rec[2]["str"]: ["str-6" "str-7" "str-8"]
rec[3]["i64"]: [9]
rec[3]["f64"]: [9]
rec[3]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=6",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(6),
			},
			records: 2,
			want: `rec[0]["i64"]: [0 1 2 3 4 5]
rec[0]["f64"]: [0 1 2 3 4 5]
rec[0]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5"]
rec[1]["i64"]: [6 7 8 9]
rec[1]["f64"]: [6 7 8 9]
rec[1]["str"]: ["str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=10",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(10),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=11",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(11),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=-1",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(-1),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[0]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := csv.NewReader(bytes.NewReader(raw), schema, tc.opts...)

			defer r.Release()

			r.Retain()
			r.Release()

			if got, want := r.Schema(), schema; !got.Equal(want) {
				t.Fatalf("invalid schema: got=%v, want=%v", got, want)
			}

			out := new(bytes.Buffer)

			n := 0
			for r.Next() {
				rec := r.Record()
				for i, col := range rec.Columns() {
					fmt.Fprintf(out, "rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
				}
				n++
			}

			if got, want := n, tc.records; got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}

			if got, want := out.String(), tc.want; got != want {
				t.Fatalf("invalid output:\ngot:\n%s\nwant:\n%s\n", got, want)
			}

			if r.Err() != nil {
				t.Fatalf("unexpected error: %v", r.Err())
			}
		})
	}
}

func TestReadCSVDecimalCols(t *testing.T) {
	data := `dec128,dec256
12.3,0.00123
1.23e-8,-1.23e-3
-1.23E+3,1.23e+5
`

	r := csv.NewReader(strings.NewReader(data), arrow.NewSchema([]arrow.Field{
		{Name: "dec128", Type: &arrow.Decimal128Type{Precision: 14, Scale: 10}, Nullable: true},
		{Name: "dec256", Type: &arrow.Decimal256Type{Precision: 11, Scale: 5}, Nullable: true},
	}, nil), csv.WithChunk(-1), csv.WithHeader(true), csv.WithComma(','), csv.WithNullReader(true, "null", "#NA"))
	defer r.Release()

	assert.True(t, r.Next())
	rec := r.Record()
	rec.Retain()
	assert.False(t, r.Next())
	defer rec.Release()

	if r.Err() != nil {
		log.Fatal(r.Err())
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, r.Schema())
	defer bldr.Release()

	dec128Bldr := bldr.Field(0).(*array.Decimal128Builder)
	dec128Bldr.Append(decimal128.New(0, 123000000000))
	dec128Bldr.Append(decimal128.New(0, 123))
	dec128Bldr.Append(decimal128.FromI64(-12300000000000))

	dec256Bldr := bldr.Field(1).(*array.Decimal256Builder)
	dec256Bldr.Append(decimal256.FromU64(123))
	dec256Bldr.Append(decimal256.FromI64(-123))
	dec256Bldr.Append(decimal256.FromU64(12300000000))

	exRec := bldr.NewRecord()
	defer exRec.Release()

	assert.Truef(t, array.RecordEqual(exRec, rec), "expected: %s\nactual: %s", exRec, rec)
}

func BenchmarkRead(b *testing.B) {
	gen := func(rows, cols int) []byte {
		buf := new(bytes.Buffer)
		for i := 0; i < rows; i++ {
			for j := 0; j < cols; j++ {
				if j > 0 {
					fmt.Fprintf(buf, ";")
				}
				fmt.Fprintf(buf, "%d;%f;str-%d", i, float64(i), i)
			}
			fmt.Fprintf(buf, "\n")
		}
		return buf.Bytes()
	}

	for _, rows := range []int{10, 1e2, 1e3, 1e4} {
		for _, cols := range []int{1, 10, 100, 1000} {
			raw := gen(rows, cols)
			for _, chunks := range []int{-1, 0, 10, 100, 1000} {
				b.Run(fmt.Sprintf("rows=%d cols=%d chunks=%d", rows, cols, chunks), func(b *testing.B) {
					benchRead(b, raw, rows, cols, chunks)
				})
			}
		}
	}
}

func benchRead(b *testing.B, raw []byte, rows, cols, chunks int) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(b, 0)

	var fields []arrow.Field
	for i := 0; i < cols; i++ {
		fields = append(fields, []arrow.Field{
			{Name: fmt.Sprintf("i64-%d", i), Type: arrow.PrimitiveTypes.Int64},
			{Name: fmt.Sprintf("f64-%d", i), Type: arrow.PrimitiveTypes.Float64},
			{Name: fmt.Sprintf("str-%d", i), Type: arrow.BinaryTypes.String},
		}...)
	}

	schema := arrow.NewSchema(fields, nil)
	chunk := 0
	if chunks != 0 {
		chunk = rows / chunks
	}
	opts := []csv.Option{
		csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
		csv.WithChunk(chunk),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := csv.NewReader(bytes.NewReader(raw), schema, opts...)

		n := int64(0)
		for r.Next() {
			n += r.Record().NumRows()
		}

		r.Release()
		if n != int64(rows) {
			b.Fatalf("invalid number of rows. want=%d, got=%d", n, rows)
		}
	}
}

func TestInferringSchema(t *testing.T) {
	var b bytes.Buffer
	wr := stdcsv.NewWriter(&b)
	wr.WriteAll([][]string{
		{"i64", "f64", "str", "ts", "bool"},
		{"123", "1.23", "foobar", "2022-05-09T00:01:01", "false"},
		{"456", "45.6", "baz", "2022-05-09T23:59:59", "true"},
		{"null", "NULL", "null", "N/A", "null"},
		{"-78", "-1.25", "", "2021-01-01T10:11:12", "TRUE"},
	})
	wr.Flush()

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	r := csv.NewInferringReader(&b, csv.WithAllocator(mem), csv.WithHeader(true), csv.WithNullReader(true, defaultNullValues...))
	defer r.Release()

	assert.Nil(t, r.Schema())
	assert.True(t, r.Next())
	assert.NoError(t, r.Err())

	expSchema := arrow.NewSchema([]arrow.Field{
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Second}, Nullable: true},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	exp, _, _ := array.RecordFromJSON(mem, expSchema, strings.NewReader(`[
		{"i64": 123, "f64": 1.23, "str": "foobar", "ts": "2022-05-09T00:01:01", "bool": false},
		{"i64": 456, "f64": 45.6, "str": "baz", "ts": "2022-05-09T23:59:59", "bool": true},
		{"i64": null, "f64": null, "str": null, "ts": null, "bool": null},
		{"i64": -78, "f64": -1.25, "str": null, "ts": "2021-01-01T10:11:12", "bool": true}
	]`))
	defer exp.Release()

	assertRowEqual := func(expected, actual arrow.Record, row int) {
		ex := expected.NewSlice(int64(row), int64(row+1))
		defer ex.Release()
		assert.Truef(t, array.RecordEqual(ex, actual), "expected: %s\ngot: %s", ex, actual)
	}

	assert.True(t, expSchema.Equal(r.Schema()), expSchema.String(), r.Schema().String())
	// verify first row:
	assertRowEqual(exp, r.Record(), 0)
	assert.True(t, r.Next())
	assertRowEqual(exp, r.Record(), 1)
	assert.True(t, r.Next())
	assertRowEqual(exp, r.Record(), 2)
	assert.True(t, r.Next())
	assertRowEqual(exp, r.Record(), 3)
	assert.False(t, r.Next())
}

func TestInferCSVOptions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	f, err := os.Open("testdata/header.csv")
	require.NoError(t, err)
	defer f.Close()

	r := csv.NewInferringReader(f, csv.WithAllocator(mem),
		csv.WithComma(';'), csv.WithComment('#'), csv.WithHeader(true),
		csv.WithNullReader(true, defaultNullValues...),
		csv.WithIncludeColumns([]string{"f64", "i32", "bool", "str", "i64", "u64", "i8"}),
		csv.WithColumnTypes(map[string]arrow.DataType{
			"i32": arrow.PrimitiveTypes.Int32,
			"i8":  arrow.PrimitiveTypes.Int8,
			"i16": arrow.PrimitiveTypes.Int16,
			"u64": arrow.PrimitiveTypes.Uint64,
		}), csv.WithChunk(-1))
	defer r.Release()

	assert.True(t, r.Next())
	rec := r.Record()
	rec.Retain()
	defer rec.Release()
	assert.False(t, r.Next())

	expSchema := arrow.NewSchema([]arrow.Field{
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
	}, nil)
	expRec, _, _ := array.RecordFromJSON(mem, expSchema, strings.NewReader(`[
		{"f64": 1.1, "i32": -1, "bool": true, "str": "str-1", "i64": -1, "u64": 1, "i8": -1},
		{"f64": 2.2, "i32": -2, "bool": false, "str": null, "i64": -2, "u64": 2, "i8": -2},
		{"f64": null, "i32": null, "bool": null, "str": null, "i64": null, "u64": null, "i8": null}
	]`))
	defer expRec.Release()

	assert.True(t, expSchema.Equal(r.Schema()), expSchema.String(), r.Schema().String())
	assert.Truef(t, array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
}

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
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestCSVWriter(t *testing.T) {
	f := new(bytes.Buffer)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
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

	w := csv.NewWriter(f, schema, csv.WithFieldSeparator(';'))
	err := w.Write(rec)
	if err != nil {
		t.Fatal(err)
	}

	want := `0;0;str-0
1;1;str-1
2;2;str-2
3;3;str-3
4;4;str-4
5;5;str-5
6;6;str-6
7;7;str-7
8;8;str-8
9;9;str-9
`

	if got, want := f.String(), want; strings.Compare(got, want) != 0 {
		t.Fatalf("invalid output:\ngot=%s\nwant=%s\n\n", got, want)
	}
}
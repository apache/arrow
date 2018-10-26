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

package array_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestRecord(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	col1 := func() array.Interface {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt32Array()
	}()
	defer col1.Release()

	col2 := func() array.Interface {
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return b.NewFloat64Array()
	}()
	defer col2.Release()

	cols := []array.Interface{col1, col2}
	rec := array.NewRecord(schema, cols, -1)
	defer rec.Release()

	rec.Retain()
	rec.Release()

	if got, want := rec.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
	}

	if got, want := rec.NumRows(), int64(10); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := rec.NumCols(), int64(2); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := rec.Column(0), cols[0]; got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}
	if got, want := rec.ColumnName(0), schema.Field(0).Name; got != want {
		t.Fatalf("invalid column name: got=%q, want=%q", got, want)
	}

	for _, tc := range []struct {
		i, j int64
		err  error
	}{
		{i: 0, j: 10, err: nil},
		{i: 1, j: 10, err: nil},
		{i: 1, j: 9, err: nil},
		{i: 0, j: 0, err: nil},
		{i: 1, j: 1, err: nil},
		{i: 10, j: 10, err: nil},
		{i: 1, j: 0, err: fmt.Errorf("arrow/array: index out of range")},
		{i: 1, j: 11, err: fmt.Errorf("arrow/array: index out of range")},
	} {
		t.Run(fmt.Sprintf("slice-%02d-%02d", tc.i, tc.j), func(t *testing.T) {
			if tc.err != nil {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected an error %q", tc.err)
					}
					switch err := e.(type) {
					case string:
						if err != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					case error:
						if err.Error() != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					default:
						t.Fatalf("invalid type for panic message: %T (err=%v)", err, err)
					}
				}()
			}
			sub := rec.NewSlice(tc.i, tc.j)
			defer sub.Release()

			if got, want := sub.NumRows(), tc.j-tc.i; got != want {
				t.Fatalf("invalid rec-slice number of rows: got=%d, want=%d", got, want)
			}
		})
	}

	for _, tc := range []struct {
		schema *arrow.Schema
		cols   []array.Interface
		rows   int64
		err    error
	}{
		{
			schema: schema,
			cols:   nil,
			rows:   -1,
			err:    fmt.Errorf("arrow/array: number of columns/fields mismatch"),
		},
		{
			schema: schema,
			cols:   cols[:1],
			rows:   0,
			err:    fmt.Errorf("arrow/array: number of columns/fields mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf("arrow/array: number of columns/fields mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf(`arrow/array: column "f2-f64" type mismatch: got=float64, want=int32`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   11,
			err:    fmt.Errorf(`arrow/array: mismatch number of rows in column "f1-i32": got=10, want=11`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   10,
			err:    nil,
		},
		{
			schema: schema,
			cols:   cols,
			rows:   3,
			err:    nil,
		},
		{
			schema: schema,
			cols:   cols,
			rows:   0,
			err:    nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			if tc.err != nil {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected an error %q", tc.err)
					}
					switch err := e.(type) {
					case string:
						if err != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					case error:
						if err.Error() != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					default:
						t.Fatalf("invalid type for panic message: %T (err=%v)", err, err)
					}
				}()
			}
			rec := array.NewRecord(tc.schema, tc.cols, tc.rows)
			defer rec.Release()
			if got, want := rec.NumRows(), tc.rows; got != want {
				t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestRecordReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	rec1 := func() array.Record {
		col1 := func() array.Interface {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
			return ib.NewInt32Array()
		}()
		defer col1.Release()

		col2 := func() array.Interface {
			b := array.NewFloat64Builder(mem)
			defer b.Release()

			b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
			return b.NewFloat64Array()
		}()
		defer col2.Release()

		cols := []array.Interface{col1, col2}
		return array.NewRecord(schema, cols, -1)
	}()
	defer rec1.Release()

	rec2 := func() array.Record {
		col1 := func() array.Interface {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
			return ib.NewInt32Array()
		}()
		defer col1.Release()

		col2 := func() array.Interface {
			b := array.NewFloat64Builder(mem)
			defer b.Release()

			b.AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
			return b.NewFloat64Array()
		}()
		defer col2.Release()

		cols := []array.Interface{col1, col2}
		return array.NewRecord(schema, cols, -1)
	}()
	defer rec2.Release()

	recs := []array.Record{rec1, rec2}
	itr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Release()

	itr.Retain()
	itr.Release()

	if got, want := itr.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema. got=%#v, want=%#v", got, want)
	}

	n := 0
	for itr.Next() {
		n++
		if got, want := itr.Record(), recs[n-1]; !reflect.DeepEqual(got, want) {
			t.Fatalf("itr[%d], invalid record. got=%#v, want=%#v", n-1, got, want)
		}
	}

	if n != len(recs) {
		t.Fatalf("invalid number of iterations. got=%d, want=%d", n, len(recs))
	}

	for _, tc := range []struct {
		name   string
		schema *arrow.Schema
		err    error
	}{
		{
			name: "mismatch-name",
			schema: arrow.NewSchema(
				[]arrow.Field{
					arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					arrow.Field{Name: "f2-XXX", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			),
			err: fmt.Errorf("arrow/array: mismatch schema"),
		},
		{
			name: "mismatch-type",
			schema: arrow.NewSchema(
				[]arrow.Field{
					arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					arrow.Field{Name: "f2-f64", Type: arrow.PrimitiveTypes.Int64},
				},
				nil,
			),
			err: fmt.Errorf("arrow/array: mismatch schema"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			itr, err := array.NewRecordReader(tc.schema, recs)
			if itr != nil {
				itr.Release()
			}
			if err == nil {
				t.Fatalf("expected an error: %v", tc.err)
			}
			if !reflect.DeepEqual(tc.err, err) {
				t.Fatalf("invalid error: got=%v, want=%v", err, tc.err)
			}
		})
	}
}

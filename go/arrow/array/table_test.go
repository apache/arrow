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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestChunked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	c1 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, nil)
	c1.Retain()
	c1.Release()
	if got, want := c1.Len(), 0; got != want {
		t.Fatalf("len differ. got=%d, want=%d", got, want)
	}
	if got, want := c1.NullN(), 0; got != want {
		t.Fatalf("nulls: got=%d, want=%d", got, want)
	}
	if got, want := c1.DataType(), arrow.PrimitiveTypes.Int32; got != want {
		t.Fatalf("dtype: got=%v, want=%v", got, want)
	}
	c1.Release()

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	fb.AppendValues([]float64{6, 7}, nil)
	f2 := fb.NewFloat64Array()
	defer f2.Release()

	fb.AppendValues([]float64{8, 9, 10}, nil)
	f3 := fb.NewFloat64Array()
	defer f3.Release()

	c2 := arrow.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]arrow.Array{f1, f2, f3},
	)
	defer c2.Release()

	if got, want := c2.Len(), 10; got != want {
		t.Fatalf("len: got=%d, want=%d", got, want)
	}
	if got, want := c2.NullN(), 0; got != want {
		t.Fatalf("nulls: got=%d, want=%d", got, want)
	}
	if got, want := c2.DataType(), arrow.PrimitiveTypes.Float64; got != want {
		t.Fatalf("dtype: got=%v, want=%v", got, want)
	}
	if got, want := c2.Chunk(0), c2.Chunks()[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("chunk: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i, j   int64
		len    int
		nulls  int
		chunks int
	}{
		{i: 0, j: 10, len: 10, nulls: 0, chunks: 3},
		{i: 2, j: 3, len: 1, nulls: 0, chunks: 1},
		{i: 9, j: 10, len: 1, nulls: 0, chunks: 1},
		{i: 0, j: 5, len: 5, nulls: 0, chunks: 1},
		{i: 5, j: 7, len: 2, nulls: 0, chunks: 1},
		{i: 7, j: 10, len: 3, nulls: 0, chunks: 1},
		{i: 10, j: 10, len: 0, nulls: 0, chunks: 0},
	} {
		t.Run("", func(t *testing.T) {
			sub := array.NewChunkedSlice(c2, tc.i, tc.j)
			defer sub.Release()

			if got, want := sub.Len(), tc.len; got != want {
				t.Fatalf("len: got=%d, want=%d", got, want)
			}
			if got, want := sub.NullN(), tc.nulls; got != want {
				t.Fatalf("nulls: got=%d, want=%d", got, want)
			}
			if got, want := sub.DataType(), arrow.PrimitiveTypes.Float64; got != want {
				t.Fatalf("dtype: got=%v, want=%v", got, want)
			}
			if got, want := len(sub.Chunks()), tc.chunks; got != want {
				t.Fatalf("chunks: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestChunkedEqualDataType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	lb1 := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer lb1.Release()

	v1 := lb1.NewArray()
	defer v1.Release()

	lb2 := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer lb2.Release()

	v2 := lb2.NewArray()
	defer v2.Release()

	c1 := arrow.NewChunked(arrow.ListOf(arrow.PrimitiveTypes.Int32), []arrow.Array{
		v1, v2,
	})
	defer c1.Release()
}

func TestChunkedInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	ib := array.NewInt32Builder(mem)
	defer ib.Release()

	ib.AppendValues([]int32{6, 7}, nil)
	f2 := ib.NewInt32Array()
	defer f2.Release()

	defer func() {
		e := recover()
		if e == nil {
			t.Fatalf("expected a panic")
		}
		if got, want := e.(string), "arrow/array: mismatch data type"; got != want {
			t.Fatalf("invalid error. got=%q, want=%q", got, want)
		}
	}()

	c1 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{
		f1, f2,
	})
	defer c1.Release()
}

func TestChunkedSliceInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	fb.AppendValues([]float64{6, 7}, nil)
	f2 := fb.NewFloat64Array()
	defer f2.Release()

	fb.AppendValues([]float64{8, 9, 10}, nil)
	f3 := fb.NewFloat64Array()
	defer f3.Release()

	c := arrow.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]arrow.Array{f1, f2, f3},
	)
	defer c.Release()

	for _, tc := range []struct {
		i, j int64
	}{
		{i: 2, j: 1},
		{i: 10, j: 11},
		{i: 11, j: 11},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("expected a panic")
				}
				if got, want := e.(string), "arrow/array: index out of range"; got != want {
					t.Fatalf("invalid error. got=%q, want=%q", got, want)
				}
			}()
			sub := array.NewChunkedSlice(c, tc.i, tc.j)
			defer sub.Release()
		})
	}
}

func TestColumn(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	type slice struct {
		i, j   int64
		len    int
		nulls  int
		chunks int
	}

	for _, tc := range []struct {
		chunk  *arrow.Chunked
		field  arrow.Field
		err    error
		slices []slice
	}{
		{
			chunk: func() *arrow.Chunked {
				ib := array.NewInt32Builder(mem)
				defer ib.Release()

				ib.AppendValues([]int32{1, 2, 3}, nil)
				i1 := ib.NewInt32Array()
				defer i1.Release()

				ib.AppendValues([]int32{4, 5, 6, 7, 8, 9, 10}, nil)
				i2 := ib.NewInt32Array()
				defer i2.Release()

				c := arrow.NewChunked(
					arrow.PrimitiveTypes.Int32,
					[]arrow.Array{i1, i2},
				)
				return c
			}(),
			field: arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			slices: []slice{
				{i: 0, j: 10, len: 10, nulls: 0, chunks: 2},
				{i: 2, j: 3, len: 1, nulls: 0, chunks: 1},
				{i: 9, j: 10, len: 1, nulls: 0, chunks: 1},
				{i: 0, j: 5, len: 5, nulls: 0, chunks: 2},
				{i: 5, j: 7, len: 2, nulls: 0, chunks: 1},
				{i: 7, j: 10, len: 3, nulls: 0, chunks: 1},
				{i: 10, j: 10, len: 0, nulls: 0, chunks: 0},
			},
		},
		{
			chunk: func() *arrow.Chunked {
				fb := array.NewFloat64Builder(mem)
				defer fb.Release()

				fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
				f1 := fb.NewFloat64Array()
				defer f1.Release()

				fb.AppendValues([]float64{6, 7}, nil)
				f2 := fb.NewFloat64Array()
				defer f2.Release()

				fb.AppendValues([]float64{8, 9, 10}, nil)
				f3 := fb.NewFloat64Array()
				defer f3.Release()

				c := arrow.NewChunked(
					arrow.PrimitiveTypes.Float64,
					[]arrow.Array{f1, f2, f3},
				)
				return c
			}(),
			field: arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			slices: []slice{
				{i: 0, j: 10, len: 10, nulls: 0, chunks: 3},
				{i: 2, j: 3, len: 1, nulls: 0, chunks: 1},
				{i: 9, j: 10, len: 1, nulls: 0, chunks: 1},
				{i: 0, j: 5, len: 5, nulls: 0, chunks: 1},
				{i: 5, j: 7, len: 2, nulls: 0, chunks: 1},
				{i: 7, j: 10, len: 3, nulls: 0, chunks: 1},
				{i: 10, j: 10, len: 0, nulls: 0, chunks: 0},
			},
		},
		{
			chunk: func() *arrow.Chunked {
				fb := array.NewFloat64Builder(mem)
				defer fb.Release()

				fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
				f1 := fb.NewFloat64Array()
				defer f1.Release()

				c := arrow.NewChunked(
					arrow.PrimitiveTypes.Float64,
					[]arrow.Array{f1},
				)
				return c
			}(),
			field: arrow.Field{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			err:   fmt.Errorf("arrow/array: inconsistent data type"),
		},
	} {
		t.Run("", func(t *testing.T) {
			defer tc.chunk.Release()

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

			col := arrow.NewColumn(tc.field, tc.chunk)
			defer col.Release()

			if got, want := col.Len(), tc.chunk.Len(); got != want {
				t.Fatalf("invalid length: got=%d, want=%d", got, want)
			}
			if got, want := col.NullN(), tc.chunk.NullN(); got != want {
				t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
			}
			if got, want := col.Data(), tc.chunk; got != want {
				t.Fatalf("invalid chunked: got=%#v, want=%#v", got, want)
			}
			if got, want := col.Field(), tc.field; !got.Equal(want) {
				t.Fatalf("invalid field: got=%#v, want=%#v", got, want)
			}
			if got, want := col.Name(), tc.field.Name; got != want {
				t.Fatalf("invalid name: got=%q, want=%q", got, want)
			}
			if got, want := col.DataType(), tc.field.Type; !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid data type: got=%#v, want=%#v", got, want)
			}

			col.Retain()
			col.Release()

			for _, slice := range tc.slices {
				t.Run("", func(t *testing.T) {
					sub := array.NewColumnSlice(col, slice.i, slice.j)
					defer sub.Release()

					if got, want := sub.Len(), slice.len; got != want {
						t.Fatalf("len: got=%d, want=%d", got, want)
					}
					if got, want := sub.NullN(), slice.nulls; got != want {
						t.Fatalf("nulls: got=%d, want=%d", got, want)
					}
					if got, want := sub.DataType(), col.DataType(); got != want {
						t.Fatalf("dtype: got=%v, want=%v", got, want)
					}
					if got, want := len(sub.Data().Chunks()), slice.chunks; got != want {
						t.Fatalf("chunks: got=%d, want=%d", got, want)
					}
				})
			}
		})
	}

}

func TestTable(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	col1 := func() *arrow.Column {
		chunk := func() *arrow.Chunked {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{1, 2, 3}, nil)
			i1 := ib.NewInt32Array()
			defer i1.Release()

			ib.AppendValues([]int32{4, 5, 6, 7, 8, 9, 10}, nil)
			i2 := ib.NewInt32Array()
			defer i2.Release()

			c := arrow.NewChunked(
				arrow.PrimitiveTypes.Int32,
				[]arrow.Array{i1, i2},
			)
			return c
		}()
		defer chunk.Release()

		return arrow.NewColumn(schema.Field(0), chunk)
	}()
	defer col1.Release()

	col2 := func() *arrow.Column {
		chunk := func() *arrow.Chunked {
			fb := array.NewFloat64Builder(mem)
			defer fb.Release()

			fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
			f1 := fb.NewFloat64Array()
			defer f1.Release()

			fb.AppendValues([]float64{6, 7}, nil)
			f2 := fb.NewFloat64Array()
			defer f2.Release()

			fb.AppendValues([]float64{8, 9, 10}, nil)
			f3 := fb.NewFloat64Array()
			defer f3.Release()

			c := arrow.NewChunked(
				arrow.PrimitiveTypes.Float64,
				[]arrow.Array{f1, f2, f3},
			)
			return c
		}()
		defer chunk.Release()

		return arrow.NewColumn(schema.Field(1), chunk)
	}()
	defer col2.Release()

	cols := []arrow.Column{*col1, *col2}

	slices := [][]arrow.Array{col1.Data().Chunks(), col2.Data().Chunks()}

	tbl := array.NewTable(schema, cols, -1)
	defer tbl.Release()

	tbl2 := array.NewTableFromSlice(schema, slices)
	defer tbl2.Release()

	tbl.Retain()
	tbl.Release()

	if got, want := tbl.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
	}

	if got, want := tbl.NumRows(), int64(10); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := tbl.NumCols(), int64(2); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := tbl.Column(0).Name(), col1.Name(); got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}

	if got, want := tbl2.NumRows(), int64(10); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := tbl2.NumCols(), int64(2); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := tbl2.Column(0).Name(), col1.Name(); got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}

	for _, tc := range []struct {
		schema *arrow.Schema
		cols   []arrow.Column
		rows   int64
		err    error
	}{
		{
			schema: schema,
			cols:   nil,
			rows:   -1,
			err:    fmt.Errorf("arrow/array: table schema mismatch"),
		},
		{
			schema: schema,
			cols:   cols[:1],
			rows:   0,
			err:    fmt.Errorf("arrow/array: table schema mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf("arrow/array: table schema mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					{Name: "f2-f64", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf(`arrow/array: column field "f2-f64" is inconsistent with schema`),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					{Name: "f2-f32", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf(`arrow/array: column field "f2-f64" is inconsistent with schema`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   11,
			err:    fmt.Errorf(`arrow/array: column "f1-i32" expected length >= 11 but got length 10`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   3,
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
			tbl := array.NewTable(tc.schema, tc.cols, tc.rows)
			defer tbl.Release()
			if got, want := tbl.NumRows(), tc.rows; got != want {
				t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestTableFromRecords(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	rec2 := b.NewRecord()
	defer rec2.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec1, rec2})
	defer tbl.Release()

	if got, want := tbl.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
	}

	if got, want := tbl.NumRows(), int64(20); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := tbl.NumCols(), int64(2); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := tbl.Column(0).Name(), schema.Field(0).Name; got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}
}

func TestTableReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	col1 := func() *arrow.Column {
		chunk := func() *arrow.Chunked {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{1, 2, 3}, nil)
			i1 := ib.NewInt32Array()
			defer i1.Release()

			ib.AppendValues([]int32{4, 5, 6, 7, 8, 9, 10}, nil)
			i2 := ib.NewInt32Array()
			defer i2.Release()

			c := arrow.NewChunked(
				arrow.PrimitiveTypes.Int32,
				[]arrow.Array{i1, i2},
			)
			return c
		}()
		defer chunk.Release()

		return arrow.NewColumn(schema.Field(0), chunk)
	}()
	defer col1.Release()

	col2 := func() *arrow.Column {
		chunk := func() *arrow.Chunked {
			fb := array.NewFloat64Builder(mem)
			defer fb.Release()

			fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
			f1 := fb.NewFloat64Array()
			defer f1.Release()

			fb.AppendValues([]float64{6, 7}, nil)
			f2 := fb.NewFloat64Array()
			defer f2.Release()

			fb.AppendValues([]float64{8, 9, 10}, nil)
			f3 := fb.NewFloat64Array()
			defer f3.Release()

			c := arrow.NewChunked(
				arrow.PrimitiveTypes.Float64,
				[]arrow.Array{f1, f2, f3},
			)
			return c
		}()
		defer chunk.Release()

		return arrow.NewColumn(schema.Field(1), chunk)
	}()
	defer col2.Release()

	cols := []arrow.Column{*col1, *col2}
	tbl := array.NewTable(schema, cols, -1)
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 1)
	defer tr.Release()

	tr.Retain()
	tr.Release()

	for tr.Next() {
	}
	if err := tr.Err(); err != nil {
		t.Fatalf("tr err: %#v", err)
	}

	for _, tc := range []struct {
		sz   int64
		n    int64
		rows []int64
	}{
		{sz: -1, n: 4, rows: []int64{3, 2, 2, 3}},
		{sz: +0, n: 4, rows: []int64{3, 2, 2, 3}},
		{sz: +1, n: 10, rows: []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{sz: +2, n: 6, rows: []int64{2, 1, 2, 2, 2, 1}},
	} {
		t.Run(fmt.Sprintf("chunksz=%d", tc.sz), func(t *testing.T) {
			tr := array.NewTableReader(tbl, tc.sz)
			defer tr.Release()

			if got, want := tr.Schema(), tbl.Schema(); !got.Equal(want) {
				t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
			}

			var (
				n   int64
				sum int64
			)
			for tr.Next() {
				rec := tr.Record()
				if got, want := rec.Schema(), tbl.Schema(); !got.Equal(want) {
					t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
				}
				if got, want := rec.NumRows(), tc.rows[n]; got != want {
					t.Fatalf("invalid number of rows[%d]: got=%d, want=%d", n, got, want)
				}
				n++
				sum += rec.NumRows()
			}
			if err := tr.Err(); err != nil {
				t.Fatalf("tr err: %#v", err)
			}

			if got, want := n, tc.n; got != want {
				t.Fatalf("invalid number of iterations: got=%d, want=%d", got, want)
			}
			if sum != tbl.NumRows() {
				t.Fatalf("invalid number of rows iterated over: got=%d, want=%d", sum, tbl.NumRows())
			}
		})
	}
}

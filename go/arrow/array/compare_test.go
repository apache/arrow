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
	"math"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/float16"
	"github.com/apache/arrow/go/v12/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrayEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			rec := recs[0]
			schema := rec.Schema()
			for i, col := range rec.Columns() {
				t.Run(schema.Field(i).Name, func(t *testing.T) {
					arr := col
					if !array.ArrayEqual(arr, arr) {
						t.Fatalf("identical arrays should compare equal:\narray=%v", arr)
					}
					sub1 := array.NewSlice(arr, 1, int64(arr.Len()))
					defer sub1.Release()

					sub2 := array.NewSlice(arr, 0, int64(arr.Len()-1))
					defer sub2.Release()

					if array.ArrayEqual(sub1, sub2) && name != "nulls" {
						t.Fatalf("non-identical arrays should not compare equal:\nsub1=%v\nsub2=%v\narrf=%v\n", sub1, sub2, arr)
					}
				})
			}
		})
	}
}

func TestArraySliceEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			rec := recs[0]
			schema := rec.Schema()
			for i, col := range rec.Columns() {
				t.Run(schema.Field(i).Name, func(t *testing.T) {
					arr := col
					if !array.SliceEqual(
						arr, 0, int64(arr.Len()),
						arr, 0, int64(arr.Len()),
					) {
						t.Fatalf("identical slices should compare equal:\narray=%v", arr)
					}
					sub1 := array.NewSlice(arr, 1, int64(arr.Len()))
					defer sub1.Release()

					sub2 := array.NewSlice(arr, 0, int64(arr.Len()-1))
					defer sub2.Release()

					if array.SliceEqual(sub1, 0, int64(sub1.Len()), sub2, 0, int64(sub2.Len())) && name != "nulls" {
						t.Fatalf("non-identical slices should not compare equal:\nsub1=%v\nsub2=%v\narrf=%v\n", sub1, sub2, arr)
					}
				})
			}
		})
	}
}

func TestArrayApproxEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			rec := recs[0]
			schema := rec.Schema()
			for i, col := range rec.Columns() {
				t.Run(schema.Field(i).Name, func(t *testing.T) {
					arr := col
					if !array.ApproxEqual(arr, arr) {
						t.Fatalf("identical arrays should compare equal:\narray=%v", arr)
					}
					sub1 := array.NewSlice(arr, 1, int64(arr.Len()))
					defer sub1.Release()

					sub2 := array.NewSlice(arr, 0, int64(arr.Len()-1))
					defer sub2.Release()

					if array.ApproxEqual(sub1, sub2) && name != "nulls" {
						t.Fatalf("non-identical arrays should not compare equal:\nsub1=%v\nsub2=%v\narrf=%v\n", sub1, sub2, arr)
					}
				})
			}
		})
	}
}

func TestArrayApproxEqualFloats(t *testing.T) {
	f16sFrom := func(vs []float64) []float16.Num {
		o := make([]float16.Num, len(vs))
		for i, v := range vs {
			o[i] = float16.New(float32(v))
		}
		return o
	}

	for _, tc := range []struct {
		name string
		a1   interface{}
		a2   interface{}
		opts []array.EqualOption
		want bool
	}{
		{
			name: "f16",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			want: true,
		},
		{
			name: "f16-no-tol",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, 7}),
			want: false,
		},
		{
			name: "f16-tol-ok",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, 7}),
			opts: []array.EqualOption{array.WithAbsTolerance(1)},
			want: true,
		},
		{
			name: "f16-nan",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			want: false,
		},
		{
			name: "f16-nan-not",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, 6}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f16-nan-ok",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: true,
		},
		{
			name: "f16-nan-no-tol",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 6, math.NaN()}),
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f16-nan-tol",
			a1:   f16sFrom([]float64{1, 2, 3, 4, 5, math.NaN()}),
			a2:   f16sFrom([]float64{1, 2, 3, 4, 6, math.NaN()}),
			opts: []array.EqualOption{array.WithNaNsEqual(true), array.WithAbsTolerance(1)},
			want: true,
		},
		{
			name: "f32",
			a1:   []float32{1, 2, 3, 4, 5, 6},
			a2:   []float32{1, 2, 3, 4, 5, 6},
			want: true,
		},
		{
			name: "f32-no-tol",
			a1:   []float32{1, 2, 3, 4, 5, 6},
			a2:   []float32{1, 2, 3, 4, 5, 7},
			want: false,
		},
		{
			name: "f32-tol-ok",
			a1:   []float32{1, 2, 3, 4, 5, 6},
			a2:   []float32{1, 2, 3, 4, 5, 7},
			opts: []array.EqualOption{array.WithAbsTolerance(1)},
			want: true,
		},
		{
			name: "f32-nan",
			a1:   []float32{1, 2, 3, 4, 5, 6},
			a2:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			want: false,
		},
		{
			name: "f32-nan-not",
			a1:   []float32{1, 2, 3, 4, 5, 6},
			a2:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f32-nan-ok",
			a1:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			a2:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: true,
		},
		{
			name: "f32-nan-no-tol",
			a1:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			a2:   []float32{1, 2, 3, 4, 6, float32(math.NaN())},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f32-nan-tol",
			a1:   []float32{1, 2, 3, 4, 5, float32(math.NaN())},
			a2:   []float32{1, 2, 3, 4, 6, float32(math.NaN())},
			opts: []array.EqualOption{array.WithNaNsEqual(true), array.WithAbsTolerance(1)},
			want: true,
		},
		{
			name: "f64",
			a1:   []float64{1, 2, 3, 4, 5, 6},
			a2:   []float64{1, 2, 3, 4, 5, 6},
			want: true,
		},
		{
			name: "f64-no-tol",
			a1:   []float64{1, 2, 3, 4, 5, 6},
			a2:   []float64{1, 2, 3, 4, 5, 7},
			want: false,
		},
		{
			name: "f64-tol-ok",
			a1:   []float64{1, 2, 3, 4, 5, 6},
			a2:   []float64{1, 2, 3, 4, 5, 7},
			opts: []array.EqualOption{array.WithAbsTolerance(1)},
			want: true,
		},
		{
			name: "f64-nan",
			a1:   []float64{1, 2, 3, 4, 5, 6},
			a2:   []float64{1, 2, 3, 4, 5, math.NaN()},
			want: false,
		},
		{
			name: "f64-nan-not",
			a1:   []float64{1, 2, 3, 4, 5, 6},
			a2:   []float64{1, 2, 3, 4, 5, math.NaN()},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f64-nan-ok",
			a1:   []float64{1, 2, 3, 4, 5, math.NaN()},
			a2:   []float64{1, 2, 3, 4, 5, math.NaN()},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: true,
		},
		{
			name: "f64-nan-no-tol",
			a1:   []float64{1, 2, 3, 4, 5, math.NaN()},
			a2:   []float64{1, 2, 3, 4, 6, math.NaN()},
			opts: []array.EqualOption{array.WithNaNsEqual(true)},
			want: false,
		},
		{
			name: "f64-nan-tol",
			a1:   []float64{1, 2, 3, 4, 5, math.NaN()},
			a2:   []float64{1, 2, 3, 4, 6, math.NaN()},
			opts: []array.EqualOption{array.WithNaNsEqual(true), array.WithAbsTolerance(1)},
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			a1 := arrayOf(mem, tc.a1, nil)
			defer a1.Release()
			a2 := arrayOf(mem, tc.a2, nil)
			defer a2.Release()

			if got, want := array.ApproxEqual(a1, a2, tc.opts...), tc.want; got != want {
				t.Fatalf("invalid comparison: got=%v, want=%v\na1: %v\na2: %v\n", got, want, a1, a2)
			}
		})
	}
}

func arrayOf(mem memory.Allocator, a interface{}, valids []bool) arrow.Array {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	switch a := a.(type) {
	case []float16.Num:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat16Array()

	case []float32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat32Array()

	case []float64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat64Array()

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}

func TestArrayEqualBaseArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b1 := array.NewBooleanBuilder(mem)
	defer b1.Release()
	b1.Append(true)
	a1 := b1.NewBooleanArray()
	defer a1.Release()

	b2 := array.NewBooleanBuilder(mem)
	defer b2.Release()
	a2 := b2.NewBooleanArray()
	defer a2.Release()

	if array.ArrayEqual(a1, a2) {
		t.Errorf("two arrays with different lengths must not be equal")
	}

	b3 := array.NewBooleanBuilder(mem)
	defer b3.Release()
	b3.AppendNull()
	a3 := b3.NewBooleanArray()
	defer a3.Release()

	if array.ArrayEqual(a1, a3) {
		t.Errorf("two arrays with different number of null values must not be equal")
	}

	b4 := array.NewInt32Builder(mem)
	defer b4.Release()
	b4.Append(0)
	a4 := b4.NewInt32Array()
	defer a4.Release()

	if array.ArrayEqual(a1, a4) {
		t.Errorf("two arrays with different types must not be equal")
	}

	b5 := array.NewBooleanBuilder(mem)
	defer b5.Release()
	b5.AppendNull()
	b5.Append(true)
	a5 := b5.NewBooleanArray()
	defer a5.Release()
	b1.AppendNull()

	if array.ArrayEqual(a1, a5) {
		t.Errorf("two arrays with different validity bitmaps must not be equal")
	}
}

func TestArrayEqualNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	null := array.NewNull(0)
	defer null.Release()

	if !array.ArrayEqual(null, null) {
		t.Fatalf("identical arrays should compare equal")
	}

	n0 := array.NewNull(10)
	defer n0.Release()

	n1 := array.NewNull(10)
	defer n1.Release()

	if !array.ArrayEqual(n0, n0) {
		t.Fatalf("identical arrays should compare equal")
	}
	if !array.ArrayEqual(n1, n1) {
		t.Fatalf("identical arrays should compare equal")
	}
	if !array.ArrayEqual(n0, n1) || !array.ArrayEqual(n1, n0) {
		t.Fatalf("n0 and n1 should compare equal")
	}

	sub07 := array.NewSlice(n0, 0, 7)
	defer sub07.Release()
	sub08 := array.NewSlice(n0, 0, 8)
	defer sub08.Release()
	sub19 := array.NewSlice(n0, 1, 9)
	defer sub19.Release()

	if !array.ArrayEqual(sub08, sub19) {
		t.Fatalf("sub08 and sub19 should compare equal")
	}

	if array.ArrayEqual(sub08, sub07) {
		t.Fatalf("sub08 and sub07 should not compare equal")
	}
}

func TestArrayEqualMaskedArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewInt32Builder(mem)
	defer ab.Release()

	valids := []bool{false, false, false, false}
	ab.AppendValues([]int32{1, 2, 0, 4}, valids)

	a1 := ab.NewInt32Array()
	defer a1.Release()

	ab.AppendValues([]int32{1, 2, 3, 4}, valids)
	a2 := ab.NewInt32Array()
	defer a2.Release()

	if !array.ArrayEqual(a1, a1) || !array.ArrayEqual(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if !array.ArrayEqual(a1, a2) {
		t.Errorf("%v must be equal to %v", a1, a2)
	}
}

func TestArrayEqualDifferentMaskedValues(t *testing.T) {
	// test 2 int32 arrays, with same nulls (but different masked values) compare equal.
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewInt32Builder(mem)
	defer ab.Release()

	valids := []bool{true, true, false, true}
	ab.AppendValues([]int32{1, 2, 0, 4}, valids)

	a1 := ab.NewInt32Array()
	defer a1.Release()

	ab.AppendValues([]int32{1, 2, 3, 4}, valids)
	a2 := ab.NewInt32Array()
	defer a2.Release()

	if !array.ArrayEqual(a1, a1) || !array.ArrayEqual(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if !array.ArrayEqual(a1, a2) {
		t.Errorf("%v must be equal to %v", a1, a2)
	}
}

func TestRecordEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			rec0 := recs[0]
			rec1 := recs[1]
			if !array.RecordEqual(rec0, rec0) {
				t.Fatalf("identical records should compare equal:\nrecord:\n%v", rec0)
			}

			if array.RecordEqual(rec0, rec1) && name != "nulls" {
				t.Fatalf("non-identical records should not compare equal:\nrec0:\n%v\nrec1:\n%v", rec0, rec1)
			}

			sub00 := rec0.NewSlice(0, recs[0].NumRows()-1)
			defer sub00.Release()
			sub01 := rec0.NewSlice(1, recs[0].NumRows())
			defer sub01.Release()

			if array.RecordEqual(sub00, sub01) && name != "nulls" {
				t.Fatalf("non-identical records should not compare equal:\nsub0:\n%v\nsub1:\n%v", sub00, sub01)
			}
		})
	}
}

func TestRecordApproxEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			rec0 := recs[0]
			rec1 := recs[1]
			if !array.RecordApproxEqual(rec0, rec0) {
				t.Fatalf("identical records should compare equal:\nrecord:\n%v", rec0)
			}

			if array.RecordApproxEqual(rec0, rec1) && name != "nulls" {
				t.Fatalf("non-identical records should not compare equal:\nrec0:\n%v\nrec1:\n%v", rec0, rec1)
			}

			sub00 := rec0.NewSlice(0, recs[0].NumRows()-1)
			defer sub00.Release()
			sub01 := rec0.NewSlice(1, recs[0].NumRows())
			defer sub01.Release()

			if array.RecordApproxEqual(sub00, sub01) && name != "nulls" {
				t.Fatalf("non-identical records should not compare equal:\nsub0:\n%v\nsub1:\n%v", sub00, sub01)
			}
		})
	}
}

func TestChunkedEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			tbl := array.NewTableFromRecords(recs[0].Schema(), recs)
			defer tbl.Release()

			for i := 0; i < int(tbl.NumCols()); i++ {
				if !array.ChunkedEqual(tbl.Column(i).Data(), tbl.Column(i).Data()) && name != "nulls" {
					t.Fatalf("identical chunked arrays should compare as equal:\narr:%v\n", tbl.Column(i).Data())
				}
			}
		})
	}
}

func TestChunkedApproxEqual(t *testing.T) {
	fb := array.NewFloat64Builder(memory.DefaultAllocator)
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

	c1 := arrow.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]arrow.Array{f1, f2, f3},
	)
	defer c1.Release()

	fb.AppendValues([]float64{1, 2, 3}, nil)
	f4 := fb.NewFloat64Array()
	defer f4.Release()

	fb.AppendValues([]float64{4, 5}, nil)
	f5 := fb.NewFloat64Array()
	defer f5.Release()

	fb.AppendValues([]float64{6, 7, 8, 9}, nil)
	f6 := fb.NewFloat64Array()
	defer f6.Release()

	fb.AppendValues([]float64{10}, nil)
	f7 := fb.NewFloat64Array()
	defer f7.Release()

	c2 := arrow.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]arrow.Array{f4, f5, f6, f7},
	)
	defer c2.Release()

	assert.True(t, array.ChunkedEqual(c1, c2))
	assert.True(t, array.ChunkedApproxEqual(c1, c2))
}

func TestTableEqual(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			tbl := array.NewTableFromRecords(recs[0].Schema(), recs)
			defer tbl.Release()

			if !array.TableEqual(tbl, tbl) {
				t.Fatalf("identical tables should compare as equal:\tbl:%v\n", tbl)
			}
			if !array.TableApproxEqual(tbl, tbl) {
				t.Fatalf("identical tables should compare as approx equal:\tbl:%v\n", tbl)
			}
		})
	}
}

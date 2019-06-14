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
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/arrdata"
	"github.com/apache/arrow/go/arrow/memory"
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

					if array.ArrayEqual(sub1, sub2) {
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
					if !array.ArraySliceEqual(
						arr, 0, int64(arr.Len()),
						arr, 0, int64(arr.Len()),
					) {
						t.Fatalf("identical slices should compare equal:\narray=%v", arr)
					}
					sub1 := array.NewSlice(arr, 1, int64(arr.Len()))
					defer sub1.Release()

					sub2 := array.NewSlice(arr, 0, int64(arr.Len()-1))
					defer sub2.Release()

					if array.ArraySliceEqual(sub1, 0, int64(sub1.Len()), sub2, 0, int64(sub2.Len())) {
						t.Fatalf("non-identical slices should not compare equal:\nsub1=%v\nsub2=%v\narrf=%v\n", sub1, sub2, arr)
					}
				})
			}
		})
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

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
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestFixedSizeListArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs      = []int32{0, 1, 2, 3, 4, 5, 6}
		lengths = []int{3, 0, 4}
		isValid = []bool{true, false, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, int32(len(vs)), arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	for i := 0; i < 10; i++ {
		vb := lb.ValueBuilder().(*array.Int32Builder)
		vb.Reserve(len(vs))

		pos := 0
		for i, length := range lengths {
			lb.Append(isValid[i])
			for j := 0; j < length; j++ {
				vb.Append(vs[pos])
				pos++
			}
		}

		arr := lb.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		arr.Retain()
		arr.Release()

		if got, want := arr.DataType().ID(), arrow.FIXED_SIZE_LIST; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}

		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		for i := range lengths {
			if got, want := arr.IsValid(i), isValid[i]; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
			if got, want := arr.IsNull(i), lengths[i] == 0; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		varr := arr.ListValues().(*array.Int32)
		if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}

func TestFixedSizeListArrayEmpty(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	lb := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()
	if got, want := arr.Len(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestFixedSizeListArrayBulkAppend(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs      = []int32{0, 1, 2, 3, 4, 5, 6}
		lengths = []int{3, 0, 4}
		isValid = []bool{true, false, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, int32(len(vs)), arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	lb.AppendValues(isValid)
	for _, v := range vs {
		vb.Append(v)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	if got, want := arr.DataType().ID(), arrow.FIXED_SIZE_LIST; got != want {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	if got, want := arr.Len(), len(isValid); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range lengths {
		if got, want := arr.IsValid(i), isValid[i]; got != want {
			t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
		}
		if got, want := arr.IsNull(i), lengths[i] == 0; got != want {
			t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
		}
	}

	varr := arr.ListValues().(*array.Int32)
	if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestFixedSizeListArrayStringer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const N = 3
	var (
		vs      = [][N]int32{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, -9, -8}}
		isValid = []bool{true, false, true, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	for i, v := range vs {
		lb.Append(isValid[i])
		vb.AppendValues(v[:], nil)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	arr.Retain()
	arr.Release()

	want := `[[0 1 2] (null) [6 7 8] [9 -9 -8]]`
	if got, want := arr.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestFixedSizeListArraySlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const N = 3
	var (
		vs      = [][N]int32{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, -9, -8}}
		isValid = []bool{true, false, true, true}
	)

	lb := array.NewFixedSizeListBuilder(pool, N, arrow.PrimitiveTypes.Int32)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int32Builder)
	vb.Reserve(len(vs))

	for i, v := range vs {
		lb.Append(isValid[i])
		vb.AppendValues(v[:], nil)
	}

	arr := lb.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	arr.Retain()
	arr.Release()

	want := `[[0 1 2] (null) [6 7 8] [9 -9 -8]]`
	if got, want := arr.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	sub := array.NewSlice(arr, 1, 3).(*array.FixedSizeList)
	defer sub.Release()

	want = `[(null) [6 7 8]]`
	if got, want := sub.String(), want; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

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

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestListArray(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()

			for i := 0; i < 10; i++ {
				vb := lb.ValueBuilder().(*array.Int32Builder)
				vb.Reserve(len(vs))

				pos := 0
				for i, length := range lengths {
					lb.AppendWithSize(isValid[i], length)
					for j := 0; j < length; j++ {
						vb.Append(vs[pos])
						pos++
					}
				}

				arr := lb.NewArray().(array.ListLike)
				defer arr.Release()

				arr.Retain()
				arr.Release()

				if got, want := arr.DataType().ID(), tt.typeID; got != want {
					t.Fatalf("got=%v, want=%v", got, want)
				}

				if got, want := arr.Len(), len(isValid); got != want {
					t.Fatalf("got=%d, want=%d", got, want)
				}

				for i := range lengths {
					if got, want := arr.IsValid(i), isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
					if got, want := arr.IsNull(i), !isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
				}

				var gotOffsets, gotSizes interface{}
				switch tt.typeID {
				case arrow.LIST:
					arr := arr.(*array.List)
					gotOffsets = arr.Offsets()
				case arrow.LARGE_LIST:
					arr := arr.(*array.LargeList)
					gotOffsets = arr.Offsets()
				case arrow.LIST_VIEW:
					arr := arr.(*array.ListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				case arrow.LARGE_LIST_VIEW:
					arr := arr.(*array.LargeListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				}

				if !reflect.DeepEqual(gotOffsets, tt.offsets) {
					t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
				}

				if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
					if !reflect.DeepEqual(gotSizes, tt.sizes) {
						t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
					}
				}

				varr := arr.ListValues().(*array.Int32)
				if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%v, want=%v", got, want)
				}
			}
		})
	}
}

// Like the list-view tests in TestListArray, but with out-of-order offsets.
func TestListViewArray(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()

			for i := 0; i < 10; i++ {
				switch lvb := lb.(type) {
				case *array.ListViewBuilder:
					lvb.AppendDimensions(5, 3)
					lb.AppendNull()
					lvb.AppendDimensions(0, 0)
					lvb.AppendDimensions(1, 4)
				case *array.LargeListViewBuilder:
					lvb.AppendDimensions(5, 3)
					lb.AppendNull()
					lvb.AppendDimensions(0, 0)
					lvb.AppendDimensions(1, 4)
				}

				vb := lb.ValueBuilder().(*array.Int32Builder)
				vb.Reserve(len(vs))
				vb.AppendValues(vs, []bool{false, true, true, true, true, true, true, true})

				arr := lb.NewArray().(array.ListLike)
				defer arr.Release()

				arr.Retain()
				arr.Release()

				if got, want := arr.DataType().ID(), tt.typeID; got != want {
					t.Fatalf("got=%v, want=%v", got, want)
				}

				if got, want := arr.Len(), len(isValid); got != want {
					t.Fatalf("got=%d, want=%d", got, want)
				}

				for i := range lengths {
					if got, want := arr.IsValid(i), isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
					if got, want := arr.IsNull(i), !isValid[i]; got != want {
						t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
					}
				}

				var gotOffsets, gotSizes interface{}
				switch tt.typeID {
				case arrow.LIST_VIEW:
					arr := arr.(*array.ListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				case arrow.LARGE_LIST_VIEW:
					arr := arr.(*array.LargeListView)
					gotOffsets = arr.Offsets()
					gotSizes = arr.Sizes()
				}

				if !reflect.DeepEqual(gotOffsets, tt.offsets) {
					t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
				}

				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}

				varr := arr.ListValues().(*array.Int32)
				if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%v, want=%v", got, want)
				}
			}
		})
	}
}

func TestListArrayEmpty(t *testing.T) {
	typ := []arrow.DataType{
		arrow.ListOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListOf(arrow.PrimitiveTypes.Int32),
		arrow.ListViewOf(arrow.PrimitiveTypes.Int32),
		arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32),
	}

	for _, dt := range typ {
		t.Run(dt.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			lb := array.NewBuilder(pool, dt)
			defer lb.Release()
			arr := lb.NewArray()
			defer arr.Release()
			if got, want := arr.Len(), 0; got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		})
	}
}

func TestListArrayBulkAppend(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				gotOffsets = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				gotOffsets = arr.Offsets()
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}
			if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestListViewArrayBulkAppend(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}
			if !reflect.DeepEqual(gotSizes, tt.sizes) {
				t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestListArraySlice(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, nil, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, nil, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST_VIEW, []int32{0, 3, 3, 3, 7}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{0, 3, 3, 3, 7}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{0, 1, 2, 3, 4, 5, 6}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				gotOffsets = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				gotOffsets = arr.Offsets()
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}

			if tt.typeID == arrow.LIST_VIEW || tt.typeID == arrow.LARGE_LIST_VIEW {
				if !reflect.DeepEqual(gotSizes, tt.sizes) {
					t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
				}
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.String(), `[[0 1 2] (null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
			assert.Equal(t, "[0,1,2]", arr.ValueStr(0))

			sub := array.NewSlice(arr, 1, 4).(array.ListLike)
			defer sub.Release()

			if got, want := sub.String(), `[(null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
		})
	}
}

func TestLisViewtArraySlice(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		sizes   interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST_VIEW, []int32{5, 0, 0, 1}, []int32{3, 0, 0, 4}, arrow.ListViewOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST_VIEW, []int64{5, 0, 0, 1}, []int64{3, 0, 0, 4}, arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.typeID.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			var (
				vs      = []int32{-1, 3, 4, 5, 6, 0, 1, 2}
				lengths = []int{3, 0, 0, 4}
				isValid = []bool{true, false, true, true}
			)

			lb := array.NewBuilder(pool, tt.dt).(array.VarLenListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST_VIEW:
				lb.(*array.ListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int32), tt.sizes.([]int32), isValid)
			case arrow.LARGE_LIST_VIEW:
				lb.(*array.LargeListViewBuilder).AppendValuesWithSizes(tt.offsets.([]int64), tt.sizes.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.VarLenListLike)
			defer arr.Release()

			if got, want := arr.DataType().ID(), tt.typeID; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.Len(), len(isValid); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range lengths {
				if got, want := arr.IsValid(i), isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := arr.IsNull(i), !isValid[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
			}

			var gotOffsets, gotSizes interface{}
			switch tt.typeID {
			case arrow.LIST_VIEW:
				arr := arr.(*array.ListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			case arrow.LARGE_LIST_VIEW:
				arr := arr.(*array.LargeListView)
				gotOffsets = arr.Offsets()
				gotSizes = arr.Sizes()
			}

			if !reflect.DeepEqual(gotOffsets, tt.offsets) {
				t.Fatalf("got=%v, want=%v", gotOffsets, tt.offsets)
			}

			if !reflect.DeepEqual(gotSizes, tt.sizes) {
				t.Fatalf("got=%v, want=%v", gotSizes, tt.sizes)
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.String(), `[[0 1 2] (null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
			assert.Equal(t, "[0,1,2]", arr.ValueStr(0))

			sub := array.NewSlice(arr, 1, 4).(array.ListLike)
			defer sub.Release()

			if got, want := sub.String(), `[(null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
		})
	}
}

func TestListStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int32Builder)

	var values = [][]int32{
		{0, 1, 2, 3, 4, 5, 6},
		{1, 2, 3, 4, 5, 6, 7},
		{2, 3, 4, 5, 6, 7, 8},
		{3, 4, 5, 6, 7, 8, 9},
	}
	for _, value := range values {
		b.AppendNull()
		b.Append(true)
		for _, el := range value {
			vb.Append(el)
			vb.AppendNull()
		}
		b.Append(false)
	}

	arr := b.NewArray().(*array.List)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.List)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestLargeListStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int32Builder)

	var values = [][]int32{
		{0, 1, 2, 3, 4, 5, 6},
		{1, 2, 3, 4, 5, 6, 7},
		{2, 3, 4, 5, 6, 7, 8},
		{3, 4, 5, 6, 7, 8, 9},
	}
	for _, value := range values {
		b.AppendNull()
		b.Append(true)
		for _, el := range value {
			vb.Append(el)
			vb.AppendNull()
		}
		b.Append(false)
	}

	arr := b.NewArray().(*array.LargeList)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewLargeListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.LargeList)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestListViewStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int32Builder)

	var values = [][]int32{
		{0, 1, 2, 3, 4, 5, 6},
		{1, 2, 3, 4, 5, 6, 7},
		{2, 3, 4, 5, 6, 7, 8},
		{3, 4, 5, 6, 7, 8, 9},
	}
	for _, value := range values {
		b.AppendNull()
		b.AppendWithSize(true, 2*len(value))
		for _, el := range value {
			vb.Append(el)
			vb.AppendNull()
		}
		b.Append(false)
	}

	arr := b.NewArray().(*array.ListView)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.ListView)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestLargeListViewStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int32Builder)

	var values = [][]int32{
		{0, 1, 2, 3, 4, 5, 6},
		{1, 2, 3, 4, 5, 6, 7},
		{2, 3, 4, 5, 6, 7, 8},
		{3, 4, 5, 6, 7, 8, 9},
	}
	for _, value := range values {
		b.AppendNull()
		b.AppendWithSize(true, 2*len(value))
		for _, el := range value {
			vb.Append(el)
			vb.AppendNull()
		}
		b.Append(false)
	}

	arr := b.NewArray().(*array.LargeListView)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewLargeListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.LargeListView)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

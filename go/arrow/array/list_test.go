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

func TestListArray(t *testing.T) {
	tests := []struct {
		typeID  arrow.Type
		offsets interface{}
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, arrow.LargeListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})},
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

			lb := array.NewBuilder(pool, tt.dt).(array.ListLikeBuilder)
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

				var got interface{}
				switch tt.typeID {
				case arrow.LIST:
					arr := arr.(*array.List)
					got = arr.Offsets()
				case arrow.LARGE_LIST:
					arr := arr.(*array.LargeList)
					got = arr.Offsets()
				}

				if !reflect.DeepEqual(got, tt.offsets) {
					t.Fatalf("got=%v, want=%v", got, tt.offsets)
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
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
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

			lb := array.NewBuilder(pool, tt.dt).(array.ListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.ListLike)
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

			var got interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				got = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				got = arr.Offsets()
			}

			if !reflect.DeepEqual(got, tt.offsets) {
				t.Fatalf("got=%v, want=%v", got, tt.offsets)
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
		dt      arrow.DataType
	}{
		{arrow.LIST, []int32{0, 3, 3, 3, 7}, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{arrow.LARGE_LIST, []int64{0, 3, 3, 3, 7}, arrow.LargeListOf(arrow.PrimitiveTypes.Int32)},
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

			lb := array.NewBuilder(pool, tt.dt).(array.ListLikeBuilder)
			defer lb.Release()
			vb := lb.ValueBuilder().(*array.Int32Builder)
			vb.Reserve(len(vs))

			switch tt.typeID {
			case arrow.LIST:
				lb.(*array.ListBuilder).AppendValues(tt.offsets.([]int32), isValid)
			case arrow.LARGE_LIST:
				lb.(*array.LargeListBuilder).AppendValues(tt.offsets.([]int64), isValid)
			}
			for _, v := range vs {
				vb.Append(v)
			}

			arr := lb.NewArray().(array.ListLike)
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

			var got interface{}
			switch tt.typeID {
			case arrow.LIST:
				arr := arr.(*array.List)
				got = arr.Offsets()
			case arrow.LARGE_LIST:
				arr := arr.(*array.LargeList)
				got = arr.Offsets()
			}

			if !reflect.DeepEqual(got, tt.offsets) {
				t.Fatalf("got=%v, want=%v", got, tt.offsets)
			}

			varr := arr.ListValues().(*array.Int32)
			if got, want := varr.Int32Values(), vs; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := arr.String(), `[[0 1 2] (null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}

			sub := array.NewSlice(arr, 1, 4).(array.ListLike)
			defer sub.Release()

			if got, want := sub.String(), `[(null) [] [3 4 5 6]]`; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}
		})
	}
}

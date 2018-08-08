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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestStructArray(t *testing.T) {
	var (
		pool = memory.NewGoAllocator()
		f1s  = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s  = []int32{1, 2, 3, 4}

		f1Lengths = []int{3, 0, 3, 4}
		f1Offsets = []int32{0, 3, 3, 6, 10}
		f1Valids  = []bool{true, false, true, true}

		isValid = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	for i := 0; i < 10; i++ {
		f1b := sb.FieldBuilder(0).(*array.ListBuilder)
		f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
		f2b := sb.FieldBuilder(1).(*array.Int32Builder)

		if got, want := sb.NumField(), 2; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		sb.Resize(len(f1Lengths))
		f1vb.Resize(len(f1s))
		f2b.Resize(len(f2s))

		pos := 0
		for i, length := range f1Lengths {
			f1b.Append(f1Valids[i])
			for j := 0; j < length; j++ {
				f1vb.Append(f1s[pos])
				pos++
			}
			f2b.Append(f2s[i])
		}

		for _, valid := range isValid {
			sb.Append(valid)
		}

		arr := sb.NewArray().(*array.Struct)
		defer arr.Release()

		if got, want := arr.DataType().ID(), arrow.STRUCT; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		for i, valid := range isValid {
			if got, want := arr.IsValid(i), valid; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		{
			f1arr := arr.Field(0).(*array.List)
			if got, want := f1arr.Len(), len(f1Lengths); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range f1Lengths {
				if got, want := f1arr.IsValid(i), f1Valids[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := f1arr.IsNull(i), f1Lengths[i] == 0; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}

			}

			if got, want := f1arr.Offsets(), f1Offsets; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			varr := f1arr.ListValues().(*array.Uint8)
			if got, want := varr.Uint8Values(), f1s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		}

		{
			f2arr := arr.Field(1).(*array.Int32)
			if got, want := f2arr.Len(), len(f2s); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if got, want := f2arr.Int32Values(), f2s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		}
	}
}

func TestStructArrayEmpty(t *testing.T) {
	pool := memory.NewGoAllocator()
	sb := array.NewStructBuilder(pool, arrow.StructOf())
	defer sb.Release()

	if got, want := sb.NumField(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	arr := sb.NewArray().(*array.Struct)

	if got, want := arr.Len(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NumField(), 0; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}
}

func TestStructArrayBulkAppend(t *testing.T) {
	var (
		pool = memory.NewGoAllocator()
		f1s  = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s  = []int32{1, 2, 3, 4}

		f1Lengths = []int{3, 0, 3, 4}
		f1Offsets = []int32{0, 3, 3, 6, 10}
		f1Valids  = []bool{true, false, true, true}

		isValid = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	for i := 0; i < 10; i++ {
		f1b := sb.FieldBuilder(0).(*array.ListBuilder)
		f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
		f2b := sb.FieldBuilder(1).(*array.Int32Builder)

		if got, want := sb.NumField(), 2; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		sb.Resize(len(f1Lengths))
		f1vb.Resize(len(f1s))
		f2b.Resize(len(f2s))

		sb.AppendValues(isValid)
		f1b.AppendValues(f1Offsets, f1Valids)
		f1vb.AppendValues(f1s, nil)
		f2b.AppendValues(f2s, nil)

		arr := sb.NewArray().(*array.Struct)
		defer arr.Release()

		if got, want := arr.DataType().ID(), arrow.STRUCT; got != want {
			t.Fatalf("got=%v, want=%v", got, want)
		}
		if got, want := arr.Len(), len(isValid); got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		for i, valid := range isValid {
			if got, want := arr.IsValid(i), valid; got != want {
				t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
			}
		}

		{
			f1arr := arr.Field(0).(*array.List)
			if got, want := f1arr.Len(), len(f1Lengths); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			for i := range f1Lengths {
				if got, want := f1arr.IsValid(i), f1Valids[i]; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}
				if got, want := f1arr.IsNull(i), f1Lengths[i] == 0; got != want {
					t.Fatalf("got[%d]=%v, want[%d]=%v", i, got, i, want)
				}

			}

			if got, want := f1arr.Offsets(), f1Offsets; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			varr := f1arr.ListValues().(*array.Uint8)
			if got, want := varr.Uint8Values(), f1s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		}

		{
			f2arr := arr.Field(1).(*array.Int32)
			if got, want := f2arr.Len(), len(f2s); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if got, want := f2arr.Int32Values(), f2s; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d, want=%d", got, want)
			}
		}
	}
}

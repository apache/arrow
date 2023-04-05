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

func TestStructArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s = []int32{1, 2, 3, 4}

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

		arr.Retain()
		arr.Release()

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
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

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
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []byte{'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'}
		f2s = []int32{1, 2, 3, 4}

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

func TestStructArrayStringer(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s = []float64{1.1, 1.2, 1.3, 1.4}
		f2s = []int32{1, 2, 3, 4}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range f1s {
		sb.Append(true)
		switch i {
		case 1:
			f1b.AppendNull()
			f2b.Append(f2s[i])
		case 2:
			f1b.Append(f1s[i])
			f2b.AppendNull()
		default:
			f1b.Append(f1s[i])
			f2b.Append(f2s[i])
		}
	}

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	want := "{[1.1 (null) 1.3 1.4] [1 2 (null) 4]}"
	got := arr.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArraySlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s    = []float64{1.1, 1.2, 1.3, 1.4}
		f2s    = []int32{1, 2, 3, 4}
		valids = []bool{true, true, true, true}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)

	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	for i := range f1s {
		sb.Append(valids[i])
		switch i {
		case 1:
			f1b.AppendNull()
			f2b.Append(f2s[i])
		case 2:
			f1b.Append(f1s[i])
			f2b.AppendNull()
		default:
			f1b.Append(f1s[i])
			f2b.Append(f2s[i])
		}
	}

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	// Slice
	arrSlice := array.NewSlice(arr, 2, 4).(*array.Struct)
	defer arrSlice.Release()

	want := "{[1.3 1.4] [(null) 4]}"
	got := arrSlice.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArrayNullBitmap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		f1s    = []float64{1.1, 1.2, 1.3, 1.4}
		f2s    = []int32{1, 2, 3, 4}
		valids = []bool{true, true, true, false}

		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
		}
		dtype = arrow.StructOf(fields...)
	)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.Float64Builder)

	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	if got, want := sb.NumField(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	sb.AppendValues(valids)
	for i := range f1s {
		f1b.Append(f1s[i])
		switch i {
		case 1:
			f2b.AppendNull()
		default:
			f2b.Append(f2s[i])
		}
	}

	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	want := "{[1.1 1.2 1.3 (null)] [1 (null) 3 (null)]}"
	got := arr.String()
	if got != want {
		t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, want)
	}
}

func TestStructArrayUnmarshalJSONMissingFields(t *testing.T) {
	pool := memory.NewGoAllocator()

	var (
		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
			{
				Name: "f3", Type: arrow.StructOf(
					[]arrow.Field{
						{Name: "f3_1", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_2", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_3", Type: arrow.BinaryTypes.String, Nullable: false},
					}...,
				),
			},
		}
		dtype = arrow.StructOf(fields...)
	)

	tests := []struct {
		name      string
		jsonInput string
		want      string
		panic     bool
	}{
		{
			name:      "missing required field",
			jsonInput: `[{"f2": 3, "f3": {"f3_1": "test"}}]`,
			panic:     true,
			want:      "",
		},
		{
			name:      "missing optional fields",
			jsonInput: `[{"f2": 3, "f3": {"f3_3": "test"}}]`,
			panic:     false,
			want:      `{[(null)] [3] {[(null)] [(null)] ["test"]}}`,
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {

				var val bool

				sb := array.NewStructBuilder(pool, dtype)
				defer sb.Release()

				if tc.panic {
					defer func() {
						e := recover()
						if e == nil {
							t.Fatalf("this should have panicked, but did not; slice value %v", val)
						}
						if got, want := e.(string), "arrow/array: index out of range"; got != want {
							t.Fatalf("invalid error. got=%q, want=%q", got, want)
						}
					}()
				} else {
					defer func() {
						if e := recover(); e != nil {
							t.Fatalf("unexpected panic: %v", e)
						}
					}()
				}

				err := sb.UnmarshalJSON([]byte(tc.jsonInput))
				if err != nil {
					t.Fatal(err)
				}

				arr := sb.NewArray().(*array.Struct)
				defer arr.Release()

				got := arr.String()
				if got != tc.want {
					t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, tc.want)
				}
			},
		)
	}
}

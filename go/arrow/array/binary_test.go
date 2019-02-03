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

package array

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)

	values := [][]byte{
		[]byte("AAA"),
		nil,
		[]byte("BBBB"),
	}
	valid := []bool{true, false, true}
	b.AppendValues(values, valid)

	b.Retain()
	b.Release()

	a := b.NewBinaryArray()
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*Binary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))
	a.Release()

	b.Release()
}

func TestBinarySliceData(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "def", "g", "hijk", "lm", "n", "opq", "rs", "tu"}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	for _, v := range values {
		b.AppendString(v)
	}

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	if got, want := arr.Len(), len(values); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	vs := make([]string, arr.Len())

	for i := range vs {
		vs[i] = arr.ValueString(i)
	}

	if got, want := vs, values; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	tests := []struct {
		interval [2]int64
		want     []string
	}{
		{
			interval: [2]int64{0, 0},
			want:     []string{},
		},
		{
			interval: [2]int64{0, 5},
			want:     []string{"a", "bc", "def", "g", "hijk"},
		},
		{
			interval: [2]int64{0, 10},
			want:     []string{"a", "bc", "def", "g", "hijk", "lm", "n", "opq", "rs", "tu"},
		},
		{
			interval: [2]int64{5, 10},
			want:     []string{"lm", "n", "opq", "rs", "tu"},
		},
		{
			interval: [2]int64{10, 10},
			want:     []string{},
		},
		{
			interval: [2]int64{2, 7},
			want:     []string{"def", "g", "hijk", "lm", "n"},
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			slice := NewSlice(arr, tc.interval[0], tc.interval[1]).(*Binary)
			defer slice.Release()

			if got, want := slice.Len(), len(tc.want); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			vs := make([]string, slice.Len())

			for i := range vs {
				vs[i] = slice.ValueString(i)
			}

			if got, want := vs, tc.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestBinarySliceDataWithNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	if got, want := arr.Len(), len(values); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 3; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	vs := make([]string, arr.Len())

	for i := range vs {
		vs[i] = arr.ValueString(i)
	}

	if got, want := vs, values; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	tests := []struct {
		interval [2]int64
		nulls    int
		want     []string
	}{
		{
			interval: [2]int64{0, 2},
			nulls:    0,
			want:     []string{"a", "bc"},
		},
		{
			interval: [2]int64{0, 3},
			nulls:    1,
			want:     []string{"a", "bc", ""},
		},
		{
			interval: [2]int64{0, 4},
			nulls:    2,
			want:     []string{"a", "bc", "", ""},
		},
		{
			interval: [2]int64{4, 8},
			nulls:    0,
			want:     []string{"hijk", "lm", "", "opq"},
		},
		{
			interval: [2]int64{2, 9},
			nulls:    3,
			want:     []string{"", "", "hijk", "lm", "", "opq", ""},
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			slice := NewSlice(arr, tc.interval[0], tc.interval[1]).(*Binary)
			defer slice.Release()

			if got, want := slice.Len(), len(tc.want); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if got, want := slice.NullN(), tc.nulls; got != want {
				t.Errorf("got=%d, want=%d", got, want)
			}

			vs := make([]string, slice.Len())

			for i := range vs {
				vs[i] = slice.ValueString(i)
			}

			if got, want := vs, tc.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestBinarySliceOutOfBounds(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "def", "g", "hijk", "lm", "n", "opq", "rs", "tu"}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	for _, v := range values {
		b.AppendString(v)
	}

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	slice := NewSlice(arr, 3, 8).(*Binary)
	defer slice.Release()

	tests := []struct {
		index int
		panic bool
	}{
		{
			index: -1,
			panic: true,
		},
		{
			index: 5,
			panic: true,
		},
		{
			index: 0,
			panic: false,
		},
		{
			index: 4,
			panic: false,
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			var val string

			if tc.panic {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("this should have panicked, but did not; slice value %q", val)
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

			val = slice.ValueString(tc.index)
		})
	}
}

func TestBinaryValueOffset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	slice := NewSlice(arr, 2, 9).(*Binary)
	defer slice.Release()

	offset := 3
	vs := values[2:9]

	for i, v := range vs {
		assert.Equal(t, offset, slice.ValueOffset(i))
		offset += len(v)
	}
}

func TestBinaryValueLen(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	slice := NewSlice(arr, 2, 9).(*Binary)
	defer slice.Release()

	vs := values[2:9]

	for i, v := range vs {
		assert.Equal(t, len(v), slice.ValueLen(i))
	}
}

func TestBinaryValueOffsets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	assert.Equal(t, []int32{0, 1, 3, 3, 3, 7, 9, 9, 12, 12, 14}, arr.ValueOffsets())

	slice := NewSlice(arr, 2, 9).(*Binary)
	defer slice.Release()

	assert.Equal(t, []int32{3, 3, 3, 7, 9, 9, 12, 12}, slice.ValueOffsets())
}

func TestBinaryValueBytes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	assert.Equal(t, []byte{'a', 'b', 'c', 'h', 'i', 'j', 'k', 'l', 'm', 'o', 'p', 'q', 't', 'u'}, arr.ValueBytes())

	slice := NewSlice(arr, 2, 9).(*Binary)
	defer slice.Release()

	assert.Equal(t, []byte{'h', 'i', 'j', 'k', 'l', 'm', 'o', 'p', 'q'}, slice.ValueBytes())
}

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

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, "QUFB", a.ValueStr(0))
	assert.Equal(t, NullValueStr, a.ValueStr(1))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*Binary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))
	assert.Equal(t, "QUFB", a.ValueStr(0))
	assert.Equal(t, NullValueStr, a.ValueStr(1))
	a.Release()

	b.Release()
}

func TestLargeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)

	values := [][]byte{
		[]byte("AAA"),
		nil,
		[]byte("BBBB"),
	}
	valid := []bool{true, false, true}
	b.AppendValues(values, valid)

	b.Retain()
	b.Release()

	assert.Panics(t, func() {
		b.NewBinaryArray()
	})

	a := b.NewLargeBinaryArray()
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))
	assert.Equal(t, "QUFB", a.ValueStr(0))
	assert.Equal(t, NullValueStr, a.ValueStr(1))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*LargeBinary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))
	assert.Equal(t, "QUFB", a.ValueStr(0))
	assert.Equal(t, NullValueStr, a.ValueStr(1))
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

func TestLargeBinaryValueOffset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*LargeBinary)
	defer arr.Release()

	slice := NewSlice(arr, 2, 9).(*LargeBinary)
	defer slice.Release()

	offset := 3
	vs := values[2:9]

	for i, v := range vs {
		assert.EqualValues(t, offset, slice.ValueOffset(i))
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

func TestLargeBinaryValueLen(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*LargeBinary)
	defer arr.Release()

	slice := NewSlice(arr, 2, 9).(*LargeBinary)
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

func TestLargeBinaryValueOffsets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*LargeBinary)
	defer arr.Release()

	assert.Equal(t, []int64{0, 1, 3, 3, 3, 7, 9, 9, 12, 12, 14}, arr.ValueOffsets())

	slice := NewSlice(arr, 2, 9).(*LargeBinary)
	defer slice.Release()

	assert.Equal(t, []int64{3, 3, 3, 7, 9, 9, 12, 12}, slice.ValueOffsets())
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

func TestLargeBinaryValueBytes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*LargeBinary)
	defer arr.Release()

	assert.Equal(t, []byte{'a', 'b', 'c', 'h', 'i', 'j', 'k', 'l', 'm', 'o', 'p', 'q', 't', 'u'}, arr.ValueBytes())

	slice := NewSlice(arr, 2, 9).(*LargeBinary)
	defer slice.Release()

	assert.Equal(t, []byte{'h', 'i', 'j', 'k', 'l', 'm', 'o', 'p', 'q'}, slice.ValueBytes())
}

func TestBinaryStringer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "é", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, true, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	got := arr.String()
	want := `["a" "bc" (null) "é" (null) "hijk" "lm" "" "opq" (null) "tu"]`

	if got != want {
		t.Fatalf("invalid stringer:\ngot= %s\nwant=%s\n", got, want)
	}
}

func TestLargeBinaryStringer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "é", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, true, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*LargeBinary)
	defer arr.Release()

	got := arr.String()
	want := `["a" "bc" (null) "é" (null) "hijk" "lm" "" "opq" (null) "tu"]`

	if got != want {
		t.Fatalf("invalid stringer:\ngot= %s\nwant=%s\n", got, want)
	}
}

func TestBinaryInvalidOffsets(t *testing.T) {
	const expectedPanic = "arrow/array: binary offsets out of bounds of data buffer"

	makeBuffers := func(valids []bool, offsets []int32, data string) []*memory.Buffer {
		offsetBuf := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
		var nullBufBytes []byte
		var nullBuf *memory.Buffer
		if valids != nil {
			nullBufBytes = make([]byte, bitutil.BytesForBits(int64(len(valids))))
			for i, v := range valids {
				bitutil.SetBitTo(nullBufBytes, i, v)
			}
			nullBuf = memory.NewBufferBytes(nullBufBytes)
		}
		return []*memory.Buffer{nullBuf, offsetBuf, memory.NewBufferBytes([]byte(data))}
	}

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{}, "")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 0, buffers, nil, 0, 0))
	}, "empty array with no offsets")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 5}, "")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 0, buffers, nil, 0, 0))
	}, "empty array, offsets ignored")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 4, 9}, "oooabcdef")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 1, buffers, nil, 0, 2))
	}, "data has offset and value offsets are valid")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 6, 9, 9}, "012345678")
		arr := NewBinaryData(NewData(arrow.BinaryTypes.Binary, 4, buffers, nil, 0, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Zero(t, arr.NullN()) {
			assert.EqualValues(t, "012", arr.Value(0))
			assert.EqualValues(t, "345", arr.Value(1))
			assert.EqualValues(t, "678", arr.Value(2))
			assert.EqualValues(t, "", arr.Value(3), "trailing empty binary value will have offset past end")
		}
	}, "simple valid case")

	assert.NotPanics(t, func() {
		buffers := makeBuffers([]bool{true, false, true, false}, []int32{0, 3, 4, 9, 9}, "oooabcdef")
		arr := NewBinaryData(NewData(arrow.BinaryTypes.Binary, 4, buffers, nil, 2, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Equal(t, 2, arr.NullN()) {
			assert.EqualValues(t, "ooo", arr.Value(0))
			assert.True(t, arr.IsNull(1))
			assert.EqualValues(t, "bcdef", arr.Value(2))
			assert.True(t, arr.IsNull(3))
		}
	}, "simple valid case with nulls")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int32{0, 5}, "abc")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 1, buffers, nil, 0, 0))
	}, "last offset is overflowing")

	assert.PanicsWithError(t, "arrow/array: binary offset buffer must have at least 2 values", func() {
		buffers := makeBuffers(nil, []int32{0}, "abc")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 1, buffers, nil, 0, 0))
	}, "last offset is missing")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 10, 15}, "oooabcdef")
		NewBinaryData(NewData(arrow.BinaryTypes.Binary, 1, buffers, nil, 0, 2))
	}, "data has offset and value offset is overflowing")
}

func TestBinaryStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valid := []bool{true, true, false, false, true, true, true, true, false, true}

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()

	b.AppendStringValues(values, valid)

	arr := b.NewArray().(*Binary)
	defer arr.Release()

	// 2. create array via AppendValueFromString

	b1 := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*Binary)
	defer arr1.Release()

	assert.True(t, Equal(arr, arr1))
}

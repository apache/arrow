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
	"bytes"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestStringArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want    = []string{"hello", "世界", "", "bye"}
		valids  = []bool{true, true, false, true}
		offsets = []int32{0, 5, 11, 11, 14}
	)

	sb := array.NewStringBuilder(mem)
	defer sb.Release()

	sb.Retain()
	sb.Release()

	assert.NoError(t, sb.AppendValueFromString(want[0]))
	sb.AppendValues(want[1:2], nil)

	sb.AppendNull()
	sb.Append(want[3])

	if got, want := sb.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := sb.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := sb.NewStringArray()
	defer arr.Release()

	arr.Retain()
	arr.Release()

	assert.Equal(t, "hello", arr.ValueStr(0))

	if got, want := arr.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	for i := range want {
		if arr.IsNull(i) != !valids[i] {
			t.Fatalf("arr[%d]-validity: got=%v want=%v", i, !arr.IsNull(i), valids[i])
		}
		switch {
		case arr.IsNull(i):
		default:
			got := arr.Value(i)
			if got != want[i] {
				t.Fatalf("arr[%d]: got=%q, want=%q", i, got, want[i])
			}
		}

		if got, want := arr.ValueOffset(i), int(offsets[i]); got != want {
			t.Fatalf("arr-offset-beg[%d]: got=%d, want=%d", i, got, want)
		}
		if got, want := arr.ValueOffset(i+1), int(offsets[i+1]); got != want {
			t.Fatalf("arr-offset-end[%d]: got=%d, want=%d", i+1, got, want)
		}
	}

	if !reflect.DeepEqual(offsets, arr.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", arr.ValueOffsets(), offsets)
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.STRING {
		t.Fatalf("invalid type: got=%q, want=string", sub.DataType().Name())
	}

	if _, ok := sub.(*array.String); !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := arr.String(), `["hello" "世界" (null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal([]byte(`hello世界bye`), arr.ValueBytes()) {
		t.Fatalf("got=%q, want=%q", string(arr.ValueBytes()), `hello世界bye`)
	}

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.String)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal(v.ValueBytes(), []byte("bye")) {
		t.Fatalf("got=%q, want=%q", string(v.ValueBytes()), "bye")
	}

	for i := 0; i < v.Len(); i++ {
		if got, want := v.ValueOffset(0), int(offsets[i+slice.Offset()]); got != want {
			t.Fatalf("val-offset-with-offset[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if !reflect.DeepEqual(offsets[2:5], v.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", v.ValueOffsets(), offsets[2:5])
	}
}

func TestStringBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []string{"hello", "世界", "", "bye"}

	ab := array.NewStringBuilder(mem)
	defer ab.Release()

	stringValues := func(a *array.String) []string {
		vs := make([]string, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	ab.AppendValues([]string{}, nil)
	a := ab.NewStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues([]string{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]string{}, nil)
	a = ab.NewStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()
}

// TestStringReset tests the Reset() method on the String type by creating two different Strings and then
// reseting the contents of string2 with the values from string1.
func TestStringReset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sb1 := array.NewStringBuilder(mem)
	sb2 := array.NewStringBuilder(mem)
	defer sb1.Release()
	defer sb2.Release()

	sb1.Append("string1")
	sb1.AppendNull()

	var (
		string1 = sb1.NewStringArray()
		string2 = sb2.NewStringArray()

		string1Data = string1.Data()
	)
	string2.Reset(string1Data)

	assert.Equal(t, "string1", string2.Value(0))
}

func TestStringInvalidOffsets(t *testing.T) {
	const expectedPanic = "arrow/array: string offsets out of bounds of data buffer"

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
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 0, buffers, nil, 0, 0))
	}, "empty array with no offsets")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 5}, "")
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 0, buffers, nil, 0, 0))
	}, "empty array, offsets ignored")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 4, 9}, "oooabcdef")
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 1, buffers, nil, 0, 2))
	}, "data has offset and value offsets are valid")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 6, 9, 9}, "012345678")
		arr := array.NewStringData(array.NewData(arrow.BinaryTypes.String, 4, buffers, nil, 0, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Zero(t, arr.NullN()) {
			assert.Equal(t, "012", arr.Value(0))
			assert.Equal(t, "345", arr.Value(1))
			assert.Equal(t, "678", arr.Value(2))
			assert.Equal(t, "", arr.Value(3), "trailing empty string value will have offset past end")
		}
	}, "simple valid case")

	assert.NotPanics(t, func() {
		buffers := makeBuffers([]bool{true, false, true, false}, []int32{0, 3, 4, 9, 9}, "oooabcdef")
		arr := array.NewStringData(array.NewData(arrow.BinaryTypes.String, 4, buffers, nil, 2, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Equal(t, 2, arr.NullN()) {
			assert.Equal(t, "ooo", arr.Value(0))
			assert.True(t, arr.IsNull(1))
			assert.Equal(t, "bcdef", arr.Value(2))
			assert.True(t, arr.IsNull(3))
		}
	}, "simple valid case with nulls")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int32{0, 5}, "abc")
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 1, buffers, nil, 0, 0))
	}, "last offset is overflowing")

	assert.PanicsWithError(t, "arrow/array: string offset buffer must have at least 2 values", func() {
		buffers := makeBuffers(nil, []int32{0}, "abc")
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 1, buffers, nil, 0, 0))
	}, "last offset is missing")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int32{0, 3, 10, 15}, "oooabcdef")
		array.NewStringData(array.NewData(arrow.BinaryTypes.String, 1, buffers, nil, 0, 2))
	}, "data has offset and value offset is overflowing")
}

func TestStringStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		values = []string{"hello", "世界", "", "bye"}
		valid  = []bool{true, true, false, true}
	)

	b := array.NewStringBuilder(mem)
	defer b.Release()

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.String)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewStringBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.String)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestLargeStringArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want    = []string{"hello", "世界", "", "bye"}
		valids  = []bool{true, true, false, true}
		offsets = []int64{0, 5, 11, 11, 14}
	)

	sb := array.NewLargeStringBuilder(mem)
	defer sb.Release()

	sb.Retain()
	sb.Release()

	sb.AppendValues(want[:2], nil)

	sb.AppendNull()
	sb.Append(want[3])

	if got, want := sb.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := sb.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := sb.NewLargeStringArray()
	defer arr.Release()

	arr.Retain()
	arr.Release()

	if got, want := arr.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	for i := range want {
		if arr.IsNull(i) != !valids[i] {
			t.Fatalf("arr[%d]-validity: got=%v want=%v", i, !arr.IsNull(i), valids[i])
		}
		switch {
		case arr.IsNull(i):
		default:
			got := arr.Value(i)
			if got != want[i] {
				t.Fatalf("arr[%d]: got=%q, want=%q", i, got, want[i])
			}
		}

		if got, want := arr.ValueOffset(i), offsets[i]; got != want {
			t.Fatalf("arr-offset-beg[%d]: got=%d, want=%d", i, got, want)
		}
		if got, want := arr.ValueOffset(i+1), offsets[i+1]; got != want {
			t.Fatalf("arr-offset-end[%d]: got=%d, want=%d", i+1, got, want)
		}
	}

	if !reflect.DeepEqual(offsets, arr.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", arr.ValueOffsets(), offsets)
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.LARGE_STRING {
		t.Fatalf("invalid type: got=%q, want=large_string", sub.DataType().Name())
	}

	if _, ok := sub.(*array.LargeString); !ok {
		t.Fatalf("could not type-assert to array.LargeString")
	}

	if got, want := arr.String(), `["hello" "世界" (null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal([]byte(`hello世界bye`), arr.ValueBytes()) {
		t.Fatalf("got=%q, want=%q", string(arr.ValueBytes()), `hello世界bye`)
	}

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.LargeString)
	if !ok {
		t.Fatalf("could not type-assert to array.LargeString")
	}

	if got, want := v.String(), `[(null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal(v.ValueBytes(), []byte("bye")) {
		t.Fatalf("got=%q, want=%q", string(v.ValueBytes()), "bye")
	}

	for i := 0; i < v.Len(); i++ {
		if got, want := v.ValueOffset(0), offsets[i+slice.Offset()]; got != want {
			t.Fatalf("val-offset-with-offset[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if !reflect.DeepEqual(offsets[2:5], v.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", v.ValueOffsets(), offsets[2:5])
	}
}

func TestLargeStringBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []string{"hello", "世界", "", "bye"}

	ab := array.NewLargeStringBuilder(mem)
	defer ab.Release()

	stringValues := func(a *array.LargeString) []string {
		vs := make([]string, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	ab.AppendValues([]string{}, nil)
	a := ab.NewLargeStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewLargeStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues([]string{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewLargeStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]string{}, nil)
	a = ab.NewLargeStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()
}

// TestStringReset tests the Reset() method on the String type by creating two different Strings and then
// reseting the contents of string2 with the values from string1.
func TestLargeStringReset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sb1 := array.NewLargeStringBuilder(mem)
	sb2 := array.NewLargeStringBuilder(mem)
	defer sb1.Release()
	defer sb2.Release()

	sb1.Append("string1")
	sb1.AppendNull()

	var (
		string1 = sb1.NewLargeStringArray()
		string2 = sb2.NewLargeStringArray()

		string1Data = string1.Data()
	)
	string2.Reset(string1Data)

	assert.Equal(t, "string1", string2.Value(0))
}

func TestLargeStringInvalidOffsets(t *testing.T) {
	const expectedPanic = "arrow/array: string offsets out of bounds of data buffer"

	makeBuffers := func(valids []bool, offsets []int64, data string) []*memory.Buffer {
		offsetBuf := memory.NewBufferBytes(arrow.Int64Traits.CastToBytes(offsets))
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
		buffers := makeBuffers(nil, []int64{}, "")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 0, buffers, nil, 0, 0))
	}, "empty array with no offsets")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int64{0, 5}, "")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 0, buffers, nil, 0, 0))
	}, "empty array, offsets ignored")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int64{0, 3, 4, 9}, "oooabcdef")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 1, buffers, nil, 0, 2))
	}, "data has offset and value offsets are valid")

	assert.NotPanics(t, func() {
		buffers := makeBuffers(nil, []int64{0, 3, 6, 9, 9}, "012345678")
		arr := array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 4, buffers, nil, 0, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Zero(t, arr.NullN()) {
			assert.Equal(t, "012", arr.Value(0))
			assert.Equal(t, "345", arr.Value(1))
			assert.Equal(t, "678", arr.Value(2))
			assert.Equal(t, "", arr.Value(3), "trailing empty string value will have offset past end")
		}
	}, "simple valid case")

	assert.NotPanics(t, func() {
		buffers := makeBuffers([]bool{true, false, true, false}, []int64{0, 3, 4, 9, 9}, "oooabcdef")
		arr := array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 4, buffers, nil, 2, 0))
		if assert.Equal(t, 4, arr.Len()) && assert.Equal(t, 2, arr.NullN()) {
			assert.Equal(t, "ooo", arr.Value(0))
			assert.True(t, arr.IsNull(1))
			assert.Equal(t, "bcdef", arr.Value(2))
			assert.True(t, arr.IsNull(3))
		}
	}, "simple valid case with nulls")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int64{0, 5}, "abc")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 1, buffers, nil, 0, 0))
	}, "last offset is overflowing")

	assert.PanicsWithError(t, "arrow/array: string offset buffer must have at least 2 values", func() {
		buffers := makeBuffers(nil, []int64{0}, "abc")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 1, buffers, nil, 0, 0))
	}, "last offset is missing")

	assert.PanicsWithValue(t, expectedPanic, func() {
		buffers := makeBuffers(nil, []int64{0, 3, 10, 15}, "oooabcdef")
		array.NewLargeStringData(array.NewData(arrow.BinaryTypes.LargeString, 1, buffers, nil, 0, 2))
	}, "data has offset and value offset is overflowing")
}

func TestLargeStringStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		values = []string{"hello", "世界", "", "bye"}
		valid  = []bool{true, true, false, true}
	)

	b := array.NewLargeStringBuilder(mem)
	defer b.Release()

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.LargeString)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewLargeStringBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.LargeString)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestStringValueLen(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	values := []string{"a", "bc", "", "", "hijk", "lm", "", "opq", "", "tu"}
	valids := []bool{true, true, false, false, true, true, true, true, false, true}

	b := array.NewStringBuilder(mem)
	defer b.Release()

	b.AppendStringValues(values, valids)

	arr := b.NewArray().(*array.String)
	defer arr.Release()

	slice := array.NewSlice(arr, 2, 9).(*array.String)
	defer slice.Release()

	vs := values[2:9]

	for i, v := range vs {
		assert.Equal(t, len(v), slice.ValueLen(i))
	}
}

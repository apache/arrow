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

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func TestFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := array.NewFixedSizeBinaryBuilder(mem, &dtype)

	zero := make([]byte, dtype.ByteWidth)

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	valid := []bool{true, false, true}
	b.AppendValues(values, valid)
	// encoded abcdefg base64
	assert.NoError(t, b.AppendValueFromString("YWJjZGVmZw=="))

	b.Retain()
	b.Release()

	a := b.NewFixedSizeBinaryArray()
	assert.Equal(t, 4, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, "YWJjZGVmZw==", a.ValueStr(3))
	assert.Equal(t, zero, a.Value(1))
	assert.Equal(t, true, a.IsNull(1))
	assert.Equal(t, false, a.IsValid(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*array.FixedSizeBinary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, zero, a.Value(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	b.Release()
}

func TestFixedSizeBinarySlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 4}
	b := array.NewFixedSizeBinaryBuilder(mem, dtype)
	defer b.Release()

	var data = [][]byte{
		[]byte("ABCD"),
		[]byte("1234"),
		nil,
		[]byte("AZER"),
	}
	b.AppendValues(data[:2], nil)
	b.AppendNull()
	b.Append(data[3])

	arr := b.NewFixedSizeBinaryArray()
	defer arr.Release()

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.FixedSizeBinary)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) "AZER"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if got, want := v.NullN(), 1; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestFixedSizeBinary_MarshalUnmarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 4}
	b := array.NewFixedSizeBinaryBuilder(mem, dtype)
	defer b.Release()

	var data = [][]byte{
		[]byte("ABCD"),
		[]byte("1234"),
		nil,
		[]byte("AZER"),
	}
	b.AppendValues(data[:2], nil)
	b.AppendNull()
	b.Append(data[3])

	arr := b.NewFixedSizeBinaryArray()
	defer arr.Release()

	jsonBytes, err := arr.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	err = b.UnmarshalJSON(jsonBytes)
	if err != nil {
		t.Fatalf("failed to unmarshal json: %v", err)
	}
	gotArr := b.NewFixedSizeBinaryArray()
	defer gotArr.Release()

	gotString := gotArr.String()
	wantString := arr.String()
	if gotString != wantString {
		t.Fatalf("got=%q, want=%q", gotString, wantString)
	}
}

func TestFixedSizeBinaryStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := &arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := array.NewFixedSizeBinaryBuilder(mem, dt)

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	valid := []bool{true, false, true}
	b.AppendValues(values, valid)
	// encoded abcdefg base64
	assert.NoError(t, b.AppendValueFromString("YWJjZGVmZw=="))

	arr := b.NewArray().(*array.FixedSizeBinary)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewFixedSizeBinaryBuilder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.FixedSizeBinary)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

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

package ipc

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/endian"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func AssertArrayDataEqualWithSwappedEndian(t *testing.T, target, expected arrow.ArrayData) bool {
	assert.NoError(t, swapEndianArrayData(target.(*array.Data)))
	swappedArray := array.MakeFromData(target)
	defer swappedArray.Release()

	expectedArray := array.MakeFromData(expected)
	defer expectedArray.Release()

	return assert.Truef(t, array.Equal(swappedArray, expectedArray), "got: %s, expected: %s\n", swappedArray, expectedArray)
}

func TestSwapEndianPrimitiveArrayData(t *testing.T) {
	nullBuffer := memory.NewBufferBytes([]byte("\xff"))

	tests := []struct {
		dt              arrow.DataType
		len             int
		input, expected string
	}{
		{arrow.Null, 0, "", ""},
		{arrow.PrimitiveTypes.Int32, 0, "", ""},
		{arrow.FixedWidthTypes.Boolean, 8, "01234567", "01234567"},
		{arrow.PrimitiveTypes.Int8, 8, "01234567", "01234567"},
		{arrow.PrimitiveTypes.Uint16, 4, "01234567", "10325476"},
		{arrow.PrimitiveTypes.Int32, 2, "01234567", "32107654"},
		{arrow.PrimitiveTypes.Uint64, 1, "01234567", "76543210"},
		{&arrow.Decimal128Type{Precision: 38, Scale: 10}, 1, "0123456789abcdef", "fedcba9876543210"},
		{&arrow.Decimal256Type{Precision: 72, Scale: 10}, 1, "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"},
		{arrow.PrimitiveTypes.Float32, 2, "01200560", "02100650"},
		{arrow.PrimitiveTypes.Float64, 1, "01200560", "06500210"},
	}

	for _, tt := range tests {
		t.Run(tt.dt.String(), func(t *testing.T) {
			var target, expected arrow.ArrayData
			if tt.dt == arrow.Null {
				target = array.NewData(arrow.Null, 0, []*memory.Buffer{nil}, nil, 0, 0)
				expected = target
			} else {
				target = array.NewData(tt.dt, tt.len, []*memory.Buffer{nullBuffer, memory.NewBufferBytes([]byte(tt.input))}, nil, 0, 0)
				expected = array.NewData(tt.dt, tt.len, []*memory.Buffer{nullBuffer, memory.NewBufferBytes([]byte(tt.expected))}, nil, 0, 0)
				defer target.Release()
				defer expected.Release()
			}
			AssertArrayDataEqualWithSwappedEndian(t, target, expected)
		})
	}

	data := array.NewData(arrow.PrimitiveTypes.Int64, 1, []*memory.Buffer{nullBuffer, memory.NewBufferBytes([]byte("01234567"))}, nil, 0, 1)
	assert.Error(t, swapEndianArrayData(data))
}

func replaceBuffer(data *array.Data, idx int, bufdata []byte) *array.Data {
	out := data.Copy()
	buffers := out.Buffers()
	buffers[idx].Release()
	buffers[idx] = memory.NewBufferBytes(bufdata)
	return out
}

func replaceBuffersInChild(data *array.Data, childIdx int, bufdata []byte) *array.Data {
	out := data.Copy()
	// assume updating only buffer[1] in child data
	children := out.Children()
	child := children[childIdx].(*array.Data).Copy()
	children[childIdx].Release()
	child.Buffers()[1].Release()
	child.Buffers()[1] = memory.NewBufferBytes(bufdata)
	children[childIdx] = child

	return out
}

func replaceBuffersInDict(data *array.Data, bufferIdx int, bufdata []byte) *array.Data {
	out := data.Copy()
	dictData := out.Dictionary().(*array.Data).Copy()
	dictData.Buffers()[bufferIdx].Release()
	dictData.Buffers()[bufferIdx] = memory.NewBufferBytes(bufdata)
	defer dictData.Release()
	out.SetDictionary(dictData)
	return out
}

func TestSwapEndianArrayDataBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// binary type assumes the json string data is base64 encoded
	// MDEyMw== -> 0123
	// NDU= -> 45
	arr, _, err := array.FromJSON(mem, arrow.BinaryTypes.Binary, strings.NewReader(`["MDEyMw==", null, "NDU="]`))
	require.NoError(t, err)
	defer arr.Release()

	var offsets []byte
	if endian.IsBigEndian {
		offsets = []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0}
	} else {
		offsets = []byte{0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6}
	}
	expected := arr.Data().(*array.Data)
	test := replaceBuffer(expected, 1, offsets)
	defer test.Release()
	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayString(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["ABCD", null, "EF"]`))
	require.NoError(t, err)
	defer arr.Release()

	var offsets []byte
	if endian.IsBigEndian {
		offsets = []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0}
	} else {
		offsets = []byte{0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffer(expected, 1, offsets)
	defer test.Release()
	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayListType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`[[0, 1, 2, 3], null, [4, 5]]`))
	require.NoError(t, err)
	defer arr.Release()

	var (
		offsets, data []byte
	)
	if endian.IsBigEndian {
		offsets = []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0}
		data = []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0}
	} else {
		offsets = []byte{0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6}
		data = []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffer(expected, 1, offsets)
	defer test.Release()
	test = replaceBuffersInChild(test, 0, data)
	defer test.Release()

	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayFixedSizeList(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32)
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`[[0, 1], null, [2, 3]]`))
	require.NoError(t, err)
	defer arr.Release()

	var data []byte
	if endian.IsBigEndian {
		data = []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}
	} else {
		data = []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffersInChild(expected, 0, data)
	defer test.Release()

	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayDictType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Int16}
	dict, _, err := array.FromJSON(mem, dt.ValueType, strings.NewReader(`[4, 5, 6, 7]`))
	require.NoError(t, err)
	defer dict.Release()

	indices, _, _ := array.FromJSON(mem, dt.IndexType, strings.NewReader("[0, 2, 3]"))
	defer indices.Release()

	arr := array.NewDictionaryArray(dt, indices, dict)
	defer arr.Release()

	var (
		data1, data2 []byte
	)
	if endian.IsBigEndian {
		data1 = []byte{0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}
		data2 = []byte{4, 0, 5, 0, 6, 0, 7, 0}
	} else {
		data1 = []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3}
		data2 = []byte{0, 4, 0, 5, 0, 6, 0, 7}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffer(expected, 1, data1)
	defer test.Release()
	test = replaceBuffersInDict(test, 1, data2)
	defer test.Release()

	// dictionary must be explicitly swapped!
	assert.NoError(t, swapEndianArrayData(test.Dictionary().(*array.Data)))
	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayStruct(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	), strings.NewReader(`[{"a": 4, "b": null}, {"a": null, "b": "foo"}]`))
	require.NoError(t, err)
	defer arr.Release()

	var data1, data2 []byte
	if endian.IsBigEndian {
		data1 = []byte{4, 0, 0, 0, 0, 0, 0, 0}
		data2 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0}
	} else {
		data1 = []byte{0, 0, 0, 4, 0, 0, 0, 0}
		data2 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffersInChild(expected, 0, data1)
	defer test.Release()
	test = replaceBuffersInChild(test, 1, data2)
	defer test.Release()
	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

func TestSwapEndianArrayExtensionType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arrInt16, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(`[0, 1, 2, 3]`))
	defer arrInt16.Release()

	extData := array.NewData(types.NewSmallintType(), arrInt16.Len(), arrInt16.Data().Buffers(), nil, 0, 0)
	defer extData.Release()

	arr := array.MakeFromData(extData)
	defer arr.Release()

	var data []byte
	if endian.IsBigEndian {
		data = []byte{0, 0, 1, 0, 2, 0, 3, 0}
	} else {
		data = []byte{0, 0, 0, 1, 0, 2, 0, 3}
	}

	expected := arr.Data().(*array.Data)
	test := replaceBuffer(expected, 1, data)
	defer test.Release()
	AssertArrayDataEqualWithSwappedEndian(t, test, expected)
}

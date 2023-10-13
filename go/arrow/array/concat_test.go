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
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/rand"
)

func TestConcatenateValueBuffersNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	inputs := make([]arrow.Array, 0)

	bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()
	inputs = append(inputs, arr)

	bldr.AppendNull()
	arr = bldr.NewArray()
	defer arr.Release()
	inputs = append(inputs, arr)

	actual, err := array.Concatenate(inputs, mem)
	assert.NoError(t, err)
	defer actual.Release()

	assert.True(t, array.Equal(actual, inputs[1]))
}

func TestConcatenate(t *testing.T) {
	tests := []struct {
		dt arrow.DataType
	}{
		{arrow.FixedWidthTypes.Boolean},
		{arrow.PrimitiveTypes.Int8},
		{arrow.PrimitiveTypes.Uint8},
		{arrow.PrimitiveTypes.Int16},
		{arrow.PrimitiveTypes.Uint16},
		{arrow.PrimitiveTypes.Int32},
		{arrow.PrimitiveTypes.Uint32},
		{arrow.PrimitiveTypes.Int64},
		{arrow.PrimitiveTypes.Uint64},
		{arrow.PrimitiveTypes.Float32},
		{arrow.PrimitiveTypes.Float64},
		{arrow.BinaryTypes.String},
		{arrow.BinaryTypes.LargeString},
		{arrow.ListOf(arrow.PrimitiveTypes.Int8)},
		{arrow.LargeListOf(arrow.PrimitiveTypes.Int8)},
		{arrow.ListViewOf(arrow.PrimitiveTypes.Int8)},
		{arrow.LargeListViewOf(arrow.PrimitiveTypes.Int8)},
		{arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int8)},
		{arrow.StructOf()},
		{arrow.MapOf(arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Int8)},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.PrimitiveTypes.Float64}},
	}

	for _, tt := range tests {
		t.Run(tt.dt.Name(), func(t *testing.T) {
			suite.Run(t, &ConcatTestSuite{
				seed:      0xdeadbeef,
				dt:        tt.dt,
				nullProbs: []float64{0.0, 0.1, 0.5, 0.9, 1.0},
				sizes:     []int32{0, 1, 2, 4, 16, 31, 1234},
			})
		})
	}
}

type ConcatTestSuite struct {
	suite.Suite

	seed uint64
	rng  gen.RandomArrayGenerator
	dt   arrow.DataType

	nullProbs []float64
	sizes     []int32

	mem *memory.CheckedAllocator
}

func (cts *ConcatTestSuite) SetupSuite() {
	cts.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	cts.rng = gen.NewRandomArrayGenerator(cts.seed, cts.mem)
}

func (cts *ConcatTestSuite) TearDownSuite() {
	cts.mem.AssertSize(cts.T(), 0)
}

func (cts *ConcatTestSuite) generateArr(size int64, nullprob float64) arrow.Array {
	switch cts.dt.ID() {
	case arrow.BOOL:
		return cts.rng.Boolean(size, 0.5, nullprob)
	case arrow.INT8:
		return cts.rng.Int8(size, 0, 127, nullprob)
	case arrow.UINT8:
		return cts.rng.Uint8(size, 0, 127, nullprob)
	case arrow.INT16:
		return cts.rng.Int16(size, 0, 127, nullprob)
	case arrow.UINT16:
		return cts.rng.Uint16(size, 0, 127, nullprob)
	case arrow.INT32:
		return cts.rng.Int32(size, 0, 127, nullprob)
	case arrow.UINT32:
		return cts.rng.Uint32(size, 0, 127, nullprob)
	case arrow.INT64:
		return cts.rng.Int64(size, 0, 127, nullprob)
	case arrow.UINT64:
		return cts.rng.Uint64(size, 0, 127, nullprob)
	case arrow.FLOAT32:
		return cts.rng.Float32(size, 0, 127, nullprob)
	case arrow.FLOAT64:
		return cts.rng.Float64(size, 0, 127, nullprob)
	case arrow.NULL:
		return array.NewNull(int(size))
	case arrow.STRING:
		return cts.rng.String(size, 0, 15, nullprob)
	case arrow.LARGE_STRING:
		return cts.rng.LargeString(size, 0, 15, nullprob)
	case arrow.LIST:
		valuesSize := size * 4
		values := cts.rng.Int8(valuesSize, 0, 127, nullprob).(*array.Int8)
		defer values.Release()
		offsetsVector := cts.offsets(int32(valuesSize), int32(size))
		// ensure the first and last offsets encompass the whole values
		offsetsVector[0] = 0
		offsetsVector[len(offsetsVector)-1] = int32(valuesSize)

		bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8)
		defer bldr.Release()

		valid := make([]bool, len(offsetsVector)-1)
		for i := range valid {
			valid[i] = true
		}
		bldr.AppendValues(offsetsVector, valid)
		vb := bldr.ValueBuilder().(*array.Int8Builder)
		for i := 0; i < values.Len(); i++ {
			if values.IsValid(i) {
				vb.Append(values.Value(i))
			} else {
				vb.AppendNull()
			}
		}
		return bldr.NewArray()
	case arrow.LARGE_LIST:
		valuesSize := size * 8
		values := cts.rng.Int8(valuesSize, 0, 127, nullprob).(*array.Int8)
		defer values.Release()
		offsetsVector := cts.largeoffsets(int64(valuesSize), int32(size))
		// ensure the first and last offsets encompass the whole values
		offsetsVector[0] = 0
		offsetsVector[len(offsetsVector)-1] = int64(valuesSize)

		bldr := array.NewLargeListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8)
		defer bldr.Release()

		valid := make([]bool, len(offsetsVector)-1)
		for i := range valid {
			valid[i] = true
		}
		bldr.AppendValues(offsetsVector, valid)
		vb := bldr.ValueBuilder().(*array.Int8Builder)
		for i := 0; i < values.Len(); i++ {
			if values.IsValid(i) {
				vb.Append(values.Value(i))
			} else {
				vb.AppendNull()
			}
		}
		return bldr.NewArray()
	case arrow.LIST_VIEW:
		arr := cts.rng.ListView(cts.dt.(arrow.VarLenListLikeType), size, 0, 20, nullprob)
		err := arr.ValidateFull()
		cts.NoError(err)
		return arr
	case arrow.LARGE_LIST_VIEW:
		arr := cts.rng.LargeListView(cts.dt.(arrow.VarLenListLikeType), size, 0, 20, nullprob)
		err := arr.ValidateFull()
		cts.NoError(err)
		return arr
	case arrow.FIXED_SIZE_LIST:
		const listsize = 3
		valuesSize := size * listsize
		values := cts.rng.Int8(valuesSize, 0, 127, nullprob)
		defer values.Release()

		data := array.NewData(arrow.FixedSizeListOf(listsize, arrow.PrimitiveTypes.Int8), int(size), []*memory.Buffer{nil}, []arrow.ArrayData{values.Data()}, 0, 0)
		defer data.Release()
		return array.MakeFromData(data)
	case arrow.STRUCT:
		foo := cts.rng.Int8(size, 0, 127, nullprob)
		defer foo.Release()
		bar := cts.rng.Float64(size, 0, 127, nullprob)
		defer bar.Release()
		baz := cts.rng.Boolean(size, 0.5, nullprob)
		defer baz.Release()

		data := array.NewData(arrow.StructOf(
			arrow.Field{Name: "foo", Type: foo.DataType(), Nullable: true},
			arrow.Field{Name: "bar", Type: bar.DataType(), Nullable: true},
			arrow.Field{Name: "baz", Type: baz.DataType(), Nullable: true}),
			int(size), []*memory.Buffer{nil}, []arrow.ArrayData{foo.Data(), bar.Data(), baz.Data()}, 0, 0)
		defer data.Release()
		return array.NewStructData(data)
	case arrow.MAP:
		valuesSize := size * 4
		keys := cts.rng.Uint16(valuesSize, 0, 127, 0).(*array.Uint16)
		defer keys.Release()
		values := cts.rng.Int8(valuesSize, 0, 127, nullprob).(*array.Int8)
		defer values.Release()

		offsetsVector := cts.offsets(int32(valuesSize), int32(size))
		offsetsVector[0] = 0
		offsetsVector[len(offsetsVector)-1] = int32(valuesSize)

		bldr := array.NewMapBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Int8, false)
		defer bldr.Release()

		kb := bldr.KeyBuilder().(*array.Uint16Builder)
		vb := bldr.ItemBuilder().(*array.Int8Builder)

		valid := make([]bool, len(offsetsVector)-1)
		for i := range valid {
			valid[i] = true
		}
		bldr.AppendValues(offsetsVector, valid)
		for i := 0; i < int(valuesSize); i++ {
			kb.Append(keys.Value(i))
			if values.IsValid(i) {
				vb.Append(values.Value(i))
			} else {
				vb.AppendNull()
			}
		}
		return bldr.NewArray()
	case arrow.DICTIONARY:
		indices := cts.rng.Int32(size, 0, 127, nullprob)
		defer indices.Release()
		dict := cts.rng.Float64(128, 0.0, 127.0, nullprob)
		defer dict.Release()
		return array.NewDictionaryArray(cts.dt, indices, dict)
	default:
		return nil
	}
}

func (cts *ConcatTestSuite) slices(arr arrow.Array, offsets []int32) []arrow.Array {
	slices := make([]arrow.Array, len(offsets)-1)
	for i := 0; i != len(slices); i++ {
		slices[i] = array.NewSlice(arr, int64(offsets[i]), int64(offsets[i+1]))
	}
	return slices
}

func (cts *ConcatTestSuite) checkTrailingBitsZeroed(bitmap *memory.Buffer, length int64) {
	if preceding := bitutil.PrecedingBitmask[length%8]; preceding != 0 {
		lastByte := bitmap.Bytes()[length/8]
		cts.Equal(lastByte&preceding, lastByte, length, preceding)
	}
}

func (cts *ConcatTestSuite) offsets(length, slicecount int32) []int32 {
	offsets := make([]int32, slicecount+1)
	dist := rand.New(rand.NewSource(cts.seed))
	for i := range offsets {
		offsets[i] = dist.Int31n(length + 1)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets
}

func (cts *ConcatTestSuite) largeoffsets(length int64, slicecount int32) []int64 {
	offsets := make([]int64, slicecount+1)
	dist := rand.New(rand.NewSource(cts.seed))
	for i := range offsets {
		offsets[i] = dist.Int63n(length + 1)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets
}

func (cts *ConcatTestSuite) TestCheckConcat() {
	for _, sz := range cts.sizes {
		cts.Run(fmt.Sprintf("size %d", sz), func() {
			offsets := cts.offsets(sz, 3)
			for _, np := range cts.nullProbs {
				cts.Run(fmt.Sprintf("nullprob %0.2f", np), func() {
					scopedMem := memory.NewCheckedAllocatorScope(cts.mem)
					defer scopedMem.CheckSize(cts.T())

					arr := cts.generateArr(int64(sz), np)
					defer arr.Release()
					expected := array.NewSlice(arr, int64(offsets[0]), int64(offsets[len(offsets)-1]))
					defer expected.Release()

					slices := cts.slices(arr, offsets)
					for _, s := range slices {
						if s.DataType().ID() == arrow.LIST_VIEW {
							err := s.(*array.ListView).ValidateFull()
							cts.NoError(err)
						}
						defer s.Release()
					}

					actual, err := array.Concatenate(slices, cts.mem)
					cts.NoError(err)
					if arr.DataType().ID() == arrow.LIST_VIEW {
						lv := actual.(*array.ListView)
						err := lv.ValidateFull()
						cts.NoError(err)
					}
					defer actual.Release()

					cts.Truef(array.Equal(expected, actual), "expected: %s\ngot: %s\n", expected, actual)
					if len(actual.Data().Buffers()) > 0 {
						if actual.Data().Buffers()[0] != nil {
							cts.checkTrailingBitsZeroed(actual.Data().Buffers()[0], int64(actual.Len()))
						}
						if actual.DataType().ID() == arrow.BOOL {
							cts.checkTrailingBitsZeroed(actual.Data().Buffers()[1], int64(actual.Len()))
						}
					}
				})
			}
		})
	}
}

func TestConcatDifferentDicts(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("simple dicts", func(t *testing.T) {
		scopedMem := memory.NewCheckedAllocatorScope(mem)
		defer scopedMem.CheckSize(t)

		dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
		dict1, err := array.DictArrayFromJSON(mem, dictType, `[1, 2, null, 3, 0]`, `["A0", "A1", "A2", "A3"]`)
		require.NoError(t, err)
		defer dict1.Release()
		dict2, err := array.DictArrayFromJSON(mem, dictType, `[null, 4, 2, 1]`, `["B0", "B1", "B2", "B3", "B4"]`)
		require.NoError(t, err)
		defer dict2.Release()

		expected, err := array.DictArrayFromJSON(mem, dictType, `[1, 2, null, 3, 0, null, 8, 6, 5]`, `["A0", "A1", "A2", "A3", "B0", "B1", "B2", "B3", "B4"]`)
		require.NoError(t, err)
		defer expected.Release()

		concat, err := array.Concatenate([]arrow.Array{dict1, dict2}, mem)
		assert.NoError(t, err)
		defer concat.Release()
		assert.Truef(t, array.Equal(concat, expected), "got: %s, expected: %s", concat, expected)
	})

	t.Run("larger", func(t *testing.T) {
		scopedMem := memory.NewCheckedAllocatorScope(mem)
		defer scopedMem.CheckSize(t)

		const size = 500
		dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}

		idxBuilder, exIdxBldr := array.NewUint16Builder(mem), array.NewUint16Builder(mem)
		defer idxBuilder.Release()
		defer exIdxBldr.Release()
		idxBuilder.Reserve(size)
		exIdxBldr.Reserve(size * 2)

		for i := uint16(0); i < size; i++ {
			idxBuilder.UnsafeAppend(i)
			exIdxBldr.UnsafeAppend(i)
		}
		for i := uint16(size); i < 2*size; i++ {
			exIdxBldr.UnsafeAppend(i)
		}

		indices, expIndices := idxBuilder.NewArray(), exIdxBldr.NewArray()
		defer indices.Release()
		defer expIndices.Release()

		// create three dictionaries. First maps i -> "{i}", second maps i->"{500+i}",
		// each for 500 values and the third maps i -> "{i}" but for 1000 values.
		// first and second concatenated should end up equaling the third. All strings
		// padded to length 8 so we can know the size ahead of time.
		valuesOneBldr, valuesTwoBldr := array.NewStringBuilder(mem), array.NewStringBuilder(mem)
		defer valuesOneBldr.Release()
		defer valuesTwoBldr.Release()

		valuesOneBldr.Reserve(size)
		valuesTwoBldr.Reserve(size)
		valuesOneBldr.ReserveData(size * 8)
		valuesTwoBldr.ReserveData(size * 8)

		for i := 0; i < size; i++ {
			valuesOneBldr.Append(fmt.Sprintf("%-8d", i))
			valuesTwoBldr.Append(fmt.Sprintf("%-8d", i+size))
		}

		dict1, dict2 := valuesOneBldr.NewArray(), valuesTwoBldr.NewArray()
		defer dict1.Release()
		defer dict2.Release()
		expectedDict, err := array.Concatenate([]arrow.Array{dict1, dict2}, mem)
		require.NoError(t, err)
		defer expectedDict.Release()

		one, two := array.NewDictionaryArray(dictType, indices, dict1), array.NewDictionaryArray(dictType, indices, dict2)
		defer one.Release()
		defer two.Release()
		expected := array.NewDictionaryArray(dictType, expIndices, expectedDict)
		defer expected.Release()

		combined, err := array.Concatenate([]arrow.Array{one, two}, mem)
		assert.NoError(t, err)
		defer combined.Release()
		assert.Truef(t, array.Equal(combined, expected), "got: %s, expected: %s", combined, expected)
	})
}

func TestConcatDictionaryPartialOverlap(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
	dictOne, err := array.DictArrayFromJSON(mem, dt, `[1, 2, null, 3, 0]`, `["A0", "A1", "C2", "C3"]`)
	require.NoError(t, err)
	defer dictOne.Release()

	dictTwo, err := array.DictArrayFromJSON(mem, dt, `[null, 4, 2, 1]`, `["B0", "B1", "C2", "C3", "B4"]`)
	require.NoError(t, err)
	defer dictTwo.Release()

	expected, err := array.DictArrayFromJSON(mem, dt, `[1, 2, null, 3, 0, null, 6, 2, 5]`, `["A0", "A1", "C2", "C3", "B0", "B1", "B4"]`)
	require.NoError(t, err)
	defer expected.Release()

	actual, err := array.Concatenate([]arrow.Array{dictOne, dictTwo}, mem)
	assert.NoError(t, err)
	defer actual.Release()

	assert.Truef(t, array.Equal(actual, expected), "got: %s, expected: %s", actual, expected)
}

func TestConcatDictionaryDifferentSizeIndex(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
	biggerDt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}
	dictOne, err := array.DictArrayFromJSON(mem, dt, `[0]`, `["A0"]`)
	require.NoError(t, err)
	defer dictOne.Release()

	dictTwo, err := array.DictArrayFromJSON(mem, biggerDt, `[0]`, `["B0"]`)
	require.NoError(t, err)
	defer dictTwo.Release()

	arr, err := array.Concatenate([]arrow.Array{dictOne, dictTwo}, mem)
	assert.Nil(t, arr)
	assert.Error(t, err)
}

func TestConcatDictionaryUnifyNullInDict(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}
	dictOne, err := array.DictArrayFromJSON(mem, dt, `[0, 1]`, `[null, "A"]`)
	require.NoError(t, err)
	defer dictOne.Release()

	dictTwo, err := array.DictArrayFromJSON(mem, dt, `[0, 1]`, `[null, "B"]`)
	require.NoError(t, err)
	defer dictTwo.Release()

	expected, err := array.DictArrayFromJSON(mem, dt, `[0, 1, 0, 2]`, `[null, "A", "B"]`)
	require.NoError(t, err)
	defer expected.Release()

	actual, err := array.Concatenate([]arrow.Array{dictOne, dictTwo}, mem)
	assert.NoError(t, err)
	defer actual.Release()

	assert.Truef(t, array.Equal(actual, expected), "got: %s, expected: %s", actual, expected)
}

func TestConcatDictionaryEnlargedIndices(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const size = math.MaxUint8 + 1
	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.PrimitiveTypes.Uint16}

	idxBuilder := array.NewUint8Builder(mem)
	defer idxBuilder.Release()
	idxBuilder.Reserve(size)
	for i := 0; i < size; i++ {
		idxBuilder.UnsafeAppend(uint8(i))
	}
	indices := idxBuilder.NewUint8Array()
	defer indices.Release()

	valuesBuilder := array.NewUint16Builder(mem)
	defer valuesBuilder.Release()
	valuesBuilder.Reserve(size)
	valuesBuilderTwo := array.NewUint16Builder(mem)
	defer valuesBuilderTwo.Release()
	valuesBuilderTwo.Reserve(size)

	for i := uint16(0); i < size; i++ {
		valuesBuilder.UnsafeAppend(i)
		valuesBuilderTwo.UnsafeAppend(i + size)
	}

	dict1, dict2 := valuesBuilder.NewUint16Array(), valuesBuilderTwo.NewUint16Array()
	defer dict1.Release()
	defer dict2.Release()

	d1, d2 := array.NewDictionaryArray(dt, indices, dict1), array.NewDictionaryArray(dt, indices, dict2)
	defer d1.Release()
	defer d2.Release()

	_, err := array.Concatenate([]arrow.Array{d1, d2}, mem)
	assert.Error(t, err)

	biggerDt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.PrimitiveTypes.Uint16}
	bigger1, bigger2 := array.NewDictionaryArray(biggerDt, dict1, dict1), array.NewDictionaryArray(biggerDt, dict1, dict2)
	defer bigger1.Release()
	defer bigger2.Release()

	combined, err := array.Concatenate([]arrow.Array{bigger1, bigger2}, mem)
	assert.NoError(t, err)
	defer combined.Release()

	assert.EqualValues(t, size*2, combined.Len())
}

func TestConcatDictionaryNullSlots(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String}
	dict1, err := array.DictArrayFromJSON(mem, dt, `[null, null, null, null]`, `[]`)
	require.NoError(t, err)
	defer dict1.Release()

	dict2, err := array.DictArrayFromJSON(mem, dt, `[null, null, null, null, 0, 1]`, `["a", "b"]`)
	require.NoError(t, err)
	defer dict2.Release()

	expected, err := array.DictArrayFromJSON(mem, dt, `[null, null, null, null, null, null, null, null, 0, 1]`, `["a", "b"]`)
	require.NoError(t, err)
	defer expected.Release()

	actual, err := array.Concatenate([]arrow.Array{dict1, dict2}, mem)
	assert.NoError(t, err)
	defer actual.Release()

	assert.Truef(t, array.Equal(actual, expected), "got: %s, expected: %s", actual, expected)
}

func TestConcatRunEndEncoded(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		offsetType arrow.DataType
		expected   interface{}
	}{
		{arrow.PrimitiveTypes.Int16, []int16{1, 11, 111, 211, 311, 411, 500, 600}},
		{arrow.PrimitiveTypes.Int32, []int32{1, 11, 111, 211, 311, 411, 500, 600}},
		{arrow.PrimitiveTypes.Int64, []int64{1, 11, 111, 211, 311, 411, 500, 600}},
	}

	for _, tt := range tests {
		t.Run(tt.offsetType.String(), func(t *testing.T) {

			arrs := make([]arrow.Array, 0)
			bldr := array.NewRunEndEncodedBuilder(mem, tt.offsetType, arrow.BinaryTypes.String)
			defer bldr.Release()
			valBldr := bldr.ValueBuilder().(*array.StringBuilder)

			bldr.Append(1)
			valBldr.Append("Hello")
			bldr.AppendNull()
			bldr.ContinueRun(9)

			bldr.Append(100)
			valBldr.Append("World")
			arrs = append(arrs, bldr.NewArray())

			bldr.Append(100)
			valBldr.Append("Goku")
			bldr.Append(100)
			valBldr.Append("Gohan")
			bldr.Append(100)
			valBldr.Append("Goten")
			arrs = append(arrs, bldr.NewArray())

			bldr.AppendNull()
			bldr.ContinueRun(99)
			bldr.Append(100)
			valBldr.Append("Vegeta")
			bldr.Append(100)
			valBldr.Append("Trunks")
			next := bldr.NewArray()
			defer next.Release()
			// remove the initial null with an offset and dig into the next run
			arrs = append(arrs, array.NewSlice(next, 111, int64(next.Len())))

			for _, a := range arrs {
				defer a.Release()
			}

			result, err := array.Concatenate(arrs, mem)
			assert.NoError(t, err)
			defer result.Release()

			rle := result.(*array.RunEndEncoded)
			assert.EqualValues(t, 8, rle.GetPhysicalLength())
			assert.EqualValues(t, 0, rle.GetPhysicalOffset())

			var values interface{}
			switch endsArr := rle.RunEndsArr().(type) {
			case *array.Int16:
				values = endsArr.Int16Values()
			case *array.Int32:
				values = endsArr.Int32Values()
			case *array.Int64:
				values = endsArr.Int64Values()
			}
			assert.Equal(t, tt.expected, values)

			expectedValues, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String,
				strings.NewReader(`["Hello", null, "World", "Goku", "Gohan", "Goten", "Vegeta", "Trunks"]`))
			defer expectedValues.Release()
			assert.Truef(t, array.Equal(expectedValues, rle.Values()), "expected: %s\ngot: %s", expectedValues, rle.Values())
		})
	}
}

func TestConcatAlmostOverflowRunEndEncoding(t *testing.T) {
	tests := []struct {
		offsetType arrow.DataType
		max        uint64
	}{
		{arrow.PrimitiveTypes.Int16, math.MaxInt16},
		{arrow.PrimitiveTypes.Int32, math.MaxInt32},
		{arrow.PrimitiveTypes.Int64, math.MaxInt64},
	}

	for _, tt := range tests {
		t.Run(tt.offsetType.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			arrs := make([]arrow.Array, 0)
			bldr := array.NewRunEndEncodedBuilder(mem, tt.offsetType, arrow.BinaryTypes.String)
			defer bldr.Release()
			valBldr := bldr.ValueBuilder().(*array.StringBuilder)

			// max is not evently divisible by 4, so we add one to each
			// to account for that so our final concatenate will overflow
			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("foo")
			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("bar")
			arrs = append(arrs, bldr.NewArray())

			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("baz")
			bldr.Append((tt.max / 4))
			valBldr.Append("bop")
			arrs = append(arrs, bldr.NewArray())

			defer func() {
				for _, a := range arrs {
					a.Release()
				}
			}()

			arr, err := array.Concatenate(arrs, mem)
			assert.NoError(t, err)
			defer arr.Release()
		})
	}
}

func TestConcatOverflowRunEndEncoding(t *testing.T) {
	tests := []struct {
		offsetType arrow.DataType
		max        uint64
	}{
		{arrow.PrimitiveTypes.Int16, math.MaxInt16},
		{arrow.PrimitiveTypes.Int32, math.MaxInt32},
		{arrow.PrimitiveTypes.Int64, math.MaxInt64},
	}

	for _, tt := range tests {
		t.Run(tt.offsetType.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			arrs := make([]arrow.Array, 0)
			bldr := array.NewRunEndEncodedBuilder(mem, tt.offsetType, arrow.BinaryTypes.String)
			defer bldr.Release()
			valBldr := bldr.ValueBuilder().(*array.StringBuilder)

			// max is not evently divisible by 4, so we add one to each
			// to account for that so our final concatenate will overflow
			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("foo")
			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("bar")
			arrs = append(arrs, bldr.NewArray())

			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("baz")
			bldr.Append((tt.max / 4) + 1)
			valBldr.Append("bop")
			arrs = append(arrs, bldr.NewArray())

			defer func() {
				for _, a := range arrs {
					a.Release()
				}
			}()

			arr, err := array.Concatenate(arrs, mem)
			assert.Nil(t, arr)
			assert.ErrorIs(t, err, arrow.ErrInvalid)
		})
	}
}

func TestConcatPanic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	allocator := &panicAllocator{
		n:         400,
		Allocator: mem,
	}

	g := gen.NewRandomArrayGenerator(0, memory.DefaultAllocator)
	ar1 := g.ArrayOf(arrow.STRING, 32, 0)
	defer ar1.Release()
	ar2 := g.ArrayOf(arrow.STRING, 32, 0)
	defer ar2.Release()

	concat, err := array.Concatenate([]arrow.Array{ar1, ar2}, allocator)
	assert.Error(t, err)
	assert.Nil(t, concat)
}

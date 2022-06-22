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
	"sort"
	"testing"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/bitutil"
	"github.com/apache/arrow/go/v9/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/stretchr/testify/assert"
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

	assert.True(t, array.ArrayEqual(actual, inputs[1]))
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
		{arrow.ListOf(arrow.PrimitiveTypes.Int8)},
		{arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int8)},
		{arrow.StructOf()},
		{arrow.MapOf(arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Int8)},
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
						defer s.Release()
					}

					actual, err := array.Concatenate(slices, cts.mem)
					cts.NoError(err)
					defer actual.Release()

					cts.Truef(array.ArrayEqual(expected, actual), "expected: %s\ngot: %s\n", expected, actual)
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

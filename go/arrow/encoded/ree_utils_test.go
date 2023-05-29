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

package encoded_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/encoded"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFindPhysicalOffset(t *testing.T) {
	tests := []struct {
		vals   []int32
		offset int
		exp    int
	}{
		{[]int32{1}, 0, 0},
		{[]int32{1, 2, 3}, 0, 0},
		{[]int32{1, 2, 3}, 1, 1},
		{[]int32{1, 2, 3}, 2, 2},
		{[]int32{2, 3, 4}, 0, 0},
		{[]int32{2, 3, 4}, 1, 0},
		{[]int32{2, 3, 4}, 2, 1},
		{[]int32{2, 3, 4}, 3, 2},
		{[]int32{2, 4, 6}, 3, 1},
		{[]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005, 1015, 1020, 1025, 1050}, 1000, 10},
		// out-of-range logical offset should return len(vals)
		{[]int32{2, 4, 6}, 6, 3},
		{[]int32{2, 4, 6}, 10000, 3},
	}

	reeType := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v find %d", tt.vals, tt.offset), func(t *testing.T) {
			child := array.NewData(arrow.PrimitiveTypes.Int32, len(tt.vals), []*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(tt.vals))}, nil, 0, 0)
			arr := array.NewData(reeType, -1, nil, []arrow.ArrayData{child}, 0, tt.offset)

			assert.Equal(t, tt.exp, encoded.FindPhysicalOffset(arr))
		})
	}
}

func TestFindPhysicalOffsetEmpty(t *testing.T) {
	child := array.NewData(arrow.PrimitiveTypes.Int32, 0, []*memory.Buffer{nil, nil}, nil, 0, 0)
	arr := array.NewData(arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String), -1, nil, []arrow.ArrayData{child}, 0, 0)
	assert.NotPanics(t, func() {
		assert.Equal(t, 0, encoded.FindPhysicalOffset(arr))
	})
}

func TestMergedRunsIter(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	leftRunEnds, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
		strings.NewReader(`[1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005, 1015, 1020, 1025, 30000]`))
	defer leftRunEnds.Release()

	rightRunEnds, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
		strings.NewReader(`[1, 2, 3, 4, 5, 2005, 2009, 2025, 2050]`))
	defer rightRunEnds.Release()

	var (
		expectedRunLengths        = []int32{5, 4, 6, 5, 5, 25}
		expectedLeftVisits        = []int32{110, 111, 111, 112, 113, 114}
		expectedRightVisits       = []int32{205, 206, 207, 207, 207, 208}
		leftPrntOffset      int32 = 1000
		leftChildOffset     int32 = 100
		rightPrntOffset     int32 = 2000
		rightChildOffset    int32 = 200

		leftChild  arrow.Array = array.NewNull(int(leftChildOffset) + leftRunEnds.Len())
		rightChild arrow.Array = array.NewNull(int(rightChildOffset) + rightRunEnds.Len())
	)

	leftChild = array.NewSlice(leftChild, int64(leftChildOffset), int64(leftChildOffset)+int64(leftRunEnds.Len()))
	rightChild = array.NewSlice(rightChild, int64(rightChildOffset), int64(rightChild.Len()))

	leftArray := arrow.Array(array.NewRunEndEncodedArray(leftRunEnds, leftChild, 1050, 0))
	defer leftArray.Release()
	rightArray := arrow.Array(array.NewRunEndEncodedArray(rightRunEnds, rightChild, 2050, 0))
	defer rightArray.Release()

	leftArray = array.NewSlice(leftArray, int64(leftPrntOffset), int64(leftArray.Len()))
	defer leftArray.Release()
	rightArray = array.NewSlice(rightArray, int64(rightPrntOffset), int64(rightArray.Len()))
	defer rightArray.Release()

	pos, logicalPos := 0, 0
	mr := encoded.NewMergedRuns([2]arrow.Array{leftArray, rightArray})
	for mr.Next() {
		assert.EqualValues(t, expectedRunLengths[pos], mr.RunLength())
		assert.EqualValues(t, expectedLeftVisits[pos], mr.IndexIntoBuffer(0))
		assert.EqualValues(t, expectedRightVisits[pos], mr.IndexIntoBuffer(1))
		assert.EqualValues(t, expectedLeftVisits[pos]-int32(leftChildOffset), mr.IndexIntoArray(0))
		assert.EqualValues(t, expectedRightVisits[pos]-int32(rightChildOffset), mr.IndexIntoArray(1))
		pos++
		logicalPos += int(mr.RunLength())
		assert.EqualValues(t, logicalPos, mr.AccumulatedRunLength())
	}
	assert.EqualValues(t, len(expectedRunLengths), pos)

	t.Run("left array only", func(t *testing.T) {
		leftOnlyRunLengths := []int32{5, 10, 5, 5, 25}
		pos, logicalPos := 0, 0
		mr := encoded.NewMergedRuns([2]arrow.Array{leftArray, leftArray})
		for mr.Next() {
			assert.EqualValues(t, leftOnlyRunLengths[pos], mr.RunLength())
			assert.EqualValues(t, 110+pos, mr.IndexIntoBuffer(0))
			assert.EqualValues(t, 110+pos, mr.IndexIntoBuffer(1))
			assert.EqualValues(t, 10+pos, mr.IndexIntoArray(0))
			assert.EqualValues(t, 10+pos, mr.IndexIntoArray(1))
			pos++
			logicalPos += int(mr.RunLength())
			assert.EqualValues(t, logicalPos, mr.AccumulatedRunLength())
		}
		assert.EqualValues(t, len(leftOnlyRunLengths), pos)
	})

	t.Run("right array only", func(t *testing.T) {
		rightOnlyRunLengths := []int32{5, 4, 16, 25}
		pos, logicalPos := 0, 0
		mr := encoded.NewMergedRuns([2]arrow.Array{rightArray, rightArray})
		for mr.Next() {
			assert.EqualValues(t, rightOnlyRunLengths[pos], mr.RunLength())
			assert.EqualValues(t, 205+pos, mr.IndexIntoBuffer(0))
			assert.EqualValues(t, 205+pos, mr.IndexIntoBuffer(1))
			assert.EqualValues(t, 5+pos, mr.IndexIntoArray(0))
			assert.EqualValues(t, 5+pos, mr.IndexIntoArray(1))
			pos++
			logicalPos += int(mr.RunLength())
			assert.EqualValues(t, logicalPos, mr.AccumulatedRunLength())
		}
		assert.EqualValues(t, len(rightOnlyRunLengths), pos)
	})
}

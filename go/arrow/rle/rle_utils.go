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

package rle

import (
	"math"
	"sort"

	"github.com/apache/arrow/go/v10/arrow"
)

func GetPhysicalOffset(data arrow.ArrayData) int {
	runEnds := arrow.Int32Traits.CastFromBytes(data.Children()[0].Buffers()[1].Bytes())
	return FindPhysicalOffset(runEnds, data.Offset())
}

func GetPhysicalLength(data arrow.ArrayData) int {
	if data.Len() == 0 {
		return 0
	}

	// find the offset of the last element and add 1
	runEnds := arrow.Int32Traits.CastFromBytes(data.Children()[0].Buffers()[1].Bytes())
	physOffset := FindPhysicalOffset(runEnds, data.Offset())
	return FindPhysicalOffset(runEnds[physOffset:], data.Offset()+data.Len()-1) + 1
}

func FindPhysicalOffset(runEnds []int32, logicalOffset int) int {
	return sort.Search(len(runEnds), func(i int) bool { return runEnds[i] > int32(logicalOffset) })
}

type MergedRuns struct {
	inputs       [2]arrow.Array
	runIndex     [2]int64
	inputRunEnds [2][]int32
	runEnds      [2]int64
	logicalLen   int
	logicalPos   int
	mergedEnd    int64
}

func NewMergedRuns(inputs [2]arrow.Array) *MergedRuns {
	mr := &MergedRuns{inputs: inputs, logicalLen: inputs[0].Len()}
	for i, in := range inputs {
		if in.DataType().ID() != arrow.RUN_LENGTH_ENCODED {
			panic("arrow/rle: NewMergedRuns can only be called with RunLengthEncoded arrays")
		}
		runEnds := arrow.Int32Traits.CastFromBytes(in.Data().Children()[0].Buffers()[1].Bytes())
		mr.inputRunEnds[i] = runEnds
		// initialize the runIndex at the physical offset - 1 so the first
		// call to Next will increment it to the correct initial offset
		// since the initial state is logicalPos == 0 and mergedEnd == 0
		mr.runIndex[i] = int64(FindPhysicalOffset(runEnds, in.Data().Offset())) - 1
	}

	return mr
}

func (mr *MergedRuns) Next() bool {
	mr.logicalPos = int(mr.mergedEnd)
	if mr.isEnd() {
		return false
	}

	for i := range mr.inputs {
		if mr.logicalPos == int(mr.runEnds[i]) {
			mr.runIndex[i]++
		}
	}
	mr.findMergedRun()

	return true
}

func (mr *MergedRuns) IndexIntoBuffer(id int) int64 {
	return mr.runIndex[id] + int64(mr.inputs[id].Data().Children()[1].Offset())
}
func (mr *MergedRuns) IndexIntoArray(id int) int64 { return mr.runIndex[id] }
func (mr *MergedRuns) RunLength() int64            { return mr.mergedEnd - int64(mr.logicalPos) }
func (mr *MergedRuns) AccumulatedRunLength() int64 { return mr.mergedEnd }

func (mr *MergedRuns) findMergedRun() {
	mr.mergedEnd = int64(math.MaxInt64)
	for i, in := range mr.inputs {
		// logical indices of the end of the run we are currently in each input
		mr.runEnds[i] = int64(mr.inputRunEnds[i][mr.runIndex[i]] - int32(in.Data().Offset()))
		// the logical length may end in the middle of a run, in case the array was sliced
		if mr.logicalLen < int(mr.runEnds[i]) {
			mr.runEnds[i] = int64(mr.logicalLen)
		}
		if mr.runEnds[i] < mr.mergedEnd {
			mr.mergedEnd = mr.runEnds[i]
		}
	}
}

func (mr *MergedRuns) isEnd() bool { return mr.logicalPos == mr.logicalLen }

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

package file

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/parquet/internal/bmi"
	"github.com/apache/arrow/go/v14/parquet/internal/utils"
	"github.com/stretchr/testify/assert"
)

func bitmapToString(bitmap []byte, bitCount int64) string {
	var bld strings.Builder
	bld.Grow(int(bitCount))
	for i := 0; i < int(bitCount); i++ {
		if bitutil.BitIsSet(bitmap, i) {
			bld.WriteByte('1')
		} else {
			bld.WriteByte('0')
		}
	}
	return bld.String()
}

func TestDefLevelsToBitmap(t *testing.T) {
	defLevels := []int16{3, 3, 3, 2, 3, 3, 3, 3, 3}
	validBits := []byte{2, 0}

	var info LevelInfo
	info.DefLevel = 3
	info.RepLevel = 1

	var io ValidityBitmapInputOutput
	io.ReadUpperBound = int64(len(defLevels))
	io.Read = -1
	io.ValidBits = validBits

	DefLevelsToBitmap(defLevels, info, &io)
	assert.Equal(t, int64(9), io.Read)
	assert.Equal(t, int64(1), io.NullCount)

	// call again with 0 definition levels make sure that valid bits is unmodified
	curByte := validBits[1]
	io.NullCount = 0
	DefLevelsToBitmap(defLevels[:0], info, &io)

	assert.Zero(t, io.Read)
	assert.Zero(t, io.NullCount)
	assert.Equal(t, curByte, validBits[1])
}

func TestDefLevelstToBitmapPowerOf2(t *testing.T) {
	defLevels := []int16{3, 3, 3, 2, 3, 3, 3, 3}
	validBits := []byte{1, 0}

	var (
		info LevelInfo
		io   ValidityBitmapInputOutput
	)

	info.RepLevel = 1
	info.DefLevel = 3
	io.Read = -1
	io.ReadUpperBound = int64(len(defLevels))
	io.ValidBits = validBits

	DefLevelsToBitmap(defLevels[4:8], info, &io)
	assert.Equal(t, int64(4), io.Read)
	assert.Zero(t, io.NullCount)
}

func TestGreaterThanBitmapGeneratesExpectedBitmasks(t *testing.T) {
	defLevels := []int16{
		0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
		0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
		0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
		0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7}

	tests := []struct {
		name     string
		num      int
		rhs      int16
		expected uint64
	}{
		{"no levels", 0, 0, 0},
		{"64 and 8", 64, 8, 0},
		{"64 and -1", 64, -1, 0xFFFFFFFFFFFFFFFF},
		// should be zero padded
		{"zero pad 47, -1", 47, -1, 0x7FFFFFFFFFFF},
		{"zero pad 64 and 6", 64, 6, 0x8080808080808080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, bmi.GreaterThanBitmap(defLevels[:tt.num], tt.rhs))
		})
	}
}

func TestWithRepetitionlevelFiltersOutEmptyListValues(t *testing.T) {
	validityBitmap := make([]byte, 8)
	io := ValidityBitmapInputOutput{
		ReadUpperBound:  64,
		Read:            1,
		NullCount:       5,
		ValidBits:       validityBitmap,
		ValidBitsOffset: 1,
	}

	info := LevelInfo{
		RepeatedAncestorDefLevel: 1,
		DefLevel:                 2,
		RepLevel:                 1,
	}

	defLevels := []int16{0, 0, 0, 2, 2, 1, 0, 2}
	DefLevelsToBitmap(defLevels, info, &io)

	assert.Equal(t, bitmapToString(validityBitmap, 8), "01101000")
	for _, x := range validityBitmap[1:] {
		assert.Zero(t, x)
	}
	assert.EqualValues(t, 6, io.NullCount)
	assert.EqualValues(t, 4, io.Read)
}

type MultiLevelTestData struct {
	defLevels []int16
	repLevels []int16
}

func TriplNestedList() MultiLevelTestData {
	// Triply nested list values borrow from write_path
	// [null, [[1, null, 3], []], []],
	// [[[]], [[], [1, 2]], null, [[3]]],
	// null,
	// []
	return MultiLevelTestData{
		defLevels: []int16{2, 7, 6, 7, 5, 3, // first row
			5, 5, 7, 7, 2, 7, // second row
			0, // third row
			1},
		repLevels: []int16{0, 1, 3, 3, 2, 1, // first row
			0, 1, 2, 3, 1, 1, // second row
			0, 0},
	}
}

func TestActualCase(t *testing.T) {
	out := make([]byte, 512)
	defs := make([]int16, 64)
	for i := range defs {
		defs[i] = 3
	}

	defs[0] = 0
	defs[25] = 0
	defs[33] = 0
	defs[49] = 0
	defs[58] = 0
	defs[59] = 0
	defs[60] = 0
	defs[61] = 0

	remaining := int64(4096)
	info := LevelInfo{
		NullSlotUsage:            0,
		DefLevel:                 3,
		RepLevel:                 1,
		RepeatedAncestorDefLevel: 2,
	}

	wr := utils.NewFirstTimeBitmapWriter(out, 0, 4096)
	v := defLevelsBatchToBitmap(defs, remaining, info, wr, true)
	assert.EqualValues(t, 56, v)
	assert.Equal(t, []byte{255, 255, 255, 255}, out[:4])
}

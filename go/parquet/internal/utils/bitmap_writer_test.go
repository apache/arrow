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

package utils_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/parquet/internal/utils"
	"github.com/stretchr/testify/suite"
)

func writeSliceToWriter(wr utils.BitmapWriter, values []int) {
	for _, v := range values {
		if v != 0 {
			wr.Set()
		} else {
			wr.Clear()
		}
		wr.Next()
	}
	wr.Finish()
}

type FirstTimeBitmapWriterSuite struct {
	suite.Suite
}

func (f *FirstTimeBitmapWriterSuite) TestNormalOperation() {
	for _, fb := range []byte{0x00, 0xFF} {
		{
			bitmap := []byte{fb, fb, fb, fb}
			wr := utils.NewFirstTimeBitmapWriter(bitmap, 0, 12)
			writeSliceToWriter(wr, []int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
			// {0b00110110, 0b1010, 0, 0}
			f.Equal([]byte{0x36, 0x0a}, bitmap[:2])
		}
		{
			bitmap := []byte{fb, fb, fb, fb}
			wr := utils.NewFirstTimeBitmapWriter(bitmap, 4, 12)
			writeSliceToWriter(wr, []int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
			// {0b00110110, 0b1010, 0, 0}
			f.Equal([]byte{0x60 | (fb & 0x0f), 0xa3}, bitmap[:2])
		}
		// Consecutive write chunks
		{
			bitmap := []byte{fb, fb, fb, fb}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 0, 6)
				writeSliceToWriter(wr, []int{0, 1, 1, 0, 1, 1})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 6, 3)
				writeSliceToWriter(wr, []int{0, 0, 0})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 9, 3)
				writeSliceToWriter(wr, []int{1, 0, 1})
			}
			f.Equal([]byte{0x36, 0x0a}, bitmap[:2])
		}
		{
			bitmap := []byte{fb, fb, fb, fb}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 4, 0)
				writeSliceToWriter(wr, []int{})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 4, 6)
				writeSliceToWriter(wr, []int{0, 1, 1, 0, 1, 1})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 10, 3)
				writeSliceToWriter(wr, []int{0, 0, 0})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 13, 0)
				writeSliceToWriter(wr, []int{})
			}
			{
				wr := utils.NewFirstTimeBitmapWriter(bitmap, 13, 3)
				writeSliceToWriter(wr, []int{1, 0, 1})
			}
			f.Equal([]byte{0x60 | (fb & 0x0f), 0xa3}, bitmap[:2])
		}
	}
}

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

func (f *FirstTimeBitmapWriterSuite) TestAppendWordOffsetOverwritesCorrectBits() {
	check := func(start byte, expectedBits string, offset int64) {
		validBits := []byte{start}
		const bitsAfterAppend = 8
		wr := utils.NewFirstTimeBitmapWriter(validBits, offset, int64(8*len(validBits))-offset)
		wr.AppendWord(0xFF, bitsAfterAppend-offset)
		wr.Finish()
		f.Equal(expectedBits, bitmapToString(validBits, bitsAfterAppend))
	}

	f.Run("CheckAppend", func() {
		tests := []struct {
			expectedBits string
			offset       int64
		}{
			{"11111111", 0},
			{"01111111", 1},
			{"00111111", 2},
			{"00011111", 3},
			{"00001111", 4},
			{"00000111", 5},
			{"00000011", 6},
			{"00000001", 7},
		}
		for _, tt := range tests {
			f.Run(tt.expectedBits, func() { check(0x00, tt.expectedBits, tt.offset) })
		}
	})

	f.Run("CheckWithSet", func() {
		tests := []struct {
			expectedBits string
			offset       int64
		}{
			{"11111111", 1},
			{"10111111", 2},
			{"10011111", 3},
			{"10001111", 4},
			{"10000111", 5},
			{"10000011", 6},
			{"10000001", 7},
		}
		for _, tt := range tests {
			f.Run(tt.expectedBits, func() { check(0x1, tt.expectedBits, tt.offset) })
		}
	})

	f.Run("CheckWithPreceding", func() {
		tests := []struct {
			expectedBits string
			offset       int64
		}{
			{"11111111", 0},
			{"11111111", 1},
			{"11111111", 2},
			{"11111111", 3},
			{"11111111", 4},
			{"11111111", 5},
			{"11111111", 6},
			{"11111111", 7},
		}
		for _, tt := range tests {
			f.Run(fmt.Sprintf("%d", tt.offset), func() { check(0xFF, tt.expectedBits, tt.offset) })
		}
	})
}

func (f *FirstTimeBitmapWriterSuite) TestAppendZeroBitsNoImpact() {
	validBits := []byte{0x00}
	wr := utils.NewFirstTimeBitmapWriter(validBits, 1, int64(len(validBits)*8))
	wr.AppendWord(0xFF, 0)
	wr.AppendWord(0xFF, 0)
	wr.AppendWord(0x01, 1)
	wr.Finish()
	f.Equal(uint8(0x2), validBits[0])
}

func (f *FirstTimeBitmapWriterSuite) TestAppendLessThanByte() {
	{
		validBits := make([]byte, 8)
		wr := utils.NewFirstTimeBitmapWriter(validBits, 1, 8)
		wr.AppendWord(0xB, 4)
		wr.Finish()
		f.Equal("01101000", bitmapToString(validBits, 8))
	}
	{
		// test with all bits initially set
		validBits := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		wr := utils.NewFirstTimeBitmapWriter(validBits, 1, 8)
		wr.AppendWord(0xB, 4)
		wr.Finish()
		f.Equal("11101000", bitmapToString(validBits, 8))
	}
}

func (f *FirstTimeBitmapWriterSuite) TestAppendByteThenMore() {
	{
		validBits := make([]byte, 8)
		wr := utils.NewFirstTimeBitmapWriter(validBits, 0, 9)
		wr.AppendWord(0xC3, 8)
		wr.AppendWord(0x01, 1)
		wr.Finish()
		f.Equal("110000111", bitmapToString(validBits, 9))
	}
	{
		// test with all bits initially set
		validBits := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		wr := utils.NewFirstTimeBitmapWriter(validBits, 0, 9)
		wr.AppendWord(0xC3, 8)
		wr.AppendWord(0x01, 1)
		wr.Finish()
		f.Equal("110000111", bitmapToString(validBits, 9))
	}
}

func (f *FirstTimeBitmapWriterSuite) TestAppendWordShiftBitsCorrectly() {
	const pattern = 0x9A9A9A9A9A9A9A9A

	tests := []struct {
		leadingBits      string
		middleBits       string
		trailingBits     string
		offset           int64
		presetBufferBits bool
	}{
		{"01011001", "01011001", "00000000", 8, false},
		{"00101100", "10101100", "10000000", 9, false},
		{"00010110", "01010110", "01000000", 10, false},
		{"00001011", "00101011", "00100000", 11, false},
		{"00000101", "10010101", "10010000", 12, false},
		{"00000010", "11001010", "11001000", 13, false},
		{"00000001", "01100101", "01100100", 14, false},
		{"00000000", "10110010", "10110010", 15, false},
		{"01011001", "01011001", "11111111", 8, true},
		{"10101100", "10101100", "10000000", 9, true},
		{"11010110", "01010110", "01000000", 10, true},
		{"11101011", "00101011", "00100000", 11, true},
		{"11110101", "10010101", "10010000", 12, true},
		{"11111010", "11001010", "11001000", 13, true},
		{"11111101", "01100101", "01100100", 14, true},
		{"11111110", "10110010", "10110010", 15, true},
	}
	for _, tt := range tests {
		f.Run(tt.leadingBits, func() {
			f.Require().GreaterOrEqual(tt.offset, int64(8))
			validBits := make([]byte, 10)
			if tt.presetBufferBits {
				for idx := range validBits {
					validBits[idx] = 0xFF
				}
			}

			validBits[0] = 0x99
			wr := utils.NewFirstTimeBitmapWriter(validBits, tt.offset, (9*int64(reflect.TypeOf(uint64(0)).Size()))-tt.offset)
			wr.AppendWord(pattern, 64)
			wr.Finish()
			f.Equal(uint8(0x99), validBits[0])
			f.Equal(tt.leadingBits, bitmapToString(validBits[1:], 8))
			for x := 2; x < 9; x++ {
				f.Equal(tt.middleBits, bitmapToString(validBits[x:], 8))
			}
			f.Equal(tt.trailingBits, bitmapToString(validBits[9:], 8))
		})
	}
}

func (f *FirstTimeBitmapWriterSuite) TestAppendWordOnlyAppropriateBytesWritten() {
	validBits := []byte{0x00, 0x00}
	bitmap := uint64(0x1FF)
	{
		wr := utils.NewFirstTimeBitmapWriter(validBits, 1, int64(8*len(validBits))-1)
		wr.AppendWord(bitmap, 7)
		wr.Finish()
		f.Equal([]byte{0xFE, 0x00}, validBits)
	}
	{
		wr := utils.NewFirstTimeBitmapWriter(validBits, 1, int64(8*len(validBits)-1))
		wr.AppendWord(bitmap, 8)
		wr.Finish()
		f.Equal([]byte{0xFE, 0x03}, validBits)
	}
}

func TestFirstTimeBitmapWriter(t *testing.T) {
	suite.Run(t, new(FirstTimeBitmapWriterSuite))
}

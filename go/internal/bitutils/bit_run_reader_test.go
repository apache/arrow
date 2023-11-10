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

package bitutils_test

import (
	"math/bits"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/endian"
	"github.com/apache/arrow/go/v14/internal/bitutils"
	"github.com/stretchr/testify/assert"
)

var toLittleEndian func(uint64) uint64

func init() {
	if endian.IsBigEndian {
		toLittleEndian = bits.ReverseBytes64
	} else {
		toLittleEndian = func(in uint64) uint64 { return in }
	}
}

func TestBitRunReaderZeroLength(t *testing.T) {
	reader := bitutils.NewBitRunReader(nil, 0, 0)
	assert.Zero(t, reader.NextRun().Len)
}

func bitmapFromSlice(vals []int, bitOffset int64) []byte {
	out := make([]byte, int(bitutil.BytesForBits(int64(len(vals))+bitOffset)))
	writer := bitutil.NewBitmapWriter(out, int(bitOffset), len(vals))
	for _, val := range vals {
		if val == 1 {
			writer.Set()
		} else {
			writer.Clear()
		}
		writer.Next()
	}
	writer.Finish()

	return out
}

func TestBitRunReader(t *testing.T) {
	tests := []struct {
		name     string
		val      []int
		bmvec    []int
		offset   int64
		len      int64
		expected []bitutils.BitRun
	}{
		{"normal operation",
			[]int{5, 0, 7, 1, 3, 0, 25, 1, 21, 0, 26, 1, 130, 0, 65, 1},
			[]int{1, 0, 1},
			0, -1,
			[]bitutils.BitRun{
				{1, true},
				{1, false},
				{1, true},
				{5, false},
				{7, true},
				{3, false},
				{25, true},
				{21, false},
				{26, true},
				{130, false},
				{65, true},
			},
		},
		{"truncated at word", []int{7, 1, 58, 0}, []int{}, 1, 63,
			[]bitutils.BitRun{{6, true}, {57, false}},
		},
		{"truncated within word multiple of 8 bits",
			[]int{7, 1, 5, 0}, []int{}, 1, 7,
			[]bitutils.BitRun{{6, true}, {1, false}},
		},
		{"truncated within word", []int{37 + 40, 0, 23, 1}, []int{}, 37, 53,
			[]bitutils.BitRun{{40, false}, {13, true}},
		},
		{"truncated multiple words", []int{5, 0, 30, 1, 95, 0}, []int{1, 0, 1},
			5, (3 + 5 + 30 + 95) - (5 + 3), []bitutils.BitRun{{3, false}, {30, true}, {92, false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bmvec := tt.bmvec

			for i := 0; i < len(tt.val); i += 2 {
				for j := 0; j < tt.val[i]; j++ {
					bmvec = append(bmvec, tt.val[i+1])
				}
			}

			bitmap := bitmapFromSlice(bmvec, 0)
			length := int64(len(bmvec)) - tt.offset
			if tt.len != -1 {
				length = tt.len
			}
			reader := bitutils.NewBitRunReader(bitmap, tt.offset, length)

			results := make([]bitutils.BitRun, 0)
			for {
				results = append(results, reader.NextRun())
				if results[len(results)-1].Len == 0 {
					break
				}
			}
			assert.Zero(t, results[len(results)-1].Len)
			results = results[:len(results)-1]

			assert.Equal(t, tt.expected, results)
		})
	}
}

func TestBitRunReaderAllFirstByteCombos(t *testing.T) {
	for offset := int64(0); offset < 8; offset++ {
		for x := int64(0); x < (1<<8)-1; x++ {
			bits := int64(toLittleEndian(uint64(x)))
			reader := bitutils.NewBitRunReader((*(*[8]byte)(unsafe.Pointer(&bits)))[:], offset, 8-offset)

			results := make([]bitutils.BitRun, 0)
			for {
				results = append(results, reader.NextRun())
				if results[len(results)-1].Len == 0 {
					break
				}
			}
			assert.Zero(t, results[len(results)-1].Len)
			results = results[:len(results)-1]

			var sum int64
			for _, r := range results {
				sum += r.Len
			}
			assert.EqualValues(t, sum, 8-offset)
		}
	}
}

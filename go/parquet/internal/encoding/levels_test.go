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

package encoding_test

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/encoding"
	"github.com/stretchr/testify/assert"
)

func generateLevels(minRepeat, maxRepeat int, maxLevel int16) []int16 {
	// for each repetition count up to max repeat
	ret := make([]int16, 0)
	for rep := minRepeat; rep <= maxRepeat; rep++ {
		var (
			repCount       = 1 << rep
			val      int16 = 0
			bwidth         = 0
		)
		// generate levels for repetition count up to max level
		for val <= maxLevel {
			for i := 0; i < repCount; i++ {
				ret = append(ret, val)
			}
			val = int16((2 << bwidth) - 1)
			bwidth++
		}
	}
	return ret
}

func encodeLevels(t *testing.T, enc parquet.Encoding, maxLvl int16, numLevels int, input []int16) []byte {
	var (
		encoder  encoding.LevelEncoder
		lvlCount = 0
		buf      = encoding.NewBufferWriter(2*numLevels, memory.DefaultAllocator)
	)

	if enc == parquet.Encodings.RLE {
		buf.SetOffset(arrow.Int32SizeBytes)
		// leave space to write the rle length value
		encoder.Init(enc, maxLvl, buf)
		lvlCount, _ = encoder.Encode(input)
		buf.SetOffset(0)
		arrow.Int32Traits.CastFromBytes(buf.Bytes())[0] = utils.ToLEInt32(int32(encoder.Len()))
	} else {
		encoder.Init(enc, maxLvl, buf)
		lvlCount, _ = encoder.Encode(input)
	}

	assert.Equal(t, numLevels, lvlCount)
	return buf.Bytes()
}

func verifyDecodingLvls(t *testing.T, enc parquet.Encoding, maxLvl int16, input []int16, buf []byte) {
	var (
		decoder        encoding.LevelDecoder
		lvlCount       = 0
		numLevels      = len(input)
		output         = make([]int16, numLevels)
		decodeCount    = 4
		numInnerLevels = numLevels / decodeCount
	)

	// decode levels and test with multiple decode calls
	_, err := decoder.SetData(enc, maxLvl, numLevels, buf)
	assert.NoError(t, err)
	// try multiple decoding on a single setdata call
	for ct := 0; ct < decodeCount; ct++ {
		offset := ct * numInnerLevels
		lvlCount, _ = decoder.Decode(output[:numInnerLevels])
		assert.Equal(t, numInnerLevels, lvlCount)
		assert.Equal(t, input[offset:offset+numInnerLevels], output[:numInnerLevels])
	}

	// check the remaining levels
	var (
		levelsCompleted = decodeCount * (numLevels / decodeCount)
		remaining       = numLevels - levelsCompleted
	)

	if remaining > 0 {
		lvlCount, _ = decoder.Decode(output[:remaining])
		assert.Equal(t, remaining, lvlCount)
		assert.Equal(t, input[levelsCompleted:], output[:remaining])
	}
	// test decode zero values
	lvlCount, _ = decoder.Decode(output[:1])
	assert.Zero(t, lvlCount)
}

func verifyDecodingMultipleSetData(t *testing.T, enc parquet.Encoding, max int16, input []int16, buf [][]byte) {
	var (
		decoder      encoding.LevelDecoder
		lvlCount     = 0
		setdataCount = len(buf)
		numLevels    = len(input) / setdataCount
		output       = make([]int16, numLevels)
	)

	for ct := 0; ct < setdataCount; ct++ {
		offset := ct * numLevels
		assert.Len(t, output, numLevels)
		_, err := decoder.SetData(enc, max, numLevels, buf[ct])
		assert.NoError(t, err)
		lvlCount, _ = decoder.Decode(output)
		assert.Equal(t, numLevels, lvlCount)
		assert.Equal(t, input[offset:offset+numLevels], output)
	}
}

func TestLevelsDecodeMultipleBitWidth(t *testing.T) {
	t.Parallel()
	// Test levels with maximum bit-width from 1 to 8
	// increase the repetition count for each iteration by a factor of 2
	var (
		minRepeat   = 0
		maxRepeat   = 7 // 128
		maxBitWidth = 8
		input       []int16
		buf         []byte
		encodings   = [2]parquet.Encoding{parquet.Encodings.RLE, parquet.Encodings.BitPacked}
	)

	for _, enc := range encodings {
		t.Run(enc.String(), func(t *testing.T) {
			// bitpacked requires a sequence of at least 8
			if enc == parquet.Encodings.BitPacked {
				minRepeat = 3
			}
			// for each max bit width
			for bitWidth := 1; bitWidth <= maxBitWidth; bitWidth++ {
				t.Run(strconv.Itoa(bitWidth), func(t *testing.T) {
					max := int16((1 << bitWidth) - 1)
					// generate levels
					input = generateLevels(minRepeat, maxRepeat, max)
					assert.NotPanics(t, func() {
						buf = encodeLevels(t, enc, max, len(input), input)
					})
					assert.NotPanics(t, func() {
						verifyDecodingLvls(t, enc, max, input, buf)
					})
				})
			}
		})
	}
}

func TestLevelsDecodeMultipleSetData(t *testing.T) {
	t.Parallel()

	var (
		minRepeat = 3
		maxRepeat = 7
		bitWidth  = 8
		maxLevel  = int16((1 << bitWidth) - 1)
		encodings = [2]parquet.Encoding{parquet.Encodings.RLE, parquet.Encodings.BitPacked}
	)

	input := generateLevels(minRepeat, maxRepeat, maxLevel)

	var (
		numLevels      = len(input)
		setdataFactor  = 8
		splitLevelSize = numLevels / setdataFactor
		buf            = make([][]byte, setdataFactor)
	)

	for _, enc := range encodings {
		t.Run(enc.String(), func(t *testing.T) {
			for rf := 0; rf < setdataFactor; rf++ {
				offset := rf * splitLevelSize
				assert.NotPanics(t, func() {
					buf[rf] = encodeLevels(t, enc, maxLevel, splitLevelSize, input[offset:offset+splitLevelSize])
				})
			}
			assert.NotPanics(t, func() {
				verifyDecodingMultipleSetData(t, enc, maxLevel, input, buf)
			})
		})
	}
}

func TestMinimumBufferSize(t *testing.T) {
	t.Parallel()

	const numToEncode = 1024
	levels := make([]int16, numToEncode)

	for idx := range levels {
		if idx%9 == 0 {
			levels[idx] = 0
		} else {
			levels[idx] = 1
		}
	}

	output := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	var encoder encoding.LevelEncoder
	encoder.Init(parquet.Encodings.RLE, 1, output)
	count, _ := encoder.Encode(levels)
	assert.Equal(t, numToEncode, count)
}

func TestMinimumBufferSize2(t *testing.T) {
	t.Parallel()

	// test the worst case for bit_width=2 consisting of
	// LiteralRun(size=8)
	// RepeatedRun(size=8)
	// LiteralRun(size=8)
	// ...
	const numToEncode = 1024
	levels := make([]int16, numToEncode)

	for idx := range levels {
		// This forces a literal run of 00000001
		// followed by eight 1s
		if (idx % 16) < 7 {
			levels[idx] = 0
		} else {
			levels[idx] = 1
		}
	}

	for bitWidth := int16(1); bitWidth <= 8; bitWidth++ {
		output := encoding.NewBufferWriter(0, memory.DefaultAllocator)

		var encoder encoding.LevelEncoder
		encoder.Init(parquet.Encodings.RLE, bitWidth, output)
		count, _ := encoder.Encode(levels)
		assert.Equal(t, numToEncode, count)
	}
}

func TestEncodeDecodeLevels(t *testing.T) {
	t.Parallel()
	const numToEncode = 2048
	levels := make([]int16, numToEncode)
	numones := 0
	for idx := range levels {
		if (idx % 16) < 7 {
			levels[idx] = 0
		} else {
			levels[idx] = 1
			numones++
		}
	}

	output := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	var encoder encoding.LevelEncoder
	encoder.Init(parquet.Encodings.RLE, 1, output)
	count, _ := encoder.Encode(levels)
	assert.Equal(t, numToEncode, count)
	encoder.Flush()

	buf := output.Bytes()
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(buf)))

	var decoder encoding.LevelDecoder
	_, err := decoder.SetData(parquet.Encodings.RLE, 1, numToEncode, append(prefix[:], buf...))
	assert.NoError(t, err)

	var levelOut [numToEncode]int16
	total, vals := decoder.Decode(levelOut[:])
	assert.EqualValues(t, numToEncode, total)
	assert.EqualValues(t, numones, vals)
	assert.Equal(t, levels, levelOut[:])
}

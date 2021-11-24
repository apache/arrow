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

package utils

import (
	"math"
	"math/bits"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow/bitutil"
)

func loadWord(byt []byte) uint64 {
	return ToLEUint64(*(*uint64)(unsafe.Pointer(&byt[0])))
}

func shiftWord(current, next uint64, shift int64) uint64 {
	if shift == 0 {
		return current
	}
	return (current >> shift) | (next << (64 - shift))
}

// BitBlockCount is returned by the various bit block counter utilities
// in order to return a length of bits and the population count of that
// slice of bits.
type BitBlockCount struct {
	Len    int16
	Popcnt int16
}

// NoneSet returns true if ALL the bits were 0 in this set, ie: Popcnt == 0
func (b BitBlockCount) NoneSet() bool {
	return b.Popcnt == 0
}

// AllSet returns true if ALL the bits were 1 in this set, ie: Popcnt == Len
func (b BitBlockCount) AllSet() bool {
	return b.Len == b.Popcnt
}

// BitBlockCounter is a utility for grabbing chunks of a bitmap at a time and efficiently
// counting the number of bits which are 1.
type BitBlockCounter struct {
	bitmap        []byte
	bitsRemaining int64
	bitOffset     int8
}

const (
	wordBits      int64 = 64
	fourWordsBits int64 = wordBits * 4
)

// NewBitBlockCounter returns a BitBlockCounter for the passed bitmap starting at startOffset
// of length nbits.
func NewBitBlockCounter(bitmap []byte, startOffset, nbits int64) *BitBlockCounter {
	return &BitBlockCounter{
		bitmap:        bitmap[startOffset/8:],
		bitsRemaining: nbits,
		bitOffset:     int8(startOffset % 8),
	}
}

// getBlockSlow is for returning a block of the requested size when there aren't
// enough bits remaining to do a full word computation.
func (b *BitBlockCounter) getBlockSlow(blockSize int64) BitBlockCount {
	runlen := int16(Min(b.bitsRemaining, blockSize))
	popcnt := int16(bitutil.CountSetBits(b.bitmap, int(b.bitOffset), int(runlen)))
	b.bitsRemaining -= int64(runlen)
	b.bitmap = b.bitmap[runlen/8:]
	return BitBlockCount{runlen, popcnt}
}

// NextFourWords returns the next run of available bits, usually 256. The
// returned pair contains the size of run and the number of true values.
// The last block will have a length less than 256 if the bitmap length
// is not a multiple of 256, and will return 0-length blocks in subsequent
// invocations.
func (b *BitBlockCounter) NextFourWords() BitBlockCount {
	if b.bitsRemaining == 0 {
		return BitBlockCount{0, 0}
	}

	totalPopcnt := 0
	if b.bitOffset == 0 {
		// if we're aligned at 0 bitoffset, then we can easily just jump from
		// word to word nice and easy.
		if b.bitsRemaining < fourWordsBits {
			return b.getBlockSlow(fourWordsBits)
		}
		totalPopcnt += bits.OnesCount64(loadWord(b.bitmap))
		totalPopcnt += bits.OnesCount64(loadWord(b.bitmap[8:]))
		totalPopcnt += bits.OnesCount64(loadWord(b.bitmap[16:]))
		totalPopcnt += bits.OnesCount64(loadWord(b.bitmap[24:]))
	} else {
		// When the offset is > 0, we need there to be a word beyond the last
		// aligned word in the bitmap for the bit shifting logic.
		if b.bitsRemaining < 5*fourWordsBits-int64(b.bitOffset) {
			return b.getBlockSlow(fourWordsBits)
		}

		current := loadWord(b.bitmap)
		next := loadWord(b.bitmap[8:])
		totalPopcnt += bits.OnesCount64(shiftWord(current, next, int64(b.bitOffset)))

		current = next
		next = loadWord(b.bitmap[16:])
		totalPopcnt += bits.OnesCount64(shiftWord(current, next, int64(b.bitOffset)))

		current = next
		next = loadWord(b.bitmap[24:])
		totalPopcnt += bits.OnesCount64(shiftWord(current, next, int64(b.bitOffset)))

		current = next
		next = loadWord(b.bitmap[32:])
		totalPopcnt += bits.OnesCount64(shiftWord(current, next, int64(b.bitOffset)))
	}
	b.bitmap = b.bitmap[bitutil.BytesForBits(fourWordsBits):]
	b.bitsRemaining -= fourWordsBits
	return BitBlockCount{256, int16(totalPopcnt)}
}

// NextWord returns the next run of available bits, usually 64. The returned
// pair contains the size of run and the number of true values. The last
// block will have a length less than 64 if the bitmap length is not a
// multiple of 64, and will return 0-length blocks in subsequent
// invocations.
func (b *BitBlockCounter) NextWord() BitBlockCount {
	if b.bitsRemaining == 0 {
		return BitBlockCount{0, 0}
	}
	popcnt := 0
	if b.bitOffset == 0 {
		if b.bitsRemaining < wordBits {
			return b.getBlockSlow(wordBits)
		}
		popcnt = bits.OnesCount64(loadWord(b.bitmap))
	} else {
		// When the offset is > 0, we need there to be a word beyond the last
		// aligned word in the bitmap for the bit shifting logic.
		if b.bitsRemaining < (2*wordBits - int64(b.bitOffset)) {
			return b.getBlockSlow(wordBits)
		}
		popcnt = bits.OnesCount64(shiftWord(loadWord(b.bitmap), loadWord(b.bitmap[8:]), int64(b.bitOffset)))
	}
	b.bitmap = b.bitmap[wordBits/8:]
	b.bitsRemaining -= wordBits
	return BitBlockCount{64, int16(popcnt)}
}

// OptionalBitBlockCounter is a useful counter to iterate through a possibly
// non-existent validity bitmap to allow us to write one code path for both
// the with-nulls and no-nulls cases without giving up a lot of performance.
type OptionalBitBlockCounter struct {
	hasBitmap bool
	pos       int64
	len       int64
	counter   *BitBlockCounter
}

// NewOptionalBitBlockCounter constructs and returns a new bit block counter that
// can properly handle the case when a bitmap is null, if it is guaranteed that the
// the bitmap is not nil, then prefer NewBitBlockCounter here.
func NewOptionalBitBlockCounter(bitmap []byte, offset, length int64) *OptionalBitBlockCounter {
	var counter *BitBlockCounter
	if bitmap != nil {
		counter = NewBitBlockCounter(bitmap, offset, length)
	}
	return &OptionalBitBlockCounter{
		hasBitmap: bitmap != nil,
		pos:       0,
		len:       length,
		counter:   counter,
	}
}

// NextBlock returns block count for next word when the bitmap is available otherwise
// return a block with length up to INT16_MAX when there is no validity
// bitmap (so all the referenced values are not null).
func (obc *OptionalBitBlockCounter) NextBlock() BitBlockCount {
	const maxBlockSize = math.MaxInt16
	if obc.hasBitmap {
		block := obc.counter.NextWord()
		obc.pos += int64(block.Len)
		return block
	}

	blockSize := int16(Min(maxBlockSize, obc.len-obc.pos))
	obc.pos += int64(blockSize)
	// all values are non-null
	return BitBlockCount{blockSize, blockSize}
}

// NextWord is like NextBlock, but returns a word-sized block even when there is no
// validity bitmap
func (obc *OptionalBitBlockCounter) NextWord() BitBlockCount {
	const wordsize = 64
	if obc.hasBitmap {
		block := obc.counter.NextWord()
		obc.pos += int64(block.Len)
		return block
	}
	blockSize := int16(Min(wordsize, obc.len-obc.pos))
	obc.pos += int64(blockSize)
	// all values are non-null
	return BitBlockCount{blockSize, blockSize}
}

// VisitBitBlocks is a utility for easily iterating through the blocks of bits in a bitmap,
// calling the appropriate visitValid/visitInvalid function as we iterate through the bits.
// visitValid is called with the bitoffset of the valid bit. Don't use this inside a tight
// loop when performance is needed and instead prefer manually constructing these loops
// in that scenario.
func VisitBitBlocks(bitmap []byte, offset, length int64, visitValid func(pos int64), visitInvalid func()) {
	counter := NewOptionalBitBlockCounter(bitmap, offset, length)
	pos := int64(0)
	for pos < length {
		block := counter.NextBlock()
		if block.AllSet() {
			for i := 0; i < int(block.Len); i, pos = i+1, pos+1 {
				visitValid(pos)
			}
		} else if block.NoneSet() {
			for i := 0; i < int(block.Len); i, pos = i+1, pos+1 {
				visitInvalid()
			}
		} else {
			for i := 0; i < int(block.Len); i, pos = i+1, pos+1 {
				if bitutil.BitIsSet(bitmap, int(offset+pos)) {
					visitValid(pos)
				} else {
					visitInvalid()
				}
			}
		}
	}
}

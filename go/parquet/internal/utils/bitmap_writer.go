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
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

var (
	// PrecedingBitmask is a convenience set of values as bitmasks for checking
	// prefix bits of a byte
	PrecedingBitmask = [8]byte{0, 1, 3, 7, 15, 31, 63, 127}
	// TrailingBitmask is the bitwise complement version of kPrecedingBitmask
	TrailingBitmask = [8]byte{255, 254, 252, 248, 240, 224, 192, 128}
)

// SetBitsTo is a convenience function to quickly set or unset all the bits
// in a bitmap starting at startOffset for length bits.
func SetBitsTo(bits []byte, startOffset, length int64, areSet bool) {
	if length == 0 {
		return
	}

	beg := startOffset
	end := startOffset + length
	var fill uint8 = 0
	if areSet {
		fill = math.MaxUint8
	}

	byteBeg := beg / 8
	byteEnd := end/8 + 1

	// don't modify bits before the startOffset by using this mask
	firstByteMask := PrecedingBitmask[beg%8]
	// don't modify bits past the length by using this mask
	lastByteMask := TrailingBitmask[end%8]

	if byteEnd == byteBeg+1 {
		// set bits within a single byte
		onlyByteMask := firstByteMask
		if end%8 != 0 {
			onlyByteMask = firstByteMask | lastByteMask
		}

		bits[byteBeg] &= onlyByteMask
		bits[byteBeg] |= fill &^ onlyByteMask
		return
	}

	// set/clear trailing bits of first byte
	bits[byteBeg] &= firstByteMask
	bits[byteBeg] |= fill &^ firstByteMask

	if byteEnd-byteBeg > 2 {
		memory.Set(bits[byteBeg+1:byteEnd-1], fill)
	}

	if end%8 == 0 {
		return
	}

	bits[byteEnd-1] &= lastByteMask
	bits[byteEnd-1] |= fill &^ lastByteMask
}

// BitmapWriter is an interface for bitmap writers so that we can use multiple
// implementations or swap if necessary.
type BitmapWriter interface {
	// Set sets the current bit that will be written
	Set()
	// Clear clears the current bit that will be written
	Clear()
	// Next advances to the next bit for the writer
	Next()
	// Finish flushes the current byte out to the bitmap slice
	Finish()
	// AppendWord takes nbits from word which should be an LSB bitmap and appends them to the bitmap.
	AppendWord(word uint64, nbits int64)
	// AppendBools appends the bit representation of the bools slice, returning the number
	// of bools that were able to fit in the remaining length of the bitmapwriter.
	AppendBools(in []bool) int
	// Pos is the current position that will be written next
	Pos() int64
	// Reset allows reusing the bitmapwriter by resetting Pos to start with length as
	// the number of bits that the writer can write.
	Reset(start, length int64)
}

type bitmapWriter struct {
	buf    []byte
	pos    int64
	length int64

	curByte    uint8
	bitMask    uint8
	byteOffset int64
}

// NewBitmapWriter returns a sequential bitwise writer that preserves surrounding
// bit values as it writes.
func NewBitmapWriter(bitmap []byte, start, length int64) BitmapWriter {
	ret := &bitmapWriter{
		buf:        bitmap,
		length:     length,
		byteOffset: start / 8,
		bitMask:    bitutil.BitMask[start%8],
	}
	if length > 0 {
		ret.curByte = bitmap[int(ret.byteOffset)]
	}
	return ret
}

func (b *bitmapWriter) Reset(start, length int64) {
	b.pos = 0
	b.byteOffset = start / 8
	b.bitMask = bitutil.BitMask[start%8]
	b.length = length
	if b.length > 0 {
		b.curByte = b.buf[int(b.byteOffset)]
	}
}

func (b *bitmapWriter) Pos() int64 { return b.pos }
func (b *bitmapWriter) Set()       { b.curByte |= b.bitMask }
func (b *bitmapWriter) Clear()     { b.curByte &= ^b.bitMask }

func (b *bitmapWriter) Next() {
	b.bitMask = b.bitMask << 1
	b.pos++
	if b.bitMask == 0 {
		b.bitMask = 0x01
		b.buf[b.byteOffset] = b.curByte
		b.byteOffset++
		if b.pos < b.length {
			b.curByte = b.buf[int(b.byteOffset)]
		}
	}
}

func (b *bitmapWriter) AppendBools(in []bool) int {
	space := Min(bitutil.BytesForBits(b.length-b.pos), int64(len(in)))

	// location that the first byte needs to be written to for appending
	appslice := b.buf[int(b.byteOffset):]
	// update everything but curByte
	bitOffset := bits.TrailingZeros32(uint32(b.bitMask))
	appslice[0] = b.curByte
	for i, b := range in[:space] {
		if b {
			bitutil.SetBit(appslice, i)
		} else {
			bitutil.ClearBit(appslice, i)
		}
	}

	b.pos += space
	b.bitMask = bitutil.BitMask[(int64(bitOffset)+space)%8]
	b.byteOffset += (int64(bitOffset) + space) / 8
	b.curByte = appslice[len(appslice)-1]

	return int(space)
}

func (b *bitmapWriter) Finish() {
	if b.length > 0 && (b.bitMask != 0x01 || b.pos < b.length) {
		b.buf[int(b.byteOffset)] = b.curByte
	}
}

func (b *bitmapWriter) AppendWord(uint64, int64) {
	panic("AppendWord not implemented")
}

type firstTimeBitmapWriter struct {
	buf    []byte
	pos    int64
	length int64

	curByte    uint8
	bitMask    uint8
	byteOffset int64
}

// NewFirstTimeBitmapWriter creates a bitmap writer that might clobber any bit values
// following the bits written to the bitmap, as such it is faster than the bitmapwriter
// that is created with NewBitmapWriter
func NewFirstTimeBitmapWriter(buf []byte, start, length int64) BitmapWriter {
	ret := &firstTimeBitmapWriter{
		buf:        buf,
		byteOffset: start / 8,
		bitMask:    bitutil.BitMask[start%8],
		length:     length,
	}
	if length > 0 {
		ret.curByte = ret.buf[int(ret.byteOffset)] & PrecedingBitmask[start%8]
	}
	return ret
}

var endianBuffer [8]byte

func (bw *firstTimeBitmapWriter) Reset(start, length int64) {
	bw.pos = 0
	bw.byteOffset = start / 8
	bw.bitMask = bitutil.BitMask[start%8]
	bw.length = length
	if length > 0 {
		bw.curByte = bw.buf[int(bw.byteOffset)] & PrecedingBitmask[start%8]
	}
}

func (bw *firstTimeBitmapWriter) Pos() int64 { return bw.pos }
func (bw *firstTimeBitmapWriter) AppendWord(word uint64, nbits int64) {
	if nbits == 0 {
		return
	}

	// location that the first byte needs to be written to for appending
	appslice := bw.buf[int(bw.byteOffset):]

	// update everything but curByte
	bw.pos += nbits
	bitOffset := bits.TrailingZeros32(uint32(bw.bitMask))
	bw.bitMask = bitutil.BitMask[(int64(bitOffset)+nbits)%8]
	bw.byteOffset += (int64(bitOffset) + nbits) / 8

	if bitOffset != 0 {
		// we're in the middle of the byte. Update the byte and shift bits appropriately
		// so we can just copy the bytes.
		carry := 8 - bitOffset
		// Carry over bits from word to curByte. We assume any extra bits in word are unset
		// so no additional accounting is needed for when nbits < carry
		bw.curByte |= uint8((word & uint64(PrecedingBitmask[carry])) << bitOffset)
		// check everything was transferred to curByte
		if nbits < int64(carry) {
			return
		}
		appslice[0] = bw.curByte
		appslice = appslice[1:]
		// move the carry bits off of word
		word = word >> carry
		nbits -= int64(carry)
	}
	bytesForWord := bitutil.BytesForBits(nbits)
	binary.LittleEndian.PutUint64(endianBuffer[:], word)
	copy(appslice, endianBuffer[:bytesForWord])

	// at this point, the previous curByte has been written, the new curByte
	// is either the last relevant byte in word or cleared if the new position
	// is byte aligned (ie. a fresh byte)
	if bw.bitMask == 0x1 {
		bw.curByte = 0
	} else {
		bw.curByte = appslice[bytesForWord-1]
	}
}

func (bw *firstTimeBitmapWriter) Set() {
	bw.curByte |= bw.bitMask
}

func (bw *firstTimeBitmapWriter) Clear() {}

func (bw *firstTimeBitmapWriter) Next() {
	bw.bitMask = uint8(bw.bitMask << 1)
	bw.pos++
	if bw.bitMask == 0 {
		// byte finished, advance to the next one
		bw.bitMask = 0x1
		bw.buf[int(bw.byteOffset)] = bw.curByte
		bw.byteOffset++
		bw.curByte = 0
	}
}

func (b *firstTimeBitmapWriter) AppendBools(in []bool) int {
	panic("Append Bools not yet implemented for firstTimeBitmapWriter")
}

func (bw *firstTimeBitmapWriter) Finish() {
	// store curByte into the bitmap
	if bw.length > 0 && bw.bitMask != 0x01 || bw.pos < bw.length {
		bw.buf[int(bw.byteOffset)] = bw.curByte
	}
}

func (bw *firstTimeBitmapWriter) Position() int64 { return bw.pos }

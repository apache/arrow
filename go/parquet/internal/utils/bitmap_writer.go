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
	PrecedingBitmask = [8]byte{0, 1, 3, 7, 15, 31, 63, 127}
	// the bitwise complement version of kPrecedingBitmask
	TrailingBitmask = [8]byte{255, 254, 252, 248, 240, 224, 192, 128}
)

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

	firstByteMask := PrecedingBitmask[beg%8]
	lastByteMask := TrailingBitmask[end%8]

	if byteEnd == byteBeg+1 {
		// set bits withina single byte
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

type BitmapWriter interface {
	Set()
	Clear()
	Next()
	Finish()
	AppendWord(uint64, int64)
	Pos() int64
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
func (b *bitmapWriter) Clear()     { b.curByte &= b.bitMask ^ 0xFF }

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

func NewFirstTimeBitmapWriter(buf []byte, start, length int64) *firstTimeBitmapWriter {
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

	appslice := bw.buf[int(bw.byteOffset):]

	bw.pos += nbits
	bitOffset := bits.TrailingZeros32(uint32(bw.bitMask))
	bw.bitMask = bitutil.BitMask[(int64(bitOffset)+nbits)%8]
	bw.byteOffset += (int64(bitOffset) + nbits) / 8

	if bitOffset != 0 {
		carry := 8 - bitOffset
		bw.curByte |= uint8((word & uint64(PrecedingBitmask[carry])) << bitOffset)
		if nbits < int64(carry) {
			return
		}
		appslice[0] = bw.curByte
		appslice = appslice[1:]
		word = word >> carry
		nbits -= int64(carry)
	}
	bytesForWord := bitutil.BytesForBits(nbits)
	binary.LittleEndian.PutUint64(endianBuffer[:], word)
	copy(appslice, endianBuffer[:bytesForWord])

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
		bw.bitMask = 0x1
		bw.buf[int(bw.byteOffset)] = bw.curByte
		bw.byteOffset++
		bw.curByte = 0
	}
}

func (bw *firstTimeBitmapWriter) Finish() {
	if bw.length > 0 && bw.bitMask != 0x01 || bw.pos < bw.length {
		bw.buf[int(bw.byteOffset)] = bw.curByte
	}
}

func (bw *firstTimeBitmapWriter) Position() int64 { return bw.pos }

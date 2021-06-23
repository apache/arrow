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

// BitmapReader is a simple bitmap reader for a byte slice.
type BitmapReader struct {
	bitmap []byte
	pos    int64
	len    int64

	current    byte
	byteOffset int64
	bitOffset  int64
}

// NewBitmapReader creates and returns a new bitmap reader for the given bitmap
func NewBitmapReader(bitmap []byte, offset, length int64) *BitmapReader {
	curbyte := byte(0)
	if length > 0 && bitmap != nil {
		curbyte = bitmap[offset/8]
	}
	return &BitmapReader{
		bitmap:     bitmap,
		byteOffset: offset / 8,
		bitOffset:  offset % 8,
		current:    curbyte,
		len:        length,
	}
}

// Set returns true if the current bit is set
func (b *BitmapReader) Set() bool {
	return (b.current & (1 << b.bitOffset)) != 0
}

// NotSet returns true if the current bit is not set
func (b *BitmapReader) NotSet() bool {
	return (b.current & (1 << b.bitOffset)) == 0
}

// Next advances the reader to the next bit in the bitmap.
func (b *BitmapReader) Next() {
	b.bitOffset++
	b.pos++
	if b.bitOffset == 8 {
		b.bitOffset = 0
		b.byteOffset++
		if b.pos < b.len {
			b.current = b.bitmap[int(b.byteOffset)]
		}
	}
}

// Pos returns the current bit position in the bitmap that the reader is looking at
func (b *BitmapReader) Pos() int64 { return b.pos }

// Len returns the total number of bits in the bitmap
func (b *BitmapReader) Len() int64 { return b.len }

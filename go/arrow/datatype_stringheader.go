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

package arrow

import (
	"bytes"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow/endian"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

const (
	StringHeaderPrefixLen  = 4
	stringHeaderInlineSize = 12
)

func IsStringHeaderInline(length int) bool {
	return length < stringHeaderInlineSize
}

// StringHeader is a variable length string (utf8) or byte slice with
// a 4 byte prefix and inline optimization for small values (12 bytes
// or fewer). This is similar to Go's standard string  but limited by
// a length of Uint32Max and up to the first four bytes of the string
// are copied into the struct. This prefix allows failing comparisons
// early and can reduce CPU cache working set when dealing with short
// strings.
//
// There are two situations:
//
//	Short string   |----|----|--------|
//	                ^    ^    ^
//	                |    |    |
//	              size prefix remaining in-line portion, zero padded
//
//	IO Long String |----|----|----|----|
//	                ^    ^     ^     ^
//	                |    |     |     |
//	              size prefix buffer index and offset to out-of-line portion
//
// Adapted from TU Munich's UmbraDB [1], Velox, DuckDB.
//
// [1]: https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf
type StringHeader struct {
	size uint32
	// the first 4 bytes of this are the prefix for the string
	// if size <= StringHeaderInlineSize, then the entire string
	// is in the data array and is zero padded.
	// if size > StringHeaderInlineSize, the next 8 bytes are 2 uint32
	// values which are the buffer index and offset in that buffer
	// containing the full string.
	data [stringHeaderInlineSize]byte
}

func (sh *StringHeader) IsInline() bool {
	return sh.size <= uint32(stringHeaderInlineSize)
}

func (sh *StringHeader) Len() int { return int(sh.size) }
func (sh *StringHeader) Prefix() [StringHeaderPrefixLen]byte {
	return *(*[4]byte)(unsafe.Pointer(&sh.data))
}

func (sh *StringHeader) BufferIndex() uint32 {
	return endian.Native.Uint32(sh.data[StringHeaderPrefixLen:])
}

func (sh *StringHeader) BufferOffset() uint32 {
	return endian.Native.Uint32(sh.data[StringHeaderPrefixLen+4:])
}

func (sh *StringHeader) InlineBytes() (data []byte) {
	debug.Assert(sh.IsInline(), "calling InlineBytes on non-inline StringHeader")
	return sh.data[:sh.size]
}

func (sh *StringHeader) SetBytes(data []byte) int {
	sh.size = uint32(len(data))
	if sh.IsInline() {
		return copy(sh.data[:], data)
	}
	return copy(sh.data[:4], data)
}

func (sh *StringHeader) SetString(data string) int {
	sh.size = uint32(len(data))
	if sh.IsInline() {
		return copy(sh.data[:], data)
	}
	return copy(sh.data[:4], data)
}

func (sh *StringHeader) SetIndexOffset(bufferIndex, offset uint32) {
	endian.Native.PutUint32(sh.data[StringHeaderPrefixLen:], bufferIndex)
	endian.Native.PutUint32(sh.data[StringHeaderPrefixLen+4:], offset)
}

func (sh *StringHeader) Equals(buffers []*memory.Buffer, other *StringHeader, otherBuffers []*memory.Buffer) bool {
	if sh.sizeAndPrefixAsInt() != other.sizeAndPrefixAsInt() {
		return false
	}

	if sh.IsInline() {
		return sh.inlinedAsInt64() == other.inlinedAsInt64()
	}

	data := buffers[sh.BufferIndex()].Bytes()[sh.BufferOffset() : sh.BufferOffset()+sh.size]
	otherData := otherBuffers[other.BufferIndex()].Bytes()[other.BufferOffset() : other.BufferOffset()+other.size]
	return bytes.Equal(data, otherData)
}

func (sh *StringHeader) inlinedAsInt64() int64 {
	s := unsafe.Slice((*int64)(unsafe.Pointer(sh)), 2)
	return s[1]
}

func (sh *StringHeader) sizeAndPrefixAsInt() int64 {
	s := unsafe.Slice((*int64)(unsafe.Pointer(sh)), 2)
	return s[0]
}

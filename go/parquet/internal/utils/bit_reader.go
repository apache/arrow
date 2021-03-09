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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

var trailingMask [64]uint64

func init() {
	for i := 0; i < 64; i++ {
		trailingMask[i] = (math.MaxUint64 >> (64 - i))
	}
}

func trailingBits(v uint64, bits uint) uint64 {
	if bits >= 64 {
		return v
	}
	return v & trailingMask[bits]
}

type reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

const buflen = 1024

type BitReader struct {
	reader     reader
	buffer     uint64
	byteoffset int64
	bitoffset  uint
	raw        [8]byte

	unpackBuf [buflen]uint32
}

func NewBitReader(r *bytes.Reader) *BitReader {
	return &BitReader{reader: r}
}

func (b *BitReader) CurOffset() int64 {
	return b.byteoffset + bitutil.BytesForBits(int64(b.bitoffset))
}

func (b *BitReader) Reset(r *bytes.Reader) {
	b.reader = r
	b.buffer = 0
	b.byteoffset = 0
	b.bitoffset = 0
}

func (b *BitReader) GetVlqInt() (uint64, bool) {
	tmp, err := binary.ReadUvarint(b)
	if err != nil {
		return 0, false
	}
	return tmp, true
}

func (b *BitReader) GetZigZagVlqInt() (int64, bool) {
	u, ok := b.GetVlqInt()
	if !ok {
		return 0, false
	}

	return int64(u>>1) ^ -int64(u&1), true
}

func (b *BitReader) ReadByte() (byte, error) {
	var tmp byte
	if ok := b.GetAligned(1, &tmp); !ok {
		return 0, errors.New("failed to read byte")
	}

	return tmp, nil
}

func (b *BitReader) GetAligned(nbytes int, v interface{}) bool {
	typBytes := int(reflect.TypeOf(v).Elem().Size())
	if nbytes > typBytes {
		return false
	}

	bread := bitutil.BytesForBits(int64(b.bitoffset))

	b.byteoffset += bread
	n, err := b.reader.ReadAt(b.raw[:nbytes], b.byteoffset)
	if err != nil && err != io.EOF {
		return false
	}
	if n != nbytes {
		return false
	}

	memory.Set(b.raw[n:typBytes], 0)

	switch v := v.(type) {
	case *byte:
		*v = b.raw[0]
	case *uint64:
		*v = binary.LittleEndian.Uint64(b.raw[:typBytes])
	case *uint32:
		*v = binary.LittleEndian.Uint32(b.raw[:typBytes])
	case *uint16:
		*v = binary.LittleEndian.Uint16(b.raw[:typBytes])
	}

	b.byteoffset += int64(nbytes)

	b.bitoffset = 0
	b.fillbuffer()
	return true
}

func (b *BitReader) fillbuffer() error {
	n, err := b.reader.ReadAt(b.raw[:], b.byteoffset)
	if err != nil && n == 0 && err != io.EOF {
		return err
	}
	for i := n; i < 8; i++ {
		b.raw[i] = 0
	}
	b.buffer = binary.LittleEndian.Uint64(b.raw[:])
	return nil
}

func (b *BitReader) next(bits uint) (v uint64, err error) {
	v = trailingBits(b.buffer, b.bitoffset+bits) >> b.bitoffset
	b.bitoffset += bits
	if b.bitoffset >= 64 {
		b.byteoffset += 8
		b.bitoffset -= 64
		if err = b.fillbuffer(); err != nil {
			return 0, err
		}
		v |= trailingBits(b.buffer, b.bitoffset) << (bits - b.bitoffset)
	}
	return
}

func (b *BitReader) GetBatchIndex(bits uint, out []IndexType) (i int, err error) {
	if bits > 32 {
		return 0, errors.New("must be 32 bits or less per read")
	}

	var val uint64

	length := len(out)
	for ; i < length && b.bitoffset != 0; i++ {
		val, err = b.next(bits)
		out[i] = IndexType(val)
		if err != nil {
			return
		}
	}

	b.reader.Seek(b.byteoffset, io.SeekStart)
	if i < length {
		numUnpacked := unpack32(b.reader, (*(*[]uint32)(unsafe.Pointer(&out)))[i:], int(bits))
		i += numUnpacked
		b.byteoffset += int64(numUnpacked * int(bits) / 8)
	}

	b.fillbuffer()
	for ; i < length; i++ {
		val, err = b.next(bits)
		out[i] = IndexType(val)
		if err != nil {
			break
		}
	}
	return
}

func (b *BitReader) GetBatchBools(out []bool) (int, error) {
	bits := uint(1)
	length := len(out)

	i := 0
	for ; i < length && b.bitoffset != 0; i++ {
		val, err := b.next(bits)
		out[i] = val != 0
		if err != nil {
			return i, err
		}
	}

	b.reader.Seek(b.byteoffset, io.SeekStart)
	buf := arrow.Uint32Traits.CastToBytes(b.unpackBuf[:])
	blen := buflen * 8
	for i < length {
		unpackSize := MinInt(blen, length-i) / 8 * 8
		n, err := b.reader.Read(buf[:bitutil.BytesForBits(int64(unpackSize))])
		if err != nil {
			return i, err
		}
		BytesToBools(buf[:n], out[i:])
		i += unpackSize
		b.byteoffset += int64(n)
	}

	b.fillbuffer()
	for ; i < length; i++ {
		val, err := b.next(bits)
		out[i] = val != 0
		if err != nil {
			return i, err
		}
	}

	return i, nil
}

func (b *BitReader) GetBatch(bits uint, out []uint64) (int, error) {
	if bits > 64 {
		return 0, errors.New("must be 64 bits or less per read")
	}

	length := len(out)

	i := 0
	for ; i < length && b.bitoffset != 0; i++ {
		val, err := b.next(bits)
		out[i] = val
		if err != nil {
			return i, err
		}
	}

	b.reader.Seek(b.byteoffset, io.SeekStart)
	for i < length {
		unpackSize := MinInt(buflen, length-i)
		numUnpacked := unpack32(b.reader, b.unpackBuf[:unpackSize], int(bits))
		if numUnpacked == 0 {
			break
		}

		for k := 0; k < numUnpacked; k++ {
			out[i+k] = uint64(b.unpackBuf[k])
		}
		i += numUnpacked
		b.byteoffset += int64(numUnpacked * int(bits) / 8)
	}

	b.fillbuffer()
	for ; i < length; i++ {
		val, err := b.next(bits)
		out[i] = val
		if err != nil {
			return i, err
		}
	}

	return i, nil
}

func (b *BitReader) GetValue(width int) (uint64, bool) {
	v := make([]uint64, 1)
	n, _ := b.GetBatch(uint(width), v)
	return v[0], n == 1
}

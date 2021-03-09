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
	"io"
	"log"

	"github.com/apache/arrow/go/arrow/bitutil"
)

type WriterAtBuffer struct {
	buf []byte
}

func NewWriterAtBuffer(buf []byte) WriterAtWithLen {
	return &WriterAtBuffer{buf}
}

func (w *WriterAtBuffer) Len() int {
	return len(w.buf)
}

func (w *WriterAtBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off > int64(len(w.buf)) {
		return 0, io.ErrUnexpectedEOF
	}

	n = copy(w.buf[off:], p)
	if n < len(p) {
		err = io.ErrUnexpectedEOF
	}
	return
}

type WriterAtWithLen interface {
	io.WriterAt
	Len() int
}

type BitWriter struct {
	wr         io.WriterAt
	buffer     uint64
	byteoffset int
	bitoffset  uint
	raw        [8]byte
}

func NewBitWriter(w io.WriterAt) *BitWriter {
	return &BitWriter{wr: w}
}

// func (b *BitWriter) Len() int {
// 	return b.wr.Len()
// }

func (b *BitWriter) ReserveBytes(nbytes int) int {
	b.Flush(true)
	ret := b.byteoffset
	b.byteoffset += nbytes
	return ret
}

func (b *BitWriter) WriteAt(val []byte, off int64) (int, error) {
	return b.wr.WriteAt(val, off)
}

func (b *BitWriter) Written() int {
	return b.byteoffset + int(bitutil.BytesForBits(int64(b.bitoffset)))
}

func (b *BitWriter) WriteValue(v uint64, nbits uint) bool {
	b.buffer |= v << b.bitoffset
	b.bitoffset += nbits

	if b.bitoffset >= 64 {
		binary.LittleEndian.PutUint64(b.raw[:], b.buffer)
		if _, err := b.wr.WriteAt(b.raw[:], int64(b.byteoffset)); err != nil {
			log.Println(err)
			return false
		}
		b.buffer = 0
		b.byteoffset += 8
		b.bitoffset -= 64
		b.buffer = v >> (nbits - b.bitoffset)
	}
	return true
}

func (b *BitWriter) Flush(align bool) {
	var nbytes int64
	if b.bitoffset > 0 {
		nbytes = bitutil.BytesForBits(int64(b.bitoffset))
		binary.LittleEndian.PutUint64(b.raw[:], b.buffer)
		b.wr.WriteAt(b.raw[:nbytes], int64(b.byteoffset))
	}

	if align {
		b.buffer = 0
		b.byteoffset += int(nbytes)
		b.bitoffset = 0
	}
}

func (b *BitWriter) WriteAligned(val uint64, nbytes int) bool {
	b.Flush(true)
	binary.LittleEndian.PutUint64(b.raw[:], val)
	if _, err := b.wr.WriteAt(b.raw[:nbytes], int64(b.byteoffset)); err != nil {
		log.Println(err)
		return false
	}
	b.byteoffset += nbytes
	return true
}

func (b *BitWriter) WriteVlqInt(v uint64) bool {
	b.Flush(true)
	var buf [binary.MaxVarintLen64]byte
	nbytes := binary.PutUvarint(buf[:], v)
	if _, err := b.wr.WriteAt(buf[:nbytes], int64(b.byteoffset)); err != nil {
		log.Println(err)
		return false
	}
	b.byteoffset += nbytes
	return true
}

func (b *BitWriter) WriteZigZagVlqInt(v int64) bool {
	return b.WriteVlqInt(uint64((v << 1) ^ (v >> 63)))
}

func (b *BitWriter) Clear() {
	b.byteoffset = 0
	b.bitoffset = 0
	b.buffer = 0
}

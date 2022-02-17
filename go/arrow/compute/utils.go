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

package compute

import (
	"io"
	"math"

	"github.com/apache/arrow/go/v8/arrow/bitutil"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"golang.org/x/xerrors"
)

type bufferWriteSeeker struct {
	buf *memory.Buffer
	pos int
	mem memory.Allocator
}

func (b *bufferWriteSeeker) Reserve(nbytes int) {
	if b.buf == nil {
		b.buf = memory.NewResizableBuffer(b.mem)
	}
	newCap := int(math.Max(float64(b.buf.Cap()), 256))
	for newCap < b.pos+nbytes {
		newCap = bitutil.NextPowerOf2(newCap)
	}
	b.buf.Reserve(newCap)
}

func (b *bufferWriteSeeker) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if b.buf == nil {
		b.Reserve(len(p))
	} else if b.pos+len(p) >= b.buf.Cap() {
		b.Reserve(len(p))
	}

	return b.UnsafeWrite(p)
}

func (b *bufferWriteSeeker) UnsafeWrite(p []byte) (n int, err error) {
	n = copy(b.buf.Buf()[b.pos:], p)
	b.pos += len(p)
	if b.pos > b.buf.Len() {
		b.buf.ResizeNoShrink(b.pos)
	}
	return
}

func (b *bufferWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	newpos, offs := 0, int(offset)
	switch whence {
	case io.SeekStart:
		newpos = offs
	case io.SeekCurrent:
		newpos = b.pos + offs
	case io.SeekEnd:
		newpos = b.buf.Len() + offs
	}
	if newpos < 0 {
		return 0, xerrors.New("negative result pos")
	}
	b.pos = newpos
	return int64(newpos), nil
}

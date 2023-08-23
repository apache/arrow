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

package array

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/endian"
	"github.com/stretchr/testify/assert"
)

func TestInt32BufferBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bb := newInt32BufferBuilder(mem)
	exp := []int32{0x01020304, 0x05060708, 0x090a0b0c, 0x0d0e0f01, 0x02030405, 0x06070809}
	bb.AppendValues(exp[:3])
	bb.AppendValues(exp[3:])

	var expBuf []byte
	if endian.IsBigEndian {
		expBuf = []byte{
			0x01, 0x02, 0x03, 0x04,
			0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c,
			0x0d, 0x0e, 0x0f, 0x01,
			0x02, 0x03, 0x04, 0x05,
			0x06, 0x07, 0x08, 0x09,
		}
	} else {
		expBuf = []byte{
			0x04, 0x03, 0x02, 0x01,
			0x08, 0x07, 0x06, 0x05,
			0x0c, 0x0b, 0x0a, 0x09,
			0x01, 0x0f, 0x0e, 0x0d,
			0x05, 0x04, 0x03, 0x02,
			0x09, 0x08, 0x07, 0x06,
		}
	}
	assert.Equal(t, expBuf, bb.Bytes(), "unexpected byte values")
	assert.Equal(t, exp, bb.Values(), "unexpected int32 values")
	assert.Equal(t, len(exp), bb.Len(), "unexpected Len()")

	buflen := bb.Len()
	bfr := bb.Finish()
	assert.Equal(t, buflen*int(unsafe.Sizeof(int32(0))), bfr.Len(), "Buffer was not resized")
	assert.Len(t, bfr.Bytes(), bfr.Len(), "Buffer.Bytes() != Buffer.Len()")
	bfr.Release()

	assert.Len(t, bb.Bytes(), 0, "BufferBuilder was not reset after Finish")
	assert.Zero(t, bb.Len(), "BufferBuilder was not reset after Finish")
	bb.Release()
}

func TestInt32BufferBuilder_AppendValue(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bb := newInt32BufferBuilder(mem)
	exp := []int32{0x01020304, 0x05060708, 0x090a0b0c, 0x0d0e0f01, 0x02030405, 0x06070809}
	for _, v := range exp {
		bb.AppendValue(v)
	}

	var expBuf []byte
	if endian.IsBigEndian {
		expBuf = []byte{
			0x01, 0x02, 0x03, 0x04,
			0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c,
			0x0d, 0x0e, 0x0f, 0x01,
			0x02, 0x03, 0x04, 0x05,
			0x06, 0x07, 0x08, 0x09,
		}
	} else {
		expBuf = []byte{
			0x04, 0x03, 0x02, 0x01,
			0x08, 0x07, 0x06, 0x05,
			0x0c, 0x0b, 0x0a, 0x09,
			0x01, 0x0f, 0x0e, 0x0d,
			0x05, 0x04, 0x03, 0x02,
			0x09, 0x08, 0x07, 0x06,
		}
	}
	assert.Equal(t, expBuf, bb.Bytes(), "unexpected byte values")
	assert.Equal(t, exp, bb.Values(), "unexpected int32 values")
	assert.Equal(t, len(exp), bb.Len(), "unexpected Len()")
	bb.Release()
}

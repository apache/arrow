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

package memory_test

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewResizableBuffer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)
	buf.Retain() // refCount == 2

	exp := 10
	buf.Resize(exp)
	assert.NotNil(t, buf.Bytes())
	assert.Equal(t, exp, len(buf.Bytes()))
	assert.Equal(t, exp, buf.Len())

	buf.Release() // refCount == 1
	assert.NotNil(t, buf.Bytes())

	buf.Release() // refCount == 0
	assert.Nil(t, buf.Bytes())
	assert.Zero(t, buf.Len())
}

func TestBufferReset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)

	newBytes := []byte("some-new-bytes")
	buf.Reset(newBytes)
	assert.Equal(t, newBytes, buf.Bytes())
	assert.Equal(t, len(newBytes), buf.Len())
}

func TestBufferSlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)
	buf.Resize(1024)
	assert.Equal(t, 1024, mem.CurrentAlloc())

	slice := memory.SliceBuffer(buf, 512, 256)
	buf.Release()
	assert.Equal(t, 1024, mem.CurrentAlloc())
	slice.Release()
}

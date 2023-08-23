// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build cgo

package mallocator_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/memory/mallocator"
	"github.com/stretchr/testify/assert"
)

func TestMallocatorAllocate(t *testing.T) {
	sizes := []int{0, 1, 4, 33, 65, 4095, 4096, 8193}
	for _, size := range sizes {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a := mallocator.NewMallocator()
			buf := a.Allocate(size)
			defer a.Free(buf)

			assert.Equal(t, size, len(buf))
			assert.LessOrEqual(t, size, cap(buf))
			// check 0-initialized
			for idx, c := range buf {
				assert.Equal(t, uint8(0), c, fmt.Sprintf("Buf not zero-initialized at %d", idx))
			}
		})
	}
}

func TestMallocatorReallocate(t *testing.T) {
	sizes := []struct {
		before, after int
	}{
		{0, 1},
		{1, 0},
		{1, 2},
		{1, 33},
		{4, 4},
		{32, 16},
		{32, 1},
	}
	for _, test := range sizes {
		t.Run(fmt.Sprintf("%dTo%d", test.before, test.after), func(t *testing.T) {
			a := mallocator.NewMallocator()
			buf := a.Allocate(test.before)

			assert.Equal(t, test.before, len(buf))
			assert.LessOrEqual(t, test.before, cap(buf))
			// check 0-initialized
			for idx, c := range buf {
				assert.Equal(t, uint8(0), c, fmt.Sprintf("Buf not zero-initialized at %d", idx))
			}

			buf = a.Reallocate(test.after, buf)
			defer a.Free(buf)
			assert.Equal(t, test.after, len(buf))
			assert.LessOrEqual(t, test.after, cap(buf))
			// check 0-initialized
			for idx, c := range buf {
				assert.Equal(t, uint8(0), c, fmt.Sprintf("Buf not zero-initialized at %d", idx))
			}
		})
	}
}

func TestMallocatorAssertSize(t *testing.T) {
	a := mallocator.NewMallocator()
	assert.Equal(t, int64(0), a.AllocatedBytes())

	buf1 := a.Allocate(64)
	a.AssertSize(t, 64)

	buf2 := a.Allocate(128)
	a.AssertSize(t, 192)
	assert.Equal(t, int64(192), a.AllocatedBytes())

	a.Free(buf1)
	a.AssertSize(t, 128)
	assert.Equal(t, int64(128), a.AllocatedBytes())

	buf2 = a.Reallocate(256, buf2)
	a.AssertSize(t, 256)
	assert.Equal(t, int64(256), a.AllocatedBytes())

	buf2 = a.Reallocate(64, buf2)
	a.AssertSize(t, 64)
	assert.Equal(t, int64(64), a.AllocatedBytes())

	a.Free(buf2)
	a.AssertSize(t, 0)
	assert.Equal(t, int64(0), a.AllocatedBytes())
}

func TestMallocatorAllocateNegative(t *testing.T) {
	a := mallocator.NewMallocator()
	assert.PanicsWithValue(t, "mallocator: negative size", func() {
		a.Allocate(-1)
	})
}

func TestMallocatorReallocateNegative(t *testing.T) {
	a := mallocator.NewMallocator()
	buf := a.Allocate(1)
	defer a.Free(buf)

	assert.PanicsWithValue(t, "mallocator: negative size", func() {
		a.Reallocate(-1, buf)
	})
}

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

// +build cgo
// +build ccalloc

package memory

import (
	"runtime"

	cga "github.com/apache/arrow/go/arrow/memory/internal/cgoalloc"
)

type CgoArrowAllocator struct {
	pool cga.CGOMemPool
}

func (alloc *CgoArrowAllocator) Allocate(size int) []byte {
	b := cga.CgoPoolAlloc(alloc.pool, size)
	return b
}

func (alloc *CgoArrowAllocator) Free(b []byte) {
	cga.CgoPoolFree(alloc.pool, b)
}

func (alloc *CgoArrowAllocator) Reallocate(size int, b []byte) []byte {
	oldSize := len(b)
	out := cga.CgoPoolRealloc(alloc.pool, size, b)

	if size > oldSize {
		// zero initialize the slice like go would do normally
		// C won't zero initialize the memory.
		Set(out[oldSize:], 0)
	}
	return out
}

func (alloc *CgoArrowAllocator) AllocatedBytes() int64 {
	return cga.CgoPoolCurBytes(alloc.pool)
}

func (alloc *CgoArrowAllocator) AssertSize(t TestingT, sz int) {
	cur := alloc.AllocatedBytes()
	if int64(sz) != cur {
		t.Helper()
		t.Errorf("invalid memory size exp=%d, got=%d", sz, cur)
	}
}

func NewCgoArrowAllocator() *CgoArrowAllocator {
	alloc := &CgoArrowAllocator{pool: cga.NewCgoArrowAllocator(enableLogging)}
	runtime.SetFinalizer(alloc, func(a *CgoArrowAllocator) { cga.ReleaseCGOMemPool(a.pool) })
	return alloc
}

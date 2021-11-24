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

package memory

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

type CheckedAllocator struct {
	mem Allocator
	sz  int64

	allocs sync.Map
}

func NewCheckedAllocator(mem Allocator) *CheckedAllocator {
	return &CheckedAllocator{mem: mem}
}

func (a *CheckedAllocator) CurrentAlloc() int { return int(atomic.LoadInt64(&a.sz)) }

func (a *CheckedAllocator) Allocate(size int) []byte {
	atomic.AddInt64(&a.sz, int64(size))
	out := a.mem.Allocate(size)
	if size == 0 {
		return out
	}

	ptr := uintptr(unsafe.Pointer(&out[0]))
	if pc, _, l, ok := runtime.Caller(allocFrames); ok {
		a.allocs.Store(ptr, &dalloc{pc: pc, line: l, sz: size})
	}
	return out
}

func (a *CheckedAllocator) Reallocate(size int, b []byte) []byte {
	atomic.AddInt64(&a.sz, int64(size-len(b)))

	oldptr := uintptr(unsafe.Pointer(&b[0]))
	out := a.mem.Reallocate(size, b)
	if size == 0 {
		return out
	}

	newptr := uintptr(unsafe.Pointer(&out[0]))
	a.allocs.Delete(oldptr)
	if pc, _, l, ok := runtime.Caller(reallocFrames); ok {
		a.allocs.Store(newptr, &dalloc{pc: pc, line: l, sz: size})
	}
	return out
}

func (a *CheckedAllocator) Free(b []byte) {
	atomic.AddInt64(&a.sz, int64(len(b)*-1))
	defer a.mem.Free(b)

	if len(b) == 0 {
		return
	}

	ptr := uintptr(unsafe.Pointer(&b[0]))
	a.allocs.Delete(ptr)
}

// typically the allocations are happening in memory.Buffer, not by consumers calling
// allocate/reallocate directly. As a result, we want to skip the caller frames
// of the inner workings of Buffer in order to find the caller that actually triggered
// the allocation via a call to Resize/Reserve/etc.
const (
	defAllocFrames   = 4
	defReallocFrames = 3
)

// Use the environment variables ARROW_CHECKED_ALLOC_FRAMES and ARROW_CHECKED_REALLOC_FRAMES
// to control how many frames up it checks when storing the caller for allocations/reallocs
// when using this to find memory leaks.
var allocFrames, reallocFrames int = defAllocFrames, defReallocFrames

func init() {
	if val, ok := os.LookupEnv("ARROW_CHECKED_ALLOC_FRAMES"); ok {
		if f, err := strconv.Atoi(val); err == nil {
			allocFrames = f
		}
	}

	if val, ok := os.LookupEnv("ARROW_CHECKED_REALLOC_FRAMES"); ok {
		if f, err := strconv.Atoi(val); err == nil {
			reallocFrames = f
		}
	}
}

type dalloc struct {
	pc   uintptr
	line int
	sz   int
}

type TestingT interface {
	Errorf(format string, args ...interface{})
	Helper()
}

func (a *CheckedAllocator) AssertSize(t TestingT, sz int) {
	a.allocs.Range(func(_, value interface{}) bool {
		info := value.(*dalloc)
		f := runtime.FuncForPC(info.pc)
		t.Errorf("LEAK of %d bytes FROM %s line %d\n", info.sz, f.Name(), info.line)
		return true
	})

	if int(atomic.LoadInt64(&a.sz)) != sz {
		t.Helper()
		t.Errorf("invalid memory size exp=%d, got=%d", sz, a.sz)
	}
}

type CheckedAllocatorScope struct {
	alloc *CheckedAllocator
	sz    int
}

func NewCheckedAllocatorScope(alloc *CheckedAllocator) *CheckedAllocatorScope {
	sz := atomic.LoadInt64(&alloc.sz)
	return &CheckedAllocatorScope{alloc: alloc, sz: int(sz)}
}

func (c *CheckedAllocatorScope) CheckSize(t TestingT) {
	sz := int(atomic.LoadInt64(&c.alloc.sz))
	if c.sz != sz {
		t.Helper()
		t.Errorf("invalid memory size exp=%d, got=%d", c.sz, sz)
	}
}

var (
	_ Allocator = (*CheckedAllocator)(nil)
)

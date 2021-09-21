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

// +build ccalloc

package cgoalloc

// #cgo !windows pkg-config: arrow
// #cgo !windows CXXFLAGS: -std=c++14
// #cgo windows LDFLAGS:  -larrow
// #include "allocator.h"
import "C"
import (
	"reflect"
	"unsafe"
)

type CGOMemPool = C.ArrowMemoryPool

func CgoPoolAlloc(pool CGOMemPool, size int) []byte {
	var out *C.uint8_t
	C.arrow_pool_allocate(pool, C.int64_t(size), (**C.uint8_t)(unsafe.Pointer(&out)))

	var ret []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	s.Data = uintptr(unsafe.Pointer(out))
	s.Len = size
	s.Cap = size

	return ret
}

func CgoPoolRealloc(pool CGOMemPool, size int, b []byte) []byte {
	oldSize := C.int64_t(len(b))
	data := (*C.uint8_t)(unsafe.Pointer(&b[0]))
	C.arrow_pool_reallocate(pool, oldSize, C.int64_t(size), &data)

	var ret []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	s.Data = uintptr(unsafe.Pointer(data))
	s.Len = size
	s.Cap = size

	return ret
}

func CgoPoolFree(pool CGOMemPool, b []byte) {
	if len(b) == 0 {
		return
	}

	oldSize := C.int64_t(len(b))
	data := (*C.uint8_t)(unsafe.Pointer(&b[0]))
	C.arrow_pool_free(pool, data, oldSize)
}

func CgoPoolCurBytes(pool CGOMemPool) int64 {
	return int64(C.arrow_pool_bytes_allocated(pool))
}

func ReleaseCGOMemPool(pool CGOMemPool) {
	C.arrow_release_pool(pool)
}

func NewCgoArrowAllocator(logging bool) CGOMemPool {
	return C.arrow_create_memory_pool(C.bool(logging))
}

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

package cdata

import (
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v15/arrow/internal/debug"
)

// #include "arrow/c/helpers.h"
// #include <stdlib.h>
import "C"

type importAllocator struct {
	bufCount int64

	arr *CArrowArray
}

func (i *importAllocator) addBuffer() {
	atomic.AddInt64(&i.bufCount, 1)
}

func (*importAllocator) Allocate(int) []byte {
	panic("cannot allocate from importAllocator")
}

func (*importAllocator) Reallocate(int, []byte) []byte {
	panic("cannot reallocate from importAllocator")
}

func (i *importAllocator) Free([]byte) {
	debug.Assert(atomic.LoadInt64(&i.bufCount) > 0, "too many releases")

	if atomic.AddInt64(&i.bufCount, -1) == 0 {
		defer C.free(unsafe.Pointer(i.arr))
		C.ArrowArrayRelease(i.arr)
		if C.ArrowArrayIsReleased(i.arr) != 1 {
			panic("did not release C mem")
		}
	}
}

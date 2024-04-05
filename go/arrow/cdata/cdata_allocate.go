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

//go:build go1.20 || tinygo

package cdata

// #include <stdlib.h>
// #include "arrow/c/abi.h"
import "C"

import (
	"unsafe"
)

func allocateArrowSchemaArr(n int) (out []CArrowSchema) {
	return unsafe.Slice((*CArrowSchema)(C.calloc(C.size_t(n),
		C.sizeof_struct_ArrowSchema)), n)
}

func allocateArrowSchemaPtrArr(n int) (out []*CArrowSchema) {
	return unsafe.Slice((**CArrowSchema)(C.calloc(C.size_t(n),
		C.size_t(unsafe.Sizeof((*CArrowSchema)(nil))))), n)
}

func allocateArrowArrayArr(n int) (out []CArrowArray) {
	return unsafe.Slice((*CArrowArray)(C.calloc(C.size_t(n),
		C.sizeof_struct_ArrowArray)), n)
}

func allocateArrowArrayPtrArr(n int) (out []*CArrowArray) {
	return unsafe.Slice((**CArrowArray)(C.calloc(C.size_t(n),
		C.size_t(unsafe.Sizeof((*CArrowArray)(nil))))), n)
}

func allocateBufferPtrArr(n int) (out []*C.void) {
	return unsafe.Slice((**C.void)(C.calloc(C.size_t(n),
		C.size_t(unsafe.Sizeof((*C.void)(nil))))), n)
}

func allocateBufferSizeArr(n int) (out []C.int64_t) {
	return unsafe.Slice((*C.int64_t)(C.calloc(C.size_t(n),
		C.sizeof_int64_t)), n)
}

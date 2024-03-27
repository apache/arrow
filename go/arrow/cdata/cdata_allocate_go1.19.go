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

//go:build !go1.20 && !tinygo

package cdata

// #include <stdlib.h>
// #include "arrow/c/abi.h"
import "C"

import (
	"reflect"
	"unsafe"
)

func allocateArrowSchemaArr(n int) (out []CArrowSchema) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.sizeof_struct_ArrowSchema))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowSchemaPtrArr(n int) (out []*CArrowSchema) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*CArrowSchema)(nil)))))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowArrayArr(n int) (out []CArrowArray) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.sizeof_struct_ArrowArray))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowArrayPtrArr(n int) (out []*CArrowArray) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*CArrowArray)(nil)))))
	s.Len = n
	s.Cap = n

	return
}

func allocateBufferPtrArr(n int) (out []*C.void) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.void)(nil)))))
	s.Len = n
	s.Cap = n

	return
}

func allocateBufferSizeArr(n int) (out []C.int64_t) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof(int64(0)))))
	s.Len = n
	s.Cap = n

	return
}

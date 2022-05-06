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
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v9/arrow"
)

// #include <stdlib.h>
// #include "arrow/c/helpers.h"
import "C"

var (
	handles   = sync.Map{}
	handleIdx uintptr
)

type dataHandle uintptr

func storeData(d arrow.ArrayData) dataHandle {
	h := atomic.AddUintptr(&handleIdx, 1)
	if h == 0 {
		panic("cgo: ran out of space")
	}
	d.Retain()
	handles.Store(h, d)
	return dataHandle(h)
}

func (d dataHandle) releaseData() {
	arrd, ok := handles.LoadAndDelete(uintptr(d))
	if !ok {
		panic("cgo: invalid datahandle")
	}
	arrd.(arrow.ArrayData).Release()
}

//export releaseExportedSchema
func releaseExportedSchema(schema *CArrowSchema) {
	if C.ArrowSchemaIsReleased(schema) == 1 {
		return
	}
	defer C.ArrowSchemaMarkReleased(schema)

	C.free(unsafe.Pointer(schema.name))
	C.free(unsafe.Pointer(schema.format))
	C.free(unsafe.Pointer(schema.metadata))

	if schema.n_children == 0 {
		return
	}

	var children []*CArrowSchema
	s := (*reflect.SliceHeader)(unsafe.Pointer(&children))
	s.Data = uintptr(unsafe.Pointer(schema.children))
	s.Len = int(schema.n_children)
	s.Cap = int(schema.n_children)

	for _, c := range children {
		C.ArrowSchemaRelease(c)
	}

	C.free(unsafe.Pointer(children[0]))
	C.free(unsafe.Pointer(schema.children))
}

//export releaseExportedArray
func releaseExportedArray(arr *CArrowArray) {
	if C.ArrowArrayIsReleased(arr) == 1 {
		return
	}
	defer C.ArrowArrayMarkReleased(arr)

	if arr.n_buffers > 0 {
		C.free(unsafe.Pointer(arr.buffers))
	}

	if arr.n_children > 0 {
		var children []*CArrowArray
		s := (*reflect.SliceHeader)(unsafe.Pointer(&children))
		s.Data = uintptr(unsafe.Pointer(arr.children))
		s.Len = int(arr.n_children)
		s.Cap = int(arr.n_children)

		for _, c := range children {
			C.ArrowArrayRelease(c)
		}
		C.free(unsafe.Pointer(children[0]))
		C.free(unsafe.Pointer(arr.children))
	}

	h := dataHandle(arr.private_data)
	h.releaseData()
}

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
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

// #include <stdlib.h>
// #include "arrow/c/helpers.h"
//
//	typedef const char cchar_t;
//	extern int streamGetSchema(struct ArrowArrayStream*, struct ArrowSchema*);
//	extern int streamGetNext(struct ArrowArrayStream*, struct ArrowArray*);
//	extern const char* streamGetError(struct ArrowArrayStream*);
//	extern void streamRelease(struct ArrowArrayStream*);
//
import "C"

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

	if schema.dictionary != nil {
		C.ArrowSchemaRelease(schema.dictionary)
		C.free(unsafe.Pointer(schema.dictionary))
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

// apache/arrow#33864: allocate a new cgo.Handle and store its address
// in a heap-allocated uintptr_t.
func createHandle(hndl cgo.Handle) unsafe.Pointer {
	// uintptr_t* hptr = malloc(sizeof(uintptr_t));
	hptr := (*C.uintptr_t)(C.malloc(C.sizeof_uintptr_t))
	// *hptr = (uintptr)hndl;
	*hptr = C.uintptr_t(uintptr(hndl))
	return unsafe.Pointer(hptr)
}

func getHandle(ptr unsafe.Pointer) cgo.Handle {
	// uintptr_t* hptr = (uintptr_t*)ptr;
	hptr := (*C.uintptr_t)(ptr)
	return cgo.Handle((uintptr)(*hptr))
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

	if arr.dictionary != nil {
		C.ArrowArrayRelease(arr.dictionary)
		C.free(unsafe.Pointer(arr.dictionary))
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

	h := getHandle(arr.private_data)
	h.Value().(arrow.ArrayData).Release()
	h.Delete()
	C.free(unsafe.Pointer(arr.private_data))
}

//export streamGetSchema
func streamGetSchema(handle *CArrowArrayStream, out *CArrowSchema) C.int {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return C.int(rdr.getSchema(out))
}

//export streamGetNext
func streamGetNext(handle *CArrowArrayStream, out *CArrowArray) C.int {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return C.int(rdr.next(out))
}

//export streamGetError
func streamGetError(handle *CArrowArrayStream) *C.cchar_t {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return rdr.getLastError()
}

//export streamRelease
func streamRelease(handle *CArrowArrayStream) {
	h := getHandle(handle.private_data)
	h.Value().(cRecordReader).release()
	h.Delete()
	C.free(unsafe.Pointer(handle.private_data))
	handle.release = nil
	handle.private_data = nil
}

func exportStream(rdr array.RecordReader, out *CArrowArrayStream) {
	out.get_schema = (*[0]byte)(C.streamGetSchema)
	out.get_next = (*[0]byte)(C.streamGetNext)
	out.get_last_error = (*[0]byte)(C.streamGetError)
	out.release = (*[0]byte)(C.streamRelease)
	h := cgo.NewHandle(cRecordReader{rdr: rdr, err: nil})
	out.private_data = createHandle(h)
}

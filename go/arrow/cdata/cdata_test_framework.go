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

//go:build test
// +build test

package cdata

// #include <stdlib.h>
// #include <stdint.h>
// #include <string.h>
// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
//
// void setup_array_stream_test(const int n_batches, struct ArrowArrayStream* out);
// static struct ArrowArray* get_test_arr() {
//   struct ArrowArray* array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
//   memset(array, 0, sizeof(*array));
//   return array;
// }
// static struct ArrowArrayStream* get_test_stream() {
//	struct ArrowArrayStream* out = (struct ArrowArrayStream*)malloc(sizeof(struct ArrowArrayStream));
//	memset(out, 0, sizeof(struct ArrowArrayStream));
//	return out;
// }
//
// void release_test_arr(struct ArrowArray* arr);
//
// static int32_t* get_data() {
//	int32_t* data = malloc(sizeof(int32_t)*10);
//  for (int i = 0; i < 10; ++i) { data[i] = i+1; }
//	return data;
// }
// void export_int32_type(struct ArrowSchema* schema);
// void export_int32_array(const int32_t*, int64_t, struct ArrowArray*);
// int test1_is_released();
// void test_primitive(struct ArrowSchema* schema, const char* fmt);
// void free_malloced_schemas(struct ArrowSchema**);
// struct ArrowSchema** test_lists(const char** fmts, const char** names, const int* nullflags, const int n);
// struct ArrowSchema** test_struct(const char** fmts, const char** names, int64_t* flags, const int n);
// struct ArrowSchema** test_map(const char** fmts, const char** names, int64_t* flags, const int n);
// struct ArrowSchema** test_schema(const char** fmts, const char** names, int64_t* flags, const int n);
// struct ArrowSchema** test_union(const char** fmts, const char** names, int64_t* flags, const int n);
// int test_exported_stream(struct ArrowArrayStream* stream);
// void test_stream_schema_fallible(struct ArrowArrayStream* stream);
// int confuse_go_gc(struct ArrowArrayStream* stream, unsigned int seed);
// extern void releaseTestArr(struct ArrowArray* array);
// extern void goReleaseTestArray(struct ArrowArray* array);
import "C"

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/internal"
	"github.com/apache/arrow/go/v16/arrow/memory/mallocator"
)

const (
	flagIsNullable    = C.ARROW_FLAG_NULLABLE
	flagMapKeysSorted = C.ARROW_FLAG_MAP_KEYS_SORTED
)

var (
	metadata1 = arrow.NewMetadata([]string{"key1", "key2"}, []string{"", "bar"})
	metadata2 = arrow.NewMetadata([]string{"key"}, []string{"abcde"})
)

func exportInt32TypeSchema() CArrowSchema {
	var s CArrowSchema
	C.export_int32_type(&s)
	return s
}

func releaseStream(s *CArrowArrayStream) {
	C.ArrowArrayStreamRelease(s)
}

func schemaIsReleased(s *CArrowSchema) bool {
	return C.ArrowSchemaIsReleased(s) == 1
}

func getMetadataKeys() ([]string, []string) {
	return []string{"key1", "key2"}, []string{"key"}
}

func getMetadataValues() ([]string, []string) {
	return []string{"", "bar"}, []string{"abcde"}
}

func exportInt32Array() *CArrowArray {
	arr := C.get_test_arr()
	C.export_int32_array(C.get_data(), C.int64_t(10), arr)
	return arr
}

func isReleased(arr *CArrowArray) bool {
	return C.ArrowArrayIsReleased(arr) == 1
}

func test1IsReleased() bool {
	return C.test1_is_released() == 1
}

func testPrimitive(fmtstr string) CArrowSchema {
	var s CArrowSchema
	fmt := C.CString(fmtstr)
	C.test_primitive(&s, fmt)
	return s
}

func freeMallocedSchemas(schemas **CArrowSchema) {
	C.free_malloced_schemas(schemas)
}

func testNested(fmts, names []string, isnull []bool) **CArrowSchema {
	if len(fmts) != len(names) {
		panic("testing nested lists must have same size fmts and names")
	}
	cfmts := make([]*C.char, len(fmts))
	cnames := make([]*C.char, len(names))
	nulls := make([]C.int, len(isnull))

	for i := range fmts {
		cfmts[i] = C.CString(fmts[i])
		cnames[i] = C.CString(names[i])
	}

	for i, v := range isnull {
		if v {
			nulls[i] = C.ARROW_FLAG_NULLABLE
		} else {
			nulls[i] = 0
		}
	}

	return C.test_lists((**C.char)(unsafe.Pointer(&cfmts[0])), (**C.char)(unsafe.Pointer(&cnames[0])), (*C.int)(unsafe.Pointer(&nulls[0])), C.int(len(fmts)))
}

func testStruct(fmts, names []string, flags []int64) **CArrowSchema {
	if len(fmts) != len(names) || len(names) != len(flags) {
		panic("testing structs must all have the same size slices in args")
	}

	cfmts := make([]*C.char, len(fmts))
	cnames := make([]*C.char, len(names))
	cflags := make([]C.int64_t, len(flags))

	for i := range fmts {
		cfmts[i] = C.CString(fmts[i])
		cnames[i] = C.CString(names[i])
		cflags[i] = C.int64_t(flags[i])
	}

	return C.test_struct((**C.char)(unsafe.Pointer(&cfmts[0])), (**C.char)(unsafe.Pointer(&cnames[0])), (*C.int64_t)(unsafe.Pointer(&cflags[0])), C.int(len(fmts)))
}

func testMap(fmts, names []string, flags []int64) **CArrowSchema {
	if len(fmts) != len(names) || len(names) != len(flags) {
		panic("testing maps must all have the same size slices in args")
	}

	cfmts := make([]*C.char, len(fmts))
	cnames := make([]*C.char, len(names))
	cflags := make([]C.int64_t, len(flags))

	for i := range fmts {
		cfmts[i] = C.CString(fmts[i])
		cnames[i] = C.CString(names[i])
		cflags[i] = C.int64_t(flags[i])
	}

	return C.test_map((**C.char)(unsafe.Pointer(&cfmts[0])), (**C.char)(unsafe.Pointer(&cnames[0])), (*C.int64_t)(unsafe.Pointer(&cflags[0])), C.int(len(fmts)))
}

func testUnion(fmts, names []string, flags []int64) **CArrowSchema {
	if len(fmts) != len(names) || len(names) != len(flags) {
		panic("testing unions must all have the same size slices in args")
	}

	cfmts := make([]*C.char, len(fmts))
	cnames := make([]*C.char, len(names))
	cflags := make([]C.int64_t, len(flags))

	for i := range fmts {
		cfmts[i] = C.CString(fmts[i])
		cnames[i] = C.CString(names[i])
		cflags[i] = C.int64_t(flags[i])
	}

	return C.test_union((**C.char)(unsafe.Pointer(&cfmts[0])), (**C.char)(unsafe.Pointer(&cnames[0])), (*C.int64_t)(unsafe.Pointer(&cflags[0])), C.int(len(fmts)))
}

func testSchema(fmts, names []string, flags []int64) **CArrowSchema {
	if len(fmts) != len(names) || len(names) != len(flags) {
		panic("testing structs must all have the same size slices in args")
	}

	cfmts := make([]*C.char, len(fmts))
	cnames := make([]*C.char, len(names))
	cflags := make([]C.int64_t, len(flags))

	for i := range fmts {
		cfmts[i] = C.CString(fmts[i])
		cnames[i] = C.CString(names[i])
		cflags[i] = C.int64_t(flags[i])
	}

	return C.test_schema((**C.char)(unsafe.Pointer(&cfmts[0])), (**C.char)(unsafe.Pointer(&cnames[0])), (*C.int64_t)(unsafe.Pointer(&cflags[0])), C.int(len(fmts)))
}

func freeAny[T any](alloc *mallocator.Mallocator, p *T, n int) {
	raw := unsafe.Slice((*byte)(unsafe.Pointer(p)), int(unsafe.Sizeof(*p))*n)
	alloc.Free(raw)
}

func freeTestMallocatorArr(carr *CArrowArray, alloc *mallocator.Mallocator) {
	freeAny(alloc, carr, 1)
}

func getTestArr(alloc *mallocator.Mallocator) *CArrowArray {
	raw := alloc.Allocate(C.sizeof_struct_ArrowArray)
	return (*CArrowArray)(unsafe.Pointer(&raw[0]))
}

type testReleaser struct {
	alloc *mallocator.Mallocator
	bufs  [][]byte
}

//export releaseTestArr
func releaseTestArr(arr *CArrowArray) {
	if C.ArrowArrayIsReleased(arr) == 1 {
		return
	}
	defer C.ArrowArrayMarkReleased(arr)

	h := getHandle(arr.private_data)
	tr := h.Value().(*testReleaser)

	alloc := tr.alloc
	for _, b := range tr.bufs {
		alloc.Free(b)
	}

	if arr.n_buffers > 0 {
		freeAny(alloc, arr.buffers, int(arr.n_buffers))
	}

	if arr.dictionary != nil {
		C.ArrowArrayRelease(arr.dictionary)
		freeAny(alloc, arr.dictionary, 1)
	}

	if arr.n_children > 0 {
		children := unsafe.Slice(arr.children, arr.n_children)
		for _, c := range children {
			C.ArrowArrayRelease(c)
			freeTestMallocatorArr(c, alloc)
		}

		freeAny(alloc, arr.children, int(arr.n_children))
	}

	h.Delete()
	C.free(unsafe.Pointer(arr.private_data))
}

func allocateBufferMallocatorPtrArr(alloc *mallocator.Mallocator, n int) []*C.void {
	raw := alloc.Allocate(int(unsafe.Sizeof((*C.void)(nil))) * n)
	return unsafe.Slice((**C.void)(unsafe.Pointer(&raw[0])), n)
}

func allocateChildrenPtrArr(alloc *mallocator.Mallocator, n int) []*CArrowArray {
	raw := alloc.Allocate(int(unsafe.Sizeof((*CArrowArray)(nil))) * n)
	return unsafe.Slice((**CArrowArray)(unsafe.Pointer(&raw[0])), n)
}

func createCArr(arr arrow.Array, alloc *mallocator.Mallocator) *CArrowArray {
	var (
		carr      = getTestArr(alloc)
		children  = (**CArrowArray)(nil)
		nchildren = C.int64_t(0)
	)

	switch arr := arr.(type) {
	case array.ListLike:
		clist := allocateChildrenPtrArr(alloc, 1)
		clist[0] = createCArr(arr.ListValues(), alloc)
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 1
	case *array.Struct:
		clist := allocateChildrenPtrArr(alloc, arr.NumField())
		for i := 0; i < arr.NumField(); i++ {
			clist[i] = createCArr(arr.Field(i), alloc)
			nchildren += 1
		}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
	case *array.RunEndEncoded:
		clist := allocateChildrenPtrArr(alloc, 2)
		clist[0] = createCArr(arr.RunEndsArr(), alloc)
		clist[1] = createCArr(arr.Values(), alloc)
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 2
	case array.Union:
		clist := allocateChildrenPtrArr(alloc, arr.NumFields())
		for i := 0; i < arr.NumFields(); i++ {
			clist[i] = createCArr(arr.Field(i), alloc)
			nchildren += 1
		}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
	}

	carr.children = children
	carr.n_children = nchildren
	carr.dictionary = nil
	carr.length = C.int64_t(arr.Len())
	carr.null_count = C.int64_t(arr.NullN())
	carr.offset = C.int64_t(arr.Data().Offset())
	carr.release = (*[0]byte)(C.goReleaseTestArray)
	tr := &testReleaser{alloc: alloc}
	h := cgo.NewHandle(tr)
	carr.private_data = createHandle(h)

	buffers := arr.Data().Buffers()
	bufOffset, nbuffers := 0, len(buffers)
	hasValidityBitmap := internal.DefaultHasValidityBitmap(arr.DataType().ID())
	if nbuffers > 0 && !hasValidityBitmap {
		nbuffers--
		bufOffset++
	}

	if nbuffers == 0 {
		return carr
	}

	tr.bufs = make([][]byte, 0, nbuffers)
	cbufs := allocateBufferMallocatorPtrArr(alloc, nbuffers)
	for i, b := range buffers[bufOffset:] {
		if b != nil {
			raw := alloc.Allocate(b.Len())
			copy(raw, b.Bytes())
			tr.bufs = append(tr.bufs, raw)
			cbufs[i] = (*C.void)(unsafe.Pointer(&raw[0]))
		} else {
			cbufs[i] = nil
		}
	}

	carr.n_buffers = C.int64_t(len(cbufs))
	if len(cbufs) > 0 {
		carr.buffers = (*unsafe.Pointer)(unsafe.Pointer(&cbufs[0]))
	}

	return carr
}

func createTestStreamObj() *CArrowArrayStream {
	return C.get_test_stream()
}

func arrayStreamTest() *CArrowArrayStream {
	st := C.get_test_stream()
	C.setup_array_stream_test(2, st)
	return st
}

func exportedStreamTest(reader array.RecordReader) error {
	out := C.get_test_stream()
	ExportRecordReader(reader, out)
	rc := C.test_exported_stream(out)
	C.free(unsafe.Pointer(out))
	if rc == 0 {
		return nil
	}
	return fmt.Errorf("Exported stream test failed with return code %d", int(rc))
}

func roundTripStreamTest(reader array.RecordReader) error {
	out := C.get_test_stream()
	ExportRecordReader(reader, out)
	rdr, err := ImportCRecordReader(out, nil)

	if err != nil {
		return err
	}

	for {
		_, err = rdr.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

func fallibleSchemaTestDeprecated() (err error) {
	stream := CArrowArrayStream{}
	C.test_stream_schema_fallible(&stream)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Panicked: %#v", r)
		}
	}()
	_ = ImportCArrayStream(&stream, nil)
	return nil
}

func fallibleSchemaTest() error {
	stream := CArrowArrayStream{}
	C.test_stream_schema_fallible(&stream)

	_, err := ImportCRecordReader(&stream, nil)
	if err != nil {
		return err
	}
	return nil
}

func confuseGoGc(reader array.RecordReader) error {
	out := C.get_test_stream()
	ExportRecordReader(reader, out)
	rc := C.confuse_go_gc(out, C.uint(rand.Int()))
	C.free(unsafe.Pointer(out))
	if rc == 0 {
		return nil
	}
	return fmt.Errorf("Exported stream test failed with return code %d", int(rc))
}

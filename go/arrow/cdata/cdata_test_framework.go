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
// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
//
// void setup_array_stream_test(const int n_batches, struct ArrowArrayStream* out);
// struct ArrowArray* get_test_arr() { return (struct ArrowArray*)(malloc(sizeof(struct ArrowArray))); }
// struct ArrowArrayStream* get_test_stream() {
//	struct ArrowArrayStream* out = (struct ArrowArrayStream*)malloc(sizeof(struct ArrowArrayStream));
//	memset(out, 0, sizeof(struct ArrowArrayStream));
//	return out;
// }
//
// void release_test_arr(struct ArrowArray* arr) {
//  for (int i = 0; i < arr->n_buffers; ++i) {
//		free((void*)arr->buffers[i]);
//	}
//  ArrowArrayMarkReleased(arr);
// }
//
// int32_t* get_data() {
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
import "C"
import (
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
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

func freeTestArr(carr *CArrowArray) {
	C.free(unsafe.Pointer(carr))
}

func createCArr(arr arrow.Array) *CArrowArray {
	var (
		carr      = C.get_test_arr()
		children  = (**CArrowArray)(nil)
		nchildren = C.int64_t(0)
	)

	switch arr := arr.(type) {
	case *array.List:
		clist := []*CArrowArray{createCArr(arr.ListValues())}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 1
	case *array.LargeList:
		clist := []*CArrowArray{createCArr(arr.ListValues())}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 1
	case *array.FixedSizeList:
		clist := []*CArrowArray{createCArr(arr.ListValues())}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 1
	case *array.Struct:
		clist := []*CArrowArray{}
		for i := 0; i < arr.NumField(); i++ {
			clist = append(clist, createCArr(arr.Field(i)))
			nchildren += 1
		}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
	case *array.Map:
		clist := []*CArrowArray{createCArr(arr.ListValues())}
		children = (**CArrowArray)(unsafe.Pointer(&clist[0]))
		nchildren += 1
	case array.Union:
		clist := []*CArrowArray{}
		for i := 0; i < arr.NumFields(); i++ {
			clist = append(clist, createCArr(arr.Field(i)))
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
	buffers := arr.Data().Buffers()
	cbuf := []unsafe.Pointer{}
	for _, b := range buffers {
		if b != nil {
			cbuf = append(cbuf, C.CBytes(b.Bytes()))
		}
	}
	carr.n_buffers = C.int64_t(len(cbuf))
	if len(cbuf) > 0 {
		carr.buffers = &cbuf[0]
	}
	carr.release = (*[0]byte)(C.release_test_arr)

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
	rdr := ImportCArrayStream(out, nil)

	for {
		_, err := rdr.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

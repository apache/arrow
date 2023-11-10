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

//go:build cdata_integration
// +build cdata_integration

package main

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/cdata"
	"github.com/apache/arrow/go/v14/arrow/internal/arrjson"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// #include <stdint.h>
// #include <stdlib.h>
import "C"

var alloc = memory.NewCheckedAllocator(memory.NewGoAllocator())

//export ArrowGo_BytesAllocated
func ArrowGo_BytesAllocated() int64 {
	return int64(alloc.CurrentAlloc())
}

//export ArrowGo_RunGC
func ArrowGo_RunGC() {
	runtime.GC()
}

//export ArrowGo_FreeError
func ArrowGo_FreeError(cError *C.char) {
	C.free(unsafe.Pointer(cError))
}

// When used in a defer() statement, this functions catches an incoming
// panic and converts it into a regular error. This avoids crashing the
// archery integration process and lets other tests proceed.
// Not all panics may be caught and some will still crash the process, though.
func handlePanic(err *error) {
	if e := recover(); e != nil {
		// Add a prefix while wrapping the panic-error
		*err = fmt.Errorf("panic: %w", e.(error))
	}
}

func newJsonReader(cJsonPath *C.char) (*arrjson.Reader, error) {
	jsonPath := C.GoString(cJsonPath)

	f, err := os.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("could not open JSON file %q: %w", jsonPath, err)
	}
	defer f.Close()

	jsonReader, err := arrjson.NewReader(f, arrjson.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("could not open JSON file reader from file %q: %w", jsonPath, err)
	}
	return jsonReader, nil
}

func exportSchemaFromJson(cJsonPath *C.char, out *cdata.CArrowSchema) error {
	jsonReader, err := newJsonReader(cJsonPath)
	if err != nil {
		return err
	}
	defer jsonReader.Release()
	schema := jsonReader.Schema()
	defer handlePanic(&err)
	cdata.ExportArrowSchema(schema, out)
	return err
}

func importSchemaAndCompareToJson(cJsonPath *C.char, cSchema *cdata.CArrowSchema) error {
	jsonReader, err := newJsonReader(cJsonPath)
	if err != nil {
		return err
	}
	defer jsonReader.Release()
	schema := jsonReader.Schema()
	importedSchema, err := cdata.ImportCArrowSchema(cSchema)
	if err != nil {
		return err
	}
	if !schema.Equal(importedSchema) || !schema.Metadata().Equal(importedSchema.Metadata()) {
		return fmt.Errorf(
			"Schemas are different:\n- Json Schema: %s\n- Imported Schema: %s",
			schema.String(),
			importedSchema.String())
	}
	return nil
}

func exportBatchFromJson(cJsonPath *C.char, num_batch int, out *cdata.CArrowArray) error {
	// XXX this function exports a single batch at a time, but the JSON reader
	// reads all batches at construction.
	jsonReader, err := newJsonReader(cJsonPath)
	if err != nil {
		return err
	}
	defer jsonReader.Release()
	batch, err := jsonReader.ReadAt(num_batch)
	if err != nil {
		return err
	}
	defer handlePanic(&err)
	cdata.ExportArrowRecordBatch(batch, out, nil)
	return err
}

func importBatchAndCompareToJson(cJsonPath *C.char, num_batch int, cArray *cdata.CArrowArray) error {
	jsonReader, err := newJsonReader(cJsonPath)
	if err != nil {
		return err
	}
	defer jsonReader.Release()
	schema := jsonReader.Schema()
	batch, err := jsonReader.ReadAt(num_batch)
	if err != nil {
		return err
	}

	importedBatch, err := cdata.ImportCRecordBatchWithSchema(cArray, schema)
	if err != nil {
		return err
	}
	defer importedBatch.Release()
	if !array.RecordEqual(batch, importedBatch) {
		return fmt.Errorf(
			"Batches are different:\n- Json Batch: %v\n- Imported Batch: %v",
			batch, importedBatch)
	}
	return nil
}

//export ArrowGo_ExportSchemaFromJson
func ArrowGo_ExportSchemaFromJson(cJsonPath *C.char, out uintptr) *C.char {
	err := exportSchemaFromJson(cJsonPath, cdata.SchemaFromPtr(out))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export ArrowGo_ExportBatchFromJson
func ArrowGo_ExportBatchFromJson(cJsonPath *C.char, num_batch int, out uintptr) *C.char {
	err := exportBatchFromJson(cJsonPath, num_batch, cdata.ArrayFromPtr(out))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export ArrowGo_ImportSchemaAndCompareToJson
func ArrowGo_ImportSchemaAndCompareToJson(cJsonPath *C.char, cSchema uintptr) *C.char {
	err := importSchemaAndCompareToJson(cJsonPath, cdata.SchemaFromPtr(cSchema))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export ArrowGo_ImportBatchAndCompareToJson
func ArrowGo_ImportBatchAndCompareToJson(cJsonPath *C.char, num_batch int, cArray uintptr) *C.char {
	err := importBatchAndCompareToJson(cJsonPath, num_batch, cdata.ArrayFromPtr(cArray))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

func main() {}

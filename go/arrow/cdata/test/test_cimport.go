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

// +build cdata_test

package main

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/cdata"
	"github.com/apache/arrow/go/arrow/memory"
)

// #include <stdint.h>
import "C"

//export runGC
func runGC() {
	runtime.GC()
}

//export importSchema
func importSchema(ptr uintptr) {
	sc := (*cdata.CArrowSchema)(unsafe.Pointer(ptr))

	schema, err := cdata.ImportCArrowSchema(sc)
	if err != nil {
		panic(err)
	}

	expectedMetadata := arrow.NewMetadata([]string{"key1"}, []string{"value1"})
	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, &expectedMetadata)
	if !schema.Equal(expectedSchema) {
		panic(fmt.Sprintf("schema didn't match: expected %s, got %s", expectedSchema, schema))
	}
	if !schema.Metadata().Equal(expectedMetadata) {
		panic(fmt.Sprintf("metadata didn't match: expected %s, got %s", expectedMetadata, schema.Metadata()))
	}

	fmt.Println("schema matches! Huzzah!")
}

//export importRecordBatch
func importRecordBatch(scptr, rbptr uintptr) {
	sc := (*cdata.CArrowSchema)(unsafe.Pointer(scptr))
	rb := (*cdata.CArrowArray)(unsafe.Pointer(rbptr))

	rec, err := cdata.ImportCRecordBatch(rb, sc)
	if err != nil {
		panic(err)
	}
	defer rec.Release()

	expectedMetadata := arrow.NewMetadata([]string{"key1"}, []string{"value1"})
	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, &expectedMetadata)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, expectedSchema)
	defer bldr.Release()

	lb := bldr.Field(0).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)

	// [[[1], [], None [2, 42]]]
	lb.Append(true)
	vb.Append(int32(1))

	lb.Append(true)
	lb.Append(false)

	lb.Append(true)
	vb.AppendValues([]int32{2, 42}, nil)

	expectedRec := bldr.NewRecord()
	defer expectedRec.Release()

	if !array.RecordEqual(expectedRec, rec) {
		panic(fmt.Sprintf("records didn't match: expected %s\n got %s", expectedRec, rec))
	}

	fmt.Println("record batch matches huzzah!")
}

func main() {}

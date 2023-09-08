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

//go:build cdata_test
// +build cdata_test

package main

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/cdata"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// #include <stdint.h>
import "C"

var alloc = memory.NewCheckedAllocator(memory.NewGoAllocator())

//export totalAllocated
func totalAllocated() int64 {
	return int64(alloc.CurrentAlloc())
}

//export runGC
func runGC() {
	runtime.GC()
}

//export importSchema
func importSchema(ptr uintptr) {
	schema, err := cdata.ImportCArrowSchema(cdata.SchemaFromPtr(ptr))
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
	sc := cdata.SchemaFromPtr(scptr)
	rb := cdata.ArrayFromPtr(rbptr)

	rec, err := cdata.ImportCRecordBatch(rb, sc)
	if err != nil {
		panic(err)
	}
	defer rec.Release()

	expectedMetadata := arrow.NewMetadata([]string{"key1"}, []string{"value1"})
	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, &expectedMetadata)

	bldr := array.NewRecordBuilder(alloc, expectedSchema)
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

func makeSchema() *arrow.Schema {
	meta := arrow.NewMetadata([]string{"key1"}, []string{"value1"})
	return arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, &meta)
}

func makeBatch() arrow.Record {
	bldr := array.NewRecordBuilder(alloc, makeSchema())
	defer bldr.Release()

	fbldr := bldr.Field(0).(*array.ListBuilder)
	valbldr := fbldr.ValueBuilder().(*array.Int32Builder)

	fbldr.Append(true)
	valbldr.Append(1)

	fbldr.Append(true)
	fbldr.AppendNull()
	fbldr.Append(true)
	valbldr.Append(2)
	valbldr.Append(42)

	return bldr.NewRecord()
}

//export exportSchema
func exportSchema(schema uintptr) {
	cdata.ExportArrowSchema(makeSchema(), cdata.SchemaFromPtr(schema))
}

//export exportRecordBatch
func exportRecordBatch(schema, record uintptr) {
	batch := makeBatch()
	defer batch.Release()

	cdata.ExportArrowRecordBatch(batch, cdata.ArrayFromPtr(record), cdata.SchemaFromPtr(schema))
}

//export importThenExportSchema
func importThenExportSchema(input, output uintptr) {
	schema, err := cdata.ImportCArrowSchema(cdata.SchemaFromPtr(input))
	if err != nil {
		panic(err)
	}

	cdata.ExportArrowSchema(schema, cdata.SchemaFromPtr(output))
}

//export importThenExportRecord
func importThenExportRecord(schemaIn, arrIn uintptr, schemaOut, arrOut uintptr) {
	rec, err := cdata.ImportCRecordBatch(cdata.ArrayFromPtr(arrIn), cdata.SchemaFromPtr(schemaIn))
	if err != nil {
		panic(err)
	}

	defer rec.Release()
	cdata.ExportArrowRecordBatch(rec, cdata.ArrayFromPtr(arrOut), cdata.SchemaFromPtr(schemaOut))
}

//export roundtripArray
func roundtripArray(arrIn, schema, arrOut uintptr) {
	_, arr, err := cdata.ImportCArray(cdata.ArrayFromPtr(arrIn), cdata.SchemaFromPtr(schema))
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	outArr := cdata.ArrayFromPtr(arrOut)
	cdata.ExportArrowArray(arr, outArr, nil)
}

func main() {}

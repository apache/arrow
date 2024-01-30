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

package main

import (
	"log"
	"os"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/ipc"
)

func writeBytes(rec arrow.Record, file_name string) {
	file, err := os.Create(file_name)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	rr, write_err := ipc.NewFileWriter(file, ipc.WithSchema(rec.Schema()))
	if write_err != nil {
		log.Fatal(write_err)
	}

	rr.Write(rec)
	rr.Close()
}


func writeBoolData() {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "two", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false,}, nil)
	b.Field(0).(*array.BooleanBuilder).AppendNull()
	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true,}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"zero", "one", "two", "three", "four"}, nil)
	rec := b.NewRecord()
	defer rec.Release()


	writeBytes(rec, "testdata_bool.arrow")
}

func writeDoubleData() {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"zero"}, nil)
	b.Field(1).(*array.StringBuilder).AppendNull()
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"two", "three", "four"}, nil)
	rec := b.NewRecord()
	defer rec.Release()


	writeBytes(rec, "testdata_double.arrow")
}

func main() {
	writeBoolData();
	writeDoubleData();
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"os"
	"log"
)

import "gir/arrow-1.0"

func BuildUInt8Array() *arrow.Array {
	builder := arrow.NewUInt8ArrayBuilder()
	for _, value := range []uint8{1, 2, 4, 8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildUInt16Array() *arrow.Array {
	builder := arrow.NewUInt16ArrayBuilder()
	for _, value := range []uint16{1, 2, 4, 8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildUInt32Array() *arrow.Array {
	builder := arrow.NewUInt32ArrayBuilder()
	for _, value := range []uint32{1, 2, 4, 8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildUInt64Array() *arrow.Array {
	builder := arrow.NewUInt64ArrayBuilder()
	for _, value := range []uint64{1, 2, 4, 8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildInt8Array() *arrow.Array {
	builder := arrow.NewInt8ArrayBuilder()
	for _, value := range []int8{1, -2, 4, -8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildInt16Array() *arrow.Array {
	builder := arrow.NewInt16ArrayBuilder()
	for _, value := range []int16{1, -2, 4, -8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildInt32Array() *arrow.Array {
	builder := arrow.NewInt32ArrayBuilder()
	for _, value := range []int32{1, -2, 4, -8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildInt64Array() *arrow.Array {
	builder := arrow.NewInt64ArrayBuilder()
	for _, value := range []int64{1, -2, 4, -8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildFloatArray() *arrow.Array {
	builder := arrow.NewFloatArrayBuilder()
	for _, value := range []float32{1.1, -2.2, 4.4, -8.8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func BuildDoubleArray() *arrow.Array {
	builder := arrow.NewDoubleArrayBuilder()
	for _, value := range []float64{1.1, -2.2, 4.4, -8.8} {
		builder.Append(value)
	}
	return builder.Finish()
}

func main() {
	var output_path string
	if len(os.Args) < 2 {
		output_path = "/tmp/stream.arrow"
	} else {
		output_path = os.Args[1]
	}

	fields := []*arrow.Field{
		arrow.NewField("uint8",  arrow.NewUInt8DataType()),
		arrow.NewField("uint16", arrow.NewUInt16DataType()),
		arrow.NewField("uint32", arrow.NewUInt32DataType()),
		arrow.NewField("uint64", arrow.NewUInt64DataType()),
		arrow.NewField("int8",   arrow.NewInt8DataType()),
		arrow.NewField("int16",  arrow.NewInt16DataType()),
		arrow.NewField("int32",  arrow.NewInt32DataType()),
		arrow.NewField("int64",  arrow.NewInt64DataType()),
		arrow.NewField("float",  arrow.NewFloatDataType()),
		arrow.NewField("double", arrow.NewDoubleDataType()),
	}
	schema := arrow.NewSchema(fields)

	output, err := arrow.NewFileOutputStream(output_path, false)
	if err != nil {
		log.Fatalf("Failed to open path: <%s>: %v", output_path, err)
	}
	writer, err := arrow.NewStreamWriter(output, schema)
	if err != nil {
		log.Fatalf("Failed to create writer: %v", err)
	}

	columns := []*arrow.Array{
		BuildUInt8Array(),
		BuildUInt16Array(),
		BuildUInt32Array(),
		BuildUInt64Array(),
		BuildInt8Array(),
		BuildInt16Array(),
		BuildInt32Array(),
		BuildInt64Array(),
		BuildFloatArray(),
		BuildDoubleArray(),
	}

	recordBatch := arrow.NewRecordBatch(schema, 4, columns)
	_, err = writer.WriteRecordBatch(recordBatch)
	if err != nil {
		log.Fatalf("Failed to write record batch #1: %v", err)
	}

	slicedColumns := make([]*arrow.Array, len(columns))
	for i, column := range columns {
		slicedColumns[i] = column.Slice(1, 3)
	}
	recordBatch = arrow.NewRecordBatch(schema, 3, slicedColumns)
	writer.WriteRecordBatch(recordBatch)
	_, err = writer.WriteRecordBatch(recordBatch)
	if err != nil {
		log.Fatalf("Failed to write record batch #2: %v", err)
	}

	writer.Close()
}

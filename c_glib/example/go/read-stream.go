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
	"fmt"
	"strings"
)

import "gir/arrow-1.0"

func PrintColumnValue(column *arrow.Array, i int64) {
	valueType := column.GetValueType()
	switch valueType {
	case arrow.TypeUint8:
		fmt.Print(arrow.ToUInt8Array(column).GetValue(i))
	case arrow.TypeUint16:
		fmt.Print(arrow.ToUInt16Array(column).GetValue(i))
	case arrow.TypeUint32:
		fmt.Print(arrow.ToUInt32Array(column).GetValue(i))
	case arrow.TypeUint64:
		fmt.Print(arrow.ToUInt64Array(column).GetValue(i))
	case arrow.TypeInt8:
		fmt.Print(arrow.ToInt8Array(column).GetValue(i))
	case arrow.TypeInt16:
		fmt.Print(arrow.ToInt16Array(column).GetValue(i))
	case arrow.TypeInt32:
		fmt.Print(arrow.ToInt32Array(column).GetValue(i))
	case arrow.TypeInt64:
		fmt.Print(arrow.ToInt64Array(column).GetValue(i))
	case arrow.TypeFloat:
		fmt.Print(arrow.ToFloatArray(column).GetValue(i))
	case arrow.TypeDouble:
		fmt.Print(arrow.ToDoubleArray(column).GetValue(i))
	default:
		fmt.Printf("unknown(%s)", valueType)
	}
}

func PrintRecordBatch(recordBatch *arrow.RecordBatch) {
	nColumns := recordBatch.GetNColumns()
	for i := uint32(0); i < nColumns; i++ {
		column := recordBatch.GetColumn(i)
		columnName := recordBatch.GetColumnName(i)
		fmt.Printf("  %s: [", columnName)
		nRows := recordBatch.GetNRows()
		for j := int64(0); j < nRows; j++ {
			if j > 0 {
				fmt.Print(", ")
			}
			PrintColumnValue(column, j)
		}
		fmt.Println("]")
	}
}

func main() {
	var path string
	if len(os.Args) < 2 {
		path = "/tmp/stream.arrow"
	} else {
		path = os.Args[1]
	}
	input, err := arrow.NewMemoryMappedInputStream(path);
	if err != nil {
		log.Fatalf("Failed to open path: <%s>: %v", path, err)
	}
	reader, err := arrow.NewRecordBatchStreamReader(input)
	if err != nil {
		log.Fatalf("Failed to parse data: %v", err)
	}
	for i := 0; true; i++ {
		recordBatch, err := reader.GetNextRecordBatch()
		if err != nil {
			log.Fatalf("Failed to get next record batch: %v", err)
		}
		if recordBatch == nil {
			break
		}
		fmt.Println(strings.Repeat("=", 40))
		fmt.Printf("record-batch[%d]:\n", i)
		PrintRecordBatch(recordBatch)
	}
}

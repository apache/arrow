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

package file_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/internal/encoding"
	"github.com/apache/arrow/go/v12/parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

func TestBufferedRowGroupNulls(t *testing.T) {
	type SimpleSchema struct {
		Col1 *int32
		Col2 *float32
		Col3 *float64
		Col4 *int64
	}

	data := []SimpleSchema{
		{thrift.Int32Ptr(5), thrift.Float32Ptr(10), thrift.Float64Ptr(20), thrift.Int64Ptr(8)},
		{nil, thrift.Float32Ptr(10), thrift.Float64Ptr(20), thrift.Int64Ptr(8)},
		{thrift.Int32Ptr(5), nil, thrift.Float64Ptr(20), thrift.Int64Ptr(8)},
		{thrift.Int32Ptr(5), thrift.Float32Ptr(10), nil, thrift.Int64Ptr(8)},
		{thrift.Int32Ptr(5), thrift.Float32Ptr(10), thrift.Float64Ptr(20), nil},
		{thrift.Int32Ptr(5), thrift.Float32Ptr(10), thrift.Float64Ptr(20), thrift.Int64Ptr(8)},
	}

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	sc, err := schema.NewSchemaFromStruct(SimpleSchema{})
	assert.NoError(t, err)

	writer := file.NewParquetWriter(sink, sc.Root())
	rgWriter := writer.AppendBufferedRowGroup()

	for _, d := range data {
		cw, _ := rgWriter.Column(0)
		if d.Col1 != nil {
			cw.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{*d.Col1}, []int16{1}, nil)
		} else {
			cw.(*file.Int32ColumnChunkWriter).WriteBatch(nil, []int16{0}, nil)
		}

		cw, _ = rgWriter.Column(1)
		if d.Col2 != nil {
			cw.(*file.Float32ColumnChunkWriter).WriteBatch([]float32{*d.Col2}, []int16{1}, nil)
		} else {
			cw.(*file.Float32ColumnChunkWriter).WriteBatch(nil, []int16{0}, nil)
		}

		cw, _ = rgWriter.Column(2)
		if d.Col3 != nil {
			cw.(*file.Float64ColumnChunkWriter).WriteBatch([]float64{*d.Col3}, []int16{1}, nil)
		} else {
			cw.(*file.Float64ColumnChunkWriter).WriteBatch(nil, []int16{0}, nil)
		}

		cw, _ = rgWriter.Column(3)
		if d.Col4 != nil {
			cw.(*file.Int64ColumnChunkWriter).WriteBatch([]int64{*d.Col4}, []int16{1}, nil)
		} else {
			cw.(*file.Int64ColumnChunkWriter).WriteBatch(nil, []int16{0}, nil)
		}
	}

	rgWriter.Close()
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, reader.NumRowGroups())
	rgr := reader.RowGroup(0)
	assert.EqualValues(t, len(data), rgr.NumRows())
}

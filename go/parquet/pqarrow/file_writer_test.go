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

package pqarrow_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWriterRowGroupNumRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 1, "two": null},
		{"one": null, "two": 2},
		{"one": null, "two": null}
	]`
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)

	output := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.Write(record))
	numRows, err := writer.RowGroupNumRows()
	require.NoError(t, err)
	assert.Equal(t, 4, numRows)
	require.NoError(t, writer.Close())
}

func TestFileWriterNumRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "one", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
		{Name: "two", Nullable: true, Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	data := `[
		{"one": 1, "two": 2},
		{"one": 1, "two": null},
		{"one": null, "two": 2},
		{"one": null, "two": null}
	]`
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
	require.NoError(t, err)

	maxRowGroupLength := 2

	output := &bytes.Buffer{}
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(maxRowGroupLength)))
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, writer.Write(record))
	rowGroupNumRows, err := writer.RowGroupNumRows()
	require.NoError(t, err)
	assert.Equal(t, maxRowGroupLength, rowGroupNumRows)

	require.NoError(t, writer.Close())
	assert.Equal(t, 4, writer.NumRows())
}

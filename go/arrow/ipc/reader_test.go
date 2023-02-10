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

package ipc

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderCatchPanic(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"foo", "bar", "baz"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	buf := new(bytes.Buffer)
	writer := NewWriter(buf, WithSchema(schema))
	require.NoError(t, writer.Write(rec))

	for i := buf.Len() - 100; i < buf.Len(); i++ {
		buf.Bytes()[i] = 0
	}

	reader, err := NewReader(buf)
	require.NoError(t, err)

	_, err = reader.Read()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "arrow/ipc: unknown error while reading")
	}
}

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

package extensions_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/extensions"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/internal/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.New()

func TestUUIDExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	builder := extensions.NewUUIDBuilder(mem)
	builder.Append(testUUID)
	builder.AppendNull()
	builder.AppendBytes(testUUID)
	arr := builder.NewArray()
	defer arr.Release()
	arrStr := arr.String()
	assert.Equal(t, fmt.Sprintf(`["%[1]s" (null) "%[1]s"]`, testUUID), arrStr)
	jsonStr, err := json.Marshal(arr)
	assert.NoError(t, err)

	arr1, _, err := array.FromJSON(mem, extensions.NewUUIDType(), bytes.NewReader(jsonStr))
	defer arr1.Release()
	assert.NoError(t, err)
	assert.True(t, array.Equal(arr1, arr))

	require.NoError(t, json.Unmarshal(jsonStr, builder))
	arr2 := builder.NewArray()
	defer arr2.Release()
	assert.True(t, array.Equal(arr2, arr))
}

func TestUUIDExtensionRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: extensions.NewUUIDType()},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*extensions.UUIDBuilder).Append(testUUID)
	builder.Field(0).(*extensions.UUIDBuilder).AppendNull()
	builder.Field(0).(*extensions.UUIDBuilder).Append(testUUID)
	record := builder.NewRecord()
	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "[{\"uuid\":\""+testUUID.String()+"\"}\n,{\"uuid\":null}\n,{\"uuid\":\""+testUUID.String()+"\"}\n]", string(b))
	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, record, record1)
}

func TestUUIDStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := extensions.NewUUIDBuilder(mem)
	b.Append(uuid.Nil)
	b.AppendNull()
	b.Append(uuid.NameSpaceURL)
	b.AppendNull()
	b.Append(testUUID)

	arr := b.NewArray()
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := extensions.NewUUIDBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray()
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestUUIDTypeBasics(t *testing.T) {
	typ := extensions.NewUUIDType()

	assert.Equal(t, "arrow.uuid", typ.ExtensionName())
	assert.True(t, typ.ExtensionEquals(typ))

	assert.True(t, arrow.TypeEqual(typ, typ))
	assert.False(t, arrow.TypeEqual(&arrow.FixedSizeBinaryType{ByteWidth: 16}, typ))
	assert.True(t, arrow.TypeEqual(&arrow.FixedSizeBinaryType{ByteWidth: 16}, typ.StorageType()))

	assert.Equal(t, "extension<arrow.uuid>", typ.String())
}

func TestUUIDTypeCreateFromArray(t *testing.T) {
	typ := extensions.NewUUIDType()

	bldr := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	defer bldr.Release()

	bldr.Append(testUUID[:])
	bldr.AppendNull()
	bldr.Append(testUUID[:])

	storage := bldr.NewArray()
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
	defer arr.Release()

	assert.Equal(t, 3, arr.Len())
	assert.Equal(t, 1, arr.NullN())

	uuidArr, ok := arr.(*extensions.UUIDArray)
	require.True(t, ok)

	require.Equal(t, testUUID, uuidArr.Value(0))
	require.Equal(t, uuid.Nil, uuidArr.Value(1))
	require.Equal(t, testUUID, uuidArr.Value(2))
}

func TestUUIDTypeBatchIPCRoundTrip(t *testing.T) {
	typ := extensions.NewUUIDType()

	bldr := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.Append(testUUID)
	bldr.AppendNull()
	bldr.AppendBytes(testUUID)

	arr := bldr.NewArray()
	defer arr.Release()

	batch := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "field", Type: typ, Nullable: true}}, nil),
		[]arrow.Array{arr}, -1)
	defer batch.Release()

	var written arrow.Record
	{
		var buf bytes.Buffer
		wr := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
		require.NoError(t, wr.Write(batch))
		require.NoError(t, wr.Close())

		rdr, err := ipc.NewReader(&buf)
		require.NoError(t, err)
		written, err = rdr.Read()
		require.NoError(t, err)
		written.Retain()
		defer written.Release()
		rdr.Release()
	}

	assert.Truef(t, batch.Schema().Equal(written.Schema()), "expected: %s, got: %s",
		batch.Schema(), written.Schema())

	assert.Truef(t, array.RecordEqual(batch, written), "expected: %s, got: %s",
		batch, written)
}

func TestMarshallUUIDArray(t *testing.T) {
	bldr := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.Append(testUUID)
	bldr.AppendNull()
	bldr.AppendBytes(testUUID)

	arr := bldr.NewArray()
	defer arr.Release()

	assert.Equal(t, 3, arr.Len())
	assert.Equal(t, 1, arr.NullN())

	uuidArr, ok := arr.(*extensions.UUIDArray)
	require.True(t, ok)

	b, err := uuidArr.MarshalJSON()
	require.NoError(t, err)

	expectedJSON := fmt.Sprintf(`["%[1]s",null,"%[1]s"]`, testUUID)
	require.Equal(t, expectedJSON, string(b))
}

func TestUUIDRecordToJSON(t *testing.T) {
	typ := extensions.NewUUIDType()

	bldr := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	uuid1 := uuid.MustParse("8c607ed4-07b2-4b9c-b5eb-c0387357f9ae")

	bldr.Append(uuid1)
	bldr.AppendNull()

	// c5f2cbd9-7094-491a-b267-167bb62efe02
	bldr.AppendBytes([16]byte{197, 242, 203, 217, 112, 148, 73, 26, 178, 103, 22, 123, 182, 46, 254, 2})

	arr := bldr.NewArray()
	defer arr.Release()

	assert.Equal(t, 3, arr.Len())
	assert.Equal(t, 1, arr.NullN())

	uuidArr, ok := arr.(*extensions.UUIDArray)
	require.True(t, ok)

	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "uuid", Type: typ, Nullable: true}}, nil), []arrow.Array{uuidArr}, 3)
	defer rec.Release()

	buf := bytes.NewBuffer([]byte("\n")) // expected output has leading newline for clearer formatting
	require.NoError(t, array.RecordToJSON(rec, buf))

	expectedJSON := `
		{"uuid":"8c607ed4-07b2-4b9c-b5eb-c0387357f9ae"}
		{"uuid":null}
		{"uuid":"c5f2cbd9-7094-491a-b267-167bb62efe02"}
	`

	expectedJSONLines := strings.Split(expectedJSON, "\n")
	actualJSONLines := strings.Split(buf.String(), "\n")

	require.Equal(t, len(expectedJSONLines), len(actualJSONLines))
	for i := range expectedJSONLines {
		if strings.TrimSpace(expectedJSONLines[i]) != "" {
			require.JSONEq(t, expectedJSONLines[i], actualJSONLines[i])
		}
	}
}

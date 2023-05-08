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

package types_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.New()

func TestExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	extBuilder := array.NewExtensionBuilder(mem, types.NewUUIDType())
	defer extBuilder.Release()
	builder := types.NewUUIDBuilder(extBuilder)
	builder.Append(testUUID)
	arr := builder.NewArray()
	defer arr.Release()
	arrStr := arr.String()
	assert.Equal(t, "[\""+testUUID.String()+"\"]", arrStr)
	jsonStr, err := json.Marshal(arr)
	assert.NoError(t, err)

	arr1, _, err := array.FromJSON(mem, types.NewUUIDType(), bytes.NewReader(jsonStr))
	defer arr1.Release()
	assert.NoError(t, err)
	assert.Equal(t, arr, arr1)
}

func TestExtensionRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid", Type: types.NewUUIDType()},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*types.UUIDBuilder).Append(testUUID)
	record := builder.NewRecord()
	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "[{\"uuid\":\""+testUUID.String()+"\"}\n]", string(b))
	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, record, record1)
}

func TestUUIDStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	extBuilder := array.NewExtensionBuilder(mem, types.NewUUIDType())
	defer extBuilder.Release()
	b := types.NewUUIDBuilder(extBuilder)
	b.Append(uuid.Nil)
	b.AppendNull()
	b.Append(uuid.NameSpaceURL)
	b.AppendNull()
	b.Append(testUUID)

	arr := b.NewArray()
	defer arr.Release()

	// 2. create array via AppendValueFromString
	extBuilder1 := array.NewExtensionBuilder(mem, types.NewUUIDType())
	defer extBuilder1.Release()
	b1 := types.NewUUIDBuilder(extBuilder1)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray()
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.New()

func TestExtensionBuilder(t *testing.T) {
	extBuilder := array.NewExtensionBuilder(memory.DefaultAllocator, types.NewUUIDType())
	builder := types.NewUUIDBuilder(extBuilder)
	builder.Append(testUUID)
	arr := builder.NewArray()
	arrStr := arr.String()
	assert.Equal(t, "[\""+testUUID.String()+"\"]", arrStr)
	jsonStr, err := json.Marshal(arr)
	assert.NoError(t, err)

	arr1, _, err := array.FromJSON(memory.DefaultAllocator, types.NewUUIDType(), bytes.NewReader(jsonStr))
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

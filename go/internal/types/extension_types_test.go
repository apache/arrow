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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/internal/json"
	"github.com/apache/arrow/go/v18/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.New()

func TestUUIDExtensionBuilder(t *testing.T) {
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

func TestUUIDExtensionRecordBuilder(t *testing.T) {
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

func TestBool8ExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	extBuilder := array.NewExtensionBuilder(mem, types.NewBool8Type())
	defer extBuilder.Release()

	builder := types.NewBool8Builder(extBuilder)
	defer builder.Release()

	builder.Append(true)
	arr := builder.NewArray()
	defer arr.Release()

	arrStr := arr.String()
	require.Equal(t, `[true]`, arrStr)

	jsonStr, err := json.Marshal(arr)
	require.NoError(t, err)

	arr1, _, err := array.FromJSON(mem, types.NewBool8Type(), bytes.NewReader(jsonStr))
	require.NoError(t, err)
	defer arr1.Release()

	require.Equal(t, arr, arr1)
}

func TestBool8ExtensionRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bool8", Type: types.NewBool8Type()},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*types.Bool8Builder).Append(true)
	record := builder.NewRecord()
	defer record.Release()

	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "[{\"bool8\":true}\n]", string(b))

	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, record, record1)

	require.NoError(t, builder.UnmarshalJSON([]byte(`{"bool8":true}`)))
	record = builder.NewRecord()
	defer record.Release()

	require.Equal(t, schema, record.Schema())
	require.Equal(t, true, record.Column(0).(*types.Bool8Array).Value(0))
}

func TestBool8StringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	extBuilder := array.NewExtensionBuilder(mem, types.NewBool8Type())
	defer extBuilder.Release()
	b := types.NewBool8Builder(extBuilder)
	b.Append(true)
	b.AppendNull()
	b.Append(false)
	b.AppendNull()
	b.Append(true)

	arr := b.NewArray()
	defer arr.Release()

	// 2. create array via AppendValueFromString
	extBuilder1 := array.NewExtensionBuilder(mem, types.NewBool8Type())
	defer extBuilder1.Release()
	b1 := types.NewBool8Builder(extBuilder1)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray()
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestCompareBool8AndBoolean(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	extBuilder := array.NewExtensionBuilder(mem, types.NewBool8Type())
	defer extBuilder.Release()

	bool8bldr := types.NewBool8Builder(extBuilder)
	defer bool8bldr.Release()

	boolbldr := array.NewBooleanBuilder(mem)
	defer boolbldr.Release()

	inputVals := []bool{true, false, false, false, true}
	inputValidity := []bool{true, false, true, false, true}

	bool8bldr.AppendValues(inputVals, inputValidity)
	bool8Arr := bool8bldr.NewExtensionArray().(*types.Bool8Array)
	defer bool8Arr.Release()

	boolbldr.AppendValues(inputVals, inputValidity)
	boolArr := boolbldr.NewBooleanArray()
	defer boolArr.Release()

	require.Equal(t, boolArr.Len(), bool8Arr.Len())
	for i := 0; i < boolArr.Len(); i++ {
		require.Equal(t, boolArr.Value(i), bool8Arr.Value(i))
	}
}

const (
	MINSIZE = 1024
	MAXSIZE = 65536
)

func BenchmarkWriteBool8Array(b *testing.B) {
	extBuilder := array.NewExtensionBuilder(memory.DefaultAllocator, types.NewBool8Type())
	defer extBuilder.Release()

	bool8bldr := types.NewBool8Builder(extBuilder)
	defer bool8bldr.Release()

	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {

			values := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}

			b.ResetTimer()
			b.SetBytes(int64(sz))
			for n := 0; n < b.N; n++ {
				bool8bldr.AppendValues(values, nil)
				bool8bldr.NewArray()
			}
		})
	}
}

func BenchmarkWriteBooleanArray(b *testing.B) {
	boolbldr := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer boolbldr.Release()

	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {

			values := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}

			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				boolbldr.AppendValues(values, nil)
				boolbldr.NewArray()
			}
		})
	}
}

func BenchmarkReadBool8Array(b *testing.B) {
	extBuilder := array.NewExtensionBuilder(memory.DefaultAllocator, types.NewBool8Type())
	defer extBuilder.Release()

	bool8bldr := types.NewBool8Builder(extBuilder)
	defer bool8bldr.Release()

	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {

			values := make([]bool, sz)
			output := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}

			bool8bldr.AppendValues(values, nil)
			bool8Arr := bool8bldr.NewArray().(*types.Bool8Array)
			defer bool8Arr.Release()

			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				for i := 0; i < bool8Arr.Len(); i++ {
					output[i] = bool8Arr.Value(i)
				}
			}
		})
	}
}

func BenchmarkReadBooleanArray(b *testing.B) {
	boolbldr := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer boolbldr.Release()

	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {

			values := make([]bool, sz)
			output := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}

			boolbldr.AppendValues(values, nil)
			boolArr := boolbldr.NewArray().(*array.Boolean)
			defer boolArr.Release()

			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				for i := 0; i < boolArr.Len(); i++ {
					output[i] = boolArr.Value(i)
				}
			}
		})
	}
}

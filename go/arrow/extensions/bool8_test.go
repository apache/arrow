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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	MINSIZE = 1024
	MAXSIZE = 65536
)

func TestBool8ExtensionBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	builder := extensions.NewBool8Builder(mem)
	defer builder.Release()

	builder.Append(true)
	builder.AppendNull()
	builder.Append(false)
	arr := builder.NewArray()
	defer arr.Release()

	arrStr := arr.String()
	require.Equal(t, "[true (null) false]", arrStr)

	jsonStr, err := json.Marshal(arr)
	require.NoError(t, err)

	arr1, _, err := array.FromJSON(mem, extensions.NewBool8Type(), bytes.NewReader(jsonStr))
	require.NoError(t, err)
	defer arr1.Release()

	require.Equal(t, arr, arr1)
}

func TestBool8ExtensionRecordBuilder(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bool8", Type: extensions.NewBool8Type()},
	}, nil)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*extensions.Bool8Builder).Append(true)
	record := builder.NewRecord()
	defer record.Release()

	b, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "[{\"bool8\":true}\n]", string(b))

	record1, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(b))
	require.NoError(t, err)
	defer record1.Release()

	require.Equal(t, record, record1)

	require.NoError(t, builder.UnmarshalJSON([]byte(`{"bool8":true}`)))
	record = builder.NewRecord()
	defer record.Release()

	require.Equal(t, schema, record.Schema())
	require.Equal(t, true, record.Column(0).(*extensions.Bool8Array).Value(0))
}

func TestBool8StringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := extensions.NewBool8Builder(mem)
	b.Append(true)
	b.AppendNull()
	b.Append(false)
	b.AppendNull()
	b.Append(true)

	arr := b.NewArray()
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := extensions.NewBool8Builder(mem)
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

	bool8bldr := extensions.NewBool8Builder(mem)
	defer bool8bldr.Release()

	boolbldr := array.NewBooleanBuilder(mem)
	defer boolbldr.Release()

	inputVals := []bool{true, false, false, false, true}
	inputValidity := []bool{true, false, true, false, true}

	bool8bldr.AppendValues(inputVals, inputValidity)
	bool8Arr := bool8bldr.NewExtensionArray().(*extensions.Bool8Array)
	defer bool8Arr.Release()

	boolbldr.AppendValues(inputVals, inputValidity)
	boolArr := boolbldr.NewBooleanArray()
	defer boolArr.Release()

	require.Equal(t, boolArr.Len(), bool8Arr.Len())
	for i := 0; i < boolArr.Len(); i++ {
		require.Equal(t, boolArr.Value(i), bool8Arr.Value(i))
	}
}

func TestReinterpretStorageEqualToValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bool8bldr := extensions.NewBool8Builder(mem)
	defer bool8bldr.Release()

	inputVals := []bool{true, false, false, false, true}
	inputValidity := []bool{true, false, true, false, true}

	bool8bldr.AppendValues(inputVals, inputValidity)
	bool8Arr := bool8bldr.NewExtensionArray().(*extensions.Bool8Array)
	defer bool8Arr.Release()

	boolValsCopy := make([]bool, bool8Arr.Len())
	for i := 0; i < bool8Arr.Len(); i++ {
		boolValsCopy[i] = bool8Arr.Value(i)
	}

	boolValsZeroCopy := bool8Arr.BoolValues()

	require.Equal(t, len(boolValsZeroCopy), len(boolValsCopy))
	for i := range boolValsCopy {
		require.Equal(t, boolValsZeroCopy[i], boolValsCopy[i])
	}
}

func TestBool8TypeBatchIPCRoundTrip(t *testing.T) {
	typ := extensions.NewBool8Type()
	arrow.RegisterExtensionType(typ)
	defer arrow.UnregisterExtensionType(typ.ExtensionName())

	storage, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[-1, 0, 1, 2, null]`))
	require.NoError(t, err)
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
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

func BenchmarkWriteBool8Array(b *testing.B) {
	bool8bldr := extensions.NewBool8Builder(memory.DefaultAllocator)
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

// storage benchmark result at package level to prevent compiler from eliminating the function call
var result []bool

func BenchmarkReadBool8Array(b *testing.B) {
	bool8bldr := extensions.NewBool8Builder(memory.DefaultAllocator)
	defer bool8bldr.Release()

	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {

			values := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}

			bool8bldr.AppendValues(values, nil)
			bool8Arr := bool8bldr.NewArray().(*extensions.Bool8Array)
			defer bool8Arr.Release()

			var r []bool
			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				r = bool8Arr.BoolValues()
			}
			result = r
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

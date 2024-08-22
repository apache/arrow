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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/extensions"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONTypeBasics(t *testing.T) {
	typ, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	require.NoError(t, err)

	typLarge, err := extensions.NewJSONType(arrow.BinaryTypes.LargeString)
	require.NoError(t, err)

	typView, err := extensions.NewJSONType(arrow.BinaryTypes.StringView)
	require.NoError(t, err)

	assert.Equal(t, "arrow.json", typ.ExtensionName())
	assert.Equal(t, "arrow.json", typLarge.ExtensionName())
	assert.Equal(t, "arrow.json", typView.ExtensionName())

	assert.True(t, typ.ExtensionEquals(typ))
	assert.True(t, typLarge.ExtensionEquals(typLarge))
	assert.True(t, typView.ExtensionEquals(typView))

	assert.False(t, arrow.TypeEqual(arrow.BinaryTypes.String, typ))
	assert.False(t, arrow.TypeEqual(typ, typLarge))
	assert.False(t, arrow.TypeEqual(typ, typView))
	assert.False(t, arrow.TypeEqual(typLarge, typView))

	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, typ.StorageType()))
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.LargeString, typLarge.StorageType()))
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.StringView, typView.StorageType()))

	assert.Equal(t, "extension<arrow.json[storage_type=utf8]>", typ.String())
	assert.Equal(t, "extension<arrow.json[storage_type=large_utf8]>", typLarge.String())
	assert.Equal(t, "extension<arrow.json[storage_type=string_view]>", typView.String())
}

var jsonTestCases = []struct {
	Name           string
	StorageType    arrow.DataType
	StorageBuilder func(mem memory.Allocator) array.Builder
}{
	{
		Name:           "string",
		StorageType:    arrow.BinaryTypes.String,
		StorageBuilder: func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
	},
	{
		Name:           "large_string",
		StorageType:    arrow.BinaryTypes.LargeString,
		StorageBuilder: func(mem memory.Allocator) array.Builder { return array.NewLargeStringBuilder(mem) },
	},
	{
		Name:           "string_view",
		StorageType:    arrow.BinaryTypes.StringView,
		StorageBuilder: func(mem memory.Allocator) array.Builder { return array.NewStringViewBuilder(mem) },
	},
}

func TestJSONTypeCreateFromArray(t *testing.T) {
	for _, tc := range jsonTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			typ, err := extensions.NewJSONType(tc.StorageType)
			require.NoError(t, err)

			bldr := tc.StorageBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValueFromString(`"foobar"`)
			bldr.AppendNull()
			bldr.AppendValueFromString(`{"foo": "bar"}`)
			bldr.AppendValueFromString(`42`)
			bldr.AppendValueFromString(`true`)
			bldr.AppendValueFromString(`[1, true, "3", null, {"five": 5}]`)

			storage := bldr.NewArray()
			defer storage.Release()

			arr := array.NewExtensionArrayWithStorage(typ, storage)
			defer arr.Release()

			assert.Equal(t, 6, arr.Len())
			assert.Equal(t, 1, arr.NullN())

			jsonArr, ok := arr.(*extensions.JSONArray)
			require.True(t, ok)

			require.Equal(t, "foobar", jsonArr.Value(0))
			require.Equal(t, nil, jsonArr.Value(1))
			require.Equal(t, map[string]any{"foo": "bar"}, jsonArr.Value(2))
			require.Equal(t, float64(42), jsonArr.Value(3))
			require.Equal(t, true, jsonArr.Value(4))
			require.Equal(t, []any{float64(1), true, "3", nil, map[string]any{"five": float64(5)}}, jsonArr.Value(5))
		})
	}
}

func TestJSONTypeBatchIPCRoundTrip(t *testing.T) {
	for _, tc := range jsonTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			typ, err := extensions.NewJSONType(tc.StorageType)
			require.NoError(t, err)

			bldr := tc.StorageBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValueFromString(`"foobar"`)
			bldr.AppendNull()
			bldr.AppendValueFromString(`{"foo": "bar"}`)
			bldr.AppendValueFromString(`42`)
			bldr.AppendValueFromString(`true`)
			bldr.AppendValueFromString(`[1, true, "3", null, {"five": 5}]`)

			storage := bldr.NewArray()
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
		})
	}
}

func TestMarshallJSONArray(t *testing.T) {
	for _, tc := range jsonTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			typ, err := extensions.NewJSONType(tc.StorageType)
			require.NoError(t, err)

			bldr := tc.StorageBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValueFromString(`"foobar"`)
			bldr.AppendNull()
			bldr.AppendValueFromString(`{"foo": "bar"}`)
			bldr.AppendValueFromString(`42`)
			bldr.AppendValueFromString(`true`)
			bldr.AppendValueFromString(`[1, true, "3", null, {"five": 5}]`)

			storage := bldr.NewArray()
			defer storage.Release()

			arr := array.NewExtensionArrayWithStorage(typ, storage)
			defer arr.Release()

			assert.Equal(t, 6, arr.Len())
			assert.Equal(t, 1, arr.NullN())

			jsonArr, ok := arr.(*extensions.JSONArray)
			require.True(t, ok)

			b, err := jsonArr.MarshalJSON()
			require.NoError(t, err)

			expectedJSON := `["foobar",null,{"foo":"bar"},42,true,[1,true,"3",null,{"five":5}]]`
			require.Equal(t, expectedJSON, string(b))
			require.Equal(t, expectedJSON, jsonArr.String())
		})
	}
}

func TestJSONRecordToJSON(t *testing.T) {
	for _, tc := range jsonTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			typ, err := extensions.NewJSONType(tc.StorageType)
			require.NoError(t, err)

			bldr := tc.StorageBuilder(memory.DefaultAllocator)
			defer bldr.Release()

			bldr.AppendValueFromString(`"foobar"`)
			bldr.AppendNull()
			bldr.AppendValueFromString(`{"foo": "bar"}`)
			bldr.AppendValueFromString(`42`)
			bldr.AppendValueFromString(`true`)
			bldr.AppendValueFromString(`[1, true, "3", null, {"five": 5}]`)

			storage := bldr.NewArray()
			defer storage.Release()

			arr := array.NewExtensionArrayWithStorage(typ, storage)
			defer arr.Release()

			assert.Equal(t, 6, arr.Len())
			assert.Equal(t, 1, arr.NullN())

			jsonArr, ok := arr.(*extensions.JSONArray)
			require.True(t, ok)

			rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "json", Type: typ, Nullable: true}}, nil), []arrow.Array{jsonArr}, 6)
			defer rec.Release()

			buf := bytes.NewBuffer([]byte("\n")) // expected output has leading newline for clearer formatting
			require.NoError(t, array.RecordToJSON(rec, buf))

			expectedJSON := `
				{"json":"foobar"}
				{"json":null}
				{"json":{"foo":"bar"}}
				{"json":42}
				{"json":true}
				{"json":[1,true,"3",null,{"five":5}]}
			`

			expectedJSONLines := strings.Split(expectedJSON, "\n")
			actualJSONLines := strings.Split(buf.String(), "\n")

			require.Equal(t, len(expectedJSONLines), len(actualJSONLines))
			for i := range expectedJSONLines {
				if strings.TrimSpace(expectedJSONLines[i]) != "" {
					require.JSONEq(t, expectedJSONLines[i], actualJSONLines[i])
				}
			}
		})
	}
}

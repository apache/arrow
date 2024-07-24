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

func TestOpaqueTypeBasics(t *testing.T) {
	typ := extensions.NewOpaqueType(arrow.Null, "type", "vendor")
	typ2 := extensions.NewOpaqueType(arrow.Null, "type2", "vendor")

	assert.Equal(t, "arrow.opaque", typ.ExtensionName())
	assert.True(t, typ.ExtensionEquals(typ))
	assert.False(t, arrow.TypeEqual(arrow.Null, typ))
	assert.False(t, arrow.TypeEqual(typ, typ2))
	assert.True(t, arrow.TypeEqual(arrow.Null, typ.StorageType()))
	assert.JSONEq(t, `{"type_name": "type", "vendor_name": "vendor"}`, typ.Serialize())
	assert.Equal(t, "type", typ.TypeName)
	assert.Equal(t, "vendor", typ.VendorName)
	assert.Equal(t, "extension<arrow.opaque[storage_type=null, type_name=type, vendor_name=vendor]>",
		typ.String())
}

func TestOpaqueTypeEquals(t *testing.T) {
	typ := extensions.NewOpaqueType(arrow.Null, "type", "vendor")
	typ2 := extensions.NewOpaqueType(arrow.Null, "type2", "vendor")
	typ3 := extensions.NewOpaqueType(arrow.Null, "type", "vendor2")
	typ4 := extensions.NewOpaqueType(arrow.PrimitiveTypes.Int64, "type", "vendor")
	typ5 := extensions.NewOpaqueType(arrow.Null, "type", "vendor")

	tests := []struct {
		lhs, rhs arrow.ExtensionType
		expected bool
	}{
		{typ, typ, true},
		{typ2, typ2, true},
		{typ3, typ3, true},
		{typ4, typ4, true},
		{typ5, typ5, true},
		{typ, typ5, true},
		{typ, typ2, false},
		{typ, typ3, false},
		{typ, typ4, false},
		{typ2, typ, false},
		{typ2, typ3, false},
		{typ2, typ4, false},
		{typ3, typ, false},
		{typ3, typ2, false},
		{typ3, typ4, false},
		{typ4, typ, false},
		{typ4, typ2, false},
		{typ4, typ3, false},
	}

	for _, tt := range tests {
		assert.Equalf(t, tt.expected, arrow.TypeEqual(tt.lhs, tt.rhs),
			"%s == %s", tt.lhs, tt.rhs)
	}
}

func TestOpaqueTypeCreateFromArray(t *testing.T) {
	typ := extensions.NewOpaqueType(arrow.BinaryTypes.String, "geometry", "adbc.postgresql")
	storage, _, err := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String,
		strings.NewReader(`["foobar", null]`))
	require.NoError(t, err)
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
	defer arr.Release()

	assert.Equal(t, 2, arr.Len())
	assert.Equal(t, 1, arr.NullN())
}

func TestOpaqueTypeDeserialize(t *testing.T) {
	tests := []struct {
		serialized string
		expected   *extensions.OpaqueType
	}{
		{`{"type_name": "type", "vendor_name": "vendor"}`,
			extensions.NewOpaqueType(arrow.Null, "type", "vendor")},
		{`{"type_name": "long name", "vendor_name": "long name"}`,
			extensions.NewOpaqueType(arrow.Null, "long name", "long name")},
		{`{"type_name": "名前", "vendor_name": "名字"}`,
			extensions.NewOpaqueType(arrow.Null, "名前", "名字")},
		{`{"type_name": "type", "vendor_name": "vendor", "extra_field": 2}`,
			extensions.NewOpaqueType(arrow.Null, "type", "vendor")},
	}

	for _, tt := range tests {
		deserialized, err := tt.expected.Deserialize(tt.expected.Storage, tt.serialized)
		require.NoError(t, err)
		assert.Truef(t, arrow.TypeEqual(tt.expected, deserialized), "%s != %s",
			tt.expected, deserialized)
	}

	typ := extensions.NewOpaqueType(arrow.Null, "type", "vendor")
	_, err := typ.Deserialize(arrow.Null, "")
	assert.ErrorContains(t, err, "unexpected end of JSON input")

	_, err = typ.Deserialize(arrow.Null, "[]")
	assert.ErrorContains(t, err, "cannot unmarshal array")

	_, err = typ.Deserialize(arrow.Null, "{}")
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assert.ErrorContains(t, err, "serialized JSON data for OpaqueType missing type_name")

	_, err = typ.Deserialize(arrow.Null, `{"type_name": ""}`)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assert.ErrorContains(t, err, "serialized JSON data for OpaqueType missing type_name")

	_, err = typ.Deserialize(arrow.Null, `{"type_name": "type"}`)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assert.ErrorContains(t, err, "serialized JSON data for OpaqueType missing vendor_name")

	_, err = typ.Deserialize(arrow.Null, `{"type_name": "type", "vendor_name": ""}`)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assert.ErrorContains(t, err, "serialized JSON data for OpaqueType missing vendor_name")
}

func TestOpaqueTypeMetadataRoundTrip(t *testing.T) {
	tests := []*extensions.OpaqueType{
		extensions.NewOpaqueType(arrow.Null, "foo", "bar"),
		extensions.NewOpaqueType(arrow.BinaryTypes.Binary, "geometry", "postgis"),
		extensions.NewOpaqueType(arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int64), "foo", "bar"),
		extensions.NewOpaqueType(arrow.BinaryTypes.String, "foo", "bar"),
	}

	for _, tt := range tests {
		serialized := tt.Serialize()
		deserialized, err := tt.Deserialize(tt.Storage, serialized)
		require.NoError(t, err)
		assert.Truef(t, arrow.TypeEqual(tt, deserialized), "%s != %s", tt, deserialized)
	}
}

func TestOpaqueTypeBatchRoundTrip(t *testing.T) {
	typ := extensions.NewOpaqueType(arrow.BinaryTypes.String, "geometry", "adbc.postgresql")
	arrow.RegisterExtensionType(typ)
	defer arrow.UnregisterExtensionType(typ.ExtensionName())

	storage, _, err := array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String,
		strings.NewReader(`["foobar", null]`))
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

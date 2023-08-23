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
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/dictutils"
	"github.com/apache/arrow/go/v14/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
)

func TestRWSchema(t *testing.T) {
	meta := arrow.NewMetadata([]string{"k1", "k2", "k3"}, []string{"v1", "v2", "v3"})

	mType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	mType.SetItemNullable(false)
	for _, tc := range []struct {
		schema *arrow.Schema
		memo   dictutils.Memo
	}{
		{
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f2", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f3", Type: arrow.PrimitiveTypes.Float64},
				{Name: "f4", Type: mType},
			}, &meta),
			memo: dictutils.Memo{},
		},
	} {
		t.Run("", func(t *testing.T) {
			b := flatbuffers.NewBuilder(0)

			tc.memo.Mapper.ImportSchema(tc.schema)
			offset := schemaToFB(b, tc.schema, &tc.memo.Mapper)
			b.Finish(offset)

			buf := b.FinishedBytes()

			fb := flatbuf.GetRootAsSchema(buf, 0)
			got, err := schemaFromFB(fb, &tc.memo)
			if err != nil {
				t.Fatal(err)
			}

			if !got.Equal(tc.schema) {
				t.Fatalf("r/w schema failed:\ngot = %#v\nwant= %#v\n", got, tc.schema)
			}

			{
				got := got.Metadata()
				want := tc.schema.Metadata()
				if got.Len() != want.Len() {
					t.Fatalf("invalid metadata len: got=%d, want=%d", got.Len(), want.Len())
				}
				if got, want := got.Keys(), want.Keys(); !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid metadata keys:\ngot =%v\nwant=%v\n", got, want)
				}
				if got, want := got.Values(), want.Values(); !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid metadata values:\ngot =%v\nwant=%v\n", got, want)
				}
			}
		})
	}
}

func TestRWFooter(t *testing.T) {
	for _, tc := range []struct {
		schema *arrow.Schema
		dicts  []fileBlock
		recs   []fileBlock
	}{
		{
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f2", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f3", Type: arrow.PrimitiveTypes.Float64},
			}, nil),
			dicts: []fileBlock{
				{Offset: 1, Meta: 2, Body: 3},
				{Offset: 4, Meta: 5, Body: 6},
				{Offset: 7, Meta: 8, Body: 9},
			},
			recs: []fileBlock{
				{Offset: 0, Meta: 10, Body: 30},
				{Offset: 10, Meta: 30, Body: 60},
				{Offset: 20, Meta: 30, Body: 40},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			o := new(bytes.Buffer)

			err := writeFileFooter(tc.schema, tc.dicts, tc.recs, o)
			if err != nil {
				t.Fatal(err)
			}

			footer := flatbuf.GetRootAsFooter(o.Bytes(), 0)

			if got, want := MetadataVersion(footer.Version()), currentMetadataVersion; got != want {
				t.Errorf("invalid metadata version: got=%[1]d %#[1]x, want=%[2]d %#[2]x", int16(got), int16(want))
			}

			schema, err := schemaFromFB(footer.Schema(nil), nil)
			if err != nil {
				t.Fatal(err)
			}

			if !schema.Equal(tc.schema) {
				t.Fatalf("schema r/w error:\ngot= %v\nwant=%v", schema, tc.schema)
			}

			if got, want := footer.DictionariesLength(), len(tc.dicts); got != want {
				t.Fatalf("dicts len differ: got=%d, want=%d", got, want)
			}

			for i, dict := range tc.dicts {
				var blk flatbuf.Block
				if !footer.Dictionaries(&blk, i) {
					t.Fatalf("could not get dictionary %d", i)
				}
				got := fileBlock{Offset: blk.Offset(), Meta: blk.MetaDataLength(), Body: blk.BodyLength()}
				want := dict
				if got != want {
					t.Errorf("dict[%d] differ:\ngot= %v\nwant=%v", i, got, want)
				}
			}

			if got, want := footer.RecordBatchesLength(), len(tc.recs); got != want {
				t.Fatalf("recs len differ: got=%d, want=%d", got, want)
			}

			for i, rec := range tc.recs {
				var blk flatbuf.Block
				if !footer.RecordBatches(&blk, i) {
					t.Fatalf("could not get record %d", i)
				}
				got := fileBlock{Offset: blk.Offset(), Meta: blk.MetaDataLength(), Body: blk.BodyLength()}
				want := rec
				if got != want {
					t.Errorf("record[%d] differ:\ngot= %v\nwant=%v", i, got, want)
				}
			}
		})
	}
}

func exampleUUID(mem memory.Allocator) arrow.Array {
	extType := types.NewUUIDType()
	bldr := array.NewExtensionBuilder(mem, extType)
	defer bldr.Release()

	bldr.Builder.(*array.FixedSizeBinaryBuilder).AppendValues(
		[][]byte{nil, []byte("abcdefghijklmno0"), []byte("abcdefghijklmno1"), []byte("abcdefghijklmno2")},
		[]bool{false, true, true, true})

	return bldr.NewArray()
}

func TestUnrecognizedExtensionType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// register the uuid type
	assert.NoError(t, arrow.RegisterExtensionType(types.NewUUIDType()))

	extArr := exampleUUID(pool)
	defer extArr.Release()

	batch := array.NewRecord(
		arrow.NewSchema([]arrow.Field{
			{Name: "f0", Type: extArr.DataType(), Nullable: true}}, nil),
		[]arrow.Array{extArr}, 4)
	defer batch.Release()

	storageArr := extArr.(array.ExtensionArray).Storage()

	var buf bytes.Buffer
	wr := NewWriter(&buf, WithAllocator(pool), WithSchema(batch.Schema()))
	assert.NoError(t, wr.Write(batch))
	wr.Close()

	// unregister the uuid type before we read back the buffer so it is
	// unrecognized when reading back the record batch.
	assert.NoError(t, arrow.UnregisterExtensionType("uuid"))
	rdr, err := NewReader(&buf, WithAllocator(pool))
	defer rdr.Release()

	assert.NoError(t, err)
	assert.True(t, rdr.Next())

	rec := rdr.Record()
	assert.NotNil(t, rec)

	// create a record batch with the same data, but the field should contain the
	// extension metadata and be of the storage type instead of being the extension type.
	extMetadata := arrow.NewMetadata([]string{ExtensionTypeKeyName, ExtensionMetadataKeyName}, []string{"uuid", "uuid-serialized"})
	batchNoExt := array.NewRecord(
		arrow.NewSchema([]arrow.Field{
			{Name: "f0", Type: storageArr.DataType(), Nullable: true, Metadata: extMetadata},
		}, nil), []arrow.Array{storageArr}, 4)
	defer batchNoExt.Release()

	assert.Truef(t, array.RecordEqual(rec, batchNoExt), "expected: %s\ngot: %s\n", batchNoExt, rec)
}

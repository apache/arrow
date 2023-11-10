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

package metadata_test

import (
	"context"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/metadata"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTableMetaData(schema *schema.Schema, props *parquet.WriterProperties, nrows int, statsInt, statsFloat metadata.EncodedStatistics) (*metadata.FileMetaData, error) {
	fbuilder := metadata.NewFileMetadataBuilder(schema, props, nil)
	rg1Builder := fbuilder.AppendRowGroup()
	// metadata
	// row group 1
	col1Builder := rg1Builder.NextColumnChunk()
	col2Builder := rg1Builder.NextColumnChunk()
	// column metadata
	dictEncodingStats := map[parquet.Encoding]int32{parquet.Encodings.RLEDict: 1}
	dataEncodingStats := map[parquet.Encoding]int32{parquet.Encodings.Plain: 1, parquet.Encodings.RLE: 1}
	statsInt.Signed = true
	col1Builder.SetStats(statsInt)
	statsFloat.Signed = true
	col2Builder.SetStats(statsFloat)

	col1Builder.Finish(metadata.ChunkMetaInfo{int64(nrows) / 2, 4, 0, 10, 512, 600}, true, false, metadata.EncodingStats{dictEncodingStats, dataEncodingStats}, nil)
	col2Builder.Finish(metadata.ChunkMetaInfo{int64(nrows) / 2, 24, 0, 30, 512, 600}, true, false, metadata.EncodingStats{dictEncodingStats, dataEncodingStats}, nil)

	rg1Builder.SetNumRows(nrows / 2)
	rg1Builder.Finish(1024, -1)

	// rowgroup2 metadata
	rg2Builder := fbuilder.AppendRowGroup()
	col1Builder = rg2Builder.NextColumnChunk()
	col2Builder = rg2Builder.NextColumnChunk()
	// column metadata
	col1Builder.SetStats(statsInt)
	col2Builder.SetStats(statsFloat)
	dictEncodingStats = make(map[parquet.Encoding]int32)
	col1Builder.Finish(metadata.ChunkMetaInfo{int64(nrows) / 2, 0 /*dictionary page offset*/, 0, 10, 512, 600}, false /* has dictionary */, false, metadata.EncodingStats{dictEncodingStats, dataEncodingStats}, nil)
	col2Builder.Finish(metadata.ChunkMetaInfo{int64(nrows) / 2, 16, 0, 26, 512, 600}, true, false, metadata.EncodingStats{dictEncodingStats, dataEncodingStats}, nil)

	rg2Builder.SetNumRows(nrows / 2)
	rg2Builder.Finish(1024, -1)

	return fbuilder.Finish()
}

func assertStatsSet(t *testing.T, m *metadata.ColumnChunkMetaData) {
	ok, err := m.StatsSet()
	assert.NoError(t, err)
	assert.True(t, ok)
}

func assertStats(t *testing.T, m *metadata.ColumnChunkMetaData) metadata.TypedStatistics {
	s, err := m.Statistics()
	assert.NoError(t, err)
	assert.NotNil(t, s)
	return s
}

func TestBuildAccess(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithVersion(parquet.V2_LATEST))

	fields := schema.FieldList{
		schema.NewInt32Node("int_col", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float_col", parquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)
	schema := schema.NewSchema(root)

	var (
		nrows      int64   = 1000
		intMin     int32   = 100
		intMax     int32   = 200
		floatMin   float32 = 100.100
		floatMax   float32 = 200.200
		statsInt   metadata.EncodedStatistics
		statsFloat metadata.EncodedStatistics
	)

	statsInt.SetNullCount(0).
		SetDistinctCount(nrows).
		SetMin((*(*[4]byte)(unsafe.Pointer(&intMin)))[:]).
		SetMax((*(*[4]byte)(unsafe.Pointer(&intMax)))[:])

	statsFloat.SetNullCount(0).
		SetDistinctCount(nrows).
		SetMin((*(*[4]byte)(unsafe.Pointer(&floatMin)))[:]).
		SetMax((*(*[4]byte)(unsafe.Pointer(&floatMax)))[:])

	faccessor, err := generateTableMetaData(schema, props, int(nrows), statsInt, statsFloat)
	require.NoError(t, err)
	serialized, err := faccessor.SerializeString(context.Background())
	assert.NoError(t, err)
	faccessorCopy, err := metadata.NewFileMetaData([]byte(serialized), nil)
	assert.NoError(t, err)

	for _, accessor := range []*metadata.FileMetaData{faccessor, faccessorCopy} {
		// file metadata
		assert.Equal(t, nrows, accessor.NumRows)
		assert.Len(t, accessor.RowGroups, 2)
		assert.EqualValues(t, parquet.V2_LATEST, accessor.Version())
		assert.Equal(t, parquet.DefaultCreatedBy, accessor.GetCreatedBy())
		assert.Equal(t, 3, accessor.NumSchemaElements())

		// row group 1 metadata
		rg1Access := accessor.RowGroup(0)
		assert.Equal(t, 2, rg1Access.NumColumns())
		assert.Equal(t, nrows/2, rg1Access.NumRows())
		assert.Equal(t, int64(1024), rg1Access.TotalByteSize())
		assert.Equal(t, int64(1024), rg1Access.TotalCompressedSize())

		rg1Col1, err := rg1Access.ColumnChunk(0)
		assert.NoError(t, err)
		assert.Equal(t, rg1Access.FileOffset(), rg1Col1.DictionaryPageOffset())

		rg1Col2, err := rg1Access.ColumnChunk(1)
		assert.NoError(t, err)
		assertStatsSet(t, rg1Col1)
		assertStatsSet(t, rg1Col2)
		assert.Equal(t, statsInt.Min, assertStats(t, rg1Col1).EncodeMin())
		assert.Equal(t, statsInt.Max, assertStats(t, rg1Col1).EncodeMax())
		assert.Equal(t, statsFloat.Min, assertStats(t, rg1Col2).EncodeMin())
		assert.Equal(t, statsFloat.Max, assertStats(t, rg1Col2).EncodeMax())
		assert.Zero(t, assertStats(t, rg1Col1).NullCount())
		assert.Zero(t, assertStats(t, rg1Col2).NullCount())
		assert.Equal(t, nrows, assertStats(t, rg1Col1).DistinctCount())
		assert.Equal(t, nrows, assertStats(t, rg1Col2).DistinctCount())
		assert.Equal(t, metadata.DefaultCompressionType, rg1Col1.Compression())
		assert.Equal(t, metadata.DefaultCompressionType, rg1Col2.Compression())
		assert.Equal(t, nrows/2, rg1Col1.NumValues())
		assert.Equal(t, nrows/2, rg1Col2.NumValues())
		assert.Len(t, rg1Col1.Encodings(), 3)
		assert.Len(t, rg1Col2.Encodings(), 3)
		assert.EqualValues(t, 512, rg1Col1.TotalCompressedSize())
		assert.EqualValues(t, 512, rg1Col2.TotalCompressedSize())
		assert.EqualValues(t, 600, rg1Col1.TotalUncompressedSize())
		assert.EqualValues(t, 600, rg1Col2.TotalUncompressedSize())
		assert.EqualValues(t, 4, rg1Col1.DictionaryPageOffset())
		assert.EqualValues(t, 24, rg1Col2.DictionaryPageOffset())
		assert.EqualValues(t, 10, rg1Col1.DataPageOffset())
		assert.EqualValues(t, 30, rg1Col2.DataPageOffset())
		assert.Len(t, rg1Col1.EncodingStats(), 3)
		assert.Len(t, rg1Col2.EncodingStats(), 3)

		// row group 2 metadata
		rg2Access := accessor.RowGroup(1)
		assert.Equal(t, 2, rg2Access.NumColumns())
		assert.Equal(t, nrows/2, rg2Access.NumRows())
		assert.EqualValues(t, 1024, rg2Access.TotalByteSize())
		assert.EqualValues(t, 1024, rg2Access.TotalCompressedSize())

		rg2Col1, err := rg2Access.ColumnChunk(0)
		assert.NoError(t, err)
		assert.Equal(t, rg2Access.FileOffset(), rg2Col1.DataPageOffset())

		rg2Col2, err := rg2Access.ColumnChunk(1)
		assert.NoError(t, err)
		assertStatsSet(t, rg1Col1)
		assertStatsSet(t, rg1Col2)
		assert.Equal(t, statsInt.Min, assertStats(t, rg1Col1).EncodeMin())
		assert.Equal(t, statsInt.Max, assertStats(t, rg1Col1).EncodeMax())
		assert.Equal(t, statsFloat.Min, assertStats(t, rg1Col2).EncodeMin())
		assert.Equal(t, statsFloat.Max, assertStats(t, rg1Col2).EncodeMax())
		assert.Zero(t, assertStats(t, rg1Col1).NullCount())
		assert.Zero(t, assertStats(t, rg1Col2).NullCount())
		assert.Equal(t, nrows, assertStats(t, rg1Col1).DistinctCount())
		assert.Equal(t, nrows, assertStats(t, rg1Col2).DistinctCount())
		assert.Equal(t, metadata.DefaultCompressionType, rg2Col1.Compression())
		assert.Equal(t, metadata.DefaultCompressionType, rg2Col2.Compression())
		assert.Equal(t, nrows/2, rg2Col1.NumValues())
		assert.Equal(t, nrows/2, rg2Col2.NumValues())
		assert.Len(t, rg2Col1.Encodings(), 2)
		assert.Len(t, rg2Col2.Encodings(), 3)
		assert.EqualValues(t, 512, rg2Col1.TotalCompressedSize())
		assert.EqualValues(t, 512, rg2Col2.TotalCompressedSize())
		assert.EqualValues(t, 600, rg2Col1.TotalUncompressedSize())
		assert.EqualValues(t, 600, rg2Col2.TotalUncompressedSize())
		assert.EqualValues(t, 0, rg2Col1.DictionaryPageOffset())
		assert.EqualValues(t, 16, rg2Col2.DictionaryPageOffset())
		assert.EqualValues(t, 10, rg2Col1.DataPageOffset())
		assert.EqualValues(t, 26, rg2Col2.DataPageOffset())
		assert.Len(t, rg2Col1.EncodingStats(), 2)
		assert.Len(t, rg2Col2.EncodingStats(), 2)

		assert.Empty(t, rg2Col1.FilePath())
		accessor.SetFilePath("/foo/bar/bar.parquet")
		assert.Equal(t, "/foo/bar/bar.parquet", rg2Col1.FilePath())
	}

	faccessor2, err := generateTableMetaData(schema, props, int(nrows), statsInt, statsFloat)
	require.NoError(t, err)
	faccessor.AppendRowGroups(faccessor2)
	assert.Len(t, faccessor.RowGroups, 4)
	assert.Equal(t, nrows*2, faccessor.NumRows)
	assert.EqualValues(t, parquet.V2_LATEST, faccessor.Version())
	assert.Equal(t, parquet.DefaultCreatedBy, faccessor.GetCreatedBy())
	assert.Equal(t, 3, faccessor.NumSchemaElements())

	faccessor1, err := faccessor.Subset([]int{2, 3})
	require.NoError(t, err)
	assert.True(t, faccessor1.Equals(faccessor2))

	faccessor1, err = faccessor2.Subset([]int{0})
	require.NoError(t, err)

	next, err := faccessor.Subset([]int{0})
	require.NoError(t, err)
	faccessor1.AppendRowGroups(next)

	sub, err := faccessor.Subset([]int{2, 0})
	require.NoError(t, err)
	assert.True(t, faccessor1.Equals(sub))
}

func TestV1VersionMetadata(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0))

	fields := schema.FieldList{
		schema.NewInt32Node("int_col", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float_col", parquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)
	schema := schema.NewSchema(root)

	fbuilder := metadata.NewFileMetadataBuilder(schema, props, nil)
	faccessor, err := fbuilder.Finish()
	require.NoError(t, err)
	assert.EqualValues(t, parquet.V1_0, faccessor.Version())
}

func TestKeyValueMetadata(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0))

	fields := schema.FieldList{
		schema.NewInt32Node("int_col", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float_col", parquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)
	schema := schema.NewSchema(root)
	kvmeta := metadata.NewKeyValueMetadata()
	kvmeta.Append("test_key", "test_value")

	fbuilder := metadata.NewFileMetadataBuilder(schema, props, kvmeta)
	faccessor, err := fbuilder.Finish()
	require.NoError(t, err)

	assert.True(t, faccessor.KeyValueMetadata().Equals(kvmeta))
}

func TestKeyValueMetadataAppend(t *testing.T) {
	props := parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0))

	fields := schema.FieldList{
		schema.NewInt32Node("int_col", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float_col", parquet.Repetitions.Required, -1),
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
	require.NoError(t, err)
	schema := schema.NewSchema(root)

	kvmeta := metadata.NewKeyValueMetadata()
	key1 := "test_key1"
	value1 := "test_value1"
	require.NoError(t, kvmeta.Append(key1, value1))

	fbuilder := metadata.NewFileMetadataBuilder(schema, props, kvmeta)

	key2 := "test_key2"
	value2 := "test_value2"
	require.NoError(t, fbuilder.AppendKeyValueMetadata(key2, value2))
	faccessor, err := fbuilder.Finish()
	require.NoError(t, err)

	kv := faccessor.KeyValueMetadata()

	got1 := kv.FindValue(key1)
	require.NotNil(t, got1)
	assert.Equal(t, value1, *got1)

	got2 := kv.FindValue(key2)
	require.NotNil(t, got2)
	assert.Equal(t, value2, *got2)
}

func TestApplicationVersion(t *testing.T) {
	version := metadata.NewAppVersion("parquet-mr version 1.7.9")
	version1 := metadata.NewAppVersion("parquet-mr version 1.8.0")
	version2 := metadata.NewAppVersion("parquet-cpp version 1.0.0")
	version3 := metadata.NewAppVersion("")
	version4 := metadata.NewAppVersion("parquet-mr version 1.5.0ab-cdh5.5.0+cd (build abcd)")
	version5 := metadata.NewAppVersion("parquet-mr")

	assert.Equal(t, "parquet-mr", version.App)
	assert.Equal(t, 1, version.Version.Major)
	assert.Equal(t, 7, version.Version.Minor)
	assert.Equal(t, 9, version.Version.Patch)

	assert.Equal(t, "parquet-cpp", version2.App)
	assert.Equal(t, 1, version2.Version.Major)
	assert.Equal(t, 0, version2.Version.Minor)
	assert.Equal(t, 0, version2.Version.Patch)

	assert.Equal(t, "parquet-mr", version4.App)
	assert.Equal(t, "abcd", version4.Build)
	assert.Equal(t, 1, version4.Version.Major)
	assert.Equal(t, 5, version4.Version.Minor)
	assert.Equal(t, 0, version4.Version.Patch)
	assert.Equal(t, "ab", version4.Version.Unknown)
	assert.Equal(t, "cdh5.5.0", version4.Version.PreRelease)
	assert.Equal(t, "cd", version4.Version.BuildInfo)

	assert.Equal(t, "parquet-mr", version5.App)
	assert.Equal(t, 0, version5.Version.Major)
	assert.Equal(t, 0, version5.Version.Minor)
	assert.Equal(t, 0, version5.Version.Patch)

	assert.True(t, version.LessThan(version1))

	var stats metadata.EncodedStatistics
	assert.False(t, version1.HasCorrectStatistics(parquet.Types.Int96, schema.NoLogicalType{}, stats, schema.SortUNKNOWN))
	assert.True(t, version.HasCorrectStatistics(parquet.Types.Int32, schema.NoLogicalType{}, stats, schema.SortSIGNED))
	assert.False(t, version.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, stats, schema.SortSIGNED))
	assert.True(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, stats, schema.SortSIGNED))
	assert.False(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, stats, schema.SortUNSIGNED))
	assert.True(t, version3.HasCorrectStatistics(parquet.Types.FixedLenByteArray, schema.NoLogicalType{}, stats, schema.SortSIGNED))

	// check that the old stats are correct if min and max are the same regardless of sort order
	var statsStr metadata.EncodedStatistics
	statsStr.SetMin([]byte("a")).SetMax([]byte("b"))
	assert.False(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, statsStr, schema.SortUNSIGNED))
	statsStr.SetMax([]byte("a"))
	assert.True(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, statsStr, schema.SortUNSIGNED))

	// check that the same holds true for ints
	var (
		intMin int32 = 100
		intMax int32 = 200
	)
	var statsInt metadata.EncodedStatistics
	statsInt.SetMin((*(*[4]byte)(unsafe.Pointer(&intMin)))[:])
	statsInt.SetMax((*(*[4]byte)(unsafe.Pointer(&intMax)))[:])
	assert.False(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, statsInt, schema.SortUNSIGNED))
	statsInt.SetMax((*(*[4]byte)(unsafe.Pointer(&intMin)))[:])
	assert.True(t, version1.HasCorrectStatistics(parquet.Types.ByteArray, schema.NoLogicalType{}, statsInt, schema.SortUNSIGNED))
}

func TestCheckBadDecimalStats(t *testing.T) {
	version1 := metadata.NewAppVersion("parquet-cpp version 3.0.0")
	version2 := metadata.NewAppVersion("parquet-cpp-arrow version 3.0.0")
	version3 := metadata.NewAppVersion("parquet-cpp-arrow version 4.0.0")

	var stats metadata.EncodedStatistics
	assert.False(t, version1.HasCorrectStatistics(parquet.Types.FixedLenByteArray, schema.NewDecimalLogicalType(5, 0), stats, schema.SortSIGNED))
	assert.False(t, version2.HasCorrectStatistics(parquet.Types.FixedLenByteArray, schema.NewDecimalLogicalType(5, 0), stats, schema.SortSIGNED))
	assert.True(t, version3.HasCorrectStatistics(parquet.Types.FixedLenByteArray, schema.NewDecimalLogicalType(5, 0), stats, schema.SortSIGNED))
}

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

package file_test

import (
	"bytes"
	"math"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/memory"
	arrutils "github.com/apache/arrow/go/v12/internal/utils"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/internal/encoding"
	"github.com/apache/arrow/go/v12/parquet/internal/encryption"
	format "github.com/apache/arrow/go/v12/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v12/parquet/internal/testutils"
	"github.com/apache/arrow/go/v12/parquet/internal/utils"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	"github.com/apache/arrow/go/v12/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	SmallSize = 100
	// larger to test some corner cases, only in some specific cases
	LargeSize = 100000
	// very large to test dictionary fallback
	VeryLargeSize = 400000
	// dictionary page size for testing fallback
	DictionaryPageSize = 1024 * 1024
)

type mockpagewriter struct {
	mock.Mock
}

func (m *mockpagewriter) Close(hasDict, fallBack bool) error {
	return m.Called(hasDict, fallBack).Error(0)
}
func (m *mockpagewriter) WriteDataPage(page file.DataPage) (int64, error) {
	args := m.Called(page)
	return int64(args.Int(0)), args.Error(1)
}
func (m *mockpagewriter) WriteDictionaryPage(page *file.DictionaryPage) (int64, error) {
	args := m.Called(page)
	return int64(args.Int(0)), args.Error(1)
}
func (m *mockpagewriter) HasCompressor() bool {
	return m.Called().Bool(0)
}
func (m *mockpagewriter) Compress(buf *bytes.Buffer, src []byte) []byte {
	return m.Called(buf, src).Get(0).([]byte)
}
func (m *mockpagewriter) Reset(sink utils.WriterTell, codec compress.Compression, compressionLevel int, metadata *metadata.ColumnChunkMetaDataBuilder, rgOrdinal, columnOrdinal int16, metaEncryptor, dataEncryptor encryption.Encryptor) error {
	return m.Called().Error(0)
}

func TestWriteDataPageV1NumValues(t *testing.T) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.ListOf(
			schema.Must(schema.NewPrimitiveNode("column", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1)),
			parquet.Repetitions.Optional, -1)),
	}, -1)))
	descr := sc.Column(0)
	props := parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithVersion(parquet.V1_0),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithDictionaryDefault(false))

	metadata := metadata.NewColumnChunkMetaDataBuilder(props, descr)
	pager := new(mockpagewriter)
	defer pager.AssertExpectations(t)
	pager.On("HasCompressor").Return(false)
	wr := file.NewColumnChunkWriter(metadata, pager, props).(*file.Int32ColumnChunkWriter)

	// write a list "[[0, 1], null, [2, null, 3]]"
	// should be 6 values, 2 nulls and 3 rows
	wr.WriteBatch([]int32{0, 1, 2, 3},
		[]int16{3, 3, 0, 3, 2, 3},
		[]int16{0, 1, 0, 0, 1, 1})

	pager.On("WriteDataPage", mock.MatchedBy(func(page file.DataPage) bool {
		pagev1, ok := page.(*file.DataPageV1)
		if !ok {
			return false
		}

		encodedStats := pagev1.Statistics()
		// only match if the page being written has 2 nulls, 6 values and 3 rows
		return pagev1.NumValues() == 6 &&
			encodedStats.HasNullCount &&
			encodedStats.NullCount == 2
	})).Return(10, nil)

	wr.FlushBufferedDataPages()
	assert.EqualValues(t, 3, wr.RowsWritten())
}

func TestWriteDataPageV2NumRows(t *testing.T) {
	// test issue from PARQUET-2066
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.ListOf(
			schema.Must(schema.NewPrimitiveNode("column", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1)),
			parquet.Repetitions.Optional, -1)),
	}, -1)))
	descr := sc.Column(0)
	props := parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false))

	metadata := metadata.NewColumnChunkMetaDataBuilder(props, descr)
	pager := new(mockpagewriter)
	defer pager.AssertExpectations(t)
	pager.On("HasCompressor").Return(false)
	wr := file.NewColumnChunkWriter(metadata, pager, props).(*file.Int32ColumnChunkWriter)

	// write a list "[[0, 1], null, [2, null, 3]]"
	// should be 6 values, 2 nulls and 3 rows
	wr.WriteBatch([]int32{0, 1, 2, 3},
		[]int16{3, 3, 0, 3, 2, 3},
		[]int16{0, 1, 0, 0, 1, 1})

	pager.On("WriteDataPage", mock.MatchedBy(func(page file.DataPage) bool {
		pagev2, ok := page.(*file.DataPageV2)
		if !ok {
			return false
		}

		encodedStats := pagev2.Statistics()
		// only match if the page being written has 2 nulls, 6 values and 3 rows
		return !pagev2.IsCompressed() &&
			pagev2.NumNulls() == 2 && encodedStats.NullCount == 2 &&
			pagev2.NumValues() == 6 &&
			pagev2.NumRows() == 3
	})).Return(10, nil)

	wr.FlushBufferedDataPages()
	assert.EqualValues(t, 3, wr.RowsWritten())
}

func TestDataPageV2RowBoundaries(t *testing.T) {
	sc := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.ListOf(
			schema.Must(schema.NewPrimitiveNode("column", parquet.Repetitions.Optional, parquet.Types.Int32, -1, -1)),
			parquet.Repetitions.Optional, -1)),
	}, -1)))
	descr := sc.Column(0)
	props := parquet.NewWriterProperties(
		parquet.WithBatchSize(128),
		parquet.WithDataPageSize(1024),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithDictionaryDefault(false))

	metadata := metadata.NewColumnChunkMetaDataBuilder(props, descr)
	pager := new(mockpagewriter)
	defer pager.AssertExpectations(t)
	pager.On("HasCompressor").Return(false)
	wr := file.NewColumnChunkWriter(metadata, pager, props).(*file.Int32ColumnChunkWriter)

	pager.On("WriteDataPage", mock.MatchedBy(func(page file.DataPage) bool {
		pagev2, ok := page.(*file.DataPageV2)
		if !ok {
			return false
		}

		// only match if the page being written has 2 nulls, 6 values and 3 rows
		return !pagev2.IsCompressed() &&
			pagev2.NumNulls() == 0 &&
			pagev2.NumValues() == 378 &&
			pagev2.NumRows() == 126
	})).Return(10, nil)

	// create rows of lists of 3 values each
	values := make([]int32, 1024)
	defLevels := make([]int16, 1024)
	repLevels := make([]int16, 1024)
	for i := range values {
		values[i] = int32(i)
		defLevels[i] = 3

		switch i % 3 {
		case 0:
			repLevels[i] = 0
		case 1, 2:
			repLevels[i] = 1
		}
	}

	wr.WriteBatch(values, defLevels, repLevels)
}

type PrimitiveWriterTestSuite struct {
	testutils.PrimitiveTypedTest
	suite.Suite

	props *parquet.WriterProperties
	descr *schema.Column

	metadata   *metadata.ColumnChunkMetaDataBuilder
	sink       *encoding.BufferWriter
	readbuffer *memory.Buffer

	bufferPool sync.Pool
}

func (p *PrimitiveWriterTestSuite) SetupTest() {
	p.SetupValuesOut(SmallSize)
	p.props = parquet.NewWriterProperties()
	p.SetupSchema(parquet.Repetitions.Required, 1)
	p.descr = p.Schema.Column(0)

	p.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := memory.NewResizableBuffer(mem)
			runtime.SetFinalizer(buf, func(obj *memory.Buffer) {
				obj.Release()
			})
			return buf
		},
	}
}

func (p *PrimitiveWriterTestSuite) TearDownTest() {
	p.bufferPool = sync.Pool{}
}

func (p *PrimitiveWriterTestSuite) buildReader(nrows int64, compression compress.Compression) file.ColumnChunkReader {
	p.readbuffer = p.sink.Finish()
	pagereader, _ := file.NewPageReader(arrutils.NewBufferedReader(bytes.NewReader(p.readbuffer.Bytes()), p.readbuffer.Len()), nrows, compression, mem, nil)
	return file.NewColumnReader(p.descr, pagereader, mem, &p.bufferPool)
}

func (p *PrimitiveWriterTestSuite) buildWriter(_ int64, columnProps parquet.ColumnProperties, opts ...parquet.WriterProperty) file.ColumnChunkWriter {
	p.sink = encoding.NewBufferWriter(0, mem)
	if columnProps.Encoding == parquet.Encodings.PlainDict || columnProps.Encoding == parquet.Encodings.RLEDict {
		opts = append(opts, parquet.WithDictionaryDefault(true), parquet.WithDictionaryPageSizeLimit(DictionaryPageSize))
	} else {
		opts = append(opts, parquet.WithDictionaryDefault(false), parquet.WithEncoding(columnProps.Encoding))
	}
	opts = append(opts, parquet.WithMaxStatsSize(columnProps.MaxStatsSize), parquet.WithStats(columnProps.StatsEnabled))
	p.props = parquet.NewWriterProperties(opts...)

	p.metadata = metadata.NewColumnChunkMetaDataBuilder(p.props, p.descr)
	pager, _ := file.NewPageWriter(p.sink, columnProps.Codec, compress.DefaultCompressionLevel, p.metadata, -1, -1, memory.DefaultAllocator, false, nil, nil)
	return file.NewColumnChunkWriter(p.metadata, pager, p.props)
}

func (p *PrimitiveWriterTestSuite) readColumn(compression compress.Compression) int64 {
	totalValues := int64(len(p.DefLevelsOut))
	reader := p.buildReader(totalValues, compression)
	return p.ReadBatch(reader, totalValues, 0, p.DefLevelsOut, p.RepLevelsOut)
}

func (p *PrimitiveWriterTestSuite) readColumnFully(compression compress.Compression) int64 {
	totalValues := int64(len(p.DefLevelsOut))
	reader := p.buildReader(totalValues, compression)
	valuesRead := int64(0)
	for valuesRead < totalValues {
		read := p.ReadBatch(reader, totalValues-valuesRead, valuesRead, p.DefLevelsOut[valuesRead:], p.RepLevelsOut[valuesRead:])
		valuesRead += read
	}
	return valuesRead
}

func (p *PrimitiveWriterTestSuite) readAndCompare(compression compress.Compression, nrows int64) {
	p.SetupValuesOut(nrows)
	p.readColumnFully(compression)
	p.Equal(p.Values, p.ValuesOut)
}

func (p *PrimitiveWriterTestSuite) writeRequiredWithSettings(encoding parquet.Encoding, compression compress.Compression, dict, stats bool, compressLvl int, nrows int64) {
	columnProperties := parquet.ColumnProperties{
		Encoding:          encoding,
		Codec:             compression,
		DictionaryEnabled: dict,
		StatsEnabled:      stats,
		CompressionLevel:  compressLvl,
	}
	writer := p.buildWriter(nrows, columnProperties, parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValues(writer, nil, nil)
	// behavior should be independant of the number of calls to Close
	writer.Close()
	writer.Close()
}

func (p *PrimitiveWriterTestSuite) writeRequiredWithSettingsSpaced(encoding parquet.Encoding, compression compress.Compression, dict, stats bool, nrows int64, compressionLvl int) {
	validBits := make([]byte, int(bitutil.BytesForBits(int64(len(p.DefLevels))))+1)
	memory.Set(validBits, 255)
	columnProperties := parquet.ColumnProperties{
		Encoding:          encoding,
		Codec:             compression,
		DictionaryEnabled: dict,
		StatsEnabled:      stats,
		CompressionLevel:  compressionLvl,
	}
	writer := p.buildWriter(nrows, columnProperties, parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValuesSpaced(writer, nil, nil, validBits, 0)
	// behavior should be independant from the number of close calls
	writer.Close()
	writer.Close()
}

func (p *PrimitiveWriterTestSuite) testRequiredWithSettings(encoding parquet.Encoding, compression compress.Compression, dict, stats bool, nrows int64, compressLvl int) {
	p.GenerateData(nrows)
	p.writeRequiredWithSettings(encoding, compression, dict, stats, compressLvl, nrows)
	p.NotPanics(func() { p.readAndCompare(compression, nrows) })
	p.writeRequiredWithSettingsSpaced(encoding, compression, dict, stats, nrows, compressLvl)
	p.NotPanics(func() { p.readAndCompare(compression, nrows) })
}

func (p *PrimitiveWriterTestSuite) testRequiredWithEncoding(encoding parquet.Encoding) {
	p.testRequiredWithSettings(encoding, compress.Codecs.Uncompressed, false, false, SmallSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) metadataNumValues() int64 {
	// metadata accessor created lazily
	metadata, _ := metadata.NewColumnChunkMetaData(p.metadata.Contents(), p.descr, nil, 0, 0, nil)
	return metadata.NumValues()
}

func (p *PrimitiveWriterTestSuite) metadataEncodings() []parquet.Encoding {
	metadata, _ := metadata.NewColumnChunkMetaData(p.metadata.Contents(), p.descr, nil, 0, 0, nil)
	return metadata.Encodings()
}

func (p *PrimitiveWriterTestSuite) metadataEncodingStats() []metadata.PageEncodingStats {
	metadata, _ := metadata.NewColumnChunkMetaData(p.metadata.Contents(), p.descr, nil, 0, 0, nil)
	return metadata.EncodingStats()
}

func (p *PrimitiveWriterTestSuite) metadataStatsHasMinMax() (hasMin, hasMax bool) {
	appVersion := metadata.NewAppVersion(p.props.CreatedBy())
	metadata, _ := metadata.NewColumnChunkMetaData(p.metadata.Contents(), p.descr, appVersion, 0, 0, nil)
	stats, _ := metadata.Statistics()
	encoded, _ := stats.Encode()
	return encoded.HasMin, encoded.HasMax
}

func (p *PrimitiveWriterTestSuite) metadataIsStatsSet() bool {
	appVersion := metadata.NewAppVersion(p.props.CreatedBy())
	metadata, _ := metadata.NewColumnChunkMetaData(p.metadata.Contents(), p.descr, appVersion, 0, 0, nil)
	set, _ := metadata.StatsSet()
	return set
}

func (p *PrimitiveWriterTestSuite) testDictionaryFallbackEncoding(version parquet.Version) {
	p.GenerateData(VeryLargeSize)
	props := parquet.DefaultColumnProperties()
	props.DictionaryEnabled = true

	if version == parquet.V1_0 {
		props.Encoding = parquet.Encodings.PlainDict
	} else {
		props.Encoding = parquet.Encodings.RLEDict
	}

	writer := p.buildWriter(VeryLargeSize, props, parquet.WithVersion(version))
	p.WriteBatchValues(writer, nil, nil)
	writer.Close()

	// Read all the rows so that we are sure that also the non-dictionary pages are read correctly
	p.SetupValuesOut(VeryLargeSize)
	valuesRead := p.readColumnFully(compress.Codecs.Uncompressed)
	p.EqualValues(VeryLargeSize, valuesRead)
	p.Equal(p.Values, p.ValuesOut)

	encodings := p.metadataEncodings()
	if p.Typ.Kind() == reflect.Bool || p.Typ == reflect.TypeOf(parquet.Int96{}) {
		// dictionary encoding is not allowed for booleans
		// there are 2 encodings (PLAIN, RLE) in a non dictionary encoding case
		p.Equal([]parquet.Encoding{parquet.Encodings.Plain, parquet.Encodings.RLE}, encodings)
	} else if version == parquet.V1_0 {
		// There are 4 encodings (PLAIN_DICTIONARY, PLAIN, RLE, PLAIN) in a fallback case
		// for version 1.0
		p.Equal([]parquet.Encoding{parquet.Encodings.PlainDict, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.Plain}, encodings)
	} else {
		// There are 4 encodings (RLE_DICTIONARY, PLAIN, RLE, PLAIN) in a fallback case for
		// version 2.0
		p.Equal([]parquet.Encoding{parquet.Encodings.RLEDict, parquet.Encodings.Plain, parquet.Encodings.RLE, parquet.Encodings.Plain}, encodings)
	}

	encodingStats := p.metadataEncodingStats()
	if p.Typ.Kind() == reflect.Bool || p.Typ == reflect.TypeOf(parquet.Int96{}) {
		p.Equal(parquet.Encodings.Plain, encodingStats[0].Encoding)
		p.Equal(format.PageType_DATA_PAGE, encodingStats[0].PageType)
	} else if version == parquet.V1_0 {
		expected := []metadata.PageEncodingStats{
			{Encoding: parquet.Encodings.PlainDict, PageType: format.PageType_DICTIONARY_PAGE},
			{Encoding: parquet.Encodings.Plain, PageType: format.PageType_DATA_PAGE},
			{Encoding: parquet.Encodings.PlainDict, PageType: format.PageType_DATA_PAGE}}
		p.Equal(expected[0], encodingStats[0])
		p.ElementsMatch(expected[1:], encodingStats[1:])
	} else {
		expected := []metadata.PageEncodingStats{
			{Encoding: parquet.Encodings.Plain, PageType: format.PageType_DICTIONARY_PAGE},
			{Encoding: parquet.Encodings.Plain, PageType: format.PageType_DATA_PAGE},
			{Encoding: parquet.Encodings.RLEDict, PageType: format.PageType_DATA_PAGE}}
		p.Equal(expected[0], encodingStats[0])
		p.ElementsMatch(expected[1:], encodingStats[1:])
	}
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlain() {
	p.testRequiredWithEncoding(parquet.Encodings.Plain)
}

func (p *PrimitiveWriterTestSuite) TestRequiredDictionary() {
	p.testRequiredWithEncoding(parquet.Encodings.PlainDict)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithStats() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Uncompressed, false, true, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithSnappy() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Snappy, false, false, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithStatsAndSnappy() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Snappy, false, true, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithBrotli() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Brotli, false, false, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithBrotliAndLevel() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Brotli, false, false, LargeSize, 10)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithStatsAndBrotli() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Brotli, false, true, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithGzip() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Gzip, false, false, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithGzipAndLevel() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Gzip, false, false, LargeSize, 10)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithStatsAndGzip() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Gzip, false, true, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithZstd() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Zstd, false, false, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithZstdAndLevel() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Zstd, false, false, LargeSize, 6)
}

func (p *PrimitiveWriterTestSuite) TestRequiredPlainWithStatsAndZstd() {
	p.testRequiredWithSettings(parquet.Encodings.Plain, compress.Codecs.Zstd, false, true, LargeSize, compress.DefaultCompressionLevel)
}

func (p *PrimitiveWriterTestSuite) TestOptionalNonRepeated() {
	p.SetupSchema(parquet.Repetitions.Optional, 1)
	p.descr = p.Schema.Column(0)

	p.GenerateData(SmallSize)
	p.DefLevels[1] = 0

	writer := p.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValues(writer, p.DefLevels, nil)
	writer.Close()

	p.Equal(int64(100), p.metadataNumValues())

	values := p.readColumn(compress.Codecs.Uncompressed)
	p.EqualValues(99, values)
	p.Equal(reflect.ValueOf(p.Values).Slice(0, 99).Interface(), reflect.ValueOf(p.ValuesOut).Slice(0, 99).Interface())
}

func (p *PrimitiveWriterTestSuite) TestOptionalSpaced() {
	p.SetupSchema(parquet.Repetitions.Optional, 1)
	p.descr = p.Schema.Column(0)

	p.GenerateData(SmallSize)
	validBits := make([]byte, int(bitutil.BytesForBits(SmallSize)))
	memory.Set(validBits, 255)
	p.DefLevels[SmallSize-1] = 0
	bitutil.ClearBit(validBits, SmallSize-1)
	p.DefLevels[1] = 0
	bitutil.ClearBit(validBits, 1)

	writer := p.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValuesSpaced(writer, p.DefLevels, nil, validBits, 0)
	writer.Close()

	p.Equal(int64(100), p.metadataNumValues())

	values := p.readColumn(compress.Codecs.Uncompressed)
	p.EqualValues(98, values)

	orig := reflect.ValueOf(p.Values)
	orig = orig.Slice(0, 99)
	reflect.Copy(orig.Slice(1, orig.Len()), orig.Slice(2, orig.Len()))
	orig = orig.Slice(0, 98)
	out := reflect.ValueOf(p.ValuesOut)
	out = out.Slice(0, 98)

	p.Equal(orig.Interface(), out.Interface())
}

func (p *PrimitiveWriterTestSuite) TestWriteRepeated() {
	// optional and repeated so def and repetition levels
	p.SetupSchema(parquet.Repetitions.Repeated, 1)
	p.descr = p.Schema.Column(0)
	p.GenerateData(SmallSize)
	p.DefLevels[1] = 0
	p.RepLevels = make([]int16, SmallSize)
	for idx := range p.RepLevels {
		p.RepLevels[idx] = 0
	}

	writer := p.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValues(writer, p.DefLevels, p.RepLevels)
	writer.Close()

	values := p.readColumn(compress.Codecs.Uncompressed)
	p.EqualValues(SmallSize-1, values)
	out := reflect.ValueOf(p.ValuesOut).Slice(0, SmallSize-1).Interface()
	vals := reflect.ValueOf(p.Values).Slice(0, SmallSize-1).Interface()
	p.Equal(vals, out)
}

func (p *PrimitiveWriterTestSuite) TestRequiredLargeChunk() {
	p.GenerateData(LargeSize)

	// Test 1: required and non-repeated, so no def or rep levels
	writer := p.buildWriter(LargeSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValues(writer, nil, nil)
	writer.Close()

	// just read the first SmallSize rows to ensure we could read it back in
	values := p.readColumn(compress.Codecs.Uncompressed)
	p.EqualValues(SmallSize, values)
	p.Equal(reflect.ValueOf(p.Values).Slice(0, SmallSize).Interface(), p.ValuesOut)
}

func (p *PrimitiveWriterTestSuite) TestDictionaryFallbackEncodingV1() {
	p.testDictionaryFallbackEncoding(parquet.V1_0)
}

func (p *PrimitiveWriterTestSuite) TestDictionaryFallbackEncodingV2() {
	p.testDictionaryFallbackEncoding(parquet.V2_LATEST)
}

func (p *PrimitiveWriterTestSuite) TestOptionalNullValueChunk() {
	// test case for NULL values
	p.SetupSchema(parquet.Repetitions.Optional, 1)
	p.descr = p.Schema.Column(0)
	p.GenerateData(LargeSize)
	p.RepLevels = make([]int16, LargeSize)
	for idx := range p.DefLevels {
		p.DefLevels[idx] = 0
		p.RepLevels[idx] = 0
	}

	writer := p.buildWriter(LargeSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	p.WriteBatchValues(writer, p.DefLevels, p.RepLevels)
	writer.Close()

	valuesRead := p.readColumn(compress.Codecs.Uncompressed)
	p.Zero(valuesRead)
}

func createWriterTestSuite(typ reflect.Type) suite.TestingSuite {
	switch typ {
	case reflect.TypeOf(true):
		return &BooleanValueWriterSuite{PrimitiveWriterTestSuite{PrimitiveTypedTest: testutils.NewPrimitiveTypedTest(typ)}}
	case reflect.TypeOf(parquet.ByteArray{}):
		return &ByteArrayWriterSuite{PrimitiveWriterTestSuite{PrimitiveTypedTest: testutils.NewPrimitiveTypedTest(typ)}}
	}
	return &PrimitiveWriterTestSuite{PrimitiveTypedTest: testutils.NewPrimitiveTypedTest(typ)}
}

func TestColumnWriter(t *testing.T) {
	t.Parallel()
	types := []struct {
		typ reflect.Type
	}{
		{reflect.TypeOf(true)},
		{reflect.TypeOf(int32(0))},
		{reflect.TypeOf(int64(0))},
		{reflect.TypeOf(float32(0))},
		{reflect.TypeOf(float64(0))},
		{reflect.TypeOf(parquet.Int96{})},
		{reflect.TypeOf(parquet.ByteArray{})},
		{reflect.TypeOf(parquet.FixedLenByteArray{})},
	}
	for _, tt := range types {
		tt := tt
		t.Run(tt.typ.String(), func(t *testing.T) {
			t.Parallel()
			suite.Run(t, createWriterTestSuite(tt.typ))
		})
	}
}

type ByteArrayWriterSuite struct {
	PrimitiveWriterTestSuite
}

func (b *ByteArrayWriterSuite) TestOmitStats() {
	// prevent writing large MIN,MAX stats
	minLen := 1024 * 4
	maxLen := 1024 * 8
	b.SetupSchema(parquet.Repetitions.Required, 1)
	b.Values = make([]parquet.ByteArray, SmallSize)
	writer := b.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	testutils.RandomByteArray(0, b.Values.([]parquet.ByteArray), b.Buffer, minLen, maxLen)
	writer.(*file.ByteArrayColumnChunkWriter).WriteBatch(b.Values.([]parquet.ByteArray), nil, nil)
	writer.Close()

	hasMin, hasMax := b.metadataStatsHasMinMax()
	b.False(hasMin)
	b.False(hasMax)
}

func (b *ByteArrayWriterSuite) TestOmitDataPageStats() {
	// prevent writing large stats in DataPageHeader
	minLen := math.Pow10(7)
	maxLen := math.Pow10(7)
	b.SetupSchema(parquet.Repetitions.Required, 1)
	colprops := parquet.DefaultColumnProperties()
	colprops.StatsEnabled = false

	writer := b.buildWriter(SmallSize, colprops, parquet.WithVersion(parquet.V1_0))
	b.Values = make([]parquet.ByteArray, 1)
	testutils.RandomByteArray(0, b.Values.([]parquet.ByteArray), b.Buffer, int(minLen), int(maxLen))
	writer.(*file.ByteArrayColumnChunkWriter).WriteBatch(b.Values.([]parquet.ByteArray), nil, nil)
	writer.Close()

	b.NotPanics(func() { b.readColumn(compress.Codecs.Uncompressed) })
}

func (b *ByteArrayWriterSuite) TestLimitStats() {
	minLen := 1024 * 4
	maxLen := 1024 * 8
	b.SetupSchema(parquet.Repetitions.Required, 1)
	colprops := parquet.DefaultColumnProperties()
	colprops.MaxStatsSize = int64(maxLen)

	writer := b.buildWriter(SmallSize, colprops, parquet.WithVersion(parquet.V1_0)).(*file.ByteArrayColumnChunkWriter)
	b.Values = make([]parquet.ByteArray, SmallSize)
	testutils.RandomByteArray(0, b.Values.([]parquet.ByteArray), b.Buffer, minLen, maxLen)
	writer.WriteBatch(b.Values.([]parquet.ByteArray), nil, nil)
	writer.Close()

	b.True(b.metadataIsStatsSet())
}

func (b *ByteArrayWriterSuite) TestCheckDefaultStats() {
	b.SetupSchema(parquet.Repetitions.Required, 1)
	writer := b.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0))
	b.GenerateData(SmallSize)
	b.WriteBatchValues(writer, nil, nil)
	writer.Close()

	b.True(b.metadataIsStatsSet())
}

type BooleanValueWriterSuite struct {
	PrimitiveWriterTestSuite
}

func (b *BooleanValueWriterSuite) TestAlternateBooleanValues() {
	b.SetupSchema(parquet.Repetitions.Required, 1)
	// We use an unusual data-page size to try to flush out Boolean encoder issues in usage of the BitMapWriter
	writer := b.buildWriter(SmallSize, parquet.DefaultColumnProperties(), parquet.WithVersion(parquet.V1_0), parquet.WithDataPageSize(7)).(*file.BooleanColumnChunkWriter)
	for i := 0; i < SmallSize; i++ {
		val := i%2 == 0
		writer.WriteBatch([]bool{val}, nil, nil)
	}
	writer.Close()
	b.readColumn(compress.Codecs.Uncompressed)
	for i := 0; i < SmallSize; i++ {
		b.Equal(i%2 == 0, b.ValuesOut.([]bool)[i])
	}
}

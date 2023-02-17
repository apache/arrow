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
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/internal/utils"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/internal/encoding"
	format "github.com/apache/arrow/go/v12/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v12/parquet/internal/thrift"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	libthrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func getDummyStats(statSize int, fillAll bool) *format.Statistics {
	statBytes := make([]byte, statSize)
	memory.Set(statBytes, 1)

	ret := format.NewStatistics()
	ret.Max = statBytes
	if fillAll {
		ret.Min = statBytes
		ret.NullCount = libthrift.Int64Ptr(42)
		ret.DistinctCount = libthrift.Int64Ptr(1)
	}
	return ret
}

func checkStatistics(t *testing.T, stats format.Statistics, actual metadata.EncodedStatistics) {
	if stats.IsSetMax() {
		assert.Equal(t, stats.Max, actual.Max)
	}
	if stats.IsSetMin() {
		assert.Equal(t, stats.Min, actual.Min)
	}
	if stats.IsSetNullCount() {
		assert.Equal(t, stats.GetNullCount(), actual.NullCount)
	}
	if stats.IsSetDistinctCount() {
		assert.Equal(t, stats.GetDistinctCount(), actual.DistinctCount)
	}
}

type PageSerdeSuite struct {
	suite.Suite

	sink   *encoding.BufferWriter
	buffer *memory.Buffer

	pageHdr       format.PageHeader
	dataPageHdr   format.DataPageHeader
	dataPageHdrV2 format.DataPageHeaderV2

	pageReader file.PageReader
}

func TestFileDeserializing(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PageSerdeSuite))
}

func (p *PageSerdeSuite) ResetStream() {
	p.sink = encoding.NewBufferWriter(0, memory.DefaultAllocator)
}

func (p *PageSerdeSuite) EndStream() {
	p.buffer = p.sink.Finish()
}

func (p *PageSerdeSuite) SetupTest() {
	p.dataPageHdr.Encoding = format.Encoding_PLAIN
	p.dataPageHdr.DefinitionLevelEncoding = format.Encoding_RLE
	p.dataPageHdr.RepetitionLevelEncoding = format.Encoding_RLE

	p.ResetStream()
}

func (p *PageSerdeSuite) InitSerializedPageReader(nrows int64, codec compress.Compression) {
	p.EndStream()

	p.pageReader, _ = file.NewPageReader(utils.NewBufferedReader(bytes.NewReader(p.buffer.Bytes()), p.buffer.Len()), nrows, codec, memory.DefaultAllocator, nil)
}

func (p *PageSerdeSuite) WriteDataPageHeader(maxSerialized int, uncompressed, compressed int32) {
	// simplifying writing serialized data page headers which may or may
	// not have meaningful data associated with them

	p.pageHdr.DataPageHeader = &p.dataPageHdr
	p.pageHdr.UncompressedPageSize = uncompressed
	p.pageHdr.CompressedPageSize = compressed
	p.pageHdr.Type = format.PageType_DATA_PAGE

	serializer := thrift.NewThriftSerializer()
	p.NotPanics(func() {
		serializer.Serialize(&p.pageHdr, p.sink, nil)
	})
}

func (p *PageSerdeSuite) WriteDataPageHeaderV2(maxSerialized int, uncompressed, compressed int32) {
	p.pageHdr.DataPageHeaderV2 = &p.dataPageHdrV2
	p.pageHdr.UncompressedPageSize = uncompressed
	p.pageHdr.CompressedPageSize = compressed
	p.pageHdr.Type = format.PageType_DATA_PAGE_V2

	serializer := thrift.NewThriftSerializer()
	p.NotPanics(func() {
		serializer.Serialize(&p.pageHdr, p.sink, nil)
	})
}

func (p *PageSerdeSuite) CheckDataPageHeader(expected format.DataPageHeader, page file.Page) {
	p.Equal(format.PageType_DATA_PAGE, page.Type())

	p.IsType(&file.DataPageV1{}, page)
	p.Equal(expected.NumValues, page.NumValues())
	p.Equal(expected.Encoding, page.Encoding())
	p.EqualValues(expected.DefinitionLevelEncoding, page.(*file.DataPageV1).DefinitionLevelEncoding())
	p.EqualValues(expected.RepetitionLevelEncoding, page.(*file.DataPageV1).RepetitionLevelEncoding())
	checkStatistics(p.T(), *expected.Statistics, page.(file.DataPage).Statistics())
}

func (p *PageSerdeSuite) CheckDataPageHeaderV2(expected format.DataPageHeaderV2, page file.Page) {
	p.Equal(format.PageType_DATA_PAGE_V2, page.Type())

	p.IsType(&file.DataPageV2{}, page)
	p.Equal(expected.NumValues, page.NumValues())
	p.Equal(expected.Encoding, page.Encoding())
	p.Equal(expected.NumNulls, page.(*file.DataPageV2).NumNulls())
	p.Equal(expected.DefinitionLevelsByteLength, page.(*file.DataPageV2).DefinitionLevelByteLen())
	p.Equal(expected.RepetitionLevelsByteLength, page.(*file.DataPageV2).RepetitionLevelByteLen())
	p.Equal(expected.IsCompressed, page.(*file.DataPageV2).IsCompressed())
	checkStatistics(p.T(), *expected.Statistics, page.(file.DataPage).Statistics())
}

func (p *PageSerdeSuite) TestDataPageV1() {
	const (
		statsSize = 512
		nrows     = 4444
	)
	p.dataPageHdr.Statistics = getDummyStats(statsSize, true)
	p.dataPageHdr.NumValues = nrows

	p.WriteDataPageHeader(1024, 0, 0)
	p.InitSerializedPageReader(nrows, compress.Codecs.Uncompressed)
	p.True(p.pageReader.Next())
	currentPage := p.pageReader.Page()
	p.CheckDataPageHeader(p.dataPageHdr, currentPage)
}

func (p *PageSerdeSuite) TestDataPageV2() {
	const (
		statsSize = 512
		nrows     = 4444
	)
	p.dataPageHdrV2.Statistics = getDummyStats(statsSize, true)
	p.dataPageHdrV2.NumValues = nrows
	p.WriteDataPageHeaderV2(1024, 0, 0)
	p.InitSerializedPageReader(nrows, compress.Codecs.Uncompressed)
	p.True(p.pageReader.Next())
	p.CheckDataPageHeaderV2(p.dataPageHdrV2, p.pageReader.Page())
}

func (p *PageSerdeSuite) TestLargePageHeaders() {
	const (
		statsSize     = 256 * 1024 // 256KB
		nrows         = 4141
		maxHeaderSize = 512 * 1024 // 512KB
	)

	p.dataPageHdr.Statistics = getDummyStats(statsSize, false)
	p.dataPageHdr.NumValues = nrows
	p.WriteDataPageHeader(maxHeaderSize, 0, 0)
	pos, err := p.sink.Seek(0, io.SeekCurrent)
	p.NoError(err)
	p.GreaterOrEqual(maxHeaderSize, int(pos))
	p.LessOrEqual(statsSize, int(pos))
	p.GreaterOrEqual(16*1024*1024, int(pos))

	p.InitSerializedPageReader(nrows, compress.Codecs.Uncompressed)
	p.True(p.pageReader.Next())
	p.CheckDataPageHeader(p.dataPageHdr, p.pageReader.Page())
}

func (p *PageSerdeSuite) TestFailLargePageHeaders() {
	const (
		statsSize      = 256 * 1024 // 256KB
		nrows          = 1337       // dummy value
		maxHeaderSize  = 512 * 1024 // 512 KB
		smallerMaxSize = 128 * 1024 // 128KB
	)
	p.dataPageHdr.Statistics = getDummyStats(statsSize, false)
	p.WriteDataPageHeader(maxHeaderSize, 0, 0)
	pos, err := p.sink.Seek(0, io.SeekCurrent)
	p.NoError(err)
	p.GreaterOrEqual(maxHeaderSize, int(pos))

	p.LessOrEqual(smallerMaxSize, int(pos))
	p.InitSerializedPageReader(nrows, compress.Codecs.Uncompressed)
	p.pageReader.SetMaxPageHeaderSize(smallerMaxSize)
	p.NotPanics(func() { p.False(p.pageReader.Next()) })
	p.Error(p.pageReader.Err())
}

func (p *PageSerdeSuite) TestCompression() {
	codecs := []compress.Compression{
		compress.Codecs.Snappy,
		compress.Codecs.Brotli,
		compress.Codecs.Gzip,
		// compress.Codecs.Lz4, // not yet implemented
		compress.Codecs.Zstd,
	}

	const (
		nrows  = 32 // dummy value
		npages = 10
	)
	p.dataPageHdr.NumValues = nrows

	fauxData := make([][]byte, npages)
	for idx := range fauxData {
		// each page is larger
		fauxData[idx] = make([]byte, (idx+1)*64)
		rand.Read(fauxData[idx])
	}
	for _, c := range codecs {
		p.Run(c.String(), func() {
			codec, _ := compress.GetCodec(c)
			for _, data := range fauxData {
				maxCompressed := codec.CompressBound(int64(len(data)))
				buffer := make([]byte, maxCompressed)
				buffer = codec.Encode(buffer, data)
				p.WriteDataPageHeader(1024, int32(len(data)), int32(len(buffer)))
				_, err := p.sink.Write(buffer)
				p.NoError(err)
			}

			p.InitSerializedPageReader(nrows*npages, c)

			for _, data := range fauxData {
				p.True(p.pageReader.Next())
				page := p.pageReader.Page()
				p.IsType(&file.DataPageV1{}, page)
				p.Equal(data, page.Data())
			}
			p.ResetStream()
		})
	}
}

func TestInvalidHeaders(t *testing.T) {
	badHeader := []byte("PAR2")
	_, err := file.NewParquetReader(bytes.NewReader(badHeader))
	assert.Error(t, err)
}

func TestInvalidFooter(t *testing.T) {
	// file is smaller than FOOTER_SIZE
	badFile := []byte("PAR1PAR")
	_, err := file.NewParquetReader(bytes.NewReader(badFile))
	assert.Error(t, err)

	// Magic Number Incorrect
	badFile2 := []byte("PAR1PAR2")
	_, err = file.NewParquetReader(bytes.NewReader(badFile2))
	assert.Error(t, err)
}

func TestIncompleteMetadata(t *testing.T) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	magic := []byte("PAR1")

	sink.Write(magic)
	sink.Write(make([]byte, 10))
	const metadataLen = 24
	binary.Write(sink, binary.LittleEndian, uint32(metadataLen))
	sink.Write(magic)
	buf := sink.Finish()
	defer buf.Release()
	_, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	assert.Error(t, err)
}

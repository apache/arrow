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

package file

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/JohnCGriffin/overflow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/internal/encryption"
	format "github.com/apache/arrow/go/v13/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/thrift"
	"github.com/apache/arrow/go/v13/parquet/metadata"
	"golang.org/x/xerrors"
)

// PageReader is the interface used by the columnreader in order to read
// and handle DataPages and loop through them.
type PageReader interface {
	// Set the maximum Page header size allowed to be read
	SetMaxPageHeaderSize(int)
	// Return the current page, or nil if there are no more
	Page() Page
	// Fetch the next page, returns false if there are no more pages
	Next() bool
	// if Next returns false, Err will return the error encountered or
	// nil if there was no error and you just hit the end of the page
	Err() error
	// Reset allows reusing a page reader
	Reset(r parquet.BufferedReader, nrows int64, compressType compress.Compression, ctx *CryptoContext)
}

// Page is an interface for handling DataPages or Dictionary Pages
type Page interface {
	// Returns which kind of page this is
	Type() format.PageType
	// Get the raw bytes of this page
	Data() []byte
	// return the encoding used for this page, Plain/RLE, etc.
	Encoding() format.Encoding
	// get the number of values in this page
	NumValues() int32
	// release this page object back into the page pool for re-use
	Release()
}

type page struct {
	buf *memory.Buffer
	typ format.PageType

	nvals    int32
	encoding format.Encoding
}

func (p *page) Type() format.PageType     { return p.typ }
func (p *page) Data() []byte              { return p.buf.Bytes() }
func (p *page) NumValues() int32          { return p.nvals }
func (p *page) Encoding() format.Encoding { return p.encoding }

// DataPage is the base interface for both DataPageV1 and DataPageV2 of the
// parquet spec.
type DataPage interface {
	Page
	UncompressedSize() int32
	Statistics() metadata.EncodedStatistics
}

// Create some pools to use for reusing the data page objects themselves so that
// we can avoid tight loops that are creating and destroying tons of individual
// objects. This combined with a Release function on the pages themselves
// which will put them back into the pool yields significant memory reduction
// and performance benefits

var dataPageV1Pool = sync.Pool{
	New: func() interface{} { return (*DataPageV1)(nil) },
}

var dataPageV2Pool = sync.Pool{
	New: func() interface{} { return (*DataPageV2)(nil) },
}

var dictPagePool = sync.Pool{
	New: func() interface{} { return (*DictionaryPage)(nil) },
}

// DataPageV1 represents a DataPage version 1 from the parquet.thrift file
type DataPageV1 struct {
	page

	defLvlEncoding   format.Encoding
	repLvlEncoding   format.Encoding
	uncompressedSize int32
	statistics       metadata.EncodedStatistics
}

// NewDataPageV1 returns a V1 data page with the given buffer as its data and the specified encoding information
//
// Will utilize objects that have been released back into the data page pool and
// re-use them if available as opposed to creating new objects. Calling Release on the
// data page object will release it back to the pool for re-use.
func NewDataPageV1(buffer *memory.Buffer, num int32, encoding, defEncoding, repEncoding parquet.Encoding, uncompressedSize int32) *DataPageV1 {
	dp := dataPageV1Pool.Get().(*DataPageV1)
	if dp == nil {
		return &DataPageV1{
			page:             page{buf: buffer, typ: format.PageType_DATA_PAGE, nvals: num, encoding: format.Encoding(encoding)},
			defLvlEncoding:   format.Encoding(defEncoding),
			repLvlEncoding:   format.Encoding(repEncoding),
			uncompressedSize: uncompressedSize,
		}
	}

	dp.buf, dp.nvals = buffer, num
	dp.encoding = format.Encoding(encoding)
	dp.defLvlEncoding, dp.repLvlEncoding = format.Encoding(defEncoding), format.Encoding(repEncoding)
	dp.statistics.HasMax, dp.statistics.HasMin = false, false
	dp.statistics.HasNullCount, dp.statistics.HasDistinctCount = false, false
	dp.uncompressedSize = uncompressedSize
	return dp
}

// NewDataPageV1WithStats is the same as NewDataPageV1, but also allows adding the stat info into the created page
func NewDataPageV1WithStats(buffer *memory.Buffer, num int32, encoding, defEncoding, repEncoding parquet.Encoding, uncompressedSize int32, stats metadata.EncodedStatistics) *DataPageV1 {
	ret := NewDataPageV1(buffer, num, encoding, defEncoding, repEncoding, uncompressedSize)
	ret.statistics = stats
	return ret
}

// Release this page back into the DataPage object pool so that it can be reused.
//
// After calling this function, the object should not be utilized anymore, otherwise
// conflicts can arise.
func (d *DataPageV1) Release() {
	d.buf.Release()
	d.buf = nil
	dataPageV1Pool.Put(d)
}

// UncompressedSize returns the size of the data in this data page when uncompressed
func (d *DataPageV1) UncompressedSize() int32 { return d.uncompressedSize }

// Statistics returns the encoded statistics on this data page
func (d *DataPageV1) Statistics() metadata.EncodedStatistics { return d.statistics }

// DefinitionLevelEncoding returns the encoding utilized for the Definition Levels
func (d *DataPageV1) DefinitionLevelEncoding() parquet.Encoding {
	return parquet.Encoding(d.defLvlEncoding)
}

// RepetitionLevelEncoding returns the encoding utilized for the Repetition Levels
func (d *DataPageV1) RepetitionLevelEncoding() parquet.Encoding {
	return parquet.Encoding(d.repLvlEncoding)
}

// DataPageV2 is the representation of the V2 data page from the parquet.thrift spec
type DataPageV2 struct {
	page

	nulls            int32
	nrows            int32
	defLvlByteLen    int32
	repLvlByteLen    int32
	compressed       bool
	uncompressedSize int32
	statistics       metadata.EncodedStatistics
}

// NewDataPageV2 constructs a new V2 data page with the provided information and a buffer of the raw data.
func NewDataPageV2(buffer *memory.Buffer, numValues, numNulls, numRows int32, encoding parquet.Encoding, defLvlsByteLen, repLvlsByteLen, uncompressed int32, isCompressed bool) *DataPageV2 {
	dp := dataPageV2Pool.Get().(*DataPageV2)
	if dp == nil {
		return &DataPageV2{
			page:             page{buf: buffer, typ: format.PageType_DATA_PAGE_V2, nvals: numValues, encoding: format.Encoding(encoding)},
			nulls:            numNulls,
			nrows:            numRows,
			defLvlByteLen:    defLvlsByteLen,
			repLvlByteLen:    repLvlsByteLen,
			compressed:       isCompressed,
			uncompressedSize: uncompressed,
		}
	}

	dp.buf, dp.nvals = buffer, numValues
	dp.encoding = format.Encoding(encoding)
	dp.nulls, dp.nrows = numNulls, numRows
	dp.defLvlByteLen, dp.repLvlByteLen = defLvlsByteLen, repLvlsByteLen
	dp.compressed, dp.uncompressedSize = isCompressed, uncompressed
	dp.statistics.HasMax, dp.statistics.HasMin = false, false
	dp.statistics.HasNullCount, dp.statistics.HasDistinctCount = false, false
	return dp
}

// NewDataPageV2WithStats is the same as NewDataPageV2 but allows providing the encoded stats with the page.
func NewDataPageV2WithStats(buffer *memory.Buffer, numValues, numNulls, numRows int32, encoding parquet.Encoding, defLvlsByteLen, repLvlsByteLen, uncompressed int32, isCompressed bool, stats metadata.EncodedStatistics) *DataPageV2 {
	ret := NewDataPageV2(buffer, numValues, numNulls, numRows, encoding, defLvlsByteLen, repLvlsByteLen, uncompressed, isCompressed)
	ret.statistics = stats
	return ret
}

// Release this page back into the DataPage object pool so that it can be reused.
//
// After calling this function, the object should not be utilized anymore, otherwise
// conflicts can arise.
func (d *DataPageV2) Release() {
	d.buf.Release()
	d.buf = nil
	dataPageV2Pool.Put(d)
}

// UncompressedSize is the size of the raw page when uncompressed. If `IsCompressed` is true, then
// the raw data in the buffer is expected to be compressed.
func (d *DataPageV2) UncompressedSize() int32 { return d.uncompressedSize }

// Statistics are the encoded statistics in the data page
func (d *DataPageV2) Statistics() metadata.EncodedStatistics { return d.statistics }

// NumNulls is the reported number of nulls in this datapage
func (d *DataPageV2) NumNulls() int32 { return d.nulls }

// NumRows is the number of rows recorded in the page header
func (d *DataPageV2) NumRows() int32 { return d.nrows }

// DefinitionLevelByteLen is the number of bytes in the buffer that are used to represent the definition levels
func (d *DataPageV2) DefinitionLevelByteLen() int32 { return d.defLvlByteLen }

// RepetitionLevelByteLen is the number of bytes in the buffer which are used to represent the repetition Levels
func (d *DataPageV2) RepetitionLevelByteLen() int32 { return d.repLvlByteLen }

// IsCompressed returns true if the data of this page is compressed
func (d *DataPageV2) IsCompressed() bool { return d.compressed }

// DictionaryPage represents the a page of data that uses dictionary encoding
type DictionaryPage struct {
	page

	sorted bool
}

// NewDictionaryPage constructs a new dictionary page with the provided data buffer and number of values.
func NewDictionaryPage(buffer *memory.Buffer, nvals int32, encoding parquet.Encoding) *DictionaryPage {
	dp := dictPagePool.Get().(*DictionaryPage)
	if dp == nil {
		return &DictionaryPage{
			page: page{
				buf:      buffer,
				typ:      format.PageType_DICTIONARY_PAGE,
				nvals:    nvals,
				encoding: format.Encoding(encoding),
			},
		}
	}

	dp.buf = buffer
	dp.nvals = nvals
	dp.encoding = format.Encoding(encoding)
	dp.sorted = false
	return dp
}

// Release this page back into the DataPage object pool so that it can be reused.
//
// After calling this function, the object should not be utilized anymore, otherwise
// conflicts can arise.
func (d *DictionaryPage) Release() {
	d.buf.Release()
	d.buf = nil
	dictPagePool.Put(d)
}

// IsSorted returns whether the dictionary itself is sorted
func (d *DictionaryPage) IsSorted() bool { return d.sorted }

type serializedPageReader struct {
	r        parquet.BufferedReader
	nrows    int64
	rowsSeen int64
	mem      memory.Allocator
	codec    compress.Codec

	curPageHdr        *format.PageHeader
	pageOrd           int16
	maxPageHeaderSize int

	curPage           Page
	cryptoCtx         CryptoContext
	dataPageAad       string
	dataPageHeaderAad string

	decompressBuffer bytes.Buffer
	err              error
}

// NewPageReader returns a page reader for the data which can be read from the provided reader and compression.
func NewPageReader(r parquet.BufferedReader, nrows int64, compressType compress.Compression, mem memory.Allocator, ctx *CryptoContext) (PageReader, error) {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	codec, err := compress.GetCodec(compressType)
	if err != nil {
		return nil, err
	}

	rdr := &serializedPageReader{
		r:                 r,
		maxPageHeaderSize: defaultMaxPageHeaderSize,
		nrows:             nrows,
		mem:               mem,
		codec:             codec,
	}
	rdr.decompressBuffer.Grow(defaultPageHeaderSize)
	if ctx != nil {
		rdr.cryptoCtx = *ctx
		rdr.initDecryption()
	}
	return rdr, nil
}

func (p *serializedPageReader) Reset(r parquet.BufferedReader, nrows int64, compressType compress.Compression, ctx *CryptoContext) {
	p.rowsSeen, p.pageOrd, p.nrows = 0, 0, nrows
	p.curPageHdr, p.curPage, p.err = nil, nil, nil
	p.r = r

	p.codec, p.err = compress.GetCodec(compressType)
	if p.err != nil {
		return
	}
	p.decompressBuffer.Reset()
	if ctx != nil {
		p.cryptoCtx = *ctx
		p.initDecryption()
	} else {
		p.cryptoCtx = CryptoContext{}
		p.dataPageAad = ""
		p.dataPageHeaderAad = ""
	}
}

func (p *serializedPageReader) Err() error { return p.err }

func (p *serializedPageReader) SetMaxPageHeaderSize(sz int) {
	p.maxPageHeaderSize = sz
}

func (p *serializedPageReader) initDecryption() {
	if p.cryptoCtx.DataDecryptor != nil {
		p.dataPageAad = encryption.CreateModuleAad(p.cryptoCtx.DataDecryptor.FileAad(), encryption.DataPageModule,
			p.cryptoCtx.RowGroupOrdinal, p.cryptoCtx.ColumnOrdinal, -1)
	}
	if p.cryptoCtx.MetaDecryptor != nil {
		p.dataPageHeaderAad = encryption.CreateModuleAad(p.cryptoCtx.MetaDecryptor.FileAad(), encryption.DataPageHeaderModule,
			p.cryptoCtx.RowGroupOrdinal, p.cryptoCtx.ColumnOrdinal, -1)
	}
}

func (p *serializedPageReader) updateDecryption(decrypt encryption.Decryptor, moduleType int8, pageAad string) {
	if p.cryptoCtx.StartDecryptWithDictionaryPage {
		aad := encryption.CreateModuleAad(decrypt.FileAad(), moduleType, p.cryptoCtx.RowGroupOrdinal, p.cryptoCtx.ColumnOrdinal, -1)
		decrypt.UpdateAad(aad)
	} else {
		pageaad := []byte(pageAad)
		encryption.QuickUpdatePageAad(pageaad, p.pageOrd)
		decrypt.UpdateAad(string(pageaad))
	}
}

func (p *serializedPageReader) Page() Page {
	return p.curPage
}

func (p *serializedPageReader) decompress(lenCompressed int, buf []byte) ([]byte, error) {
	p.decompressBuffer.Grow(lenCompressed)
	if _, err := io.CopyN(&p.decompressBuffer, p.r, int64(lenCompressed)); err != nil {
		return nil, err
	}

	data := p.decompressBuffer.Bytes()
	if p.cryptoCtx.DataDecryptor != nil {
		data = p.cryptoCtx.DataDecryptor.Decrypt(p.decompressBuffer.Bytes())
	}

	return p.codec.Decode(buf, data), nil
}

type dataheader interface {
	IsSetStatistics() bool
	GetStatistics() *format.Statistics
}

func extractStats(dataHeader dataheader) (pageStats metadata.EncodedStatistics) {
	if dataHeader.IsSetStatistics() {
		stats := dataHeader.GetStatistics()
		if stats.IsSetMaxValue() {
			pageStats.SetMax(stats.GetMaxValue())
		} else if stats.IsSetMax() {
			pageStats.SetMax(stats.GetMax())
		}
		if stats.IsSetMinValue() {
			pageStats.SetMin(stats.GetMinValue())
		} else if stats.IsSetMin() {
			pageStats.SetMin(stats.GetMin())
		}

		if stats.IsSetNullCount() {
			pageStats.SetNullCount(stats.GetNullCount())
		}
		if stats.IsSetDistinctCount() {
			pageStats.SetDistinctCount(stats.GetDistinctCount())
		}
	}
	return
}

func (p *serializedPageReader) Next() bool {
	// Loop here because there may be unhandled page types that we skip until
	// finding a page that we do know what to do with
	if p.curPage != nil {
		p.curPage.Release()
	}
	p.curPage = nil
	p.curPageHdr = format.NewPageHeader()
	p.err = nil

	for p.rowsSeen < p.nrows {
		allowedPgSz := defaultPageHeaderSize
		p.decompressBuffer.Reset()
		for {
			view, err := p.r.Peek(allowedPgSz)
			if err != nil && err != io.EOF {
				p.err = err
				return false
			}

			if len(view) == 0 {
				return false
			}

			extra := 0
			if p.cryptoCtx.MetaDecryptor != nil {
				p.updateDecryption(p.cryptoCtx.MetaDecryptor, encryption.DictPageHeaderModule, p.dataPageHeaderAad)
				view = p.cryptoCtx.MetaDecryptor.Decrypt(view)
				extra = p.cryptoCtx.MetaDecryptor.CiphertextSizeDelta()
			}

			remaining, err := thrift.DeserializeThrift(p.curPageHdr, view)
			if err != nil {
				allowedPgSz *= 2
				if allowedPgSz > p.maxPageHeaderSize {
					p.err = xerrors.New("parquet: deserializing page header failed")
					return false
				}
				continue
			}

			p.r.Discard(len(view) - int(remaining) + extra)
			break
		}

		lenCompressed := int(p.curPageHdr.GetCompressedPageSize())
		lenUncompressed := int(p.curPageHdr.GetUncompressedPageSize())
		if lenCompressed < 0 || lenUncompressed < 0 {
			p.err = xerrors.New("parquet: invalid page header")
			return false
		}

		if p.cryptoCtx.DataDecryptor != nil {
			p.updateDecryption(p.cryptoCtx.DataDecryptor, encryption.DictPageModule, p.dataPageAad)
		}

		buf := memory.NewResizableBuffer(p.mem)
		defer buf.Release()
		buf.ResizeNoShrink(lenUncompressed)

		switch p.curPageHdr.GetType() {
		case format.PageType_DICTIONARY_PAGE:
			p.cryptoCtx.StartDecryptWithDictionaryPage = false
			dictHeader := p.curPageHdr.GetDictionaryPageHeader()
			if dictHeader.GetNumValues() < 0 {
				p.err = xerrors.New("parquet: invalid page header (negative number of values)")
				return false
			}

			data, err := p.decompress(lenCompressed, buf.Bytes())
			if err != nil {
				p.err = err
				return false
			}
			if len(data) != lenUncompressed {
				p.err = fmt.Errorf("parquet: metadata said %d bytes uncompressed dictionary page, got %d bytes", lenUncompressed, len(data))
				return false
			}

			// make dictionary page
			p.curPage = &DictionaryPage{
				page: page{
					buf:      memory.NewBufferBytes(data),
					typ:      p.curPageHdr.Type,
					nvals:    dictHeader.GetNumValues(),
					encoding: dictHeader.GetEncoding(),
				},
				sorted: dictHeader.IsSetIsSorted() && dictHeader.GetIsSorted(),
			}

		case format.PageType_DATA_PAGE:
			p.pageOrd++
			dataHeader := p.curPageHdr.GetDataPageHeader()
			if dataHeader.GetNumValues() < 0 {
				p.err = xerrors.New("parquet: invalid page header (negative number of values)")
				return false
			}

			p.rowsSeen += int64(dataHeader.GetNumValues())
			data, err := p.decompress(lenCompressed, buf.Bytes())
			if err != nil {
				p.err = err
				return false
			}
			if len(data) != lenUncompressed {
				p.err = fmt.Errorf("parquet: metadata said %d bytes uncompressed data page, got %d bytes", lenUncompressed, len(data))
				return false
			}

			// make datapagev1
			p.curPage = &DataPageV1{
				page: page{
					buf:      memory.NewBufferBytes(data),
					typ:      p.curPageHdr.Type,
					nvals:    dataHeader.GetNumValues(),
					encoding: dataHeader.GetEncoding(),
				},
				defLvlEncoding:   dataHeader.GetDefinitionLevelEncoding(),
				repLvlEncoding:   dataHeader.GetRepetitionLevelEncoding(),
				uncompressedSize: int32(lenUncompressed),
				statistics:       extractStats(dataHeader),
			}
		case format.PageType_DATA_PAGE_V2:
			p.pageOrd++
			dataHeader := p.curPageHdr.GetDataPageHeaderV2()
			if dataHeader.GetNumValues() < 0 {
				p.err = xerrors.New("parquet: invalid page header (negative number of values)")
				return false
			}

			if dataHeader.GetDefinitionLevelsByteLength() < 0 || dataHeader.GetRepetitionLevelsByteLength() < 0 {
				p.err = xerrors.New("parquet: invalid page header (negative levels byte length)")
				return false
			}

			compressed := dataHeader.GetIsCompressed()
			// extract stats
			p.rowsSeen += int64(dataHeader.GetNumValues())
			levelsBytelen, ok := overflow.Add(int(dataHeader.GetDefinitionLevelsByteLength()), int(dataHeader.GetRepetitionLevelsByteLength()))
			if !ok {
				p.err = xerrors.New("parquet: levels size too large (corrupt file?)")
				return false
			}

			if compressed {
				if levelsBytelen > 0 {
					io.ReadFull(p.r, buf.Bytes()[:levelsBytelen])
				}
				if _, p.err = p.decompress(lenCompressed-levelsBytelen, buf.Bytes()[levelsBytelen:]); p.err != nil {
					return false
				}
			} else {
				io.ReadFull(p.r, buf.Bytes())
			}
			buf.Retain()

			if buf.Len() != lenUncompressed {
				p.err = fmt.Errorf("parquet: metadata said %d bytes uncompressed data page, got %d bytes", lenUncompressed, buf.Len())
				return false
			}

			// make datapage v2
			p.curPage = &DataPageV2{
				page: page{
					buf:      buf,
					typ:      p.curPageHdr.Type,
					nvals:    dataHeader.GetNumValues(),
					encoding: dataHeader.GetEncoding(),
				},
				nulls:            dataHeader.GetNumNulls(),
				nrows:            dataHeader.GetNumRows(),
				defLvlByteLen:    dataHeader.GetDefinitionLevelsByteLength(),
				repLvlByteLen:    dataHeader.GetRepetitionLevelsByteLength(),
				compressed:       compressed,
				uncompressedSize: int32(lenUncompressed),
				statistics:       extractStats(dataHeader),
			}
		default:
			// we don't know this page type, we're allowed to skip non-data pages
			continue
		}
		return true
	}

	return false
}

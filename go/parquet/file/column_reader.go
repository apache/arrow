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
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/parquet"
	"github.com/apache/arrow/go/parquet/internal/encoding"
	"github.com/apache/arrow/go/parquet/internal/encryption"
	format "github.com/apache/arrow/go/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/parquet/internal/utils"
	"github.com/apache/arrow/go/parquet/schema"
	"golang.org/x/xerrors"
)

const (
	// 4 MB is the default maximum page header size
	defaultMaxPageHeaderSize = 4 * 1024 * 1024
	// 16 KB is the default expected page header size
	defaultPageHeaderSize = 16 * 1024
)

//go:generate go run ../../arrow/_tools/tmpl/main.go -i -data=../internal/encoding/physical_types.tmpldata column_reader_types.gen.go.tmpl

func isDictIndexEncoding(e format.Encoding) bool {
	return e == format.Encoding_RLE_DICTIONARY || e == format.Encoding_PLAIN_DICTIONARY
}

func colHasSpacedValues(c *schema.Column) bool {
	if c.MaxRepetitionLevel() > 0 {
		// repeated + flat case
		return c.SchemaNode().RepetitionType() != parquet.Repetitions.Required
	}

	// non-repeated+nested case
	// find if a node forces nulls in the lowest level along the hierarchy
	n := c.SchemaNode()
	for n != nil {
		if n.RepetitionType() == parquet.Repetitions.Optional {
			return true
		}
		n = n.Parent()
	}
	return false
}

// CryptoContext is a context for keeping track of the current methods for decrypting.
// It keeps track of the row group and column numbers along with references to the
// decryptor objects.
type CryptoContext struct {
	StartDecryptWithDictionaryPage bool
	RowGroupOrdinal                int16
	ColumnOrdinal                  int16
	MetaDecryptor                  encryption.Decryptor
	DataDecryptor                  encryption.Decryptor
}

// ColumnReader is the basic interface for all column readers.
//
// To actually Read out the column data, you need to convert to the properly
// typed ColumnReader type such as *BooleanColumnReader etc.
type ColumnReader interface {
	// HasNext returns whether there is more data to be read in this column
	// and row group.
	HasNext() bool
	// Type returns the underlying physical type of the column
	Type() parquet.Type
	// Descriptor returns the column schema container
	Descriptor() *schema.Column
	// if HasNext returns false because of an error, this will return the error
	// it encountered. Otherwise this will be nil if it's just the end of the
	// column
	Err() error
	// Skip buffered values
	consumeBufferedValues(int64)
	// number of available values left
	numAvail() int64
	// read the definition levels and return the number of definitions,
	// and the number of values to be read (number of def levels == maxdef level)
	// it also populates the passed in slice which should be sized appropriately.
	readDefinitionLevels(levels []int16) (int, int64)
	// read the repetition levels and return the number of repetition levels read
	// also populates the passed in slice, which should be sized appropriately.
	readRepetitionLevels(levels []int16) int
	// get the current page reader
	pager() PageReader
	// set a page reader into the columnreader so it can be reused.
	setPageReader(PageReader)
}

type columnReader struct {
	descr             *schema.Column
	rdr               PageReader
	repetitionDecoder encoding.LevelDecoder
	definitionDecoder encoding.LevelDecoder

	curPage     Page
	curEncoding format.Encoding
	curDecoder  encoding.TypedDecoder

	// number of currently buffered values in the current page
	numBuffered int64
	// the number of values we've decoded so far
	numDecoded int64
	// will be true if we have read in a new dictionary page
	newDict bool
	mem     memory.Allocator

	decoders      map[format.Encoding]encoding.TypedDecoder
	decoderTraits encoding.DecoderTraits

	// is set when an error is encountered
	err          error
	defLvlBuffer []int16
}

// NewColumnReader returns a column reader for the provided column initialized with the given pagereader that will
// provide the pages of data for this column. The type is determined from the column passed in.
func NewColumnReader(descr *schema.Column, pageReader PageReader, mem memory.Allocator) ColumnReader {
	base := columnReader{descr: descr, rdr: pageReader, mem: mem, decoders: make(map[format.Encoding]encoding.TypedDecoder)}
	switch descr.PhysicalType() {
	case parquet.Types.FixedLenByteArray:
		base.decoderTraits = &encoding.FixedLenByteArrayDecoderTraits
		return &FixedLenByteArrayColumnReader{base}
	case parquet.Types.Float:
		base.decoderTraits = &encoding.Float32DecoderTraits
		return &Float32ColumnReader{base}
	case parquet.Types.Double:
		base.decoderTraits = &encoding.Float64DecoderTraits
		return &Float64ColumnReader{base}
	case parquet.Types.ByteArray:
		base.decoderTraits = &encoding.ByteArrayDecoderTraits
		return &ByteArrayColumnReader{base}
	case parquet.Types.Int32:
		base.decoderTraits = &encoding.Int32DecoderTraits
		return &Int32ColumnReader{base}
	case parquet.Types.Int64:
		base.decoderTraits = &encoding.Int64DecoderTraits
		return &Int64ColumnReader{base}
	case parquet.Types.Int96:
		base.decoderTraits = &encoding.Int96DecoderTraits
		return &Int96ColumnReader{base}
	case parquet.Types.Boolean:
		base.decoderTraits = &encoding.BooleanDecoderTraits
		return &BooleanColumnReader{base}
	}
	return nil
}

func (c *columnReader) Err() error                    { return c.err }
func (c *columnReader) Type() parquet.Type            { return c.descr.PhysicalType() }
func (c *columnReader) Descriptor() *schema.Column    { return c.descr }
func (c *columnReader) consumeBufferedValues(n int64) { c.numDecoded += n }
func (c *columnReader) numAvail() int64               { return c.numBuffered - c.numDecoded }
func (c *columnReader) pager() PageReader             { return c.rdr }
func (c *columnReader) setPageReader(rdr PageReader) {
	c.rdr = rdr
	c.decoders = make(map[format.Encoding]encoding.TypedDecoder)
	c.err = nil
}

func (c *columnReader) getDefLvlBuffer(sz int64) []int16 {
	if int64(len(c.defLvlBuffer)) < sz {
		c.defLvlBuffer = make([]int16, sz)
		return c.defLvlBuffer
	}

	return c.defLvlBuffer[:sz]
}

// HasNext returns whether there is more data to be read in this column
// and row group.
func (c *columnReader) HasNext() bool {
	if c.numBuffered == 0 || c.numDecoded == c.numBuffered {
		return c.readNewPage() && c.numBuffered != 0
	}
	return true
}

func (c *columnReader) configureDict(page *DictionaryPage) error {
	enc := page.encoding
	if enc == format.Encoding_PLAIN_DICTIONARY || enc == format.Encoding_PLAIN {
		enc = format.Encoding_RLE_DICTIONARY
	}

	if _, ok := c.decoders[enc]; ok {
		return xerrors.New("parquet: column cannot have more than one dictionary.")
	}

	switch page.Encoding() {
	case format.Encoding_PLAIN, format.Encoding_PLAIN_DICTIONARY:
		dict := c.decoderTraits.Decoder(parquet.Encodings.Plain, c.descr, false, c.mem)
		dict.SetData(int(page.NumValues()), page.Data())

		decoder := c.decoderTraits.Decoder(parquet.Encodings.Plain, c.descr, true, c.mem).(encoding.DictDecoder)
		decoder.SetDict(dict)
		c.decoders[enc] = decoder
	default:
		return xerrors.New("parquet: dictionary index must be plain encoding")
	}

	c.curDecoder = c.decoders[enc]
	c.newDict = true
	return nil
}

// read a new page from the page reader
func (c *columnReader) readNewPage() bool {
	for c.rdr.Next() { // keep going until we get a data page
		c.curPage = c.rdr.Page()
		if c.curPage == nil {
			break
		}

		var lvlByteLen int64
		switch p := c.curPage.(type) {
		case *DictionaryPage:
			if err := c.configureDict(p); err != nil {
				c.err = err
				return false
			}
			continue
		case *DataPageV1:
			lvlByteLen, c.err = c.initLevelDecodersV1(p, p.repLvlEncoding, p.defLvlEncoding)
			if c.err != nil {
				return false
			}
		case *DataPageV2:
			lvlByteLen, c.err = c.initLevelDecodersV2(p)
			if c.err != nil {
				return false
			}
		default:
			// we can skip non-data pages
			continue
		}

		c.err = c.initDataDecoder(c.curPage, lvlByteLen)
		return c.err == nil
	}
	c.err = c.rdr.Err()
	return false
}

func (c *columnReader) initLevelDecodersV2(page *DataPageV2) (int64, error) {
	c.numBuffered = int64(page.nvals)
	c.numDecoded = 0
	buf := page.Data()
	totalLvlLen := int64(page.repLvlBytelen) + int64(page.defLvlBytelen)

	if totalLvlLen > int64(len(buf)) {
		return totalLvlLen, xerrors.New("parquet: data page too small for levels (corrupt header?)")
	}

	if c.descr.MaxRepetitionLevel() > 0 {
		c.repetitionDecoder.SetDataV2(page.repLvlBytelen, c.descr.MaxRepetitionLevel(), int(c.numBuffered), buf)
		buf = buf[page.repLvlBytelen:]
	}

	if c.descr.MaxDefinitionLevel() > 0 {
		c.definitionDecoder.SetDataV2(page.defLvlBytelen, c.descr.MaxDefinitionLevel(), int(c.numBuffered), buf)
	}

	return totalLvlLen, nil
}

func (c *columnReader) initLevelDecodersV1(page *DataPageV1, repLvlEncoding, defLvlEncoding format.Encoding) (int64, error) {
	c.numBuffered = int64(page.nvals)
	c.numDecoded = 0

	buf := page.Data()
	maxSize := len(buf)
	levelsByteLen := int64(0)

	// Data page layout: Repetition Levels - Definition Levels - encoded values.
	// Levels are encoded as rle or bit-packed
	if c.descr.MaxRepetitionLevel() > 0 {
		repBytes, err := c.repetitionDecoder.SetData(parquet.Encoding(repLvlEncoding), c.descr.MaxRepetitionLevel(), int(c.numBuffered), buf)
		if err != nil {
			return levelsByteLen, err
		}
		buf = buf[repBytes:]
		maxSize -= repBytes
		levelsByteLen += int64(repBytes)
	}

	if c.descr.MaxDefinitionLevel() > 0 {
		defBytes, err := c.definitionDecoder.SetData(parquet.Encoding(defLvlEncoding), c.descr.MaxDefinitionLevel(), int(c.numBuffered), buf)
		if err != nil {
			return levelsByteLen, err
		}
		levelsByteLen += int64(defBytes)
		maxSize -= defBytes
	}

	return levelsByteLen, nil
}

func (c *columnReader) initDataDecoder(page Page, lvlByteLen int64) error {
	buf := page.Data()
	if int64(len(buf)) < lvlByteLen {
		return xerrors.New("parquet: page smaller than size of encoded levels")
	}

	buf = buf[lvlByteLen:]
	encoding := page.Encoding()

	if isDictIndexEncoding(encoding) {
		encoding = format.Encoding_RLE_DICTIONARY
	}

	if decoder, ok := c.decoders[encoding]; ok {
		c.curDecoder = decoder
	} else {
		switch encoding {
		case format.Encoding_PLAIN,
			format.Encoding_DELTA_BYTE_ARRAY,
			format.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			format.Encoding_DELTA_BINARY_PACKED:
			c.curDecoder = c.decoderTraits.Decoder(parquet.Encoding(encoding), c.descr, false, c.mem)
			c.decoders[encoding] = c.curDecoder
		case format.Encoding_RLE_DICTIONARY:
			return xerrors.New("parquet: dictionary page must be before data page")
		case format.Encoding_BYTE_STREAM_SPLIT:
			return xerrors.New("parquet: unsupported data encoding")
		default:
			return xerrors.New("parquet: unknown encoding type")
		}
	}

	c.curEncoding = encoding
	c.curDecoder.SetData(int(c.numBuffered), buf)
	return nil
}

func (c *columnReader) readDefinitionLevels(levels []int16) (int, int64) {
	if c.descr.MaxDefinitionLevel() == 0 {
		return 0, 0
	}

	return c.definitionDecoder.Decode(levels)
}

func (c *columnReader) readRepetitionLevels(levels []int16) int {
	if c.descr.MaxRepetitionLevel() == 0 {
		return 0
	}

	nlevels, _ := c.repetitionDecoder.Decode(levels)
	return nlevels
}

func (c *columnReader) determineNumToRead(batchLen int64, defLvls, repLvls []int16) (ndefs int, toRead int64, err error) {
	if !c.HasNext() {
		return 0, 0, c.err
	}

	size := utils.Min(batchLen, c.numBuffered-c.numDecoded)

	if c.descr.MaxDefinitionLevel() > 0 {
		if defLvls == nil {
			defLvls = c.getDefLvlBuffer(size)
		}
		ndefs, toRead = c.readDefinitionLevels(defLvls[:size])
	} else {
		toRead = size
	}

	if c.descr.MaxRepetitionLevel() > 0 && repLvls != nil {
		nreps := c.readRepetitionLevels(repLvls[:size])
		if defLvls != nil && ndefs != nreps {
			err = xerrors.New("parquet: number of decoded rep/def levels did not match")
		}
	}
	return
}

func (c *columnReader) determineNumToReadSpaced(batchLen int64, defLvls, repLvls []int16) (ndefs int, toRead int64, err error) {
	if !c.HasNext() {
		return 0, 0, c.err
	}

	batchLen = utils.Min(batchLen, c.numBuffered-c.numDecoded)

	if c.descr.MaxDefinitionLevel() > 0 {
		ndefs, toRead = c.readDefinitionLevels(defLvls[:batchLen])

		if c.descr.MaxRepetitionLevel() > 0 {
			nreps := c.readRepetitionLevels(repLvls[:batchLen])
			if ndefs != nreps {
				err = xerrors.New("parquet: number of decoded rep/def levels did not match")
			}
		}
	} else {
		toRead = batchLen
	}
	return
}

// skip some number of rows using readFn as the function to read the data and throw it away.
// If we can skip a whole page based on its metadata, then we do so, otherwise we read the
// page until we have skipped the number of rows desired.
func (c *columnReader) skip(nrows int64, readFn func(batch int64, buf []byte) (int64, error)) (int64, error) {
	var err error
	toskip := nrows
	for c.HasNext() && toskip > 0 {
		// if number to skip is more than the number of undecoded values, skip the page
		if toskip > (c.numBuffered - c.numDecoded) {
			toskip -= c.numBuffered - c.numDecoded
			c.numDecoded = c.numBuffered
		} else {
			var (
				batchSize int64 = 1024
				valsRead  int64 = 0
			)

			scratch := memory.NewResizableBuffer(c.mem)
			scratch.Reserve(c.decoderTraits.BytesRequired(int(batchSize)))
			defer scratch.Release()

			for {
				batchSize = utils.Min(batchSize, toskip)
				valsRead, err = readFn(batchSize, scratch.Buf())
				toskip -= valsRead
				if valsRead <= 0 || toskip <= 0 || err != nil {
					break
				}
			}
		}
	}
	if c.err != nil {
		err = c.err
	}
	return nrows - toskip, err
}

// base function for reading a batch of values, this will read until it either reads in batchSize values or
// it hits the end of the column, including reading multiple pages.
func (c *columnReader) readBatch(batchSize int64, defLvls, repLvls []int16, readFn readerFunc) (int64, int, error) {
	var (
		totalValues int64
		totalRead   int
		err         error
		read        int
		defs        []int16
		reps        []int16
		ndefs       int
		toRead      int64
	)

	for c.HasNext() && totalValues < batchSize && err == nil {
		if defLvls != nil {
			defs = defLvls[totalValues:]
		}
		if repLvls != nil {
			reps = repLvls[totalValues:]
		}
		ndefs, toRead, err = c.determineNumToRead(batchSize-totalValues, defs, reps)
		if err != nil {
			return totalValues, totalRead, err
		}

		read, err = readFn(int64(totalRead), toRead)
		totalVals := int64(utils.MaxInt(ndefs, read))
		c.consumeBufferedValues(totalVals)

		totalValues += totalVals
		totalRead += read
	}
	return totalValues, totalRead, err
}

type readerFunc func(int64, int64) (int, error)
type spacedReaderFunc func(int64, int, []byte, int64) (int, error)

// base function for reading a batch of spaced values
func (c *columnReader) readBatchSpaced(batchSize int64, defLvls, repLvls []int16, validBits []byte, validBitsOffset int64, readFn readerFunc, readSpacedFn spacedReaderFunc) (totalVals, valsRead, nullCount, levelsRead int64, err error) {
	// TODO(mtopol): keep reading data pages until batchSize is reached or the row group is finished
	// implemented for reading non-spaced, but this is a bit more difficult to implement for spaced
	// values, so it's not yet implemented. For now this needs to be called until it returns 0 read
	ndefs, toRead, err := c.determineNumToReadSpaced(batchSize, defLvls, repLvls)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	var vals int

	if ndefs > 0 {
		if !colHasSpacedValues(c.descr) {
			vals, err = readFn(0, toRead)
			totalVals = int64(vals)
			bitutil.SetBitsTo(validBits, validBitsOffset, totalVals, true)
			valsRead = totalVals
		} else {
			info := LevelInfo{
				RepeatedAncestorDefLevel: c.descr.MaxDefinitionLevel() - 1,
				DefLevel:                 c.descr.MaxDefinitionLevel(),
				RepLevel:                 c.descr.MaxRepetitionLevel(),
			}
			validity := ValidityBitmapInputOutput{
				ReadUpperBound:  int64(ndefs),
				ValidBits:       validBits,
				ValidBitsOffset: validBitsOffset,
				NullCount:       nullCount,
				Read:            valsRead,
			}

			DefLevelsToBitmap(defLvls[:ndefs], info, &validity)
			nullCount = validity.NullCount
			valsRead = validity.Read
			vals, err = readSpacedFn(valsRead, int(nullCount), validBits, validBitsOffset)
			totalVals = int64(vals)
		}
		levelsRead = int64(ndefs)
	} else {
		// required field read all values
		vals, err = readFn(0, toRead)
		totalVals = int64(vals)
		bitutil.SetBitsTo(validBits, validBitsOffset, totalVals, true)
		nullCount = 0
		valsRead = totalVals
		levelsRead = totalVals
	}

	c.consumeBufferedValues(levelsRead)
	return
}

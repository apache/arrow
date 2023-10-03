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

package testutils

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/utils"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/internal/encoding"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/stretchr/testify/mock"
)

type DataPageBuilder struct {
	sink    io.Writer
	version parquet.DataPageVersion

	nvals          int
	encoding       parquet.Encoding
	defLvlEncoding parquet.Encoding
	repLvlEncoding parquet.Encoding
	defLvlBytesLen int
	repLvlBytesLen int
	hasDefLvls     bool
	hasRepLvls     bool
	hasValues      bool
}

var mem = memory.NewGoAllocator()

func (d *DataPageBuilder) appendLevels(lvls []int16, maxLvl int16, e parquet.Encoding) int {
	if e != parquet.Encodings.RLE {
		panic("parquet: only rle encoding currently implemented")
	}

	buf := encoding.NewBufferWriter(encoding.LevelEncodingMaxBufferSize(e, maxLvl, len(lvls)), memory.DefaultAllocator)
	var enc encoding.LevelEncoder
	enc.Init(e, maxLvl, buf)
	enc.Encode(lvls)

	rleBytes := enc.Len()
	if d.version == parquet.DataPageV1 {
		if err := binary.Write(d.sink, binary.LittleEndian, int32(rleBytes)); err != nil {
			panic(err)
		}
	}

	if _, err := d.sink.Write(buf.Bytes()[:rleBytes]); err != nil {
		panic(err)
	}
	return rleBytes
}

func (d *DataPageBuilder) AppendDefLevels(lvls []int16, maxLvl int16) {
	d.defLvlBytesLen = d.appendLevels(lvls, maxLvl, parquet.Encodings.RLE)

	d.nvals = utils.MaxInt(len(lvls), d.nvals)
	d.defLvlEncoding = parquet.Encodings.RLE
	d.hasDefLvls = true
}

func (d *DataPageBuilder) AppendRepLevels(lvls []int16, maxLvl int16) {
	d.repLvlBytesLen = d.appendLevels(lvls, maxLvl, parquet.Encodings.RLE)

	d.nvals = utils.MaxInt(len(lvls), d.nvals)
	d.repLvlEncoding = parquet.Encodings.RLE
	d.hasRepLvls = true
}

func (d *DataPageBuilder) AppendValues(desc *schema.Column, values interface{}, e parquet.Encoding) {
	enc := encoding.NewEncoder(desc.PhysicalType(), e, false, desc, mem)
	var sz int
	switch v := values.(type) {
	case []bool:
		enc.(encoding.BooleanEncoder).Put(v)
		sz = len(v)
	case []int32:
		enc.(encoding.Int32Encoder).Put(v)
		sz = len(v)
	case []int64:
		enc.(encoding.Int64Encoder).Put(v)
		sz = len(v)
	case []parquet.Int96:
		enc.(encoding.Int96Encoder).Put(v)
		sz = len(v)
	case []float32:
		enc.(encoding.Float32Encoder).Put(v)
		sz = len(v)
	case []float64:
		enc.(encoding.Float64Encoder).Put(v)
		sz = len(v)
	case []parquet.ByteArray:
		enc.(encoding.ByteArrayEncoder).Put(v)
		sz = len(v)
	default:
		panic(fmt.Sprintf("no testutil data page builder for type %T", values))
	}
	buf, _ := enc.FlushValues()
	_, err := d.sink.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}

	d.nvals = utils.MaxInt(sz, d.nvals)
	d.encoding = e
	d.hasValues = true
}

type DictionaryPageBuilder struct {
	traits        encoding.DictEncoder
	numDictValues int32
	hasValues     bool
}

func NewDictionaryPageBuilder(d *schema.Column) *DictionaryPageBuilder {
	return &DictionaryPageBuilder{
		encoding.NewEncoder(d.PhysicalType(), parquet.Encodings.Plain, true, d, mem).(encoding.DictEncoder),
		0, false}
}

func (d *DictionaryPageBuilder) AppendValues(values interface{}) encoding.Buffer {
	switch v := values.(type) {
	case []int32:
		d.traits.(encoding.Int32Encoder).Put(v)
	case []int64:
		d.traits.(encoding.Int64Encoder).Put(v)
	case []parquet.Int96:
		d.traits.(encoding.Int96Encoder).Put(v)
	case []float32:
		d.traits.(encoding.Float32Encoder).Put(v)
	case []float64:
		d.traits.(encoding.Float64Encoder).Put(v)
	case []parquet.ByteArray:
		d.traits.(encoding.ByteArrayEncoder).Put(v)
	default:
		panic(fmt.Sprintf("no testutil dictionary page builder for type %T", values))
	}

	d.numDictValues = int32(d.traits.NumEntries())
	d.hasValues = true
	buf, _ := d.traits.FlushValues()
	return buf
}

func (d *DictionaryPageBuilder) WriteDict() *memory.Buffer {
	buf := memory.NewBufferBytes(make([]byte, d.traits.DictEncodedSize()))
	d.traits.WriteDict(buf.Bytes())
	return buf
}

func (d *DictionaryPageBuilder) NumValues() int32 {
	return d.numDictValues
}

func MakeDataPage(dataPageVersion parquet.DataPageVersion, d *schema.Column, values interface{}, nvals int, e parquet.Encoding, indexBuffer encoding.Buffer, defLvls, repLvls []int16, maxDef, maxRep int16) file.Page {
	num := 0

	stream := encoding.NewBufferWriter(1024, mem)
	builder := DataPageBuilder{sink: stream, version: dataPageVersion}

	if len(repLvls) > 0 {
		builder.AppendRepLevels(repLvls, maxRep)
	}
	if len(defLvls) > 0 {
		builder.AppendDefLevels(defLvls, maxDef)
	}

	if e == parquet.Encodings.Plain {
		builder.AppendValues(d, values, e)
		num = builder.nvals
	} else {
		stream.Write(indexBuffer.Bytes())
		num = utils.MaxInt(builder.nvals, nvals)
	}

	buf := stream.Finish()
	if dataPageVersion == parquet.DataPageV1 {
		return file.NewDataPageV1(buf, int32(num), e, builder.defLvlEncoding, builder.repLvlEncoding, int32(buf.Len()))
	}
	return file.NewDataPageV2(buf, int32(num), 0, int32(num), e, int32(builder.defLvlBytesLen), int32(builder.repLvlBytesLen), int32(buf.Len()), false)
}

func MakeDictPage(d *schema.Column, values interface{}, valuesPerPage []int, e parquet.Encoding) (*file.DictionaryPage, []encoding.Buffer) {
	bldr := NewDictionaryPageBuilder(d)
	npages := len(valuesPerPage)

	ref := reflect.ValueOf(values)
	valStart := 0

	rleIndices := make([]encoding.Buffer, 0, npages)
	for _, nvals := range valuesPerPage {
		rleIndices = append(rleIndices, bldr.AppendValues(ref.Slice(valStart, valStart+nvals).Interface()))
		valStart += nvals
	}

	buffer := bldr.WriteDict()
	return file.NewDictionaryPage(buffer, bldr.NumValues(), parquet.Encodings.Plain), rleIndices
}

type MockPageReader struct {
	mock.Mock

	curpage int
}

func (m *MockPageReader) Err() error {
	return m.Called().Error(0)
}

func (m *MockPageReader) Reset(parquet.BufferedReader, int64, compress.Compression, *file.CryptoContext) {
}

func (m *MockPageReader) SetMaxPageHeaderSize(int) {}

func (m *MockPageReader) Page() file.Page {
	return m.TestData().Get("pages").Data().([]file.Page)[m.curpage-1]
}

func (m *MockPageReader) Next() bool {
	pageList := m.TestData().Get("pages").Data().([]file.Page)
	m.curpage++
	return len(pageList) >= m.curpage
}

func PaginatePlain(version parquet.DataPageVersion, d *schema.Column, values reflect.Value, defLevels, repLevels []int16,
	maxDef, maxRep int16, lvlsPerPage int, valuesPerPage []int, enc parquet.Encoding) []file.Page {

	var (
		npages      = len(valuesPerPage)
		defLvlStart = 0
		defLvlEnd   = 0
		repLvlStart = 0
		repLvlEnd   = 0
		valueStart  = 0
	)

	pageList := make([]file.Page, 0, npages)
	for i := 0; i < npages; i++ {
		if maxDef > 0 {
			defLvlStart = i * lvlsPerPage
			defLvlEnd = (i + 1) * lvlsPerPage
		}
		if maxRep > 0 {
			repLvlStart = i * lvlsPerPage
			repLvlEnd = (i + 1) * lvlsPerPage
		}

		page := MakeDataPage(version, d,
			values.Slice(valueStart, valueStart+valuesPerPage[i]).Interface(),
			valuesPerPage[i], enc, nil, defLevels[defLvlStart:defLvlEnd],
			repLevels[repLvlStart:repLvlEnd], maxDef, maxRep)
		valueStart += valuesPerPage[i]
		pageList = append(pageList, page)
	}
	return pageList
}

func PaginateDict(version parquet.DataPageVersion, d *schema.Column, values reflect.Value, defLevels, repLevels []int16, maxDef, maxRep int16, lvlsPerPage int, valuesPerPage []int, enc parquet.Encoding) []file.Page {
	var (
		npages   = len(valuesPerPage)
		pages    = make([]file.Page, 0, npages)
		defStart = 0
		defEnd   = 0
		repStart = 0
		repEnd   = 0
	)

	dictPage, rleIndices := MakeDictPage(d, values.Interface(), valuesPerPage, enc)
	pages = append(pages, dictPage)
	for i := 0; i < npages; i++ {
		if maxDef > 0 {
			defStart = i * lvlsPerPage
			defEnd = (i + 1) * lvlsPerPage
		}
		if maxRep > 0 {
			repStart = i * lvlsPerPage
			repEnd = (i + 1) * lvlsPerPage
		}
		page := MakeDataPage(version, d, nil, valuesPerPage[i], enc, rleIndices[i],
			defLevels[defStart:defEnd], repLevels[repStart:repEnd], maxDef, maxRep)
		pages = append(pages, page)
	}
	return pages
}

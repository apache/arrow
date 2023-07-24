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
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func initValues(values reflect.Value) {
	if values.Kind() != reflect.Slice {
		panic("must init values with slice")
	}

	r := rand.New(rand.NewSource(0))
	typ := values.Type().Elem()
	switch {
	case typ.Kind() == reflect.Bool:
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int31n(2) == 1))
		}
	case typ.Bits() <= 32:
		max := int64(math.MaxInt32)
		min := int64(math.MinInt32)
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int63n(max-min+1) + min).Convert(reflect.TypeOf(int32(0))))
		}
	case typ.Bits() <= 64:
		max := int64(math.MaxInt64)
		min := int64(math.MinInt64)
		for i := 0; i < values.Len(); i++ {
			values.Index(i).Set(reflect.ValueOf(r.Int63n(max-min+1) + min))
		}
	}
}

func initDictValues(values reflect.Value, numDicts int) {
	repeatFactor := values.Len() / numDicts
	initValues(values)
	// add some repeated values
	for j := 1; j < repeatFactor; j++ {
		for i := 0; i < numDicts; i++ {
			values.Index(numDicts*j + i).Set(values.Index(i))
		}
	}
	// computed only dict_per_page * repeat_factor - 1 values < num_values compute remaining
	for i := numDicts * repeatFactor; i < values.Len(); i++ {
		values.Index(i).Set(values.Index(i - numDicts*repeatFactor))
	}
}

func makePages(version parquet.DataPageVersion, d *schema.Column, npages, lvlsPerPage int, typ reflect.Type, enc parquet.Encoding) ([]file.Page, int, reflect.Value, []int16, []int16) {
	nlevels := lvlsPerPage * npages
	nvalues := 0

	maxDef := d.MaxDefinitionLevel()
	maxRep := d.MaxRepetitionLevel()

	var (
		defLevels []int16
		repLevels []int16
	)

	valuesPerPage := make([]int, npages)
	if maxDef > 0 {
		defLevels = make([]int16, nlevels)
		testutils.FillRandomInt16(0, 0, maxDef, defLevels)
		for idx := range valuesPerPage {
			numPerPage := 0
			for i := 0; i < lvlsPerPage; i++ {
				if defLevels[i+idx*lvlsPerPage] == maxDef {
					numPerPage++
					nvalues++
				}
			}
			valuesPerPage[idx] = numPerPage
		}
	} else {
		nvalues = nlevels
		valuesPerPage[0] = lvlsPerPage
		for i := 1; i < len(valuesPerPage); i *= 2 {
			copy(valuesPerPage[i:], valuesPerPage[:i])
		}
	}

	if maxRep > 0 {
		repLevels = make([]int16, nlevels)
		testutils.FillRandomInt16(0, 0, maxRep, repLevels)
	}

	values := reflect.MakeSlice(reflect.SliceOf(typ), nvalues, nvalues)
	if enc == parquet.Encodings.Plain {
		initValues(values)
		return testutils.PaginatePlain(version, d, values, defLevels, repLevels, maxDef, maxRep, lvlsPerPage, valuesPerPage, parquet.Encodings.Plain), nvalues, values, defLevels, repLevels
	} else if enc == parquet.Encodings.PlainDict || enc == parquet.Encodings.RLEDict {
		initDictValues(values, lvlsPerPage)
		return testutils.PaginateDict(version, d, values, defLevels, repLevels, maxDef, maxRep, lvlsPerPage, valuesPerPage, parquet.Encodings.RLEDict), nvalues, values, defLevels, repLevels
	}
	panic("invalid encoding type for make pages")
}

//lint:ignore U1000 compareVectorWithDefLevels
func compareVectorWithDefLevels(left, right reflect.Value, defLevels []int16, maxDef, maxRep int16) assert.Comparison {
	return func() bool {
		if left.Kind() != reflect.Slice || right.Kind() != reflect.Slice {
			return false
		}

		if left.Type().Elem() != right.Type().Elem() {
			return false
		}

		iLeft, iRight := 0, 0
		for _, def := range defLevels {
			if def == maxDef {
				if !reflect.DeepEqual(left.Index(iLeft).Interface(), right.Index(iRight).Interface()) {
					return false
				}
				iLeft++
				iRight++
			} else if def == (maxDef - 1) {
				// null entry on the lowest nested level
				iRight++
			} else if def < (maxDef - 1) {
				// null entry on higher nesting level, only supported for non-repeating data
				if maxRep == 0 {
					iRight++
				}
			}
		}
		return true
	}
}

var mem = memory.DefaultAllocator

type PrimitiveReaderSuite struct {
	suite.Suite

	dataPageVersion parquet.DataPageVersion
	pager           file.PageReader
	reader          file.ColumnChunkReader
	pages           []file.Page
	values          reflect.Value
	defLevels       []int16
	repLevels       []int16
	nlevels         int
	nvalues         int
	maxDefLvl       int16
	maxRepLvl       int16

	bufferPool sync.Pool
}

func (p *PrimitiveReaderSuite) SetupTest() {
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

func (p *PrimitiveReaderSuite) TearDownTest() {
	p.clear()
	p.bufferPool = sync.Pool{}
}

func (p *PrimitiveReaderSuite) initReader(d *schema.Column) {
	m := new(testutils.MockPageReader)
	m.Test(p.T())
	m.TestData().Set("pages", p.pages)
	m.On("Err").Return((error)(nil))
	p.pager = m
	p.reader = file.NewColumnReader(d, m, mem, &p.bufferPool)
}

func (p *PrimitiveReaderSuite) checkResults(typ reflect.Type) {
	vresult := reflect.MakeSlice(reflect.SliceOf(typ), p.nvalues, p.nvalues)
	dresult := make([]int16, p.nlevels)
	rresult := make([]int16, p.nlevels)

	var (
		read        int64 = 0
		totalRead   int   = 0
		batchActual int   = 0
		batchSize   int32 = 8
		batch       int   = 0
	)

	p.Require().NotNil(p.reader)

	// this will cover both cases:
	// 1) batch size < page size (multiple ReadBatch from a single page)
	// 2) batch size > page size (BatchRead limits to single page)
	for {
		switch rdr := p.reader.(type) {
		case *file.Int32ColumnChunkReader:
			intVals := make([]int32, batchSize)
			read, batch, _ = rdr.ReadBatch(int64(batchSize), intVals, dresult[batchActual:], rresult[batchActual:])
			for i := 0; i < batch; i++ {
				vresult.Index(totalRead + i).Set(reflect.ValueOf(intVals[i]))
			}

		case *file.BooleanColumnChunkReader:
			boolVals := make([]bool, batchSize)
			read, batch, _ = rdr.ReadBatch(int64(batchSize), boolVals, dresult[batchActual:], rresult[batchActual:])
			for i := 0; i < batch; i++ {
				vresult.Index(totalRead + i).Set(reflect.ValueOf(boolVals[i]))
			}
		default:
			p.Fail("column reader not implemented")
		}

		totalRead += batch
		batchActual += int(read)
		batchSize = int32(utils.MinInt(1<<24, utils.MaxInt(int(batchSize*2), 4096)))
		if batch <= 0 {
			break
		}
	}

	p.Equal(p.nlevels, batchActual)
	p.Equal(p.nvalues, totalRead)
	p.Equal(p.values.Interface(), vresult.Interface())
	if p.maxDefLvl > 0 {
		p.Equal(p.defLevels, dresult)
	}
	if p.maxRepLvl > 0 {
		p.Equal(p.repLevels, rresult)
	}

	// catch improper writes at EOS
	switch rdr := p.reader.(type) {
	case *file.Int32ColumnChunkReader:
		intVals := make([]int32, batchSize)
		read, batchActual, _ = rdr.ReadBatch(5, intVals, nil, nil)
	case *file.BooleanColumnChunkReader:
		boolVals := make([]bool, batchSize)
		read, batchActual, _ = rdr.ReadBatch(5, boolVals, nil, nil)
	default:
		p.Fail("column reader not implemented")
	}

	p.Zero(batchActual)
	p.Zero(read)
}

func (p *PrimitiveReaderSuite) clear() {
	p.values = reflect.ValueOf(nil)
	p.defLevels = nil
	p.repLevels = nil
	p.pages = nil
	p.pager = nil
	p.reader = nil
}

func (p *PrimitiveReaderSuite) testPlain(npages, levels int, d *schema.Column, typ reflect.Type) {
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levels, typ, parquet.Encodings.Plain)
	p.nlevels = npages * levels
	p.initReader(d)
	p.checkResults(typ)
	p.clear()
}

func (p *PrimitiveReaderSuite) testDict(npages, levels int, d *schema.Column, typ reflect.Type) {
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levels, typ, parquet.Encodings.RLEDict)
	p.nlevels = npages * levels
	p.initReader(d)
	p.checkResults(typ)
	p.clear()
}

func (p *PrimitiveReaderSuite) TestBoolFlatRequired() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(true))
}

func (p *PrimitiveReaderSuite) TestBoolFlatOptional() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("b", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(true))
}

func (p *PrimitiveReaderSuite) TestBoolFlatOptionalSkip() {
	const (
		levelsPerPage int = 1000
		npages        int = 5
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewBooleanNode("a", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(true), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]bool, levelsPerPage/2)
	dresult := make([]int16, levelsPerPage/2)
	rresult := make([]int16, levelsPerPage/2)

	rdr := p.reader.(*file.BooleanColumnChunkReader)

	values := p.values.Interface().([]bool)
	rIdx := int64(0)

	p.Run("skip_size > page_size", func() {
		// skip first 2 pages
		skipped, _ := rdr.Skip(int64(2 * levelsPerPage))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:]
			}
			rIdx++
		}
		p.Equal(int64(2*levelsPerPage), skipped)

		// Read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})

	p.Run("skip_size == page_size", func() {
		// skip one page worth of values across page 2 and 3
		skipped, _ := rdr.Skip(int64(levelsPerPage))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:]
			}
			rIdx++
		}
		p.Equal(int64(levelsPerPage), skipped)

		// read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})

	p.Run("skip_size < page_size", func() {
		// skip limited to a single page
		// skip half a page
		skipped, _ := rdr.Skip(int64(levelsPerPage / 2))
		// move test values forward
		for i := int64(0); i < skipped; i++ {
			if p.defLevels[rIdx] == p.maxDefLvl {
				values = values[1:] // move test values forward
			}
			rIdx++
		}
		p.Equal(int64(0.5*float32(levelsPerPage)), skipped)

		// Read half a page
		rowsRead, valsRead, _ := rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := values[0:valsRead]
		p.Equal(subVals, vresult[:valsRead])
		// move test values forward
		rIdx += rowsRead
		values = values[valsRead:]
	})
}

func (p *PrimitiveReaderSuite) TestInt32FlatRequired() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func (p *PrimitiveReaderSuite) TestInt32FlatOptional() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("b", parquet.Repetitions.Optional, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func (p *PrimitiveReaderSuite) TestInt32FlatRepeated() {
	const (
		levelsPerPage int = 100
		npages        int = 50
	)

	p.maxDefLvl = 4
	p.maxRepLvl = 2
	typ := schema.NewInt32Node("c", parquet.Repetitions.Repeated, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.testPlain(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
	p.testDict(npages, levelsPerPage, d, reflect.TypeOf(int32(0)))
}

func (p *PrimitiveReaderSuite) TestReadBatchMultiPage() {
	const (
		levelsPerPage int = 100
		npages        int = 3
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(int32(0)), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]int32, levelsPerPage*npages)
	dresult := make([]int16, levelsPerPage*npages)
	rresult := make([]int16, levelsPerPage*npages)

	rdr := p.reader.(*file.Int32ColumnChunkReader)
	total, read, err := rdr.ReadBatch(int64(levelsPerPage*npages), vresult, dresult, rresult)
	p.NoError(err)
	p.EqualValues(levelsPerPage*npages, total)
	p.EqualValues(levelsPerPage*npages, read)
}

func (p *PrimitiveReaderSuite) TestInt32FlatRequiredSkip() {
	const (
		levelsPerPage int = 100
		npages        int = 5
	)

	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	d := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	p.pages, p.nvalues, p.values, p.defLevels, p.repLevels = makePages(p.dataPageVersion, d, npages, levelsPerPage, reflect.TypeOf(int32(0)), parquet.Encodings.Plain)
	p.initReader(d)

	vresult := make([]int32, levelsPerPage/2)
	dresult := make([]int16, levelsPerPage/2)
	rresult := make([]int16, levelsPerPage/2)

	rdr := p.reader.(*file.Int32ColumnChunkReader)

	p.Run("skip_size > page_size", func() {
		// Skip first 2 pages
		skipped, _ := rdr.Skip(int64(2 * levelsPerPage))
		p.Equal(int64(2*levelsPerPage), skipped)

		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(2*levelsPerPage, int(2.5*float64(levelsPerPage))).Interface().([]int32)
		p.Equal(subVals, vresult)
	})

	p.Run("skip_size == page_size", func() {
		// skip across two pages
		skipped, _ := rdr.Skip(int64(levelsPerPage))
		p.Equal(int64(levelsPerPage), skipped)
		// read half a page
		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(int(3.5*float64(levelsPerPage)), 4*levelsPerPage).Interface().([]int32)
		p.Equal(subVals, vresult)
	})

	p.Run("skip_size < page_size", func() {
		// skip limited to a single page
		// Skip half a page
		skipped, _ := rdr.Skip(int64(levelsPerPage / 2))
		p.Equal(int64(0.5*float32(levelsPerPage)), skipped)
		// Read half a page
		rdr.ReadBatch(int64(levelsPerPage/2), vresult, dresult, rresult)
		subVals := p.values.Slice(int(4.5*float64(levelsPerPage)), p.values.Len()).Interface().([]int32)
		p.Equal(subVals, vresult)
	})
}

func (p *PrimitiveReaderSuite) TestRepetitionLvlBytesWithMaxRepZero() {
	const batchSize = 4
	p.maxDefLvl = 1
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Optional, -1)
	descr := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	// Bytes here came from the example parquet file in ARROW-17453's int32
	// column which was delta bit-packed. The key part is the first three
	// bytes: the page header reports 1 byte for repetition levels even
	// though the max rep level is 0. If that byte isn't skipped then
	// we get def levels of [1, 1, 0, 0] instead of the correct [1, 1, 1, 0].
	pageData := [...]byte{0x3, 0x3, 0x7, 0x80, 0x1, 0x4, 0x3,
		0x18, 0x1, 0x2, 0x0, 0x0, 0x0, 0xc,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

	p.pages = append(p.pages, file.NewDataPageV2(memory.NewBufferBytes(pageData[:]), batchSize, 1, batchSize,
		parquet.Encodings.DeltaBinaryPacked, 2, 1, int32(len(pageData)), false))

	p.initReader(descr)
	p.NotPanics(func() { p.reader.HasNext() })

	var (
		values  [4]int32
		defLvls [4]int16
	)
	i32Rdr := p.reader.(*file.Int32ColumnChunkReader)
	total, read, err := i32Rdr.ReadBatch(batchSize, values[:], defLvls[:], nil)
	p.NoError(err)
	p.EqualValues(batchSize, total)
	p.EqualValues(3, read)
	p.Equal([]int16{1, 1, 1, 0}, defLvls[:])
	p.Equal([]int32{12, 11, 13, 0}, values[:])
}

func (p *PrimitiveReaderSuite) TestDictionaryEncodedPages() {
	p.maxDefLvl = 0
	p.maxRepLvl = 0
	typ := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
	descr := schema.NewColumn(typ, p.maxDefLvl, p.maxRepLvl)
	dummy := memory.NewResizableBuffer(mem)

	p.Run("Dict: Plain, Data: RLEDict", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.Plain)
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.RLEDict, dummy, nil, nil, 0, 0)

		p.pages = append(p.pages, dictPage, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.reader.HasNext() })
		p.NoError(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Dict: Plain Dictionary, Data: Plain Dictionary", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.PlainDict)
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.PlainDict, dummy, nil, nil, 0, 0)
		p.pages = append(p.pages, dictPage, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.reader.HasNext() })
		p.NoError(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Panic if dict page not first", func() {
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.RLEDict, dummy, nil, nil, 0, 0)
		p.pages = append(p.pages, dataPage)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Only RLE is supported", func() {
		dictPage := file.NewDictionaryPage(dummy, 0, parquet.Encodings.DeltaByteArray)
		p.pages = append(p.pages, dictPage)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Cannot have more than one dict", func() {
		dictPage1 := file.NewDictionaryPage(dummy, 0, parquet.Encodings.PlainDict)
		dictPage2 := file.NewDictionaryPage(dummy, 0, parquet.Encodings.Plain)
		p.pages = append(p.pages, dictPage1, dictPage2)
		p.initReader(descr)
		p.NotPanics(func() { p.False(p.reader.HasNext()) })
		p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.Run("Unsupported encoding", func() {
		dataPage := testutils.MakeDataPage(p.dataPageVersion, descr, nil, 0, parquet.Encodings.DeltaByteArray, dummy, nil, nil, 0, 0)
		p.pages = append(p.pages, dataPage)
		p.initReader(descr)
		p.Panics(func() { p.reader.HasNext() })
		// p.Error(p.reader.Err())
		p.pages = p.pages[:0]
	})

	p.pages = p.pages[:2]
}

func TestPrimitiveReader(t *testing.T) {
	t.Parallel()
	t.Run("datapage v1", func(t *testing.T) {
		suite.Run(t, new(PrimitiveReaderSuite))
	})
	t.Run("datapage v2", func(t *testing.T) {
		suite.Run(t, &PrimitiveReaderSuite{dataPageVersion: parquet.DataPageV2})
	})
}

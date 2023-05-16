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

//go:build go1.18

package pqarrow_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func (ps *ParquetIOTestSuite) TestSingleColumnOptionalDictionaryWrite() {
	for _, dt := range fullTypeList {
		// skip tests for bool as we don't do dictionaries for it
		if dt.ID() == arrow.BOOL {
			continue
		}

		ps.Run(dt.Name(), func() {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(ps.T(), 0)

			bldr := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: dt})
			defer bldr.Release()

			values := testutils.RandomNullable(dt, smallSize, 10)
			defer values.Release()
			ps.Require().NoError(bldr.AppendArray(values))

			arr := bldr.NewDictionaryArray()
			defer arr.Release()

			sc := ps.makeSimpleSchema(arr.DataType(), parquet.Repetitions.Optional)
			data := ps.writeColumn(mem, sc, arr)
			ps.readAndCheckSingleColumnFile(mem, data, values)
		})
	}
}

func TestPqarrowDictionaries(t *testing.T) {
	suite.Run(t, &ArrowWriteDictionarySuite{dataPageVersion: parquet.DataPageV1})
	suite.Run(t, &ArrowWriteDictionarySuite{dataPageVersion: parquet.DataPageV2})
	testSuite := &ArrowReadDictSuite{}
	for _, np := range testSuite.NullProbabilities() {
		testSuite.nullProb = np
		t.Run(fmt.Sprintf("nullprob=%.2f", np), func(t *testing.T) {
			suite.Run(t, testSuite)
		})
	}
}

type ArrowWriteDictionarySuite struct {
	suite.Suite

	dataPageVersion parquet.DataPageVersion
}

func (ad *ArrowWriteDictionarySuite) fromJSON(mem memory.Allocator, dt arrow.DataType, data string) arrow.Array {
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(data))
	ad.Require().NoError(err)
	return arr
}

func (ad *ArrowWriteDictionarySuite) TestStatisticsWithFallback() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ad.T(), 0)

	testDictionaries := []arrow.Array{
		ad.fromJSON(mem, arrow.BinaryTypes.String, `["b", "c", "d", "a", "b", "c", "d", "a"]`),
		ad.fromJSON(mem, arrow.BinaryTypes.String, `["b", "c", "d", "a", "b", "c", "d", "a"]`),
		ad.fromJSON(mem, arrow.BinaryTypes.Binary, `["ZA==", "Yw==", "Yg==", "YQ==", "ZA==", "Yw==", "Yg==", "YQ=="]`),
		ad.fromJSON(mem, arrow.BinaryTypes.LargeString, `["a", "b", "c", "a", "b", "c"]`),
	}

	testIndices := []arrow.Array{
		// ["b", null, "a", "b", null, "a"]
		ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[0, null, 3, 0, null, 3]`),
		// ["b", "c", null, "b", "c", null]
		ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[0, 1, null, 0, 1, null]`),
		// ["ZA==", "Yw==", "YQ==", "ZA==", "Yw==", "YQ=="]
		ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[0, 1, 3, 0, 1, 3]`),
		ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[null, null, null, null, null, null]`),
	}

	defer func() {
		for _, d := range testDictionaries {
			d.Release()
		}
		for _, i := range testIndices {
			i.Release()
		}
	}()

	// arrays will be written with 3 values per row group, 2 values per data page
	// the row groups are identical for ease of testing
	expectedValidCounts := []int32{2, 2, 3, 0}
	expectedNullCounts := []int32{1, 1, 0, 3}
	expectedNumDataPages := []int{2, 2, 2, 1}
	expectedValidByPage := [][]int32{
		{1, 1},
		{2, 0},
		{2, 1},
		{0}}
	expectedNullByPage := [][]int64{
		{1, 0},
		{0, 1},
		{0, 0},
		{3}}
	expectedDictCounts := []int32{4, 4, 4, 3}
	// pairs of (min, max)
	expectedMinMax := [][2]string{
		{"a", "b"},
		{"b", "c"},
		{"a", "d"},
		{"", ""}}

	expectedMinByPage := [][][]string{
		{{"b", "a"}, {"b", "a"}},
		{{"b", "b"}, {"b", "b"}},
		{{"c", "a"}, {"c", "a"}}}
	expectedMaxByPage := [][][]string{
		{{"b", "a"}, {"b", "a"}},
		{{"c", "c"}, {"c", "c"}},
		{{"d", "a"}, {"d", "a"}}}
	expectedHasMinMaxByPage := [][][]bool{
		{{true, true}, {true, true}},
		// second page of each rowgroup only contains a null,
		// so there's no stat on that page
		{{true, false}, {true, false}},
		{{true, true}, {true, true}},
		{{false}, {false}}}

	for caseIndex, dict := range testDictionaries {
		ad.Run(dict.DataType().String(), func() {
			dictType := &arrow.DictionaryType{
				IndexType: testIndices[caseIndex].DataType(),
				ValueType: dict.DataType(),
			}
			dictEncoded := array.NewDictionaryArray(dictType, testIndices[caseIndex], dict)
			defer dictEncoded.Release()
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "values", Type: dictEncoded.DataType(), Nullable: true}}, nil)
			col := arrow.NewColumnFromArr(schema.Field(0), dictEncoded)
			defer col.Release()
			tbl := array.NewTable(schema, []arrow.Column{col}, int64(dictEncoded.Len()))
			defer tbl.Release()

			writerProperties := parquet.NewWriterProperties(
				parquet.WithMaxRowGroupLength(3),
				parquet.WithDataPageVersion(ad.dataPageVersion),
				parquet.WithBatchSize(2),
				parquet.WithDictionaryDefault(true),
				parquet.WithDataPageSize(2),
				parquet.WithStats(true),
			)

			var buf bytes.Buffer
			ad.Require().NoError(pqarrow.WriteTable(tbl, &buf, math.MaxInt64, writerProperties,
				pqarrow.DefaultWriterProps()))

			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			ad.Require().NoError(err)
			defer rdr.Close()

			metadata := rdr.MetaData()
			ad.Len(metadata.RowGroups, 2)

			for i := 0; i < rdr.NumRowGroups(); i++ {
				rg := metadata.RowGroup(i)
				ad.EqualValues(1, rg.NumColumns())
				col, err := rg.ColumnChunk(0)
				ad.Require().NoError(err)
				stats, err := col.Statistics()
				ad.Require().NoError(err)

				ad.EqualValues(expectedValidCounts[caseIndex], stats.NumValues())
				ad.EqualValues(expectedNullCounts[caseIndex], stats.NullCount())

				caseExpectedMinMax := expectedMinMax[caseIndex]
				ad.Equal(caseExpectedMinMax[0], string(stats.EncodeMin()))
				ad.Equal(caseExpectedMinMax[1], string(stats.EncodeMax()))
			}

			for rowGroup := 0; rowGroup < 2; rowGroup++ {
				pr, err := rdr.RowGroup(0).GetColumnPageReader(0)
				ad.Require().NoError(err)
				ad.True(pr.Next())
				page := pr.Page()
				ad.NotNil(page)
				ad.NoError(pr.Err())
				ad.Require().IsType((*file.DictionaryPage)(nil), page)
				dictPage := page.(*file.DictionaryPage)
				ad.EqualValues(expectedDictCounts[caseIndex], dictPage.NumValues())

				for pageIdx := 0; pageIdx < expectedNumDataPages[caseIndex]; pageIdx++ {
					ad.True(pr.Next())
					page = pr.Page()
					ad.NotNil(page)
					ad.NoError(pr.Err())

					dataPage, ok := page.(file.DataPage)
					ad.Require().True(ok)
					stats := dataPage.Statistics()
					ad.EqualValues(expectedNullByPage[caseIndex][pageIdx], stats.NullCount)

					expectHasMinMax := expectedHasMinMaxByPage[caseIndex][rowGroup][pageIdx]
					ad.Equal(expectHasMinMax, stats.HasMin)
					ad.Equal(expectHasMinMax, stats.HasMax)

					if expectHasMinMax {
						ad.Equal(expectedMinByPage[caseIndex][rowGroup][pageIdx], string(stats.Min))
						ad.Equal(expectedMaxByPage[caseIndex][rowGroup][pageIdx], string(stats.Max))
					}

					ad.EqualValues(expectedValidByPage[caseIndex][pageIdx]+int32(expectedNullByPage[caseIndex][pageIdx]),
						dataPage.NumValues())
				}

				ad.False(pr.Next())
			}
		})
	}
}

func (ad *ArrowWriteDictionarySuite) TestStatisticsUnifiedDictionary() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ad.T(), 0)

	// two chunks with a shared dictionary
	var (
		tbl      arrow.Table
		dictType = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String}
		schema = arrow.NewSchema([]arrow.Field{
			{Name: "values", Type: dictType, Nullable: true}}, nil)
	)

	{
		// it's important there are no duplicate values in the dictionary,
		// otherwise we trigger the WriteDense() code path which side-steps
		// dictionary encoding.
		testDictionary := ad.fromJSON(mem, arrow.BinaryTypes.String, `["b", "c", "d", "a"]`)
		defer testDictionary.Release()

		testIndices := []arrow.Array{
			// ["a", null, "a", "a", null, "a"]
			ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[3, null, 3, 3, null, 3]`),
			// ["b", "a", null, "b", null, "c"]
			ad.fromJSON(mem, arrow.PrimitiveTypes.Int32, `[0, 3, null, 0, null, 1]`),
		}
		chunks := []arrow.Array{
			array.NewDictionaryArray(dictType, testIndices[0], testDictionary),
			array.NewDictionaryArray(dictType, testIndices[1], testDictionary),
		}
		testIndices[0].Release()
		testIndices[1].Release()

		tbl = array.NewTableFromSlice(schema, [][]arrow.Array{chunks})
		defer tbl.Release()

		chunks[0].Release()
		chunks[1].Release()
	}

	var buf bytes.Buffer
	{
		// write data as two row groups, one with 9 rows and one with 3
		props := parquet.NewWriterProperties(
			parquet.WithMaxRowGroupLength(9),
			parquet.WithDataPageVersion(ad.dataPageVersion),
			parquet.WithBatchSize(3),
			parquet.WithDataPageSize(3),
			parquet.WithDictionaryDefault(true),
			parquet.WithStats(true))

		ad.Require().NoError(pqarrow.WriteTable(tbl, &buf, math.MaxInt64, props, pqarrow.DefaultWriterProps()))
	}

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	ad.Require().NoError(err)
	defer rdr.Close()

	metadata := rdr.MetaData()
	ad.Len(metadata.RowGroups, 2)
	ad.EqualValues(9, metadata.RowGroup(0).NumRows())
	ad.EqualValues(3, metadata.RowGroup(1).NumRows())

	col0, err := metadata.RowGroup(0).ColumnChunk(0)
	ad.Require().NoError(err)
	col1, err := metadata.RowGroup(1).ColumnChunk(0)
	ad.Require().NoError(err)

	stats0, err := col0.Statistics()
	ad.Require().NoError(err)
	stats1, err := col1.Statistics()
	ad.Require().NoError(err)

	ad.EqualValues(6, stats0.NumValues())
	ad.EqualValues(2, stats1.NumValues())
	ad.EqualValues(3, stats0.NullCount())
	ad.EqualValues(1, stats1.NullCount())
	ad.Equal([]byte("a"), stats0.EncodeMin())
	ad.Equal([]byte("b"), stats1.EncodeMin())
	ad.Equal([]byte("b"), stats0.EncodeMax())
	ad.Equal([]byte("c"), stats1.EncodeMax())
}

const numRowGroups = 16

type ArrowReadDictSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator

	denseVals     arrow.Array
	expectedDense arrow.Table
	props         pqarrow.ArrowReadProperties
	nullProb      float64

	buf bytes.Buffer

	options struct {
		numRows      int
		numRowGroups int
		numUniques   int
	}
}

func (ar *ArrowReadDictSuite) generateData(nullProb float64) {
	const minLen = 2
	const maxLen = 100
	rag := testutils.NewRandomArrayGenerator(0)

	ar.denseVals = rag.StringWithRepeats(ar.mem, int64(ar.options.numRows),
		int64(ar.options.numUniques), minLen, maxLen, nullProb)

	chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{ar.denseVals})
	defer chunked.Release()
	ar.expectedDense = makeSimpleTable(chunked, true)
}

func (ar *ArrowReadDictSuite) SetupTest() {
	ar.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	ar.buf.Reset()

	ar.options = struct {
		numRows      int
		numRowGroups int
		numUniques   int
	}{1024 * numRowGroups, numRowGroups, 128}

	ar.props = pqarrow.ArrowReadProperties{}
	ar.generateData(ar.nullProb)
}

func (ar *ArrowReadDictSuite) TearDownTest() {
	if ar.denseVals != nil {
		ar.denseVals.Release()
	}
	ar.expectedDense.Release()

	ar.mem.AssertSize(ar.T(), 0)
}

func (ar *ArrowReadDictSuite) writeSimple() {
	// write num_row_groups row groups; each row group will have a
	// different dictionary
	ar.Require().NoError(pqarrow.WriteTable(ar.expectedDense, &ar.buf, int64(ar.options.numRows/ar.options.numRowGroups),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(true), parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
}

func (ArrowReadDictSuite) NullProbabilities() []float64 {
	return []float64{0.0, 0.5, 1}
}

func (ar *ArrowReadDictSuite) checkReadWholeFile(expected arrow.Table) {
	tbl, err := pqarrow.ReadTable(context.Background(),
		bytes.NewReader(ar.buf.Bytes()), nil, ar.props, ar.mem)
	ar.Require().NoError(err)
	defer tbl.Release()

	ar.Truef(array.TableEqual(expected, tbl), "expected: %s\ngot: %s", expected, tbl)
}

func (ar *ArrowReadDictSuite) checkStreamReadWholeFile(expected arrow.Table) {
	reader, err := file.NewParquetReader(bytes.NewReader(ar.buf.Bytes()))
	ar.Require().NoError(err)
	defer reader.Close()

	rdr, err := pqarrow.NewFileReader(reader, ar.props, ar.mem)
	ar.Require().NoError(err)

	rrdr, err := rdr.GetRecordReader(context.Background(), nil, nil)
	ar.Require().NoError(err)
	defer rrdr.Release()

	recs := make([]arrow.Record, 0)
	for rrdr.Next() {
		rec := rrdr.Record()
		rec.Retain()
		defer rec.Release()
		recs = append(recs, rec)
	}

	tbl := array.NewTableFromRecords(rrdr.Schema(), recs)
	defer tbl.Release()

	ar.Truef(array.TableEqual(expected, tbl), "expected: %s\ngot: %s", expected, tbl)
}

func (ar *ArrowReadDictSuite) getReader() *pqarrow.FileReader {
	reader, err := file.NewParquetReader(bytes.NewReader(ar.buf.Bytes()))
	ar.Require().NoError(err)

	rdr, err := pqarrow.NewFileReader(reader, ar.props, ar.mem)
	ar.Require().NoError(err)
	return rdr
}

func asDict32Encoded(mem memory.Allocator, arr arrow.Array) arrow.Array {
	bldr := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String})
	defer bldr.Release()
	bldr.AppendArray(arr)
	return bldr.NewArray()
}

func (ar *ArrowReadDictSuite) TestReadWholeFileDict() {
	ar.props.SetReadDict(0, true)
	ar.writeSimple()

	numRowGroups := ar.options.numRowGroups
	chunkSize := ar.options.numRows / ar.options.numRowGroups

	chunks := make([]arrow.Array, numRowGroups)
	for i := 0; i < numRowGroups; i++ {
		start := int64(chunkSize * i)
		sl := array.NewSlice(ar.denseVals, start, start+int64(chunkSize))
		defer sl.Release()
		chunks[i] = asDict32Encoded(ar.mem, sl)
		defer chunks[i].Release()
	}

	chunked := arrow.NewChunked(chunks[0].DataType(), chunks)
	defer chunked.Release()

	exTable := makeSimpleTable(chunked, true)
	defer exTable.Release()

	ar.checkReadWholeFile(exTable)
}

func (ar *ArrowReadDictSuite) TestZeroChunksListOfDictionary() {
	ar.props.SetReadDict(0, true)
	ar.denseVals.Release()
	ar.denseVals = nil

	values := arrow.NewChunked(arrow.ListOf(arrow.BinaryTypes.String), []arrow.Array{})
	defer values.Release()

	ar.options.numRowGroups = 1
	ar.options.numRows = 0
	ar.options.numUniques = 0
	ar.expectedDense.Release()
	ar.expectedDense = makeSimpleTable(values, false)

	ar.writeSimple()

	rdr := ar.getReader()
	defer rdr.ParquetReader().Close()

	colReader, err := rdr.GetColumn(context.Background(), 0)
	ar.Require().NoError(err)
	defer colReader.Release()

	chnked, err := colReader.NextBatch(1 << 15)
	ar.Require().NoError(err)
	defer chnked.Release()
	ar.Zero(chnked.Len())
	ar.Len(chnked.Chunks(), 1)
}

func (ar *ArrowReadDictSuite) TestIncrementalReads() {
	ar.options.numRows = 100
	ar.options.numUniques = 10

	ar.denseVals.Release()
	ar.expectedDense.Release()
	ar.generateData(ar.nullProb)

	ar.props.SetReadDict(0, true)
	// just write a single row group
	ar.Require().NoError(pqarrow.WriteTable(ar.expectedDense, &ar.buf, int64(ar.options.numRows),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(true), parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))

	// read in one shot
	expected, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(ar.buf.Bytes()), nil, ar.props, ar.mem)
	ar.Require().NoError(err)
	defer expected.Release()

	rdr := ar.getReader()
	defer rdr.ParquetReader().Close()
	col, err := rdr.GetColumn(context.Background(), 0)
	ar.Require().NoError(err)
	defer col.Release()

	const numReads = 4
	batchSize := ar.options.numRows / numReads

	ctx := compute.WithAllocator(context.Background(), ar.mem)

	for i := 0; i < numReads; i++ {
		chunk, err := col.NextBatch(int64(batchSize))
		ar.Require().NoError(err)
		// no need to manually release chunk, like other record readers
		// the col reader holds onto the current record and will release it
		// when the next is requested or when the reader is released
		resultDense, err := compute.CastArray(ctx, chunk.Chunk(0),
			compute.SafeCastOptions(arrow.BinaryTypes.String))
		ar.Require().NoError(err)
		defer resultDense.Release()

		sl := array.NewSlice(ar.denseVals, int64(i*batchSize), int64((i*batchSize)+batchSize))
		defer sl.Release()

		ar.Truef(array.Equal(sl, resultDense), "expected: %s\ngot: %s", sl, resultDense)
	}
}

func (ar *ArrowReadDictSuite) TestStreamReadWholeFileDict() {
	ar.options.numRows = 100
	ar.options.numUniques = 10

	ar.denseVals.Release()
	ar.expectedDense.Release()
	ar.generateData(ar.nullProb)

	ar.writeSimple()
	ar.props.BatchSize = int64(ar.options.numRows * 2)
	ar.checkStreamReadWholeFile(ar.expectedDense)
}

func (ar *ArrowReadDictSuite) TestReadWholeFileDense() {
	ar.props.SetReadDict(0, false)
	ar.writeSimple()
	ar.checkReadWholeFile(ar.expectedDense)
}

func doRoundTrip(t *testing.T, tbl arrow.Table, rowGroupSize int64, wrProps *parquet.WriterProperties, arrWrProps *pqarrow.ArrowWriterProperties, arrReadProps pqarrow.ArrowReadProperties) arrow.Table {
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, rowGroupSize, wrProps, *arrWrProps))

	out, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()), nil, arrReadProps, wrProps.Allocator())
	require.NoError(t, err)
	return out
}

func TestArrowWriteChangingDictionaries(t *testing.T) {
	const (
		numUnique            = 50
		repeat               = 5000
		minLen, maxLen int32 = 2, 20
	)

	rag := testutils.NewRandomArrayGenerator(0)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := rag.StringWithRepeats(mem, repeat*numUnique, numUnique, minLen, maxLen, 0.1)
	defer values.Release()

	valuesChunk := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	defer valuesChunk.Release()

	expected := makeSimpleTable(valuesChunk, true)
	defer expected.Release()

	const numChunks = 10
	chunks := make([]arrow.Array, numChunks)
	chunkSize := valuesChunk.Len() / numChunks
	for i := 0; i < numChunks; i++ {
		start := int64(chunkSize * i)
		sl := array.NewSlice(values, start, start+int64(chunkSize))
		defer sl.Release()
		chunks[i] = asDict32Encoded(mem, sl)
		defer chunks[i].Release()
	}

	dictChunked := arrow.NewChunked(chunks[0].DataType(), chunks)
	defer dictChunked.Release()
	dictTable := makeSimpleTable(dictChunked, true)
	defer dictTable.Release()

	props := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
	actual := doRoundTrip(t, dictTable, int64(values.Len())/2, parquet.NewWriterProperties(parquet.WithAllocator(mem)),
		&props, pqarrow.ArrowReadProperties{})
	defer actual.Release()

	assert.Truef(t, array.TableEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func TestArrowAutoReadAsDictionary(t *testing.T) {
	const (
		numUnique            = 50
		repeat               = 100
		minLen, maxLen int32 = 2, 20
	)

	rag := testutils.NewRandomArrayGenerator(0)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	values := rag.StringWithRepeats(mem, repeat*numUnique, numUnique, minLen, maxLen, 0.1)
	defer values.Release()

	dictValues := asDict32Encoded(mem, values)
	defer dictValues.Release()

	dictChunk := arrow.NewChunked(dictValues.DataType(), []arrow.Array{dictValues})
	defer dictChunk.Release()

	valuesChunk := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	defer valuesChunk.Release()

	expected := makeSimpleTable(dictChunk, true)
	defer expected.Release()
	expectedDense := makeSimpleTable(valuesChunk, true)
	defer expectedDense.Release()

	wrProps := parquet.NewWriterProperties(parquet.WithAllocator(mem), parquet.WithDictionaryDefault(true))
	propsStoreSchema := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	actual := doRoundTrip(t, expected, int64(valuesChunk.Len()), wrProps, &propsStoreSchema, pqarrow.ArrowReadProperties{})
	defer actual.Release()

	assert.Truef(t, array.TableEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)

	propsNoStoreSchema := pqarrow.NewArrowWriterProperties()
	actualDense := doRoundTrip(t, expected, int64(valuesChunk.Len()), wrProps, &propsNoStoreSchema, pqarrow.ArrowReadProperties{})
	defer actualDense.Release()

	assert.Truef(t, array.TableEqual(expectedDense, actualDense), "expected: %s\ngot: %s", expectedDense, actualDense)
}

func TestArrowWriteNestedSubfieldDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	offsets, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0, 2, 3]`))
	defer offsets.Release()
	indices, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0, 0]`))
	defer indices.Release()
	dict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["foo"]`))
	defer dict.Release()

	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	dictValues := array.NewDictionaryArray(dictType, indices, dict)
	defer dictValues.Release()

	data := array.NewData(arrow.ListOf(dictType), 3, []*memory.Buffer{nil, offsets.Data().Buffers()[1]},
		[]arrow.ArrayData{dictValues.Data()}, 0, 0)
	defer data.Release()
	values := array.NewListData(data)
	defer values.Release()

	chk := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	defer chk.Release()

	tbl := makeSimpleTable(chk, true)
	defer tbl.Release()
	propsStoreSchema := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	actual := doRoundTrip(t, tbl, int64(values.Len()), parquet.NewWriterProperties(), &propsStoreSchema, pqarrow.ArrowReadProperties{})
	defer actual.Release()

	assert.Truef(t, array.TableEqual(tbl, actual), "expected: %s\ngot: %s", tbl, actual)
}

func TestDictOfEmptyStringsRoundtrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "reserved1", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()

	for i := 0; i < 6; i++ {
		bldr.AppendEmptyValue()
	}

	arr := bldr.NewArray()
	defer arr.Release()
	col1 := arrow.NewColumnFromArr(schema.Field(0), arr)
	defer col1.Release()
	tbl := array.NewTable(schema, []arrow.Column{col1}, 6)
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 6,
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(true)),
		pqarrow.NewArrowWriterProperties()))

	result, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()), nil, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	defer result.Release()

	assert.EqualValues(t, 6, result.NumRows())
	assert.EqualValues(t, 1, result.NumCols())
	col := result.Column(0).Data().Chunk(0)
	assert.Equal(t, arrow.STRING, col.DataType().ID())

	for i := 0; i < 6; i++ {
		assert.Zero(t, col.(*array.String).Value(i))
	}
}

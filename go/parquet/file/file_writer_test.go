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
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/internal/encoding"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SerializeTestSuite struct {
	testutils.PrimitiveTypedTest
	suite.Suite

	numCols      int
	numRowGroups int
	rowsPerRG    int
	rowsPerBatch int
}

func (t *SerializeTestSuite) SetupTest() {
	t.numCols = 4
	t.numRowGroups = 4
	t.rowsPerRG = 50
	t.rowsPerBatch = 10
	t.SetupSchema(parquet.Repetitions.Optional, t.numCols)
}

func (t *SerializeTestSuite) fileSerializeTest(codec compress.Compression, expected compress.Compression) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < t.numCols; i++ {
		opts = append(opts, parquet.WithCompressionFor(t.Schema.Column(i).Name(), codec))
	}

	props := parquet.NewWriterProperties(opts...)

	writer := file.NewParquetWriter(sink, t.Schema.Root(), file.WithWriterProps(props))
	t.GenerateData(int64(t.rowsPerRG))
	for rg := 0; rg < t.numRowGroups/2; rg++ {
		rgw := writer.AppendRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := rgw.NextColumn()
			t.WriteBatchValues(cw, t.DefLevels, nil)
			cw.Close()
			// ensure column() api which is specific to bufferedrowgroups cannot be called
			t.Panics(func() { rgw.(file.BufferedRowGroupWriter).Column(col) })
		}
		rgw.Close()
	}

	// write half buffered row groups
	for rg := 0; rg < t.numRowGroups/2; rg++ {
		rgw := writer.AppendBufferedRowGroup()
		for batch := 0; batch < (t.rowsPerRG / t.rowsPerBatch); batch++ {
			for col := 0; col < t.numCols; col++ {
				cw, _ := rgw.Column(col)
				offset := batch * t.rowsPerBatch
				t.WriteBatchSubset(t.rowsPerBatch, offset, cw, t.DefLevels[offset:t.rowsPerBatch+offset], nil)
				// Ensure NextColumn api which is specific to RowGroup cannot be called
				t.Panics(func() { rgw.(file.SerialRowGroupWriter).NextColumn() })
			}
		}
		for col := 0; col < t.numCols; col++ {
			cw, _ := rgw.Column(col)
			cw.Close()
		}
		rgw.Close()
	}
	writer.Close()

	nrows := t.numRowGroups * t.rowsPerRG
	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	t.NoError(err)
	t.Equal(t.numCols, reader.MetaData().Schema.NumColumns())
	t.Equal(t.numRowGroups, reader.NumRowGroups())
	t.EqualValues(nrows, reader.NumRows())

	for rg := 0; rg < t.numRowGroups; rg++ {
		rgr := reader.RowGroup(rg)
		t.Equal(t.numCols, rgr.NumColumns())
		t.EqualValues(t.rowsPerRG, rgr.NumRows())
		chunk, _ := rgr.MetaData().ColumnChunk(0)
		t.Equal(expected, chunk.Compression())

		valuesRead := int64(0)

		for i := 0; i < t.numCols; i++ {
			chunk, _ := rgr.MetaData().ColumnChunk(i)
			t.False(chunk.HasIndexPage())
			t.DefLevelsOut = make([]int16, t.rowsPerRG)
			t.RepLevelsOut = make([]int16, t.rowsPerRG)
			colReader, err := rgr.Column(i)
			t.NoError(err)
			t.SetupValuesOut(int64(t.rowsPerRG))
			valuesRead = t.ReadBatch(colReader, int64(t.rowsPerRG), 0, t.DefLevelsOut, t.RepLevelsOut)
			t.EqualValues(t.rowsPerRG, valuesRead)
			t.Equal(t.Values, t.ValuesOut)
			t.Equal(t.DefLevels, t.DefLevelsOut)
		}
	}
}

func (t *SerializeTestSuite) unequalNumRows(maxRows int64, rowsPerCol []int64) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	props := parquet.NewWriterProperties()
	writer := file.NewParquetWriter(sink, t.Schema.Root(), file.WithWriterProps(props))
	defer writer.Close()

	rgw := writer.AppendRowGroup()
	t.GenerateData(maxRows)
	for col := 0; col < t.numCols; col++ {
		cw, _ := rgw.NextColumn()
		t.WriteBatchSubset(int(rowsPerCol[col]), 0, cw, t.DefLevels[:rowsPerCol[col]], nil)
		cw.Close()
	}
	t.Error(rgw.Close())
}

func (t *SerializeTestSuite) unequalNumRowsBuffered(maxRows int64, rowsPerCol []int64) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())
	defer writer.Close()

	rgw := writer.AppendBufferedRowGroup()
	t.GenerateData(maxRows)
	for col := 0; col < t.numCols; col++ {
		cw, _ := rgw.Column(col)
		t.WriteBatchSubset(int(rowsPerCol[col]), 0, cw, t.DefLevels[:rowsPerCol[col]], nil)
		cw.Close()
	}
	t.Error(rgw.Close())
}

func (t *SerializeTestSuite) TestZeroRows() {
	t.NotPanics(func() {
		sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
		writer := file.NewParquetWriter(sink, t.Schema.Root())
		defer writer.Close()

		srgw := writer.AppendRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := srgw.NextColumn()
			cw.Close()
		}
		srgw.Close()

		brgw := writer.AppendBufferedRowGroup()
		for col := 0; col < t.numCols; col++ {
			cw, _ := brgw.Column(col)
			cw.Close()
		}
		brgw.Close()
	})
}

func (t *SerializeTestSuite) TestTooManyColumns() {
	t.SetupSchema(parquet.Repetitions.Optional, 1)
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())
	rgw := writer.AppendRowGroup()

	rgw.NextColumn()                      // first column
	t.Panics(func() { rgw.NextColumn() }) // only one column!
}

func (t *SerializeTestSuite) TestRepeatedTooFewRows() {
	// optional and repeated, so definition and repetition levels
	t.SetupSchema(parquet.Repetitions.Repeated, 1)
	const nrows = 100
	t.GenerateData(nrows)

	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	writer := file.NewParquetWriter(sink, t.Schema.Root())

	rgw := writer.AppendRowGroup()
	t.RepLevels = make([]int16, nrows)
	for idx := range t.RepLevels {
		t.RepLevels[idx] = 0
	}

	cw, _ := rgw.NextColumn()
	t.WriteBatchValues(cw, t.DefLevels, t.RepLevels)
	cw.Close()

	t.RepLevels[3] = 1 // this makes it so that values 2 and 3 are a single row
	// as a result there's one too few rows in the result

	t.Panics(func() {
		cw, _ = rgw.NextColumn()
		t.WriteBatchValues(cw, t.DefLevels, t.RepLevels)
		cw.Close()
	})
}

func (t *SerializeTestSuite) TestTooFewRows() {
	rowsPerCol := []int64{100, 100, 100, 99}
	t.NotPanics(func() { t.unequalNumRows(100, rowsPerCol) })
	t.NotPanics(func() { t.unequalNumRowsBuffered(100, rowsPerCol) })
}

func (t *SerializeTestSuite) TestTooManyRows() {
	rowsPerCol := []int64{100, 100, 100, 101}
	t.NotPanics(func() { t.unequalNumRows(101, rowsPerCol) })
	t.NotPanics(func() { t.unequalNumRowsBuffered(101, rowsPerCol) })
}

func (t *SerializeTestSuite) TestSmallFile() {
	codecs := []compress.Compression{
		compress.Codecs.Uncompressed,
		compress.Codecs.Snappy,
		compress.Codecs.Brotli,
		compress.Codecs.Gzip,
		compress.Codecs.Zstd,
		// compress.Codecs.Lz4,
		// compress.Codecs.Lzo,
	}
	for _, c := range codecs {
		t.Run(c.String(), func() {
			t.NotPanics(func() { t.fileSerializeTest(c, c) })
		})
	}
}

func TestBufferedDisabledDictionary(t *testing.T) {
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)
	fields := schema.FieldList{schema.NewInt32Node("col", parquet.Repetitions.Required, 1)}
	sc, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, 0)
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))

	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))
	rgw := writer.AppendBufferedRowGroup()
	cwr, _ := rgw.Column(0)
	cw := cwr.(*file.Int32ColumnChunkWriter)
	cw.WriteBatch([]int32{1}, nil, nil)
	rgw.Close()
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()
	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, reader.NumRowGroups())
	rgReader := reader.RowGroup(0)
	assert.EqualValues(t, 1, rgReader.NumRows())
	chunk, _ := rgReader.MetaData().ColumnChunk(0)
	assert.False(t, chunk.HasDictionaryPage())
}

func TestBufferedMultiPageDisabledDictionary(t *testing.T) {
	const (
		valueCount = 10000
		pageSize   = 16384
	)
	var (
		sink  = encoding.NewBufferWriter(0, memory.DefaultAllocator)
		props = parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(pageSize))
		sc, _ = schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
			schema.NewInt32Node("col", parquet.Repetitions.Required, -1),
		}, -1)
	)

	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(props))
	rgWriter := writer.AppendBufferedRowGroup()
	cwr, _ := rgWriter.Column(0)
	cw := cwr.(*file.Int32ColumnChunkWriter)
	valuesIn := make([]int32, 0, valueCount)
	for i := int32(0); i < valueCount; i++ {
		valuesIn = append(valuesIn, (i%100)+1)
	}
	cw.WriteBatch(valuesIn, nil, nil)
	rgWriter.Close()
	writer.Close()
	buffer := sink.Finish()
	defer buffer.Release()

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, reader.NumRowGroups())
	valuesOut := make([]int32, valueCount)

	for r := 0; r < reader.NumRowGroups(); r++ {
		rgr := reader.RowGroup(r)
		assert.EqualValues(t, 1, rgr.NumColumns())
		assert.EqualValues(t, valueCount, rgr.NumRows())

		var totalRead int64
		col, err := rgr.Column(0)
		assert.NoError(t, err)
		colReader := col.(*file.Int32ColumnChunkReader)
		for colReader.HasNext() {
			total, _, _ := colReader.ReadBatch(valueCount-totalRead, valuesOut[totalRead:], nil, nil)
			totalRead += total
		}
		assert.EqualValues(t, valueCount, totalRead)
		assert.Equal(t, valuesIn, valuesOut)
	}
}

func TestAllNulls(t *testing.T) {
	sc, _ := schema.NewGroupNode("root", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("nulls", parquet.Repetitions.Optional, -1),
	}, -1)
	sink := encoding.NewBufferWriter(0, memory.DefaultAllocator)

	writer := file.NewParquetWriter(sink, sc)
	rgw := writer.AppendRowGroup()
	cwr, _ := rgw.NextColumn()
	cw := cwr.(*file.Int32ColumnChunkWriter)

	var (
		values    [3]int32
		defLevels = [...]int16{0, 0, 0}
	)

	cw.WriteBatch(values[:], defLevels[:], nil)
	cw.Close()
	rgw.Close()
	writer.Close()

	buffer := sink.Finish()
	defer buffer.Release()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.BufferedStreamEnabled = true

	reader, err := file.NewParquetReader(bytes.NewReader(buffer.Bytes()), file.WithReadProps(props))
	assert.NoError(t, err)

	rgr := reader.RowGroup(0)
	col, err := rgr.Column(0)
	assert.NoError(t, err)
	cr := col.(*file.Int32ColumnChunkReader)

	defLevels[0] = -1
	defLevels[1] = -1
	defLevels[2] = -1
	valRead, read, _ := cr.ReadBatch(3, values[:], defLevels[:], nil)
	assert.EqualValues(t, 3, valRead)
	assert.EqualValues(t, 0, read)
	assert.Equal(t, []int16{0, 0, 0}, defLevels[:])
}

func createSerializeTestSuite(typ reflect.Type) suite.TestingSuite {
	return &SerializeTestSuite{PrimitiveTypedTest: testutils.NewPrimitiveTypedTest(typ)}
}

func TestSerialize(t *testing.T) {
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
	}
	for _, tt := range types {
		tt := tt
		t.Run(tt.typ.String(), func(t *testing.T) {
			t.Parallel()
			suite.Run(t, createSerializeTestSuite(tt.typ))
		})
	}
}

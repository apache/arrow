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

package pqarrow_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/types"
	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/internal/encoding"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func makeSimpleTable(values *arrow.Chunked, nullable bool) arrow.Table {
	sc := arrow.NewSchema([]arrow.Field{{Name: "col", Type: values.DataType(), Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"-1"})}}, nil)
	column := arrow.NewColumn(sc.Field(0), values)
	defer column.Release()
	return array.NewTable(sc, []arrow.Column{*column}, -1)
}

func makeDateTimeTypesTable(mem memory.Allocator, expected bool, addFieldMeta bool) arrow.Table {
	isValid := []bool{true, true, true, false, true, true}

	// roundtrip without modification
	f0 := arrow.Field{Name: "f0", Type: arrow.FixedWidthTypes.Date32, Nullable: true}
	f1 := arrow.Field{Name: "f1", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true}
	f2 := arrow.Field{Name: "f2", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true}
	f3 := arrow.Field{Name: "f3", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true}
	f3X := arrow.Field{Name: "f3", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true}
	f4 := arrow.Field{Name: "f4", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true}
	f5 := arrow.Field{Name: "f5", Type: arrow.FixedWidthTypes.Time64us, Nullable: true}
	f6 := arrow.Field{Name: "f6", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true}

	fieldList := []arrow.Field{f0, f1, f2}
	if expected {
		fieldList = append(fieldList, f3X)
	} else {
		fieldList = append(fieldList, f3)
	}
	fieldList = append(fieldList, f4, f5, f6)

	if addFieldMeta {
		for idx := range fieldList {
			fieldList[idx].Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(idx + 1)})
		}
	}
	arrsc := arrow.NewSchema(fieldList, nil)

	d32Values := []arrow.Date32{1489269000, 1489270000, 1489271000, 1489272000, 1489272000, 1489273000}
	ts64nsValues := []arrow.Timestamp{1489269000000, 1489270000000, 1489271000000, 1489272000000, 1489272000000, 1489273000000}
	ts64usValues := []arrow.Timestamp{1489269000, 1489270000, 1489271000, 1489272000, 1489272000, 1489273000}
	ts64msValues := []arrow.Timestamp{1489269, 1489270, 1489271, 1489272, 1489272, 1489273}
	t32Values := []arrow.Time32{1489269000, 1489270000, 1489271000, 1489272000, 1489272000, 1489273000}
	t64nsValues := []arrow.Time64{1489269000000, 1489270000000, 1489271000000, 1489272000000, 1489272000000, 1489273000000}
	t64usValues := []arrow.Time64{1489269000, 1489270000, 1489271000, 1489272000, 1489272000, 1489273000}

	builders := make([]array.Builder, 0, len(fieldList))
	for _, f := range fieldList {
		bldr := array.NewBuilder(mem, f.Type)
		defer bldr.Release()
		builders = append(builders, bldr)
	}

	builders[0].(*array.Date32Builder).AppendValues(d32Values, isValid)
	builders[1].(*array.TimestampBuilder).AppendValues(ts64msValues, isValid)
	builders[2].(*array.TimestampBuilder).AppendValues(ts64usValues, isValid)
	if expected {
		builders[3].(*array.TimestampBuilder).AppendValues(ts64usValues, isValid)
	} else {
		builders[3].(*array.TimestampBuilder).AppendValues(ts64nsValues, isValid)
	}
	builders[4].(*array.Time32Builder).AppendValues(t32Values, isValid)
	builders[5].(*array.Time64Builder).AppendValues(t64usValues, isValid)
	builders[6].(*array.Time64Builder).AppendValues(t64nsValues, isValid)

	cols := make([]arrow.Column, 0, len(fieldList))
	for idx, field := range fieldList {
		arr := builders[idx].NewArray()
		defer arr.Release()

		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		defer chunked.Release()
		col := arrow.NewColumn(field, chunked)
		defer col.Release()
		cols = append(cols, *col)
	}

	return array.NewTable(arrsc, cols, int64(len(isValid)))
}

func TestWriteArrowCols(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, false, false)
	defer tbl.Release()

	psc, err := pqarrow.ToParquet(tbl.Schema(), nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	require.NoError(t, err)

	manifest, err := pqarrow.NewSchemaManifest(psc, nil, nil)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()
	writer := file.NewParquetWriter(sink, psc.Root(), file.WithWriterProps(parquet.NewWriterProperties(parquet.WithVersion(parquet.V2_4))))

	srgw := writer.AppendRowGroup()
	ctx := pqarrow.NewArrowWriteContext(context.TODO(), nil)

	for i := int64(0); i < tbl.NumCols(); i++ {
		acw, err := pqarrow.NewArrowColumnWriter(tbl.Column(int(i)).Data(), 0, tbl.NumRows(), manifest, srgw, int(i))
		require.NoError(t, err)
		require.NoError(t, acw.Write(ctx))
	}
	require.NoError(t, srgw.Close())
	require.NoError(t, writer.Close())

	expected := makeDateTimeTypesTable(mem, true, false)
	defer expected.Release()

	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)

	assert.EqualValues(t, expected.NumCols(), reader.MetaData().Schema.NumColumns())
	assert.EqualValues(t, expected.NumRows(), reader.NumRows())
	assert.EqualValues(t, 1, reader.NumRowGroups())

	rgr := reader.RowGroup(0)

	for i := 0; i < int(expected.NumCols()); i++ {
		var (
			total        int64
			read         int
			defLevelsOut = make([]int16, int(expected.NumRows()))
			arr          = expected.Column(i).Data().Chunk(0)
		)
		switch expected.Schema().Field(i).Type.(arrow.FixedWidthDataType).BitWidth() {
		case 32:
			col, err := rgr.Column(i)
			assert.NoError(t, err)
			colReader := col.(*file.Int32ColumnChunkReader)
			vals := make([]int32, int(expected.NumRows()))
			total, read, err = colReader.ReadBatch(expected.NumRows(), vals, defLevelsOut, nil)
			require.NoError(t, err)

			nulls := 0
			for j := 0; j < arr.Len(); j++ {
				if arr.IsNull(j) {
					nulls++
					continue
				}

				switch v := arr.(type) {
				case *array.Date32:
					assert.EqualValues(t, v.Value(j), vals[j-nulls])
				case *array.Time32:
					assert.EqualValues(t, v.Value(j), vals[j-nulls])
				}
			}
		case 64:
			col, err := rgr.Column(i)
			assert.NoError(t, err)
			colReader := col.(*file.Int64ColumnChunkReader)
			vals := make([]int64, int(expected.NumRows()))
			total, read, err = colReader.ReadBatch(expected.NumRows(), vals, defLevelsOut, nil)
			require.NoError(t, err)

			nulls := 0
			for j := 0; j < arr.Len(); j++ {
				if arr.IsNull(j) {
					nulls++
					continue
				}

				switch v := arr.(type) {
				case *array.Date64:
					assert.EqualValues(t, v.Value(j), vals[j-nulls])
				case *array.Time64:
					assert.EqualValues(t, v.Value(j), vals[j-nulls])
				case *array.Timestamp:
					assert.EqualValues(t, v.Value(j), vals[j-nulls])
				}
			}
		}
		assert.EqualValues(t, expected.NumRows(), total)
		assert.EqualValues(t, expected.NumRows()-1, read)
		assert.Equal(t, []int16{1, 1, 1, 0, 1, 1}, defLevelsOut)
	}
}

func TestWriteArrowInt96(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tbl := makeDateTimeTypesTable(mem, false, false)
	defer tbl.Release()

	props := pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true), pqarrow.WithAllocator(mem))

	psc, err := pqarrow.ToParquet(tbl.Schema(), nil, props)
	require.NoError(t, err)

	manifest, err := pqarrow.NewSchemaManifest(psc, nil, nil)
	require.NoError(t, err)

	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()

	writer := file.NewParquetWriter(sink, psc.Root(), file.WithWriterProps(parquet.NewWriterProperties(parquet.WithAllocator(mem))))

	srgw := writer.AppendRowGroup()
	ctx := pqarrow.NewArrowWriteContext(context.TODO(), &props)

	for i := int64(0); i < tbl.NumCols(); i++ {
		acw, err := pqarrow.NewArrowColumnWriter(tbl.Column(int(i)).Data(), 0, tbl.NumRows(), manifest, srgw, int(i))
		require.NoError(t, err)
		require.NoError(t, acw.Write(ctx))
	}
	require.NoError(t, srgw.Close())
	require.NoError(t, writer.Close())

	expected := makeDateTimeTypesTable(mem, false, false)
	defer expected.Release()

	reader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	require.NoError(t, err)

	assert.EqualValues(t, expected.NumCols(), reader.MetaData().Schema.NumColumns())
	assert.EqualValues(t, expected.NumRows(), reader.NumRows())
	assert.EqualValues(t, 1, reader.NumRowGroups())

	rgr := reader.RowGroup(0)
	tsRdr, err := rgr.Column(3)
	assert.NoError(t, err)
	assert.Equal(t, parquet.Types.Int96, tsRdr.Type())

	rdr := tsRdr.(*file.Int96ColumnChunkReader)
	vals := make([]parquet.Int96, expected.NumRows())
	defLevels := make([]int16, int(expected.NumRows()))

	total, read, _ := rdr.ReadBatch(expected.NumRows(), vals, defLevels, nil)
	assert.EqualValues(t, expected.NumRows(), total)
	assert.EqualValues(t, expected.NumRows()-1, read)
	assert.Equal(t, []int16{1, 1, 1, 0, 1, 1}, defLevels)

	data := expected.Column(3).Data().Chunk(0).(*array.Timestamp)
	assert.EqualValues(t, data.Value(0), vals[0].ToTime().UnixNano())
	assert.EqualValues(t, data.Value(1), vals[1].ToTime().UnixNano())
	assert.EqualValues(t, data.Value(2), vals[2].ToTime().UnixNano())
	assert.EqualValues(t, data.Value(4), vals[3].ToTime().UnixNano())
	assert.EqualValues(t, data.Value(5), vals[4].ToTime().UnixNano())
}

func writeTableToBuffer(t *testing.T, mem memory.Allocator, tbl arrow.Table, rowGroupSize int64, props pqarrow.ArrowWriterProperties) *memory.Buffer {
	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()
	wrprops := parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0))
	psc, err := pqarrow.ToParquet(tbl.Schema(), wrprops, props)
	require.NoError(t, err)

	manifest, err := pqarrow.NewSchemaManifest(psc, nil, nil)
	require.NoError(t, err)

	writer := file.NewParquetWriter(sink, psc.Root(), file.WithWriterProps(wrprops))
	ctx := pqarrow.NewArrowWriteContext(context.TODO(), &props)

	offset := int64(0)
	for offset < tbl.NumRows() {
		sz := utils.Min(rowGroupSize, tbl.NumRows()-offset)
		srgw := writer.AppendRowGroup()
		for i := 0; i < int(tbl.NumCols()); i++ {
			col := tbl.Column(i)
			acw, err := pqarrow.NewArrowColumnWriter(col.Data(), offset, sz, manifest, srgw, i)
			require.NoError(t, err)
			require.NoError(t, acw.Write(ctx))
		}
		srgw.Close()
		offset += sz
	}
	writer.Close()

	return sink.Finish()
}

func simpleRoundTrip(t *testing.T, tbl arrow.Table, rowGroupSize int64) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	buf := writeTableToBuffer(t, mem, tbl, rowGroupSize, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	defer buf.Release()

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	ardr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	for i := 0; i < int(tbl.NumCols()); i++ {
		crdr, err := ardr.GetColumn(context.TODO(), i)
		require.NoError(t, err)

		chunked, err := crdr.NextBatch(tbl.NumRows())
		require.NoError(t, err)

		require.EqualValues(t, tbl.NumRows(), chunked.Len())

		chunkList := tbl.Column(i).Data().Chunks()
		offset := int64(0)
		for _, chnk := range chunkList {
			slc := array.NewChunkedSlice(chunked, offset, offset+int64(chnk.Len()))
			defer slc.Release()

			assert.EqualValues(t, chnk.Len(), slc.Len())
			if len(slc.Chunks()) == 1 {
				offset += int64(chnk.Len())
				assert.True(t, array.Equal(chnk, slc.Chunk(0)))
			}
		}
		crdr.Release()
	}
}

func TestWriteEmptyLists(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.ListOf(arrow.FixedWidthTypes.Date32)},
		{Name: "f2", Type: arrow.ListOf(arrow.FixedWidthTypes.Date64)},
		{Name: "f3", Type: arrow.ListOf(arrow.FixedWidthTypes.Timestamp_us)},
		{Name: "f4", Type: arrow.ListOf(arrow.FixedWidthTypes.Timestamp_ms)},
		{Name: "f5", Type: arrow.ListOf(arrow.FixedWidthTypes.Time32ms)},
		{Name: "f6", Type: arrow.ListOf(arrow.FixedWidthTypes.Time64ns)},
		{Name: "f7", Type: arrow.ListOf(arrow.FixedWidthTypes.Time64us)},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()
	for _, b := range bldr.Fields() {
		b.AppendNull()
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V1_0),
	)
	arrprops := pqarrow.DefaultWriterProps()
	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(sc, &buf, props, arrprops)
	require.NoError(t, err)
	err = fw.Write(rec)
	require.NoError(t, err)
	err = fw.Close()
	require.NoError(t, err)
}

func TestArrowReadWriteTableChunkedCols(t *testing.T) {
	chunkSizes := []int{2, 4, 10, 2}
	const totalLen = int64(18)

	rng := testutils.NewRandomArrayGenerator(0)

	arr := rng.Int32(totalLen, 0, math.MaxInt32/2, 0.9)
	defer arr.Release()

	offset := int64(0)
	chunks := make([]arrow.Array, 0)
	for _, chnksize := range chunkSizes {
		chk := array.NewSlice(arr, offset, offset+int64(chnksize))
		defer chk.Release()
		defer chk.Release() // for NewChunked below
		chunks = append(chunks, chk)
	}

	sc := arrow.NewSchema([]arrow.Field{{Name: "field", Type: arr.DataType(), Nullable: true}}, nil)

	chk := arrow.NewChunked(arr.DataType(), chunks)
	defer chk.Release()

	tbl := array.NewTable(sc, []arrow.Column{*arrow.NewColumn(sc.Field(0), chk)}, -1)
	defer tbl.Release()

	simpleRoundTrip(t, tbl, 2)
	simpleRoundTrip(t, tbl, 10)
}

// set this up for checking our expected results so we can test the functions
// that generate them which we export
func getLogicalType(typ arrow.DataType) schema.LogicalType {
	switch typ.ID() {
	case arrow.DICTIONARY:
		return getLogicalType(typ.(*arrow.DictionaryType).ValueType)
	case arrow.INT8:
		return schema.NewIntLogicalType(8, true)
	case arrow.UINT8:
		return schema.NewIntLogicalType(8, false)
	case arrow.INT16:
		return schema.NewIntLogicalType(16, true)
	case arrow.UINT16:
		return schema.NewIntLogicalType(16, false)
	case arrow.INT32:
		return schema.NewIntLogicalType(32, true)
	case arrow.UINT32:
		return schema.NewIntLogicalType(32, false)
	case arrow.INT64:
		return schema.NewIntLogicalType(64, true)
	case arrow.UINT64:
		return schema.NewIntLogicalType(64, false)
	case arrow.STRING, arrow.LARGE_STRING:
		return schema.StringLogicalType{}
	case arrow.DATE32:
		return schema.DateLogicalType{}
	case arrow.DATE64:
		return schema.DateLogicalType{}
	case arrow.TIMESTAMP:
		ts := typ.(*arrow.TimestampType)
		adjustedUTC := len(ts.TimeZone) == 0
		switch ts.Unit {
		case arrow.Microsecond:
			return schema.NewTimestampLogicalType(adjustedUTC, schema.TimeUnitMicros)
		case arrow.Millisecond:
			return schema.NewTimestampLogicalType(adjustedUTC, schema.TimeUnitMillis)
		case arrow.Nanosecond:
			return schema.NewTimestampLogicalType(adjustedUTC, schema.TimeUnitNanos)
		default:
			panic("only milli, micro and nano units supported for arrow timestamp")
		}
	case arrow.TIME32:
		return schema.NewTimeLogicalType(false, schema.TimeUnitMillis)
	case arrow.TIME64:
		ts := typ.(*arrow.Time64Type)
		switch ts.Unit {
		case arrow.Microsecond:
			return schema.NewTimeLogicalType(false, schema.TimeUnitMicros)
		case arrow.Nanosecond:
			return schema.NewTimeLogicalType(false, schema.TimeUnitNanos)
		default:
			panic("only micro and nano seconds are supported for arrow TIME64")
		}
	case arrow.DECIMAL:
		dec := typ.(*arrow.Decimal128Type)
		return schema.NewDecimalLogicalType(dec.Precision, dec.Scale)
	}
	return schema.NoLogicalType{}
}

func getPhysicalType(typ arrow.DataType) parquet.Type {
	switch typ.ID() {
	case arrow.DICTIONARY:
		return getPhysicalType(typ.(*arrow.DictionaryType).ValueType)
	case arrow.BOOL:
		return parquet.Types.Boolean
	case arrow.UINT8, arrow.INT8, arrow.UINT16, arrow.INT16, arrow.UINT32, arrow.INT32:
		return parquet.Types.Int32
	case arrow.INT64, arrow.UINT64:
		return parquet.Types.Int64
	case arrow.FLOAT32:
		return parquet.Types.Float
	case arrow.FLOAT64:
		return parquet.Types.Double
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.STRING, arrow.LARGE_STRING:
		return parquet.Types.ByteArray
	case arrow.FIXED_SIZE_BINARY, arrow.DECIMAL:
		return parquet.Types.FixedLenByteArray
	case arrow.DATE32:
		return parquet.Types.Int32
	case arrow.DATE64:
		// convert to date32 internally
		return parquet.Types.Int32
	case arrow.TIME32:
		return parquet.Types.Int32
	case arrow.TIME64, arrow.TIMESTAMP:
		return parquet.Types.Int64
	default:
		return parquet.Types.Int32
	}
}

const (
	boolTestValue = true
	uint8TestVal  = uint8(64)
	int8TestVal   = int8(-64)
	uint16TestVal = uint16(1024)
	int16TestVal  = int16(-1024)
	uint32TestVal = uint32(1024)
	int32TestVal  = int32(-1024)
	uint64TestVal = uint64(1024)
	int64TestVal  = int64(-1024)
	tsTestValue   = arrow.Timestamp(14695634030000)
	date32TestVal = arrow.Date32(170000)
	floatTestVal  = float32(2.1)
	doubleTestVal = float64(4.2)
	strTestVal    = "Test"

	smallSize = 100
)

type ParquetIOTestSuite struct {
	suite.Suite
}

func (ps *ParquetIOTestSuite) SetupTest() {
	ps.NoError(arrow.RegisterExtensionType(types.NewUUIDType()))
}

func (ps *ParquetIOTestSuite) TearDownTest() {
	if arrow.GetExtensionType("uuid") != nil {
		ps.NoError(arrow.UnregisterExtensionType("uuid"))
	}
}

func (ps *ParquetIOTestSuite) makeSimpleSchema(typ arrow.DataType, rep parquet.Repetition) *schema.GroupNode {
	byteWidth := int32(-1)

	switch typ := typ.(type) {
	case *arrow.FixedSizeBinaryType:
		byteWidth = int32(typ.ByteWidth)
	case *arrow.Decimal128Type:
		byteWidth = pqarrow.DecimalSize(typ.Precision)
	case *arrow.DictionaryType:
		valuesType := typ.ValueType
		switch dt := valuesType.(type) {
		case *arrow.FixedSizeBinaryType:
			byteWidth = int32(dt.ByteWidth)
		case *arrow.Decimal128Type:
			byteWidth = pqarrow.DecimalSize(dt.Precision)
		}
	}

	pnode, _ := schema.NewPrimitiveNodeLogical("column1", rep, getLogicalType(typ), getPhysicalType(typ), int(byteWidth), -1)
	return schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{pnode}, -1))
}

func (ps *ParquetIOTestSuite) makePrimitiveTestCol(mem memory.Allocator, size int, typ arrow.DataType) arrow.Array {
	switch typ.ID() {
	case arrow.BOOL:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(boolTestValue)
		}
		return bldr.NewArray()
	case arrow.INT8:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(int8TestVal)
		}
		return bldr.NewArray()
	case arrow.UINT8:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(uint8TestVal)
		}
		return bldr.NewArray()
	case arrow.INT16:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(int16TestVal)
		}
		return bldr.NewArray()
	case arrow.UINT16:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(uint16TestVal)
		}
		return bldr.NewArray()
	case arrow.INT32:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(int32TestVal)
		}
		return bldr.NewArray()
	case arrow.UINT32:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(uint32TestVal)
		}
		return bldr.NewArray()
	case arrow.INT64:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(int64TestVal)
		}
		return bldr.NewArray()
	case arrow.UINT64:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(uint64TestVal)
		}
		return bldr.NewArray()
	case arrow.FLOAT32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(floatTestVal)
		}
		return bldr.NewArray()
	case arrow.FLOAT64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append(doubleTestVal)
		}
		return bldr.NewArray()
	}
	return nil
}

func (ps *ParquetIOTestSuite) makeTestFile(mem memory.Allocator, typ arrow.DataType, arr arrow.Array, numChunks int) []byte {
	sc := ps.makeSimpleSchema(typ, parquet.Repetitions.Required)
	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()
	writer := file.NewParquetWriter(sink, sc, file.WithWriterProps(parquet.NewWriterProperties(parquet.WithAllocator(mem))))

	props := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
	ctx := pqarrow.NewArrowWriteContext(context.TODO(), &props)
	rowGroupSize := arr.Len() / numChunks

	for i := 0; i < numChunks; i++ {
		rgw := writer.AppendRowGroup()
		cw, err := rgw.NextColumn()
		ps.NoError(err)

		start := i * rowGroupSize
		slc := array.NewSlice(arr, int64(start), int64(start+rowGroupSize))
		defer slc.Release()
		ps.NoError(pqarrow.WriteArrowToColumn(ctx, cw, slc, nil, nil, false))
		ps.NoError(cw.Close())
		ps.NoError(rgw.Close())
	}
	ps.NoError(writer.Close())
	buf := sink.Finish()
	defer buf.Release()
	return buf.Bytes()
}

func (ps *ParquetIOTestSuite) createReader(mem memory.Allocator, data []byte) *pqarrow.FileReader {
	rdr, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
	ps.NoError(err)

	reader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	ps.NoError(err)
	return reader
}

func (ps *ParquetIOTestSuite) readTable(rdr *pqarrow.FileReader) arrow.Table {
	tbl, err := rdr.ReadTable(context.TODO())
	ps.NoError(err)
	ps.NotNil(tbl)
	return tbl
}

func (ps *ParquetIOTestSuite) checkSingleColumnRequiredTableRead(mem memory.Allocator, typ arrow.DataType, numChunks int) {
	values := ps.makePrimitiveTestCol(mem, smallSize, typ)
	defer values.Release()

	data := ps.makeTestFile(mem, typ, values, numChunks)
	reader := ps.createReader(mem, data)

	tbl := ps.readTable(reader)
	defer tbl.Release()

	ps.EqualValues(1, tbl.NumCols())
	ps.EqualValues(smallSize, tbl.NumRows())

	chunked := tbl.Column(0).Data()
	ps.Len(chunked.Chunks(), 1)
	ps.True(array.Equal(values, chunked.Chunk(0)))
}

func (ps *ParquetIOTestSuite) checkSingleColumnRead(mem memory.Allocator, typ arrow.DataType, numChunks int) {
	values := ps.makePrimitiveTestCol(mem, smallSize, typ)
	defer values.Release()

	data := ps.makeTestFile(mem, typ, values, numChunks)
	reader := ps.createReader(mem, data)

	cr, err := reader.GetColumn(context.TODO(), 0)
	ps.NoError(err)
	defer cr.Release()

	chunked, err := cr.NextBatch(smallSize)
	ps.NoError(err)

	ps.Len(chunked.Chunks(), 1)
	ps.True(array.Equal(values, chunked.Chunk(0)))
}

func (ps *ParquetIOTestSuite) TestDateTimeTypesReadWriteTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	toWrite := makeDateTimeTypesTable(mem, false, true)
	defer toWrite.Release()
	buf := writeTableToBuffer(ps.T(), mem, toWrite, toWrite.NumRows(), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	defer buf.Release()

	reader := ps.createReader(mem, buf.Bytes())
	tbl := ps.readTable(reader)
	defer tbl.Release()

	expected := makeDateTimeTypesTable(mem, true, true)
	defer expected.Release()

	ps.Equal(expected.NumCols(), tbl.NumCols())
	ps.Equal(expected.NumRows(), tbl.NumRows())
	ps.Truef(expected.Schema().Equal(tbl.Schema()), "expected schema: %s\ngot schema: %s", expected.Schema(), tbl.Schema())

	for i := 0; i < int(expected.NumCols()); i++ {
		exChunk := expected.Column(i).Data()
		tblChunk := tbl.Column(i).Data()

		ps.Equal(len(exChunk.Chunks()), len(tblChunk.Chunks()))
		ps.Truef(array.Equal(exChunk.Chunk(0), tblChunk.Chunk(0)), "expected %s\ngot %s", exChunk.Chunk(0), tblChunk.Chunk(0))
	}
}

func (ps *ParquetIOTestSuite) TestDateTimeTypesWithInt96ReadWriteTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	expected := makeDateTimeTypesTable(mem, false, true)
	defer expected.Release()
	buf := writeTableToBuffer(ps.T(), mem, expected, expected.NumRows(), pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true)))
	defer buf.Release()

	reader := ps.createReader(mem, buf.Bytes())
	tbl := ps.readTable(reader)
	defer tbl.Release()

	ps.Equal(expected.NumCols(), tbl.NumCols())
	ps.Equal(expected.NumRows(), tbl.NumRows())
	ps.Truef(expected.Schema().Equal(tbl.Schema()), "expected schema: %s\ngot schema: %s", expected.Schema(), tbl.Schema())

	for i := 0; i < int(expected.NumCols()); i++ {
		exChunk := expected.Column(i).Data()
		tblChunk := tbl.Column(i).Data()

		ps.Equal(len(exChunk.Chunks()), len(tblChunk.Chunks()))
		ps.Truef(array.Equal(exChunk.Chunk(0), tblChunk.Chunk(0)), "expected %s\ngot %s", exChunk.Chunk(0), tblChunk.Chunk(0))
	}
}

func (ps *ParquetIOTestSuite) TestLargeBinaryReadWriteTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	// While we may write using LargeString, when we read, we get an array.String back out.
	// So we're building a normal array.String to use with array.Equal
	lsBldr := array.NewLargeStringBuilder(mem)
	defer lsBldr.Release()
	lbBldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer lbBldr.Release()

	for i := 0; i < smallSize; i++ {
		s := strconv.FormatInt(int64(i), 10)
		lsBldr.Append(s)
		lbBldr.Append([]byte(s))
	}

	lsValues := lsBldr.NewArray()
	defer lsValues.Release()
	lbValues := lbBldr.NewArray()
	defer lbValues.Release()

	lsField := arrow.Field{Name: "large_string", Type: arrow.BinaryTypes.LargeString, Nullable: true}
	lbField := arrow.Field{Name: "large_binary", Type: arrow.BinaryTypes.LargeBinary, Nullable: true}
	expected := array.NewTable(
		arrow.NewSchema([]arrow.Field{lsField, lbField}, nil),
		[]arrow.Column{
			*arrow.NewColumn(lsField, arrow.NewChunked(lsField.Type, []arrow.Array{lsValues})),
			*arrow.NewColumn(lbField, arrow.NewChunked(lbField.Type, []arrow.Array{lbValues})),
		},
		-1,
	)
	defer lsValues.Release() // NewChunked
	defer lbValues.Release() // NewChunked
	defer expected.Release()
	ps.roundTripTable(mem, expected, true)
}

func (ps *ParquetIOTestSuite) TestReadSingleColumnFile() {
	types := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	nchunks := []int{1, 4}

	for _, n := range nchunks {
		for _, dt := range types {
			ps.Run(fmt.Sprintf("%s %d chunks", dt.Name(), n), func() {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(ps.T(), 0)
				ps.checkSingleColumnRead(mem, dt, n)
			})
		}
	}
}

func (ps *ParquetIOTestSuite) TestSingleColumnRequiredRead() {
	types := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}

	nchunks := []int{1, 4}

	for _, n := range nchunks {
		for _, dt := range types {
			ps.Run(fmt.Sprintf("%s %d chunks", dt.Name(), n), func() {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(ps.T(), 0)

				ps.checkSingleColumnRequiredTableRead(mem, dt, n)
			})
		}
	}
}

func (ps *ParquetIOTestSuite) TestReadDecimals() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bigEndian := []parquet.ByteArray{
		// 123456
		[]byte{1, 226, 64},
		// 987654
		[]byte{15, 18, 6},
		// -123456
		[]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 29, 192},
	}

	bldr := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 6, Scale: 3})
	defer bldr.Release()

	bldr.Append(decimal128.FromU64(123456))
	bldr.Append(decimal128.FromU64(987654))
	bldr.Append(decimal128.FromI64(-123456))

	expected := bldr.NewDecimal128Array()
	defer expected.Release()

	sc := schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNodeLogical("decimals", parquet.Repetitions.Required, schema.NewDecimalLogicalType(6, 3), parquet.Types.ByteArray, -1, -1)),
	}, -1))

	sink := encoding.NewBufferWriter(0, mem)
	defer sink.Release()
	writer := file.NewParquetWriter(sink, sc)

	rgw := writer.AppendRowGroup()
	cw, _ := rgw.NextColumn()
	cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(bigEndian, nil, nil)
	cw.Close()
	rgw.Close()
	writer.Close()

	rdr := ps.createReader(mem, sink.Bytes())
	cr, err := rdr.GetColumn(context.TODO(), 0)
	ps.NoError(err)

	chunked, err := cr.NextBatch(smallSize)
	ps.NoError(err)
	defer chunked.Release()

	ps.Len(chunked.Chunks(), 1)
	ps.True(array.Equal(expected, chunked.Chunk(0)))
}

func (ps *ParquetIOTestSuite) TestReadNestedStruct() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	dt := arrow.StructOf(arrow.Field{
		Name: "nested",
		Type: arrow.StructOf(
			arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			arrow.Field{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		),
	})
	field := arrow.Field{Name: "struct", Type: dt, Nullable: true}

	builder := array.NewStructBuilder(mem, dt)
	defer builder.Release()
	nested := builder.FieldBuilder(0).(*array.StructBuilder)

	builder.Append(true)
	nested.Append(true)
	nested.FieldBuilder(0).(*array.BooleanBuilder).Append(true)
	nested.FieldBuilder(1).(*array.Int32Builder).Append(int32(-1))
	nested.FieldBuilder(2).(*array.Int64Builder).Append(int64(-2))
	builder.AppendNull()

	arr := builder.NewStructArray()
	defer arr.Release()

	expected := array.NewTable(
		arrow.NewSchema([]arrow.Field{field}, nil),
		[]arrow.Column{*arrow.NewColumn(field, arrow.NewChunked(dt, []arrow.Array{arr}))},
		-1,
	)
	defer arr.Release() // NewChunked
	defer expected.Release()
	ps.roundTripTable(mem, expected, true)
}

func (ps *ParquetIOTestSuite) writeColumn(mem memory.Allocator, sc *schema.GroupNode, values arrow.Array) []byte {
	var buf bytes.Buffer
	arrsc, err := pqarrow.FromParquet(schema.NewSchema(sc), nil, nil)
	ps.NoError(err)

	writer, err := pqarrow.NewFileWriter(arrsc, &buf, parquet.NewWriterProperties(parquet.WithDictionaryDefault(false)), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	ps.NoError(err)

	writer.NewRowGroup()
	ps.NoError(writer.WriteColumnData(values))
	//defer values.Release()
	ps.NoError(writer.Close())
	ps.NoError(writer.Close())

	return buf.Bytes()
}

func (ps *ParquetIOTestSuite) readAndCheckSingleColumnFile(mem memory.Allocator, data []byte, values arrow.Array) {
	reader := ps.createReader(mem, data)
	cr, err := reader.GetColumn(context.TODO(), 0)
	ps.NoError(err)
	ps.NotNil(cr)
	defer cr.Release()

	chunked, err := cr.NextBatch(smallSize)
	ps.NoError(err)

	ps.Len(chunked.Chunks(), 1)
	ps.NotNil(chunked.Chunk(0))

	ps.True(array.Equal(values, chunked.Chunk(0)))
}

var fullTypeList = []arrow.DataType{
	arrow.FixedWidthTypes.Boolean,
	arrow.PrimitiveTypes.Uint8,
	arrow.PrimitiveTypes.Int8,
	arrow.PrimitiveTypes.Uint16,
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Uint32,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Int64,
	arrow.FixedWidthTypes.Date32,
	arrow.PrimitiveTypes.Float32,
	arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.Binary,
	&arrow.FixedSizeBinaryType{ByteWidth: 10},
	&arrow.Decimal128Type{Precision: 1, Scale: 0},
	&arrow.Decimal128Type{Precision: 5, Scale: 4},
	&arrow.Decimal128Type{Precision: 10, Scale: 9},
	&arrow.Decimal128Type{Precision: 19, Scale: 18},
	&arrow.Decimal128Type{Precision: 23, Scale: 22},
	&arrow.Decimal128Type{Precision: 27, Scale: 26},
	&arrow.Decimal128Type{Precision: 38, Scale: 37},
}

func (ps *ParquetIOTestSuite) TestSingleColumnRequiredWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(ps.T(), 0)

			values := testutils.RandomNonNull(mem, dt, smallSize)
			defer values.Release()
			sc := ps.makeSimpleSchema(dt, parquet.Repetitions.Required)
			data := ps.writeColumn(mem, sc, values)
			ps.readAndCheckSingleColumnFile(mem, data, values)
		})
	}
}

func (ps *ParquetIOTestSuite) roundTripTable(_ memory.Allocator, expected arrow.Table, storeSchema bool) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator) // FIXME: currently overriding allocator to isolate leaks between roundTripTable and caller
	//defer mem.AssertSize(ps.T(), 0)                            // FIXME: known leak

	var buf bytes.Buffer
	var props pqarrow.ArrowWriterProperties
	if storeSchema {
		props = pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(mem))
	} else {
		props = pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
	}

	writeProps := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	ps.Require().NoError(pqarrow.WriteTable(expected, &buf, expected.NumRows(), writeProps, props))

	reader := ps.createReader(mem, buf.Bytes())
	defer reader.ParquetReader().Close()

	tbl := ps.readTable(reader)
	defer tbl.Release()

	ps.Equal(expected.NumCols(), tbl.NumCols())
	ps.Equal(expected.NumRows(), tbl.NumRows())

	exChunk := expected.Column(0).Data()
	tblChunk := tbl.Column(0).Data()

	ps.Equal(len(exChunk.Chunks()), len(tblChunk.Chunks()))
	exc := exChunk.Chunk(0)
	tbc := tblChunk.Chunk(0)
	ps.Truef(array.ApproxEqual(exc, tbc), "expected: %T %s\ngot: %T %s", exc, exc, tbc, tbc)
}

func makeEmptyListsArray(size int) arrow.Array {
	// allocate an offsets buffer with only zeros
	offsetsNbytes := arrow.Int32Traits.BytesRequired(size + 1)
	offsetsBuffer := make([]byte, offsetsNbytes)

	childBuffers := []*memory.Buffer{nil, nil}
	childData := array.NewData(arrow.PrimitiveTypes.Float32, 0, childBuffers, nil, 0, 0)
	defer childData.Release()
	buffers := []*memory.Buffer{nil, memory.NewBufferBytes(offsetsBuffer)}
	arrayData := array.NewData(arrow.ListOf(childData.DataType()), size, buffers, []arrow.ArrayData{childData}, 0, 0)
	defer arrayData.Release()
	return array.MakeFromData(arrayData)
}

func makeListArray(values arrow.Array, size, nullcount int) arrow.Array {
	nonNullEntries := size - nullcount - 1
	lengthPerEntry := values.Len() / nonNullEntries

	offsets := make([]byte, arrow.Int32Traits.BytesRequired(size+1))
	offsetsArr := arrow.Int32Traits.CastFromBytes(offsets)

	nullBitmap := make([]byte, int(bitutil.BytesForBits(int64(size))))

	curOffset := 0
	for i := 0; i < size; i++ {
		offsetsArr[i] = int32(curOffset)
		if !(((i % 2) == 0) && ((i / 2) < nullcount)) {
			// non-null list (list with index 1 is always empty)
			bitutil.SetBit(nullBitmap, i)
			if i != 1 {
				curOffset += lengthPerEntry
			}
		}
	}
	offsetsArr[size] = int32(values.Len())

	listData := array.NewData(arrow.ListOf(values.DataType()), size,
		[]*memory.Buffer{memory.NewBufferBytes(nullBitmap), memory.NewBufferBytes(offsets)},
		[]arrow.ArrayData{values.Data()}, nullcount, 0)
	defer listData.Release()
	return array.NewListData(listData)
}

func prepareEmptyListsTable(size int) arrow.Table {
	lists := makeEmptyListsArray(size)
	defer lists.Release()
	chunked := arrow.NewChunked(lists.DataType(), []arrow.Array{lists})
	defer chunked.Release()
	return makeSimpleTable(chunked, true)
}

func prepareListTable(dt arrow.DataType, size int, nullableLists bool, nullableElems bool, nullCount int) arrow.Table {
	nc := nullCount
	if !nullableElems {
		nc = 0
	}
	values := testutils.RandomNullable(dt, size*size, nc)
	defer values.Release()
	// also test that slice offsets are respected
	values = array.NewSlice(values, 5, int64(values.Len()))
	defer values.Release()

	if !nullableLists {
		nullCount = 0
	}
	lists := makeListArray(values, size, nullCount)
	defer lists.Release()

	chunked := arrow.NewChunked(lists.DataType(), []arrow.Array{lists})
	defer chunked.Release()

	return makeSimpleTable(array.NewChunkedSlice(chunked, 3, int64(size)), nullableLists)
}

func prepareListOfListTable(dt arrow.DataType, size, nullCount int, nullableParentLists, nullableLists, nullableElems bool) arrow.Table {
	nc := nullCount
	if !nullableElems {
		nc = 0
	}

	values := testutils.RandomNullable(dt, size*6, nc)
	defer values.Release()

	if nullableLists {
		nc = nullCount
	} else {
		nc = 0
	}

	lists := makeListArray(values, size*3, nc)
	defer lists.Release()

	if !nullableParentLists {
		nullCount = 0
	}

	parentLists := makeListArray(lists, size, nullCount)
	defer parentLists.Release()

	chunked := arrow.NewChunked(parentLists.DataType(), []arrow.Array{parentLists})
	defer chunked.Release()

	return makeSimpleTable(chunked, nullableParentLists)
}

func (ps *ParquetIOTestSuite) TestSingleEmptyListsColumnReadWrite() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	//defer mem.AssertSize(ps.T(), 0) // FIXME: known leak

	expected := prepareEmptyListsTable(smallSize)
	defer expected.Release()
	buf := writeTableToBuffer(ps.T(), mem, expected, smallSize, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	defer buf.Release()

	reader := ps.createReader(mem, buf.Bytes())
	tbl := ps.readTable(reader)
	defer tbl.Release()

	ps.EqualValues(expected.NumCols(), tbl.NumCols())
	ps.EqualValues(expected.NumRows(), tbl.NumRows())

	exChunk := expected.Column(0).Data()
	tblChunk := tbl.Column(0).Data()

	ps.Equal(len(exChunk.Chunks()), len(tblChunk.Chunks()))
	ps.True(array.Equal(exChunk.Chunk(0), tblChunk.Chunk(0)))
}

func (ps *ParquetIOTestSuite) TestSingleColumnOptionalReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(ps.T(), 0)

			values := testutils.RandomNullable(dt, smallSize, 10)
			defer values.Release()
			sc := ps.makeSimpleSchema(dt, parquet.Repetitions.Optional)
			data := ps.writeColumn(mem, sc, values)
			ps.readAndCheckSingleColumnFile(mem, data, values)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSingleNullableListNullableColumnReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			expected := prepareListTable(dt, smallSize, true, true, 10)
			defer expected.Release()
			ps.roundTripTable(memory.DefaultAllocator, expected, false)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSingleRequiredListNullableColumnReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			expected := prepareListTable(dt, smallSize, false, true, 10)
			defer expected.Release()
			ps.roundTripTable(memory.DefaultAllocator, expected, false)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSingleNullableListRequiredColumnReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			expected := prepareListTable(dt, smallSize, true, false, 10)
			defer expected.Release()
			ps.roundTripTable(memory.DefaultAllocator, expected, false)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSingleRequiredListRequiredColumnReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			expected := prepareListTable(dt, smallSize, false, false, 0)
			defer expected.Release()
			ps.roundTripTable(memory.DefaultAllocator, expected, false)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSingleNullableListRequiredListRequiredColumnReadWrite() {
	for _, dt := range fullTypeList {
		ps.Run(dt.Name(), func() {
			expected := prepareListOfListTable(dt, smallSize, 2, true, false, false)
			defer expected.Release()
			ps.roundTripTable(memory.DefaultAllocator, expected, false)
		})
	}
}

func (ps *ParquetIOTestSuite) TestSimpleStruct() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	links := arrow.StructOf(arrow.Field{Name: "Backward", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "Forward", Type: arrow.PrimitiveTypes.Int64, Nullable: true})

	bldr := array.NewStructBuilder(mem, links)
	defer bldr.Release()

	backBldr := bldr.FieldBuilder(0).(*array.Int64Builder)
	forwardBldr := bldr.FieldBuilder(1).(*array.Int64Builder)

	bldr.Append(true)
	backBldr.AppendNull()
	forwardBldr.Append(20)

	bldr.Append(true)
	backBldr.Append(10)
	forwardBldr.Append(40)

	data := bldr.NewArray()
	defer data.Release()

	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{{Name: "links", Type: links}}, nil),
		[]arrow.Column{*arrow.NewColumn(arrow.Field{Name: "links", Type: links}, arrow.NewChunked(links, []arrow.Array{data}))}, -1)
	defer data.Release() // NewChunked
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, false)
}

func (ps *ParquetIOTestSuite) TestSingleColumnNullableStruct() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	links := arrow.StructOf(arrow.Field{Name: "Backward", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	bldr := array.NewStructBuilder(mem, links)
	defer bldr.Release()

	backBldr := bldr.FieldBuilder(0).(*array.Int64Builder)

	bldr.AppendNull()
	bldr.Append(true)
	backBldr.Append(10)

	data := bldr.NewArray()
	defer data.Release()

	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{{Name: "links", Type: links, Nullable: true}}, nil),
		[]arrow.Column{*arrow.NewColumn(arrow.Field{Name: "links", Type: links, Nullable: true}, arrow.NewChunked(links, []arrow.Array{data}))}, -1)
	defer data.Release() // NewChunked
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, false)
}

func (ps *ParquetIOTestSuite) TestNestedRequiredFieldStruct() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	intField := arrow.Field{Name: "int_array", Type: arrow.PrimitiveTypes.Int32}
	intBldr := array.NewInt32Builder(mem)
	defer intBldr.Release()
	intBldr.AppendValues([]int32{0, 1, 2, 3, 4, 5, 7, 8}, nil)

	intArr := intBldr.NewArray()
	defer intArr.Release()

	validity := memory.NewBufferBytes([]byte{0xCC})
	defer validity.Release()

	structField := arrow.Field{Name: "root", Type: arrow.StructOf(intField), Nullable: true}
	structData := array.NewData(structField.Type, 8, []*memory.Buffer{validity}, []arrow.ArrayData{intArr.Data()}, 4, 0)
	defer structData.Release()
	stData := array.NewStructData(structData)
	defer stData.Release()

	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{structField}, nil),
		[]arrow.Column{*arrow.NewColumn(structField,
			arrow.NewChunked(structField.Type, []arrow.Array{stData}))}, -1)
	defer stData.Release() // NewChunked
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, false)
}

func (ps *ParquetIOTestSuite) TestNestedNullableField() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	intField := arrow.Field{Name: "int_array", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	intBldr := array.NewInt32Builder(mem)
	defer intBldr.Release()
	intBldr.AppendValues([]int32{0, 1, 2, 3, 4, 5, 7, 8}, []bool{true, false, true, false, true, true, false, true})

	intArr := intBldr.NewArray()
	defer intArr.Release()

	validity := memory.NewBufferBytes([]byte{0xCC})
	defer validity.Release()

	structField := arrow.Field{Name: "root", Type: arrow.StructOf(intField), Nullable: true}
	data := array.NewData(structField.Type, 8, []*memory.Buffer{validity}, []arrow.ArrayData{intArr.Data()}, 4, 0)
	defer data.Release()
	stData := array.NewStructData(data)
	defer stData.Release()

	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{structField}, nil),
		[]arrow.Column{*arrow.NewColumn(structField,
			arrow.NewChunked(structField.Type, []arrow.Array{stData}))}, -1)
	defer stData.Release() // NewChunked
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, false)
}

func (ps *ParquetIOTestSuite) TestNestedEmptyList() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewStructBuilder(mem, arrow.StructOf(
		arrow.Field{
			Name: "root",
			Type: arrow.StructOf(
				arrow.Field{
					Name: "child1",
					Type: arrow.ListOf(arrow.StructOf(
						arrow.Field{
							Name: "child2",
							Type: arrow.ListOf(arrow.StructOf(
								arrow.Field{
									Name: "name",
									Type: arrow.BinaryTypes.String,
								},
							)),
						},
					)),
				},
			),
		},
	))
	defer bldr.Release()

	rootBldr := bldr.FieldBuilder(0).(*array.StructBuilder)
	child1Bldr := rootBldr.FieldBuilder(0).(*array.ListBuilder)
	child1ElBldr := child1Bldr.ValueBuilder().(*array.StructBuilder)
	child2Bldr := child1ElBldr.FieldBuilder(0).(*array.ListBuilder)
	leafBldr := child2Bldr.ValueBuilder().(*array.StructBuilder)
	nameBldr := leafBldr.FieldBuilder(0).(*array.StringBuilder)

	// target structure 8 times
	// {
	//   "root": {
	//     "child1": [
	//       { "child2": [{ "name": "foo" }] },
	//       { "child2": [] }
	//     ]
	//   }
	// }

	for i := 0; i < 8; i++ {
		bldr.Append(true)
		rootBldr.Append(true)
		child1Bldr.Append(true)

		child1ElBldr.Append(true)
		child2Bldr.Append(true)
		leafBldr.Append(true)
		nameBldr.Append("foo")

		child1ElBldr.Append(true)
		child2Bldr.Append(true)
	}

	arr := bldr.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: "x", Type: arr.DataType(), Nullable: true}
	expected := array.NewTableFromSlice(arrow.NewSchema([]arrow.Field{field}, nil), [][]arrow.Array{{arr}})
	defer expected.Release()

	ps.roundTripTable(mem, expected, false)
}

func (ps *ParquetIOTestSuite) TestCanonicalNestedRoundTrip() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	docIdField := arrow.Field{Name: "DocID", Type: arrow.PrimitiveTypes.Int64}
	linksField := arrow.Field{Name: "Links", Type: arrow.StructOf(
		arrow.Field{Name: "Backward", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		arrow.Field{Name: "Forward", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	), Nullable: true}

	nameStruct := arrow.StructOf(
		arrow.Field{Name: "Language", Nullable: true, Type: arrow.ListOf(
			arrow.StructOf(arrow.Field{Name: "Code", Type: arrow.BinaryTypes.String},
				arrow.Field{Name: "Country", Type: arrow.BinaryTypes.String, Nullable: true}))},
		arrow.Field{Name: "Url", Type: arrow.BinaryTypes.String, Nullable: true})

	nameField := arrow.Field{Name: "Name", Type: arrow.ListOf(nameStruct)}
	sc := arrow.NewSchema([]arrow.Field{docIdField, linksField, nameField}, nil)

	docIDArr, _, err := array.FromJSON(mem, docIdField.Type, strings.NewReader("[10, 20]"))
	ps.Require().NoError(err)
	defer docIDArr.Release()

	linksIDArr, _, err := array.FromJSON(mem, linksField.Type, strings.NewReader(`[{"Backward":[], "Forward":[20, 40, 60]}, {"Backward":[10, 30], "Forward": [80]}]`))
	ps.Require().NoError(err)
	defer linksIDArr.Release()

	nameArr, _, err := array.FromJSON(mem, nameField.Type, strings.NewReader(`
			[[{"Language": [{"Code": "en_us", "Country": "us"},
							{"Code": "en_us", "Country": null}],
			   "Url": "http://A"},
			  {"Url": "http://B", "Language": null},
			  {"Language": [{"Code": "en-gb", "Country": "gb"}], "Url": null}],
			  [{"Url": "http://C", "Language": null}]]`))
	ps.Require().NoError(err)
	defer nameArr.Release()

	expected := array.NewTable(sc, []arrow.Column{
		*arrow.NewColumn(docIdField, arrow.NewChunked(docIdField.Type, []arrow.Array{docIDArr})),
		*arrow.NewColumn(linksField, arrow.NewChunked(linksField.Type, []arrow.Array{linksIDArr})),
		*arrow.NewColumn(nameField, arrow.NewChunked(nameField.Type, []arrow.Array{nameArr})),
	}, 2)
	defer docIDArr.Release()   // NewChunked
	defer linksIDArr.Release() // NewChunked
	defer nameArr.Release()    // NewChunked
	defer expected.Release()

	ps.roundTripTable(mem, expected, false)
}

func (ps *ParquetIOTestSuite) TestFixedSizeList() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Int16)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int16Builder)

	bldr.AppendValues([]bool{true, true, true})
	vb.AppendValues([]int16{1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)

	data := bldr.NewArray()
	defer data.Release() // NewArray

	field := arrow.Field{Name: "root", Type: data.DataType(), Nullable: true}
	cnk := arrow.NewChunked(field.Type, []arrow.Array{data})
	defer data.Release() // NewChunked

	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Column{*arrow.NewColumn(field, cnk)}, -1)
	defer cnk.Release() // NewColumn
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, true)
}

func (ps *ParquetIOTestSuite) TestNull() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewNullBuilder(mem)
	defer bldr.Release()

	bldr.AppendNull()
	bldr.AppendNull()
	bldr.AppendNull()

	data := bldr.NewArray()
	defer data.Release()

	field := arrow.Field{Name: "x", Type: data.DataType(), Nullable: true}
	expected := array.NewTable(
		arrow.NewSchema([]arrow.Field{field}, nil),
		[]arrow.Column{*arrow.NewColumn(field, arrow.NewChunked(field.Type, []arrow.Array{data}))},
		-1,
	)

	ps.roundTripTable(mem, expected, true)
}

// ARROW-17169
func (ps *ParquetIOTestSuite) TestNullableListOfStruct() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewListBuilder(mem, arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	))
	defer bldr.Release()

	stBldr := bldr.ValueBuilder().(*array.StructBuilder)
	aBldr := stBldr.FieldBuilder(0).(*array.Int32Builder)
	bBldr := stBldr.FieldBuilder(1).(*array.StringBuilder)

	for i := 0; i < 320; i++ {
		if i%5 == 0 {
			bldr.AppendNull()
			continue
		}
		bldr.Append(true)
		for j := 0; j < 4; j++ {
			stBldr.Append(true)
			aBldr.Append(int32(i + j))
			bBldr.Append(strconv.Itoa(i + j))
		}
	}

	arr := bldr.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: "x", Type: arr.DataType(), Nullable: true}
	expected := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil),
		[]arrow.Column{*arrow.NewColumn(field, arrow.NewChunked(field.Type, []arrow.Array{arr}))}, -1)
	defer arr.Release() // NewChunked
	defer expected.Release()

	ps.roundTripTable(mem, expected, false)
}

func (ps *ParquetIOTestSuite) TestStructWithListOfNestedStructs() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewStructBuilder(mem, arrow.StructOf(
		arrow.Field{
			Nullable: true,
			Name:     "l",
			Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{
					Nullable: true,
					Name:     "a",
					Type: arrow.StructOf(
						arrow.Field{
							Nullable: true,
							Name:     "b",
							Type:     arrow.BinaryTypes.String,
						},
					),
				},
			)),
		},
	))
	defer bldr.Release()

	lBldr := bldr.FieldBuilder(0).(*array.ListBuilder)
	stBldr := lBldr.ValueBuilder().(*array.StructBuilder)
	aBldr := stBldr.FieldBuilder(0).(*array.StructBuilder)
	bBldr := aBldr.FieldBuilder(0).(*array.StringBuilder)

	bldr.AppendNull()
	bldr.Append(true)
	lBldr.Append(true)
	for i := 0; i < 8; i++ {
		stBldr.Append(true)
		aBldr.Append(true)
		bBldr.Append(strconv.Itoa(i))
	}

	arr := bldr.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: "x", Type: arr.DataType(), Nullable: true}
	expected := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil),
		[]arrow.Column{*arrow.NewColumn(field, arrow.NewChunked(field.Type, []arrow.Array{arr}))}, -1)
	defer arr.Release() // NewChunked
	defer expected.Release()

	ps.roundTripTable(mem, expected, false)
}

func TestParquetArrowIO(t *testing.T) {
	suite.Run(t, new(ParquetIOTestSuite))
}

func TestBufferedRecWrite(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "struct_i64_f64", Type: arrow.StructOf(
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true})},
	}, nil)

	structData := array.NewData(sc.Field(2).Type, SIZELEN,
		[]*memory.Buffer{nil, nil},
		[]arrow.ArrayData{testutils.RandomNullable(arrow.PrimitiveTypes.Int64, SIZELEN, 0).Data(), testutils.RandomNullable(arrow.PrimitiveTypes.Float64, SIZELEN, 0).Data()}, 0, 0)
	defer structData.Release()
	cols := []arrow.Array{
		testutils.RandomNullable(sc.Field(0).Type, SIZELEN, SIZELEN/5),
		testutils.RandomNullable(sc.Field(1).Type, SIZELEN, SIZELEN/5),
		array.NewStructData(structData),
	}

	rec := array.NewRecord(sc, cols, SIZELEN)
	defer rec.Release()

	var (
		buf bytes.Buffer
	)

	wr, err := pqarrow.NewFileWriter(sc, &buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy), parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(100*1024)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	require.NoError(t, err)

	p1 := rec.NewSlice(0, SIZELEN/2)
	defer p1.Release()
	require.NoError(t, wr.WriteBuffered(p1))

	p2 := rec.NewSlice(SIZELEN/2, SIZELEN)
	defer p2.Release()
	require.NoError(t, wr.WriteBuffered(p2))

	wr.Close()

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, rdr.NumRowGroups())
	assert.EqualValues(t, SIZELEN, rdr.NumRows())
	rdr.Close()

	tbl, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(buf.Bytes()), nil, pqarrow.ArrowReadProperties{}, nil)
	assert.NoError(t, err)
	defer tbl.Release()

	assert.EqualValues(t, SIZELEN, tbl.NumRows())
}

func (ps *ParquetIOTestSuite) TestArrowMapTypeRoundTrip() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	bldr := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	defer bldr.Release()

	kb := bldr.KeyBuilder().(*array.StringBuilder)
	ib := bldr.ItemBuilder().(*array.Int32Builder)

	bldr.Append(true)
	kb.AppendValues([]string{"Fee", "Fi", "Fo", "Fum"}, nil)
	ib.AppendValues([]int32{1, 2, 3, 4}, nil)

	bldr.Append(true)
	kb.AppendValues([]string{"Fee", "Fi", "Fo"}, nil)
	ib.AppendValues([]int32{5, 4, 3}, nil)

	bldr.AppendNull()

	bldr.Append(true)
	kb.AppendValues([]string{"Fo", "Fi", "Fee"}, nil)
	ib.AppendValues([]int32{-1, 2, 3}, []bool{false, true, true})

	arr := bldr.NewArray()
	defer arr.Release()

	fld := arrow.Field{Name: "mapped", Type: arr.DataType(), Nullable: true}
	cnk := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer arr.Release() // NewChunked
	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{fld}, nil), []arrow.Column{*arrow.NewColumn(fld, cnk)}, -1)
	defer cnk.Release() // NewColumn
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, true)
}

func (ps *ParquetIOTestSuite) TestArrowExtensionTypeRoundTrip() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	extBuilder := array.NewExtensionBuilder(mem, types.NewUUIDType())
	defer extBuilder.Release()
	builder := types.NewUUIDBuilder(extBuilder)
	builder.Append(uuid.New())
	arr := builder.NewArray()
	defer arr.Release()

	fld := arrow.Field{Name: "uuid", Type: arr.DataType(), Nullable: true}
	cnk := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer arr.Release() // NewChunked
	tbl := array.NewTable(arrow.NewSchema([]arrow.Field{fld}, nil), []arrow.Column{*arrow.NewColumn(fld, cnk)}, -1)
	defer cnk.Release() // NewColumn
	defer tbl.Release()

	ps.roundTripTable(mem, tbl, true)
}

func (ps *ParquetIOTestSuite) TestArrowUnknownExtensionTypeRoundTrip() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(ps.T(), 0)

	var written, expected arrow.Table

	{
		// Prepare `written` table with the extension type registered.
		extType := types.NewUUIDType()
		bldr := array.NewExtensionBuilder(mem, extType)
		defer bldr.Release()

		bldr.Builder.(*array.FixedSizeBinaryBuilder).AppendValues(
			[][]byte{nil, []byte("abcdefghijklmno0"), []byte("abcdefghijklmno1"), []byte("abcdefghijklmno2")},
			[]bool{false, true, true, true})

		arr := bldr.NewArray()
		defer arr.Release()

		if arrow.GetExtensionType("uuid") != nil {
			ps.NoError(arrow.UnregisterExtensionType("uuid"))
		}

		fld := arrow.Field{Name: "uuid", Type: arr.DataType(), Nullable: true}
		cnk := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		defer arr.Release() // NewChunked
		written = array.NewTable(arrow.NewSchema([]arrow.Field{fld}, nil), []arrow.Column{*arrow.NewColumn(fld, cnk)}, -1)
		defer cnk.Release() // NewColumn
		defer written.Release()
	}

	{
		// Prepare `expected` table with the extension type unregistered in the underlying type.
		bldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 16})
		defer bldr.Release()
		bldr.AppendValues(
			[][]byte{nil, []byte("abcdefghijklmno0"), []byte("abcdefghijklmno1"), []byte("abcdefghijklmno2")},
			[]bool{false, true, true, true})

		arr := bldr.NewArray()
		defer arr.Release()

		fld := arrow.Field{Name: "uuid", Type: arr.DataType(), Nullable: true}
		cnk := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		defer arr.Release() // NewChunked
		expected = array.NewTable(arrow.NewSchema([]arrow.Field{fld}, nil), []arrow.Column{*arrow.NewColumn(fld, cnk)}, -1)
		defer cnk.Release() // NewColumn
		defer expected.Release()
	}

	// sanity check before going deeper
	ps.Equal(expected.NumCols(), written.NumCols())
	ps.Equal(expected.NumRows(), written.NumRows())

	// just like roundTripTable() but different written vs. expected tables
	var buf bytes.Buffer
	props := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(mem))

	writeProps := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	ps.Require().NoError(pqarrow.WriteTable(written, &buf, written.NumRows(), writeProps, props))

	reader := ps.createReader(mem, buf.Bytes())
	defer reader.ParquetReader().Close()

	tbl := ps.readTable(reader)
	defer tbl.Release()

	ps.Equal(expected.NumCols(), tbl.NumCols())
	ps.Equal(expected.NumRows(), tbl.NumRows())

	exChunk := expected.Column(0).Data()
	tblChunk := tbl.Column(0).Data()

	ps.Equal(len(exChunk.Chunks()), len(tblChunk.Chunks()))
	exc := exChunk.Chunk(0)
	tbc := tblChunk.Chunk(0)
	ps.Truef(array.Equal(exc, tbc), "expected: %T %s\ngot: %T %s", exc, exc, tbc, tbc)

	expectedMd := arrow.MetadataFrom(map[string]string{
		ipc.ExtensionTypeKeyName:     "uuid",
		ipc.ExtensionMetadataKeyName: "uuid-serialized",
		"PARQUET:field_id":           "-1",
	})
	ps.Truef(expectedMd.Equal(tbl.Column(0).Field().Metadata), "expected: %v\ngot: %v", expectedMd, tbl.Column(0).Field().Metadata)
}

func TestWriteTableMemoryAllocation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "struct_i64_f64", Type: arrow.StructOf(
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true})},
		{Name: "arr_i64", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "uuid", Type: types.NewUUIDType(), Nullable: true},
	}, nil)

	bld := array.NewRecordBuilder(mem, sc)
	bld.Field(0).(*array.Float32Builder).Append(1.0)
	bld.Field(1).(*array.Int32Builder).Append(1)
	sbld := bld.Field(2).(*array.StructBuilder)
	sbld.Append(true)
	sbld.FieldBuilder(0).(*array.Int64Builder).Append(1)
	sbld.FieldBuilder(1).(*array.Float64Builder).Append(1.0)
	abld := bld.Field(3).(*array.ListBuilder)
	abld.Append(true)
	abld.ValueBuilder().(*array.Int64Builder).Append(2)
	bld.Field(4).(*types.UUIDBuilder).Append(uuid.MustParse("00000000-0000-0000-0000-000000000001"))

	rec := bld.NewRecord()
	bld.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(sc, &buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
	require.NoError(t, err)

	require.NoError(t, wr.Write(rec))
	rec.Release()
	wr.Close()

	require.Zero(t, mem.CurrentAlloc())
}

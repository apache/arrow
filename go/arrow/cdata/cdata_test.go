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

//go:build cgo && test
// +build cgo,test

// use test tag so that we only run these tests when the "test" tag is present
// so that the .c and other framework infrastructure is only compiled in during
// testing, and the .c files and symbols are not present in release builds.

package cdata

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/cgo"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestSchemaExport(t *testing.T) {
	sc := exportInt32TypeSchema()
	f, err := importSchema(&sc)
	assert.NoError(t, err)

	keys, _ := getMetadataKeys()
	vals, _ := getMetadataValues()

	assert.Equal(t, arrow.PrimitiveTypes.Int32, f.Type)
	assert.Equal(t, keys, f.Metadata.Keys())
	assert.Equal(t, vals, f.Metadata.Values())

	// schema was released when importing
	assert.True(t, schemaIsReleased(&sc))
}

func TestSimpleArrayExport(t *testing.T) {
	assert.False(t, test1IsReleased())

	testarr := exportInt32Array()
	arr, err := ImportCArrayWithType(testarr, arrow.PrimitiveTypes.Int32)
	assert.NoError(t, err)

	assert.False(t, test1IsReleased())
	assert.True(t, isReleased(testarr))

	arr.Release()
	runtime.GC()
	assert.Eventually(t, test1IsReleased, 1*time.Second, 10*time.Millisecond)
}

func TestSimpleArrayAndSchema(t *testing.T) {
	sc := exportInt32TypeSchema()
	testarr := exportInt32Array()

	// grab address of the buffer we stuck into the ArrowArray object
	buflist := (*[2]unsafe.Pointer)(unsafe.Pointer(testarr.buffers))
	origvals := (*[10]int32)(unsafe.Pointer(buflist[1]))

	fld, arr, err := ImportCArray(testarr, &sc)
	assert.NoError(t, err)
	assert.Equal(t, arrow.PrimitiveTypes.Int32, fld.Type)
	assert.EqualValues(t, 10, arr.Len())

	// verify that the address is the same of the first integer for the
	// slice that is being used by the arrow.Array and the original buffer
	vals := arr.(*array.Int32).Int32Values()
	assert.Same(t, &vals[0], &origvals[0])

	// and that the values are correct
	for i, v := range vals {
		assert.Equal(t, int32(i+1), v)
	}
}

func TestPrimitiveSchemas(t *testing.T) {
	tests := []struct {
		typ arrow.DataType
		fmt string
	}{
		{arrow.PrimitiveTypes.Int8, "c"},
		{arrow.PrimitiveTypes.Int16, "s"},
		{arrow.PrimitiveTypes.Int32, "i"},
		{arrow.PrimitiveTypes.Int64, "l"},
		{arrow.PrimitiveTypes.Uint8, "C"},
		{arrow.PrimitiveTypes.Uint16, "S"},
		{arrow.PrimitiveTypes.Uint32, "I"},
		{arrow.PrimitiveTypes.Uint64, "L"},
		{arrow.FixedWidthTypes.Boolean, "b"},
		{arrow.Null, "n"},
		{arrow.FixedWidthTypes.Float16, "e"},
		{arrow.PrimitiveTypes.Float32, "f"},
		{arrow.PrimitiveTypes.Float64, "g"},
		{&arrow.FixedSizeBinaryType{ByteWidth: 3}, "w:3"},
		{arrow.BinaryTypes.Binary, "z"},
		{arrow.BinaryTypes.LargeBinary, "Z"},
		{arrow.BinaryTypes.String, "u"},
		{arrow.BinaryTypes.LargeString, "U"},
		{&arrow.Decimal128Type{Precision: 16, Scale: 4}, "d:16,4"},
		{&arrow.Decimal128Type{Precision: 15, Scale: 0}, "d:15,0"},
		{&arrow.Decimal128Type{Precision: 15, Scale: -4}, "d:15,-4"},
	}

	for _, tt := range tests {
		t.Run(tt.typ.Name(), func(t *testing.T) {
			sc := testPrimitive(tt.fmt)

			f, err := ImportCArrowField(&sc)
			assert.NoError(t, err)

			assert.True(t, arrow.TypeEqual(tt.typ, f.Type))

			assert.True(t, schemaIsReleased(&sc))
		})
	}
}

func TestImportTemporalSchema(t *testing.T) {
	tests := []struct {
		typ arrow.DataType
		fmt string
	}{
		{arrow.FixedWidthTypes.Date32, "tdD"},
		{arrow.FixedWidthTypes.Date64, "tdm"},
		{arrow.FixedWidthTypes.Time32s, "tts"},
		{arrow.FixedWidthTypes.Time32ms, "ttm"},
		{arrow.FixedWidthTypes.Time64us, "ttu"},
		{arrow.FixedWidthTypes.Time64ns, "ttn"},
		{arrow.FixedWidthTypes.Duration_s, "tDs"},
		{arrow.FixedWidthTypes.Duration_ms, "tDm"},
		{arrow.FixedWidthTypes.Duration_us, "tDu"},
		{arrow.FixedWidthTypes.Duration_ns, "tDn"},
		{arrow.FixedWidthTypes.MonthInterval, "tiM"},
		{arrow.FixedWidthTypes.DayTimeInterval, "tiD"},
		{arrow.FixedWidthTypes.MonthDayNanoInterval, "tin"},
		{arrow.FixedWidthTypes.Timestamp_s, "tss:"},
		{&arrow.TimestampType{Unit: arrow.Second, TimeZone: "Europe/Paris"}, "tss:Europe/Paris"},
		{arrow.FixedWidthTypes.Timestamp_ms, "tsm:"},
		{&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Europe/Paris"}, "tsm:Europe/Paris"},
		{arrow.FixedWidthTypes.Timestamp_us, "tsu:"},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Paris"}, "tsu:Europe/Paris"},
		{arrow.FixedWidthTypes.Timestamp_ns, "tsn:"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Europe/Paris"}, "tsn:Europe/Paris"},
	}

	for _, tt := range tests {
		t.Run(tt.typ.Name(), func(t *testing.T) {
			sc := testPrimitive(tt.fmt)

			f, err := ImportCArrowField(&sc)
			assert.NoError(t, err)

			assert.True(t, arrow.TypeEqual(tt.typ, f.Type))

			assert.True(t, schemaIsReleased(&sc))
		})
	}
}

func TestListSchemas(t *testing.T) {
	tests := []struct {
		typ    arrow.DataType
		fmts   []string
		names  []string
		isnull []bool
	}{
		{arrow.ListOf(arrow.PrimitiveTypes.Int8), []string{"+l", "c"}, []string{"", "item"}, []bool{true}},
		{arrow.FixedSizeListOfNonNullable(2, arrow.PrimitiveTypes.Int64), []string{"+w:2", "l"}, []string{"", "item"}, []bool{false}},
		{arrow.ListOfNonNullable(arrow.ListOf(arrow.PrimitiveTypes.Int32)), []string{"+l", "+l", "i"}, []string{"", "item", "item"}, []bool{false, true}},
	}

	for _, tt := range tests {
		t.Run(tt.typ.Name(), func(t *testing.T) {
			sc := testNested(tt.fmts, tt.names, tt.isnull)
			defer freeMallocedSchemas(sc)

			top := (*[1]*CArrowSchema)(unsafe.Pointer(sc))[0]
			f, err := ImportCArrowField(top)
			assert.NoError(t, err)

			assert.True(t, arrow.TypeEqual(tt.typ, f.Type))

			assert.True(t, schemaIsReleased(top))
		})
	}
}

func TestStructSchemas(t *testing.T) {
	tests := []struct {
		typ   arrow.DataType
		fmts  []string
		names []string
		flags []int64
	}{
		{arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: metadata2},
		), []string{"+s", "c", "u"}, []string{"", "a", "b"}, []int64{flagIsNullable, flagIsNullable, flagIsNullable}},
	}

	for _, tt := range tests {
		t.Run(tt.typ.Name(), func(t *testing.T) {
			sc := testStruct(tt.fmts, tt.names, tt.flags)
			defer freeMallocedSchemas(sc)

			top := (*[1]*CArrowSchema)(unsafe.Pointer(sc))[0]
			f, err := ImportCArrowField(top)
			assert.NoError(t, err)

			assert.True(t, arrow.TypeEqual(tt.typ, f.Type))

			assert.True(t, schemaIsReleased(top))
		})
	}
}

func TestMapSchemas(t *testing.T) {
	tests := []struct {
		typ        *arrow.MapType
		keysSorted bool
		fmts       []string
		names      []string
		flags      []int64
	}{
		{arrow.MapOf(arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String), false, []string{"+m", "+s", "c", "u"}, []string{"", "entries", "key", "value"}, []int64{flagIsNullable, 0, 0, flagIsNullable}},
		{arrow.MapOf(arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String), true, []string{"+m", "+s", "c", "u"}, []string{"", "entries", "key", "value"}, []int64{flagIsNullable | flagMapKeysSorted, 0, 0, flagIsNullable}},
	}

	for _, tt := range tests {
		t.Run(tt.typ.Name(), func(t *testing.T) {
			sc := testMap(tt.fmts, tt.names, tt.flags)
			defer freeMallocedSchemas(sc)

			top := (*[1]*CArrowSchema)(unsafe.Pointer(sc))[0]
			f, err := ImportCArrowField(top)
			assert.NoError(t, err)

			tt.typ.KeysSorted = tt.keysSorted
			assert.True(t, arrow.TypeEqual(tt.typ, f.Type))

			assert.True(t, schemaIsReleased(top))
		})
	}
}

func TestSchema(t *testing.T) {
	// schema is exported as an equivalent struct type (+ top-level metadata)
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "nulls", Type: arrow.Null, Nullable: false},
		{Name: "values", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: metadata1},
	}, &metadata2)

	cst := testSchema([]string{"+s", "n", "l"}, []string{"", "nulls", "values"}, []int64{0, 0, flagIsNullable})
	defer freeMallocedSchemas(cst)

	top := (*[1]*CArrowSchema)(unsafe.Pointer(cst))[0]
	out, err := ImportCArrowSchema(top)
	assert.NoError(t, err)

	assert.True(t, sc.Equal(out))
	assert.True(t, sc.Metadata().Equal(out.Metadata()))

	assert.True(t, schemaIsReleased(top))
}

func createTestInt8Arr() arrow.Array {
	bld := array.NewInt8Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]int8{1, 2, 0, -3}, []bool{true, true, false, true})
	return bld.NewInt8Array()
}

func createTestInt16Arr() arrow.Array {
	bld := array.NewInt16Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]int16{1, 2, -3}, []bool{true, true, true})
	return bld.NewInt16Array()
}

func createTestInt32Arr() arrow.Array {
	bld := array.NewInt32Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]int32{1, 2, 0, -3}, []bool{true, true, false, true})
	return bld.NewInt32Array()
}

func createTestInt64Arr() arrow.Array {
	bld := array.NewInt64Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]int64{1, 2, -3}, []bool{true, true, true})
	return bld.NewInt64Array()
}

func createTestUint8Arr() arrow.Array {
	bld := array.NewUint8Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]uint8{1, 2, 0, 3}, []bool{true, true, false, true})
	return bld.NewUint8Array()
}

func createTestUint16Arr() arrow.Array {
	bld := array.NewUint16Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]uint16{1, 2, 3}, []bool{true, true, true})
	return bld.NewUint16Array()
}

func createTestUint32Arr() arrow.Array {
	bld := array.NewUint32Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]uint32{1, 2, 0, 3}, []bool{true, true, false, true})
	return bld.NewUint32Array()
}

func createTestUint64Arr() arrow.Array {
	bld := array.NewUint64Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]uint64{1, 2, 3}, []bool{true, true, true})
	return bld.NewUint64Array()
}

func createTestBoolArr() arrow.Array {
	bld := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]bool{true, false, false}, []bool{true, true, false})
	return bld.NewBooleanArray()
}

func createTestNullArr() arrow.Array {
	return array.NewNull(2)
}

func createTestFloat32Arr() arrow.Array {
	bld := array.NewFloat32Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]float32{1.5, 0}, []bool{true, false})
	return bld.NewFloat32Array()
}

func createTestFloat64Arr() arrow.Array {
	bld := array.NewFloat64Builder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]float64{1.5, 0}, []bool{true, false})
	return bld.NewFloat64Array()
}

func createTestFSBArr() arrow.Array {
	bld := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 3})
	defer bld.Release()

	bld.AppendValues([][]byte{[]byte("foo"), []byte("bar"), nil}, []bool{true, true, false})
	return bld.NewFixedSizeBinaryArray()
}

func createTestBinaryArr() arrow.Array {
	bld := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer bld.Release()

	bld.AppendValues([][]byte{[]byte("foo"), []byte("bar"), nil}, []bool{true, true, false})
	return bld.NewBinaryArray()
}

func createTestStrArr() arrow.Array {
	bld := array.NewStringBuilder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]string{"foo", "bar", ""}, []bool{true, true, false})
	return bld.NewStringArray()
}

func createTestLargeBinaryArr() arrow.Array {
	bld := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.LargeBinary)
	defer bld.Release()

	bld.AppendValues([][]byte{[]byte("foo"), []byte("bar"), nil}, []bool{true, true, false})
	return bld.NewLargeBinaryArray()
}

func createTestLargeStrArr() arrow.Array {
	bld := array.NewLargeStringBuilder(memory.DefaultAllocator)
	defer bld.Release()

	bld.AppendValues([]string{"foo", "bar", ""}, []bool{true, true, false})
	return bld.NewLargeStringArray()
}

func createTestDecimalArr() arrow.Array {
	bld := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 16, Scale: 4})
	defer bld.Release()

	bld.AppendValues([]decimal128.Num{decimal128.FromU64(12345670), decimal128.FromU64(0)}, []bool{true, false})
	return bld.NewDecimal128Array()
}

func TestPrimitiveArrs(t *testing.T) {
	tests := []struct {
		name string
		fn   func() arrow.Array
	}{
		{"int8", createTestInt8Arr},
		{"uint8", createTestUint8Arr},
		{"int16", createTestInt16Arr},
		{"uint16", createTestUint16Arr},
		{"int32", createTestInt32Arr},
		{"uint32", createTestUint32Arr},
		{"int64", createTestInt64Arr},
		{"uint64", createTestUint64Arr},
		{"bool", createTestBoolArr},
		{"null", createTestNullArr},
		{"float32", createTestFloat32Arr},
		{"float64", createTestFloat64Arr},
		{"fixed size binary", createTestFSBArr},
		{"binary", createTestBinaryArr},
		{"utf8", createTestStrArr},
		{"largebinary", createTestLargeBinaryArr},
		{"largeutf8", createTestLargeStrArr},
		{"decimal128", createTestDecimalArr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.fn()
			defer arr.Release()

			carr := createCArr(arr)
			defer freeTestArr(carr)

			imported, err := ImportCArrayWithType(carr, arr.DataType())
			assert.NoError(t, err)
			assert.True(t, array.ArrayEqual(arr, imported))
			assert.True(t, isReleased(carr))

			imported.Release()
		})
	}
}

func TestPrimitiveSliced(t *testing.T) {
	arr := createTestInt16Arr()
	defer arr.Release()

	sl := array.NewSlice(arr, 1, 2)
	defer sl.Release()

	carr := createCArr(sl)
	defer freeTestArr(carr)

	imported, err := ImportCArrayWithType(carr, arr.DataType())
	assert.NoError(t, err)
	assert.True(t, array.ArrayEqual(sl, imported))
	assert.True(t, array.SliceEqual(arr, 1, 2, imported, 0, int64(imported.Len())))
	assert.True(t, isReleased(carr))

	imported.Release()
}

func createTestListArr() arrow.Array {
	bld := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8)
	defer bld.Release()

	vb := bld.ValueBuilder().(*array.Int8Builder)

	bld.Append(true)
	vb.AppendValues([]int8{1, 2}, []bool{true, true})

	bld.Append(true)
	vb.AppendValues([]int8{3, 0}, []bool{true, false})

	bld.AppendNull()

	return bld.NewArray()
}

func createTestLargeListArr() arrow.Array {
	bld := array.NewLargeListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8)
	defer bld.Release()

	vb := bld.ValueBuilder().(*array.Int8Builder)

	bld.Append(true)
	vb.AppendValues([]int8{1, 2}, []bool{true, true})

	bld.Append(true)
	vb.AppendValues([]int8{3, 0}, []bool{true, false})

	bld.AppendNull()

	return bld.NewArray()
}

func createTestFixedSizeList() arrow.Array {
	bld := array.NewFixedSizeListBuilder(memory.DefaultAllocator, 2, arrow.PrimitiveTypes.Int64)
	defer bld.Release()

	vb := bld.ValueBuilder().(*array.Int64Builder)

	bld.Append(true)
	vb.AppendValues([]int64{1, 2}, []bool{true, true})

	bld.Append(true)
	vb.AppendValues([]int64{3, 0}, []bool{true, false})

	bld.AppendNull()
	return bld.NewArray()
}

func createTestStructArr() arrow.Array {
	bld := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	))
	defer bld.Release()

	f1bld := bld.FieldBuilder(0).(*array.Int8Builder)
	f2bld := bld.FieldBuilder(1).(*array.StringBuilder)

	bld.Append(true)
	f1bld.Append(1)
	f2bld.Append("foo")

	bld.Append(true)
	f1bld.Append(2)
	f2bld.AppendNull()

	return bld.NewArray()
}

func createTestMapArr() arrow.Array {
	bld := array.NewMapBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String, false)
	defer bld.Release()

	kb := bld.KeyBuilder().(*array.Int8Builder)
	vb := bld.ItemBuilder().(*array.StringBuilder)

	bld.Append(true)
	kb.Append(1)
	vb.Append("foo")
	kb.Append(2)
	vb.AppendNull()

	bld.Append(true)
	kb.Append(3)
	vb.Append("bar")

	return bld.NewArray()
}

func createTestSparseUnion() arrow.Array {
	return createTestUnionArr(arrow.SparseMode)
}

func createTestDenseUnion() arrow.Array {
	return createTestUnionArr(arrow.DenseMode)
}

func createTestUnionArr(mode arrow.UnionMode) arrow.Array {
	fields := []arrow.Field{
		arrow.Field{Name: "u0", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "u1", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
	}
	typeCodes := []arrow.UnionTypeCode{5, 10}
	bld := array.NewBuilder(memory.DefaultAllocator, arrow.UnionOf(mode, fields, typeCodes)).(array.UnionBuilder)
	defer bld.Release()

	u0Bld := bld.Child(0).(*array.Int32Builder)
	u1Bld := bld.Child(1).(*array.Uint8Builder)

	bld.Append(5)
	if mode == arrow.SparseMode {
		u1Bld.AppendNull()
	}
	u0Bld.Append(128)
	bld.Append(5)
	if mode == arrow.SparseMode {
		u1Bld.AppendNull()
	}
	u0Bld.Append(256)
	bld.Append(10)
	if mode == arrow.SparseMode {
		u0Bld.AppendNull()
	}
	u1Bld.Append(127)
	bld.Append(10)
	if mode == arrow.SparseMode {
		u0Bld.AppendNull()
	}
	u1Bld.Append(25)

	return bld.NewArray()
}

func TestNestedArrays(t *testing.T) {
	tests := []struct {
		name string
		fn   func() arrow.Array
	}{
		{"list", createTestListArr},
		{"large list", createTestLargeListArr},
		{"fixed size list", createTestFixedSizeList},
		{"struct", createTestStructArr},
		{"map", createTestMapArr},
		{"sparse union", createTestSparseUnion},
		{"dense union", createTestDenseUnion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.fn()
			defer arr.Release()

			carr := createCArr(arr)
			defer freeTestArr(carr)

			imported, err := ImportCArrayWithType(carr, arr.DataType())
			assert.NoError(t, err)
			assert.True(t, array.ArrayEqual(arr, imported))
			assert.True(t, isReleased(carr))

			imported.Release()
		})
	}
}

func TestRecordBatch(t *testing.T) {
	arr := createTestStructArr()
	defer arr.Release()

	carr := createCArr(arr)
	defer freeTestArr(carr)

	sc := testStruct([]string{"+s", "c", "u"}, []string{"", "a", "b"}, []int64{0, flagIsNullable, flagIsNullable})
	defer freeMallocedSchemas(sc)

	top := (*[1]*CArrowSchema)(unsafe.Pointer(sc))[0]
	rb, err := ImportCRecordBatch(carr, top)
	assert.NoError(t, err)
	defer rb.Release()

	assert.EqualValues(t, 2, rb.NumCols())
	rbschema := rb.Schema()
	assert.Equal(t, "a", rbschema.Field(0).Name)
	assert.Equal(t, "b", rbschema.Field(1).Name)

	rec := array.NewRecord(rbschema, []arrow.Array{arr.(*array.Struct).Field(0), arr.(*array.Struct).Field(1)}, -1)
	defer rec.Release()

	assert.True(t, array.RecordEqual(rb, rec))
}

func TestRecordReaderStream(t *testing.T) {
	stream := arrayStreamTest()
	defer releaseStream(stream)

	rdr := ImportCArrayStream(stream, nil)
	i := 0
	for {
		rec, err := rdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}

		assert.EqualValues(t, 2, rec.NumCols())
		assert.Equal(t, "a", rec.ColumnName(0))
		assert.Equal(t, "b", rec.ColumnName(1))
		i++
		for j := 0; j < int(rec.NumRows()); j++ {
			assert.Equal(t, int32((j+1)*i), rec.Column(0).(*array.Int32).Value(j))
		}
		assert.Equal(t, "foo", rec.Column(1).(*array.String).Value(0))
		assert.Equal(t, "bar", rec.Column(1).(*array.String).Value(1))
		assert.Equal(t, "baz", rec.Column(1).(*array.String).Value(2))
	}
}

func TestExportRecordReaderStream(t *testing.T) {
	reclist := arrdata.Records["primitives"]
	rdr, _ := array.NewRecordReader(reclist[0].Schema(), reclist)

	out := createTestStreamObj()
	ExportRecordReader(rdr, out)

	assert.NotNil(t, out.get_schema)
	assert.NotNil(t, out.get_next)
	assert.NotNil(t, out.get_last_error)
	assert.NotNil(t, out.release)
	assert.NotNil(t, out.private_data)

	h := *(*cgo.Handle)(out.private_data)
	assert.Same(t, rdr, h.Value().(cRecordReader).rdr)

	importedRdr := ImportCArrayStream(out, nil)
	i := 0
	for {
		rec, err := importedRdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}

		assert.Truef(t, array.RecordEqual(reclist[i], rec), "expected: %s\ngot: %s", reclist[i], rec)
		i++
	}
	assert.EqualValues(t, len(reclist), i)
}

func TestEmptyListExport(t *testing.T) {
	bldr := array.NewBuilder(memory.DefaultAllocator, arrow.LargeListOf(arrow.PrimitiveTypes.Int32))
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()

	var out CArrowArray
	ExportArrowArray(arr, &out, nil)

	assert.Zero(t, out.length)
	assert.Zero(t, out.null_count)
	assert.Zero(t, out.offset)
	assert.EqualValues(t, 2, out.n_buffers)
	assert.NotNil(t, out.buffers)
	assert.EqualValues(t, 1, out.n_children)
	assert.NotNil(t, out.children)
}

func TestEmptyDictExport(t *testing.T) {
	bldr := array.NewBuilder(memory.DefaultAllocator, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String, Ordered: true})
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()

	var out CArrowArray
	var sc CArrowSchema
	ExportArrowArray(arr, &out, &sc)

	assert.EqualValues(t, 'c', *sc.format)
	assert.NotZero(t, sc.flags&1)
	assert.Zero(t, sc.n_children)
	assert.NotNil(t, sc.dictionary)
	assert.EqualValues(t, 'u', *sc.dictionary.format)

	assert.Zero(t, out.length)
	assert.Zero(t, out.null_count)
	assert.Zero(t, out.offset)
	assert.EqualValues(t, 2, out.n_buffers)
	assert.Zero(t, out.n_children)
	assert.Nil(t, out.children)
	assert.NotNil(t, out.dictionary)

	assert.Zero(t, out.dictionary.length)
	assert.Zero(t, out.dictionary.null_count)
	assert.Zero(t, out.dictionary.offset)
	assert.EqualValues(t, 3, out.dictionary.n_buffers)
	assert.Zero(t, out.dictionary.n_children)
	assert.Nil(t, out.dictionary.children)
	assert.Nil(t, out.dictionary.dictionary)
}

func TestEmptyStringExport(t *testing.T) {
	// apache/arrow#33936: regression test
	bldr := array.NewBuilder(memory.DefaultAllocator, &arrow.StringType{})
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()

	var out CArrowArray
	var sc CArrowSchema
	ExportArrowArray(arr, &out, &sc)

	assert.EqualValues(t, 'u', *sc.format)
	assert.Zero(t, sc.n_children)
	assert.Nil(t, sc.dictionary)

	assert.EqualValues(t, 3, out.n_buffers)
	buffers := (*[3]unsafe.Pointer)(unsafe.Pointer(out.buffers))
	assert.EqualValues(t, unsafe.Pointer(nil), buffers[0])
	assert.NotEqualValues(t, unsafe.Pointer(nil), buffers[1])
	assert.NotEqualValues(t, unsafe.Pointer(nil), buffers[2])
}

func TestEmptyUnionExport(t *testing.T) {
	// apache/arrow#33936: regression test
	bldr := array.NewBuilder(memory.DefaultAllocator, arrow.SparseUnionOf([]arrow.Field{
		{Name: "child", Type: &arrow.Int64Type{}},
	}, []arrow.UnionTypeCode{0}))
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()

	var out CArrowArray
	var sc CArrowSchema
	ExportArrowArray(arr, &out, &sc)

	assert.EqualValues(t, 1, sc.n_children)
	assert.Nil(t, sc.dictionary)

	assert.EqualValues(t, 1, out.n_buffers)
	buffers := (*[1]unsafe.Pointer)(unsafe.Pointer(out.buffers))
	assert.NotEqualValues(t, unsafe.Pointer(nil), buffers[0])
}

func TestRecordReaderExport(t *testing.T) {
	// Regression test for apache/arrow#33767
	reclist := arrdata.Records["primitives"]
	rdr, _ := array.NewRecordReader(reclist[0].Schema(), reclist)

	if err := exportedStreamTest(rdr); err != nil {
		t.Fatalf("Failed to test exported stream: %#v", err)
	}
}

type failingReader struct {
	opCount int
}

func (r *failingReader) Retain()  {}
func (r *failingReader) Release() {}
func (r *failingReader) Schema() *arrow.Schema {
	r.opCount -= 1
	if r.opCount == 0 {
		return nil
	}
	return arrdata.Records["primitives"][0].Schema()
}
func (r *failingReader) Next() bool {
	r.opCount -= 1
	return r.opCount > 0
}
func (r *failingReader) Record() arrow.Record {
	arrdata.Records["primitives"][0].Retain()
	return arrdata.Records["primitives"][0]
}
func (r *failingReader) Err() error {
	if r.opCount == 0 {
		return fmt.Errorf("Expected error message")
	}
	return nil
}

func TestRecordReaderError(t *testing.T) {
	// Regression test for apache/arrow#33789
	err := roundTripStreamTest(&failingReader{opCount: 1})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	assert.Contains(t, err.Error(), "Expected error message")

	err = roundTripStreamTest(&failingReader{opCount: 2})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	assert.Contains(t, err.Error(), "Expected error message")

	err = roundTripStreamTest(&failingReader{opCount: 3})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	assert.Contains(t, err.Error(), "Expected error message")
}

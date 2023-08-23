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

package ipc_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func TestArrow12072(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "idx", Type: arrow.PrimitiveTypes.Int64},
			{Name: "A", Type: arrow.PrimitiveTypes.Int64},
			{Name: "B", Type: arrow.PrimitiveTypes.Int64},
			{Name: "C", Type: arrow.BinaryTypes.String},
		},
		nil, // no metadata
	)
	mem := memory.NewGoAllocator()
	counter := int64(0)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	const size = 3
	for i := 0; i < size; i++ {
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{counter}, nil)
		counter++
		b.Field(1).(*array.Int64Builder).AppendValues(
			[]int64{int64(rand.Intn(100))}, nil)
		b.Field(2).(*array.Int64Builder).AppendValues(
			[]int64{int64(rand.Intn(100))}, nil)
		b.Field(3).(*array.StringBuilder).AppendValues(
			[]string{strconv.Itoa(rand.Intn(100))}, nil)
	}

	rec := b.NewRecord()
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 1)
	defer tr.Release()

	data := []arrow.Record{}
	for tr.Next() {
		rec := tr.Record()
		rec.Retain()
		defer rec.Release()
		data = append(data, rec)
	}

	// tests writing out and then reading back in slices of the same record of length 1 each
	// testing the bug that was reported in ARROW-12072 involving offsets for string arrays
	// and correct truncation of slices when writing ipc FixedWidthDataType
	for _, rec := range data {
		var buf []byte
		assert.NotPanics(t, func() {
			var output bytes.Buffer
			w := ipc.NewWriter(&output, ipc.WithSchema(rec.Schema()))
			assert.NoError(t, w.Write(rec))
			assert.NoError(t, w.Close())
			buf = output.Bytes()
		})

		assert.NotPanics(t, func() {
			rdr, err := ipc.NewReader(bytes.NewReader(buf))
			assert.NoError(t, err)
			for rdr.Next() {
				out := rdr.Record()
				assert.Truef(t, array.RecordEqual(rec, out), "expected: %s\ngot: %s\n", rec, out)
			}
			assert.NoError(t, rdr.Err())
		})
	}
}

type testMessageReader struct {
	counter int
}

func (r *testMessageReader) Message() (*ipc.Message, error) {
	if r.counter == 0 {
		r.counter++
		// return schema message
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		if err := writer.Close(); err != nil {
			return nil, err
		}
		reader := ipc.NewMessageReader(&buf)
		return reader.Message()
	}
	// return error
	return nil, errors.New("Error!")
}
func (r *testMessageReader) Release() {}
func (r *testMessageReader) Retain()  {}

// Ensure that if the MessageReader errors, we get the error from Read
func TestArrow14769(t *testing.T) {
	reader, err := ipc.NewReaderFromMessageReader(&testMessageReader{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Read()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Expected an error, got %s", err)
	}
	if err.Error() != "Error!" {
		t.Fatalf("Expected an error, not %s", err)
	}
}

func makeTestCol(t *testing.T, alloc memory.Allocator, vals []int32, nulls []bool) (arrow.Field, *arrow.Column) {
	t.Helper()
	fld := arrow.Field{Name: "test", Type: arrow.PrimitiveTypes.Int32, Nullable: nulls != nil}

	b := array.NewInt32Builder(alloc)
	defer b.Release()
	b.AppendValues(vals, nulls)

	arr := b.NewArray()
	defer arr.Release()

	chk := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})
	defer chk.Release()

	return fld, arrow.NewColumn(fld, chk)
}

func makeTestTable(t *testing.T, fld arrow.Field, col *arrow.Column) arrow.Table {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{fld}, nil)
	return array.NewTable(schema, []arrow.Column{*col}, -1)
}

func writeThenReadTable(t *testing.T, alloc memory.Allocator, table arrow.Table) arrow.Table {
	t.Helper()

	// write the table into a buffer
	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithAllocator(alloc), ipc.WithSchema(table.Schema()))
	tr := array.NewTableReader(table, 0)
	defer tr.Release()
	for tr.Next() {
		require.NoError(t, writer.Write(tr.Record()))
	}
	require.NoError(t, writer.Close())

	// read the table from the buffer
	reader, err := ipc.NewReader(buf, ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer reader.Release()
	records := make([]arrow.Record, 0)
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		defer rec.Release()
		records = append(records, rec)
	}
	require.NoError(t, reader.Err())
	return array.NewTableFromRecords(reader.Schema(), records)
}

func TestWriteColumnWithOffset(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	t.Run("some nulls", func(t *testing.T) {
		vals := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		nulls := []bool{true, false, true, false, true, false, true, false, true, false, true}
		fld, col := makeTestCol(t, alloc, vals, nulls)
		defer col.Release()

		// slice the column so there are offsets
		col = array.NewColumnSlice(col, 3, 8)
		defer col.Release()

		table := makeTestTable(t, fld, col)
		defer table.Release()

		table = writeThenReadTable(t, alloc, table)
		defer table.Release()

		require.EqualValues(t, 1, table.NumCols())
		col = table.Column(0)
		colArr := col.Data().Chunk(0).(*array.Int32)
		require.EqualValues(t, 5, colArr.Len())
		assert.True(t, colArr.IsNull(0))
		assert.False(t, colArr.IsNull(1))
		assert.True(t, colArr.IsNull(2))
		assert.False(t, colArr.IsNull(3))
		assert.True(t, colArr.IsNull(4))
	})

	t.Run("all nulls", func(t *testing.T) {
		vals := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		nulls := []bool{false, false, false, false, false, false, false, false, false, false, false}
		fld, col := makeTestCol(t, alloc, vals, nulls)
		defer col.Release()

		// slice the column so there are offsets
		col = array.NewColumnSlice(col, 3, 8)
		defer col.Release()

		table := makeTestTable(t, fld, col)
		defer table.Release()

		table = writeThenReadTable(t, alloc, table)
		defer table.Release()

		require.EqualValues(t, 1, table.NumCols())
		col = table.Column(0)
		colArr := col.Data().Chunk(0).(*array.Int32)
		require.EqualValues(t, 5, colArr.Len())
		for i := 0; i < colArr.Len(); i++ {
			assert.True(t, colArr.IsNull(i))
		}
	})

	t.Run("no nulls", func(t *testing.T) {
		vals := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		nulls := []bool{true, true, true, true, true, true, true, true, true, true, true}
		fld, col := makeTestCol(t, alloc, vals, nulls)
		defer col.Release()

		// slice the column so there are offsets
		col = array.NewColumnSlice(col, 3, 8)
		defer col.Release()

		table := makeTestTable(t, fld, col)
		defer table.Release()

		table = writeThenReadTable(t, alloc, table)
		defer table.Release()

		require.EqualValues(t, 1, table.NumCols())
		col = table.Column(0)
		colArr := col.Data().Chunk(0).(*array.Int32)
		require.EqualValues(t, 5, colArr.Len())
		for i := 0; i < colArr.Len(); i++ {
			assert.False(t, colArr.IsNull(i))
		}
	})
}

func TestIPCTable(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4}, []bool{true, true, false, true})

	rec1 := b.NewRecord()
	defer rec1.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec1})
	defer tbl.Release()

	var buf bytes.Buffer
	ipcWriter := ipc.NewWriter(&buf, ipc.WithAllocator(pool), ipc.WithSchema(schema))
	defer func(ipcWriter *ipc.Writer) {
		err := ipcWriter.Close()
		if err != nil {
			t.Fatalf("error closing ipc writer: %s", err.Error())
		}
	}(ipcWriter)

	t.Log("Reading data before")
	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	n := 0
	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			t.Logf("rec[%d][%q]: %v nulls:%v\n", n,
				rec.ColumnName(i), col, col.NullBitmapBytes())
		}
		n++
		err := ipcWriter.Write(rec)
		if err != nil {
			panic(err)
		}
	}

	t.Log("Reading data after")
	ipcReader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(pool))
	if err != nil {
		panic(err)
	}
	n = 0
	for ipcReader.Next() {
		rec := ipcReader.Record()
		for i, col := range rec.Columns() {
			t.Logf("rec[%d][%q]: %v nulls:%v\n", n,
				rec.ColumnName(i), col, col.NullBitmapBytes())
		}
		n++
	}
}

// ARROW-18317
func TestDictionary(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// A schema with a single dictionary field
	schema := arrow.NewSchema([]arrow.Field{{Name: "field", Type: &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}}}, nil)

	// IPC writer and reader
	var bufWriter bytes.Buffer
	ipcWriter := ipc.NewWriter(&bufWriter, ipc.WithSchema(schema), ipc.WithAllocator(pool), ipc.WithDictionaryDeltas(false))
	defer ipcWriter.Close()

	bufReader := bytes.NewReader([]byte{})
	var ipcReader *ipc.Reader

	bldr := array.NewBuilder(pool, schema.Field(0).Type)
	defer bldr.Release()
	require.NoError(t, bldr.UnmarshalJSON([]byte(`["value_0"]`)))

	arr := bldr.NewArray()
	defer arr.Release()
	// Create a first record with field = "value_0"
	record := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer record.Release()

	expectedJson, err := record.MarshalJSON()
	require.NoError(t, err)
	// Serialize and deserialize the record via an IPC stream
	json, ipcReader, err := encodeDecodeIpcStream(t, record, &bufWriter, ipcWriter, bufReader, ipcReader)
	require.NoError(t, err)
	// Compare the expected JSON with the actual JSON
	require.JSONEq(t, string(expectedJson), string(json))

	// Create a second record with field = "value_1"
	require.NoError(t, bldr.UnmarshalJSON([]byte(`["value_1"]`)))
	arr = bldr.NewArray()
	defer arr.Release()
	record = array.NewRecord(schema, []arrow.Array{arr}, 1)

	// record, _, err = array.RecordFromJSON(pool, schema, strings.NewReader(`[{"field": ["value_1"]}]`))
	// require.NoError(t, err)
	defer record.Release()

	expectedJson, err = record.MarshalJSON()
	require.NoError(t, err)
	// Serialize and deserialize the record via an IPC stream
	json, ipcReader, err = encodeDecodeIpcStream(t, record, &bufWriter, ipcWriter, bufReader, ipcReader)
	require.NoError(t, err)
	// Compare the expected JSON with the actual JSON
	// field = "value_0" but should be "value_1"
	require.JSONEq(t, string(expectedJson), string(json))
	require.NoError(t, ipcReader.Err())
	ipcReader.Release()
}

// ARROW-18326
func TestDictionaryDeltas(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// A schema with a single dictionary field
	schema := arrow.NewSchema([]arrow.Field{{Name: "field", Type: &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}}}, nil)

	// IPC writer and reader
	var bufWriter bytes.Buffer
	ipcWriter := ipc.NewWriter(&bufWriter, ipc.WithSchema(schema), ipc.WithAllocator(pool), ipc.WithDictionaryDeltas(true))
	defer ipcWriter.Close()

	bufReader := bytes.NewReader([]byte{})
	var ipcReader *ipc.Reader

	bldr := array.NewBuilder(pool, schema.Field(0).Type)
	defer bldr.Release()
	require.NoError(t, bldr.UnmarshalJSON([]byte(`["value_0"]`)))

	arr := bldr.NewArray()
	defer arr.Release()
	// Create a first record with field = "value_0"
	record := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer record.Release()

	expectedJson, err := record.MarshalJSON()
	require.NoError(t, err)
	// Serialize and deserialize the record via an IPC stream
	json, ipcReader, err := encodeDecodeIpcStream(t, record, &bufWriter, ipcWriter, bufReader, ipcReader)
	require.NoError(t, err)
	// Compare the expected JSON with the actual JSON
	require.JSONEq(t, string(expectedJson), string(json))

	// Create a second record with field = "value_1"
	require.NoError(t, bldr.UnmarshalJSON([]byte(`["value_1"]`)))
	arr = bldr.NewArray()
	defer arr.Release()
	record = array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer record.Release()

	expectedJson, err = record.MarshalJSON()
	require.NoError(t, err)
	// Serialize and deserialize the record via an IPC stream
	json, ipcReader, err = encodeDecodeIpcStream(t, record, &bufWriter, ipcWriter, bufReader, ipcReader)
	require.NoError(t, err)
	// Compare the expected JSON with the actual JSON
	// field = "value_0" but should be "value_1"
	require.JSONEq(t, string(expectedJson), string(json))
	require.NoError(t, ipcReader.Err())
	ipcReader.Release()
}

// Encode and decode a record over a tuple of IPC writer and reader.
// IPC writer and reader are the same from one call to another.
func encodeDecodeIpcStream(t *testing.T,
	record arrow.Record,
	bufWriter *bytes.Buffer, ipcWriter *ipc.Writer,
	bufReader *bytes.Reader, ipcReader *ipc.Reader) ([]byte, *ipc.Reader, error) {

	// Serialize the record via an ipc writer
	if err := ipcWriter.Write(record); err != nil {
		return nil, ipcReader, err
	}
	serializedRecord := bufWriter.Bytes()
	bufWriter.Reset()

	// Deserialize the record via an ipc reader
	bufReader.Reset(serializedRecord)
	if ipcReader == nil {
		newIpcReader, err := ipc.NewReader(bufReader)
		if err != nil {
			return nil, newIpcReader, err
		}
		ipcReader = newIpcReader
	}
	ipcReader.Next()
	record = ipcReader.Record()

	// Return the decoded record as a json string
	json, err := record.MarshalJSON()
	if err != nil {
		return nil, ipcReader, err
	}
	return json, ipcReader, nil
}

func Example_mapSlice() {
	mem := memory.DefaultAllocator
	dt := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "map",
		Type: dt,
	}}, nil)

	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`[
		[{"key": "index1", "value": "main2"}],
		[{"key": "index3", "value": "main4"}, {"key": "tag_int", "value": ""}],
		[{"key":"index5","value":"main6"},{"key":"tag_int","value":""}],
		[{"key":"index6","value":"main7"},{"key":"tag_int","value":""}],
		[{"key":"index7","value":"main8"},{"key":"tag_int","value":""}],
		[{"key":"index8","value":"main9"}]
	]`))
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	rec2 := rec.NewSlice(1, 2)
	defer rec2.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec2); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	r, err := ipc.NewReader(&buf)
	if err != nil {
		panic(err)
	}
	defer r.Release()

	r.Next()
	fmt.Println(r.Record())

	// Output:
	// record:
	//   schema:
	//   fields: 1
	//     - map: type=map<utf8, utf8, items_nullable>
	//   rows: 1
	//   col[0][map]: [{["index3" "tag_int"] ["main4" ""]}]
}

func Example_listSlice() {
	mem := memory.DefaultAllocator
	dt := arrow.ListOf(arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "list",
		Type: dt,
	}}, nil)

	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`[
		["index1"], 
		["index3", "tag_int"], ["index5", "tag_int"],
		["index6", "tag_int"], ["index7", "tag_int"], 
		["index7", "tag_int"],
		["index8"]
	]`))
	if err != nil {
		panic(err)
	}
	defer arr.Release()

	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	rec2 := rec.NewSlice(1, 2)
	defer rec2.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec2); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	r, err := ipc.NewReader(&buf)
	if err != nil {
		panic(err)
	}
	defer r.Release()

	r.Next()
	fmt.Println(r.Record())

	// Output:
	// record:
	//   schema:
	//   fields: 1
	//     - list: type=list<item: utf8, nullable>
	//   rows: 1
	//   col[0][list]: [["index3" "tag_int"]]
}

func TestIpcEmptyMap(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "map",
		Type: dt,
	}}, nil)

	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(`[]`))
	require.NoError(t, err)
	defer arr.Release()

	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	require.NoError(t, w.Write(rec))
	assert.NoError(t, w.Close())

	r, err := ipc.NewReader(&buf)
	require.NoError(t, err)
	defer r.Release()

	assert.True(t, r.Next())
	assert.Zero(t, r.Record().NumRows())
	assert.True(t, arrow.TypeEqual(dt, r.Record().Column(0).DataType()))
}

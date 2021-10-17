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
	"math/rand"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
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

	tbl := array.NewTableFromRecords(schema, []array.Record{rec})
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 1)
	defer tr.Release()

	data := []array.Record{}
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

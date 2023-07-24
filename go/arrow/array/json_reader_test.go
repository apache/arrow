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

package array_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

const jsondata = `
	{"region": "NY", "model": "3", "sales": 742.0}
	{"region": "NY", "model": "S", "sales": 304.125}
	{"region": "NY", "model": "X", "sales": 136.25}
	{"region": "NY", "model": "Y", "sales": 27.5}
	{"region": "CA", "model": "3", "sales": 512}
	{"region": "CA", "model": "S", "sales": 978}
	{"region": "CA", "model": "X", "sales": 1.0}
	{"region": "CA", "model": "Y", "sales": 69}
	{"region": "QC", "model": "3", "sales": 273.5}
	{"region": "QC", "model": "S", "sales": 13}
	{"region": "QC", "model": "X", "sales": 54}
	{"region": "QC", "model": "Y", "sales": 21}
	{"region": "QC", "model": "3", "sales": 152.25}
	{"region": "QC", "model": "S", "sales": 10}
	{"region": "QC", "model": "X", "sales": 42}
	{"region": "QC", "model": "Y", "sales": 37}`

func TestJSONReader(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema)
	defer rdr.Release()

	n := 0
	for rdr.Next() {
		n++
		rec := rdr.Record()
		assert.NotNil(t, rec)
		assert.EqualValues(t, 1, rec.NumRows())
		assert.EqualValues(t, 3, rec.NumCols())
	}

	assert.NoError(t, rdr.Err())
	assert.Equal(t, 16, n)
}

func TestJSONReaderAll(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema, array.WithAllocator(mem), array.WithChunk(-1))
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.Record()
	assert.NotNil(t, rec)
	assert.NoError(t, rdr.Err())

	assert.EqualValues(t, 16, rec.NumRows())
	assert.EqualValues(t, 3, rec.NumCols())
	assert.False(t, rdr.Next())
}

func TestJSONReaderChunked(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rdr := array.NewJSONReader(strings.NewReader(jsondata), schema, array.WithAllocator(mem), array.WithChunk(4))
	defer rdr.Release()

	n := 0
	for rdr.Next() {
		n++
		rec := rdr.Record()
		assert.NotNil(t, rec)
		assert.NoError(t, rdr.Err())
		assert.EqualValues(t, 4, rec.NumRows())
	}

	assert.Equal(t, 4, n)
	assert.NoError(t, rdr.Err())
}

func TestUnmarshalJSON(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "model", Type: arrow.BinaryTypes.String},
		{Name: "sales", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	recordBuilder := array.NewRecordBuilder(mem, schema)
	defer recordBuilder.Release()

	jsondata := `{"region": "NY", "model": "3", "sales": 742.0, "extra": 1234}`

	err := recordBuilder.UnmarshalJSON([]byte(jsondata))
	assert.NoError(t, err)

	record := recordBuilder.NewRecord()
	defer record.Release()

	assert.NotNil(t, record)
}

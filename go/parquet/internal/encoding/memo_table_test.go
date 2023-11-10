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

package encoding_test

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/hashing"
	"github.com/apache/arrow/go/v14/parquet/internal/encoding"
	"github.com/stretchr/testify/suite"
)

type MemoTableTestSuite struct {
	suite.Suite
}

func TestMemoTable(t *testing.T) {
	suite.Run(t, new(MemoTableTestSuite))
}

func (m *MemoTableTestSuite) assertGetNotFound(table encoding.MemoTable, v interface{}) {
	_, ok := table.Get(v)
	m.False(ok)
}

func (m *MemoTableTestSuite) assertGet(table encoding.MemoTable, v interface{}, expected int) {
	idx, ok := table.Get(v)
	m.Equal(expected, idx)
	m.True(ok)
}

func (m *MemoTableTestSuite) assertGetOrInsert(table encoding.MemoTable, v interface{}, expected int) {
	idx, _, err := table.GetOrInsert(v)
	m.NoError(err)
	m.Equal(expected, idx)
}

func (m *MemoTableTestSuite) assertGetNullNotFound(table encoding.MemoTable) {
	_, ok := table.GetNull()
	m.False(ok)
}

func (m *MemoTableTestSuite) assertGetNull(table encoding.MemoTable, expected int) {
	idx, ok := table.GetNull()
	m.Equal(expected, idx)
	m.True(ok)
}

func (m *MemoTableTestSuite) assertGetOrInsertNull(table encoding.MemoTable, expected int) {
	idx, _ := table.GetOrInsertNull()
	m.Equal(expected, idx)
}

func (m *MemoTableTestSuite) TestInt64() {
	const (
		A int64 = 1234
		B int64 = 0
		C int64 = -98765321
		D int64 = 12345678901234
		E int64 = -1
		F int64 = 1
		G int64 = 9223372036854775807
		H int64 = -9223372036854775807 - 1
	)

	// table := encoding.NewInt64MemoTable(nil)
	table := hashing.NewInt64MemoTable(0)
	m.Zero(table.Size())
	m.assertGetNotFound(table, A)
	m.assertGetNullNotFound(table)
	m.assertGetOrInsert(table, A, 0)
	m.assertGetNotFound(table, B)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, D, 3)
	m.assertGetOrInsert(table, E, 4)
	m.assertGetOrInsertNull(table, 5)

	m.assertGet(table, A, 0)
	m.assertGetOrInsert(table, A, 0)
	m.assertGet(table, E, 4)
	m.assertGetOrInsert(table, E, 4)

	m.assertGetOrInsert(table, F, 6)
	m.assertGetOrInsert(table, G, 7)
	m.assertGetOrInsert(table, H, 8)

	m.assertGetOrInsert(table, G, 7)
	m.assertGetOrInsert(table, F, 6)
	m.assertGetOrInsertNull(table, 5)
	m.assertGetOrInsert(table, E, 4)
	m.assertGetOrInsert(table, D, 3)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, A, 0)

	const sz int = 9
	m.Equal(sz, table.Size())
	m.Panics(func() {
		values := make([]int32, sz)
		table.CopyValues(values)
	}, "should panic because wrong type")
	m.Panics(func() {
		values := make([]int64, sz-3)
		table.CopyValues(values)
	}, "should panic because out of bounds")

	{
		values := make([]int64, sz)
		table.CopyValues(values)
		m.Equal([]int64{A, B, C, D, E, 0, F, G, H}, values)
	}
	{
		const offset = 3
		values := make([]int64, sz-offset)
		table.CopyValuesSubset(offset, values)
		m.Equal([]int64{D, E, 0, F, G, H}, values)
	}
}

func (m *MemoTableTestSuite) TestFloat64() {
	const (
		A float64 = 0.0
		B float64 = 1.5
		C float64 = -0.1
	)
	var (
		D = math.Inf(1)
		E = -D
		F = math.NaN()                                       // uses Quiet NaN i.e. 0x7FF8000000000001
		G = math.Float64frombits(uint64(0x7FF0000000000001)) // test Signalling NaN
		H = math.Float64frombits(uint64(0xFFF7FFFFFFFFFFFF)) // other NaN bit pattern
	)

	// table := encoding.NewFloat64MemoTable(nil)
	table := hashing.NewFloat64MemoTable(0)
	m.Zero(table.Size())
	m.assertGetNotFound(table, A)
	m.assertGetNullNotFound(table)
	m.assertGetOrInsert(table, A, 0)
	m.assertGetNotFound(table, B)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, D, 3)
	m.assertGetOrInsert(table, E, 4)
	m.assertGetOrInsert(table, F, 5)
	m.assertGetOrInsert(table, G, 5)
	m.assertGetOrInsert(table, H, 5)

	m.assertGet(table, A, 0)
	m.assertGetOrInsert(table, A, 0)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, D, 3)
	m.assertGet(table, E, 4)
	m.assertGetOrInsert(table, E, 4)
	m.assertGet(table, F, 5)
	m.assertGetOrInsert(table, F, 5)
	m.assertGet(table, G, 5)
	m.assertGetOrInsert(table, G, 5)
	m.assertGet(table, H, 5)
	m.assertGetOrInsert(table, H, 5)

	m.Equal(6, table.Size())
	expected := []float64{A, B, C, D, E, F}
	m.Panics(func() {
		values := make([]int32, 6)
		table.CopyValues(values)
	}, "should panic because wrong type")
	m.Panics(func() {
		values := make([]float64, 3)
		table.CopyValues(values)
	}, "should panic because out of bounds")

	values := make([]float64, len(expected))
	table.CopyValues(values)
	for idx, ex := range expected {
		if math.IsNaN(ex) {
			m.True(math.IsNaN(values[idx]))
		} else {
			m.Equal(ex, values[idx])
		}
	}
}

func (m *MemoTableTestSuite) TestBinaryBasics() {
	const (
		A = ""
		B = "a"
		C = "foo"
		D = "bar"
		E = "\000"
		F = "\000trailing"
	)

	table := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary))
	defer table.Release()

	m.Zero(table.Size())
	m.assertGetNotFound(table, A)
	m.assertGetNullNotFound(table)
	m.assertGetOrInsert(table, A, 0)
	m.assertGetNotFound(table, B)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, D, 3)
	m.assertGetOrInsert(table, E, 4)
	m.assertGetOrInsert(table, F, 5)
	m.assertGetOrInsertNull(table, 6)

	m.assertGet(table, A, 0)
	m.assertGetOrInsert(table, A, 0)
	m.assertGet(table, B, 1)
	m.assertGetOrInsert(table, B, 1)
	m.assertGetOrInsert(table, C, 2)
	m.assertGetOrInsert(table, D, 3)
	m.assertGetOrInsert(table, E, 4)
	m.assertGet(table, F, 5)
	m.assertGetOrInsert(table, F, 5)
	m.assertGetNull(table, 6)
	m.assertGetOrInsertNull(table, 6)

	m.Equal(7, table.Size())
	m.Equal(17, table.ValuesSize())

	size := table.Size()
	{
		offsets := make([]int32, size+1)
		table.CopyOffsets(offsets)
		m.Equal([]int32{0, 0, 1, 4, 7, 8, 17, 17}, offsets)

		expectedValues := "afoobar"
		expectedValues += "\000"
		expectedValues += "\000"
		expectedValues += "trailing"
		values := make([]byte, 17)
		table.CopyValues(values)
		m.Equal(expectedValues, string(values))
	}

	{
		startOffset := 4
		offsets := make([]int32, size+1-int(startOffset))
		table.CopyOffsetsSubset(startOffset, offsets)
		m.Equal([]int32{0, 1, 10, 10}, offsets)

		expectedValues := ""
		expectedValues += "\000"
		expectedValues += "\000"
		expectedValues += "trailing"

		values := make([]byte, 10)
		table.CopyValuesSubset(startOffset, values)
		m.Equal(expectedValues, string(values))
	}

	{
		startOffset := 1
		values := make([]string, 0)
		table.VisitValues(startOffset, func(b []byte) {
			values = append(values, string(b))
		})
		m.Equal([]string{B, C, D, E, F, ""}, values)
	}
}

func (m *MemoTableTestSuite) TestBinaryEmpty() {
	table := encoding.NewBinaryMemoTable(memory.DefaultAllocator)
	defer table.Release()

	m.Zero(table.Size())
	offsets := make([]int32, 1)
	table.CopyOffsetsSubset(0, offsets)
	m.Equal(int32(0), offsets[0])
}

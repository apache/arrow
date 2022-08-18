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

package compute

import (
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

type ExecSpanItrSuite struct {
	ComputeInternalsTestSuite

	iter spanIterator
}

func (e *ExecSpanItrSuite) setupIterator(batch *ExecBatch, maxChunk int64) {
	var err error
	_, e.iter, err = iterateExecSpans(batch, maxChunk, true)
	e.NoError(err)
}

func (e *ExecSpanItrSuite) checkIteration(input *ExecBatch, chunksize int, exBatchSizes []int) {
	e.setupIterator(input, int64(chunksize))
	var (
		batch  *ExecSpan
		curPos int64
		pos    int64
		next   bool
	)

	for _, sz := range exBatchSizes {
		batch, pos, next = e.iter()
		e.True(next)
		e.EqualValues(sz, batch.Len)

		for j, val := range input.Values {
			switch val := val.(type) {
			case *ScalarDatum:
				e.Truef(scalar.Equals(batch.Values[j].Scalar, val.Value), "expected: %s\ngot: %s", val.Value, batch.Values[j].Scalar)
			case *ArrayDatum:
				arr := val.MakeArray()
				sl := array.NewSlice(arr, curPos, curPos+batch.Len)
				got := batch.Values[j].Array.MakeArray()

				e.Truef(array.Equal(sl, got), "expected: %s\ngot: %s", sl, got)

				got.Release()
				arr.Release()
				sl.Release()
			case *ChunkedDatum:
				carr := val.Value
				if batch.Len == 0 {
					e.Zero(carr.Len())
				} else {
					chkd := array.NewChunkedSlice(carr, curPos, curPos+batch.Len)
					defer chkd.Release()
					e.Len(chkd.Chunks(), 1)
					got := batch.Values[j].Array.MakeArray()
					defer got.Release()
					e.Truef(array.Equal(got, chkd.Chunk(0)), "expected: %s\ngot: %s", chkd.Chunk(0), got)
				}
			}
		}

		curPos += int64(sz)
		e.EqualValues(curPos, pos)
	}

	batch, pos, next = e.iter()
	e.Nil(batch)
	e.False(next)
	e.EqualValues(input.Len, pos)
}

func (e *ExecSpanItrSuite) TestBasics() {
	const length = 100

	arr1 := e.getInt32Arr(length, 0.1)
	defer arr1.Release()
	arr2 := e.getFloat64Arr(length, 0.1)
	defer arr2.Release()

	input := &ExecBatch{
		Len:    length,
		Values: []Datum{NewDatum(arr1), NewDatum(arr2), NewDatum(int32(3))},
	}
	defer func() {
		for _, v := range input.Values {
			v.Release()
		}
	}()

	e.Run("simple", func() {
		e.setupIterator(input, DefaultMaxChunkSize)

		batch, pos, next := e.iter()
		e.True(next)
		e.Len(batch.Values, 3)
		e.EqualValues(length, batch.Len)
		e.EqualValues(length, pos)

		in1 := input.Values[0].(*ArrayDatum).MakeArray()
		defer in1.Release()
		in2 := input.Values[1].(*ArrayDatum).MakeArray()
		defer in2.Release()
		out1 := batch.Values[0].Array.MakeArray()
		defer out1.Release()
		out2 := batch.Values[1].Array.MakeArray()
		defer out2.Release()

		e.Truef(array.Equal(in1, out1), "expected: %s\ngot: %s", in1, out1)
		e.Truef(array.Equal(in2, out2), "expected: %s\ngot: %s", in2, out2)
		e.True(scalar.Equals(input.Values[2].(*ScalarDatum).Value, batch.Values[2].Scalar), input.Values[2].(*ScalarDatum).Value, batch.Values[2].Scalar)

		_, pos, next = e.iter()
		e.EqualValues(length, pos)
		e.False(next)
	})

	e.Run("iterations", func() {
		e.checkIteration(input, 16, []int{16, 16, 16, 16, 16, 16, 4})
	})
}

func (e *ExecSpanItrSuite) TestInputValidation() {
	arr1 := e.getInt32Arr(10, 0.1)
	defer arr1.Release()
	arr2 := e.getInt32Arr(9, 0.1)
	defer arr2.Release()

	// length mismatch
	batch := &ExecBatch{
		Values: []Datum{&ArrayDatum{arr1.Data()}, &ArrayDatum{arr2.Data()}},
		Len:    10,
	}

	_, _, err := iterateExecSpans(batch, DefaultMaxChunkSize, true)
	e.ErrorIs(err, arrow.ErrInvalid)

	// swap order of input
	batch.Values = []Datum{&ArrayDatum{arr2.Data()}, &ArrayDatum{arr1.Data()}}

	_, _, err = iterateExecSpans(batch, DefaultMaxChunkSize, true)
	e.ErrorIs(err, arrow.ErrInvalid)

	batch.Values = []Datum{&ArrayDatum{arr1.Data()}}
	_, _, err = iterateExecSpans(batch, DefaultMaxChunkSize, true)
	e.NoError(err)
}

func (e *ExecSpanItrSuite) TestChunkedArrays() {
	arr1 := e.getInt32Chunked([]int64{0, 20, 10})
	defer arr1.Release()
	arr2 := e.getInt32Chunked([]int64{15, 15})
	defer arr2.Release()
	arr3 := e.getInt32Arr(30, 0.1)
	defer arr3.Release()

	batch := &ExecBatch{
		Values: []Datum{
			&ChunkedDatum{arr1}, &ChunkedDatum{arr2}, &ArrayDatum{arr3.Data()},
			NewDatum(int32(5)), NewDatum(scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean))},
		Len: 30,
	}

	e.checkIteration(batch, 10, []int{10, 5, 5, 10})
	e.checkIteration(batch, 20, []int{15, 5, 10})
	e.checkIteration(batch, 30, []int{15, 5, 10})
}

func (e *ExecSpanItrSuite) TestZeroLengthInput() {
	carr := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{})
	checkArgs := func(batch *ExecBatch) {
		_, itr, err := iterateExecSpans(batch, DefaultMaxChunkSize, true)
		e.NoError(err)
		itrSpan, _, next := itr()

		e.False(next)
		e.Nil(itrSpan)
	}

	input := &ExecBatch{Len: 0}

	// zero-length chunkedarray with zero chunks
	input.Values = []Datum{&ChunkedDatum{carr}}
	checkArgs(input)

	// zero-length array
	arr := e.getInt32Arr(0, 0.1)
	defer arr.Release()
	input.Values = []Datum{&ArrayDatum{arr.Data()}}
	checkArgs(input)

	// chunkedarray with single empty chunk
	carr = e.getInt32Chunked([]int64{0})
	input.Values = []Datum{&ChunkedDatum{carr}}
	checkArgs(input)
}

func TestExecSpanIterator(t *testing.T) {
	suite.Run(t, new(ExecSpanItrSuite))
}

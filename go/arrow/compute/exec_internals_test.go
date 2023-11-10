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

package compute

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/compute/exec"
	"github.com/apache/arrow/go/v14/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

type ComputeInternalsTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator

	execCtx ExecCtx
	ctx     *exec.KernelCtx
	rng     gen.RandomArrayGenerator
}

func (c *ComputeInternalsTestSuite) SetupTest() {
	c.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	c.rng = gen.NewRandomArrayGenerator(0, c.mem)

	c.resetCtx()
}

func (c *ComputeInternalsTestSuite) TearDownTest() {
	c.mem.AssertSize(c.T(), 0)
}

func (c *ComputeInternalsTestSuite) assertArrayEqual(expected, got arrow.Array) {
	c.Truef(array.Equal(expected, got), "expected: %s\ngot: %s", expected, got)
}

func (c *ComputeInternalsTestSuite) assertDatumEqual(expected arrow.Array, got Datum) {
	arr := got.(*ArrayDatum).MakeArray()
	defer arr.Release()
	c.Truef(array.Equal(expected, arr), "expected: %s\ngot: %s", expected, arr)
}

func (c *ComputeInternalsTestSuite) resetCtx() {
	c.execCtx = ExecCtx{Registry: GetFunctionRegistry(),
		ChunkSize: DefaultMaxChunkSize, PreallocContiguous: true}
	c.ctx = &exec.KernelCtx{Ctx: SetExecCtx(context.Background(), c.execCtx)}
}

func (c *ComputeInternalsTestSuite) getBoolArr(sz int64, trueprob, nullprob float64) arrow.Array {
	return c.rng.Boolean(sz, trueprob, nullprob)
}

func (c *ComputeInternalsTestSuite) getUint8Arr(sz int64, nullprob float64) arrow.Array {
	return c.rng.Uint8(sz, 0, 100, nullprob)
}

func (c *ComputeInternalsTestSuite) getInt32Arr(sz int64, nullprob float64) arrow.Array {
	return c.rng.Int32(sz, 0, 1000, nullprob)
}

func (c *ComputeInternalsTestSuite) getFloat64Arr(sz int64, nullprob float64) arrow.Array {
	return c.rng.Float64(sz, 0, 1000, nullprob)
}

func (c *ComputeInternalsTestSuite) getInt32Chunked(szs []int64) *arrow.Chunked {
	chunks := make([]arrow.Array, 0)
	for i, s := range szs {
		chunks = append(chunks, c.getInt32Arr(s, 0.1))
		defer chunks[i].Release()
	}
	return arrow.NewChunked(arrow.PrimitiveTypes.Int32, chunks)
}

func (c *ComputeInternalsTestSuite) assertValidityZeroExtraBits(data []byte, length, offset int) {
	bitExtent := ((offset + length + 7) / 8) * 8
	for i := offset + length; i < bitExtent; i++ {
		c.False(bitutil.BitIsSet(data, i))
	}
}

type PropagateNullsSuite struct {
	ComputeInternalsTestSuite
}

func (p *PropagateNullsSuite) TestUnknownNullCountWithNullsZeroCopies() {
	const length int = 16
	bitmap := [8]byte{254, 0, 0, 0, 0, 0, 0, 0}
	nulls := memory.NewBufferBytes(bitmap[:])

	output := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nil, nil}, nil, 0, 0)
	input := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nulls, nil}, nil, array.UnknownNullCount, 0)

	var outSpan exec.ArraySpan
	outSpan.SetMembers(output)
	batch := ExecBatch{Values: []Datum{NewDatum(input)}, Len: int64(length)}
	p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(&batch), &outSpan))
	p.Same(nulls, outSpan.Buffers[0].Owner)
	p.EqualValues(array.UnknownNullCount, outSpan.Nulls)
	p.Equal(9, int(outSpan.Len)-bitutil.CountSetBits(outSpan.Buffers[0].Buf, int(outSpan.Offset), int(outSpan.Len)))
}

func (p *PropagateNullsSuite) TestUnknownNullCountWithoutNulls() {
	const length int = 16
	bitmap := [8]byte{255, 255, 0, 0, 0, 0, 0, 0}
	nulls := memory.NewBufferBytes(bitmap[:])

	output := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nil, nil}, nil, 0, 0)
	input := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nulls, nil}, nil, array.UnknownNullCount, 0)

	var outSpan exec.ArraySpan
	outSpan.SetMembers(output)
	batch := ExecBatch{Values: []Datum{NewDatum(input)}, Len: int64(length)}
	p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(&batch), &outSpan))
	p.EqualValues(-1, outSpan.Nulls)
	p.Same(nulls, outSpan.Buffers[0].Owner)
}

func (p *PropagateNullsSuite) TestSetAllNulls() {
	const length int = 16
	checkSetAll := func(vals []Datum, prealloc bool) {
		// fresh bitmap with all 1s
		bitmapData := [2]byte{255, 255}
		preallocatedMem := memory.NewBufferBytes(bitmapData[:])

		output := &exec.ArraySpan{
			Type:  arrow.FixedWidthTypes.Boolean,
			Len:   int64(length),
			Nulls: array.UnknownNullCount,
		}

		if prealloc {
			output.Buffers[0].SetBuffer(preallocatedMem)
		}

		batch := &ExecBatch{Values: vals, Len: int64(length)}
		p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(batch), output))

		if prealloc {
			// ensure that the buffer object is the same when we pass preallocated
			// memory to it
			p.Same(preallocatedMem, output.Buffers[0].Owner)
		} else {
			defer output.Buffers[0].Owner.Release()
		}

		p.NotNil(output.Buffers[0].Buf)
		expected := [2]byte{0, 0}
		p.True(bytes.Equal(expected[:], output.Buffers[0].Buf))
	}

	var vals []Datum
	const trueProb float64 = 0.5
	p.Run("Null Scalar", func() {
		i32Val := scalar.MakeScalar(int32(3))
		vals = []Datum{NewDatum(i32Val), NewDatum(scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean))}
		checkSetAll(vals, true)
		checkSetAll(vals, false)

		arr := p.getBoolArr(int64(length), trueProb, 0)
		defer arr.Release()
		vals[0] = NewDatum(arr)
		defer vals[0].Release()
		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("one all null", func() {
		arrAllNulls := p.getBoolArr(int64(length), trueProb, 1)
		defer arrAllNulls.Release()
		arrHalf := p.getBoolArr(int64(length), trueProb, 0.5)
		defer arrHalf.Release()
		vals = []Datum{NewDatum(arrHalf), NewDatum(arrAllNulls)}
		defer vals[0].Release()
		defer vals[1].Release()

		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("one value is NullType", func() {
		nullarr := array.NewNull(length)
		arr := p.getBoolArr(int64(length), trueProb, 0)
		defer arr.Release()
		vals = []Datum{NewDatum(arr), NewDatum(nullarr)}
		defer vals[0].Release()
		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("Other scenarios", func() {
		// an all-null bitmap is zero-copied over, even though
		// there is a null-scalar earlier in the batch
		outSpan := &exec.ArraySpan{
			Type: arrow.FixedWidthTypes.Boolean,
			Len:  int64(length),
		}
		arrAllNulls := p.getBoolArr(int64(length), trueProb, 1)
		defer arrAllNulls.Release()

		batch := &ExecBatch{
			Values: []Datum{
				NewDatum(scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean)),
				NewDatum(arrAllNulls),
			},
			Len: int64(length),
		}
		defer batch.Values[1].Release()

		p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(batch), outSpan))
		p.Same(arrAllNulls.Data().Buffers()[0], outSpan.Buffers[0].Owner)
		outSpan.Buffers[0].Owner.Release()
	})
}

func (p *PropagateNullsSuite) TestSingleValueWithNulls() {
	const length int64 = 100
	arr := p.getBoolArr(length, 0.5, 0.5)
	defer arr.Release()

	checkSliced := func(offset int64, prealloc bool, outOffset int64) {
		// unaligned bitmap, zero copy not possible
		sliced := array.NewSlice(arr, offset, int64(arr.Len()))
		defer sliced.Release()
		vals := []Datum{NewDatum(sliced)}
		defer vals[0].Release()

		output := &exec.ArraySpan{
			Type:   arrow.FixedWidthTypes.Boolean,
			Len:    vals[0].Len(),
			Offset: outOffset,
		}

		batch := &ExecBatch{Values: vals, Len: vals[0].Len()}

		var preallocatedBitmap *memory.Buffer
		if prealloc {
			preallocatedBitmap = memory.NewResizableBuffer(p.mem)
			preallocatedBitmap.Resize(int(bitutil.BytesForBits(int64(sliced.Len()) + outOffset)))
			defer preallocatedBitmap.Release()
			output.Buffers[0].SetBuffer(preallocatedBitmap)
			output.Buffers[0].SelfAlloc = true
		} else {
			p.EqualValues(0, output.Offset)
		}

		p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(batch), output))
		if !prealloc {
			parentBuf := arr.Data().Buffers()[0]
			if offset == 0 {
				// validity bitmap same, no slice
				p.Same(parentBuf, output.Buffers[0].Owner)
			} else if offset%8 == 0 {
				// validity bitmap sliced
				p.NotSame(parentBuf, output.Buffers[0].Owner)
				p.Same(parentBuf, output.Buffers[0].Owner.Parent())
				defer output.Buffers[0].Owner.Release()
			} else {
				// new memory for offset not 0 mod 8
				p.NotSame(parentBuf, output.Buffers[0].Owner)
				p.Nil(output.Buffers[0].Owner.Parent())
				defer output.Buffers[0].Owner.Release()
			}
		} else {
			// preallocated, so check that the validity bitmap is unbothered
			p.Same(preallocatedBitmap, output.Buffers[0].Owner)
		}

		p.EqualValues(sliced.NullN(), output.UpdateNullCount())
		p.True(bitutil.BitmapEquals(
			sliced.NullBitmapBytes(), output.Buffers[0].Buf,
			int64(sliced.Data().Offset()), output.Offset, output.Len))
		p.assertValidityZeroExtraBits(output.Buffers[0].Buf, int(output.Len), int(output.Offset))
	}

	tests := []struct {
		offset, outoffset int64
		prealloc          bool
	}{
		{8, 0, false},
		{7, 0, false},
		{8, 0, true},
		{7, 0, true},
		{8, 4, true},
		{7, 4, true},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("off=%d,prealloc=%t,outoff=%d", tt.offset, tt.prealloc, tt.outoffset)
		p.Run(name, func() {
			checkSliced(tt.offset, tt.prealloc, tt.outoffset)
		})
	}
}

func (p *PropagateNullsSuite) TestIntersectsNulls() {
	const length = 16
	var (
		// 0b01111111 0b11001111
		bitmap1 = [8]byte{127, 207, 0, 0, 0, 0, 0, 0}
		// 0b11111110 0b01111111
		bitmap2 = [8]byte{254, 127, 0, 0, 0, 0, 0, 0}
		// 0b11101111 0b11111110
		bitmap3 = [8]byte{239, 254, 0, 0, 0, 0, 0, 0}
	)

	arr1 := array.NewData(arrow.FixedWidthTypes.Boolean, length,
		[]*memory.Buffer{memory.NewBufferBytes(bitmap1[:]), nil}, nil, array.UnknownNullCount, 0)
	arr2 := array.NewData(arrow.FixedWidthTypes.Boolean, length,
		[]*memory.Buffer{memory.NewBufferBytes(bitmap2[:]), nil}, nil, array.UnknownNullCount, 0)
	arr3 := array.NewData(arrow.FixedWidthTypes.Boolean, length,
		[]*memory.Buffer{memory.NewBufferBytes(bitmap3[:]), nil}, nil, array.UnknownNullCount, 0)

	checkCase := func(vals []Datum, exNullCount int, exBitmap []byte, prealloc bool, outoffset int) {
		batch := &ExecBatch{Values: vals, Len: length}

		output := &exec.ArraySpan{Type: arrow.FixedWidthTypes.Boolean, Len: length}

		var nulls *memory.Buffer
		if prealloc {
			// make the buffer one byte bigger so we can have non-zero offsets
			nulls = memory.NewResizableBuffer(p.mem)
			nulls.Resize(3)
			defer nulls.Release()
			output.Buffers[0].SetBuffer(nulls)
			output.Buffers[0].SelfAlloc = true
		} else {
			// non-zero output offset not permitted unless output memory is preallocated
			p.Equal(0, outoffset)
		}

		output.Offset = int64(outoffset)

		p.NoError(propagateNulls(p.ctx, ExecSpanFromBatch(batch), output))

		// preallocated memory used
		if prealloc {
			p.Same(nulls, output.Buffers[0].Owner)
		} else {
			defer output.Buffers[0].Owner.Release()
		}

		p.EqualValues(array.UnknownNullCount, output.Nulls)
		p.EqualValues(exNullCount, output.UpdateNullCount())

		p.True(bitutil.BitmapEquals(exBitmap, output.Buffers[0].Buf, 0, output.Offset, length))
		p.assertValidityZeroExtraBits(output.Buffers[0].Buf, int(output.Len), int(output.Offset))
	}

	p.Run("0b01101110 0b01001110", func() {
		// 0b01101110 0b01001110
		expected := [2]byte{110, 78}
		checkCase([]Datum{NewDatum(arr1), NewDatum(arr2), NewDatum(arr3)}, 7, expected[:], false, 0)
		checkCase([]Datum{NewDatum(arr1), NewDatum(arr2), NewDatum(arr3)}, 7, expected[:], true, 0)
		checkCase([]Datum{NewDatum(arr1), NewDatum(arr2), NewDatum(arr3)}, 7, expected[:], true, 4)
	})

	p.Run("0b01111110 0b01001111", func() {
		expected := [2]byte{126, 79}
		checkCase([]Datum{NewDatum(arr1), NewDatum(arr2)}, 5, expected[:], false, 0)
		checkCase([]Datum{NewDatum(arr1), NewDatum(arr2)}, 5, expected[:], true, 4)
	})
}

func TestComputeInternals(t *testing.T) {
	suite.Run(t, new(PropagateNullsSuite))
}

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
		batch  exec.ExecSpan
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
	e.Zero(batch)
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
		e.Zero(itrSpan)
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

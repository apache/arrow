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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

type ComputeInternalsTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator

	execCtx *ExecCtx
	ctx     *KernelCtx
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

func (c *ComputeInternalsTestSuite) resetCtx() {
	c.execCtx = &ExecCtx{Alloc: c.mem}
	c.ctx = &KernelCtx{ExecCtx: c.execCtx}
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

type PropagateNullsSuite struct {
	ComputeInternalsTestSuite
}

func (p *PropagateNullsSuite) TestUnknownNullCountWithNullsZeroCopies() {
	const length int = 16
	bitmap := [8]byte{254, 0, 0, 0, 0, 0, 0, 0}
	nulls := memory.NewBufferBytes(bitmap[:])

	output := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nil, nil}, nil, 0, 0)
	input := array.NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nulls, nil}, nil, array.UnknownNullCount, 0)

	var outSpan ArraySpan
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

	var outSpan ArraySpan
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

		output := &ArraySpan{
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

		arr := p.rng.Boolean(int64(length), trueProb, 0)
		defer arr.Release()
		vals[0] = NewDatum(arr)
		defer vals[0].Release()
		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("one all null", func() {
		arrAllNulls := p.rng.Boolean(int64(length), trueProb, 1)
		defer arrAllNulls.Release()
		arrHalf := p.rng.Boolean(int64(length), trueProb, 0.5)
		defer arrHalf.Release()
		vals = []Datum{NewDatum(arrHalf), NewDatum(arrAllNulls)}
		defer vals[0].Release()
		defer vals[1].Release()

		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("one value is NullType", func() {
		nullarr := array.NewNull(length)
		arr := p.rng.Boolean(int64(length), trueProb, 0)
		defer arr.Release()
		vals = []Datum{NewDatum(arr), NewDatum(nullarr)}
		defer vals[0].Release()
		checkSetAll(vals, true)
		checkSetAll(vals, false)
	})

	p.Run("Other scenarios", func() {
		// an all-null bitmap is zero-copied over, even though
		// there is a null-scalar earlier in the batch
		outSpan := &ArraySpan{
			Type: arrow.FixedWidthTypes.Boolean,
			Len:  int64(length),
		}
		arrAllNulls := p.rng.Boolean(int64(length), trueProb, 1)
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
	arr := p.rng.Boolean(length, 0.5, 0.5)
	defer arr.Release()

	checkSliced := func(offset int64, prealloc bool, outOffset int64) {
		// unaligned bitmap, zero copy not possible
		sliced := array.NewSlice(arr, offset, int64(arr.Len()))
		defer sliced.Release()
		vals := []Datum{NewDatum(sliced)}
		defer vals[0].Release()

		output := &ArraySpan{
			Type:   arrow.FixedWidthTypes.Boolean,
			Len:    vals[0].Len(),
			Offset: outOffset,
		}

		batch := &ExecBatch{Values: vals, Len: vals[0].Len()}

		var preallocatedBitmap *memory.Buffer
		if prealloc {
			preallocatedBitmap = memory.NewResizableBuffer(p.mem)
			defer preallocatedBitmap.Release()
			preallocatedBitmap.Resize(int(bitutil.BytesForBits(int64(sliced.Len()) + outOffset)))
			output.Buffers[0].SetBuffer(preallocatedBitmap)
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

func TestComputeInternals(t *testing.T) {
	suite.Run(t, new(PropagateNullsSuite))
}

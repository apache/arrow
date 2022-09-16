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

package kernels

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/internal/bitutils"
)

type NullSelectionBehavior int8

const (
	DropNulls NullSelectionBehavior = iota
	EmitNulls
)

type FilterOptions struct {
	NullSelection NullSelectionBehavior `compute:"null_selection_behavior"`
}

func (FilterOptions) TypeName() string { return "FilterOptions" }

type FilterState = FilterOptions

type TakeOptions struct {
	BoundsCheck bool
}

func (TakeOptions) TypeName() string { return "TakeOptions" }

type TakeState = TakeOptions

func getFilterOutputSize(filter *exec.ArraySpan, nullSelection NullSelectionBehavior) (size int64) {
	if filter.MayHaveNulls() {
		counter := bitutils.NewBinaryBitBlockCounter(filter.Buffers[1].Buf,
			filter.Buffers[0].Buf, filter.Offset, filter.Offset, filter.Len)

		pos := int64(0)
		if nullSelection == EmitNulls {
			for pos < filter.Len {
				block := counter.NextOrNotWord()
				size += int64(block.Popcnt)
				pos += int64(block.Len)
			}
		} else {
			for pos < filter.Len {
				block := counter.NextAndWord()
				size += int64(block.Popcnt)
				pos += int64(block.Len)
			}
		}
		return
	}

	// filter has no nulls, so we can just use CountSetBits
	return int64(bitutil.CountSetBits(filter.Buffers[1].Buf, int(filter.Offset), int(filter.Len)))
}

func preallocateData(ctx *exec.KernelCtx, length int64, bitWidth int, allocateValidity bool, out *exec.ExecResult) {
	out.Len = length
	if allocateValidity {
		out.Buffers[0].WrapBuffer(ctx.AllocateBitmap(length))
	}
	if bitWidth == 1 {
		out.Buffers[1].WrapBuffer(ctx.AllocateBitmap(length))
	} else {
		out.Buffers[1].WrapBuffer(ctx.Allocate(int(length) * (bitWidth / 8)))
	}
}

type writeFiltered interface {
	OutPos() int
	WriteValue(int64)
	WriteValueSegment(int64, int64)
	WriteNull()
}

type dropNullCounter struct {
	dataCounter         bitutils.BitBlockCounter
	dataValidityCounter bitutils.BinaryBitBlockCounter
	hasValidity         bool
}

func newDropNullCounter(validity []byte, data []byte, offset int64, length int64) *dropNullCounter {
	return &dropNullCounter{
		dataCounter:         *bitutils.NewBitBlockCounter(data, offset, length),
		dataValidityCounter: *bitutils.NewBinaryBitBlockCounter(data, validity, offset, offset, length),
		hasValidity:         len(validity) > 0,
	}
}

func (n *dropNullCounter) NextBlock() bitutils.BitBlockCount {
	if n.hasValidity {
		// filter is true AND not null
		return n.dataValidityCounter.NextAndWord()
	}
	return n.dataCounter.NextWord()
}

func primitiveFilterImpl(wr writeFiltered, values *exec.ArraySpan, filter *exec.ArraySpan, nullSelection NullSelectionBehavior, out *exec.ExecResult) {
	var (
		valuesIsValid = values.Buffers[0].Buf
		filterIsValid = filter.Buffers[0].Buf
		filterData    = filter.Buffers[1].Buf
		outIsValid    = out.Buffers[0].Buf
	)

	if filter.Nulls == 0 && values.Nulls == 0 {
		// fast filter path when values and filters have no nulls
		bitutils.VisitSetBitRuns(filterData, filter.Offset, values.Len,
			func(pos, length int64) error {
				wr.WriteValueSegment(pos, length)
				return nil
			})
		return
	}

	var (
		dropNulls          = newDropNullCounter(filterIsValid, filterData, filter.Offset, values.Len)
		dataCounter        = bitutils.NewOptionalBitBlockCounter(valuesIsValid, values.Offset, values.Len)
		filterValidCounter = bitutils.NewOptionalBitBlockCounter(filterIsValid, filter.Offset, values.Len)
		writeNotNull       = func(idx int64) {
			bitutil.SetBit(outIsValid, int(out.Offset)+wr.OutPos())
			wr.WriteValue(idx)
		}
		writeMaybeNull = func(idx int64) {
			bitutil.SetBitTo(outIsValid, int(out.Offset)+wr.OutPos(),
				bitutil.BitIsSet(valuesIsValid, int(values.Offset+idx)))
			wr.WriteValue(idx)
		}
		inPos int64
	)

	for inPos < values.Len {
		filterBlock := dropNulls.NextBlock()
		filterValidBlock := filterValidCounter.NextWord()
		dataBlock := dataCounter.NextWord()

		switch {
		case filterBlock.AllSet() && dataBlock.AllSet():
			// faster path: all values in block are included and not null
			bitutil.SetBitsTo(outIsValid, out.Offset+int64(wr.OutPos()), int64(filterBlock.Len), true)
			wr.WriteValueSegment(inPos, int64(filterBlock.Len))
			inPos += int64(filterBlock.Len)
		case filterBlock.AllSet():
			// faster: all values are selected, but some are null
			// batch copy bits from values validity bitmap to output validity bitmap
			bitutil.CopyBitmap(valuesIsValid, int(values.Offset+inPos), int(filterBlock.Len),
				outIsValid, int(out.Offset)+wr.OutPos())
			wr.WriteValueSegment(inPos, int64(filterBlock.Len))
			inPos += int64(filterBlock.Len)
		case filterBlock.NoneSet() && nullSelection == DropNulls:
			// for this exceedingly common case in low-selectivity filters
			// we can skip further analysis of the data and move onto the next block
			inPos += int64(filterBlock.Len)
		default:
			// some filter values are false or null
			if dataBlock.AllSet() {
				// no values are null
				if filterValidBlock.AllSet() {
					// filter is non-null but some values are false
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							writeNotNull(inPos)
						}
						inPos++
					}
				} else if nullSelection == DropNulls {
					// if any values are selected, they ARE NOT  null
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterIsValid, int(filter.Offset+inPos)) &&
							bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							writeNotNull(inPos)
						}
						inPos++
					}
				} else { // nullselect == EmitNulls
					// data values in this block are not null
					for i := 0; i < int(filterBlock.Len); i++ {
						isValid := bitutil.BitIsSet(filterIsValid, int(filter.Offset+inPos))
						if isValid && bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							// filter slot is non-null and set
							writeNotNull(inPos)
						} else if !isValid {
							// filter slot is null, so we have a null in the output
							bitutil.ClearBit(outIsValid, int(out.Offset)+wr.OutPos())
							wr.WriteNull()
						}
						inPos++
					}
				}
			} else { // !dataBlock.AllSet()
				// some values are null
				if filterValidBlock.AllSet() {
					// filter is non-null but some values are false
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							writeMaybeNull(inPos)
						}
						inPos++
					}
				} else if nullSelection == DropNulls {
					// if any values are selected they ARE NOT null
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterIsValid, int(filter.Offset+inPos)) && bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							writeMaybeNull(inPos)
						}
						inPos++
					}
				} else { // nullselect == emitnulls
					// Data values in this block are not null
					for i := 0; i < int(filterBlock.Len); i++ {
						isValid := bitutil.BitIsSet(filterIsValid, int(filter.Offset+inPos))
						if isValid && bitutil.BitIsSet(filterData, int(filter.Offset+inPos)) {
							// filter slot is non-null and set
							writeMaybeNull(inPos)
						} else if !isValid {
							// filter slot is null, so we have a null in the output
							bitutil.ClearBit(outIsValid, int(out.Offset)+wr.OutPos())
							wr.WriteNull()
						}
						inPos++
					}
				}
			}
		}
	}
}

type filterWriter[T exec.UintTypes] struct {
	outPosition  int
	outOffset    int
	valuesOffset int
	valuesData   []T
	outData      []T
}

func (f *filterWriter[T]) OutPos() int { return f.outPosition }

func (f *filterWriter[T]) WriteValue(inPos int64) {
	f.outData[f.outPosition] = f.valuesData[inPos]
	f.outPosition++
}

func (f *filterWriter[T]) WriteValueSegment(inStart, length int64) {
	copy(f.outData[f.outPosition:], f.valuesData[inStart:inStart+length])
	f.outPosition += int(length)
}

func (f *filterWriter[T]) WriteNull() {
	var z T
	f.outData[f.outPosition] = z
	f.outPosition++
}

type boolFilterWriter struct {
	outPosition  int
	outOffset    int
	valuesOffset int
	valuesData   []byte
	outData      []byte
}

func (b *boolFilterWriter) OutPos() int { return b.outPosition }

func (b *boolFilterWriter) WriteValue(inPos int64) {
	bitutil.SetBitTo(b.outData, b.outOffset+b.outPosition,
		bitutil.BitIsSet(b.valuesData, b.valuesOffset+int(inPos)))
}

func (b *boolFilterWriter) WriteValueSegment(inStart, length int64) {
	bitutil.CopyBitmap(b.valuesData, b.valuesOffset+int(inStart), int(length),
		b.outData, b.outOffset+b.outPosition)
	b.outPosition += int(length)
}

func (b *boolFilterWriter) WriteNull() {
	bitutil.ClearBit(b.outData, b.outOffset+b.outPosition)
	b.outPosition++
}

func PrimitiveFilter(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		values        = &batch.Values[0].Array
		filter        = &batch.Values[1].Array
		nullSelection = ctx.State.(FilterState).NullSelection
		outputLength  = getFilterOutputSize(filter, nullSelection)
	)

	// the output precomputed null count is unknown except in the narrow
	// condition that all the values are non-null and the filter will not
	// cause any new nulls to be created
	if values.Nulls == 0 && (nullSelection == DropNulls || filter.Nulls == 0) {
		out.Nulls = 0
	} else {
		out.Nulls = array.UnknownNullCount
	}

	// when neither the values nor filter is known to have any nulls,
	// we will elect the optimized ExecNonNull path where there is no
	// need to populate a validity bitmap.
	allocateValidity := values.Nulls != 0 || filter.Nulls != 0
	bitWidth := values.Type.(arrow.FixedWidthDataType).BitWidth()
	preallocateData(ctx, outputLength, bitWidth, allocateValidity, out)

	var wr writeFiltered
	switch bitWidth {
	case 1:
		wr = &boolFilterWriter{
			outOffset:    int(out.Offset),
			valuesOffset: int(values.Offset),
			outData:      out.Buffers[1].Buf,
			valuesData:   values.Buffers[1].Buf,
		}
	case 8:
		wr = &filterWriter[uint8]{
			outOffset:    int(out.Offset),
			valuesOffset: int(values.Offset),
			outData:      exec.GetSpanValues[uint8](out, 1),
			valuesData:   exec.GetSpanValues[uint8](values, 1),
		}
	case 16:
		wr = &filterWriter[uint16]{
			outOffset:    int(out.Offset),
			valuesOffset: int(values.Offset),
			outData:      exec.GetSpanValues[uint16](out, 1),
			valuesData:   exec.GetSpanValues[uint16](values, 1),
		}
	case 32:
		wr = &filterWriter[uint32]{
			outOffset:    int(out.Offset),
			valuesOffset: int(values.Offset),
			outData:      exec.GetSpanValues[uint32](out, 1),
			valuesData:   exec.GetSpanValues[uint32](values, 1),
		}
	case 64:
		wr = &filterWriter[uint64]{
			outOffset:    int(out.Offset),
			valuesOffset: int(values.Offset),
			outData:      exec.GetSpanValues[uint64](out, 1),
			valuesData:   exec.GetSpanValues[uint64](values, 1),
		}
	default:
		return fmt.Errorf("%w: invalid values bit width", arrow.ErrType)
	}

	primitiveFilterImpl(wr, values, filter, nullSelection, out)
	return nil
}

func primitiveTakeImpl[IdxT exec.UintTypes, ValT exec.IntTypes](values, indices *exec.ArraySpan, out *exec.ExecResult) {
	var (
		valuesData    = exec.GetSpanValues[ValT](values, 1)
		valuesIsValid = values.Buffers[0].Buf
		valuesOffset  = values.Offset

		indicesData    = exec.GetSpanValues[IdxT](indices, 1)
		indicesIsValid = indices.Buffers[0].Buf
		indicesOffset  = indices.Offset

		outData    = exec.GetSpanValues[ValT](out, 1)
		outIsValid = out.Buffers[0].Buf
		outOffset  = out.Offset
	)

	pos, validCount := int64(0), int64(0)
	if values.Nulls == 0 && indices.Nulls == 0 {
		// values and indices are both never null
		// this means we didn't allocate the validity bitmap
		// and can simplify everything
		for i, idx := range indicesData {
			outData[i] = valuesData[idx]
		}
		out.Nulls = 0
		return
	}

	indicesBitCounter := bitutils.NewOptionalBitBlockCounter(indicesIsValid, indicesOffset, indices.Len)
	for pos < indices.Len {
		block := indicesBitCounter.NextBlock()
		if values.Nulls == 0 {
			// values are never null, so things are easier
			validCount += int64(block.Popcnt)
			if block.AllSet() {
				// fastest path: neither values nor index nulls
				bitutil.SetBitsTo(outIsValid, outOffset+pos, int64(block.Len), true)
				for i := 0; i < int(block.Len); i++ {
					outData[pos] = valuesData[indicesData[pos]]
					pos++
				}
			} else if block.Popcnt > 0 {
				// slow path: some indices but not all are null
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(indicesIsValid, int(indicesOffset+pos)) {
						// index is not null
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						outData[pos] = valuesData[indicesData[pos]]
					}
					pos++
				}
			} else {
				pos += int64(block.Len)
			}
		} else {
			// values have nulls, so we must do random access into the values bitmap
			if block.AllSet() {
				// faster path: indices are not null but values may be
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(valuesIsValid, int(valuesOffset)+int(indicesData[pos])) {
						// value is not null
						outData[pos] = valuesData[indicesData[pos]]
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						validCount++
					}
					pos++
				}
			} else if block.Popcnt > 0 {
				// slow path: some but not all indices are null. since we
				// are doing random access in general we have to check the
				// value nullness one by one
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(indicesIsValid, int(indicesOffset+pos)) &&
						bitutil.BitIsSet(valuesIsValid, int(valuesOffset)+int(indicesData[pos])) {
						// index is not null && value is not null
						outData[pos] = valuesData[indicesData[pos]]
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						validCount++
					}
					pos++
				}
			} else {
				pos += int64(block.Len)
			}
		}
	}

	out.Nulls = out.Len - validCount
}

func booleanTakeImpl[IdxT exec.UintTypes](values, indices *exec.ArraySpan, out *exec.ExecResult) {
	var (
		valuesData    = values.Buffers[1].Buf
		valuesIsValid = values.Buffers[0].Buf
		valuesOffset  = values.Offset

		indicesData    = exec.GetSpanValues[IdxT](indices, 1)
		indicesIsValid = indices.Buffers[0].Buf
		indicesOffset  = indices.Offset

		outData    = out.Buffers[1].Buf
		outIsValid = out.Buffers[0].Buf
		outOffset  = out.Offset
	)

	placeDataBit := func(loc int64, index IdxT) {
		bitutil.SetBitTo(outData, int(outOffset+loc), bitutil.BitIsSet(valuesData, int(valuesOffset)+int(index)))
	}

	pos, validCount := int64(0), int64(0)
	if values.Nulls == 0 && indices.Nulls == 0 {
		// values and indices are both never null
		// this means we didn't allocate the validity bitmap
		// and can simplify everything
		for i, idx := range indicesData {
			placeDataBit(int64(i), idx)
		}
		out.Nulls = 0
		return
	}

	indicesBitCounter := bitutils.NewOptionalBitBlockCounter(indicesIsValid, indicesOffset, indices.Len)
	for pos < indices.Len {
		block := indicesBitCounter.NextBlock()
		if values.Nulls == 0 {
			// values are never null so things are easier
			validCount += int64(block.Popcnt)
			if block.AllSet() {
				// fastest path: neither values nor index nulls
				bitutil.SetBitsTo(outIsValid, outOffset+pos, int64(block.Len), true)
				for i := 0; i < int(block.Len); i++ {
					placeDataBit(pos, indicesData[pos])
					pos++
				}
			} else if block.Popcnt > 0 {
				// slow path: some but not all indices are null
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(indicesIsValid, int(indicesOffset+pos)) {
						// index is not null
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						placeDataBit(pos, indicesData[pos])
					}
					pos++
				}
			} else {
				pos += int64(block.Len)
			}
		} else {
			// values have nulls so we must do random access into the values bitmap
			if block.AllSet() {
				// faster path: indices are not null but values may be
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(valuesIsValid, int(valuesOffset)+int(indicesData[pos])) {
						// value is not null
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						placeDataBit(pos, indicesData[pos])
						validCount++
					}
					pos++
				}
			} else if block.Popcnt > 0 {
				// slow path: some but not all indices are null.
				// we have to check the values one by one
				for i := 0; i < int(block.Len); i++ {
					if bitutil.BitIsSet(indicesIsValid, int(indicesOffset+pos)) &&
						bitutil.BitIsSet(valuesIsValid, int(valuesOffset)+int(indicesData[pos])) {
						placeDataBit(pos, indicesData[pos])
						bitutil.SetBit(outIsValid, int(outOffset+pos))
						validCount++
					}
					pos++
				}
			} else {
				pos += int64(block.Len)
			}
		}
	}
	out.Nulls = out.Len - validCount
}

func booleanTakeDispatch(values, indices *exec.ArraySpan, out *exec.ExecResult) error {
	switch indices.Type.(arrow.FixedWidthDataType).Bytes() {
	case 1:
		booleanTakeImpl[uint8](values, indices, out)
	case 2:
		booleanTakeImpl[uint16](values, indices, out)
	case 4:
		booleanTakeImpl[uint32](values, indices, out)
	case 8:
		booleanTakeImpl[uint64](values, indices, out)
	default:
		return fmt.Errorf("%w: invalid indices byte width", arrow.ErrIndex)
	}
	return nil
}

func takeIdxDispatch[ValT exec.IntTypes](values, indices *exec.ArraySpan, out *exec.ExecResult) error {
	switch indices.Type.(arrow.FixedWidthDataType).Bytes() {
	case 1:
		primitiveTakeImpl[uint8, ValT](values, indices, out)
	case 2:
		primitiveTakeImpl[uint16, ValT](values, indices, out)
	case 4:
		primitiveTakeImpl[uint32, ValT](values, indices, out)
	case 8:
		primitiveTakeImpl[uint64, ValT](values, indices, out)
	default:
		return fmt.Errorf("%w: invalid indices byte width", arrow.ErrIndex)
	}
	return nil
}

func PrimitiveTake(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		values  = &batch.Values[0].Array
		indices = &batch.Values[1].Array
	)

	if ctx.State.(TakeState).BoundsCheck {
		if err := checkIndexBounds(indices, uint64(values.Len)); err != nil {
			return err
		}
	}

	bitWidth := values.Type.(arrow.FixedWidthDataType).BitWidth()
	allocateValidity := values.Nulls != 0 || indices.Nulls != 0
	preallocateData(ctx, indices.Len, bitWidth, allocateValidity, out)

	switch bitWidth {
	case 1:
		return booleanTakeDispatch(values, indices, out)
	case 8:
		return takeIdxDispatch[int8](values, indices, out)
	case 16:
		return takeIdxDispatch[int16](values, indices, out)
	case 32:
		return takeIdxDispatch[int32](values, indices, out)
	case 64:
		return takeIdxDispatch[int64](values, indices, out)
	default:
		return fmt.Errorf("%w: invalid values byte width for take", arrow.ErrInvalid)
	}
}

func NullTake(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	if ctx.State.(TakeState).BoundsCheck {
		if err := checkIndexBounds(&batch.Values[1].Array, uint64(batch.Values[0].Array.Len)); err != nil {
			return err
		}
	}

	// batch.length doesn't take into account the take indices
	out.Len = batch.Values[1].Array.Len
	out.Type = arrow.Null
	return nil
}

func NullFilter(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	outputLength := getFilterOutputSize(&batch.Values[1].Array, ctx.State.(FilterState).NullSelection)
	out.Len = outputLength
	out.Type = arrow.Null
	return nil
}

func filterExec(ctx *exec.KernelCtx, outputLen int64, values, selection *exec.ArraySpan, out *exec.ExecResult, visitValid func(idx int64) error, visitNull func() error) error {
	var (
		nullSelection = ctx.State.(FilterState).NullSelection
		filterData    = selection.Buffers[1].Buf
		filterIsValid = selection.Buffers[0].Buf
		filterOffset  = selection.Offset

		// we use 3 block counters for fast scanning
		//
		// values valid counter: for values null/not-null
		// filter valid counter: for filter null/not-null
		// filter counter: for filter true/false
		valuesIsValid      = bitutil.OptionalBitIndexer{Bitmap: values.Buffers[0].Buf, Offset: int(values.Offset)}
		valuesValidCounter = bitutils.NewOptionalBitBlockCounter(values.Buffers[0].Buf, values.Offset, values.Len)
		filterValidCounter = bitutils.NewOptionalBitBlockCounter(filterIsValid, filterOffset, selection.Len)
		filterCounter      = bitutils.NewBitBlockCounter(filterData, filterOffset, selection.Len)
		inPos              int64

		validityBuilder = validityBuilder{mem: exec.GetAllocator(ctx.Ctx)}
	)

	validityBuilder.Reserve(outputLen)

	appendNotNull := func(idx int64) error {
		validityBuilder.UnsafeAppend(true)
		return visitValid(idx)
	}

	appendNull := func() error {
		validityBuilder.UnsafeAppend(false)
		return visitNull()
	}

	appendMaybeNull := func(idx int64) error {
		if valuesIsValid.GetBit(int(idx)) {
			return appendNotNull(idx)
		}
		return appendNull()
	}

	for inPos < selection.Len {
		filterValidBlock := filterValidCounter.NextWord()
		valuesValidBlock := valuesValidCounter.NextWord()
		filterBlock := filterCounter.NextWord()

		switch {
		case filterBlock.NoneSet() && nullSelection == DropNulls:
			// for this exceedingly common case in low-selectivity filters
			// we can skip further analysis of the data and move onto the next block
			inPos += int64(filterBlock.Len)
		case filterValidBlock.AllSet():
			// simpler path: no filter values are null
			if filterBlock.AllSet() {
				// fastest path, filter values are all true and not null
				if valuesValidBlock.AllSet() {
					// values aren't null either
					validityBuilder.UnsafeAppendN(int64(filterBlock.Len), true)
					for i := 0; i < int(filterBlock.Len); i++ {
						if err := visitValid(inPos); err != nil {
							return err
						}
						inPos++
					}
				} else {
					// some values are null in this block
					for i := 0; i < int(filterBlock.Len); i++ {
						if err := appendMaybeNull(inPos); err != nil {
							return err
						}
						inPos++
					}
				}
			} else { // !filterBlock.AllSet()
				// some filter values are false, but all not null
				if valuesValidBlock.AllSet() {
					// all the values are not-null, so we can skip null checking for them
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
							if err := appendNotNull(inPos); err != nil {
								return err
							}
						}
						inPos++
					}
				} else {
					// some of the values in the block are null
					// gotta check each one :(
					for i := 0; i < int(filterBlock.Len); i++ {
						if bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
							if err := appendMaybeNull(inPos); err != nil {
								return err
							}
						}
						inPos++
					}
				}
			}
		default:
			// !filterValidBlock.AllSet()
			// some filter values are null, so we have to handle drop
			// versus emit null
			if nullSelection == DropNulls {
				// filter null values are treated as false
				for i := 0; i < int(filterBlock.Len); i++ {
					if bitutil.BitIsSet(filterIsValid, int(filterOffset+inPos)) &&
						bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
						if err := appendMaybeNull(inPos); err != nil {
							return err
						}
					}
					inPos++
				}
			} else {
				// filter null values are appended to output as null
				// whether the value in the corresponding slot is valid
				// or not
				var err error
				for i := 0; i < int(filterBlock.Len); i++ {
					filterNotNull := bitutil.BitIsSet(filterIsValid, int(filterOffset+inPos))
					if filterNotNull && bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
						err = appendMaybeNull(inPos)
					} else if !filterNotNull {
						// emit null case
						err = appendNull()
					}
					if err != nil {
						return err
					}
					inPos++
				}
			}
		}
	}

	out.Len = int64(validityBuilder.bitLength)
	out.Nulls = int64(validityBuilder.falseCount)
	out.Buffers[0].WrapBuffer(validityBuilder.Finish())
	return nil
}

func binaryFilterNonNull[OffsetT int32 | int64](ctx *exec.KernelCtx, values, filter *exec.ArraySpan, outputLen int64, nullSelection NullSelectionBehavior, out *exec.ExecResult) error {
	var (
		offsetBuilder = newBufferBuilder[OffsetT](exec.GetAllocator(ctx.Ctx))
		dataBuilder   = newBufferBuilder[uint8](exec.GetAllocator(ctx.Ctx))
		rawOffsets    = exec.GetSpanOffsets[OffsetT](values, 1)
		rawData       = values.Buffers[2].Buf
	)

	offsetBuilder.reserve(int(outputLen) + 1)
	// get a rough estimate and pre-size the data builder
	if values.Len > 0 {
		meanValueLength := float64(rawOffsets[values.Len]-rawOffsets[0]) / float64(values.Len)
		dataBuilder.reserve(int(meanValueLength * float64(outputLen)))
	}

	spaceAvail := dataBuilder.cap()
	var offset OffsetT
	filterData := filter.Buffers[1].Buf

	err := bitutils.VisitSetBitRuns(filterData, filter.Offset, filter.Len,
		func(pos, length int64) error {
			start, end := rawOffsets[pos], rawOffsets[pos+length]
			// bulk-append raw data
			runDataBytes := (end - start)
			if runDataBytes > OffsetT(spaceAvail) {
				dataBuilder.reserve(int(runDataBytes))
				spaceAvail = dataBuilder.cap() - dataBuilder.len()
			}
			dataBuilder.unsafeAppendSlice(rawData[start:end])
			spaceAvail -= int(runDataBytes)
			curOffset := start
			for i := int64(0); i < length; i++ {
				offsetBuilder.unsafeAppend(offset)
				offset += rawOffsets[i+pos+1] - curOffset
				curOffset = rawOffsets[i+pos+1]
			}
			return nil
		})

	if err != nil {
		return err
	}

	offsetBuilder.unsafeAppend(offset)
	out.Len = outputLen
	out.Buffers[1].WrapBuffer(offsetBuilder.finish())
	out.Buffers[2].WrapBuffer(dataBuilder.finish())
	return nil
}

func binaryFilterImpl[OffsetT int32 | int64](ctx *exec.KernelCtx, values, filter *exec.ArraySpan, outputLen int64, nullSelection NullSelectionBehavior, out *exec.ExecResult) error {
	var (
		filterData    = filter.Buffers[1].Buf
		filterIsValid = filter.Buffers[0].Buf
		filterOffset  = filter.Offset

		valuesIsValid = values.Buffers[0].Buf
		valuesOffset  = values.Offset
		// output bitmap should already be zero'd out so we just
		// have to set valid bits to true
		outIsValid = out.Buffers[0].Buf

		rawOffsets    = exec.GetSpanOffsets[OffsetT](values, 1)
		rawData       = values.Buffers[2].Buf
		offsetBuilder = newBufferBuilder[OffsetT](exec.GetAllocator(ctx.Ctx))
		dataBuilder   = newBufferBuilder[uint8](exec.GetAllocator(ctx.Ctx))
	)

	offsetBuilder.reserve(int(outputLen) + 1)
	if values.Len > 0 {
		meanValueLength := float64(rawOffsets[values.Len]-rawOffsets[0]) / float64(values.Len)
		dataBuilder.reserve(int(meanValueLength * float64(outputLen)))
	}

	spaceAvail := dataBuilder.cap()
	var offset OffsetT

	// we use 3 block counters for fast scanning of the filter
	//
	// * valuesValidCounter: for values null/not-null
	// * filterValidCounter: for filter null/not-null
	// * filterCounter: for filter true/false
	valuesValidCounter := bitutils.NewOptionalBitBlockCounter(values.Buffers[0].Buf, values.Offset, values.Len)
	filterValidCounter := bitutils.NewOptionalBitBlockCounter(filterIsValid, filterOffset, filter.Len)
	filterCounter := bitutils.NewBitBlockCounter(filterData, filterOffset, filter.Len)

	inPos, outPos := int64(0), int64(0)

	appendRaw := func(data []byte) {
		if len(data) > spaceAvail {
			dataBuilder.reserve(len(data))
			spaceAvail = dataBuilder.cap() - dataBuilder.len()
		}
		dataBuilder.unsafeAppendSlice(data)
		spaceAvail -= len(data)
	}

	appendSingle := func() {
		data := rawData[rawOffsets[inPos]:rawOffsets[inPos+1]]
		appendRaw(data)
		offset += OffsetT(len(data))
	}

	for inPos < filter.Len {
		filterValidBlock, valuesValidBlock := filterValidCounter.NextWord(), valuesValidCounter.NextWord()
		filterBlock := filterCounter.NextWord()
		switch {
		case filterBlock.NoneSet() && nullSelection == DropNulls:
			// for this exceedingly common case in low-selectivity filters
			// we can skip further analysis of the data and move on to the
			// next block
			inPos += int64(filterBlock.Len)
		case filterValidBlock.AllSet():
			// simpler path: no filter values are null
			if filterBlock.AllSet() {
				// fastest path: filter values are all true and not null
				if valuesValidBlock.AllSet() {
					// the values aren't null either
					bitutil.SetBitsTo(outIsValid, outPos, int64(filterBlock.Len), true)

					// bulk-append raw data
					start, end := rawOffsets[inPos], rawOffsets[inPos+int64(filterBlock.Len)]
					appendRaw(rawData[start:end])
					// append offsets
					for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
						offsetBuilder.unsafeAppend(offset)
						offset += rawOffsets[inPos+1] - rawOffsets[inPos]
					}
					outPos += int64(filterBlock.Len)
				} else {
					// some of the values in this block are null
					for i := 0; i < int(filterBlock.Len); i, inPos, outPos = i+1, inPos+1, outPos+1 {
						offsetBuilder.unsafeAppend(offset)
						if bitutil.BitIsSet(valuesIsValid, int(valuesOffset+inPos)) {
							bitutil.SetBit(outIsValid, int(outPos))
							appendSingle()
						}
					}
				}
				continue
			}
			// !filterBlock.AllSet()
			// some of the filter values are false, but all not null
			if valuesValidBlock.AllSet() {
				// all the values are non-null, so we can skip null checking
				for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
					if bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
						offsetBuilder.unsafeAppend(offset)
						bitutil.SetBit(outIsValid, int(outPos))
						outPos++
						appendSingle()
					}
				}
			} else {
				// some of the values in the block are null, so we have to check
				for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
					if bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
						offsetBuilder.unsafeAppend(offset)
						if bitutil.BitIsSet(valuesIsValid, int(valuesOffset+inPos)) {
							bitutil.SetBit(outIsValid, int(outPos))
							appendSingle()
						}
						outPos++
					}
				}
			}
		default:
			// !filterValidBlock.AllSet()
			// some of the filter values are null, so we have to handle
			// the DROP vs EMIT_NULL null selection behavior
			if nullSelection == DropNulls {
				// filter null values are treated as false
				if valuesValidBlock.AllSet() {
					for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
						if bitutil.BitIsSet(filterIsValid, int(filterOffset+inPos)) &&
							bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
							offsetBuilder.unsafeAppend(offset)
							bitutil.SetBit(outIsValid, int(outPos))
							outPos++
							appendSingle()
						}
					}
				} else {
					for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
						if bitutil.BitIsSet(filterIsValid, int(filterOffset+inPos)) &&
							bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
							offsetBuilder.unsafeAppend(offset)
							if bitutil.BitIsSet(valuesIsValid, int(valuesOffset+inPos)) {
								bitutil.SetBit(outIsValid, int(outPos))
								appendSingle()
							}
							outPos++
						}
					}
				}
			} else {
				for i := 0; i < int(filterBlock.Len); i, inPos = i+1, inPos+1 {
					filterNotNull := bitutil.BitIsSet(filterIsValid, int(filterOffset+inPos))
					if filterNotNull && bitutil.BitIsSet(filterData, int(filterOffset+inPos)) {
						offsetBuilder.unsafeAppend(offset)
						if bitutil.BitIsSet(valuesIsValid, int(valuesOffset+inPos)) {
							bitutil.SetBit(outIsValid, int(outPos))
							appendSingle()
						}
						outPos++
					} else if !filterNotNull {
						offsetBuilder.unsafeAppend(offset)
						outPos++
					}
				}
			}
		}
	}

	offsetBuilder.unsafeAppend(offset)
	out.Len = outputLen
	out.Buffers[1].WrapBuffer(offsetBuilder.finish())
	out.Buffers[2].WrapBuffer(dataBuilder.finish())
	return nil
}

func takeExecImpl[T exec.UintTypes](ctx *exec.KernelCtx, outputLen int64, values, indices *exec.ArraySpan, out *exec.ExecResult, visitValid func(int64) error, visitNull func() error) error {
	var (
		validityBuilder = validityBuilder{mem: exec.GetAllocator(ctx.Ctx)}
		indicesValues   = exec.GetSpanValues[T](indices, 1)
		isValid         = indices.Buffers[0].Buf
		valuesHaveNulls = values.MayHaveNulls()

		indicesIsValid = bitutil.OptionalBitIndexer{Bitmap: isValid, Offset: int(indices.Offset)}
		valuesIsValid  = bitutil.OptionalBitIndexer{Bitmap: values.Buffers[0].Buf, Offset: int(values.Offset)}
		bitCounter     = bitutils.NewOptionalBitBlockCounter(isValid, indices.Offset, indices.Len)
		pos            int64
	)

	validityBuilder.Reserve(outputLen)
	for pos < indices.Len {
		block := bitCounter.NextBlock()
		indicesHaveNulls := block.Popcnt < block.Len
		if !indicesHaveNulls && !valuesHaveNulls {
			// fastest path, neither indices nor values have nulls
			validityBuilder.UnsafeAppendN(int64(block.Len), true)
			for i := 0; i < int(block.Len); i++ {
				if err := visitValid(int64(indicesValues[pos])); err != nil {
					return err
				}
				pos++
			}
		} else if block.Popcnt > 0 {
			// since we have to branch on whether indices are null or not,
			// we combine the "non-null indices block but some values null"
			// and "some null indices block but values non-null" into single loop
			for i := 0; i < int(block.Len); i++ {
				if (!indicesHaveNulls || indicesIsValid.GetBit(int(pos))) && valuesIsValid.GetBit(int(indicesValues[pos])) {
					validityBuilder.UnsafeAppend(true)
					if err := visitValid(int64(indicesValues[pos])); err != nil {
						return err
					}
				} else {
					validityBuilder.UnsafeAppend(false)
					if err := visitNull(); err != nil {
						return err
					}
				}
				pos++
			}
		} else {
			// the whole block is null
			validityBuilder.UnsafeAppendN(int64(block.Len), false)
			for i := 0; i < int(block.Len); i++ {
				if err := visitNull(); err != nil {
					return err
				}
			}
			pos += int64(block.Len)
		}
	}

	out.Len = int64(validityBuilder.bitLength)
	out.Nulls = int64(validityBuilder.falseCount)
	out.Buffers[0].WrapBuffer(validityBuilder.Finish())
	return nil
}

func takeExec(ctx *exec.KernelCtx, outputLen int64, values, indices *exec.ArraySpan, out *exec.ExecResult, visitValid func(int64) error, visitNull func() error) error {
	indexWidth := indices.Type.(arrow.FixedWidthDataType).Bytes()

	switch indexWidth {
	case 1:
		return takeExecImpl[uint8](ctx, outputLen, values, indices, out, visitValid, visitNull)
	case 2:
		return takeExecImpl[uint16](ctx, outputLen, values, indices, out, visitValid, visitNull)
	case 4:
		return takeExecImpl[uint32](ctx, outputLen, values, indices, out, visitValid, visitNull)
	case 8:
		return takeExecImpl[uint64](ctx, outputLen, values, indices, out, visitValid, visitNull)
	default:
		return fmt.Errorf("%w: invalid index width", arrow.ErrInvalid)
	}
}

type outputFn func(*exec.KernelCtx, int64, *exec.ArraySpan, *exec.ArraySpan, *exec.ExecResult, func(int64) error, func() error) error
type implFn func(*exec.KernelCtx, *exec.ExecSpan, int64, *exec.ExecResult, outputFn) error

func FilterExec(impl implFn, fn outputFn) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			selection    = &batch.Values[1].Array
			outputLength = getFilterOutputSize(selection, ctx.State.(FilterState).NullSelection)
		)
		return impl(ctx, batch, outputLength, out, fn)
	}
}

func TakeExec(impl implFn, fn outputFn) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
		if ctx.State.(TakeState).BoundsCheck {
			if err := checkIndexBounds(&batch.Values[1].Array, uint64(batch.Values[0].Array.Len)); err != nil {
				return err
			}
		}

		return impl(ctx, batch, batch.Values[1].Array.Len, out, fn)
	}
}

func VarBinaryImpl[OffsetT int32 | int64](ctx *exec.KernelCtx, batch *exec.ExecSpan, outputLength int64, out *exec.ExecResult, fn outputFn) error {
	var (
		values        = &batch.Values[0].Array
		selection     = &batch.Values[1].Array
		rawOffsets    = exec.GetSpanOffsets[OffsetT](values, 1)
		rawData       = values.Buffers[2].Buf
		offsetBuilder = newBufferBuilder[OffsetT](exec.GetAllocator(ctx.Ctx))
		dataBuilder   = newBufferBuilder[uint8](exec.GetAllocator(ctx.Ctx))
	)

	// presize the data builder with a rough estimate of the required data size
	if values.Len > 0 {
		dataLength := rawOffsets[values.Len] - rawOffsets[0]
		meanValueLen := float64(dataLength) / float64(values.Len)
		dataBuilder.reserve(int(meanValueLen))
	}

	offsetBuilder.reserve(int(outputLength) + 1)
	spaceAvail := dataBuilder.cap()
	var offset OffsetT
	err := fn(ctx, outputLength, values, selection, out,
		func(idx int64) error {
			offsetBuilder.unsafeAppend(offset)
			valOffset := rawOffsets[idx]
			valSize := rawOffsets[idx+1] - valOffset

			offset += valSize
			if valSize > OffsetT(spaceAvail) {
				dataBuilder.reserve(int(valSize))
				spaceAvail = dataBuilder.cap() - dataBuilder.len()
			}
			dataBuilder.unsafeAppendSlice(rawData[valOffset : valOffset+valSize])
			spaceAvail -= int(valSize)
			return nil
		}, func() error {
			offsetBuilder.unsafeAppend(offset)
			return nil
		})

	if err != nil {
		return err
	}

	offsetBuilder.unsafeAppend(offset)
	out.Buffers[1].WrapBuffer(offsetBuilder.finish())
	out.Buffers[2].WrapBuffer(dataBuilder.finish())
	return nil
}

func FSBImpl(ctx *exec.KernelCtx, batch *exec.ExecSpan, outputLength int64, out *exec.ExecResult, fn outputFn) error {
	var (
		values    = &batch.Values[0].Array
		selection = &batch.Values[1].Array
		valueSize = int64(values.Type.(arrow.FixedWidthDataType).Bytes())
		valueData = values.Buffers[1].Buf[values.Offset*valueSize:]
	)

	out.Buffers[1].WrapBuffer(ctx.Allocate(int(valueSize * outputLength)))
	buf := out.Buffers[1].Buf

	err := fn(ctx, outputLength, values, selection, out,
		func(idx int64) error {
			start := idx * int64(valueSize)
			copy(buf, valueData[start:start+valueSize])
			buf = buf[valueSize:]
			return nil
		},
		func() error {
			buf = buf[valueSize:]
			return nil
		})

	if err != nil {
		out.Buffers[1].Buf = nil
		out.Buffers[1].Owner.Release()
		out.Buffers[1].Owner = nil
		return err
	}

	return nil
}

func FilterBinary(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		nullSelect = ctx.State.(FilterState).NullSelection
		values     = &batch.Values[0].Array
		filter     = &batch.Values[1].Array
		outputLen  = getFilterOutputSize(filter, nullSelect)
	)

	// the output precomputed null count is unknown except in the
	// narrow condition that all the values are non-null and the filter
	// will not cause any new nulls to be created
	if values.Nulls == 0 && (nullSelect == DropNulls || filter.Nulls == 0) {
		out.Nulls = 0
	} else {
		out.Nulls = array.UnknownNullCount
	}

	typeID := values.Type.ID()
	if values.Nulls == 0 && filter.Nulls == 0 {
		// faster no nulls case
		switch {
		case arrow.IsBinaryLike(typeID):
			return binaryFilterNonNull[int32](ctx, values, filter, outputLen, nullSelect, out)
		case arrow.IsLargeBinaryLike(typeID):
			return binaryFilterNonNull[int64](ctx, values, filter, outputLen, nullSelect, out)
		default:
			return fmt.Errorf("%w: invalid type for binary filter", arrow.ErrInvalid)
		}
	}

	// output may have nulls
	out.Buffers[0].WrapBuffer(ctx.AllocateBitmap(outputLen))
	switch {
	case arrow.IsBinaryLike(typeID):
		return binaryFilterImpl[int32](ctx, values, filter, outputLen, nullSelect, out)
	case arrow.IsLargeBinaryLike(typeID):
		return binaryFilterImpl[int64](ctx, values, filter, outputLen, nullSelect, out)
	}

	return fmt.Errorf("%w: invalid type for binary filter", arrow.ErrInvalid)
}

type SelectionKernelData struct {
	In   exec.InputType
	Exec exec.ArrayKernelExec
}

func GetVectorSelectionKernels() (filterkernels, takeKernels []SelectionKernelData) {
	filterkernels = []SelectionKernelData{
		{In: exec.NewMatchedInput(exec.Primitive()), Exec: PrimitiveFilter},
		{In: exec.NewExactInput(arrow.Null), Exec: NullFilter},
		{In: exec.NewIDInput(arrow.DECIMAL128), Exec: FilterExec(FSBImpl, filterExec)},
		{In: exec.NewIDInput(arrow.DECIMAL256), Exec: FilterExec(FSBImpl, filterExec)},
		{In: exec.NewIDInput(arrow.FIXED_SIZE_BINARY), Exec: FilterExec(FSBImpl, filterExec)},
		{In: exec.NewMatchedInput(exec.BinaryLike()), Exec: FilterBinary},
		{In: exec.NewMatchedInput(exec.LargeBinaryLike()), Exec: FilterBinary},
	}

	takeKernels = []SelectionKernelData{
		{In: exec.NewExactInput(arrow.Null), Exec: NullTake},
		{In: exec.NewMatchedInput(exec.Primitive()), Exec: PrimitiveTake},
		{In: exec.NewIDInput(arrow.DECIMAL128), Exec: TakeExec(FSBImpl, takeExec)},
		{In: exec.NewIDInput(arrow.DECIMAL256), Exec: TakeExec(FSBImpl, takeExec)},
		{In: exec.NewIDInput(arrow.FIXED_SIZE_BINARY), Exec: TakeExec(FSBImpl, takeExec)},
		{In: exec.NewMatchedInput(exec.BinaryLike()), Exec: TakeExec(VarBinaryImpl[int32], takeExec)},
		{In: exec.NewMatchedInput(exec.LargeBinaryLike()), Exec: TakeExec(VarBinaryImpl[int64], takeExec)},
	}
	return
}

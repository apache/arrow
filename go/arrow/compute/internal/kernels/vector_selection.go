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

func FilterFSB(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	var (
		values       = &batch.Values[0].Array
		selection    = &batch.Values[1].Array
		outputLength = getFilterOutputSize(selection, ctx.State.(FilterState).NullSelection)
		valueSize    = int64(values.Type.(arrow.FixedWidthDataType).Bytes())
		valueData    = values.Buffers[1].Buf[values.Offset*valueSize:]
	)

	out.Buffers[1].WrapBuffer(ctx.Allocate(int(valueSize * outputLength)))
	buf := out.Buffers[1].Buf

	err := filterExec(ctx, outputLength, values, selection, out,
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

type SelectionKernelData struct {
	In   exec.InputType
	Exec exec.ArrayKernelExec
}

func GetVectorSelectionKernels() (filterkernels, takeKernels []SelectionKernelData) {
	filterkernels = []SelectionKernelData{
		{In: exec.NewMatchedInput(exec.Primitive()), Exec: PrimitiveFilter},
		{In: exec.NewExactInput(arrow.Null), Exec: NullFilter},
		{In: exec.NewIDInput(arrow.DECIMAL128), Exec: FilterFSB},
		{In: exec.NewIDInput(arrow.DECIMAL256), Exec: FilterFSB},
	}

	return
}

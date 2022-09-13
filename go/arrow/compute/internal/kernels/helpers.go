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
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/internal/bitutils"
	"golang.org/x/exp/constraints"
)

// ScalarUnary returns a kernel for performing a unary operation on
// FixedWidth types which is implemented using the passed in function
// which will receive a slice containing the raw input data along with
// a slice to populate for the output data.
//
// Note that bool is not included in exec.FixedWidthTypes since it is
// represented as a bitmap, not as a slice of bool.
func ScalarUnary[OutT, Arg0T exec.FixedWidthTypes](op func(*exec.KernelCtx, []Arg0T, []OutT) error) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := in.Values[0].Array
		inData := exec.GetSpanValues[Arg0T](&arg0, 1)
		outData := exec.GetSpanValues[OutT](out, 1)
		return op(ctx, inData, outData)
	}
}

// ScalarUnaryNotNull is for generating a kernel to operate only on the
// non-null values in the input array. The zerovalue of the output type
// is used for any null input values.
func ScalarUnaryNotNull[OutT, Arg0T exec.FixedWidthTypes](op func(*exec.KernelCtx, Arg0T, *error) OutT) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			arg0     = &in.Values[0].Array
			arg0Data = exec.GetSpanValues[Arg0T](arg0, 1)
			outPos   = 0
			def      OutT
			outData  = exec.GetSpanValues[OutT](out, 1)
			bitmap   = arg0.Buffers[0].Buf
			err      error
		)

		bitutils.VisitBitBlocks(bitmap, arg0.Offset, arg0.Len,
			func(pos int64) {
				outData[outPos] = op(ctx, arg0Data[pos], &err)
				outPos++
			}, func() {
				outData[outPos] = def
				outPos++
			})
		return err
	}
}

// ScalarUnaryBoolOutput is like ScalarUnary only it is for cases of boolean
// output. The function should take in a slice of the input type and a slice
// of bytes to fill with the output boolean bitmap.
func ScalarUnaryBoolOutput[Arg0T exec.FixedWidthTypes](op func(*exec.KernelCtx, []Arg0T, []byte) error) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := in.Values[0].Array
		inData := exec.GetSpanValues[Arg0T](&arg0, 1)
		return op(ctx, inData, out.Buffers[1].Buf)
	}
}

// ScalarUnaryNotNullBinaryArgBoolOut creates a unary kernel that accepts
// a binary type input (Binary [offset int32], String [offset int32],
// LargeBinary [offset int64], LargeString [offset int64]) and returns
// a boolean output which is never null.
//
// It implements the handling to iterate the offsets and values calling
// the provided function on each byte slice. The provided default value
// will be used as the output for elements of the input that are null.
func ScalarUnaryNotNullBinaryArgBoolOut[OffsetT int32 | int64](defVal bool, op func(*exec.KernelCtx, []byte, *error) bool) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			arg0        = in.Values[0].Array
			outData     = out.Buffers[1].Buf
			outPos      = 0
			arg0Offsets = exec.GetSpanOffsets[OffsetT](&arg0, 1)
			arg0Data    = arg0.Buffers[2].Buf
			bitmap      = arg0.Buffers[0].Buf
			err         error
		)

		bitutils.VisitBitBlocks(bitmap, arg0.Offset, arg0.Len,
			func(pos int64) {
				v := arg0Data[arg0Offsets[pos]:arg0Offsets[pos+1]]
				bitutil.SetBitTo(outData, int(out.Offset)+outPos, op(ctx, v, &err))
				outPos++
			}, func() {
				bitutil.SetBitTo(outData, int(out.Offset)+outPos, defVal)
				outPos++
			})
		return err
	}
}

// ScalarUnaryNotNullBinaryArg creates a unary kernel that accepts
// a binary type input (Binary [offset int32], String [offset int32],
// LargeBinary [offset int64], LargeString [offset int64]) and returns
// a FixedWidthType output which is never null.
//
// It implements the handling to iterate the offsets and values calling
// the provided function on each byte slice. The zero value of the OutT
// will be used as the output for elements of the input that are null.
func ScalarUnaryNotNullBinaryArg[OutT exec.FixedWidthTypes, OffsetT int32 | int64](op func(*exec.KernelCtx, []byte, *error) OutT) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			arg0        = &in.Values[0].Array
			outData     = exec.GetSpanValues[OutT](out, 1)
			outPos      = 0
			arg0Offsets = exec.GetSpanOffsets[OffsetT](arg0, 1)
			def         OutT
			arg0Data    = arg0.Buffers[2].Buf
			bitmap      = arg0.Buffers[0].Buf
			err         error
		)

		bitutils.VisitBitBlocks(bitmap, arg0.Offset, arg0.Len,
			func(pos int64) {
				v := arg0Data[arg0Offsets[pos]:arg0Offsets[pos+1]]
				outData[outPos] = op(ctx, v, &err)
				outPos++
			}, func() {
				outData[outPos] = def
				outPos++
			})
		return err
	}
}

// ScalarUnaryBoolArg is like ScalarUnary except it specifically expects a
// function that takes a byte slice since booleans arrays are represented
// as a bitmap.
func ScalarUnaryBoolArg[OutT exec.FixedWidthTypes](op func(*exec.KernelCtx, []byte, []OutT) error) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, input *exec.ExecSpan, out *exec.ExecResult) error {
		outData := exec.GetSpanValues[OutT](out, 1)
		return op(ctx, input.Values[0].Array.Buffers[1].Buf, outData)
	}
}

// SizeOf determines the size in number of bytes for an integer
// based on the generic value in a way that the compiler should
// be able to easily evaluate and create as a constant.
func SizeOf[T constraints.Integer]() uint {
	x := uint16(1 << 8)
	y := uint32(2 << 16)
	z := uint64(4 << 32)
	return 1 + uint(T(x))>>8 + uint(T(y))>>16 + uint(T(z))>>32
}

// MinOf returns the minimum value for a given type since there is not
// currently a generic way to do this with Go generics yet.
func MinOf[T constraints.Integer]() T {
	if ones := ^T(0); ones < 0 {
		return ones << (8*SizeOf[T]() - 1)
	}
	return 0
}

// MaxOf determines the max value for a given type since there is not
// currently a generic way to do this for Go generics yet as all of the
// math.Max/Min values are constants.
func MaxOf[T constraints.Integer]() T {
	ones := ^T(0)
	if ones < 0 {
		return ones ^ (ones << (8*SizeOf[T]() - 1))
	}
	return ones
}

func getSafeMinSameSign[I, O constraints.Integer]() I {
	if SizeOf[I]() > SizeOf[O]() {
		return I(MinOf[O]())
	}
	return MinOf[I]()
}

func getSafeMaxSameSign[I, O constraints.Integer]() I {
	if SizeOf[I]() > SizeOf[O]() {
		return I(MaxOf[O]())
	}
	return MaxOf[I]()
}

func getSafeMaxSignedUnsigned[I constraints.Signed, O constraints.Unsigned]() I {
	if SizeOf[I]() <= SizeOf[O]() {
		return MaxOf[I]()
	}
	return I(MaxOf[O]())
}

func getSafeMaxUnsignedSigned[I constraints.Unsigned, O constraints.Signed]() I {
	if SizeOf[I]() < SizeOf[O]() {
		return MaxOf[I]()
	}
	return I(MaxOf[O]())
}

func getSafeMinMaxSigned[T constraints.Signed](target arrow.Type) (min, max T) {
	switch target {
	case arrow.UINT8:
		min, max = 0, getSafeMaxSignedUnsigned[T, uint8]()
	case arrow.UINT16:
		min, max = 0, getSafeMaxSignedUnsigned[T, uint16]()
	case arrow.UINT32:
		min, max = 0, getSafeMaxSignedUnsigned[T, uint32]()
	case arrow.UINT64:
		min, max = 0, getSafeMaxSignedUnsigned[T, uint64]()
	case arrow.INT8:
		min = getSafeMinSameSign[T, int8]()
		max = getSafeMaxSameSign[T, int8]()
	case arrow.INT16:
		min = getSafeMinSameSign[T, int16]()
		max = getSafeMaxSameSign[T, int16]()
	case arrow.INT32:
		min = getSafeMinSameSign[T, int32]()
		max = getSafeMaxSameSign[T, int32]()
	case arrow.INT64:
		min = getSafeMinSameSign[T, int64]()
		max = getSafeMaxSameSign[T, int64]()
	}
	return
}

func getSafeMinMaxUnsigned[T constraints.Unsigned](target arrow.Type) (min, max T) {
	min = 0
	switch target {
	case arrow.UINT8:
		max = getSafeMaxSameSign[T, uint8]()
	case arrow.UINT16:
		max = getSafeMaxSameSign[T, uint16]()
	case arrow.UINT32:
		max = getSafeMaxSameSign[T, uint32]()
	case arrow.UINT64:
		max = getSafeMaxSameSign[T, uint64]()
	case arrow.INT8:
		max = getSafeMaxUnsignedSigned[T, int8]()
	case arrow.INT16:
		max = getSafeMaxUnsignedSigned[T, int16]()
	case arrow.INT32:
		max = getSafeMaxUnsignedSigned[T, int32]()
	case arrow.INT64:
		max = getSafeMaxUnsignedSigned[T, int64]()
	}
	return
}

func intsCanFit(data *exec.ArraySpan, target arrow.Type) error {
	if !arrow.IsInteger(target) {
		return fmt.Errorf("%w: target type is not an integer type %s", arrow.ErrInvalid, target)
	}

	switch data.Type.ID() {
	case arrow.INT8:
		min, max := getSafeMinMaxSigned[int8](target)
		return intsInRange(data, min, max)
	case arrow.UINT8:
		min, max := getSafeMinMaxUnsigned[uint8](target)
		return intsInRange(data, min, max)
	case arrow.INT16:
		min, max := getSafeMinMaxSigned[int16](target)
		return intsInRange(data, min, max)
	case arrow.UINT16:
		min, max := getSafeMinMaxUnsigned[uint16](target)
		return intsInRange(data, min, max)
	case arrow.INT32:
		min, max := getSafeMinMaxSigned[int32](target)
		return intsInRange(data, min, max)
	case arrow.UINT32:
		min, max := getSafeMinMaxUnsigned[uint32](target)
		return intsInRange(data, min, max)
	case arrow.INT64:
		min, max := getSafeMinMaxSigned[int64](target)
		return intsInRange(data, min, max)
	case arrow.UINT64:
		min, max := getSafeMinMaxUnsigned[uint64](target)
		return intsInRange(data, min, max)
	default:
		return fmt.Errorf("%w: invalid type for int bounds checking", arrow.ErrInvalid)
	}
}

func intsInRange[T exec.IntTypes | exec.UintTypes](data *exec.ArraySpan, lowerBound, upperBound T) error {
	if MinOf[T]() >= lowerBound && MaxOf[T]() <= upperBound {
		return nil
	}

	isOutOfBounds := func(val T) bool {
		return val < lowerBound || val > upperBound
	}
	isOutOfBoundsMaybeNull := func(val T, isValid bool) bool {
		return isValid && (val < lowerBound || val > upperBound)
	}
	getError := func(val T) error {
		return fmt.Errorf("%w: integer value %d not in range: %d to %d",
			arrow.ErrInvalid, val, lowerBound, upperBound)
	}

	values := exec.GetSpanValues[T](data, 1)
	bitmap := data.Buffers[0].Buf

	bitCounter := bitutils.NewOptionalBitBlockCounter(bitmap, data.Offset, data.Len)
	pos, offsetPos := 0, data.Offset
	for pos < int(data.Len) {
		block := bitCounter.NextBlock()
		outOfBounds := false

		if block.Popcnt == block.Len {
			// fast path: branchless
			i := 0
			for chunk := 0; chunk < int(block.Len)/8; chunk++ {
				for j := 0; j < 8; j++ {
					outOfBounds = outOfBounds || isOutOfBounds(values[i])
					i++
				}
			}
			for ; i < int(block.Len); i++ {
				outOfBounds = outOfBounds || isOutOfBounds(values[i])
			}
		} else if block.Popcnt > 0 {
			// values may be null, only bounds check non-null vals
			i := 0
			for chunk := 0; chunk < int(block.Len)/8; chunk++ {
				for j := 0; j < 8; j++ {
					outOfBounds = outOfBounds || isOutOfBoundsMaybeNull(
						values[i], bitutil.BitIsSet(bitmap, int(offsetPos)+i))
					i++
				}
			}
			for ; i < int(block.Len); i++ {
				outOfBounds = outOfBounds || isOutOfBoundsMaybeNull(
					values[i], bitutil.BitIsSet(bitmap, int(offsetPos)+i))
			}
		}
		if outOfBounds {
			if data.Nulls > 0 {
				for i := 0; i < int(block.Len); i++ {
					if isOutOfBoundsMaybeNull(values[i], bitutil.BitIsSet(bitmap, int(offsetPos)+i)) {
						return getError(values[i])
					}
				}
			} else {
				for i := 0; i < int(block.Len); i++ {
					if isOutOfBounds(values[i]) {
						return getError(values[i])
					}
				}
			}
		}

		values = values[block.Len:]
		pos += int(block.Len)
		offsetPos += int64(block.Len)
	}
	return nil
}

type numeric interface {
	exec.IntTypes | exec.UintTypes | constraints.Float
}

func memCpySpan[T numeric](in, out *exec.ArraySpan) {
	inData := exec.GetSpanValues[T](in, 1)
	outData := exec.GetSpanValues[T](out, 1)
	copy(outData, inData)
}

func castNumberMemCpy(in, out *exec.ArraySpan) {
	switch in.Type.ID() {
	case arrow.INT8:
		memCpySpan[int8](in, out)
	case arrow.UINT8:
		memCpySpan[uint8](in, out)
	case arrow.INT16:
		memCpySpan[int16](in, out)
	case arrow.UINT16:
		memCpySpan[uint16](in, out)
	case arrow.INT32:
		memCpySpan[int32](in, out)
	case arrow.UINT32:
		memCpySpan[uint32](in, out)
	case arrow.INT64:
		memCpySpan[int64](in, out)
	case arrow.UINT64:
		memCpySpan[uint64](in, out)
	case arrow.FLOAT32:
		memCpySpan[float32](in, out)
	case arrow.FLOAT64:
		memCpySpan[float64](in, out)
	}
}

func castNumberToNumberUnsafe(in, out *exec.ArraySpan) {
	if in.Type.ID() == out.Type.ID() {
		castNumberMemCpy(in, out)
		return
	}

	inputOffset := in.Type.(arrow.FixedWidthDataType).Bytes() * int(in.Offset)
	outputOffset := out.Type.(arrow.FixedWidthDataType).Bytes() * int(out.Offset)
	castNumericUnsafe(in.Type.ID(), out.Type.ID(), in.Buffers[1].Buf[inputOffset:], out.Buffers[1].Buf[outputOffset:], int(in.Len))
}

func maxDecimalDigitsForInt(id arrow.Type) (int32, error) {
	switch id {
	case arrow.INT8, arrow.UINT8:
		return 3, nil
	case arrow.INT16, arrow.UINT16:
		return 5, nil
	case arrow.INT32, arrow.UINT32:
		return 10, nil
	case arrow.INT64:
		return 19, nil
	case arrow.UINT64:
		return 20, nil
	}
	return -1, fmt.Errorf("%w: not an integer type: %s", arrow.ErrInvalid, id)
}

func ResolveOutputFromOptions(ctx *exec.KernelCtx, _ []arrow.DataType) (arrow.DataType, error) {
	opts := ctx.State.(CastState)
	return opts.ToType, nil
}

var OutputTargetType = exec.NewComputedOutputType(ResolveOutputFromOptions)

func resolveToFirstType(_ *exec.KernelCtx, args []arrow.DataType) (arrow.DataType, error) {
	return args[0], nil
}

var OutputFirstType = exec.NewComputedOutputType(resolveToFirstType)

type validityBuilder struct {
	mem    memory.Allocator
	buffer *memory.Buffer

	data       []byte
	bitLength  int
	falseCount int
}

func (v *validityBuilder) Resize(n int64) {
	if v.buffer == nil {
		v.buffer = memory.NewResizableBuffer(v.mem)
	}

	v.buffer.ResizeNoShrink(int(bitutil.BytesForBits(n)))
	v.data = v.buffer.Bytes()
}

func (v *validityBuilder) Reserve(n int64) {
	if v.buffer == nil {
		v.buffer = memory.NewResizableBuffer(v.mem)
	}

	v.buffer.Reserve(v.buffer.Cap() + int(bitutil.BytesForBits(n)))
	v.data = v.buffer.Buf()
}

func (v *validityBuilder) UnsafeAppend(val bool) {
	bitutil.SetBitTo(v.data, v.bitLength, val)
	if !val {
		v.falseCount++
	}
	v.bitLength++
}

func (v *validityBuilder) UnsafeAppendN(n int64, val bool) {
	bitutil.SetBitsTo(v.data, int64(v.bitLength), n, val)
	if !val {
		v.falseCount += int(n)
	}
	v.bitLength += int(n)
}

func (v *validityBuilder) Append(val bool) {
	v.Reserve(1)
	v.UnsafeAppend(val)
}

func (v *validityBuilder) AppendN(n int64, val bool) {
	v.Reserve(n)
	v.UnsafeAppendN(n, val)
}

func (v *validityBuilder) Finish() (buf *memory.Buffer) {
	if v.bitLength > 0 {
		v.buffer.Resize(int(bitutil.BytesForBits(int64(v.bitLength))))
	}

	v.bitLength, v.falseCount = 0, 0
	buf = v.buffer
	v.buffer = nil
	return
}

type execBufBuilder struct {
	mem    memory.Allocator
	buffer *memory.Buffer
	data   []byte
	sz     int
}

func (bldr *execBufBuilder) resize(newcap int) {
	if bldr.buffer == nil {
		bldr.buffer = memory.NewResizableBuffer(bldr.mem)
	}

	bldr.buffer.ResizeNoShrink(newcap)
	bldr.data = bldr.buffer.Bytes()
}

func (bldr *execBufBuilder) reserve(additional int) {
	if bldr.buffer == nil {
		bldr.buffer = memory.NewResizableBuffer(bldr.mem)
	}

	mincap := bldr.sz + additional
	if mincap <= cap(bldr.data) {
		return
	}
	bldr.buffer.ResizeNoShrink(mincap)
	bldr.data = bldr.buffer.Buf()
}

func (bldr *execBufBuilder) unsafeAppend(data []byte) {
	copy(bldr.data[bldr.sz:], data)
	bldr.sz += len(data)
}

func (bldr *execBufBuilder) unsafeAppendN(n int, val byte) {
	bldr.data[bldr.sz] = val
	for i := 1; i < n; i *= 2 {
		copy(bldr.data[bldr.sz+i:], bldr.data[bldr.sz:bldr.sz+i])
	}
	bldr.sz += n
}

func (bldr *execBufBuilder) append(data []byte) {
	if bldr.sz+len(data) > cap(bldr.data) {
		bldr.resize(bldr.sz + len(data))
	}
	bldr.unsafeAppend(data)
}

func (bldr *execBufBuilder) appendN(n int, val byte) {
	bldr.reserve(n)
	bldr.unsafeAppendN(n, val)
}

func (bldr *execBufBuilder) advance(n int) {
	bldr.reserve(n)
	bldr.sz += n
}

func (bldr *execBufBuilder) unsafeAdvance(n int) {
	bldr.sz += n
}

func (bldr *execBufBuilder) finish() (buf *memory.Buffer) {
	bldr.buffer.Resize(bldr.sz)
	buf = bldr.buffer
	bldr.buffer, bldr.sz = nil, 0
	return
}

type bufferBuilder[T exec.FixedWidthTypes] struct {
	execBufBuilder
	zero T
}

func newBufferBuilder[T exec.FixedWidthTypes](mem memory.Allocator) *bufferBuilder[T] {
	return &bufferBuilder[T]{
		execBufBuilder: execBufBuilder{
			mem: mem,
		},
	}
}

func (b *bufferBuilder[T]) reserve(additional int) {
	b.execBufBuilder.reserve(additional * int(unsafe.Sizeof(b.zero)))
}

func (b *bufferBuilder[T]) resize(newcap int) {
	b.execBufBuilder.resize(newcap * int(unsafe.Sizeof(b.zero)))
}

func (b *bufferBuilder[T]) unsafeAppend(value T) {
	b.execBufBuilder.unsafeAppend(exec.GetBytes([]T{value}))
}

func (b *bufferBuilder[T]) unsafeAppendN(n int, value T) {
	data := exec.GetData[T](b.data)[b.len():]
	b.execBufBuilder.unsafeAdvance(n * int(unsafe.Sizeof(value)))
	data[0] = value
	for i := 1; i < n; i *= 2 {
		copy(data[i:], data[:i])
	}
}

func (b *bufferBuilder[T]) unsafeAppendSlice(values []T) {
	b.execBufBuilder.unsafeAppend(exec.GetBytes(values))
}

func (b *bufferBuilder[T]) advance(n int) {
	b.execBufBuilder.advance(n * int(unsafe.Sizeof(b.zero)))
}

func (b *bufferBuilder[T]) append(value T) {
	b.execBufBuilder.append(exec.GetBytes([]T{value}))
}

func (b *bufferBuilder[T]) len() int { return b.sz / int(unsafe.Sizeof(b.zero)) }

func (b *bufferBuilder[T]) appendN(n int, value T) {
	b.reserve(n + b.len())
	b.unsafeAppendN(n, value)
}

func (b *bufferBuilder[T]) appendSlice(values []T) {
	b.execBufBuilder.append(exec.GetBytes(values))
}

func (b *bufferBuilder[T]) cap() int {
	return cap(b.data) / int(unsafe.Sizeof(b.zero))
}

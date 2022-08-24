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
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/internal/bitutils"
	"golang.org/x/exp/constraints"
)

func CastIntToInt(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	opts := ctx.State.(CastOptions)
	if !opts.AllowIntOverflow {
		if err := intsCanFit(&batch.Values[0].Array, out.Type.ID()); err != nil {
			return err
		}
	}
	castNumberToNumberUnsafe(&batch.Values[0].Array, out)
	return nil
}

func CastFloatingToFloating(_ *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	castNumberToNumberUnsafe(&batch.Values[0].Array, out)
	return nil
}

func CastFloatingToInteger(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	opts := ctx.State.(CastOptions)
	castNumberToNumberUnsafe(&batch.Values[0].Array, out)
	if !opts.AllowFloatTruncate {
		return checkFloatToIntTrunc(&batch.Values[0].Array, out)
	}
	return nil
}

func CastIntegerToFloating(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	opts := ctx.State.(CastOptions)
	if !opts.AllowFloatTruncate {
		if err := checkIntToFloatTrunc(&batch.Values[0].Array, out.Type.ID()); err != nil {
			return err
		}
	}
	castNumberToNumberUnsafe(&batch.Values[0].Array, out)
	return nil
}

func boolToNum[T numeric](_ *exec.KernelCtx, in []byte, out []T) error {
	var (
		zero T
		one  = T(1)
	)

	for i := range out {
		if bitutil.BitIsSet(in, i) {
			out[i] = one
		} else {
			out[i] = zero
		}
	}
	return nil
}

func checkFloatTrunc[InT constraints.Float, OutT exec.IntTypes | exec.UintTypes](in, out *exec.ArraySpan) error {
	wasTrunc := func(out OutT, in InT) bool {
		return InT(out) != in
	}
	wasTruncMaybeNull := func(out OutT, in InT, isValid bool) bool {
		return isValid && (InT(out) != in)
	}
	getError := func(val InT) error {
		return fmt.Errorf("%w: float value %f was truncated converting to %s",
			arrow.ErrInvalid, val, out.Type)
	}

	inData := exec.GetSpanValues[InT](in, 1)
	outData := exec.GetSpanValues[OutT](out, 1)

	bitmap := in.Buffers[0].Buf
	bitCounter := bitutils.NewOptionalBitBlockCounter(bitmap, in.Offset, in.Len)
	pos, offsetPos := int64(0), int64(0)
	for pos < in.Len {
		block := bitCounter.NextBlock()
		outOfBounds := false
		if block.Popcnt == block.Len {
			// fast path: branchless
			for i := 0; i < int(block.Len); i++ {
				outOfBounds = outOfBounds || wasTrunc(outData[i], inData[i])
			}
		} else if block.Popcnt > 0 {
			// must only bounds check non-null
			for i := 0; i < int(block.Len); i++ {
				outOfBounds = outOfBounds || wasTruncMaybeNull(outData[i], inData[i], bitutil.BitIsSet(bitmap, int(offsetPos)+i))
			}
		}
		if outOfBounds {
			if in.Nulls > 0 {
				for i := 0; i < int(block.Len); i++ {
					if wasTruncMaybeNull(outData[i], inData[i], bitutil.BitIsSet(bitmap, int(offsetPos)+i)) {
						return getError(inData[i])
					}
				}
			} else {
				for i := 0; i < int(block.Len); i++ {
					if wasTrunc(outData[i], inData[i]) {
						return getError(inData[i])
					}
				}
			}
		}
		inData = inData[block.Len:]
		outData = outData[block.Len:]
		pos += int64(block.Len)
		offsetPos += int64(block.Len)
	}
	return nil
}

func checkFloatToIntTruncImpl[T constraints.Float](in, out *exec.ArraySpan) error {
	switch out.Type.ID() {
	case arrow.INT8:
		return checkFloatTrunc[T, int8](in, out)
	case arrow.UINT8:
		return checkFloatTrunc[T, uint8](in, out)
	case arrow.INT16:
		return checkFloatTrunc[T, int16](in, out)
	case arrow.UINT16:
		return checkFloatTrunc[T, uint16](in, out)
	case arrow.INT32:
		return checkFloatTrunc[T, int32](in, out)
	case arrow.UINT32:
		return checkFloatTrunc[T, uint32](in, out)
	case arrow.INT64:
		return checkFloatTrunc[T, int64](in, out)
	case arrow.UINT64:
		return checkFloatTrunc[T, uint64](in, out)
	}
	debug.Assert(false, "float to int truncation only for integer output")
	return nil
}

func checkFloatToIntTrunc(in, out *exec.ArraySpan) error {
	switch in.Type.ID() {
	case arrow.FLOAT32:
		return checkFloatToIntTruncImpl[float32](in, out)
	case arrow.FLOAT64:
		return checkFloatToIntTruncImpl[float64](in, out)
	}
	debug.Assert(false, "float to int truncation only for float32 and float64")
	return nil
}

func checkIntToFloatTrunc(in *exec.ArraySpan, outType arrow.Type) error {
	switch in.Type.ID() {
	case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16:
		// small integers are all exactly representable as whole numbers
		return nil
	case arrow.INT32:
		if outType == arrow.FLOAT64 {
			return nil
		}
		const limit = int32(1 << 24)
		return intsInRange(in, -limit, limit)
	case arrow.UINT32:
		if outType == arrow.FLOAT64 {
			return nil
		}
		return intsInRange(in, 0, uint32(1<<24))
	case arrow.INT64:
		if outType == arrow.FLOAT32 {
			const limit = int64(1 << 24)
			return intsInRange(in, -limit, limit)
		}
		const limit = int64(1 << 53)
		return intsInRange(in, -limit, limit)
	case arrow.UINT64:
		if outType == arrow.FLOAT32 {
			return intsInRange(in, 0, uint64(1<<24))
		}
		return intsInRange(in, 0, uint64(1<<53))
	}
	debug.Assert(false, "intToFloatTrunc should only be called with int input")
	return nil
}

func addCommonNumberCasts[T numeric](outTy arrow.DataType, kernels []exec.ScalarKernel) []exec.ScalarKernel {
	kernels = append(kernels, GetCommonCastKernels(outTy.ID(), outTy)...)

	kernels = append(kernels, exec.NewScalarKernel(
		[]exec.InputType{exec.NewExactInput(arrow.FixedWidthTypes.Boolean)},
		exec.NewOutputType(outTy), ScalarUnaryBoolArg[T](boolToNum[T]), nil))

	// generatevarbinarybase
	return kernels
}

func GetCastToInteger[T exec.IntTypes | exec.UintTypes](outType arrow.DataType) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)

	output := exec.NewOutputType(outType)
	for _, inTy := range intTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(inTy)}, output,
			CastIntToInt, nil))
	}

	for _, inTy := range floatingTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(inTy)}, output,
			CastFloatingToInteger, nil))
	}

	kernels = addCommonNumberCasts[T](outType, kernels)
	// from decimal to integer
	return kernels
}

func GetCastToFloating[T constraints.Float](outType arrow.DataType) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)

	output := exec.NewOutputType(outType)
	for _, inTy := range intTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(inTy)}, output,
			CastIntegerToFloating, nil))
	}

	for _, inTy := range floatingTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(inTy)}, output,
			CastFloatingToFloating, nil))
	}

	kernels = addCommonNumberCasts[T](outType, kernels)

	// from decimal to floating
	return kernels
}

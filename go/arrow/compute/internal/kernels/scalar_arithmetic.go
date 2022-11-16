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
	"time"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/decimal256"
)

// scalar kernel that ignores (assumed all-null inputs) and returns null
func NullToNullExec(_ *exec.KernelCtx, _ *exec.ExecSpan, _ *exec.ExecResult) error {
	return nil
}

func NullExecKernel(nargs int) exec.ScalarKernel {
	in := make([]exec.InputType, nargs)
	for i := range in {
		in[i] = exec.NewIDInput(arrow.NULL)
	}
	return exec.NewScalarKernel(in, exec.NewOutputType(arrow.Null), NullToNullExec, nil)
}

func GetArithmeticFunctionTimeDuration(op ArithmeticOp) []exec.ScalarKernel {
	mult := (time.Hour * 24)
	return []exec.ScalarKernel{exec.NewScalarKernel([]exec.InputType{
		exec.NewExactInput(arrow.FixedWidthTypes.Time32s),
		exec.NewExactInput(&arrow.DurationType{Unit: arrow.Second})}, OutputFirstType,
		timeDurationOp[arrow.Time32, arrow.Time32, arrow.Duration](int64(mult.Seconds()), op), nil),
		exec.NewScalarKernel([]exec.InputType{
			exec.NewExactInput(arrow.FixedWidthTypes.Time32ms),
			exec.NewExactInput(&arrow.DurationType{Unit: arrow.Millisecond})}, OutputFirstType,
			timeDurationOp[arrow.Time32, arrow.Time32, arrow.Duration](int64(mult.Milliseconds()), op), nil),
		exec.NewScalarKernel([]exec.InputType{
			exec.NewExactInput(arrow.FixedWidthTypes.Time64us),
			exec.NewExactInput(&arrow.DurationType{Unit: arrow.Microsecond})}, OutputFirstType,
			timeDurationOp[arrow.Time64, arrow.Time64, arrow.Duration](int64(mult.Microseconds()), op), nil),
		exec.NewScalarKernel([]exec.InputType{
			exec.NewExactInput(arrow.FixedWidthTypes.Time64ns),
			exec.NewExactInput(&arrow.DurationType{Unit: arrow.Nanosecond})}, OutputFirstType,
			timeDurationOp[arrow.Time64, arrow.Time64, arrow.Duration](int64(mult.Nanoseconds()), op), nil)}
}

func GetDecimalBinaryKernels(op ArithmeticOp) []exec.ScalarKernel {
	var outType exec.OutputType
	switch op {
	case OpAdd, OpSub, OpAddChecked, OpSubChecked:
		outType = exec.NewComputedOutputType(resolveDecimalAddOrSubtractType)
	case OpMul, OpMulChecked:
		outType = exec.NewComputedOutputType(resolveDecimalMultiplyOutput)
	case OpDiv, OpDivChecked:
		outType = exec.NewComputedOutputType(resolveDecimalDivideOutput)
	}

	in128, in256 := exec.NewIDInput(arrow.DECIMAL128), exec.NewIDInput(arrow.DECIMAL256)
	exec128, exec256 := getArithmeticDecimal[decimal128.Num](op), getArithmeticDecimal[decimal256.Num](op)
	return []exec.ScalarKernel{
		exec.NewScalarKernel([]exec.InputType{in128, in128}, outType, exec128, nil),
		exec.NewScalarKernel([]exec.InputType{in256, in256}, outType, exec256, nil),
	}
}

func GetArithmeticBinaryKernels(op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)
	for _, ty := range numericTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty), exec.NewExactInput(ty)},
			exec.NewOutputType(ty), ArithmeticExecSameType(ty.ID(), op), nil))
	}

	return append(kernels, NullExecKernel(2))
}

func GetDecimalUnaryKernels(op ArithmeticOp) []exec.ScalarKernel {
	outType := OutputFirstType
	in128 := exec.NewIDInput(arrow.DECIMAL128)
	in256 := exec.NewIDInput(arrow.DECIMAL256)

	exec128, exec256 := getArithmeticDecimal[decimal128.Num](op), getArithmeticDecimal[decimal256.Num](op)
	return []exec.ScalarKernel{
		exec.NewScalarKernel([]exec.InputType{in128}, outType, exec128, nil),
		exec.NewScalarKernel([]exec.InputType{in256}, outType, exec256, nil),
	}
}

func GetArithmeticUnaryKernels(op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)
	for _, ty := range numericTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty)}, exec.NewOutputType(ty),
			ArithmeticExec(ty.ID(), ty.ID(), op), nil))
	}

	return append(kernels, NullExecKernel(1))
}

func GetArithmeticUnarySignedKernels(op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)
	for _, ty := range append(signedIntTypes, floatingTypes...) {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty)}, exec.NewOutputType(ty),
			ArithmeticExec(ty.ID(), ty.ID(), op), nil))
	}

	return append(kernels, NullExecKernel(1))
}

func GetArithmeticUnaryFloatingPointKernels(op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)
	for _, ty := range floatingTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty)}, exec.NewOutputType(ty),
			ArithmeticExec(ty.ID(), ty.ID(), op), nil))
	}

	return append(kernels, NullExecKernel(1))
}

func GetArithmeticUnaryFixedIntOutKernels(otype arrow.DataType, op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)

	out := exec.NewOutputType(otype)
	for _, ty := range numericTypes {
		otype := otype
		out := out
		if arrow.IsFloating(ty.ID()) {
			otype = ty
			out = exec.NewOutputType(ty)
		}

		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty)}, out,
			ArithmeticExec(ty.ID(), otype.ID(), op), nil))
	}

	kernels = append(kernels, exec.NewScalarKernel(
		[]exec.InputType{exec.NewIDInput(arrow.DECIMAL128)},
		exec.NewOutputType(arrow.PrimitiveTypes.Int64),
		getArithmeticDecimal[decimal128.Num](op), nil))
	kernels = append(kernels, exec.NewScalarKernel(
		[]exec.InputType{exec.NewIDInput(arrow.DECIMAL256)},
		exec.NewOutputType(arrow.PrimitiveTypes.Int64),
		getArithmeticDecimal[decimal256.Num](op), nil))

	return append(kernels, NullExecKernel(1))
}

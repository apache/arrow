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
	"context"
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/kernels"
)

type arithmeticFunction struct {
	ScalarFunction

	promote decimalPromotion
}

func (fn *arithmeticFunction) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, fn, opts, -1, args...)
}

func (fn *arithmeticFunction) checkDecimals(vals ...arrow.DataType) error {
	if !hasDecimal(vals...) {
		return nil
	}

	if len(vals) != 2 {
		return nil
	}

	if fn.promote == decPromoteNone {
		return fmt.Errorf("%w: invalid decimal function: %s", arrow.ErrInvalid, fn.name)
	}

	return castBinaryDecimalArgs(fn.promote, vals...)
}

func (fn *arithmeticFunction) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
	if err := fn.checkArity(len(vals)); err != nil {
		return nil, err
	}

	if err := fn.checkDecimals(vals...); err != nil {
		return nil, err
	}

	if kn, err := fn.DispatchExact(vals...); err == nil {
		return kn, nil
	}

	ensureDictionaryDecoded(vals...)

	// only promote types for binary funcs
	if len(vals) == 2 {
		replaceNullWithOtherType(vals...)
		if unit, istime := commonTemporalResolution(vals...); istime {
			replaceTemporalTypes(unit, vals...)
		} else {
			if dt := commonNumeric(vals...); dt != nil {
				replaceTypes(dt, vals...)
			}
		}
	}

	return fn.DispatchExact(vals...)
}

// an arithmetic function which promotes integers and decimal
// arguments to doubles.
type arithmeticFloatingPointFunc struct {
	arithmeticFunction
}

func (fn *arithmeticFloatingPointFunc) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, fn, opts, -1, args...)
}

func (fn *arithmeticFloatingPointFunc) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
	if err := fn.checkArity(len(vals)); err != nil {
		return nil, err
	}

	if kn, err := fn.DispatchExact(vals...); err == nil {
		return kn, nil
	}

	ensureDictionaryDecoded(vals...)

	if len(vals) == 2 {
		replaceNullWithOtherType(vals...)
	}

	for i, v := range vals {
		if arrow.IsInteger(v.ID()) || arrow.IsDecimal(v.ID()) {
			vals[i] = arrow.PrimitiveTypes.Float64
		}
	}

	if dt := commonNumeric(vals...); dt != nil {
		replaceTypes(dt, vals...)
	}

	return fn.DispatchExact(vals...)
}

var (
	addDoc FunctionDoc
)

func RegisterScalarArithmetic(reg FunctionRegistry) {
	ops := []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"add_unchecked", kernels.OpAdd, decPromoteAdd},
		{"add", kernels.OpAddChecked, decPromoteAdd},
	}

	for _, o := range ops {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), addDoc), o.decPromote}
		kns := append(kernels.GetArithmeticBinaryKernels(o.op), kernels.GetDecimalBinaryKernels(o.op)...)
		kns = append(kns, kernels.GetArithmeticFunctionTimeDuration(o.op)...)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}

		for _, unit := range arrow.TimeUnitValues {
			inType := exec.NewMatchedInput(exec.TimestampTypeUnit(unit))
			inDuration := exec.NewExactInput(&arrow.DurationType{Unit: unit})
			ex := kernels.ArithmeticExecSameType(arrow.TIMESTAMP, o.op)
			err := fn.AddNewKernel([]exec.InputType{inType, inDuration}, kernels.OutputFirstType, ex, nil)
			if err != nil {
				panic(err)
			}
			err = fn.AddNewKernel([]exec.InputType{inDuration, inType}, kernels.OutputLastType, ex, nil)
			if err != nil {
				panic(err)
			}

			matchDur := exec.NewMatchedInput(exec.DurationTypeUnit(unit))
			ex = kernels.ArithmeticExecSameType(arrow.DURATION, o.op)
			err = fn.AddNewKernel([]exec.InputType{matchDur, matchDur}, exec.NewOutputType(&arrow.DurationType{Unit: unit}), ex, nil)
			if err != nil {
				panic(err)
			}
		}

		reg.AddFunction(fn, false)
	}

	ops = []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"sub_unchecked", kernels.OpSub, decPromoteAdd},
		{"sub", kernels.OpSubChecked, decPromoteAdd},
	}

	for _, o := range ops {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), addDoc), o.decPromote}
		kns := append(kernels.GetArithmeticBinaryKernels(o.op), kernels.GetDecimalBinaryKernels(o.op)...)
		kns = append(kns, kernels.GetArithmeticFunctionTimeDuration(o.op)...)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}

		for _, unit := range arrow.TimeUnitValues {
			// timestamp - timestamp => duration
			inType := exec.NewMatchedInput(exec.TimestampTypeUnit(unit))
			ex := kernels.ArithmeticExecSameType(arrow.TIMESTAMP, o.op)
			err := fn.AddNewKernel([]exec.InputType{inType, inType}, kernels.OutputResolveTemporal, ex, nil)
			if err != nil {
				panic(err)
			}

			// timestamp - duration => timestamp
			inDuration := exec.NewExactInput(&arrow.DurationType{Unit: unit})
			ex = kernels.ArithmeticExecSameType(arrow.TIMESTAMP, o.op)
			err = fn.AddNewKernel([]exec.InputType{inType, inDuration}, kernels.OutputFirstType, ex, nil)
			if err != nil {
				panic(err)
			}

			// duration - duration = duration
			matchDur := exec.NewMatchedInput(exec.DurationTypeUnit(unit))
			ex = kernels.ArithmeticExecSameType(arrow.DURATION, o.op)
			err = fn.AddNewKernel([]exec.InputType{matchDur, matchDur}, exec.NewOutputType(&arrow.DurationType{Unit: unit}), ex, nil)
			if err != nil {
				panic(err)
			}
		}

		// time32 - time32 = duration
		for _, unit := range []arrow.TimeUnit{arrow.Second, arrow.Millisecond} {
			inType := exec.NewMatchedInput(exec.Time32TypeUnit(unit))
			internalEx := kernels.ArithmeticExecSameType(arrow.TIME32, o.op)
			ex := func(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
				if err := internalEx(ctx, batch, out); err != nil {
					return err
				}
				// the allocated space is for duration (an int64) but we
				// wrote the time32 - time32 as if the output was time32
				// so a quick copy in reverse expands the int32s to int64.
				rawData := exec.GetData[int32](out.Buffers[1].Buf)
				outData := exec.GetData[int64](out.Buffers[1].Buf)

				for i := out.Len - 1; i >= 0; i-- {
					outData[i] = int64(rawData[i])
				}
				return nil
			}

			err := fn.AddNewKernel([]exec.InputType{inType, inType},
				exec.NewOutputType(&arrow.DurationType{Unit: unit}), ex, nil)
			if err != nil {
				panic(err)
			}
		}

		// time64 - time64 = duration
		for _, unit := range []arrow.TimeUnit{arrow.Microsecond, arrow.Nanosecond} {
			inType := exec.NewMatchedInput(exec.Time64TypeUnit(unit))
			ex := kernels.ArithmeticExecSameType(arrow.TIME64, o.op)
			err := fn.AddNewKernel([]exec.InputType{inType, inType}, exec.NewOutputType(&arrow.DurationType{Unit: unit}), ex, nil)
			if err != nil {
				panic(err)
			}
		}

		inDate32 := exec.NewExactInput(arrow.FixedWidthTypes.Date32)
		ex := kernels.SubtractDate32(o.op)
		err := fn.AddNewKernel([]exec.InputType{inDate32, inDate32}, exec.NewOutputType(arrow.FixedWidthTypes.Duration_s), ex, nil)
		if err != nil {
			panic(err)
		}

		inDate64 := exec.NewExactInput(arrow.FixedWidthTypes.Date64)
		ex = kernels.ArithmeticExecSameType(arrow.DATE64, o.op)
		err = fn.AddNewKernel([]exec.InputType{inDate64, inDate64}, exec.NewOutputType(arrow.FixedWidthTypes.Duration_ms), ex, nil)
		if err != nil {
			panic(err)
		}

		reg.AddFunction(fn, false)
	}

	oplist := []struct {
		funcName    string
		op          kernels.ArithmeticOp
		decPromote  decimalPromotion
		commutative bool
	}{
		{"multiply_unchecked", kernels.OpMul, decPromoteMultiply, true},
		{"multiply", kernels.OpMulChecked, decPromoteMultiply, true},
		{"divide_unchecked", kernels.OpDiv, decPromoteDivide, false},
		{"divide", kernels.OpDivChecked, decPromoteDivide, false},
	}

	for _, o := range oplist {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), addDoc), o.decPromote}
		for _, k := range append(kernels.GetArithmeticBinaryKernels(o.op), kernels.GetDecimalBinaryKernels(o.op)...) {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}

		for _, unit := range arrow.TimeUnitValues {
			durInput := exec.NewExactInput(&arrow.DurationType{Unit: unit})
			i64Input := exec.NewExactInput(arrow.PrimitiveTypes.Int64)
			durOutput := exec.NewOutputType(&arrow.DurationType{Unit: unit})
			ex := kernels.ArithmeticExecSameType(arrow.DURATION, o.op)
			err := fn.AddNewKernel([]exec.InputType{durInput, i64Input}, durOutput, ex, nil)
			if err != nil {
				panic(err)
			}
			if o.commutative {
				err = fn.AddNewKernel([]exec.InputType{i64Input, durInput}, durOutput, ex, nil)
				if err != nil {
					panic(err)
				}
			}
		}

		reg.AddFunction(fn, false)
	}

	ops = []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"abs_unchecked", kernels.OpAbsoluteValue, decPromoteNone},
		{"abs", kernels.OpAbsoluteValueChecked, decPromoteNone},
		{"negate_unchecked", kernels.OpNegate, decPromoteNone},
	}

	for _, o := range ops {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Unary(), addDoc), decPromoteNone}
		kns := append(kernels.GetArithmeticUnaryKernels(o.op), kernels.GetDecimalUnaryKernels(o.op)...)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}

		reg.AddFunction(fn, false)
	}

	fn := &arithmeticFunction{*NewScalarFunction("negate", Unary(), addDoc), decPromoteNone}
	kns := append(kernels.GetArithmeticUnarySignedKernels(kernels.OpNegateChecked), kernels.GetDecimalUnaryKernels(kernels.OpNegateChecked)...)
	for _, k := range kns {
		if err := fn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(fn, false)

	ops = []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"sqrt_unchecked", kernels.OpSqrt, decPromoteNone},
		{"sqrt", kernels.OpSqrtChecked, decPromoteNone},
	}

	for _, o := range ops {
		fn := &arithmeticFloatingPointFunc{arithmeticFunction{*NewScalarFunction(o.funcName, Unary(), addDoc), decPromoteNone}}
		kns := kernels.GetArithmeticUnaryFloatingPointKernels(o.op)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}

		reg.AddFunction(fn, false)
	}

	fn = &arithmeticFunction{*NewScalarFunction("sign", Unary(), addDoc), decPromoteNone}
	kns = kernels.GetArithmeticUnaryFixedIntOutKernels(arrow.PrimitiveTypes.Int8, kernels.OpSign)
	for _, k := range kns {
		if err := fn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(fn, false)
}

func impl(ctx context.Context, fn string, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, left, right)
}

// Add performs an addition between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is added to each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if NoCheckOverflow is false (default).
func Add(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	return impl(ctx, "add", opts, left, right)
}

// Sub performs a subtraction between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is subtracted from each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if NoCheckOverflow is false (default).
func Subtract(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	return impl(ctx, "sub", opts, left, right)
}

// Multiply performs a multiplication between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is multiplied against each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if NoCheckOverflow is false (default).
func Multiply(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	return impl(ctx, "multiply", opts, left, right)
}

// Divide performs a division between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is used with each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if NoCheckOverflow is false (default).
//
// Will error on divide by zero regardless of whether or not checking for
// overflows.
func Divide(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	return impl(ctx, "divide", opts, left, right)
}

// AbsoluteValue returns the AbsoluteValue for each element in the input
// argument. It accepts either a scalar or an array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if CheckOverflow is true.
func AbsoluteValue(ctx context.Context, opts ArithmeticOptions, input Datum) (Datum, error) {
	fn := "abs"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, input)
}

// Negate returns a result containing the negation of each element in the
// input argument. It accepts either a scalar or an array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// or to throw an error on unsigned types.
func Negate(ctx context.Context, opts ArithmeticOptions, input Datum) (Datum, error) {
	fn := "negate"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, input)
}

// Sign returns -1, 0, or 1 depending on the sign of each element in the
// input. For x in the input:
//
//	if x > 0: 1
//  if x < 0: -1
//  if x == 0: 0
//
func Sign(ctx context.Context, input Datum) (Datum, error) {
	return CallFunction(ctx, "sign", nil, input)
}

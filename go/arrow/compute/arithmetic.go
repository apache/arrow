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
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/decimal256"
	"github.com/apache/arrow/go/v11/arrow/scalar"
)

type (
	RoundOptions           = kernels.RoundOptions
	RoundMode              = kernels.RoundMode
	RoundToMultipleOptions = kernels.RoundToMultipleOptions
)

const (
	// Round to nearest integer less than or equal in magnitude (aka "floor")
	RoundDown = kernels.RoundDown
	// Round to nearest integer greater than or equal in magnitude (aka "ceil")
	RoundUp = kernels.RoundUp
	// Get integral part without fractional digits (aka "trunc")
	RoundTowardsZero = kernels.TowardsZero
	// Round negative values with DOWN and positive values with UP
	RoundTowardsInfinity = kernels.AwayFromZero
	// Round ties with DOWN (aka "round half towards negative infinity")
	RoundHalfDown = kernels.HalfDown
	// Round ties with UP (aka "round half towards positive infinity")
	RoundHalfUp = kernels.HalfUp
	// Round ties with TowardsZero (aka "round half away from infinity")
	RoundHalfTowardsZero = kernels.HalfTowardsZero
	// Round ties with AwayFromZero (aka "round half towards infinity")
	RoundHalfTowardsInfinity = kernels.HalfAwayFromZero
	// Round ties to nearest even integer
	RoundHalfToEven = kernels.HalfToEven
	// Round ties to nearest odd integer
	RoundHalfToOdd = kernels.HalfToOdd
)

var (
	DefaultRoundOptions           = RoundOptions{NDigits: 0, Mode: RoundHalfToEven}
	DefaultRoundToMultipleOptions = RoundToMultipleOptions{
		Multiple: scalar.NewFloat64Scalar(1), Mode: RoundHalfToEven}
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

// function that promotes only decimal arguments to float64
type arithmeticDecimalToFloatingPointFunc struct {
	arithmeticFunction
}

func (fn *arithmeticDecimalToFloatingPointFunc) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, fn, opts, -1, args...)
}

func (fn *arithmeticDecimalToFloatingPointFunc) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
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

	for i, t := range vals {
		if arrow.IsDecimal(t.ID()) {
			vals[i] = arrow.PrimitiveTypes.Float64
		}
	}

	if dt := commonNumeric(vals...); dt != nil {
		replaceTypes(dt, vals...)
	}

	return fn.DispatchExact(vals...)
}

// function that promotes only integer arguments to float64
type arithmeticIntegerToFloatingPointFunc struct {
	arithmeticFunction
}

func (fn *arithmeticIntegerToFloatingPointFunc) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, fn, opts, -1, args...)
}

func (fn *arithmeticIntegerToFloatingPointFunc) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
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
	if len(vals) == 2 {
		replaceNullWithOtherType(vals...)
	}

	for i, t := range vals {
		if arrow.IsInteger(t.ID()) {
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
		{"sin_unchecked", kernels.OpSin, decPromoteNone},
		{"sin", kernels.OpSinChecked, decPromoteNone},
		{"cos_unchecked", kernels.OpCos, decPromoteNone},
		{"cos", kernels.OpCosChecked, decPromoteNone},
		{"tan_unchecked", kernels.OpTan, decPromoteNone},
		{"tan", kernels.OpTanChecked, decPromoteNone},
		{"asin_unchecked", kernels.OpAsin, decPromoteNone},
		{"asin", kernels.OpAsinChecked, decPromoteNone},
		{"acos_unchecked", kernels.OpAcos, decPromoteNone},
		{"acos", kernels.OpAcosChecked, decPromoteNone},
		{"atan", kernels.OpAtan, decPromoteNone},
		{"ln_unchecked", kernels.OpLn, decPromoteNone},
		{"ln", kernels.OpLnChecked, decPromoteNone},
		{"log10_unchecked", kernels.OpLog10, decPromoteNone},
		{"log10", kernels.OpLog10Checked, decPromoteNone},
		{"log2_unchecked", kernels.OpLog2, decPromoteNone},
		{"log2", kernels.OpLog2Checked, decPromoteNone},
		{"log1p_unchecked", kernels.OpLog1p, decPromoteNone},
		{"log1p", kernels.OpLog1pChecked, decPromoteNone},
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

	ops = []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"atan2", kernels.OpAtan2, decPromoteNone},
		{"logb_unchecked", kernels.OpLogb, decPromoteNone},
		{"logb", kernels.OpLogbChecked, decPromoteNone},
	}

	for _, o := range ops {
		fn := &arithmeticFloatingPointFunc{arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), addDoc), decPromoteNone}}
		kns := kernels.GetArithmeticFloatingPointKernels(o.op)
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

	ops = []struct {
		funcName   string
		op         kernels.ArithmeticOp
		decPromote decimalPromotion
	}{
		{"power_unchecked", kernels.OpPower, decPromoteNone},
		{"power", kernels.OpPowerChecked, decPromoteNone},
	}

	for _, o := range ops {
		fn := &arithmeticDecimalToFloatingPointFunc{arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), EmptyFuncDoc), o.decPromote}}
		kns := kernels.GetArithmeticBinaryKernels(o.op)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}
		reg.AddFunction(fn, false)
	}

	bitWiseOps := []struct {
		funcName string
		op       kernels.BitwiseOp
	}{
		{"bit_wise_and", kernels.OpBitAnd},
		{"bit_wise_or", kernels.OpBitOr},
		{"bit_wise_xor", kernels.OpBitXor},
	}

	for _, o := range bitWiseOps {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), EmptyFuncDoc), decPromoteNone}
		kns := kernels.GetBitwiseBinaryKernels(o.op)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}
		reg.AddFunction(fn, false)
	}

	fn = &arithmeticFunction{*NewScalarFunction("bit_wise_not", Unary(), EmptyFuncDoc), decPromoteNone}
	for _, k := range kernels.GetBitwiseUnaryKernels() {
		if err := fn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(fn, false)

	shiftOps := []struct {
		funcName string
		dir      kernels.ShiftDir
		checked  bool
	}{
		{"shift_left", kernels.ShiftLeft, true},
		{"shift_left_unchecked", kernels.ShiftLeft, false},
		{"shift_right", kernels.ShiftRight, true},
		{"shift_right_unchecked", kernels.ShiftRight, false},
	}

	for _, o := range shiftOps {
		fn := &arithmeticFunction{*NewScalarFunction(o.funcName, Binary(), EmptyFuncDoc), decPromoteNone}
		kns := kernels.GetShiftKernels(o.dir, o.checked)
		for _, k := range kns {
			if err := fn.AddKernel(k); err != nil {
				panic(err)
			}
		}
		reg.AddFunction(fn, false)
	}

	floorFn := &arithmeticIntegerToFloatingPointFunc{arithmeticFunction{*NewScalarFunction("floor", Unary(), EmptyFuncDoc), decPromoteNone}}
	kns = kernels.GetSimpleRoundKernels(kernels.RoundDown)
	for _, k := range kns {
		if err := floorFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	floorFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL128)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal128.Num](kernels.RoundDown), nil)
	floorFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL256)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal256.Num](kernels.RoundDown), nil)
	reg.AddFunction(floorFn, false)

	ceilFn := &arithmeticIntegerToFloatingPointFunc{arithmeticFunction{*NewScalarFunction("ceil", Unary(), EmptyFuncDoc), decPromoteNone}}
	kns = kernels.GetSimpleRoundKernels(kernels.RoundUp)
	for _, k := range kns {
		if err := ceilFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	ceilFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL128)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal128.Num](kernels.RoundUp), nil)
	ceilFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL256)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal256.Num](kernels.RoundUp), nil)
	reg.AddFunction(ceilFn, false)

	truncFn := &arithmeticIntegerToFloatingPointFunc{arithmeticFunction{*NewScalarFunction("trunc", Unary(), EmptyFuncDoc), decPromoteNone}}
	kns = kernels.GetSimpleRoundKernels(kernels.TowardsZero)
	for _, k := range kns {
		if err := truncFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	truncFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL128)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal128.Num](kernels.TowardsZero), nil)
	truncFn.AddNewKernel([]exec.InputType{exec.NewIDInput(arrow.DECIMAL256)},
		kernels.OutputFirstType, kernels.FixedRoundDecimalExec[decimal256.Num](kernels.TowardsZero), nil)
	reg.AddFunction(truncFn, false)

	roundFn := &arithmeticIntegerToFloatingPointFunc{arithmeticFunction{*NewScalarFunction("round", Unary(), EmptyFuncDoc), decPromoteNone}}
	kns = kernels.GetRoundUnaryKernels(kernels.InitRoundState, kernels.UnaryRoundExec)
	for _, k := range kns {
		if err := roundFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	roundFn.defaultOpts = DefaultRoundOptions
	reg.AddFunction(roundFn, false)

	roundToMultipleFn := &arithmeticIntegerToFloatingPointFunc{arithmeticFunction{*NewScalarFunction("round_to_multiple", Unary(), EmptyFuncDoc), decPromoteNone}}
	kns = kernels.GetRoundUnaryKernels(kernels.InitRoundToMultipleState, kernels.UnaryRoundToMultipleExec)
	for _, k := range kns {
		if err := roundToMultipleFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	roundToMultipleFn.defaultOpts = DefaultRoundToMultipleOptions
	reg.AddFunction(roundToMultipleFn, false)
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

// Power returns base**exp for each element in the input arrays. Should work
// for both Arrays and Scalars
func Power(ctx context.Context, opts ArithmeticOptions, base, exp Datum) (Datum, error) {
	fn := "power"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, base, exp)
}

// ShiftLeft only accepts integral types and shifts each element of the
// first argument to the left by the value of the corresponding element
// in the second argument.
//
// The value to shift by should be >= 0 and < precision of the type.
func ShiftLeft(ctx context.Context, opts ArithmeticOptions, lhs, rhs Datum) (Datum, error) {
	fn := "shift_left"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, lhs, rhs)
}

// ShiftRight only accepts integral types and shifts each element of the
// first argument to the right by the value of the corresponding element
// in the second argument.
//
// The value to shift by should be >= 0 and < precision of the type.
func ShiftRight(ctx context.Context, opts ArithmeticOptions, lhs, rhs Datum) (Datum, error) {
	fn := "shift_right"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, lhs, rhs)
}

func Sin(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "sin"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Cos(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "cos"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Tan(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "tan"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Asin(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "asin"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Acos(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "acos"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Atan(ctx context.Context, arg Datum) (Datum, error) {
	return CallFunction(ctx, "atan", nil, arg)
}

func Atan2(ctx context.Context, x, y Datum) (Datum, error) {
	return CallFunction(ctx, "atan2", nil, x, y)
}

func Ln(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "ln"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Log10(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "log10"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Log2(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "log2"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Log1p(ctx context.Context, opts ArithmeticOptions, arg Datum) (Datum, error) {
	fn := "log1p"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, arg)
}

func Logb(ctx context.Context, opts ArithmeticOptions, x, base Datum) (Datum, error) {
	fn := "logb"
	if opts.NoCheckOverflow {
		fn += "_unchecked"
	}
	return CallFunction(ctx, fn, nil, x, base)
}

func Round(ctx context.Context, opts RoundOptions, arg Datum) (Datum, error) {
	return CallFunction(ctx, "round", &opts, arg)
}

func RoundToMultiple(ctx context.Context, opts RoundToMultipleOptions, arg Datum) (Datum, error) {
	return CallFunction(ctx, "round_to_multiple", &opts, arg)
}

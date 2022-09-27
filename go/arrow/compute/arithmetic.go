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
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/kernels"
)

type arithmeticFunction struct {
	ScalarFunction
}

func (fn *arithmeticFunction) checkDecimals(vals ...arrow.DataType) error {
	if !hasDecimal(vals...) {
		return nil
	}

	if len(vals) != 2 {
		return nil
	}

	op := fn.name[:strings.Index(fn.name, "_")]
	switch op {
	case "add", "subtract":
		return castBinaryDecimalArgs(decPromoteAdd, vals...)
	case "multiply":
		return castBinaryDecimalArgs(decPromoteMultiply, vals...)
	case "divide":
		return castBinaryDecimalArgs(decPromoteDivide, vals...)
	default:
		return fmt.Errorf("%w: invalid decimal function: %s", arrow.ErrInvalid, fn.name)
	}
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

var (
	addDoc FunctionDoc
)

func RegisterScalarArithmetic(reg FunctionRegistry) {
	addFn := &arithmeticFunction{*NewScalarFunction("add", Binary(), addDoc)}
	for _, k := range kernels.GetArithmeticKernels(kernels.OpAdd) {
		if err := addFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(addFn, false)

	addCheckedFn := &arithmeticFunction{*NewScalarFunction("add_checked", Binary(), addDoc)}
	for _, k := range kernels.GetArithmeticKernels(kernels.OpAddChecked) {
		if err := addCheckedFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(addCheckedFn, false)

	subFn := &arithmeticFunction{*NewScalarFunction("sub", Binary(), addDoc)}
	for _, k := range kernels.GetArithmeticKernels(kernels.OpSub) {
		if err := subFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(subFn, false)

	subCheckedFn := &arithmeticFunction{*NewScalarFunction("sub_checked", Binary(), addDoc)}
	for _, k := range kernels.GetArithmeticKernels(kernels.OpSubChecked) {
		if err := subCheckedFn.AddKernel(k); err != nil {
			panic(err)
		}
	}

	reg.AddFunction(subCheckedFn, false)
}

// Add performs an addition between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is added to each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if CheckOverflow is true.
func Add(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	fn := "add"
	if opts.CheckOverflow {
		fn = "add_checked"
	}
	return CallFunction(ctx, fn, nil, left, right)
}

// Sub performs a subtraction between the passed in arguments (scalar or array)
// and returns the result. If one argument is a scalar and the other is an
// array, the scalar value is subtracted from each value of the array.
//
// ArithmeticOptions specifies whether or not to check for overflows,
// performance is faster if not explicitly checking for overflows but
// will error on an overflow if CheckOverflow is true.
func Subtract(ctx context.Context, opts ArithmeticOptions, left, right Datum) (Datum, error) {
	fn := "sub"
	if opts.CheckOverflow {
		fn = "sub_checked"
	}
	return CallFunction(ctx, fn, nil, left, right)
}

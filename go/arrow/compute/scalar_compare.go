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

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/kernels"
)

type compareFunction struct {
	ScalarFunction
}

func (fn *compareFunction) Execute(ctx context.Context, opt FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, fn, opt, -1, args...)
}

func (fn *compareFunction) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
	if err := fn.checkArity(len(vals)); err != nil {
		return nil, err
	}

	if hasDecimal(vals...) {
		if err := castBinaryDecimalArgs(decPromoteAdd, vals...); err != nil {
			return nil, err
		}
	}

	if kn, err := fn.DispatchExact(vals...); err == nil {
		return kn, nil
	}

	ensureDictionaryDecoded(vals...)
	replaceNullWithOtherType(vals...)

	if dt := commonNumeric(vals...); dt != nil {
		replaceTypes(dt, vals...)
	} else if dt := commonTemporal(vals...); dt != nil {
		replaceTypes(dt, vals...)
	} else if dt := commonBinary(vals...); dt != nil {
		replaceTypes(dt, vals...)
	}

	return fn.DispatchExact(vals...)
}

func RegisterScalarComparisons(reg FunctionRegistry) {
	eqFn := &compareFunction{*NewScalarFunction("equal", Binary(), EmptyFuncDoc)}
	for _, k := range kernels.CompareKernels(kernels.CmpEQ) {
		if err := eqFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	reg.AddFunction(eqFn, false)

	neqFn := &compareFunction{*NewScalarFunction("not_equal", Binary(), EmptyFuncDoc)}
	for _, k := range kernels.CompareKernels(kernels.CmpNE) {
		if err := neqFn.AddKernel(k); err != nil {
			panic(err)
		}
	}
	reg.AddFunction(neqFn, false)
}

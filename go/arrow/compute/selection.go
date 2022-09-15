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

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/kernels"
)

var (
	filterDoc      FunctionDoc
	filterMetaFunc = NewMetaFunction("filter", Binary(), filterDoc,
		func(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
			if args[1].(ArrayLikeDatum).Type().ID() != arrow.BOOL {
				return nil, fmt.Errorf("%w: fitler argument must be boolean type",
					arrow.ErrNotImplemented)
			}

			switch args[0].Kind() {
			case KindRecord:
				return nil, fmt.Errorf("%w: record batch filtering", arrow.ErrNotImplemented)
			case KindTable:
				return nil, fmt.Errorf("%w: table filtering", arrow.ErrNotImplemented)
			default:
				return CallFunction(ctx, "array_filter", opts, args...)
			}
		})
	takeDoc      FunctionDoc
	takeMetaFunc = NewMetaFunction("take", Binary(), takeDoc,
		func(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
			indexKind := args[1].Kind()
			if indexKind != KindArray && indexKind != KindChunked {
				return nil, fmt.Errorf("%w: unsupported types for take operation: values=%s, indices=%s",
					arrow.ErrNotImplemented, args[0], args[1])
			}

			switch args[0].Kind() {
			case KindArray, KindChunked:
				return CallFunction(ctx, "array_take", opts, args...)
			case KindRecord:
			case KindTable:
			}

			return nil, fmt.Errorf("%w: unsupported types for take operation: values=%s, indices=%s",
				arrow.ErrNotImplemented, args[0], args[1])
		})
)

func Take(ctx context.Context, opts *TakeOptions, values, indices Datum) (Datum, error) {
	return CallFunction(ctx, "array_take", opts, values, indices)
}

func TakeArray(ctx context.Context, values, indices arrow.Array) (arrow.Array, error) {
	v := NewDatum(values)
	idx := NewDatum(indices)
	defer v.Release()
	defer idx.Release()

	out, err := CallFunction(ctx, "array_take", nil, v, idx)
	if err != nil {
		return nil, err
	}
	defer out.Release()

	return out.(*ArrayDatum).MakeArray(), nil
}

// RegisterVectorSelection registers functions that select specific
// values from arrays such as Take and Filter
func RegisterVectorSelection(reg FunctionRegistry) {
	filterMetaFunc.defaultOpts = DefaultFilterOptions()
	takeMetaFunc.defaultOpts = DefaultTakeOptions()
	reg.AddFunction(filterMetaFunc, false)
	reg.AddFunction(takeMetaFunc, false)
	filterKernels, takeKernels := kernels.GetVectorSelectionKernels()

	vfunc := NewVectorFunction("array_filter", Binary(), EmptyFuncDoc)
	vfunc.defaultOpts = &kernels.FilterOptions{}

	selectionType := exec.NewExactInput(arrow.FixedWidthTypes.Boolean)
	basekernel := exec.NewVectorKernelWithSig(nil, nil, exec.OptionsInit[kernels.FilterState])
	for _, kd := range filterKernels {
		basekernel.Signature = &exec.KernelSignature{
			InputTypes: []exec.InputType{kd.In, selectionType},
			OutType:    kernels.OutputFirstType,
		}
		basekernel.ExecFn = kd.Exec
		vfunc.AddKernel(basekernel)
	}
	reg.AddFunction(vfunc, false)

	vfunc = NewVectorFunction("array_take", Binary(), EmptyFuncDoc)
	vfunc.defaultOpts = DefaultTakeOptions()

	selectionType = exec.NewMatchedInput(exec.Integer())
	basekernel = exec.NewVectorKernelWithSig(nil, nil, exec.OptionsInit[kernels.TakeState])
	basekernel.CanExecuteChunkWise = false
	for _, kd := range takeKernels {
		basekernel.Signature = &exec.KernelSignature{
			InputTypes: []exec.InputType{kd.In, selectionType},
			OutType:    kernels.OutputFirstType,
		}
		basekernel.ExecFn = kd.Exec
		vfunc.AddKernel(basekernel)
	}
	reg.AddFunction(vfunc, false)
}

// Filter is a wrapper convenience that is equivalent to calling
// CallFunction(ctx, "filter", &options, values, filter) for filtering
// an input array (values) by a boolean array (filter). The two inputs
// must be the same length.
func Filter(ctx context.Context, values, filter Datum, options FilterOptions) (Datum, error) {
	return CallFunction(ctx, "filter", &options, values, filter)
}

// FilterArray is a convenience method for calling Filter without having
// to manually construct the intervening Datum objects (they will be
// created for you internally here).
func FilterArray(ctx context.Context, values, filter arrow.Array, options FilterOptions) (arrow.Array, error) {
	valDatum := NewDatum(values)
	filterDatum := NewDatum(filter)
	defer valDatum.Release()
	defer filterDatum.Release()

	outDatum, err := Filter(ctx, valDatum, filterDatum, options)
	if err != nil {
		return nil, err
	}

	defer outDatum.Release()
	return outDatum.(*ArrayDatum).MakeArray(), nil
}

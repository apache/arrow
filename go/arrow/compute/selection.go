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
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/kernels"
	"golang.org/x/sync/errgroup"
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

func Take(ctx context.Context, opts TakeOptions, values, indices Datum) (Datum, error) {
	return CallFunction(ctx, "array_take", &opts, values, indices)
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

func TakeArrayOpts(ctx context.Context, values, indices arrow.Array, opts TakeOptions) (arrow.Array, error) {
	v := NewDatum(values)
	idx := NewDatum(indices)
	defer v.Release()
	defer idx.Release()

	out, err := CallFunction(ctx, "array_take", &opts, v, idx)
	if err != nil {
		return nil, err
	}
	defer out.Release()

	return out.(*ArrayDatum).MakeArray(), nil
}

type listArr interface {
	arrow.Array
	ListValues() arrow.Array
}

func selectListImpl(fn exec.ArrayKernelExec) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
		if err := fn(ctx, batch, out); err != nil {
			return err
		}

		// out.Children[0] contains the child indexes of values that we
		// want to take after processing.
		values := batch.Values[0].Array.MakeArray().(listArr)
		defer values.Release()

		childIndices := out.Children[0].MakeArray()
		defer childIndices.Release()

		takenChild, err := TakeArrayOpts(ctx.Ctx, values.ListValues(), childIndices, kernels.TakeOptions{BoundsCheck: false})
		if err != nil {
			return err
		}
		defer takenChild.Release()

		out.Children[0].TakeOwnership(takenChild.Data())
		return nil
	}
}

func denseUnionImpl(fn exec.ArrayKernelExec) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
		if err := fn(ctx, batch, out); err != nil {
			return err
		}

		typedValues := batch.Values[0].Array.MakeArray().(*array.DenseUnion)
		defer typedValues.Release()

		eg, cctx := errgroup.WithContext(ctx.Ctx)
		eg.SetLimit(GetExecCtx(ctx.Ctx).NP)

		for i := 0; i < typedValues.NumFields(); i++ {
			i := i
			eg.Go(func() error {
				arr := typedValues.Field(i)
				childIndices := out.Children[i].MakeArray()
				defer childIndices.Release()
				taken, err := TakeArrayOpts(cctx, arr, childIndices, kernels.TakeOptions{})
				if err != nil {
					return err
				}
				defer taken.Release()
				out.Children[i].TakeOwnership(taken.Data())
				return nil
			})
		}

		return eg.Wait()
	}
}

func extensionFilterImpl(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	extArray := batch.Values[0].Array.MakeArray().(array.ExtensionArray)
	defer extArray.Release()

	selection := batch.Values[1].Array.MakeArray()
	defer selection.Release()
	result, err := FilterArray(ctx.Ctx, extArray.Storage(), selection, FilterOptions(ctx.State.(kernels.FilterState)))
	if err != nil {
		return err
	}
	defer result.Release()

	out.TakeOwnership(result.Data())
	out.Type = extArray.DataType()
	return nil
}

func extensionTakeImpl(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	extArray := batch.Values[0].Array.MakeArray().(array.ExtensionArray)
	defer extArray.Release()

	selection := batch.Values[1].Array.MakeArray()
	defer selection.Release()
	result, err := TakeArrayOpts(ctx.Ctx, extArray.Storage(), selection, TakeOptions(ctx.State.(kernels.TakeState)))
	if err != nil {
		return err
	}
	defer result.Release()

	out.TakeOwnership(result.Data())
	out.Type = extArray.DataType()
	return nil
}

func structFilter(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// transform filter to selection indices and use take
	indices, err := kernels.GetTakeIndices(exec.GetAllocator(ctx.Ctx),
		&batch.Values[1].Array, ctx.State.(kernels.FilterState).NullSelection)
	if err != nil {
		return err
	}
	defer indices.Release()

	filter := NewDatum(indices)
	defer filter.Release()

	valData := batch.Values[0].Array.MakeData()
	defer valData.Release()

	vals := NewDatum(valData)
	defer vals.Release()

	result, err := Take(ctx.Ctx, kernels.TakeOptions{BoundsCheck: false}, vals, filter)
	if err != nil {
		return err
	}
	defer result.Release()

	out.TakeOwnership(result.(*ArrayDatum).Value)
	return nil
}

func structTake(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// generate top level validity bitmap
	if err := kernels.TakeExec(kernels.StructImpl)(ctx, batch, out); err != nil {
		return err
	}

	values := batch.Values[0].Array.MakeArray().(*array.Struct)
	defer values.Release()

	// select from children without bounds checking
	out.Children = make([]exec.ArraySpan, values.NumField())
	eg, cctx := errgroup.WithContext(ctx.Ctx)
	eg.SetLimit(GetExecCtx(ctx.Ctx).NP)

	selection := batch.Values[1].Array.MakeArray()
	defer selection.Release()

	for i := range out.Children {
		i := i
		eg.Go(func() error {
			taken, err := TakeArrayOpts(cctx, values.Field(i), selection, kernels.TakeOptions{BoundsCheck: false})
			if err != nil {
				return err
			}
			defer taken.Release()

			out.Children[i].TakeOwnership(taken.Data())
			return nil
		})
	}

	return eg.Wait()
}

// RegisterVectorSelection registers functions that select specific
// values from arrays such as Take and Filter
func RegisterVectorSelection(reg FunctionRegistry) {
	filterMetaFunc.defaultOpts = DefaultFilterOptions()
	takeMetaFunc.defaultOpts = DefaultTakeOptions()
	reg.AddFunction(filterMetaFunc, false)
	reg.AddFunction(takeMetaFunc, false)
	filterKernels, takeKernels := kernels.GetVectorSelectionKernels()

	filterKernels = append(filterKernels, []kernels.SelectionKernelData{
		{In: exec.NewIDInput(arrow.LIST), Exec: selectListImpl(kernels.FilterExec(kernels.ListImpl[int32]))},
		{In: exec.NewIDInput(arrow.LARGE_LIST), Exec: selectListImpl(kernels.FilterExec(kernels.ListImpl[int64]))},
		{In: exec.NewIDInput(arrow.FIXED_SIZE_LIST), Exec: selectListImpl(kernels.FilterExec(kernels.FSLImpl))},
		{In: exec.NewIDInput(arrow.DENSE_UNION), Exec: denseUnionImpl(kernels.FilterExec(kernels.DenseUnionImpl))},
		{In: exec.NewIDInput(arrow.EXTENSION), Exec: extensionFilterImpl},
		{In: exec.NewIDInput(arrow.STRUCT), Exec: structFilter},
	}...)

	takeKernels = append(takeKernels, []kernels.SelectionKernelData{
		{In: exec.NewIDInput(arrow.LIST), Exec: selectListImpl(kernels.TakeExec(kernels.ListImpl[int32]))},
		{In: exec.NewIDInput(arrow.LARGE_LIST), Exec: selectListImpl(kernels.TakeExec(kernels.ListImpl[int64]))},
		{In: exec.NewIDInput(arrow.FIXED_SIZE_LIST), Exec: selectListImpl(kernels.TakeExec(kernels.FSLImpl))},
		{In: exec.NewIDInput(arrow.DENSE_UNION), Exec: denseUnionImpl(kernels.TakeExec(kernels.DenseUnionImpl))},
		{In: exec.NewIDInput(arrow.EXTENSION), Exec: extensionTakeImpl},
		{In: exec.NewIDInput(arrow.STRUCT), Exec: structTake},
	}...)

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

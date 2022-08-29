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
	"sync"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/kernels"
)

var (
	castTable map[arrow.Type]*castFunction
	castInit  sync.Once

	castDoc = FunctionDoc{
		Summary:         "cast values to another data type",
		Description:     "Behavior when values wouldn't fit in the target type\ncan be controlled through CastOptions.",
		ArgNames:        []string{"input"},
		OptionsType:     "CastOptions",
		OptionsRequired: true,
	}
	castMetaFunc = NewMetaFunction("cast", Unary(), castDoc,
		func(ctx context.Context, fo FunctionOptions, d ...Datum) (Datum, error) {
			castOpts := fo.(*CastOptions)
			if castOpts == nil || castOpts.ToType == nil {
				return nil, fmt.Errorf("%w: cast requires that options be passed with a ToType", arrow.ErrInvalid)
			}

			if arrow.TypeEqual(d[0].(ArrayLikeDatum).Type(), castOpts.ToType) {
				return NewDatum(d[0]), nil
			}

			fn, err := getCastFunction(castOpts.ToType)
			if err != nil {
				return nil, fmt.Errorf("%w from %s", err, d[0].(ArrayLikeDatum).Type())
			}

			return fn.Execute(ctx, fo, d...)
		})
)

func RegisterScalarCast(reg FunctionRegistry) {
	reg.AddFunction(castMetaFunc, false)
}

type castFunction struct {
	ScalarFunction

	inIDs []arrow.Type
	out   arrow.Type
}

func newCastFunction(name string, outType arrow.Type) *castFunction {
	return &castFunction{
		ScalarFunction: *NewScalarFunction(name, Unary(), EmptyFuncDoc),
		out:            outType,
		inIDs:          make([]arrow.Type, 0, 1),
	}
}

func (cf *castFunction) AddTypeCast(in arrow.Type, kernel exec.ScalarKernel) error {
	kernel.Init = exec.OptionsInit[kernels.CastState]
	if err := cf.AddKernel(kernel); err != nil {
		return err
	}
	cf.inIDs = append(cf.inIDs, in)
	return nil
}

func (cf *castFunction) AddNewTypeCast(inID arrow.Type, inTypes []exec.InputType, out exec.OutputType,
	ex exec.ArrayKernelExec, nullHandle exec.NullHandling, memAlloc exec.MemAlloc) error {

	kn := exec.NewScalarKernel(inTypes, out, ex, nil)
	kn.NullHandling = nullHandle
	kn.MemAlloc = memAlloc
	return cf.AddTypeCast(inID, kn)
}

func (cf *castFunction) DispatchExact(vals ...arrow.DataType) (exec.Kernel, error) {
	if err := cf.checkArity(len(vals)); err != nil {
		return nil, err
	}

	candidates := make([]*exec.ScalarKernel, 0, 1)
	for i := range cf.kernels {
		if cf.kernels[i].Signature.MatchesInputs(vals) {
			candidates = append(candidates, &cf.kernels[i])
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: unsupported cast from %s to %s using function %s",
			arrow.ErrNotImplemented, vals[0], cf.out, cf.name)
	}

	if len(candidates) == 1 {
		// one match!
		return candidates[0], nil
	}

	// in this situation we may have both an EXACT type and
	// a SAME_TYPE_ID match. So we will see if there is an exact
	// match among the candidates and if not, we just return the
	// first one
	for _, k := range candidates {
		arg0 := k.Signature.InputTypes[0]
		if arg0.Kind == exec.InputExact {
			// found one!
			return k, nil
		}
	}

	// just return some kernel that matches since we didn't find an exact
	return candidates[0], nil
}

func addCastFuncs(fn []*castFunction) {
	for _, f := range fn {
		castTable[f.out] = f
	}
}

func initCastTable() {
	castTable = make(map[arrow.Type]*castFunction)
	addCastFuncs(getBooleanCasts())
}

func getCastFunction(to arrow.DataType) (*castFunction, error) {
	castInit.Do(initCastTable)

	fn, ok := castTable[to.ID()]
	if ok {
		return fn, nil
	}

	return nil, fmt.Errorf("%w: unsupported cast to %s", arrow.ErrNotImplemented, to)
}

func getBooleanCasts() []*castFunction {
	fn := newCastFunction("cast_boolean", arrow.BOOL)
	kns := kernels.GetBooleanCastKernels()

	for _, k := range kns {
		if err := fn.AddTypeCast(k.Signature.InputTypes[0].Type.ID(), k); err != nil {
			panic(err)
		}
	}

	return []*castFunction{fn}
}

// CastDatum is a convenience function for casting a Datum to another type.
// It is equivalent to calling CallFunction(ctx, "cast", opts, Datum) and
// should work for Scalar, Array or ChunkedArray Datums.
func CastDatum(ctx context.Context, val Datum, opts *CastOptions) (Datum, error) {
	return CallFunction(ctx, "cast", opts, val)
}

// CastArray is a convenience function for casting an Array to another type.
// It is equivalent to constructing a Datum for the array and using
// CallFunction(ctx, "cast", ...).
func CastArray(ctx context.Context, val arrow.Array, opts *CastOptions) (arrow.Array, error) {
	d := NewDatum(val)
	defer d.Release()

	out, err := CastDatum(ctx, d, opts)
	if err != nil {
		return nil, err
	}

	defer out.Release()
	return out.(*ArrayDatum).MakeArray(), nil
}

// CanCast returns true if there is an implementation for casting an array
// or scalar value from the specified DataType to the other data type.
func CanCast(from, to arrow.DataType) bool {
	fn, err := getCastFunction(to)
	if err != nil {
		return false
	}

	for _, id := range fn.inIDs {
		if from.ID() == id {
			return true
		}
	}
	return false
}

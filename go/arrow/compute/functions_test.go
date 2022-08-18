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

package compute_test

import (
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute"
	"github.com/stretchr/testify/assert"
)

func TestArityBasics(t *testing.T) {
	nullary := compute.Nullary()
	assert.Equal(t, 0, nullary.NArgs)
	assert.False(t, nullary.IsVarArgs)

	unary := compute.Unary()
	assert.Equal(t, 1, unary.NArgs)
	assert.False(t, unary.IsVarArgs)

	binary := compute.Binary()
	assert.Equal(t, 2, binary.NArgs)
	assert.False(t, binary.IsVarArgs)

	ternary := compute.Ternary()
	assert.Equal(t, 3, ternary.NArgs)
	assert.False(t, ternary.IsVarArgs)

	varargs := compute.VarArgs(2)
	assert.Equal(t, 2, varargs.NArgs)
	assert.True(t, varargs.IsVarArgs)
}

func TestScalarFunctionBasics(t *testing.T) {
	fn := compute.NewScalarFunction("scalar_test", compute.Binary(), compute.EmptyFuncDoc)
	varArgsFn := compute.NewScalarFunction("varargs_test", compute.VarArgs(1), compute.EmptyFuncDoc)

	assert.Implements(t, (*compute.Function)(nil), fn)
	assert.Equal(t, "scalar_test", fn.Name())
	assert.Equal(t, 2, fn.Arity().NArgs)
	assert.False(t, fn.Arity().IsVarArgs)
	assert.Equal(t, compute.FuncScalar, fn.Kind())

	assert.Equal(t, "varargs_test", varArgsFn.Name())
	assert.Equal(t, 1, varArgsFn.Arity().NArgs)
	assert.True(t, varArgsFn.Arity().IsVarArgs)
	assert.Equal(t, compute.FuncScalar, varArgsFn.Kind())
}

var execNYI = func(*compute.KernelCtx, *compute.ExecSpan, *compute.ExecResult) error {
	return arrow.ErrNotImplemented
}

func checkAddDispatch(t *testing.T, fn *compute.ScalarFunction, exec compute.ArrayKernelExec) {
	assert.Zero(t, fn.NumKernels())
	assert.Len(t, fn.Kernels(), 0)

	inTypes := []compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int32),
		compute.NewExactInput(arrow.PrimitiveTypes.Int32)}
	out := compute.NewOutputType(arrow.PrimitiveTypes.Int32)

	assert.NoError(t, fn.AddNewKernel(inTypes, out, exec, nil))
	assert.NoError(t, fn.AddNewKernel([]compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int32),
		compute.NewExactInput(arrow.PrimitiveTypes.Int8),
	}, compute.NewOutputType(arrow.PrimitiveTypes.Int32), exec, nil))

	// duplicate sig is okay
	assert.NoError(t, fn.AddNewKernel(inTypes, out, exec, nil))

	kernel := compute.NewScalarKernel([]compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Float64),
		compute.NewExactInput(arrow.PrimitiveTypes.Float64),
	}, compute.NewOutputType(arrow.PrimitiveTypes.Float64), exec, nil)
	assert.NoError(t, fn.AddKernel(*kernel))

	assert.EqualValues(t, 4, fn.NumKernels())
	assert.Len(t, fn.Kernels(), 4)

	// now some invalid kernels
	assert.ErrorIs(t, fn.AddNewKernel([]compute.InputType{},
		compute.NewOutputType(arrow.PrimitiveTypes.Int32), exec, nil), arrow.ErrInvalid)

	assert.ErrorIs(t, fn.AddNewKernel([]compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int32),
	}, compute.NewOutputType(arrow.PrimitiveTypes.Int32), exec, nil), arrow.ErrInvalid)

	assert.ErrorIs(t, fn.AddNewKernel([]compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int8),
		compute.NewExactInput(arrow.PrimitiveTypes.Int8),
		compute.NewExactInput(arrow.PrimitiveTypes.Int8),
	}, compute.NewOutputType(arrow.PrimitiveTypes.Int32), exec, nil), arrow.ErrInvalid)

	// add valid and invalid kernel using kernel struct directly
	valid := compute.NewScalarKernel([]compute.InputType{
		compute.NewExactInput(arrow.FixedWidthTypes.Boolean),
		compute.NewExactInput(arrow.FixedWidthTypes.Boolean),
	}, compute.NewOutputType(arrow.FixedWidthTypes.Boolean), exec, nil)
	invalid := compute.NewScalarKernel([]compute.InputType{
		compute.NewExactInput(arrow.FixedWidthTypes.Boolean),
	}, compute.NewOutputType(arrow.FixedWidthTypes.Boolean), exec, nil)

	assert.NoError(t, fn.AddKernel(*valid))
	assert.ErrorIs(t, fn.AddKernel(*invalid), arrow.ErrInvalid)

	dispatched, err := fn.DispatchExact(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	assert.NoError(t, err)
	expectedSig := &compute.KernelSignature{InputTypes: inTypes, OutType: out}
	assert.True(t, dispatched.GetSig().Equals(expectedSig))

	// no kernel available
	dispatched, err = fn.DispatchExact(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	assert.Nil(t, dispatched)
	assert.ErrorIs(t, err, arrow.ErrNotImplemented)

	// wrong arity
	_, err = fn.DispatchExact()
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	_, err = fn.DispatchExact(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
	assert.ErrorIs(t, err, arrow.ErrInvalid)
}

func TestScalarFunctionDispatch(t *testing.T) {
	fn := compute.NewScalarFunction("scalar_test", compute.Binary(), compute.EmptyFuncDoc)
	checkAddDispatch(t, fn, execNYI)
}

func TestArrayFunctionVarArgs(t *testing.T) {
	vaFunc := compute.NewScalarFunction("va_test", compute.VarArgs(1), compute.EmptyFuncDoc)
	vaArgs := []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)}

	assert.NoError(t, vaFunc.AddNewKernel(vaArgs,
		compute.NewOutputType(arrow.PrimitiveTypes.Int8), execNYI, nil))

	assert.ErrorIs(t, vaFunc.AddNewKernel([]compute.InputType{},
		compute.NewOutputType(arrow.PrimitiveTypes.Int8), execNYI, nil), arrow.ErrInvalid)
	// varargs expects a singleinput type
	assert.ErrorIs(t, vaFunc.AddNewKernel([]compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int8), compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		compute.NewOutputType(arrow.PrimitiveTypes.Int8), execNYI, nil), arrow.ErrInvalid)

	// invalid sig
	nonVaKernel := compute.NewScalarKernelWithSig(&compute.KernelSignature{
		InputTypes: vaArgs, OutType: compute.NewOutputType(arrow.PrimitiveTypes.Int8)},
		execNYI, nil)
	assert.ErrorIs(t, vaFunc.AddKernel(*nonVaKernel), arrow.ErrInvalid)

	args := []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8}
	kn, err := vaFunc.DispatchExact(args...)
	assert.NoError(t, err)
	assert.True(t, kn.GetSig().MatchesInputs(args))

	// no dispatch possible because args incompatible
	args[2] = arrow.PrimitiveTypes.Int32
	_, err = vaFunc.DispatchExact(args...)
	assert.ErrorIs(t, err, arrow.ErrNotImplemented)
}

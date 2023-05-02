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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

func ExecCopyArray(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	debug.Assert(len(batch.Values) == 1, "wrong number of values")
	valueSize := int64(batch.Values[0].Type().(arrow.FixedWidthDataType).BitWidth() / 8)

	arg0 := batch.Values[0].Array
	dst := out.Buffers[1].Buf[out.Offset*valueSize:]
	src := arg0.Buffers[1].Buf[arg0.Offset*valueSize:]
	copy(dst, src[:batch.Len*valueSize])
	return nil
}

func ExecComputedBitmap(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// propagate nulls not used. check that out bitmap isn't the same already
	// as the input bitmap
	arg0 := batch.Values[0].Array
	if bitutil.CountSetBits(arg0.Buffers[1].Buf, int(arg0.Offset), int(batch.Len)) > 0 {
		// check that the bitmap hasn't already been copied
		debug.Assert(!bitutil.BitmapEquals(arg0.Buffers[0].Buf, out.Buffers[0].Buf,
			arg0.Offset, out.Offset, batch.Len), "bitmap should not have already been copied")
	}

	bitutil.CopyBitmap(arg0.Buffers[0].Buf, int(arg0.Offset), int(batch.Len), out.Buffers[0].Buf, int(out.Offset))
	return ExecCopyArray(ctx, batch, out)
}

func ExecNoPreallocatedData(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// validity preallocated, not data
	debug.Assert(out.Offset == 0, "invalid offset for non-prealloc")
	valueSize := int64(batch.Values[0].Type().(arrow.FixedWidthDataType).BitWidth() / 8)
	out.Buffers[1].SetBuffer(ctx.Allocate(int(out.Len * valueSize)))
	out.Buffers[1].SelfAlloc = true
	return ExecCopyArray(ctx, batch, out)
}

func ExecNoPreallocatedAnything(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// neither validity nor data preallocated
	debug.Assert(out.Offset == 0, "invalid offset for non-prealloc")
	out.Buffers[0].SetBuffer(ctx.AllocateBitmap(out.Len))
	out.Buffers[0].SelfAlloc = true
	arg0 := batch.Values[0].Array
	bitutil.CopyBitmap(arg0.Buffers[0].Buf, int(arg0.Offset), int(batch.Len), out.Buffers[0].Buf, 0)

	// reuse kernel that allocates data
	return ExecNoPreallocatedData(ctx, batch, out)
}

type ExampleOptions struct {
	Value scalar.Scalar
}

func (e *ExampleOptions) TypeName() string { return "example" }

type ExampleState struct {
	Value scalar.Scalar
}

func InitStateful(_ *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
	value := args.Options.(*ExampleOptions).Value
	return &ExampleState{Value: value}, nil
}

func ExecStateful(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	state := ctx.State.(*ExampleState)
	multiplier := state.Value.(*scalar.Int32).Value

	arg0 := batch.Values[0].Array
	arg0Data := exec.GetSpanValues[int32](&arg0, 1)
	dst := exec.GetSpanValues[int32](out, 1)
	for i, v := range arg0Data {
		dst[i] = v * multiplier
	}
	return nil
}

func ExecAddInt32(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	left := exec.GetSpanValues[int32](&batch.Values[0].Array, 1)
	right := exec.GetSpanValues[int32](&batch.Values[1].Array, 1)
	outValues := exec.GetSpanValues[int32](out, 1)
	for i := 0; i < int(batch.Len); i++ {
		outValues[i] = left[i] + right[i]
	}
	return nil
}

type CallScalarFuncSuite struct {
	ComputeInternalsTestSuite
}

func (c *CallScalarFuncSuite) addCopyFuncs() {
	registry = GetFunctionRegistry()

	fn := NewScalarFunction("test_copy", Unary(), EmptyFuncDoc)
	types := []arrow.DataType{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64}
	for _, t := range types {
		c.NoError(fn.AddNewKernel([]exec.InputType{exec.NewExactInput(t)},
			exec.NewOutputType(t), ExecCopyArray, nil))
	}
	c.True(registry.AddFunction(fn, false))

	// a version which doesn't want the executor to call propagatenulls
	fn2 := NewScalarFunction("test_copy_computed_bitmap", Unary(), EmptyFuncDoc)
	kernel := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Uint8)},
		exec.NewOutputType(arrow.PrimitiveTypes.Uint8), ExecComputedBitmap, nil)
	kernel.NullHandling = exec.NullComputedPrealloc
	c.NoError(fn2.AddKernel(kernel))
	c.True(registry.AddFunction(fn2, false))
}

func (c *CallScalarFuncSuite) addNoPreallocFuncs() {
	registry = GetFunctionRegistry()

	// a function that allocates its own output memory. we have cases
	// for both non-preallocated data and non-preallocated bitmap
	f1 := NewScalarFunction("test_nopre_data", Unary(), EmptyFuncDoc)
	f2 := NewScalarFunction("test_nopre_validity_or_data", Unary(), EmptyFuncDoc)

	kernel := exec.NewScalarKernel(
		[]exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Uint8)},
		exec.NewOutputType(arrow.PrimitiveTypes.Uint8),
		ExecNoPreallocatedData, nil)
	kernel.MemAlloc = exec.MemNoPrealloc
	c.NoError(f1.AddKernel(kernel))

	kernel.ExecFn = ExecNoPreallocatedAnything
	kernel.NullHandling = exec.NullComputedNoPrealloc
	c.NoError(f2.AddKernel(kernel))

	c.True(registry.AddFunction(f1, false))
	c.True(registry.AddFunction(f2, false))
}

func (c *CallScalarFuncSuite) addStatefulFunc() {
	registry := GetFunctionRegistry()

	// this functions behavior depends on a static parameter that
	// is made available to the execution through its options object
	fn := NewScalarFunction("test_stateful", Unary(), EmptyFuncDoc)

	c.NoError(fn.AddNewKernel([]exec.InputType{exec.NewExactInput(arrow.PrimitiveTypes.Int32)},
		exec.NewOutputType(arrow.PrimitiveTypes.Int32), ExecStateful, InitStateful))

	c.True(registry.AddFunction(fn, false))
}

func (c *CallScalarFuncSuite) addScalarFunc() {
	registry := GetFunctionRegistry()

	fn := NewScalarFunction("test_scalar_add_int32", Binary(), EmptyFuncDoc)
	c.NoError(fn.AddNewKernel([]exec.InputType{
		exec.NewExactInput(arrow.PrimitiveTypes.Int32),
		exec.NewExactInput(arrow.PrimitiveTypes.Int32)},
		exec.NewOutputType(arrow.PrimitiveTypes.Int32), ExecAddInt32, nil))
	c.True(registry.AddFunction(fn, false))
}

func (c *CallScalarFuncSuite) SetupSuite() {
	c.addCopyFuncs()
	c.addNoPreallocFuncs()
	c.addStatefulFunc()
	c.addScalarFunc()
}

func (c *CallScalarFuncSuite) TestArgumentValidation() {
	// copy accepts only a single array arg
	arr := c.getInt32Arr(10, 0.1)
	defer arr.Release()
	d1 := &ArrayDatum{Value: arr.Data()}

	c.Run("too many args", func() {
		args := []Datum{d1, d1}
		_, err := CallFunction(c.ctx.Ctx, "test_copy", nil, args...)
		c.ErrorIs(err, arrow.ErrInvalid)
	})

	c.Run("too few args", func() {
		_, err := CallFunction(c.ctx.Ctx, "test_copy", nil)
		c.ErrorIs(err, arrow.ErrInvalid)
	})

	d1Scalar := NewDatum(int32(5))
	result, err := CallFunction(c.ctx.Ctx, "test_copy", nil, d1)
	c.NoError(err)
	result.Release()
	result, err = CallFunction(c.ctx.Ctx, "test_copy", nil, d1Scalar)
	c.NoError(err)
	result.Release()
}

func (c *CallScalarFuncSuite) TestPreallocationCases() {
	nullProb := float64(0.2)
	arr := c.getUint8Arr(100, nullProb)
	defer arr.Release()

	funcNames := []string{"test_copy", "test_copy_computed_bitmap"}
	for _, funcName := range funcNames {
		c.Run(funcName, func() {
			c.resetCtx()

			c.Run("single output default", func() {
				result, err := CallFunction(c.ctx.Ctx, funcName, nil, &ArrayDatum{arr.Data()})
				c.NoError(err)
				defer result.Release()
				c.Equal(KindArray, result.Kind())
				c.assertDatumEqual(arr, result)
			})

			c.Run("exec chunks", func() {
				// set the exec_chunksize to be smaller so now we have
				// several invocations of the kernel,
				// but still only one output array
				c.execCtx.ChunkSize = 80
				result, err := CallFunction(SetExecCtx(c.ctx.Ctx, c.execCtx), funcName, nil, &ArrayDatum{arr.Data()})
				c.NoError(err)
				defer result.Release()
				c.Equal(KindArray, result.Kind())
				c.assertDatumEqual(arr, result)
			})

			c.Run("not multiple 8 chunk", func() {
				// chunksize is not a multiple of 8
				c.execCtx.ChunkSize = 11
				result, err := CallFunction(SetExecCtx(c.ctx.Ctx, c.execCtx), funcName, nil, &ArrayDatum{arr.Data()})
				c.NoError(err)
				defer result.Release()
				c.Equal(KindArray, result.Kind())
				c.assertDatumEqual(arr, result)
			})

			c.Run("chunked", func() {
				// input is chunked, output is one big chunk
				chk1, chk2 := array.NewSlice(arr, 0, 10), array.NewSlice(arr, 10, int64(arr.Len()))
				defer chk1.Release()
				defer chk2.Release()
				carr := arrow.NewChunked(arr.DataType(), []arrow.Array{chk1, chk2})
				defer carr.Release()

				result, err := CallFunction(SetExecCtx(c.ctx.Ctx, c.execCtx), funcName, nil, &ChunkedDatum{carr})
				c.NoError(err)
				defer result.Release()
				c.Equal(KindChunked, result.Kind())
				actual := result.(*ChunkedDatum).Value
				c.Len(actual.Chunks(), 1)
				c.Truef(array.ChunkedEqual(actual, carr), "expected: %s\ngot: %s", carr, actual)
			})

			c.Run("independent", func() {
				// preallocate independently for each batch
				c.execCtx.PreallocContiguous = false
				c.execCtx.ChunkSize = 40
				result, err := CallFunction(SetExecCtx(c.ctx.Ctx, c.execCtx), funcName, nil, &ArrayDatum{arr.Data()})
				c.NoError(err)
				defer result.Release()
				c.Equal(KindChunked, result.Kind())

				carr := result.(*ChunkedDatum).Value
				c.Len(carr.Chunks(), 3)
				sl := array.NewSlice(arr, 0, 40)
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(0))
				sl = array.NewSlice(arr, 40, 80)
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(1))
				sl = array.NewSlice(arr, 80, int64(arr.Len()))
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(2))
			})
		})
	}
}

func (c *CallScalarFuncSuite) TestBasicNonStandardCases() {
	// test some more cases
	//
	// * validity bitmap computed by kernel rather than propagate nulls
	// * data not pre-allocated
	// * validity bitmap not pre-allocated

	nullProb := float64(0.2)
	arr := c.getUint8Arr(1000, nullProb)
	defer arr.Release()
	args := []Datum{&ArrayDatum{arr.Data()}}

	for _, funcName := range []string{"test_nopre_data", "test_nopre_validity_or_data"} {
		c.Run("funcName", func() {
			c.resetCtx()
			c.Run("single output default", func() {
				result, err := CallFunction(c.ctx.Ctx, funcName, nil, args...)
				c.NoError(err)
				defer result.Release()
				c.Equal(KindArray, result.Kind())
				c.assertDatumEqual(arr, result)
			})

			c.Run("split into 3 chunks", func() {
				c.execCtx.ChunkSize = 400
				result, err := CallFunction(SetExecCtx(c.ctx.Ctx, c.execCtx), funcName, nil, args...)
				c.NoError(err)
				defer result.Release()

				c.Equal(KindChunked, result.Kind())

				carr := result.(*ChunkedDatum).Value
				c.Len(carr.Chunks(), 3)
				sl := array.NewSlice(arr, 0, 400)
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(0))
				sl = array.NewSlice(arr, 400, 800)
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(1))
				sl = array.NewSlice(arr, 800, int64(arr.Len()))
				defer sl.Release()
				c.assertArrayEqual(sl, carr.Chunk(2))
			})
		})
	}
}

func (c *CallScalarFuncSuite) TestStatefulKernel() {
	input, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3, null, 5]`))
	defer input.Release()

	multiplier := scalar.MakeScalar(int32(2))
	expected, _, _ := array.FromJSON(c.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[2, 4, 6, null, 10]`))
	defer expected.Release()

	options := &ExampleOptions{multiplier}
	result, err := CallFunction(c.ctx.Ctx, "test_stateful", options, &ArrayDatum{input.Data()})
	c.NoError(err)
	defer result.Release()
	c.assertDatumEqual(expected, result)
}

func (c *CallScalarFuncSuite) TestScalarFunction() {
	args := []Datum{NewDatum(int32(5)), NewDatum(int32(7))}
	result, err := CallFunction(c.ctx.Ctx, "test_scalar_add_int32", nil, args...)
	c.NoError(err)
	defer result.Release()

	c.Equal(KindScalar, result.Kind())
	expected := scalar.MakeScalar(int32(12))
	c.True(scalar.Equals(expected, result.(*ScalarDatum).Value))
}

func TestCallScalarFunctions(t *testing.T) {
	suite.Run(t, new(CallScalarFuncSuite))
}

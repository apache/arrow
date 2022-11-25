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

package compute_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/compute"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/decimal256"
	"github.com/apache/arrow/go/v11/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/arrow/scalar"
	"github.com/klauspost/cpuid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/constraints"
)

var (
	CpuCacheSizes = [...]int{ // defaults
		32 * 1024,   // level 1: 32K
		256 * 1024,  // level 2: 256K
		3072 * 1024, // level 3: 3M
	}
)

func init() {
	if cpuid.CPU.Cache.L1D != -1 {
		CpuCacheSizes[0] = cpuid.CPU.Cache.L1D
	}
	if cpuid.CPU.Cache.L2 != -1 {
		CpuCacheSizes[1] = cpuid.CPU.Cache.L2
	}
	if cpuid.CPU.Cache.L3 != -1 {
		CpuCacheSizes[2] = cpuid.CPU.Cache.L3
	}
}

func assertNullToNull(t *testing.T, ctx context.Context, fn string, mem memory.Allocator) {
	f, ok := compute.GetFunctionRegistry().GetFunction(fn)
	require.True(t, ok)
	nulls := array.MakeArrayOfNull(mem, arrow.Null, 7)
	defer nulls.Release()
	n := f.Arity().NArgs

	t.Run("null to null array", func(t *testing.T) {
		args := make([]compute.Datum, n)
		for i := 0; i < n; i++ {
			args[i] = &compute.ArrayDatum{nulls.Data()}
		}

		result, err := compute.CallFunction(ctx, fn, nil, args...)
		assert.NoError(t, err)
		defer result.Release()
		out := result.(*compute.ArrayDatum).MakeArray()
		defer out.Release()
		assertArraysEqual(t, nulls, out)
	})

	t.Run("null to null scalar", func(t *testing.T) {
		args := make([]compute.Datum, n)
		for i := 0; i < n; i++ {
			args[i] = compute.NewDatum(scalar.ScalarNull)
		}

		result, err := compute.CallFunction(ctx, fn, nil, args...)
		assert.NoError(t, err)
		assertScalarEquals(t, scalar.ScalarNull, result.(*compute.ScalarDatum).Value)
	})
}

type unaryArithmeticFunc = func(context.Context, compute.ArithmeticOptions, compute.Datum) (compute.Datum, error)

// type unaryFunc = func(compute.Datum) (compute.Datum, error)

type binaryArithmeticFunc = func(context.Context, compute.ArithmeticOptions, compute.Datum, compute.Datum) (compute.Datum, error)

type binaryFunc = func(left, right compute.Datum) (compute.Datum, error)

func assertScalarEquals(t *testing.T, expected, actual scalar.Scalar, opt ...scalar.EqualOption) {
	assert.Truef(t, scalar.ApproxEquals(expected, actual, opt...), "expected: %s\ngot: %s", expected, actual)
}

func assertBinop(t *testing.T, fn binaryFunc, left, right, expected arrow.Array, opt []array.EqualOption, scalarOpt []scalar.EqualOption) {
	actual, err := fn(&compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()})
	require.NoError(t, err)
	defer actual.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, actual, opt...)

	// also check (Scalar, Scalar) operations
	for i := 0; i < expected.Len(); i++ {
		s, err := scalar.GetScalar(expected, i)
		require.NoError(t, err)
		lhs, _ := scalar.GetScalar(left, i)
		rhs, _ := scalar.GetScalar(right, i)

		actual, err := fn(&compute.ScalarDatum{Value: lhs}, &compute.ScalarDatum{Value: rhs})
		assert.NoError(t, err)
		assertScalarEquals(t, s, actual.(*compute.ScalarDatum).Value, scalarOpt...)
	}
}

func assertBinopErr(t *testing.T, fn binaryFunc, left, right arrow.Array, expectedMsg string) {
	_, err := fn(&compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{Value: right.Data()})
	assert.ErrorIs(t, err, arrow.ErrInvalid)
	assert.ErrorContains(t, err, expectedMsg)
}

type BinaryFuncTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func (b *BinaryFuncTestSuite) SetupTest() {
	b.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	b.ctx = compute.WithAllocator(context.TODO(), b.mem)
}

func (b *BinaryFuncTestSuite) TearDownTest() {
	b.mem.AssertSize(b.T(), 0)
}

func (b *BinaryFuncTestSuite) getArr(dt arrow.DataType, str string) arrow.Array {
	arr, _, err := array.FromJSON(b.mem, dt, strings.NewReader(str), array.WithUseNumber())
	b.Require().NoError(err)
	return arr
}

type Float16BinaryFuncTestSuite struct {
	BinaryFuncTestSuite
}

func (b *Float16BinaryFuncTestSuite) assertBinopErr(fn binaryFunc, lhs, rhs string) {
	left, _, _ := array.FromJSON(b.mem, arrow.FixedWidthTypes.Float16, strings.NewReader(lhs), array.WithUseNumber())
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, arrow.FixedWidthTypes.Float16, strings.NewReader(rhs), array.WithUseNumber())
	defer right.Release()

	_, err := fn(&compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{right.Data()})
	b.ErrorIs(err, arrow.ErrNotImplemented)
}

func (b *Float16BinaryFuncTestSuite) TestAdd() {
	for _, overflow := range []bool{false, true} {
		b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
			opts := compute.ArithmeticOptions{NoCheckOverflow: overflow}
			b.assertBinopErr(func(left, right compute.Datum) (compute.Datum, error) {
				return compute.Add(b.ctx, opts, left, right)
			}, `[1.5]`, `[1.5]`)
		})
	}
}

func (b *Float16BinaryFuncTestSuite) TestSub() {
	for _, overflow := range []bool{false, true} {
		b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
			opts := compute.ArithmeticOptions{NoCheckOverflow: overflow}
			b.assertBinopErr(func(left, right compute.Datum) (compute.Datum, error) {
				return compute.Subtract(b.ctx, opts, left, right)
			}, `[1.5]`, `[1.5]`)
		})
	}
}

type BinaryArithmeticSuite[T exec.NumericTypes] struct {
	BinaryFuncTestSuite

	opts            compute.ArithmeticOptions
	min, max        T
	equalOpts       []array.EqualOption
	scalarEqualOpts []scalar.EqualOption
}

func (BinaryArithmeticSuite[T]) DataType() arrow.DataType {
	return exec.GetDataType[T]()
}

func (b *BinaryArithmeticSuite[T]) setNansEqual(val bool) {
	b.equalOpts = []array.EqualOption{array.WithNaNsEqual(val)}
	b.scalarEqualOpts = []scalar.EqualOption{scalar.WithNaNsEqual(val)}
}

func (b *BinaryArithmeticSuite[T]) SetupTest() {
	b.BinaryFuncTestSuite.SetupTest()
	b.opts.NoCheckOverflow = false
}

func (b *BinaryArithmeticSuite[T]) makeNullScalar() scalar.Scalar {
	return scalar.MakeNullScalar(b.DataType())
}

func (b *BinaryArithmeticSuite[T]) makeScalar(val T) scalar.Scalar {
	return scalar.MakeScalar(val)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScalars(fn binaryArithmeticFunc, lhs, rhs T, expected T) {
	left, right := b.makeScalar(lhs), b.makeScalar(rhs)
	exp := b.makeScalar(expected)

	actual, err := fn(b.ctx, b.opts, &compute.ScalarDatum{Value: left}, &compute.ScalarDatum{Value: right})
	b.NoError(err)
	sc := actual.(*compute.ScalarDatum).Value

	assertScalarEquals(b.T(), exp, sc)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScalarValArr(fn binaryArithmeticFunc, lhs T, rhs, expected string) {
	left := b.makeScalar(lhs)
	b.assertBinopScalarArr(fn, left, rhs, expected)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScalarArr(fn binaryArithmeticFunc, lhs scalar.Scalar, rhs, expected string) {
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs))
	defer right.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	actual, err := fn(b.ctx, b.opts, &compute.ScalarDatum{Value: lhs}, &compute.ArrayDatum{Value: right.Data()})
	b.NoError(err)
	defer actual.Release()
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: exp.Data()}, actual)
}

func (b *BinaryArithmeticSuite[T]) assertBinopArrScalarVal(fn binaryArithmeticFunc, lhs string, rhs T, expected string) {
	right := b.makeScalar(rhs)
	b.assertBinopArrScalar(fn, lhs, right, expected)
}

func (b *BinaryArithmeticSuite[T]) assertBinopArrScalar(fn binaryArithmeticFunc, lhs string, rhs scalar.Scalar, expected string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs))
	defer left.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	actual, err := fn(b.ctx, b.opts, &compute.ArrayDatum{Value: left.Data()}, &compute.ScalarDatum{Value: rhs})
	b.NoError(err)
	defer actual.Release()
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: exp.Data()}, actual, b.equalOpts...)
}

func (b *BinaryArithmeticSuite[T]) assertBinop(fn binaryArithmeticFunc, lhs, rhs, expected string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs), array.WithUseNumber())
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs), array.WithUseNumber())
	defer right.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected), array.WithUseNumber())
	defer exp.Release()

	assertBinop(b.T(), func(left, right compute.Datum) (compute.Datum, error) {
		return fn(b.ctx, b.opts, left, right)
	}, left, right, exp, b.equalOpts, b.scalarEqualOpts)
}

func (b *BinaryArithmeticSuite[T]) setOverflowCheck(value bool) {
	b.opts.NoCheckOverflow = !value
}

func (b *BinaryArithmeticSuite[T]) assertBinopErr(fn binaryArithmeticFunc, lhs, rhs, expectedMsg string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs), array.WithUseNumber())
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs), array.WithUseNumber())
	defer right.Release()

	assertBinopErr(b.T(), func(left, right compute.Datum) (compute.Datum, error) {
		return fn(b.ctx, b.opts, left, right)
	}, left, right, expectedMsg)
}

func (b *BinaryArithmeticSuite[T]) TestAdd() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinop(compute.Add, `[]`, `[]`, `[]`)
				b.assertBinop(compute.Add, `[3, 2, 6]`, `[1, 0, 2]`, `[4, 2, 8]`)
				// nulls on one side
				b.assertBinop(compute.Add, `[null, 1, null]`, `[3, 4, 5]`, `[null, 5, null]`)
				b.assertBinop(compute.Add, `[3, 4, 5]`, `[null, 1, null]`, `[null, 5, null]`)
				// nulls on both sides
				b.assertBinop(compute.Add, `[null, 1, 2]`, `[3, 4, null]`, `[null, 5, null]`)
				// all nulls
				b.assertBinop(compute.Add, `[null]`, `[null]`, `[null]`)

				// scalar on the left
				b.assertBinopScalarValArr(compute.Add, 3, `[1, 2]`, `[4, 5]`)
				b.assertBinopScalarValArr(compute.Add, 3, `[null, 2]`, `[null, 5]`)
				b.assertBinopScalarArr(compute.Add, b.makeNullScalar(), `[1, 2]`, `[null, null]`)
				b.assertBinopScalarArr(compute.Add, b.makeNullScalar(), `[null, 2]`, `[null, null]`)
				// scalar on the right
				b.assertBinopArrScalarVal(compute.Add, `[1, 2]`, 3, `[4, 5]`)
				b.assertBinopArrScalarVal(compute.Add, `[null, 2]`, 3, `[null, 5]`)
				b.assertBinopArrScalar(compute.Add, `[1, 2]`, b.makeNullScalar(), `[null, null]`)
				b.assertBinopArrScalar(compute.Add, `[null, 2]`, b.makeNullScalar(), `[null, null]`)

				if !arrow.IsFloating(b.DataType().ID()) && overflow {
					val := fmt.Sprintf("[%v]", b.max)
					b.assertBinopErr(compute.Add, val, val, "overflow")
				}
			})
		}
	})
}

func (b *BinaryArithmeticSuite[T]) TestSub() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinop(compute.Subtract, `[]`, `[]`, `[]`)
				b.assertBinop(compute.Subtract, `[3, 2, 6]`, `[1, 0, 2]`, `[2, 2, 4]`)
				// nulls on one side
				b.assertBinop(compute.Subtract, `[null, 4, null]`, `[2, 1, 0]`, `[null, 3, null]`)
				b.assertBinop(compute.Subtract, `[3, 4, 5]`, `[null, 1, null]`, `[null, 3, null]`)
				// nulls on both sides
				b.assertBinop(compute.Subtract, `[null, 4, 3]`, `[2, 1, null]`, `[null, 3, null]`)
				// all nulls
				b.assertBinop(compute.Subtract, `[null]`, `[null]`, `[null]`)

				// scalar on the left
				b.assertBinopScalarValArr(compute.Subtract, 3, `[1, 2]`, `[2, 1]`)
				b.assertBinopScalarValArr(compute.Subtract, 3, `[null, 2]`, `[null, 1]`)
				b.assertBinopScalarArr(compute.Subtract, b.makeNullScalar(), `[1, 2]`, `[null, null]`)
				b.assertBinopScalarArr(compute.Subtract, b.makeNullScalar(), `[null, 2]`, `[null, null]`)
				// scalar on the right
				b.assertBinopArrScalarVal(compute.Subtract, `[4, 5]`, 3, `[1, 2]`)
				b.assertBinopArrScalarVal(compute.Subtract, `[null, 5]`, 3, `[null, 2]`)
				b.assertBinopArrScalar(compute.Subtract, `[1, 2]`, b.makeNullScalar(), `[null, null]`)
				b.assertBinopArrScalar(compute.Subtract, `[null, 2]`, b.makeNullScalar(), `[null, null]`)

				if !arrow.IsFloating(b.DataType().ID()) && overflow {
					b.assertBinopErr(compute.Subtract, fmt.Sprintf("[%v]", b.min), fmt.Sprintf("[%v]", b.max), "overflow")
				}
			})
		}
	})
}

func (b *BinaryArithmeticSuite[T]) TestMuliply() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinop(compute.Multiply, `[]`, `[]`, `[]`)
				b.assertBinop(compute.Multiply, `[3, 2, 6]`, `[1, 0, 2]`, `[3, 0, 12]`)
				// nulls on one side
				b.assertBinop(compute.Multiply, `[null, 2, null]`, `[4, 5, 6]`, `[null, 10, null]`)
				b.assertBinop(compute.Multiply, `[4, 5, 6]`, `[null, 2, null]`, `[null, 10, null]`)
				// nulls on both sides
				b.assertBinop(compute.Multiply, `[null, 2, 3]`, `[4, 5, null]`, `[null, 10, null]`)
				// all nulls
				b.assertBinop(compute.Multiply, `[null]`, `[null]`, `[null]`)

				// scalar on left
				b.assertBinopScalarValArr(compute.Multiply, 3, `[4, 5]`, `[12, 15]`)
				b.assertBinopScalarValArr(compute.Multiply, 3, `[null, 5]`, `[null, 15]`)
				b.assertBinopScalarArr(compute.Multiply, b.makeNullScalar(), `[1, 2]`, `[null, null]`)
				b.assertBinopScalarArr(compute.Multiply, b.makeNullScalar(), `[null, 2]`, `[null, null]`)
				// scalar on right
				b.assertBinopArrScalarVal(compute.Multiply, `[4, 5]`, 3, `[12, 15]`)
				b.assertBinopArrScalarVal(compute.Multiply, `[null, 5]`, 3, `[null, 15]`)
				b.assertBinopArrScalar(compute.Multiply, `[1, 2]`, b.makeNullScalar(), `[null, null]`)
				b.assertBinopArrScalar(compute.Multiply, `[null, 2]`, b.makeNullScalar(), `[null, null]`)
			})
		}
	})
}

func (b *BinaryArithmeticSuite[T]) TestDiv() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("no_overflow_check=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				// empty arrays
				b.assertBinop(compute.Divide, `[]`, `[]`, `[]`)
				// ordinary arrays
				b.assertBinop(compute.Divide, `[3, 2, 6]`, `[1, 1, 2]`, `[3, 2, 3]`)
				// with nulls
				b.assertBinop(compute.Divide, `[null, 10, 30, null, 20]`, `[1, 5, 2, 5, 10]`, `[null, 2, 15, null, 2]`)
				if !arrow.IsFloating(b.DataType().ID()) {
					// scalar divided by array
					b.assertBinopScalarValArr(compute.Divide, 33, `[null, 1, 3, null, 2]`, `[null, 33, 11, null, 16]`)
					// array divided by scalar
					b.assertBinopArrScalarVal(compute.Divide, `[null, 10, 30, null, 2]`, 3, `[null, 3, 10, null, 0]`)
					// scalar divided by scalar
					b.assertBinopScalars(compute.Divide, 16, 7, 2)
				} else {
					b.assertBinop(compute.Divide, `[3.4, 0.64, 1.28]`, `[1, 2, 4]`, `[3.4, 0.32, 0.32]`)
					b.assertBinop(compute.Divide, `[null, 1, 3.3, null, 2]`, `[1, 4, 2, 5, 0.1]`, `[null, 0.25, 1.65, null, 20]`)
					b.assertBinopScalarValArr(compute.Divide, 10, `[null, 1, 2.5, null, 2, 5]`, `[null, 10, 4, null, 5, 2]`)
					b.assertBinopArrScalarVal(compute.Divide, `[null, 1, 2.5, null, 2, 5]`, 10, `[null, 0.1, 0.25, null, 0.2, 0.5]`)

					b.assertBinop(compute.Divide, `[3.4, "Inf", "-Inf"]`, `[1, 2, 3]`, `[3.4, "Inf", "-Inf"]`)
					b.setNansEqual(true)
					b.assertBinop(compute.Divide, `[3.4, "NaN", 2.0]`, `[1, 2, 2.0]`, `[3.4, "NaN", 1.0]`)
					b.assertBinopScalars(compute.Divide, 21, 3, 7)
				}
			})
		}
	})
}

func (b *BinaryArithmeticSuite[T]) TestDivideByZero() {
	if !arrow.IsFloating(b.DataType().ID()) {
		for _, checkOverflow := range []bool{false, true} {
			b.setOverflowCheck(checkOverflow)
			b.assertBinopErr(compute.Divide, `[3, 2, 6]`, `[1, 1, 0]`, "divide by zero")
		}
	} else {
		b.setOverflowCheck(true)
		b.assertBinopErr(compute.Divide, `[3, 2, 6]`, `[1, 1, 0]`, "divide by zero")
		b.assertBinopErr(compute.Divide, `[3, 2, 0]`, `[1, 1, 0]`, "divide by zero")
		b.assertBinopErr(compute.Divide, `[3, 2, -6]`, `[1, 1, 0]`, "divide by zero")

		b.setOverflowCheck(false)
		b.setNansEqual(true)
		b.assertBinop(compute.Divide, `[3, 2, 6]`, `[1, 1, 0]`, `[3, 2, "Inf"]`)
		b.assertBinop(compute.Divide, `[3, 2, 0]`, `[1, 1, 0]`, `[3, 2, "NaN"]`)
		b.assertBinop(compute.Divide, `[3, 2, -6]`, `[1, 1, 0]`, `[3, 2, "-Inf"]`)
	}
}

func (b *BinaryArithmeticSuite[T]) TestPower() {
	b.setNansEqual(true)
	b.Run(b.DataType().String(), func() {
		for _, checkOverflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("checkOverflow=%t", checkOverflow), func() {
				b.setOverflowCheck(checkOverflow)

				b.assertBinop(compute.Power, `[]`, `[]`, `[]`)
				if !arrow.IsFloating(b.DataType().ID()) {
					b.assertBinop(compute.Power, `[3, 2, 6, 2]`, `[1, 1, 2, 0]`, `[3, 2, 36, 1]`)
					b.assertBinop(compute.Power, `[null, 2, 3, null, 20]`, `[1, 6, 2, 5, 1]`, `[null, 64, 9, null, 20]`)
					b.assertBinopScalarValArr(compute.Power, 3, `[null, 3, 4, null, 2]`, `[null, 27, 81, null, 9]`)
					b.assertBinopArrScalarVal(compute.Power, `[null, 10, 3, null, 2]`, 2, `[null, 100, 9, null, 4]`)
					b.assertBinopScalars(compute.Power, 4, 3, 64)
					b.assertBinop(compute.Power, `[0, 1, 0]`, `[0, 0, 42]`, `[1, 1, 0]`)

					if checkOverflow {
						b.assertBinopErr(compute.Power, fmt.Sprintf("[%v]", b.max), `[10]`, "overflow")
					} else {
						b.assertBinopScalars(compute.Power, b.max, 10, 1)
					}
				} else {
					b.assertBinop(compute.Power, `[3.4, 16, 0.64, 1.2, 0]`, `[1, 0.5, 2, 4, 0]`, `[3.4, 4, 0.4096, 2.0736, 1]`)
					b.assertBinop(compute.Power, `[null, 1, 3.3, null, 2]`, `[1, 4, 2, 5, 0.1]`, `[null, 1, 10.89, null, 1.07177346]`)
					b.assertBinopScalarValArr(compute.Power, 10, `[null, 1, 2.5, null, 2, 5]`, `[null, 10, 316.227766017, null, 100, 100000]`)
					b.assertBinopArrScalarVal(compute.Power, `[null, 1, 2.5, null, 2, 5]`, 10, `[null, 1, 9536.74316406, null, 1024, 9765625]`)
					b.assertBinop(compute.Power, `[3.4, "Inf", "-Inf", 1.1, 10000]`, `[1, 2, 3, "Inf", 100000]`, `[3.4, "Inf", "-Inf", "Inf", "Inf"]`)
					b.assertBinop(compute.Power, `[3.4, "NaN", 2.0]`, `[1, 2, 2.0]`, `[3.4, "NaN", 4.0]`)
					b.assertBinop(compute.Power, `[0.0, 0.0]`, `[-1.0, -3.0]`, `["Inf", "Inf"]`)
				}
			})
		}
	})
}

type BinaryIntegralArithmeticSuite[T exec.IntTypes | exec.UintTypes] struct {
	BinaryArithmeticSuite[T]
}

func (b *BinaryIntegralArithmeticSuite[T]) TestShiftLeft() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("check_overflow=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinop(compute.ShiftLeft, `[]`, `[]`, `[]`)
				b.assertBinop(compute.ShiftLeft, `[0, 1, 2, 3]`, `[2, 3, 4, 5]`, `[0, 8, 32, 96]`)
				b.assertBinop(compute.ShiftLeft, `[0, null, 2, 3]`, `[2, 3, 4, 5]`, `[0, null, 32, 96]`)
				b.assertBinop(compute.ShiftLeft, `[0, 1, 2, 3]`, `[2, 3, null, 5]`, `[0, 8, null, 96]`)
				b.assertBinop(compute.ShiftLeft, `[0, null, 2, 3]`, `[2, 3, null, 5]`, `[0, null, null, 96]`)
				b.assertBinop(compute.ShiftLeft, `[null]`, `[null]`, `[null]`)
				b.assertBinopScalarValArr(compute.ShiftLeft, 2, `[null, 5]`, `[null, 64]`)
				b.assertBinopScalarArr(compute.ShiftLeft, b.makeNullScalar(), `[null, 5]`, `[null, null]`)
				b.assertBinopArrScalarVal(compute.ShiftLeft, `[null, 5]`, 3, `[null, 40]`)
				b.assertBinopArrScalar(compute.ShiftLeft, `[null, 5]`, b.makeNullScalar(), `[null, null]`)
			})
		}
	})
}

func (b *BinaryIntegralArithmeticSuite[T]) TestShiftRight() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("check_overflow=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinop(compute.ShiftRight, `[]`, `[]`, `[]`)
				b.assertBinop(compute.ShiftRight, `[0, 1, 4, 8]`, `[1, 1, 1, 4]`, `[0, 0, 2, 0]`)
				b.assertBinop(compute.ShiftRight, `[0, null, 4, 8]`, `[1, 1, 1, 4]`, `[0, null, 2, 0]`)
				b.assertBinop(compute.ShiftRight, `[0, 1, 4, 8]`, `[1, 1, null, 4]`, `[0, 0, null, 0]`)
				b.assertBinop(compute.ShiftRight, `[0, null, 4, 8]`, `[1, 1, null, 4]`, `[0, null, null, 0]`)
				b.assertBinop(compute.ShiftRight, `[null]`, `[null]`, `[null]`)
				b.assertBinopScalarValArr(compute.ShiftRight, 64, `[null, 2, 6]`, `[null, 16, 1]`)
				b.assertBinopScalarArr(compute.ShiftRight, b.makeNullScalar(), `[null, 2, 6]`, `[null, null, null]`)
				b.assertBinopArrScalarVal(compute.ShiftRight, `[null, 3, 96]`, 3, `[null, 0, 12]`)
				b.assertBinopArrScalar(compute.ShiftRight, `[null, 3, 96]`, b.makeNullScalar(), `[null, null, null]`)
			})
		}
	})
}

func (b *BinaryIntegralArithmeticSuite[T]) TestShiftLeftOverflowError() {
	b.Run(b.DataType().String(), func() {
		bitWidth := b.DataType().(arrow.FixedWidthDataType).BitWidth()
		if !arrow.IsUnsignedInteger(b.DataType().ID()) {
			bitWidth--
		}

		b.setOverflowCheck(true)
		b.assertBinop(compute.ShiftLeft, `[1]`, fmt.Sprintf("[%d]", bitWidth-1),
			fmt.Sprintf("[%d]", T(1)<<(bitWidth-1)))
		b.assertBinop(compute.ShiftLeft, `[2]`, fmt.Sprintf("[%d]", bitWidth-2),
			fmt.Sprintf("[%d]", T(1)<<(bitWidth-1)))
		if arrow.IsUnsignedInteger(b.DataType().ID()) {
			b.assertBinop(compute.ShiftLeft, `[2]`, fmt.Sprintf("[%d]", bitWidth-1), `[0]`)
			b.assertBinop(compute.ShiftLeft, `[4]`, fmt.Sprintf("[%d]", bitWidth-1), `[0]`)
			b.assertBinopErr(compute.ShiftLeft, `[1]`, fmt.Sprintf("[%d]", bitWidth), "shift amount must be >= 0 and less than precision of type")
		} else {
			// shift a bit into the sign bit
			b.assertBinop(compute.ShiftLeft, `[2]`, fmt.Sprintf("[%d]", bitWidth-1),
				fmt.Sprintf("[%d]", b.min))
			// shift a bit past the sign bit
			b.assertBinop(compute.ShiftLeft, `[4]`, fmt.Sprintf("[%d]", bitWidth-1), `[0]`)
			b.assertBinop(compute.ShiftLeft, fmt.Sprintf("[%d]", b.min), `[1]`, `[0]`)
			b.assertBinopErr(compute.ShiftLeft, `[1, 2]`, `[1, -1]`, "shift amount must be >= 0 and less than precision of type")
			b.assertBinopErr(compute.ShiftLeft, `[1]`, fmt.Sprintf("[%d]", bitWidth), "shift amount must be >= 0 and less than precision of type")

			b.setOverflowCheck(false)
			b.assertBinop(compute.ShiftLeft, `[1, 1]`, fmt.Sprintf("[-1, %d]", bitWidth), `[1, 1]`)
		}
	})
}

func (b *BinaryIntegralArithmeticSuite[T]) TestShiftRightOverflowError() {
	b.Run(b.DataType().String(), func() {
		bitWidth := b.DataType().(arrow.FixedWidthDataType).BitWidth()
		if !arrow.IsUnsignedInteger(b.DataType().ID()) {
			bitWidth--
		}

		b.setOverflowCheck(true)

		b.assertBinop(compute.ShiftRight, fmt.Sprintf("[%d]", b.max), fmt.Sprintf("[%d]", bitWidth-1), `[1]`)
		if arrow.IsUnsignedInteger(b.DataType().ID()) {
			b.assertBinopErr(compute.ShiftRight, `[1]`, fmt.Sprintf("[%d]", bitWidth), "shift amount must be >= 0 and less than precision of type")
		} else {
			b.assertBinop(compute.ShiftRight, `[-1, -1]`, `[1, 5]`, `[-1, -1]`)
			b.assertBinop(compute.ShiftRight, fmt.Sprintf("[%d]", b.min), `[1]`, fmt.Sprintf("[%d]", b.min/2))

			b.assertBinopErr(compute.ShiftRight, `[1, 2]`, `[1, -1]`, "shift amount must be >= 0 and less than precision of type")
			b.assertBinopErr(compute.ShiftRight, `[1]`, fmt.Sprintf("[%d]", bitWidth), "shift amount must be >= 0 and less than precision of type")

			b.setOverflowCheck(false)
			b.assertBinop(compute.ShiftRight, `[1, 1]`, fmt.Sprintf("[-1, %d]", bitWidth), `[1, 1]`)
		}
	})
}

func TestBinaryArithmetic(t *testing.T) {
	suite.Run(t, &BinaryIntegralArithmeticSuite[int8]{BinaryArithmeticSuite[int8]{min: math.MinInt8, max: math.MaxInt8}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[uint8]{BinaryArithmeticSuite[uint8]{min: 0, max: math.MaxUint8}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[int16]{BinaryArithmeticSuite[int16]{min: math.MinInt16, max: math.MaxInt16}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[uint16]{BinaryArithmeticSuite[uint16]{min: 0, max: math.MaxUint16}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[int32]{BinaryArithmeticSuite[int32]{min: math.MinInt32, max: math.MaxInt32}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[uint32]{BinaryArithmeticSuite[uint32]{min: 0, max: math.MaxUint32}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[int64]{BinaryArithmeticSuite[int64]{min: math.MinInt64, max: math.MaxInt64}})
	suite.Run(t, &BinaryIntegralArithmeticSuite[uint64]{BinaryArithmeticSuite[uint64]{min: 0, max: math.MaxUint64}})
	suite.Run(t, &BinaryArithmeticSuite[float32]{min: -math.MaxFloat32, max: math.MaxFloat32})
	suite.Run(t, &BinaryArithmeticSuite[float64]{min: -math.MaxFloat64, max: math.MaxFloat64})
	suite.Run(t, new(Float16BinaryFuncTestSuite))
	suite.Run(t, new(DecimalBinaryArithmeticSuite))
	suite.Run(t, new(ScalarBinaryTemporalArithmeticSuite))
}

func TestBinaryArithmeticDispatchBest(t *testing.T) {
	for _, name := range []string{"add", "sub", "multiply", "divide", "power"} {
		for _, suffix := range []string{"", "_unchecked"} {
			name += suffix
			t.Run(name, func(t *testing.T) {

				tests := []struct {
					left, right arrow.DataType
					expected    arrow.DataType
				}{
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.Null, arrow.PrimitiveTypes.Int32},
					{arrow.Null, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Int32},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Int64},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint64, arrow.PrimitiveTypes.Int64},
					{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint8},
					{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint16},
					{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32},
					{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float32},
					{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64},
					{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Float64},
						arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64},
					{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Float64},
						arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Float64},
				}

				for _, tt := range tests {
					CheckDispatchBest(t, name, []arrow.DataType{tt.left, tt.right}, []arrow.DataType{tt.expected, tt.expected})
				}
			})
		}
	}
}

type DecimalArithmeticSuite struct {
	BinaryFuncTestSuite
}

func (*DecimalArithmeticSuite) positiveScales() []arrow.DataType {
	return []arrow.DataType{
		&arrow.Decimal128Type{Precision: 4, Scale: 2},
		&arrow.Decimal256Type{Precision: 4, Scale: 2},
		&arrow.Decimal128Type{Precision: 38, Scale: 2},
		&arrow.Decimal256Type{Precision: 76, Scale: 2},
	}
}

func (*DecimalArithmeticSuite) negativeScales() []arrow.DataType {
	return []arrow.DataType{
		&arrow.Decimal128Type{Precision: 2, Scale: -2},
		&arrow.Decimal256Type{Precision: 2, Scale: -2},
	}
}

func (ds *DecimalArithmeticSuite) checkDecimalToFloat(fn string, args []compute.Datum) {
	// validate that fn(*decimals) is the same as
	// fn([cast(x, float64) x for x in decimals])

	newArgs := make([]compute.Datum, len(args))
	for i, arg := range args {
		if arrow.IsDecimal(arg.(compute.ArrayLikeDatum).Type().ID()) {
			casted, err := compute.CastDatum(ds.ctx, arg, compute.NewCastOptions(arrow.PrimitiveTypes.Float64, true))
			ds.Require().NoError(err)
			defer casted.Release()
			newArgs[i] = casted
		} else {
			newArgs[i] = arg
		}
	}

	expected, err := compute.CallFunction(ds.ctx, fn, nil, newArgs...)
	ds.Require().NoError(err)
	defer expected.Release()
	actual, err := compute.CallFunction(ds.ctx, fn, nil, args...)
	ds.Require().NoError(err)
	defer actual.Release()

	assertDatumsEqual(ds.T(), expected, actual)
}

func (ds *DecimalArithmeticSuite) checkFail(fn string, args []compute.Datum, substr string, opts compute.FunctionOptions) {
	_, err := compute.CallFunction(ds.ctx, fn, opts, args...)
	ds.ErrorIs(err, arrow.ErrInvalid)
	ds.ErrorContains(err, substr)
}

func (ds *DecimalArithmeticSuite) decimalArrayFromJSON(ty arrow.DataType, str string) arrow.Array {
	arr, _, err := array.FromJSON(ds.mem, ty, strings.NewReader(str))
	ds.Require().NoError(err)
	return arr
}

type DecimalBinaryArithmeticSuite struct {
	DecimalArithmeticSuite
}

func (ds *DecimalBinaryArithmeticSuite) TestDispatchBest() {
	// decimal, floating point
	ds.Run("dec/floatingpoint", func() {
		for _, fn := range []string{"add", "sub", "multiply", "divide"} {
			for _, suffix := range []string{"", "_unchecked"} {
				fn += suffix
				ds.Run(fn, func() {

					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 1, Scale: 0},
						arrow.PrimitiveTypes.Float32}, []arrow.DataType{
						arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal256Type{Precision: 1, Scale: 0}, arrow.PrimitiveTypes.Float64},
						[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						arrow.PrimitiveTypes.Float32, &arrow.Decimal256Type{Precision: 1, Scale: 0}},
						[]arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						arrow.PrimitiveTypes.Float64, &arrow.Decimal128Type{Precision: 1, Scale: 0}},
						[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
				})
			}
		}
	})

	// decimal, decimal => decimal
	// decimal, integer => decimal
	ds.Run("dec/dec_int", func() {
		for _, fn := range []string{"add", "sub"} {
			for _, suffix := range []string{"", "_unchecked"} {
				fn += suffix
				ds.Run(fn, func() {
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						arrow.PrimitiveTypes.Int64, &arrow.Decimal128Type{Precision: 1, Scale: 0}},
						[]arrow.DataType{&arrow.Decimal128Type{Precision: 19, Scale: 0},
							&arrow.Decimal128Type{Precision: 1, Scale: 0}})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 1, Scale: 0}, arrow.PrimitiveTypes.Int64},
						[]arrow.DataType{&arrow.Decimal128Type{Precision: 1, Scale: 0},
							&arrow.Decimal128Type{Precision: 19, Scale: 0}})

					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
						[]arrow.DataType{&arrow.Decimal128Type{Precision: 2, Scale: 1},
							&arrow.Decimal128Type{Precision: 2, Scale: 1}})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
						[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
							&arrow.Decimal256Type{Precision: 2, Scale: 1}})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
						[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
							&arrow.Decimal256Type{Precision: 2, Scale: 1}})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
						[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
							&arrow.Decimal256Type{Precision: 2, Scale: 1}})

					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 2, Scale: 0}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
						[]arrow.DataType{&arrow.Decimal128Type{Precision: 3, Scale: 1},
							&arrow.Decimal128Type{Precision: 2, Scale: 1}})
					CheckDispatchBest(ds.T(), fn, []arrow.DataType{
						&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 0}},
						[]arrow.DataType{&arrow.Decimal128Type{Precision: 2, Scale: 1},
							&arrow.Decimal128Type{Precision: 3, Scale: 1}})
				})
			}
		}
	})

	{
		fn := "multiply"
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix
			ds.Run(fn, func() {
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					arrow.PrimitiveTypes.Int64, &arrow.Decimal128Type{Precision: 1}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 19},
						&arrow.Decimal128Type{Precision: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 1}, arrow.PrimitiveTypes.Int64},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 1},
						&arrow.Decimal128Type{Precision: 19}})

				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 2, Scale: 1},
						&arrow.Decimal128Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 2, Scale: 1},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})

				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 0}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 2, Scale: 0},
						&arrow.Decimal128Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 0}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 2, Scale: 1},
						&arrow.Decimal128Type{Precision: 2, Scale: 0}})
			})
		}
	}

	{
		fn := "divide"
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix
			ds.Run(fn, func() {
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					arrow.PrimitiveTypes.Int64, &arrow.Decimal128Type{Precision: 1, Scale: 0}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 23, Scale: 4},
						&arrow.Decimal128Type{Precision: 1, Scale: 0}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 1, Scale: 0}, arrow.PrimitiveTypes.Int64},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 21, Scale: 20},
						&arrow.Decimal128Type{Precision: 19, Scale: 0}})

				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 6, Scale: 5},
						&arrow.Decimal128Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 6, Scale: 5},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 6, Scale: 5},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal256Type{Precision: 6, Scale: 5},
						&arrow.Decimal256Type{Precision: 2, Scale: 1}})

				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 0}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 7, Scale: 5},
						&arrow.Decimal128Type{Precision: 2, Scale: 1}})
				CheckDispatchBest(ds.T(), fn, []arrow.DataType{
					&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 0}},
					[]arrow.DataType{&arrow.Decimal128Type{Precision: 5, Scale: 4},
						&arrow.Decimal128Type{Precision: 2, Scale: 0}})
			})
		}
	}

	for _, name := range []string{"power", "power_unchecked"} {
		ds.Run(name, func() {
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				&arrow.Decimal128Type{Precision: 2, Scale: 1}, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				&arrow.Decimal256Type{Precision: 2, Scale: 1}, &arrow.Decimal256Type{Precision: 2, Scale: 1}},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				&arrow.Decimal128Type{Precision: 2, Scale: 1}, arrow.PrimitiveTypes.Int64},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				arrow.PrimitiveTypes.Int32, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				&arrow.Decimal128Type{Precision: 2, Scale: 1}, arrow.PrimitiveTypes.Float64},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
			CheckDispatchBest(ds.T(), name, []arrow.DataType{
				arrow.PrimitiveTypes.Float32, &arrow.Decimal128Type{Precision: 2, Scale: 1}},
				[]arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64})
		})
	}
}

func (ds *DecimalBinaryArithmeticSuite) TestAddSubtractDec128() {
	left, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 30, Scale: 3},
		strings.NewReader(`["1.000", "-123456789012345678901234567.890", "98765432109876543210.987", "-999999999999999999999999999.999"]`))
	defer left.Release()
	right, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 20, Scale: 9},
		strings.NewReader(`["-1.000000000", "12345678901.234567890", "98765.432101234", "-99999999999.999999999"]`))
	defer right.Release()
	added, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 37, Scale: 9},
		strings.NewReader(`["0.000000000", "-123456789012345666555555666.655432110", "98765432109876641976.419101234", "-1000000000000000099999999999.998999999"]`))
	defer added.Release()
	subtracted, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 37, Scale: 9},
		strings.NewReader(`["2.000000000", "-123456789012345691246913469.124567890", "98765432109876444445.554898766", "-999999999999999899999999999.999000001"]`))
	defer subtracted.Release()

	leftDatum, rightDatum := &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()}
	checkScalarBinary(ds.T(), "add", leftDatum, rightDatum, &compute.ArrayDatum{Value: added.Data()}, nil)
	checkScalarBinary(ds.T(), "sub", leftDatum, rightDatum, &compute.ArrayDatum{Value: subtracted.Data()}, nil)
}

func (ds *DecimalBinaryArithmeticSuite) TestAddSubtractDec256() {
	left, _, _ := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 30, Scale: 20},
		strings.NewReader(`[
			"-1.00000000000000000001",
			"1234567890.12345678900000000000",
			"-9876543210.09876543210987654321",
			"9999999999.99999999999999999999"
		  ]`))
	defer left.Release()
	right, _, _ := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 30, Scale: 10},
		strings.NewReader(`[
			"1.0000000000",
			"-1234567890.1234567890",
			"6789.5432101234",
			"99999999999999999999.9999999999"
		  ]`))
	defer right.Release()
	added, _, _ := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 41, Scale: 20},
		strings.NewReader(`[
			"-0.00000000000000000001",
			"0.00000000000000000000",
			"-9876536420.55555530870987654321",
			"100000000009999999999.99999999989999999999"
		  ]`))
	defer added.Release()
	subtracted, _, _ := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 41, Scale: 20},
		strings.NewReader(`[
			"-2.00000000000000000001",
			"2469135780.24691357800000000000",
			"-9876549999.64197555550987654321",
			"-99999999989999999999.99999999990000000001"
		  ]`))
	defer subtracted.Release()

	leftDatum, rightDatum := &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()}
	checkScalarBinary(ds.T(), "add", leftDatum, rightDatum, &compute.ArrayDatum{Value: added.Data()}, nil)
	checkScalarBinary(ds.T(), "sub", leftDatum, rightDatum, &compute.ArrayDatum{Value: subtracted.Data()}, nil)
}

func (ds *DecimalBinaryArithmeticSuite) TestAddSubScalars() {
	ds.Run("scalar_array", func() {
		left := scalar.NewDecimal128Scalar(decimal128.New(0, 123456), &arrow.Decimal128Type{Precision: 6, Scale: 1})
		right, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 10, Scale: 3},
			strings.NewReader(`["1.234", "1234.000", "-9876.543", "666.888"]`))
		defer right.Release()
		added, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 11, Scale: 3},
			strings.NewReader(`["12346.834", "13579.600", "2469.057", "13012.488"]`))
		defer added.Release()
		leftSubRight, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 11, Scale: 3},
			strings.NewReader(`["12344.366", "11111.600", "22222.143", "11678.712"]`))
		defer leftSubRight.Release()
		rightSubLeft, _, _ := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 11, Scale: 3},
			strings.NewReader(`["-12344.366", "-11111.600", "-22222.143", "-11678.712"]`))
		defer rightSubLeft.Release()

		rightDatum := &compute.ArrayDatum{right.Data()}
		addedDatum := &compute.ArrayDatum{added.Data()}
		checkScalarBinary(ds.T(), "add", compute.NewDatum(left), rightDatum, addedDatum, nil)
		checkScalarBinary(ds.T(), "add", rightDatum, compute.NewDatum(left), addedDatum, nil)
		checkScalarBinary(ds.T(), "sub", compute.NewDatum(left), rightDatum, &compute.ArrayDatum{leftSubRight.Data()}, nil)
		checkScalarBinary(ds.T(), "sub", rightDatum, compute.NewDatum(left), &compute.ArrayDatum{rightSubLeft.Data()}, nil)
	})

	ds.Run("scalar_scalar", func() {
		left := scalar.NewDecimal256Scalar(decimal256.FromU64(666), &arrow.Decimal256Type{Precision: 3})
		right := scalar.NewDecimal256Scalar(decimal256.FromU64(888), &arrow.Decimal256Type{Precision: 3})
		added := scalar.NewDecimal256Scalar(decimal256.FromU64(1554), &arrow.Decimal256Type{Precision: 4})
		subtracted := scalar.NewDecimal256Scalar(decimal256.FromI64(-222), &arrow.Decimal256Type{Precision: 4})
		checkScalarBinary(ds.T(), "add", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(added), nil)
		checkScalarBinary(ds.T(), "sub", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(subtracted), nil)
	})

	ds.Run("dec128_dec256", func() {
		left := scalar.NewDecimal128Scalar(decimal128.FromU64(666), &arrow.Decimal128Type{Precision: 3})
		right := scalar.NewDecimal256Scalar(decimal256.FromU64(888), &arrow.Decimal256Type{Precision: 3})
		added := scalar.NewDecimal256Scalar(decimal256.FromU64(1554), &arrow.Decimal256Type{Precision: 4})
		checkScalarBinary(ds.T(), "add", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(added), nil)
		checkScalarBinary(ds.T(), "add", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(added), nil)
	})

	ds.Run("decimal_float", func() {
		left := scalar.NewDecimal128Scalar(decimal128.FromU64(666), &arrow.Decimal128Type{Precision: 3})
		right := scalar.MakeScalar(float64(888))
		added := scalar.MakeScalar(float64(1554))
		checkScalarBinary(ds.T(), "add", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(added), nil)
		checkScalarBinary(ds.T(), "add", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(added), nil)
	})

	ds.Run("decimal_integer", func() {
		left := scalar.NewDecimal128Scalar(decimal128.FromU64(666), &arrow.Decimal128Type{Precision: 3})
		right := scalar.MakeScalar(int64(888))
		added := scalar.NewDecimal128Scalar(decimal128.FromU64(1554), &arrow.Decimal128Type{Precision: 20})
		subtracted := scalar.NewDecimal128Scalar(decimal128.FromI64(-222), &arrow.Decimal128Type{Precision: 20})
		checkScalarBinary(ds.T(), "add", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(added), nil)
		checkScalarBinary(ds.T(), "sub", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(subtracted), nil)
	})
}

func (ds *DecimalBinaryArithmeticSuite) TestMultiply() {
	ds.Run("array x array, decimal128", func() {
		left, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 20, Scale: 10},
			strings.NewReader(`["1234567890.1234567890", "-0.0000000001", "-9999999999.9999999999"]`))
		ds.Require().NoError(err)
		defer left.Release()
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 13, Scale: 3},
			strings.NewReader(`["1234567890.123", "0.001", "-9999999999.999"]`))
		ds.Require().NoError(err)
		defer right.Release()
		expected, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 34, Scale: 13},
			strings.NewReader(`["1524157875323319737.98709039504701", "-0.0000000000001", "99999999999989999999.0000000000001"]`))
		ds.Require().NoError(err)
		defer expected.Release()

		checkScalarBinary(ds.T(), "multiply_unchecked", &compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{right.Data()}, &compute.ArrayDatum{expected.Data()}, nil)
	})

	ds.Run("array x array decimal256", func() {
		left, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 30, Scale: 3},
			strings.NewReader(`["123456789012345678901234567.890", "0.000"]`))
		ds.Require().NoError(err)
		defer left.Release()
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 20, Scale: 9},
			strings.NewReader(`["-12345678901.234567890", "99999999999.999999999"]`))
		ds.Require().NoError(err)
		defer right.Release()
		expected, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 51, Scale: 12},
			strings.NewReader(`["-1524157875323883675034293577501905199.875019052100", "0.000000000000"]`))
		ds.Require().NoError(err)
		defer expected.Release()
		checkScalarBinary(ds.T(), "multiply_unchecked", &compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{right.Data()}, &compute.ArrayDatum{expected.Data()}, nil)
	})

	ds.Run("scalar x array", func() {
		left, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3, Scale: 2}, "3.14")
		ds.Require().NoError(err)
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 1, Scale: 0},
			strings.NewReader(`["1", "2", "3", "4", "5"]`))
		ds.Require().NoError(err)
		defer right.Release()
		expected, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 5, Scale: 2},
			strings.NewReader(`["3.14", "6.28", "9.42", "12.56", "15.70"]`))
		ds.Require().NoError(err)
		defer expected.Release()

		leftDatum, rightDatum := &compute.ScalarDatum{left}, &compute.ArrayDatum{right.Data()}
		expDatum := &compute.ArrayDatum{expected.Data()}

		checkScalarBinary(ds.T(), "multiply_unchecked", leftDatum, rightDatum, expDatum, nil)
		checkScalarBinary(ds.T(), "multiply_unchecked", rightDatum, leftDatum, expDatum, nil)
	})

	ds.Run("scalar x scalar", func() {
		left, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 1}, "1")
		ds.Require().NoError(err)
		right, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 1}, "1")
		ds.Require().NoError(err)
		expected, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3}, "1")
		ds.Require().NoError(err)
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(expected), nil)
	})

	ds.Run("decimal128 x decimal256", func() {
		left, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3, Scale: 2}, "6.66")
		right, _ := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 3, Scale: 1}, "88.8")
		expected, _ := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 7, Scale: 3}, "591.408")
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(expected), nil)
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(expected), nil)
	})

	ds.Run("decimal x float", func() {
		left, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3}, "666")
		right := scalar.MakeScalar(float64(888))
		expected := scalar.MakeScalar(float64(591408))
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(expected), nil)
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(expected), nil)
	})

	ds.Run("decimal x integer", func() {
		left, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3}, "666")
		right := scalar.MakeScalar(int64(888))
		expected, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 23}, "591408")
		checkScalarBinary(ds.T(), "multiply_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(expected), nil)
	})
}

func (ds *DecimalBinaryArithmeticSuite) TestDivide() {
	ds.Run("array / array, decimal128", func() {
		left, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 13, Scale: 3},
			strings.NewReader(`["1234567890.123", "0.001"]`))
		ds.Require().NoError(err)
		defer left.Release()
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 3, Scale: 0},
			strings.NewReader(`["-987", "999"]`))
		ds.Require().NoError(err)
		defer right.Release()
		expected, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 17, Scale: 7},
			strings.NewReader(`["-1250828.6627386", "0.0000010"]`))
		ds.Require().NoError(err)
		defer expected.Release()

		checkScalarBinary(ds.T(), "divide_unchecked", &compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{right.Data()}, &compute.ArrayDatum{expected.Data()}, nil)
	})

	ds.Run("array / array decimal256", func() {
		left, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 20, Scale: 10},
			strings.NewReader(`["1234567890.1234567890", "9999999999.9999999999"]`))
		ds.Require().NoError(err)
		defer left.Release()
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 13, Scale: 3},
			strings.NewReader(`["1234567890.123", "0.001"]`))
		ds.Require().NoError(err)
		defer right.Release()
		expected, _, err := array.FromJSON(ds.mem, &arrow.Decimal256Type{Precision: 34, Scale: 21},
			strings.NewReader(`["1.000000000000369999093", "9999999999999.999999900000000000000"]`))
		ds.Require().NoError(err)
		defer expected.Release()
		checkScalarBinary(ds.T(), "divide_unchecked", &compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{right.Data()}, &compute.ArrayDatum{expected.Data()}, nil)
	})

	ds.Run("scalar / array", func() {
		left, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 1, Scale: 0}, "1")
		ds.Require().NoError(err)
		right, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 1, Scale: 0},
			strings.NewReader(`["1", "2", "3", "4"]`))
		ds.Require().NoError(err)
		defer right.Release()
		leftDivRight, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 5, Scale: 4},
			strings.NewReader(`["1.0000", "0.5000", "0.3333", "0.2500"]`))
		ds.Require().NoError(err)
		defer leftDivRight.Release()
		rightDivLeft, _, err := array.FromJSON(ds.mem, &arrow.Decimal128Type{Precision: 5, Scale: 4},
			strings.NewReader(`["1.0000", "2.0000", "3.0000", "4.0000"]`))
		ds.Require().NoError(err)
		defer rightDivLeft.Release()

		leftDatum, rightDatum := &compute.ScalarDatum{left}, &compute.ArrayDatum{right.Data()}

		checkScalarBinary(ds.T(), "divide_unchecked", leftDatum, rightDatum, &compute.ArrayDatum{leftDivRight.Data()}, nil)
		checkScalarBinary(ds.T(), "divide_unchecked", rightDatum, leftDatum, &compute.ArrayDatum{rightDivLeft.Data()}, nil)
	})

	ds.Run("scalar / scalar", func() {
		left, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 6, Scale: 5}, "2.71828")
		ds.Require().NoError(err)
		right, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 6, Scale: 5}, "3.14159")
		ds.Require().NoError(err)
		expected, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 13, Scale: 7}, "0.8652561")
		ds.Require().NoError(err)
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(expected), nil)
	})

	ds.Run("decimal128 / decimal256", func() {
		left, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 6, Scale: 5}, "2.71828")
		ds.Require().NoError(err)
		right, err := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 6, Scale: 5}, "3.14159")
		ds.Require().NoError(err)
		leftDivRight, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 13, Scale: 7}, "0.8652561")
		ds.Require().NoError(err)
		rightDivLeft, err := scalar.ParseScalar(&arrow.Decimal256Type{Precision: 13, Scale: 7}, "1.1557271")
		ds.Require().NoError(err)
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(leftDivRight), nil)
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(rightDivLeft), nil)
	})

	ds.Run("decimal / float", func() {
		left, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3}, "100")
		right := scalar.MakeScalar(float64(50))
		leftDivRight := scalar.MakeScalar(float64(2))
		rightDivLeft := scalar.MakeScalar(float64(0.5))
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(leftDivRight), nil)
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(rightDivLeft), nil)
	})

	ds.Run("decimal / integer", func() {
		left, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 3}, "100")
		right := scalar.MakeScalar(int64(50))
		leftDivRight, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 23, Scale: 20}, "2.0000000000000000000")
		rightDivLeft, _ := scalar.ParseScalar(&arrow.Decimal128Type{Precision: 23, Scale: 4}, "0.5000")
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(left), compute.NewDatum(right), compute.NewDatum(leftDivRight), nil)
		checkScalarBinary(ds.T(), "divide_unchecked", compute.NewDatum(right), compute.NewDatum(left), compute.NewDatum(rightDivLeft), nil)
	})
}

type DecimalUnaryArithmeticSuite struct {
	DecimalArithmeticSuite
}

func (ds *DecimalUnaryArithmeticSuite) TestAbsoluteValue() {
	max128 := decimal128.GetMaxValue(38)
	max256 := decimal256.GetMaxValue(76)
	ds.Run("decimal", func() {
		for _, fn := range []string{"abs_unchecked", "abs"} {
			ds.Run(fn, func() {
				for _, ty := range ds.positiveScales() {
					ds.Run(ty.String(), func() {
						empty, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`[]`))
						defer empty.Release()
						in, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["1.00", "-42.15", null]`))
						defer in.Release()
						exp, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["1.00", "42.15", null]`))
						defer exp.Release()

						checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{empty.Data()}}, &compute.ArrayDatum{empty.Data()}, nil)
						checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{in.Data()}}, &compute.ArrayDatum{exp.Data()}, nil)
					})
				}

				checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal128Scalar(max128.Negate(), &arrow.Decimal128Type{Precision: 38}))},
					compute.NewDatum(scalar.NewDecimal128Scalar(max128, &arrow.Decimal128Type{Precision: 38})), nil)
				checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal256Scalar(max256.Negate(), &arrow.Decimal256Type{Precision: 76}))},
					compute.NewDatum(scalar.NewDecimal256Scalar(max256, &arrow.Decimal256Type{Precision: 76})), nil)
				for _, ty := range ds.negativeScales() {
					ds.Run(ty.String(), func() {
						empty, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`[]`))
						defer empty.Release()
						in, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["12E2", "-42E2", null]`))
						defer in.Release()
						exp, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["12E2", "42E2", null]`))
						defer exp.Release()

						checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{empty.Data()}}, &compute.ArrayDatum{empty.Data()}, nil)
						checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{in.Data()}}, &compute.ArrayDatum{exp.Data()}, nil)
					})
				}
			})
		}
	})
}

func (ds *DecimalUnaryArithmeticSuite) TestNegate() {
	max128 := decimal128.GetMaxValue(38)
	max256 := decimal256.GetMaxValue(76)

	for _, fn := range []string{"negate_unchecked", "negate"} {
		ds.Run(fn, func() {
			for _, ty := range ds.positiveScales() {
				empty, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`[]`))
				defer empty.Release()
				in, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["0.00", "1.00", "-42.15", null]`))
				defer in.Release()
				exp, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["0.00", "-1.00", "42.15", null]`))
				defer exp.Release()

				checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{empty.Data()}}, &compute.ArrayDatum{empty.Data()}, nil)
				checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{in.Data()}}, &compute.ArrayDatum{exp.Data()}, nil)
			}

			checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal128Scalar(max128.Negate(), &arrow.Decimal128Type{Precision: 38}))},
				compute.NewDatum(scalar.NewDecimal128Scalar(max128, &arrow.Decimal128Type{Precision: 38})), nil)
			checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal256Scalar(max256.Negate(), &arrow.Decimal256Type{Precision: 76}))},
				compute.NewDatum(scalar.NewDecimal256Scalar(max256, &arrow.Decimal256Type{Precision: 76})), nil)
			checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal128Scalar(max128, &arrow.Decimal128Type{Precision: 38}))},
				compute.NewDatum(scalar.NewDecimal128Scalar(max128.Negate(), &arrow.Decimal128Type{Precision: 38})), nil)
			checkScalar(ds.T(), fn, []compute.Datum{compute.NewDatum(scalar.NewDecimal256Scalar(max256, &arrow.Decimal256Type{Precision: 76}))},
				compute.NewDatum(scalar.NewDecimal256Scalar(max256.Negate(), &arrow.Decimal256Type{Precision: 76})), nil)
			for _, ty := range ds.negativeScales() {
				ds.Run(ty.String(), func() {
					empty, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`[]`))
					defer empty.Release()
					in, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["0", "12E2", "-42E2", null]`))
					defer in.Release()
					exp, _, _ := array.FromJSON(ds.mem, ty, strings.NewReader(`["0", "-12E2", "42E2", null]`))
					defer exp.Release()

					checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{empty.Data()}}, &compute.ArrayDatum{empty.Data()}, nil)
					checkScalar(ds.T(), fn, []compute.Datum{&compute.ArrayDatum{in.Data()}}, &compute.ArrayDatum{exp.Data()}, nil)
				})
			}
		})
	}
}

func (ds *DecimalUnaryArithmeticSuite) TestSquareRoot() {
	for _, fn := range []string{"sqrt_unchecked", "sqrt"} {
		ds.Run(fn, func() {
			for _, ty := range ds.positiveScales() {
				ds.Run(ty.String(), func() {
					empty := ds.decimalArrayFromJSON(ty, `[]`)
					defer empty.Release()
					arr := ds.decimalArrayFromJSON(ty, `["4.00", "16.00", "36.00", null]`)
					defer arr.Release()

					ds.checkDecimalToFloat(fn, []compute.Datum{&compute.ArrayDatum{Value: empty.Data()}})
					ds.checkDecimalToFloat(fn, []compute.Datum{&compute.ArrayDatum{Value: arr.Data()}})

					neg := ds.decimalArrayFromJSON(ty, `["-2.00"]`)
					defer neg.Release()
					ds.checkFail("sqrt", []compute.Datum{&compute.ArrayDatum{Value: neg.Data()}}, "square root of negative number", nil)
				})
			}

			for _, ty := range ds.negativeScales() {
				ds.Run(ty.String(), func() {
					empty := ds.decimalArrayFromJSON(ty, `[]`)
					defer empty.Release()
					arr := ds.decimalArrayFromJSON(ty, `["400", "1600", "3600", null]`)
					defer arr.Release()

					ds.checkDecimalToFloat(fn, []compute.Datum{&compute.ArrayDatum{Value: empty.Data()}})
					ds.checkDecimalToFloat(fn, []compute.Datum{&compute.ArrayDatum{Value: arr.Data()}})

					neg := ds.decimalArrayFromJSON(ty, `["-400"]`)
					defer neg.Release()
					ds.checkFail("sqrt", []compute.Datum{&compute.ArrayDatum{Value: neg.Data()}}, "square root of negative number", nil)
				})
			}
		})
	}
}

type ScalarBinaryTemporalArithmeticSuite struct {
	BinaryFuncTestSuite
}

var (
	date32JSON = `[0, 11016, -25932, 23148, 18262, 18261, 18260, 14609, 14610, 14612,
	14613, 13149, 13148, 14241, 14242, 15340, null]`
	date32JSON2 = `[365, 10650, -25901, 23118, 18263, 18259, 18260, 14609, 14610, 14612,
	14613, 13149, 13148, 14240, 13937, 15400, null]`
	date64JSON = `[0, 951782400000, -2240524800000, 1999987200000, 1577836800000,
	1577750400000, 1577664000000, 1262217600000, 1262304000000, 1262476800000,
	1262563200000, 1136073600000, 1135987200000, 1230422400000, 1230508800000,
	1325376000000, null]`
	date64JSON2 = `[31536000000, 920160000000, -2237846400000, 1997395200000,
	1577923200000, 1577577600000, 1577664000000, 1262217600000, 1262304000000,
	1262476800000, 1262563200000, 1136073600000, 1135987200000, 1230336000000,
	1204156800000, 1330560000000, null]`
	timeJSONs = `[59, 84203, 3560, 12800, 3905, 7810, 11715, 15620, 19525, 23430, 27335,
	31240, 35145, 0, 0, 3723, null]`
	timeJSONs2 = `[59, 84203, 12642, 7182, 68705, 7390, 915, 16820, 19525, 5430, 84959,
	31207, 35145, 0, 0, 3723, null]`
	timeJSONms = `[59123, 84203999, 3560001, 12800000, 3905001, 7810002, 11715003, 15620004,
	19525005, 23430006, 27335000, 31240000, 35145000, 0, 0, 3723000, null]`
	timeJSONms2 = `[59103, 84203999, 12642001, 7182000, 68705005, 7390000, 915003, 16820004,
	19525005, 5430006, 84959000, 31207000, 35145000, 0, 0, 3723000, null]`
	timeJSONus = `[59123456, 84203999999, 3560001001, 12800000000, 3905001000, 7810002000,
	11715003000, 15620004132, 19525005321, 23430006163, 27335000000,
	31240000000, 35145000000, 0, 0, 3723000000, null]`
	timeJSONus2 = `[59103476, 84203999999, 12642001001, 7182000000, 68705005000, 7390000000,
	915003000, 16820004432, 19525005021, 5430006163, 84959000000,
	31207000000, 35145000000, 0, 0, 3723000000, null]`
	timeJSONns = `[59123456789, 84203999999999, 3560001001001, 12800000000000, 3905001000000,
	7810002000000, 11715003000000, 15620004132000, 19525005321000,
	23430006163000, 27335000000000, 31240000000000, 35145000000000, 0, 0,
	3723000000000, null]`
	timeJSONns2 = `[59103476799, 84203999999909, 12642001001001, 7182000000000, 68705005000000,
	7390000000000, 915003000000, 16820004432000, 19525005021000, 5430006163000,
	84959000000000, 31207000000000, 35145000000000, 0, 0, 3723000000000, null]`
)

func (s *ScalarBinaryTemporalArithmeticSuite) TestTemporalAddSub() {
	tests := []struct {
		val1 string
		val2 string
		dt   arrow.DataType
		exp  arrow.DataType
	}{
		{date32JSON, date32JSON2, arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Duration_s},
		{date64JSON, date64JSON2, arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Duration_ms},
		{timeJSONs, timeJSONs2, arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Duration_s},
		{timeJSONms, timeJSONms2, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Duration_ms},
		{timeJSONus, timeJSONus2, arrow.FixedWidthTypes.Time64us, arrow.FixedWidthTypes.Duration_us},
		{timeJSONns, timeJSONns2, arrow.FixedWidthTypes.Time64ns, arrow.FixedWidthTypes.Duration_ns},
	}

	for _, tt := range tests {
		s.Run(tt.dt.String(), func() {
			for _, checked := range []bool{true, false} {
				s.Run(fmt.Sprintf("checked=%t", checked), func() {
					opts := compute.ArithmeticOptions{NoCheckOverflow: !checked}
					arr1, _, _ := array.FromJSON(s.mem, tt.dt, strings.NewReader(tt.val1))
					defer arr1.Release()
					arr2, _, _ := array.FromJSON(s.mem, tt.dt, strings.NewReader(tt.val2))
					defer arr2.Release()

					datum1 := &compute.ArrayDatum{Value: arr1.Data()}
					datum2 := &compute.ArrayDatum{Value: arr2.Data()}

					result, err := compute.Subtract(s.ctx, opts, datum1, datum2)
					s.Require().NoError(err)
					defer result.Release()
					res := result.(*compute.ArrayDatum)
					s.Truef(arrow.TypeEqual(tt.exp, res.Type()),
						"expected: %s\n got: %s", tt.exp, res.Type())

					out, err := compute.Add(s.ctx, opts, datum2, result)
					s.Require().NoError(err)
					defer out.Release()

					// date32 - date32 / date64 - date64 produce durations
					// and date + duration == timestamp so we need to cast
					// the timestamp back to a date in that case. Otherwise
					// we get back time32/time64 in those cases and can
					// compare them accurately.
					if arrow.TypeEqual(arr1.DataType(), out.(*compute.ArrayDatum).Type()) {
						assertDatumsEqual(s.T(), datum1, out)
					} else {
						casted, err := compute.CastDatum(s.ctx, out, compute.SafeCastOptions(arr1.DataType()))
						s.Require().NoError(err)
						defer casted.Release()
						assertDatumsEqual(s.T(), datum1, casted)
					}

				})
			}
		})
	}
}

func TestUnaryDispatchBest(t *testing.T) {
	for _, fn := range []string{"abs"} {
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix
			t.Run(fn, func(t *testing.T) {
				for _, ty := range numericTypes {
					t.Run(ty.String(), func(t *testing.T) {
						CheckDispatchBest(t, fn, []arrow.DataType{ty}, []arrow.DataType{ty})
						CheckDispatchBest(t, fn, []arrow.DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: ty}},
							[]arrow.DataType{ty})
					})
				}
			})
		}
	}

	for _, fn := range []string{"negate_unchecked", "sign"} {
		t.Run(fn, func(t *testing.T) {
			for _, ty := range numericTypes {
				t.Run(ty.String(), func(t *testing.T) {
					CheckDispatchBest(t, fn, []arrow.DataType{ty}, []arrow.DataType{ty})
					CheckDispatchBest(t, fn, []arrow.DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: ty}},
						[]arrow.DataType{ty})
				})
			}
		})
	}

	for _, fn := range []string{"negate"} {
		t.Run(fn, func(t *testing.T) {
			for _, ty := range append(signedIntTypes, floatingTypes...) {
				t.Run(ty.String(), func(t *testing.T) {
					CheckDispatchBest(t, fn, []arrow.DataType{ty}, []arrow.DataType{ty})
					CheckDispatchBest(t, fn, []arrow.DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: ty}},
						[]arrow.DataType{ty})
				})
			}
		})
	}
}

func TestUnaryArithmeticNull(t *testing.T) {
	for _, fn := range []string{"abs", "negate"} {
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix
			assertNullToNull(t, context.TODO(), fn, memory.DefaultAllocator)
		}
	}

	for _, fn := range []string{"sign"} {
		assertNullToNull(t, context.TODO(), fn, memory.DefaultAllocator)
	}
}

type UnaryArithmeticSuite[T exec.NumericTypes] struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context

	opts compute.ArithmeticOptions
}

func (b *UnaryArithmeticSuite[T]) SetupTest() {
	b.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	b.ctx = compute.WithAllocator(context.TODO(), b.mem)
	b.opts = compute.ArithmeticOptions{}
}

func (b *UnaryArithmeticSuite[T]) TearDownTest() {
	b.mem.AssertSize(b.T(), 0)
}

func (b *UnaryArithmeticSuite[T]) setOverflowCheck(v bool) {
	b.opts.NoCheckOverflow = !v
}

func (*UnaryArithmeticSuite[T]) datatype() arrow.DataType {
	return exec.GetDataType[T]()
}

func (us *UnaryArithmeticSuite[T]) makeNullScalar() scalar.Scalar {
	return scalar.MakeNullScalar(us.datatype())
}

func (us *UnaryArithmeticSuite[T]) makeScalar(v T) scalar.Scalar {
	return scalar.MakeScalar(v)
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOpValError(fn unaryArithmeticFunc, arg T, msg string) {
	in := us.makeScalar(arg)
	_, err := fn(us.ctx, us.opts, compute.NewDatum(in))
	us.ErrorIs(err, arrow.ErrInvalid)
	us.ErrorContains(err, msg)
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOpNotImplemented(fn unaryArithmeticFunc, arg T, msg string) {
	in := us.makeScalar(arg)
	_, err := fn(us.ctx, us.opts, compute.NewDatum(in))
	us.ErrorIs(err, arrow.ErrNotImplemented)
	us.ErrorContains(err, msg)
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOpVals(fn unaryArithmeticFunc, arg, expected T) {
	in := us.makeScalar(arg)
	exp := us.makeScalar(expected)

	actual, err := fn(us.ctx, us.opts, compute.NewDatum(in))
	us.Require().NoError(err)
	assertScalarEquals(us.T(), exp, actual.(*compute.ScalarDatum).Value)
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOpScalars(fn unaryArithmeticFunc, arg, exp scalar.Scalar) {
	actual, err := fn(us.ctx, us.opts, compute.NewDatum(arg))
	us.Require().NoError(err)
	assertScalarEquals(us.T(), exp, actual.(*compute.ScalarDatum).Value)
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOpArrs(fn unaryArithmeticFunc, arg, exp arrow.Array) {
	datum := &compute.ArrayDatum{arg.Data()}
	actual, err := fn(us.ctx, us.opts, datum)
	us.Require().NoError(err)
	defer actual.Release()
	assertDatumsEqual(us.T(), &compute.ArrayDatum{exp.Data()}, actual)

	// also check scalar ops
	for i := 0; i < arg.Len(); i++ {
		expScalar, err := scalar.GetScalar(exp, i)
		us.NoError(err)
		argScalar, err := scalar.GetScalar(arg, i)
		us.NoError(err)

		actual, err := fn(us.ctx, us.opts, compute.NewDatum(argScalar))
		us.Require().NoError(err)
		assertDatumsEqual(us.T(), compute.NewDatum(expScalar), compute.NewDatum(actual))
	}
}

func (us *UnaryArithmeticSuite[T]) assertUnaryOp(fn unaryArithmeticFunc, arg, exp string) {
	in, _, err := array.FromJSON(us.mem, us.datatype(), strings.NewReader(arg), array.WithUseNumber())
	us.Require().NoError(err)
	defer in.Release()
	expected, _, err := array.FromJSON(us.mem, us.datatype(), strings.NewReader(exp), array.WithUseNumber())
	us.Require().NoError(err)
	defer expected.Release()

	us.assertUnaryOpArrs(fn, in, expected)
}

type UnaryArithmeticSigned[T exec.IntTypes] struct {
	UnaryArithmeticSuite[T]
}

func (us *UnaryArithmeticSigned[T]) TestAbsoluteValue() {
	var (
		dt  = us.datatype()
		min = kernels.MinOf[T]()
		max = kernels.MaxOf[T]()
	)

	fn := func(in, exp string) {
		us.assertUnaryOp(compute.AbsoluteValue, in, exp)
	}

	us.Run(dt.String(), func() {
		for _, checkOverflow := range []bool{true, false} {
			us.setOverflowCheck(checkOverflow)
			us.Run(fmt.Sprintf("check_overflow=%t", checkOverflow), func() {
				// empty array
				fn(`[]`, `[]`)
				// scalar/arrays with nulls
				fn(`[null]`, `[null]`)
				fn(`[1, null -10]`, `[1, null, 10]`)
				us.assertUnaryOpScalars(compute.AbsoluteValue, us.makeNullScalar(), us.makeNullScalar())
				// scalar/arrays with zeros
				fn(`[0, -0]`, `[0, 0]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, -0, 0)
				us.assertUnaryOpVals(compute.AbsoluteValue, 0, 0)
				// ordinary scalars/arrays (positive inputs)
				fn(`[1, 10, 127]`, `[1, 10, 127]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, 1, 1)
				// ordinary scalars/arrays (negative inputs)
				fn(`[-1, -10, -127]`, `[1, 10, 127]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, -1, 1)
				// min/max
				us.assertUnaryOpVals(compute.AbsoluteValue, max, max)
				if checkOverflow {
					us.assertUnaryOpValError(compute.AbsoluteValue, min, "overflow")
				} else {
					us.assertUnaryOpVals(compute.AbsoluteValue, min, min)
				}
			})
		}
	})
}

func (us *UnaryArithmeticSigned[T]) TestNegate() {
	var (
		dt  = us.datatype()
		min = kernels.MinOf[T]()
		max = kernels.MaxOf[T]()
	)

	fn := func(in, exp string) {
		us.assertUnaryOp(compute.Negate, in, exp)
	}

	us.Run(dt.String(), func() {
		for _, checkOverflow := range []bool{true, false} {
			us.setOverflowCheck(checkOverflow)
			us.Run(fmt.Sprintf("check_overflow=%t", checkOverflow), func() {
				fn(`[]`, `[]`)
				// scalar/arrays with nulls
				fn(`[null]`, `[null]`)
				fn(`[1, null -10]`, `[-1, null, 10]`)
				// ordinary scalars/arrays (positive inputs)
				fn(`[1, 10, 127]`, `[-1, -10, -127]`)
				us.assertUnaryOpVals(compute.Negate, 1, -1)
				// ordinary scalars/arrays (negative inputs)
				fn(`[-1, -10, -127]`, `[1, 10, 127]`)
				us.assertUnaryOpVals(compute.Negate, -1, 1)
				// min/max
				us.assertUnaryOpVals(compute.Negate, min+1, max)
				us.assertUnaryOpVals(compute.Negate, max, min+1)
			})
		}
	})
}

type UnaryArithmeticUnsigned[T exec.UintTypes] struct {
	UnaryArithmeticSuite[T]
}

func (us *UnaryArithmeticUnsigned[T]) TestAbsoluteValue() {
	var (
		min, max T = 0, kernels.MaxOf[T]()
	)

	fn := func(in, exp string) {
		us.assertUnaryOp(compute.AbsoluteValue, in, exp)
	}

	us.Run(us.datatype().String(), func() {
		for _, checkOverflow := range []bool{true, false} {
			us.setOverflowCheck(checkOverflow)
			us.Run(fmt.Sprintf("check_overflow=%t", checkOverflow), func() {
				fn(`[]`, `[]`)
				fn(`[null]`, `[null]`)
				us.assertUnaryOpScalars(compute.AbsoluteValue, us.makeNullScalar(), us.makeNullScalar())
				fn(`[0, 1, 10, 127]`, `[0, 1, 10, 127]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, min, min)
				us.assertUnaryOpVals(compute.AbsoluteValue, max, max)
			})
		}
	})
}

func (us *UnaryArithmeticUnsigned[T]) TestNegate() {
	var (
		dt = us.datatype()
	)

	fn := func(in, exp string) {
		us.assertUnaryOp(compute.Negate, in, exp)
	}

	us.Run(dt.String(), func() {
		us.setOverflowCheck(true)
		us.assertUnaryOpNotImplemented(compute.Negate, 1, "no kernel matching input types")

		us.setOverflowCheck(false)
		fn(`[]`, `[]`)
		fn(`[null]`, `[null]`)
		us.assertUnaryOpVals(compute.Negate, 1, ^T(1)+1)
	})
}

type UnaryArithmeticFloating[T constraints.Float] struct {
	UnaryArithmeticSuite[T]

	min, max T
}

func (us *UnaryArithmeticFloating[T]) TestAbsoluteValue() {
	fn := func(in, exp string) {
		us.assertUnaryOp(compute.AbsoluteValue, in, exp)
	}

	us.Run(us.datatype().String(), func() {
		for _, checkOverflow := range []bool{true, false} {
			us.setOverflowCheck(checkOverflow)
			us.Run(fmt.Sprintf("check_overflow=%t", checkOverflow), func() {
				fn(`[]`, `[]`)
				fn(`[null]`, `[null]`)
				fn(`[1.3, null, -10.80]`, `[1.3, null, 10.80]`)
				us.assertUnaryOpScalars(compute.AbsoluteValue, us.makeNullScalar(), us.makeNullScalar())
				fn(`[0.0, -0.0]`, `[0.0, 0.0]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, T(math.Copysign(0, -1)), 0)
				us.assertUnaryOpVals(compute.AbsoluteValue, 0, 0)
				fn(`[1.3, 10.80, 12748.001]`, `[1.3, 10.80, 12748.001]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, 1.3, 1.3)
				fn(`[-1.3, -10.80, -12748.001]`, `[1.3, 10.80, 12748.001]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, -1.3, 1.3)
				fn(`["Inf", "-Inf"]`, `["Inf", "Inf"]`)
				us.assertUnaryOpVals(compute.AbsoluteValue, us.min, us.max)
				us.assertUnaryOpVals(compute.AbsoluteValue, us.max, us.max)
			})
		}
	})
}

func (us *UnaryArithmeticFloating[T]) TestNegate() {
	var (
		dt = us.datatype()
	)

	fn := func(in, exp string) {
		us.assertUnaryOp(compute.Negate, in, exp)
	}

	us.Run(dt.String(), func() {
		for _, checkOverflow := range []bool{true, false} {
			us.setOverflowCheck(checkOverflow)
			us.Run(fmt.Sprintf("check_overflow=%t", checkOverflow), func() {
				fn(`[]`, `[]`)
				// scalar/arrays with nulls
				fn(`[null]`, `[null]`)
				fn(`[1.5, null -10.25]`, `[-1.5, null, 10.25]`)
				// ordinary scalars/arrays (positive inputs)
				fn(`[0.5, 10.123, 127.321]`, `[-0.5, -10.123, -127.321]`)
				us.assertUnaryOpVals(compute.Negate, 1.25, -1.25)
				// ordinary scalars/arrays (negative inputs)
				fn(`[-0.5, -10.123, -127.321]`, `[0.5, 10.123, 127.321]`)
				us.assertUnaryOpVals(compute.Negate, -1.25, 1.25)
				// min/max
				us.assertUnaryOpVals(compute.Negate, us.min, us.max)
				us.assertUnaryOpVals(compute.Negate, us.max, us.min)
			})
		}
	})
}

func TestUnaryArithmetic(t *testing.T) {
	suite.Run(t, new(UnaryArithmeticSigned[int8]))
	suite.Run(t, new(UnaryArithmeticSigned[int16]))
	suite.Run(t, new(UnaryArithmeticSigned[int32]))
	suite.Run(t, new(UnaryArithmeticSigned[int64]))
	suite.Run(t, new(UnaryArithmeticUnsigned[uint8]))
	suite.Run(t, new(UnaryArithmeticUnsigned[uint16]))
	suite.Run(t, new(UnaryArithmeticUnsigned[uint32]))
	suite.Run(t, new(UnaryArithmeticUnsigned[uint64]))
	suite.Run(t, &UnaryArithmeticFloating[float32]{min: -math.MaxFloat32, max: math.MaxFloat32})
	suite.Run(t, &UnaryArithmeticFloating[float64]{min: -math.MaxFloat64, max: math.MaxFloat64})
	suite.Run(t, new(DecimalUnaryArithmeticSuite))
}

type BitwiseArithmeticSuite[T exec.IntTypes | exec.UintTypes] struct {
	BinaryFuncTestSuite
}

func (bs *BitwiseArithmeticSuite[T]) datatype() arrow.DataType {
	return exec.GetDataType[T]()
}

// to make it easier to test different widths, tests give bytes which
// get repeated to make an array of the actual type
func (bs *BitwiseArithmeticSuite[T]) expandByteArray(values []byte) arrow.Array {
	vals := make([]T, len(values)+1)
	sz := kernels.SizeOf[T]()
	for i, v := range values {
		memory.Set(unsafe.Slice((*byte)(unsafe.Pointer(&vals[i])), sz), v)
	}
	valid := make([]bool, len(vals))
	for i := range values {
		valid[i] = true
	}
	return exec.ArrayFromSliceWithValid(bs.mem, vals, valid)
}

func (bs *BitwiseArithmeticSuite[T]) assertBinaryOp(fn string, arg0, arg1, expected []byte) {
	in0, in1 := bs.expandByteArray(arg0), bs.expandByteArray(arg1)
	out := bs.expandByteArray(expected)
	defer func() {
		in0.Release()
		in1.Release()
		out.Release()
	}()

	actual, err := compute.CallFunction(bs.ctx, fn, nil, &compute.ArrayDatum{in0.Data()}, &compute.ArrayDatum{in1.Data()})
	bs.Require().NoError(err)
	defer actual.Release()
	assertDatumsEqual(bs.T(), &compute.ArrayDatum{out.Data()}, actual)

	for i := 0; i < out.Len(); i++ {
		a0, err := scalar.GetScalar(in0, i)
		bs.Require().NoError(err)
		a1, err := scalar.GetScalar(in1, i)
		bs.Require().NoError(err)
		exp, err := scalar.GetScalar(out, i)
		bs.Require().NoError(err)

		actual, err := compute.CallFunction(bs.ctx, fn, nil, compute.NewDatum(a0), compute.NewDatum(a1))
		bs.Require().NoError(err)
		assertScalarEquals(bs.T(), exp, actual.(*compute.ScalarDatum).Value)
	}
}

func (bs *BitwiseArithmeticSuite[T]) TestBitWiseAnd() {
	bs.Run(bs.datatype().String(), func() {
		bs.assertBinaryOp("bit_wise_and", []byte{0x00, 0xFF, 0x00, 0xFF},
			[]byte{0x00, 0x00, 0xFF, 0xFF}, []byte{0x00, 0x00, 0x00, 0xFF})
	})
}

func (bs *BitwiseArithmeticSuite[T]) TestBitWiseOr() {
	bs.Run(bs.datatype().String(), func() {
		bs.assertBinaryOp("bit_wise_or", []byte{0x00, 0xFF, 0x00, 0xFF},
			[]byte{0x00, 0x00, 0xFF, 0xFF}, []byte{0x00, 0xFF, 0xFF, 0xFF})
	})
}

func (bs *BitwiseArithmeticSuite[T]) TestBitWiseXor() {
	bs.Run(bs.datatype().String(), func() {
		bs.assertBinaryOp("bit_wise_xor", []byte{0x00, 0xFF, 0x00, 0xFF},
			[]byte{0x00, 0x00, 0xFF, 0xFF}, []byte{0x00, 0xFF, 0xFF, 0x00})
	})
}

func TestBitwiseArithmetic(t *testing.T) {
	suite.Run(t, new(BitwiseArithmeticSuite[int8]))
	suite.Run(t, new(BitwiseArithmeticSuite[uint8]))
	suite.Run(t, new(BitwiseArithmeticSuite[int16]))
	suite.Run(t, new(BitwiseArithmeticSuite[uint16]))
	suite.Run(t, new(BitwiseArithmeticSuite[int32]))
	suite.Run(t, new(BitwiseArithmeticSuite[uint32]))
	suite.Run(t, new(BitwiseArithmeticSuite[int64]))
	suite.Run(t, new(BitwiseArithmeticSuite[uint64]))
}

const seed = 0x94378165

type binaryOp = func(ctx context.Context, left, right compute.Datum) (compute.Datum, error)

func Add(ctx context.Context, left, right compute.Datum) (compute.Datum, error) {
	var opts compute.ArithmeticOptions
	return compute.Add(ctx, opts, left, right)
}

func Subtract(ctx context.Context, left, right compute.Datum) (compute.Datum, error) {
	var opts compute.ArithmeticOptions
	return compute.Subtract(ctx, opts, left, right)
}

func AddUnchecked(ctx context.Context, left, right compute.Datum) (compute.Datum, error) {
	opts := compute.ArithmeticOptions{NoCheckOverflow: true}
	return compute.Add(ctx, opts, left, right)
}

func SubtractUnchecked(ctx context.Context, left, right compute.Datum) (compute.Datum, error) {
	opts := compute.ArithmeticOptions{NoCheckOverflow: true}
	return compute.Subtract(ctx, opts, left, right)
}

func arrayScalarKernel(b *testing.B, sz int, nullProp float64, op binaryOp, dt arrow.DataType) {
	b.Run("array scalar", func(b *testing.B) {
		var (
			mem                     = memory.NewCheckedAllocator(memory.DefaultAllocator)
			arraySize               = int64(sz / dt.(arrow.FixedWidthDataType).Bytes())
			min       int64         = 6
			max                     = min + 15
			sc, _                   = scalar.MakeScalarParam(6, dt)
			rhs       compute.Datum = &compute.ScalarDatum{Value: sc}
			rng                     = gen.NewRandomArrayGenerator(seed, mem)
		)

		lhs := rng.Numeric(dt.ID(), arraySize, min, max, nullProp)
		b.Cleanup(func() {
			lhs.Release()
		})

		var (
			res  compute.Datum
			err  error
			ctx  = context.Background()
			left = &compute.ArrayDatum{Value: lhs.Data()}
		)

		b.SetBytes(arraySize)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			res, err = op(ctx, left, rhs)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			res.Release()
			b.StartTimer()
		}
	})
}

func arrayArrayKernel(b *testing.B, sz int, nullProp float64, op binaryOp, dt arrow.DataType) {
	b.Run("array array", func(b *testing.B) {
		var (
			mem             = memory.NewCheckedAllocator(memory.DefaultAllocator)
			arraySize       = int64(sz / dt.(arrow.FixedWidthDataType).Bytes())
			rmin      int64 = 1
			rmax            = rmin + 6 // 7
			lmin            = rmax + 1 // 8
			lmax            = lmin + 6 // 14
			rng             = gen.NewRandomArrayGenerator(seed, mem)
		)

		lhs := rng.Numeric(dt.ID(), arraySize, lmin, lmax, nullProp)
		rhs := rng.Numeric(dt.ID(), arraySize, rmin, rmax, nullProp)
		b.Cleanup(func() {
			lhs.Release()
			rhs.Release()
		})
		var (
			res   compute.Datum
			err   error
			ctx   = context.Background()
			left  = &compute.ArrayDatum{Value: lhs.Data()}
			right = &compute.ArrayDatum{Value: rhs.Data()}
		)

		b.SetBytes(arraySize)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			res, err = op(ctx, left, right)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			res.Release()
			b.StartTimer()
		}
	})
}

func BenchmarkScalarArithmetic(b *testing.B) {
	args := []struct {
		sz       int
		nullProb float64
	}{
		{CpuCacheSizes[2], 0},
		{CpuCacheSizes[2], 0.5},
		{CpuCacheSizes[2], 1},
	}

	testfns := []struct {
		name string
		op   binaryOp
	}{
		{"Add", Add},
		{"AddUnchecked", AddUnchecked},
		{"Subtract", Subtract},
		{"SubtractUnchecked", SubtractUnchecked},
	}

	for _, dt := range numericTypes {
		b.Run(dt.String(), func(b *testing.B) {
			for _, benchArgs := range args {
				b.Run(fmt.Sprintf("sz=%d/nullprob=%.2f", benchArgs.sz, benchArgs.nullProb), func(b *testing.B) {
					for _, tfn := range testfns {
						b.Run(tfn.name, func(b *testing.B) {
							arrayArrayKernel(b, benchArgs.sz, benchArgs.nullProb, tfn.op, dt)
							arrayScalarKernel(b, benchArgs.sz, benchArgs.nullProb, tfn.op, dt)
						})
					}
				})
			}
		})
	}
}

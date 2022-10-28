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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/compute"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/decimal256"
	"github.com/apache/arrow/go/v11/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/arrow/scalar"
	"github.com/klauspost/cpuid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

type binaryArithmeticFunc = func(context.Context, compute.ArithmeticOptions, compute.Datum, compute.Datum) (compute.Datum, error)

type binaryFunc = func(left, right compute.Datum) (compute.Datum, error)

func assertScalarEquals(t *testing.T, expected, actual scalar.Scalar) {
	assert.Truef(t, scalar.Equals(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func assertBinop(t *testing.T, fn binaryFunc, left, right, expected arrow.Array) {
	actual, err := fn(&compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()})
	require.NoError(t, err)
	defer actual.Release()
	assertDatumsEqual(t, &compute.ArrayDatum{Value: expected.Data()}, actual)

	// also check (Scalar, Scalar) operations
	for i := 0; i < expected.Len(); i++ {
		s, err := scalar.GetScalar(expected, i)
		require.NoError(t, err)
		lhs, _ := scalar.GetScalar(left, i)
		rhs, _ := scalar.GetScalar(right, i)

		actual, err := fn(&compute.ScalarDatum{Value: lhs}, &compute.ScalarDatum{Value: rhs})
		assert.NoError(t, err)
		assertScalarEquals(t, s, actual.(*compute.ScalarDatum).Value)
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

	opts     compute.ArithmeticOptions
	min, max T
}

func (BinaryArithmeticSuite[T]) DataType() arrow.DataType {
	return exec.GetDataType[T]()
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
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: exp.Data()}, actual)
}

func (b *BinaryArithmeticSuite[T]) assertBinop(fn binaryArithmeticFunc, lhs, rhs, expected string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs))
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs))
	defer right.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	assertBinop(b.T(), func(left, right compute.Datum) (compute.Datum, error) {
		return fn(b.ctx, b.opts, left, right)
	}, left, right, exp)
}

func (b *BinaryArithmeticSuite[T]) setOverflowCheck(value bool) {
	b.opts.NoCheckOverflow = value
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

				if !arrow.IsFloating(b.DataType().ID()) && !overflow {
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

				if !arrow.IsFloating(b.DataType().ID()) && !overflow {
					b.assertBinopErr(compute.Subtract, fmt.Sprintf("[%v]", b.min), fmt.Sprintf("[%v]", b.max), "overflow")
				}
			})
		}
	})
}

func TestBinaryArithmetic(t *testing.T) {
	suite.Run(t, &BinaryArithmeticSuite[int8]{min: math.MinInt8, max: math.MaxInt8})
	suite.Run(t, &BinaryArithmeticSuite[uint8]{min: 0, max: math.MaxUint8})
	suite.Run(t, &BinaryArithmeticSuite[int16]{min: math.MinInt16, max: math.MaxInt16})
	suite.Run(t, &BinaryArithmeticSuite[uint16]{min: 0, max: math.MaxUint16})
	suite.Run(t, &BinaryArithmeticSuite[int32]{min: math.MinInt32, max: math.MaxInt32})
	suite.Run(t, &BinaryArithmeticSuite[uint32]{min: 0, max: math.MaxUint32})
	suite.Run(t, &BinaryArithmeticSuite[int64]{min: math.MinInt64, max: math.MaxInt64})
	suite.Run(t, &BinaryArithmeticSuite[uint64]{min: 0, max: math.MaxUint64})
	suite.Run(t, &BinaryArithmeticSuite[float32]{min: -math.MaxFloat32, max: math.MaxFloat32})
	suite.Run(t, &BinaryArithmeticSuite[float64]{min: -math.MaxFloat64, max: math.MaxFloat64})
	suite.Run(t, new(Float16BinaryFuncTestSuite))
	suite.Run(t, new(DecimalBinaryArithmeticSuite))
}

func TestBinaryArithmeticDispatchBest(t *testing.T) {
	for _, name := range []string{"add", "sub"} {
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
	suite.Suite

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

type DecimalBinaryArithmeticSuite struct {
	DecimalArithmeticSuite
}

func (ds *DecimalBinaryArithmeticSuite) TestDispatchBest() {
	// decimal, floating point
	for _, fn := range []string{"add", "sub"} {
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix

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
		}
	}

	// decimal, decimal => decimal
	// decimal, integer => decimal
	for _, fn := range []string{"add", "sub"} {
		for _, suffix := range []string{"", "_unchecked"} {
			fn += suffix

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
		}
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
			"-9876549999.641975555509876543212",
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

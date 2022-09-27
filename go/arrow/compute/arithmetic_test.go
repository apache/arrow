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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

type binaryFunc = func(context.Context, compute.ArithmeticOptions, compute.Datum, compute.Datum) (compute.Datum, error)

type BinaryArithmeticSuite[T exec.NumericTypes] struct {
	suite.Suite

	mem  *memory.CheckedAllocator
	opts compute.ArithmeticOptions
	ctx  context.Context
}

func (BinaryArithmeticSuite[T]) DataType() arrow.DataType {
	return exec.GetDataType[T]()
}

func (b *BinaryArithmeticSuite[T]) SetupTest() {
	b.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	b.opts.CheckOverflow = false
	b.ctx = compute.WithAllocator(context.TODO(), b.mem)
}

func (b *BinaryArithmeticSuite[T]) TearDownTest() {
	b.mem.AssertSize(b.T(), 0)
}

func (b *BinaryArithmeticSuite[T]) makeNullScalar() scalar.Scalar {
	return scalar.MakeNullScalar(b.DataType())
}

func (b *BinaryArithmeticSuite[T]) makeScalar(val T) scalar.Scalar {
	return scalar.MakeScalar(val)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScalars(fn binaryFunc, lhs, rhs T, expected T) {
	left, right := b.makeScalar(lhs), b.makeScalar(rhs)
	exp := b.makeScalar(expected)

	actual, err := fn(b.ctx, b.opts, &compute.ScalarDatum{Value: left}, &compute.ScalarDatum{Value: right})
	b.NoError(err)
	sc := actual.(*compute.ScalarDatum).Value

	b.Truef(scalar.Equals(exp, sc), "expected: %s\ngot: %s", exp, sc)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScArr(fn binaryFunc, lhs T, rhs, expected string) {
	left := b.makeScalar(lhs)
	b.assertBinopScalarArr(fn, left, rhs, expected)
}

func (b *BinaryArithmeticSuite[T]) assertBinopScalarArr(fn binaryFunc, lhs scalar.Scalar, rhs, expected string) {
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs))
	defer right.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	actual, err := fn(b.ctx, b.opts, &compute.ScalarDatum{Value: lhs}, &compute.ArrayDatum{Value: right.Data()})
	b.NoError(err)
	defer actual.Release()
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: exp.Data()}, actual)
}

func (b *BinaryArithmeticSuite[T]) assertBinopArrSc(fn binaryFunc, lhs string, rhs T, expected string) {
	right := b.makeScalar(rhs)
	b.assertBinopArrScalar(fn, lhs, right, expected)
}

func (b *BinaryArithmeticSuite[T]) assertBinopArrScalar(fn binaryFunc, lhs string, rhs scalar.Scalar, expected string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs))
	defer left.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	actual, err := fn(b.ctx, b.opts, &compute.ArrayDatum{Value: left.Data()}, &compute.ScalarDatum{Value: rhs})
	b.NoError(err)
	defer actual.Release()
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: exp.Data()}, actual)
}

func (b *BinaryArithmeticSuite[T]) assertBinopArrays(fn binaryFunc, lhs, rhs, expected string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs))
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs))
	defer right.Release()
	exp, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(expected))
	defer exp.Release()

	b.assertBinop(fn, left, right, exp)
}

func (b *BinaryArithmeticSuite[T]) assertBinop(fn binaryFunc, left, right, expected arrow.Array) {
	actual, err := fn(b.ctx, b.opts, &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()})
	b.Require().NoError(err)
	defer actual.Release()
	assertDatumsEqual(b.T(), &compute.ArrayDatum{Value: expected.Data()}, actual)

	// also check (Scalar, Scalar) operations
	for i := 0; i < expected.Len(); i++ {
		s, err := scalar.GetScalar(expected, i)
		b.Require().NoError(err)
		lhs, _ := scalar.GetScalar(left, i)
		rhs, _ := scalar.GetScalar(right, i)

		actual, err := fn(b.ctx, b.opts, &compute.ScalarDatum{Value: lhs}, &compute.ScalarDatum{Value: rhs})
		b.NoError(err)
		b.Truef(scalar.Equals(s, actual.(*compute.ScalarDatum).Value), "expected: %s\ngot: %s", s, actual)
	}
}

func (b *BinaryArithmeticSuite[T]) setOverflowCheck(value bool) {
	b.opts.CheckOverflow = value
}

func (b *BinaryArithmeticSuite[T]) assertBinopErr(fn binaryFunc, lhs, rhs, expectedMsg string) {
	left, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(lhs))
	defer left.Release()
	right, _, _ := array.FromJSON(b.mem, b.DataType(), strings.NewReader(rhs))
	defer right.Release()

	_, err := fn(b.ctx, b.opts, &compute.ArrayDatum{left.Data()}, &compute.ArrayDatum{Value: right.Data()})
	b.ErrorIs(err, arrow.ErrInvalid)
	b.ErrorContains(err, expectedMsg)
}

func (b *BinaryArithmeticSuite[T]) TestAdd() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("overflow=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinopArrays(compute.Add, `[]`, `[]`, `[]`)
				b.assertBinopArrays(compute.Add, `[3, 2, 6]`, `[1, 0, 2]`, `[4, 2, 8]`)
				// nulls on one side
				b.assertBinopArrays(compute.Add, `[null, 1, null]`, `[3, 4, 5]`, `[null, 5, null]`)
				b.assertBinopArrays(compute.Add, `[3, 4, 5]`, `[null, 1, null]`, `[null, 5, null]`)
				// nulls on both sides
				b.assertBinopArrays(compute.Add, `[null, 1, 2]`, `[3, 4, null]`, `[null, 5, null]`)
				// all nulls
				b.assertBinopArrays(compute.Add, `[null]`, `[null]`, `[null]`)

				// scalar on the left
				b.assertBinopScArr(compute.Add, 3, `[1, 2]`, `[4, 5]`)
				b.assertBinopScArr(compute.Add, 3, `[null, 2]`, `[null, 5]`)
				b.assertBinopScalarArr(compute.Add, b.makeNullScalar(), `[1, 2]`, `[null, null]`)
				b.assertBinopScalarArr(compute.Add, b.makeNullScalar(), `[null, 2]`, `[null, null]`)
				// scalar on the right
				b.assertBinopArrSc(compute.Add, `[1, 2]`, 3, `[4, 5]`)
				b.assertBinopArrSc(compute.Add, `[null, 2]`, 3, `[null, 5]`)
				b.assertBinopArrScalar(compute.Add, `[1, 2]`, b.makeNullScalar(), `[null, null]`)
				b.assertBinopArrScalar(compute.Add, `[null, 2]`, b.makeNullScalar(), `[null, null]`)
			})
		}
	})
}

func (b *BinaryArithmeticSuite[T]) TestSub() {
	b.Run(b.DataType().String(), func() {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("overflow=%t", overflow), func() {
				b.setOverflowCheck(overflow)

				b.assertBinopArrays(compute.Subtract, `[]`, `[]`, `[]`)
				b.assertBinopArrays(compute.Subtract, `[3, 2, 6]`, `[1, 0, 2]`, `[2, 2, 4]`)
				// nulls on one side
				b.assertBinopArrays(compute.Subtract, `[null, 4, null]`, `[2, 1, 0]`, `[null, 3, null]`)
				b.assertBinopArrays(compute.Subtract, `[3, 4, 5]`, `[null, 1, null]`, `[null, 3, null]`)
				// nulls on both sides
				b.assertBinopArrays(compute.Subtract, `[null, 4, 3]`, `[2, 1, null]`, `[null, 3, null]`)
				// all nulls
				b.assertBinopArrays(compute.Subtract, `[null]`, `[null]`, `[null]`)

				// scalar on the left
				b.assertBinopScArr(compute.Subtract, 3, `[1, 2]`, `[2, 1]`)
				b.assertBinopScArr(compute.Subtract, 3, `[null, 2]`, `[null, 1]`)
				b.assertBinopScalarArr(compute.Subtract, b.makeNullScalar(), `[1, 2]`, `[null, null]`)
				b.assertBinopScalarArr(compute.Subtract, b.makeNullScalar(), `[null, 2]`, `[null, null]`)
				// scalar on the right
				b.assertBinopArrSc(compute.Subtract, `[4, 5]`, 3, `[1, 2]`)
				b.assertBinopArrSc(compute.Subtract, `[null, 5]`, 3, `[null, 2]`)
				b.assertBinopArrScalar(compute.Subtract, `[1, 2]`, b.makeNullScalar(), `[null, null]`)
				b.assertBinopArrScalar(compute.Subtract, `[null, 2]`, b.makeNullScalar(), `[null, null]`)
			})
		}
	})
}

func TestBinaryArithmetic(t *testing.T) {
	suite.Run(t, new(BinaryArithmeticSuite[int8]))
	suite.Run(t, new(BinaryArithmeticSuite[uint8]))
	suite.Run(t, new(BinaryArithmeticSuite[int16]))
	suite.Run(t, new(BinaryArithmeticSuite[uint16]))
	suite.Run(t, new(BinaryArithmeticSuite[int32]))
	suite.Run(t, new(BinaryArithmeticSuite[uint32]))
	suite.Run(t, new(BinaryArithmeticSuite[int64]))
	suite.Run(t, new(BinaryArithmeticSuite[uint64]))
	suite.Run(t, new(BinaryArithmeticSuite[float32]))
	suite.Run(t, new(BinaryArithmeticSuite[float64]))
}

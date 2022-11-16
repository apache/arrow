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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/bitutil"
	"github.com/apache/arrow/go/v11/arrow/compute"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/arrow/scalar"
	"github.com/stretchr/testify/suite"
)

type CompareSuite struct {
	BinaryFuncTestSuite
}

func (c *CompareSuite) validateCompareDatum(op kernels.CompareOperator, lhs, rhs, expected compute.Datum) {
	result, err := compute.CallFunction(c.ctx, op.String(), nil, lhs, rhs)
	c.Require().NoError(err)
	defer result.Release()

	assertDatumsEqual(c.T(), expected, result)
}

func (c *CompareSuite) validateCompare(op kernels.CompareOperator, dt arrow.DataType, lhsStr, rhsStr, expStr string) {
	lhs, _, err := array.FromJSON(c.mem, dt, strings.NewReader(lhsStr), array.WithUseNumber())
	c.Require().NoError(err)
	rhs, _, err := array.FromJSON(c.mem, dt, strings.NewReader(rhsStr), array.WithUseNumber())
	c.Require().NoError(err)
	exp, _, err := array.FromJSON(c.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(expStr), array.WithUseNumber())
	c.Require().NoError(err)
	defer func() {
		lhs.Release()
		rhs.Release()
		exp.Release()
	}()
	c.validateCompareDatum(op, &compute.ArrayDatum{lhs.Data()}, &compute.ArrayDatum{rhs.Data()}, &compute.ArrayDatum{exp.Data()})
}

func (c *CompareSuite) validateCompareArrScalar(op kernels.CompareOperator, dt arrow.DataType, lhsStr string, rhs compute.Datum, expStr string) {
	lhs, _, err := array.FromJSON(c.mem, dt, strings.NewReader(lhsStr), array.WithUseNumber())
	c.Require().NoError(err)
	exp, _, err := array.FromJSON(c.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(expStr), array.WithUseNumber())
	c.Require().NoError(err)
	defer func() {
		lhs.Release()
		exp.Release()
	}()
	c.validateCompareDatum(op, &compute.ArrayDatum{lhs.Data()}, rhs, &compute.ArrayDatum{exp.Data()})
}

func (c *CompareSuite) validateCompareScalarArr(op kernels.CompareOperator, dt arrow.DataType, lhs compute.Datum, rhsStr string, expStr string) {
	rhs, _, err := array.FromJSON(c.mem, dt, strings.NewReader(rhsStr), array.WithUseNumber())
	c.Require().NoError(err)
	exp, _, err := array.FromJSON(c.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(expStr), array.WithUseNumber())
	c.Require().NoError(err)
	defer func() {
		rhs.Release()
		exp.Release()
	}()
	c.validateCompareDatum(op, lhs, &compute.ArrayDatum{rhs.Data()}, &compute.ArrayDatum{exp.Data()})
}

func slowCompare[T exec.NumericTypes | string](op kernels.CompareOperator, lhs, rhs T) bool {
	switch op {
	case kernels.CmpEQ:
		return lhs == rhs
	case kernels.CmpNE:
		return lhs != rhs
	case kernels.CmpLT:
		return lhs < rhs
	case kernels.CmpLE:
		return lhs <= rhs
	case kernels.CmpGT:
		return lhs > rhs
	case kernels.CmpGE:
		return lhs >= rhs
	default:
		return false
	}
}

func simpleScalarArrayCompare[T exec.NumericTypes](mem memory.Allocator, op kernels.CompareOperator, lhs, rhs compute.Datum) compute.Datum {
	var (
		swap  = lhs.Kind() == compute.KindArray
		span  exec.ArraySpan
		itr   exec.ArrayIter[T]
		value T
	)

	if swap {
		span.SetMembers(lhs.(*compute.ArrayDatum).Value)
		itr = exec.NewPrimitiveIter[T](&span)
		value = kernels.UnboxScalar[T](rhs.(*compute.ScalarDatum).Value.(scalar.PrimitiveScalar))
	} else {
		span.SetMembers(rhs.(*compute.ArrayDatum).Value)
		itr = exec.NewPrimitiveIter[T](&span)
		value = kernels.UnboxScalar[T](lhs.(*compute.ScalarDatum).Value.(scalar.PrimitiveScalar))
	}

	bitmap := make([]bool, span.Len)
	for i := 0; i < int(span.Len); i++ {
		if swap {
			bitmap[i] = slowCompare(op, itr.Next(), value)
		} else {
			bitmap[i] = slowCompare(op, value, itr.Next())
		}
	}

	var result arrow.Array
	if span.Nulls == 0 {
		result = exec.ArrayFromSlice(mem, bitmap)
	} else {
		nullBitmap := make([]bool, span.Len)
		rdr := bitutil.NewBitmapReader(span.Buffers[0].Buf, int(span.Offset), int(span.Len))
		for i := 0; i < int(span.Len); i++ {
			nullBitmap[i] = rdr.Set()
			rdr.Next()
		}
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(bitmap, nullBitmap)
		result = bldr.NewArray()
	}

	defer result.Release()
	return compute.NewDatum(result)
}

func simpleScalarArrayCompareString(mem memory.Allocator, op kernels.CompareOperator, lhs, rhs compute.Datum) compute.Datum {
	var (
		swap  = lhs.Kind() == compute.KindArray
		value string
		arr   *array.String
	)

	if swap {
		arr = lhs.(*compute.ArrayDatum).MakeArray().(*array.String)
		defer arr.Release()
		value = string(rhs.(*compute.ScalarDatum).Value.(*scalar.String).Data())
	} else {
		arr = rhs.(*compute.ArrayDatum).MakeArray().(*array.String)
		defer arr.Release()
		value = string(lhs.(*compute.ScalarDatum).Value.(*scalar.String).Data())
	}

	bitmap := make([]bool, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if swap {
			bitmap[i] = slowCompare(op, arr.Value(i), value)
		} else {
			bitmap[i] = slowCompare(op, value, arr.Value(i))
		}
	}

	var result arrow.Array
	if arr.NullN() == 0 {
		result = exec.ArrayFromSlice(mem, bitmap)
	} else {
		nullBitmap := make([]bool, arr.Len())
		rdr := bitutil.NewBitmapReader(arr.NullBitmapBytes(), arr.Offset(), arr.Len())
		for i := 0; i < arr.Len(); i++ {
			nullBitmap[i] = rdr.Set()
			rdr.Next()
		}
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(bitmap, nullBitmap)
		result = bldr.NewArray()
	}

	defer result.Release()
	return compute.NewDatum(result)
}

func nullBitmapFromArrays(lhs, rhs arrow.Array) []bool {
	nullBitmap := make([]bool, lhs.Len())

	left := func(i int) bool {
		if lhs.NullN() == 0 {
			return true
		}
		return lhs.IsValid(i)
	}

	right := func(i int) bool {
		if rhs.NullN() == 0 {
			return true
		}
		return rhs.IsValid(i)
	}

	for i := 0; i < lhs.Len(); i++ {
		nullBitmap[i] = left(i) && right(i)
	}
	return nullBitmap
}

type valuer[T any] interface {
	Value(int) T
}

func simpleArrArrCompare[T exec.NumericTypes | string](mem memory.Allocator, op kernels.CompareOperator, lhs, rhs compute.Datum) compute.Datum {
	var (
		lArr   = lhs.(*compute.ArrayDatum).MakeArray()
		rArr   = lhs.(*compute.ArrayDatum).MakeArray()
		length = lArr.Len()
		bitmap = make([]bool, length)

		lvals = lArr.(valuer[T])
		rvals = rArr.(valuer[T])
	)
	defer lArr.Release()
	defer rArr.Release()

	for i := 0; i < length; i++ {
		bitmap[i] = slowCompare(op, lvals.Value(i), rvals.Value(i))
	}

	var result arrow.Array
	if lArr.NullN() == 0 && rArr.NullN() == 0 {
		result = exec.ArrayFromSlice(mem, bitmap)
	} else {
		nullBitmap := nullBitmapFromArrays(lArr, rArr)
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(bitmap, nullBitmap)
		result = bldr.NewArray()
	}

	defer result.Release()
	return compute.NewDatum(result)
}

type NumericCompareSuite[T exec.NumericTypes] struct {
	CompareSuite
}

func (n *NumericCompareSuite[T]) validateCompareComputed(op kernels.CompareOperator, lhs, rhs compute.Datum) {
	var expected compute.Datum

	hasScalar := lhs.Kind() == compute.KindScalar || rhs.Kind() == compute.KindScalar
	if hasScalar {
		expected = simpleScalarArrayCompare[T](n.mem, op, lhs, rhs)
	} else {
		expected = simpleArrArrCompare[T](n.mem, op, lhs, rhs)
	}

	defer expected.Release()
	n.CompareSuite.validateCompareDatum(op, lhs, rhs, expected)
}

func (n *NumericCompareSuite[T]) TestSimpleCompareArrayScalar() {
	dt := exec.GetDataType[T]()
	one := compute.NewDatum(scalar.MakeScalar(T(1)))

	n.Run(dt.String(), func() {
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[]`, one, `[]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[null]`, one, `[null]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[0, 0, 1, 1, 2, 2]`, one,
			`[false, false, true, true, false, false]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[0, 1, 2, 3, 4, 5]`, one,
			`[false, true, false, false, false, false]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[5, 4, 3, 2, 1, 0]`, one,
			`[false, false, false, false, true, false]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[null, 0, 1, 1]`, one,
			`[null, false, true, true]`)

		n.validateCompareArrScalar(kernels.CmpNE, dt, `[]`, one, `[]`)
		n.validateCompareArrScalar(kernels.CmpNE, dt, `[null]`, one, `[null]`)
		n.validateCompareArrScalar(kernels.CmpNE, dt, `[0, 0, 1, 1, 2, 2]`, one,
			`[true, true, false, false, true, true]`)
		n.validateCompareArrScalar(kernels.CmpNE, dt, `[0, 1, 2, 3, 4, 5]`, one,
			`[true, false, true, true, true, true]`)
		n.validateCompareArrScalar(kernels.CmpNE, dt, `[5, 4, 3, 2, 1, 0]`, one,
			`[true, true, true, true, false, true]`)
		n.validateCompareArrScalar(kernels.CmpNE, dt, `[null, 0, 1, 1]`, one,
			`[null, true, false, false]`)
	})
}

func (n *NumericCompareSuite[T]) TestSimpleCompareScalarArray() {
	dt := exec.GetDataType[T]()
	one := compute.NewDatum(scalar.MakeScalar(T(1)))

	n.Run(dt.String(), func() {
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[]`, `[]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[null]`, `[null]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[0, 0, 1, 1, 2, 2]`,
			`[false, false, true, true, false, false]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[0, 1, 2, 3, 4, 5]`,
			`[false, true, false, false, false, false]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[5, 4, 3, 2, 1, 0]`,
			`[false, false, false, false, true, false]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, one, `[null, 0, 1, 1]`,
			`[null, false, true, true]`)

		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[]`, `[]`)
		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[null]`, `[null]`)
		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[0, 0, 1, 1, 2, 2]`,
			`[true, true, false, false, true, true]`)
		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[0, 1, 2, 3, 4, 5]`,
			`[true, false, true, true, true, true]`)
		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[5, 4, 3, 2, 1, 0]`,
			`[true, true, true, true, false, true]`)
		n.validateCompareScalarArr(kernels.CmpNE, dt, one, `[null, 0, 1, 1]`,
			`[null, true, false, false]`)
	})
}

func (n *NumericCompareSuite[T]) TestNullScalar() {
	dt := exec.GetDataType[T]()
	null := compute.NewDatum(scalar.MakeNullScalar(dt))

	n.Run(dt.String(), func() {
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[]`, null, `[]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, null, `[]`, `[]`)
		n.validateCompareArrScalar(kernels.CmpEQ, dt, `[null]`, null, `[null]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, null, `[null]`, `[null]`)
		n.validateCompareScalarArr(kernels.CmpEQ, dt, null, `[1, 2, 3]`, `[null, null, null]`)
	})
}

func (n *NumericCompareSuite[T]) TestSimpleCompareArrArr() {
	dt := exec.GetDataType[T]()

	n.Run(dt.String(), func() {
		n.validateCompare(kernels.CmpEQ, dt, `[]`, `[]`, `[]`)
		n.validateCompare(kernels.CmpEQ, dt, `[null]`, `[null]`, `[null]`)
		n.validateCompare(kernels.CmpEQ, dt, `[1]`, `[1]`, `[true]`)
		n.validateCompare(kernels.CmpEQ, dt, `[1]`, `[2]`, `[false]`)
		n.validateCompare(kernels.CmpEQ, dt, `[null]`, `[1]`, `[null]`)
		n.validateCompare(kernels.CmpEQ, dt, `[1]`, `[null]`, `[null]`)
	})
}

func TestComparisons(t *testing.T) {
	suite.Run(t, new(NumericCompareSuite[int8]))
	suite.Run(t, new(NumericCompareSuite[int16]))
	suite.Run(t, new(NumericCompareSuite[int32]))
	suite.Run(t, new(NumericCompareSuite[int64]))
	suite.Run(t, new(NumericCompareSuite[uint8]))
	suite.Run(t, new(NumericCompareSuite[uint16]))
	suite.Run(t, new(NumericCompareSuite[uint32]))
	suite.Run(t, new(NumericCompareSuite[uint64]))
	suite.Run(t, new(NumericCompareSuite[float32]))
	suite.Run(t, new(NumericCompareSuite[float64]))
}

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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/bitutil"
	"github.com/apache/arrow/go/v11/arrow/compute"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v11/arrow/internal/testing/gen"
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
		rArr   = rhs.(*compute.ArrayDatum).MakeArray()
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

type CompareTimestampSuite struct {
	CompareSuite
}

func (c *CompareTimestampSuite) TestBasics() {
	var (
		example1JSON = `["1970-01-01", "2000-02-29", "1900-02-28"]`
		example2JSON = `["1970-01-02", "2000-02-01", "1900-02-28"]`
	)

	checkCase := func(dt arrow.DataType, op kernels.CompareOperator, expected string) {
		c.validateCompare(op, dt, example1JSON, example2JSON, expected)
	}

	seconds := arrow.FixedWidthTypes.Timestamp_s
	millis := arrow.FixedWidthTypes.Timestamp_ms
	micro := arrow.FixedWidthTypes.Timestamp_us
	nano := arrow.FixedWidthTypes.Timestamp_ns

	checkCase(seconds, kernels.CmpEQ, `[false, false, true]`)
	checkCase(millis, kernels.CmpEQ, `[false, false, true]`)
	checkCase(micro, kernels.CmpEQ, `[false, false, true]`)
	checkCase(nano, kernels.CmpEQ, `[false, false, true]`)

	checkCase(seconds, kernels.CmpNE, `[true, true, false]`)
	checkCase(millis, kernels.CmpNE, `[true, true, false]`)
	checkCase(micro, kernels.CmpNE, `[true, true, false]`)
	checkCase(nano, kernels.CmpNE, `[true, true, false]`)

	secondsUTC := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "utc"}
	checkCase(secondsUTC, kernels.CmpEQ, `[false, false, true]`)
}

func (c *CompareTimestampSuite) TestDiffParams() {
	cases := []struct {
		fn  string
		exp string
	}{
		{"equal", `[false, false, true]`},
		{"not_equal", `[true, true, false]`},
	}

	const lhsJSON = `["1970-01-01", "2000-02-29", "1900-02-28"]`
	const rhsJSON = `["1970-01-02", "2000-02-01", "1900-02-28"]`

	for _, op := range cases {
		c.Run(op.fn, func() {
			exp := c.getArr(arrow.FixedWidthTypes.Boolean, op.exp)
			defer exp.Release()

			expected := &compute.ArrayDatum{exp.Data()}
			c.Run("diff units", func() {
				lhs := c.getArr(&arrow.TimestampType{Unit: arrow.Second}, lhsJSON)
				defer lhs.Release()
				rhs := c.getArr(&arrow.TimestampType{Unit: arrow.Millisecond}, rhsJSON)
				defer rhs.Release()

				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhs.Data()}, &compute.ArrayDatum{rhs.Data()}, expected, nil)
			})
			c.Run("diff time zones", func() {
				lhs := c.getArr(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/New_York"}, lhsJSON)
				defer lhs.Release()
				rhs := c.getArr(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/Phoenix"}, rhsJSON)
				defer rhs.Release()

				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhs.Data()}, &compute.ArrayDatum{rhs.Data()}, expected, nil)
			})
			c.Run("native to zoned", func() {
				lhs := c.getArr(&arrow.TimestampType{Unit: arrow.Second}, lhsJSON)
				defer lhs.Release()
				rhs := c.getArr(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/Phoenix"}, rhsJSON)
				defer rhs.Release()

				_, err := compute.CallFunction(c.ctx, op.fn, nil, &compute.ArrayDatum{lhs.Data()}, &compute.ArrayDatum{rhs.Data()})
				c.ErrorIs(err, arrow.ErrInvalid)
				c.ErrorContains(err, "cannot compare timestamp with timezone to timestamp without timezone")

				lhs = c.getArr(&arrow.TimestampType{Unit: arrow.Second, TimeZone: "America/New_York"}, lhsJSON)
				defer lhs.Release()
				rhs = c.getArr(&arrow.TimestampType{Unit: arrow.Second}, rhsJSON)
				defer rhs.Release()

				_, err = compute.CallFunction(c.ctx, op.fn, nil, &compute.ArrayDatum{lhs.Data()}, &compute.ArrayDatum{rhs.Data()})
				c.ErrorIs(err, arrow.ErrInvalid)
				c.ErrorContains(err, "cannot compare timestamp with timezone to timestamp without timezone")
			})
		})
	}
}

func (c *CompareTimestampSuite) TestScalarArray() {
	const scalarStr = "1970-01-02"
	const arrayJSON = `["1970-01-02", "2000-02-01", null, "1900-02-28"]`

	checkArrCase := func(scType, arrayType arrow.DataType, op kernels.CompareOperator, expectedJSON, flipExpectedJSON string) {
		scalarSide, err := scalar.MakeScalarParam(scalarStr, scType)
		c.Require().NoError(err)
		arraySide := c.getArr(arrayType, arrayJSON)
		defer arraySide.Release()

		expected := c.getArr(arrow.FixedWidthTypes.Boolean, expectedJSON)
		defer expected.Release()
		flipExpected := c.getArr(arrow.FixedWidthTypes.Boolean, flipExpectedJSON)
		defer flipExpected.Release()

		cases := []struct{ side1, side2, expected compute.Datum }{
			{compute.NewDatum(scalarSide), &compute.ArrayDatum{arraySide.Data()}, &compute.ArrayDatum{expected.Data()}},
			{&compute.ArrayDatum{arraySide.Data()}, compute.NewDatum(scalarSide), &compute.ArrayDatum{flipExpected.Data()}},
		}

		for _, arrCase := range cases {
			lhs, rhs := arrCase.side1, arrCase.side2
			if arrow.TypeEqual(scType, arrayType) {
				c.validateCompareDatum(op, lhs, rhs, arrCase.expected)
			} else {
				_, err := compute.CallFunction(c.ctx, op.String(), nil, lhs, rhs)
				c.ErrorIs(err, arrow.ErrInvalid)
				c.ErrorContains(err, "cannot compare timestamp with timezone to timestamp without timezone")
			}
		}
	}

	for _, unit := range arrow.TimeUnitValues {
		c.Run(unit.String(), func() {
			tests := []struct{ t0, t1 arrow.DataType }{
				{&arrow.TimestampType{Unit: unit}, &arrow.TimestampType{Unit: unit}},
				{&arrow.TimestampType{Unit: unit}, &arrow.TimestampType{Unit: unit, TimeZone: "utc"}},
				{&arrow.TimestampType{Unit: unit, TimeZone: "utc"}, &arrow.TimestampType{Unit: unit}},
				{&arrow.TimestampType{Unit: unit, TimeZone: "utc"}, &arrow.TimestampType{Unit: unit, TimeZone: "utc"}},
			}
			for _, tt := range tests {
				checkArrCase(tt.t0, tt.t1, kernels.CmpEQ, `[true, false, null, false]`, `[true, false, null, false]`)
				checkArrCase(tt.t0, tt.t1, kernels.CmpNE, `[false, true, null, true]`, `[false, true, null, true]`)
			}
		})
	}
}

type CompareDecimalSuite struct {
	CompareSuite
}

func (c *CompareDecimalSuite) TestArrayScalar() {
	cases := []struct{ fn, exp string }{
		{"equal", `[true, false, false, null]`},
		{"not_equal", `[false, true, true, null]`},
	}

	for _, id := range []arrow.Type{arrow.DECIMAL128, arrow.DECIMAL256} {
		c.Run(id.String(), func() {
			ty, _ := arrow.NewDecimalType(id, 3, 2)

			lhsArr := c.getArr(ty, `["1.23", "2.34", "-1.23", null]`)
			lhsFloatArr := c.getArr(arrow.PrimitiveTypes.Float64, `[1.23, 2.34, -1.23, null]`)
			lhsIntLikeArr := c.getArr(ty, `["1.00", "2.00", "-1.00", null]`)
			defer func() {
				lhsArr.Release()
				lhsFloatArr.Release()
				lhsIntLikeArr.Release()
			}()

			lhs := &compute.ArrayDatum{lhsArr.Data()}
			lhsFloat := &compute.ArrayDatum{lhsFloatArr.Data()}
			lhsIntLike := &compute.ArrayDatum{lhsIntLikeArr.Data()}

			rhs, _ := scalar.MakeScalarParam("1.23", ty)
			rhsFloat := scalar.MakeScalar(float64(1.23))
			rhsInt := scalar.MakeScalar(int64(1))
			for _, tc := range cases {
				c.Run(tc.fn, func() {
					exp := c.getArr(arrow.FixedWidthTypes.Boolean, tc.exp)
					defer exp.Release()
					expected := &compute.ArrayDatum{exp.Data()}

					checkScalarBinary(c.T(), tc.fn, lhs, compute.NewDatum(rhs), expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhsFloat, compute.NewDatum(rhs), expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhs, compute.NewDatum(rhsFloat), expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhsIntLike, compute.NewDatum(rhsInt), expected, nil)
				})
			}
		})
	}
}

func (c *CompareDecimalSuite) TestScalarArray() {
	cases := []struct{ fn, exp string }{
		{"equal", `[true, false, false, null]`},
		{"not_equal", `[false, true, true, null]`},
	}

	for _, id := range []arrow.Type{arrow.DECIMAL128, arrow.DECIMAL256} {
		c.Run(id.String(), func() {
			ty, _ := arrow.NewDecimalType(id, 3, 2)

			rhsArr := c.getArr(ty, `["1.23", "2.34", "-1.23", null]`)
			rhsFloatArr := c.getArr(arrow.PrimitiveTypes.Float64, `[1.23, 2.34, -1.23, null]`)
			rhsIntLikeArr := c.getArr(ty, `["1.00", "2.00", "-1.00", null]`)
			defer func() {
				rhsArr.Release()
				rhsFloatArr.Release()
				rhsIntLikeArr.Release()
			}()

			rhs := &compute.ArrayDatum{rhsArr.Data()}
			rhsFloat := &compute.ArrayDatum{rhsFloatArr.Data()}
			rhsIntLike := &compute.ArrayDatum{rhsIntLikeArr.Data()}

			lhs, _ := scalar.MakeScalarParam("1.23", ty)
			lhsFloat := scalar.MakeScalar(float64(1.23))
			lhsInt := scalar.MakeScalar(int64(1))
			for _, tc := range cases {
				c.Run(tc.fn, func() {
					exp := c.getArr(arrow.FixedWidthTypes.Boolean, tc.exp)
					defer exp.Release()
					expected := &compute.ArrayDatum{exp.Data()}

					checkScalarBinary(c.T(), tc.fn, compute.NewDatum(lhs), rhs, expected, nil)
					checkScalarBinary(c.T(), tc.fn, compute.NewDatum(lhs), rhsFloat, expected, nil)
					checkScalarBinary(c.T(), tc.fn, compute.NewDatum(lhsFloat), rhs, expected, nil)
					checkScalarBinary(c.T(), tc.fn, compute.NewDatum(lhsInt), rhsIntLike, expected, nil)
				})
			}
		})
	}
}

func (c *CompareDecimalSuite) TestArrayArray() {
	cases := []struct{ fn, exp string }{
		{"equal", `[true, false, false, true, false, false, null, null]`},
		{"not_equal", `[false, true, true, false, true, true, null, null]`},
	}

	for _, id := range []arrow.Type{arrow.DECIMAL128, arrow.DECIMAL256} {
		c.Run(id.String(), func() {
			ty, _ := arrow.NewDecimalType(id, 3, 2)

			lhsArr := c.getArr(ty, `["1.23", "1.23", "2.34", "-1.23", "-1.23", "1.23", "1.23", null]`)
			lhsFloatArr := c.getArr(arrow.PrimitiveTypes.Float64, `[1.23, 1.23, 2.34, -1.23, -1.23, 1.23, 1.23, null]`)
			lhsIntLikeArr := c.getArr(ty, `["1.00", "1.00", "2.00", "-1.00", "-1.00", "1.00", "1.00", null]`)
			defer func() {
				lhsArr.Release()
				lhsFloatArr.Release()
				lhsIntLikeArr.Release()
			}()

			lhs := &compute.ArrayDatum{lhsArr.Data()}
			lhsFloat := &compute.ArrayDatum{lhsFloatArr.Data()}
			lhsIntLike := &compute.ArrayDatum{lhsIntLikeArr.Data()}

			rhsArr := c.getArr(ty, `["1.23", "2.34", "1.23", "-1.23", "1.23", "-1.23", null, "1.23"]`)
			rhsFloatArr := c.getArr(arrow.PrimitiveTypes.Float64, `[1.23, 2.34, 1.23, -1.23, 1.23, -1.23, null, 1.23]`)
			rhsIntArr := c.getArr(arrow.PrimitiveTypes.Int64, `[1, 2, 1, -1, 1, -1, null, 1]`)
			defer func() {
				rhsArr.Release()
				rhsFloatArr.Release()
				rhsIntArr.Release()
			}()

			rhs := &compute.ArrayDatum{rhsArr.Data()}
			rhsFloat := &compute.ArrayDatum{rhsFloatArr.Data()}
			rhsInt := &compute.ArrayDatum{rhsIntArr.Data()}

			empty := c.getArr(ty, `[]`)
			emptyExp := c.getArr(arrow.FixedWidthTypes.Boolean, `[]`)
			null := c.getArr(ty, `[null]`)
			nullExp := c.getArr(arrow.FixedWidthTypes.Boolean, `[null]`)
			defer func() {
				empty.Release()
				emptyExp.Release()
				null.Release()
				nullExp.Release()
			}()

			for _, tc := range cases {
				c.Run(tc.fn, func() {
					exp := c.getArr(arrow.FixedWidthTypes.Boolean, tc.exp)
					defer exp.Release()
					expected := &compute.ArrayDatum{exp.Data()}

					checkScalarBinary(c.T(), tc.fn, &compute.ArrayDatum{empty.Data()},
						&compute.ArrayDatum{empty.Data()}, &compute.ArrayDatum{emptyExp.Data()}, nil)
					checkScalarBinary(c.T(), tc.fn, &compute.ArrayDatum{null.Data()},
						&compute.ArrayDatum{null.Data()}, &compute.ArrayDatum{nullExp.Data()}, nil)
					checkScalarBinary(c.T(), tc.fn, lhs, rhs, expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhsFloat, rhs, expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhs, rhsFloat, expected, nil)
					checkScalarBinary(c.T(), tc.fn, lhsIntLike, rhsInt, expected, nil)
				})
			}
		})
	}
}

func (c *CompareDecimalSuite) TestDiffParams() {
	cases := []struct{ fn, exp string }{
		{"equal", `[true, false, false, true, false, false]`},
		{"not_equal", `[false, true, true, false, true, true]`},
	}

	for _, id := range []arrow.Type{arrow.DECIMAL128, arrow.DECIMAL256} {
		c.Run(id.String(), func() {
			ty1, _ := arrow.NewDecimalType(id, 3, 2)
			ty2, _ := arrow.NewDecimalType(id, 4, 3)

			lhsArr := c.getArr(ty1, `["1.23", "1.23", "2.34", "-1.23", "-1.23", "1.23"]`)
			rhsArr := c.getArr(ty2, `["1.230", "2.340", "1.230", "-1.230", "1.230", "-1.230"]`)
			defer func() {
				lhsArr.Release()
				rhsArr.Release()
			}()

			lhs := &compute.ArrayDatum{lhsArr.Data()}
			rhs := &compute.ArrayDatum{rhsArr.Data()}

			for _, tc := range cases {
				c.Run(tc.fn, func() {
					exp := c.getArr(arrow.FixedWidthTypes.Boolean, tc.exp)
					defer exp.Release()
					expected := &compute.ArrayDatum{exp.Data()}

					checkScalarBinary(c.T(), tc.fn, lhs, rhs, expected, nil)
				})
			}
		})
	}
}

type CompareFixedSizeBinary struct {
	CompareSuite
}

type fsbCompareCase struct {
	lhsType, rhsType arrow.DataType
	lhs, rhs         string
	// index into cases[...].exp
	resultIdx int
}

func (c *CompareFixedSizeBinary) TestArrayScalar() {
	ty1 := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	ty2 := &arrow.FixedSizeBinaryType{ByteWidth: 1}

	cases := []struct {
		fn  string
		exp []string
	}{
		{"equal", []string{
			`[false, true, false, null]`,
			`[false, false, false, null]`,
			`[false, false, false, null]`}},
		{"not_equal", []string{
			`[true, false, true, null]`,
			`[true, true, true, null]`,
			`[true, true, true, null]`,
		}},
	}

	// base64 encoding
	const (
		valAba = `YWJh`
		valAbc = `YWJj`
		valAbd = `YWJk`
		valA   = `YQ`
		valB   = `Yg`
		valC   = `Yw`
	)

	const (
		lhs1bin = `["` + valAba + `","` + valAbc + `","` + valAbd + `", null]`
		lhs1    = `["aba", "abc", "abd", null]`
		rhs1    = "abc"
		lhs2bin = `["` + valA + `","` + valB + `","` + valC + `", null]`
		lhs2    = `["a", "b", "c", null]`
		rhs2    = "b"
	)

	types := []fsbCompareCase{
		{ty1, ty1, lhs1bin, rhs1, 0},
		{ty2, ty2, lhs2bin, rhs2, 0},
		{ty1, ty2, lhs1bin, rhs2, 1},
		{ty2, ty1, lhs2bin, rhs1, 2},
		{ty1, arrow.BinaryTypes.Binary, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.Binary, ty1, lhs1bin, rhs1, 0},
		{ty1, arrow.BinaryTypes.LargeBinary, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.LargeBinary, ty1, lhs1bin, rhs1, 0},
		{ty1, arrow.BinaryTypes.String, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.String, ty1, lhs1, rhs1, 0},
		{ty1, arrow.BinaryTypes.LargeString, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.LargeString, ty1, lhs1, rhs1, 0},
	}

	expNull := c.getArr(arrow.FixedWidthTypes.Boolean, `[null]`)
	defer expNull.Release()

	for _, op := range cases {
		c.Run(op.fn, func() {
			for _, tc := range types {
				lhs := c.getArr(tc.lhsType, tc.lhs)
				defer lhs.Release()
				rhs, _ := scalar.MakeScalarParam(tc.rhs, tc.rhsType)
				exp := c.getArr(arrow.FixedWidthTypes.Boolean, op.exp[tc.resultIdx])
				defer exp.Release()

				expected := &compute.ArrayDatum{exp.Data()}

				null := c.getArr(tc.lhsType, `[null]`)
				defer null.Release()
				scNull := scalar.MakeNullScalar(tc.rhsType)

				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{null.Data()}, compute.NewDatum(scNull),
					&compute.ArrayDatum{expNull.Data()}, nil)
				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhs.Data()},
					compute.NewDatum(rhs), expected, nil)
			}
		})
	}
}

func (c *CompareFixedSizeBinary) TestScalarArray() {
	ty1 := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	ty2 := &arrow.FixedSizeBinaryType{ByteWidth: 1}

	cases := []struct {
		fn  string
		exp []string
	}{
		{"equal", []string{
			`[false, true, false, null]`,
			`[false, false, false, null]`,
			`[false, false, false, null]`}},
		{"not_equal", []string{
			`[true, false, true, null]`,
			`[true, true, true, null]`,
			`[true, true, true, null]`,
		}},
	}

	// base64 encoding
	const (
		valAba = `YWJh`
		valAbc = `YWJj`
		valAbd = `YWJk`
		valA   = `YQ`
		valB   = `Yg`
		valC   = `Yw`
	)

	const (
		lhs1    = "abc"
		rhs1bin = `["` + valAba + `","` + valAbc + `","` + valAbd + `", null]`
		rhs1    = `["aba", "abc", "abd", null]`
		lhs2    = "b"
		rhs2bin = `["` + valA + `","` + valB + `","` + valC + `", null]`
		rhs2    = `["a", "b", "c", null]`
	)

	types := []fsbCompareCase{
		{ty1, ty1, lhs1, rhs1bin, 0},
		{ty2, ty2, lhs2, rhs2bin, 0},
		{ty1, ty2, lhs1, rhs2bin, 1},
		{ty2, ty1, lhs2, rhs1bin, 2},
		{ty1, arrow.BinaryTypes.Binary, lhs1, rhs1bin, 0},
		{arrow.BinaryTypes.Binary, ty1, lhs1, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.LargeBinary, lhs1, rhs1bin, 0},
		{arrow.BinaryTypes.LargeBinary, ty1, lhs1, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.String, lhs1, rhs1, 0},
		{arrow.BinaryTypes.String, ty1, lhs1, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.LargeString, lhs1, rhs1, 0},
		{arrow.BinaryTypes.LargeString, ty1, lhs1, rhs1bin, 0},
	}

	expNull := c.getArr(arrow.FixedWidthTypes.Boolean, `[null]`)
	defer expNull.Release()

	for _, op := range cases {
		c.Run(op.fn, func() {
			for _, tc := range types {
				lhs, _ := scalar.MakeScalarParam(tc.lhs, tc.lhsType)
				rhs := c.getArr(tc.rhsType, tc.rhs)
				defer rhs.Release()
				exp := c.getArr(arrow.FixedWidthTypes.Boolean, op.exp[tc.resultIdx])
				defer exp.Release()

				expected := &compute.ArrayDatum{exp.Data()}

				null := c.getArr(tc.rhsType, `[null]`)
				defer null.Release()
				scNull := scalar.MakeNullScalar(tc.lhsType)

				checkScalarBinary(c.T(), op.fn, compute.NewDatum(scNull), &compute.ArrayDatum{null.Data()},
					&compute.ArrayDatum{expNull.Data()}, nil)
				checkScalarBinary(c.T(), op.fn, compute.NewDatum(lhs),
					&compute.ArrayDatum{rhs.Data()}, expected, nil)
			}
		})
	}
}

func (c *CompareFixedSizeBinary) TestArrayArray() {
	ty1 := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	ty2 := &arrow.FixedSizeBinaryType{ByteWidth: 1}

	cases := []struct {
		fn  string
		exp []string
	}{
		{"equal", []string{
			`[true, false, false, null, null]`,
			`[true, false, false, null, null]`,
			`[true, false, false, null, null]`,
			`[true, false, false, null, null]`,
			`[false, false, false, null, null]`,
			`[false, false, false, null, null]`}},
		{"not_equal", []string{
			`[false, true, true, null, null]`,
			`[false, true, true, null, null]`,
			`[false, true, true, null, null]`,
			`[false, true, true, null, null]`,
			`[true, true, true, null, null]`,
			`[true, true, true, null, null]`}},
	}

	// base64 encoding
	const (
		valAba = `YWJh`
		valAbc = `YWJj`
		valAbd = `YWJk`
		valA   = `YQ`
		valB   = `Yg`
		valC   = `Yw`
		valD   = `ZA`
	)

	const (
		lhs1bin = `["` + valAbc + `","` + valAbc + `","` + valAbd + `", null, "` + valAbc + `"]`
		rhs1bin = `["` + valAbc + `","` + valAbd + `","` + valAbc + `","` + valAbc + `", null]`
		lhs1    = `["abc", "abc", "abd", null, "abc"]`
		rhs1    = `["abc", "abd", "abc", "abc", null]`
		lhs2    = `["a", "a", "d", null, "a"]`
		rhs2    = `["a", "d", "c", "a", null]`
		lhs2bin = `["` + valA + `","` + valA + `","` + valD + `", null, "` + valA + `"]`
		rhs2bin = `["` + valA + `","` + valD + `","` + valC + `","` + valA + `", null]`
	)

	types := []fsbCompareCase{
		{ty1, ty1, lhs1bin, rhs1bin, 0},
		{ty1, ty1, rhs1bin, lhs1bin, 1},
		{ty2, ty2, lhs2bin, rhs2bin, 2},
		{ty2, ty2, rhs2bin, lhs2bin, 3},
		{ty1, ty2, lhs1bin, rhs2bin, 4},
		{ty2, ty1, lhs2bin, rhs1bin, 5},
		{ty1, arrow.BinaryTypes.Binary, lhs1bin, rhs1bin, 0},
		{arrow.BinaryTypes.Binary, ty1, lhs1bin, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.LargeBinary, lhs1bin, rhs1bin, 0},
		{arrow.BinaryTypes.LargeBinary, ty1, lhs1bin, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.String, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.String, ty1, lhs1, rhs1bin, 0},
		{ty1, arrow.BinaryTypes.LargeString, lhs1bin, rhs1, 0},
		{arrow.BinaryTypes.LargeString, ty1, lhs1, rhs1bin, 0},
	}

	expEmpty := c.getArr(arrow.FixedWidthTypes.Boolean, `[]`)
	defer expEmpty.Release()
	expNull := c.getArr(arrow.FixedWidthTypes.Boolean, `[null]`)
	defer expNull.Release()

	for _, op := range cases {
		c.Run(op.fn, func() {
			for _, tc := range types {
				lhs := c.getArr(tc.lhsType, tc.lhs)
				defer lhs.Release()
				rhs := c.getArr(tc.rhsType, tc.rhs)
				defer rhs.Release()
				exp := c.getArr(arrow.FixedWidthTypes.Boolean, op.exp[tc.resultIdx])
				defer exp.Release()

				expected := &compute.ArrayDatum{exp.Data()}

				lhsEmpty := c.getArr(tc.lhsType, `[]`)
				defer lhsEmpty.Release()
				rhsEmpty := c.getArr(tc.rhsType, `[]`)
				defer rhsEmpty.Release()
				lhsNull := c.getArr(tc.lhsType, `[null]`)
				defer lhsNull.Release()
				rhsNull := c.getArr(tc.rhsType, `[null]`)
				defer rhsNull.Release()

				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhsEmpty.Data()}, &compute.ArrayDatum{rhsEmpty.Data()},
					&compute.ArrayDatum{expEmpty.Data()}, nil)
				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhsNull.Data()}, &compute.ArrayDatum{rhsNull.Data()},
					&compute.ArrayDatum{expNull.Data()}, nil)
				checkScalarBinary(c.T(), op.fn, &compute.ArrayDatum{lhs.Data()},
					&compute.ArrayDatum{rhs.Data()}, expected, nil)
			}
		})
	}
}

type CompareStringSuite struct {
	CompareSuite
}

func (c *CompareStringSuite) TestSimpleCompareArrayScalar() {
	one := compute.NewDatum(scalar.MakeScalar("one"))

	dt := arrow.BinaryTypes.String

	op := kernels.CmpEQ
	c.validateCompareArrScalar(op, dt, `[]`, one, `[]`)
	c.validateCompareArrScalar(op, dt, `[null]`, one, `[null]`)
	c.validateCompareArrScalar(op, dt, `["zero", "zero", "one", "one", "two", "two"]`, one,
		`[false, false, true, true, false, false]`)
	c.validateCompareArrScalar(op, dt, `["zero", "one", "two", "three", "four", "five"]`, one,
		`[false, true, false, false, false, false]`)
	c.validateCompareArrScalar(op, dt, `["five", "four", "three", "two", "one", "zero"]`, one,
		`[false, false, false, false, true, false]`)
	c.validateCompareArrScalar(op, dt, `[null, "zero", "one", "one"]`, one, `[null, false, true, true]`)

	na := compute.NewDatum(scalar.MakeNullScalar(dt))
	c.validateCompareArrScalar(op, dt, `[null, "zero", "one", "one"]`, na, `[null, null, null, null]`)
	c.validateCompareScalarArr(op, dt, na, `[null, "zero", "one", "one"]`, `[null, null, null, null]`)

	op = kernels.CmpNE
	c.validateCompareArrScalar(op, dt, `[]`, one, `[]`)
	c.validateCompareArrScalar(op, dt, `[null]`, one, `[null]`)
	c.validateCompareArrScalar(op, dt, `["zero", "zero", "one", "one", "two", "two"]`, one,
		`[true, true, false, false, true, true]`)
	c.validateCompareArrScalar(op, dt, `["zero", "one", "two", "three", "four", "five"]`, one,
		`[true, false, true, true, true, true]`)
	c.validateCompareArrScalar(op, dt, `["five", "four", "three", "two", "one", "zero"]`, one,
		`[true, true, true, true, false, true]`)
	c.validateCompareArrScalar(op, dt, `[null, "zero", "one", "one"]`, one, `[null, true, false, false]`)
}

func (c *CompareStringSuite) validateCompareComputed(op kernels.CompareOperator, lhs, rhs compute.Datum) {
	var expected compute.Datum

	hasScalar := lhs.Kind() == compute.KindScalar || rhs.Kind() == compute.KindScalar
	if hasScalar {
		expected = simpleScalarArrayCompareString(c.mem, op, lhs, rhs)
	} else {
		expected = simpleArrArrCompare[string](c.mem, op, lhs, rhs)
	}

	defer expected.Release()
	c.CompareSuite.validateCompareDatum(op, lhs, rhs, expected)
}

func (c *CompareStringSuite) TestRandomCompareArrayArray() {
	rng := gen.NewRandomArrayGenerator(0x5416447, c.mem)
	for i := 3; i < 5; i++ {
		c.Run(fmt.Sprintf("len=%d", 1<<i), func() {
			for _, nullProb := range []float64{0.0, 0.01, 0.1, 0.25, 0.5, 1.0} {
				c.Run(fmt.Sprintf("nullprob=%0.2f", nullProb), func() {
					for _, op := range []kernels.CompareOperator{kernels.CmpEQ, kernels.CmpNE} {
						c.Run(op.String(), func() {
							length := int64(1 << i)
							lhs := rng.String(length<<i, 0, 16, nullProb)
							defer lhs.Release()
							rhs := rng.String(length<<i, 0, 16, nullProb)
							defer rhs.Release()

							c.validateCompareComputed(op,
								&compute.ArrayDatum{lhs.Data()},
								&compute.ArrayDatum{rhs.Data()})
						})
					}
				})
			}
		})
	}
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
	suite.Run(t, new(CompareTimestampSuite))
	suite.Run(t, new(CompareDecimalSuite))
	suite.Run(t, new(CompareFixedSizeBinary))
	suite.Run(t, new(CompareStringSuite))
}

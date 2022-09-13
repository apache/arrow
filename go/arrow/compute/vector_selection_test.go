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
	"github.com/apache/arrow/go/v10/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v10/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const randomSeed = 0x0ff1ce

type FilterKernelTestSuite struct {
	suite.Suite

	mem                 *memory.CheckedAllocator
	dropOpts, emitNulls compute.FilterOptions
}

func (f *FilterKernelTestSuite) SetupSuite() {
	f.dropOpts.NullSelection = compute.SelectionDropNulls
	f.emitNulls.NullSelection = compute.SelectionEmitNulls
}

func (f *FilterKernelTestSuite) SetupTest() {
	f.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (f *FilterKernelTestSuite) TearDownTest() {
	f.mem.AssertSize(f.T(), 0)
}

func (f *FilterKernelTestSuite) getArr(dt arrow.DataType, str string) arrow.Array {
	arr, _, err := array.FromJSON(f.mem, dt, strings.NewReader(str), array.WithUseNumber())
	f.Require().NoError(err)
	return arr
}

func (f *FilterKernelTestSuite) doAssertFilter(values, filter, expected arrow.Array) {
	ctx := context.TODO()
	valDatum := compute.NewDatum(values)
	defer valDatum.Release()
	filterDatum := compute.NewDatum(filter)
	defer filterDatum.Release()

	f.Run("emit_null", func() {
		out, err := compute.Filter(ctx, valDatum, filterDatum, f.emitNulls)
		f.Require().NoError(err)
		defer out.Release()
		actual := out.(*compute.ArrayDatum).MakeArray()
		defer actual.Release()
		f.Truef(array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
	})

	// f.Run("drop", func() {
	// 	out, err := compute.Filter(ctx, valDatum, filterDatum, f.dropOpts)
	// 	f.NoError(err)
	// 	defer out.Release()
	// 	actual := out.(*compute.ArrayDatum).MakeArray()
	// 	defer actual.Release()
	// 	f.Truef(array.Equal(expected, actual), "expected: %s\ngot: %s", expected, actual)
	// })
}

func (f *FilterKernelTestSuite) assertFilter(values, filter, expected arrow.Array) {
	f.doAssertFilter(values, filter, expected)

	if values.DataType().ID() == arrow.DENSE_UNION {
		// concatenation of dense union not supported
		return
	}

	// check slicing: add(M=3) dummy values at the start and end of values
	// add N(=2) dummy values at the start and end of filter
	f.Run("sliced values and filter", func() {
		valuesFiller := array.MakeArrayOfNull(f.mem, values.DataType(), 3)
		defer valuesFiller.Release()
		filterFiller, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[true, false]`))
		defer filterFiller.Release()

		valuesSliced, err := array.Concatenate([]arrow.Array{valuesFiller, values, valuesFiller}, f.mem)
		f.Require().NoError(err)
		defer valuesSliced.Release()

		filterSliced, err := array.Concatenate([]arrow.Array{filterFiller, filter, filterFiller}, f.mem)
		f.Require().NoError(err)
		defer filterSliced.Release()

		valuesSliced = array.NewSlice(valuesSliced, 3, int64(3+values.Len()))
		filterSliced = array.NewSlice(filterSliced, 2, int64(2+filter.Len()))
		defer valuesSliced.Release()
		defer filterSliced.Release()

		f.doAssertFilter(valuesSliced, filterSliced, expected)
	})
}

func (f *FilterKernelTestSuite) assertFilterJSON(dt arrow.DataType, values, filter, expected string) {
	valuesArr, _, _ := array.FromJSON(f.mem, dt, strings.NewReader(values), array.WithUseNumber())
	defer valuesArr.Release()
	filterArr, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(filter))
	defer filterArr.Release()
	expectedArr, _, _ := array.FromJSON(f.mem, dt, strings.NewReader(expected), array.WithUseNumber())
	defer expectedArr.Release()

	f.assertFilter(valuesArr, filterArr, expectedArr)
}

func (f *FilterKernelTestSuite) TestNoValidityBitmapButUnknownNullCount() {
	values := f.getArr(arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	defer values.Release()
	filter := f.getArr(arrow.FixedWidthTypes.Boolean, `[true, true, false, true]`)
	defer filter.Release()

	expected, err := compute.FilterArray(context.TODO(), values, filter, *compute.DefaultFilterOptions())
	f.Require().NoError(err)
	defer expected.Release()

	filter.Data().(*array.Data).SetNullN(array.UnknownNullCount)
	result, err := compute.FilterArray(context.TODO(), values, filter, *compute.DefaultFilterOptions())
	f.Require().NoError(err)
	defer result.Release()

	assertArraysEqual(f.T(), expected, result)
}

type FilterKernelWithNull struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithNull) TestFilterNull() {
	f.assertFilterJSON(arrow.Null, `[]`, `[]`, `[]`)
	f.assertFilterJSON(arrow.Null, `[null, null, null]`, `[false, true, false]`, `[null]`)
	f.assertFilterJSON(arrow.Null, `[null, null, null]`, `[true, true, false]`, `[null, null]`)
}

type FilterKernelWithBoolean struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithBoolean) TestFilterBoolean() {
	f.assertFilterJSON(arrow.FixedWidthTypes.Boolean, `[]`, `[]`, `[]`)
	f.assertFilterJSON(arrow.FixedWidthTypes.Boolean,
		`[true, false, true]`, `[false, true, false]`, `[false]`)
	f.assertFilterJSON(arrow.FixedWidthTypes.Boolean,
		`[null, false, true]`, `[false, true, false]`, `[false]`)
	f.assertFilterJSON(arrow.FixedWidthTypes.Boolean,
		`[true, false, true]`, `[null, true, false]`, `[null, false]`)
}

func (f *FilterKernelWithBoolean) TestDefaultOptions() {
	values := f.getArr(arrow.PrimitiveTypes.Int8, `[7, 8, null, 9]`)
	valDatum := compute.NewDatum(values)
	values.Release()
	defer valDatum.Release()
	filter := f.getArr(arrow.FixedWidthTypes.Boolean, `[true, true, false, null]`)
	filterDatum := compute.NewDatum(filter)
	filter.Release()
	defer filterDatum.Release()

	noOpts, err := compute.CallFunction(context.TODO(), "filter", nil, valDatum, filterDatum)
	f.Require().NoError(err)
	defer noOpts.Release()

	defOpts, err := compute.CallFunction(context.TODO(), "filter", compute.DefaultFilterOptions(), valDatum, filterDatum)
	f.Require().NoError(err)
	defer defOpts.Release()

	assertDatumsEqual(f.T(), defOpts, noOpts)
}

type FilterKernelNumeric struct {
	FilterKernelTestSuite

	dt arrow.DataType
}

func (f *FilterKernelNumeric) TestFilterNumeric() {
	f.Run(f.dt.String(), func() {
		f.assertFilterJSON(f.dt, `[]`, `[]`, `[]`)
		f.assertFilterJSON(f.dt, `[9]`, `[false]`, `[]`)
		f.assertFilterJSON(f.dt, `[9]`, `[true]`, `[9]`)
		f.assertFilterJSON(f.dt, `[9]`, `[null]`, `[null]`)
		f.assertFilterJSON(f.dt, `[null]`, `[false]`, `[]`)
		f.assertFilterJSON(f.dt, `[null]`, `[true]`, `[null]`)
		f.assertFilterJSON(f.dt, `[null]`, `[null]`, `[null]`)

		f.assertFilterJSON(f.dt, `[7, 8, 9]`, `[false, true, false]`, `[8]`)
		f.assertFilterJSON(f.dt, `[7, 8, 9]`, `[true, false, true]`, `[7, 9]`)
		f.assertFilterJSON(f.dt, `[null, 8, 9]`, `[false, true, false]`, `[8]`)
		f.assertFilterJSON(f.dt, `[7, 8, 9]`, `[null, true, false]`, `[null, 8]`)
		f.assertFilterJSON(f.dt, `[7, 8, 9]`, `[true, null, true]`, `[7, null, 9]`)

		val := f.getArr(f.dt, `[7, 8, 9]`)
		defer val.Release()
		filter := f.getArr(arrow.FixedWidthTypes.Boolean, `[false, true, true, true, false, true]`)
		defer filter.Release()
		filter = array.NewSlice(filter, 3, 6)
		defer filter.Release()
		exp := f.getArr(f.dt, `[7, 9]`)
		defer exp.Release()

		f.assertFilter(val, filter, exp)

		invalidFilter := f.getArr(arrow.FixedWidthTypes.Boolean, `[]`)
		defer invalidFilter.Release()

		_, err := compute.FilterArray(context.TODO(), val, invalidFilter, f.emitNulls)
		f.ErrorIs(err, arrow.ErrInvalid)
		_, err = compute.FilterArray(context.TODO(), val, invalidFilter, f.dropOpts)
		f.ErrorIs(err, arrow.ErrInvalid)
	})
}

type comparator[T exec.NumericTypes] func(a, b T) bool

func getComparator[T exec.NumericTypes](op kernels.CompareOperator) comparator[T] {
	return []comparator[T]{
		// EQUAL
		func(a, b T) bool { return a == b },
		// NOT EQUAL
		func(a, b T) bool { return a != b },
		// GREATER
		func(a, b T) bool { return a > b },
		// GREATER_EQUAL
		func(a, b T) bool { return a >= b },
		// LESS
		func(a, b T) bool { return a < b },
		// LESS_EQUAL
		func(a, b T) bool { return a <= b },
	}[int8(op)]
}

func compareAndFilterImpl[T exec.NumericTypes](mem memory.Allocator, data []T, fn func(T) bool) arrow.Array {
	filtered := make([]T, 0, len(data))
	for _, v := range data {
		if fn(v) {
			filtered = append(filtered, v)
		}
	}
	return exec.ArrayFromSlice(mem, filtered)
}

func compareAndFilterValue[T exec.NumericTypes](mem memory.Allocator, data []T, val T, op kernels.CompareOperator) arrow.Array {
	cmp := getComparator[T](op)
	return compareAndFilterImpl(mem, data, func(e T) bool { return cmp(e, val) })
}

func compareAndFilterSlice[T exec.NumericTypes](mem memory.Allocator, data, other []T, op kernels.CompareOperator) arrow.Array {
	cmp := getComparator[T](op)
	i := 0
	return compareAndFilterImpl(mem, data, func(e T) bool {
		ret := cmp(e, other[i])
		i++
		return ret
	})
}

func createFilterImpl[T exec.NumericTypes](mem memory.Allocator, data []T, fn func(T) bool) arrow.Array {
	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()
	for _, v := range data {
		bldr.Append(fn(v))
	}
	return bldr.NewArray()
}

func createFilterValue[T exec.NumericTypes](mem memory.Allocator, data []T, val T, op kernels.CompareOperator) arrow.Array {
	cmp := getComparator[T](op)
	return createFilterImpl(mem, data, func(e T) bool { return cmp(e, val) })
}

func createFilterSlice[T exec.NumericTypes](mem memory.Allocator, data, other []T, op kernels.CompareOperator) arrow.Array {
	cmp := getComparator[T](op)
	i := 0
	return createFilterImpl(mem, data, func(e T) bool {
		ret := cmp(e, other[i])
		i++
		return ret
	})
}

func compareScalarAndFilterRandomNumeric[T exec.NumericTypes](t *testing.T, mem memory.Allocator) {
	dt := exec.GetDataType[T]()

	rng := gen.NewRandomArrayGenerator(randomSeed, mem)
	t.Run("compare scalar and filter", func(t *testing.T) {
		for i := 3; i < 10; i++ {
			length := int64(1 << i)
			t.Run(fmt.Sprintf("random %d", length), func(t *testing.T) {
				arr := rng.Numeric(dt.ID(), length, 0, 100, 0)
				defer arr.Release()
				data := exec.GetData[T](arr.Data().Buffers()[1].Bytes())
				for _, op := range []kernels.CompareOperator{kernels.CmpEQ, kernels.CmpNE, kernels.CmpGT, kernels.CmpLE} {
					selection := createFilterValue(mem, data, 50, op)
					defer selection.Release()

					filtered, err := compute.FilterArray(context.TODO(), arr, selection, *compute.DefaultFilterOptions())
					assert.NoError(t, err)
					defer filtered.Release()

					expected := compareAndFilterValue(mem, data, 50, op)
					defer expected.Release()

					assertArraysEqual(t, expected, filtered)
				}
			})
		}
	})
}

func compareArrayAndFilterRandomNumeric[T exec.NumericTypes](t *testing.T, mem memory.Allocator) {
	dt := exec.GetDataType[T]()
	rng := gen.NewRandomArrayGenerator(randomSeed, mem)
	t.Run("compare array and filter", func(t *testing.T) {
		for i := 3; i < 10; i++ {
			length := int64(1 << i)
			t.Run(fmt.Sprintf("length %d", length), func(t *testing.T) {
				lhs := rng.Numeric(dt.ID(), length, 0, 100, 0)
				defer lhs.Release()
				rhs := rng.Numeric(dt.ID(), length, 0, 100, 0)
				defer rhs.Release()

				data := exec.GetData[T](lhs.Data().Buffers()[1].Bytes())
				other := exec.GetData[T](rhs.Data().Buffers()[1].Bytes())
				for _, op := range []kernels.CompareOperator{kernels.CmpEQ, kernels.CmpNE, kernels.CmpGT, kernels.CmpLE} {
					selection := createFilterSlice(mem, data, other, op)
					defer selection.Release()

					filtered, err := compute.FilterArray(context.TODO(), lhs, selection, *compute.DefaultFilterOptions())
					require.NoError(t, err)
					defer filtered.Release()

					expected := compareAndFilterSlice(mem, data, other, op)
					defer expected.Release()

					assertArraysEqual(t, expected, filtered)
				}
			})
		}
	})
}

func (f *FilterKernelNumeric) TestCompareScalarAndFilterRandom() {
	switch f.dt.ID() {
	case arrow.INT8:
		compareScalarAndFilterRandomNumeric[int8](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[int8](f.T(), f.mem)
	case arrow.UINT8:
		compareScalarAndFilterRandomNumeric[uint8](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[uint8](f.T(), f.mem)
	case arrow.INT16:
		compareScalarAndFilterRandomNumeric[int16](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[int16](f.T(), f.mem)
	case arrow.UINT16:
		compareScalarAndFilterRandomNumeric[uint16](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[uint16](f.T(), f.mem)
	case arrow.INT32:
		compareScalarAndFilterRandomNumeric[int32](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[int32](f.T(), f.mem)
	case arrow.UINT32:
		compareScalarAndFilterRandomNumeric[uint32](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[uint32](f.T(), f.mem)
	case arrow.INT64:
		compareScalarAndFilterRandomNumeric[int64](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[int64](f.T(), f.mem)
	case arrow.UINT64:
		compareScalarAndFilterRandomNumeric[uint64](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[uint64](f.T(), f.mem)
	case arrow.FLOAT32:
		compareScalarAndFilterRandomNumeric[float32](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[float32](f.T(), f.mem)
	case arrow.FLOAT64:
		compareScalarAndFilterRandomNumeric[float64](f.T(), f.mem)
		compareArrayAndFilterRandomNumeric[float64](f.T(), f.mem)
	}
}

type FilterKernelWithDecimal struct {
	FilterKernelTestSuite

	dt arrow.DataType
}

func (f *FilterKernelWithDecimal) TestFilterDecimalNumeric() {
	f.assertFilterJSON(f.dt, `[]`, `[]`, `[]`)

	f.assertFilterJSON(f.dt, `["9.00"]`, `[false]`, `[]`)
	f.assertFilterJSON(f.dt, `["9.00"]`, `[true]`, `["9.00"]`)
	f.assertFilterJSON(f.dt, `["9.00"]`, `[null]`, `[null]`)
	f.assertFilterJSON(f.dt, `[null]`, `[false]`, `[]`)
	f.assertFilterJSON(f.dt, `[null]`, `[true]`, `[null]`)
	f.assertFilterJSON(f.dt, `[null]`, `[null]`, `[null]`)

	f.assertFilterJSON(f.dt, `["7.12", "8.00", "9.87"]`, `[false, true, false]`, `["8.00"]`)
	f.assertFilterJSON(f.dt, `["7.12", "8.00", "9.87"]`, `[true, false, true]`, `["7.12", "9.87"]`)
	f.assertFilterJSON(f.dt, `[null, "8.00", "9.87"]`, `[false, true, false]`, `["8.00"]`)
	f.assertFilterJSON(f.dt, `["7.12", "8.00", "9.87"]`, `[null, true, false]`, `[null, "8.00"]`)
	f.assertFilterJSON(f.dt, `["7.12", "8.00", "9.87"]`, `[true, null, true]`, `["7.12", null, "9.87"]`)

	val := f.getArr(f.dt, `["7.12", "8.00", "9.87"]`)
	defer val.Release()
	filter := f.getArr(arrow.FixedWidthTypes.Boolean, `[false, true, true, true, false, true]`)
	defer filter.Release()
	filter = array.NewSlice(filter, 3, 6)
	defer filter.Release()
	exp := f.getArr(f.dt, `["7.12", "9.87"]`)
	defer exp.Release()

	f.assertFilter(val, filter, exp)

	invalidFilter := f.getArr(arrow.FixedWidthTypes.Boolean, `[]`)
	defer invalidFilter.Release()

	_, err := compute.FilterArray(context.TODO(), val, invalidFilter, f.emitNulls)
	f.ErrorIs(err, arrow.ErrInvalid)
	_, err = compute.FilterArray(context.TODO(), val, invalidFilter, f.dropOpts)
	f.ErrorIs(err, arrow.ErrInvalid)
}

type FilterKernelWithString struct {
	FilterKernelTestSuite

	dt arrow.DataType
}

func (f *FilterKernelWithString) TestFilterString() {
	f.Run(f.dt.String(), func() {
		f.assertFilterJSON(f.dt, `["YQ==", "Yg==", "Yw=="]`, `[false, true, false]`, `["Yg=="]`)
		f.assertFilterJSON(f.dt, `[null, "Yg==", "Yw=="]`, `[false, true, false]`, `["Yg=="]`)
		f.assertFilterJSON(f.dt, `["YQ==", "Yg==", "Yw=="]`, `[null, true, false]`, `[null, "Yg=="]`)
	})
}

func TestFilterKernels(t *testing.T) {
	suite.Run(t, new(FilterKernelWithNull))
	suite.Run(t, new(FilterKernelWithBoolean))
	for _, dt := range numericTypes {
		suite.Run(t, &FilterKernelNumeric{dt: dt})
	}
	for _, dt := range []arrow.DataType{&arrow.Decimal128Type{Precision: 3, Scale: 2}, &arrow.Decimal256Type{Precision: 3, Scale: 2}} {
		suite.Run(t, &FilterKernelWithDecimal{dt: dt})
	}
	for _, dt := range baseBinaryTypes {
		suite.Run(t, &FilterKernelWithString{dt: dt})
	}
}

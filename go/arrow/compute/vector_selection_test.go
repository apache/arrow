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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/compute/exec"
	"github.com/apache/arrow/go/v14/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v14/arrow/internal/testing/gen"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/apache/arrow/go/v14/internal/types"
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
	ctx := compute.WithAllocator(context.TODO(), f.mem)
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

type TakeKernelTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func (tk *TakeKernelTestSuite) SetupTest() {
	tk.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	tk.ctx = compute.WithAllocator(context.TODO(), tk.mem)
}

func (tk *TakeKernelTestSuite) TearDownTest() {
	tk.mem.AssertSize(tk.T(), 0)
}

func (tk *TakeKernelTestSuite) assertTakeArrays(values, indices, expected arrow.Array) {
	actual, err := compute.TakeArray(tk.ctx, values, indices)
	tk.Require().NoError(err)
	defer actual.Release()
	assertArraysEqual(tk.T(), expected, actual)
}

func (tk *TakeKernelTestSuite) takeJSON(dt arrow.DataType, values string, idxType arrow.DataType, indices string) (arrow.Array, error) {
	valArr, _, _ := array.FromJSON(tk.mem, dt, strings.NewReader(values), array.WithUseNumber())
	defer valArr.Release()
	indArr, _, _ := array.FromJSON(tk.mem, idxType, strings.NewReader(indices))
	defer indArr.Release()

	return compute.TakeArray(tk.ctx, valArr, indArr)
}

func (tk *TakeKernelTestSuite) checkTake(dt arrow.DataType, valuesJSON, indicesJSON, expJSON string) {
	values, _, _ := array.FromJSON(tk.mem, dt, strings.NewReader(valuesJSON), array.WithUseNumber())
	defer values.Release()
	expected, _, _ := array.FromJSON(tk.mem, dt, strings.NewReader(expJSON), array.WithUseNumber())
	defer expected.Release()

	for _, idxType := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint32} {
		tk.Run(fmt.Sprintf("idxtype %s", idxType), func() {
			indices, _, _ := array.FromJSON(tk.mem, idxType, strings.NewReader(indicesJSON))
			defer indices.Release()

			tk.assertTakeArrays(values, indices, expected)

			if dt.ID() != arrow.DENSE_UNION {
				tk.Run("sliced values", func() {
					valuesFiller := array.MakeArrayOfNull(tk.mem, dt, 2)
					defer valuesFiller.Release()

					valuesSliced, _ := array.Concatenate([]arrow.Array{valuesFiller, values, valuesFiller}, tk.mem)
					defer valuesSliced.Release()
					valuesSliced = array.NewSlice(valuesSliced, 2, 2+int64(values.Len()))
					defer valuesSliced.Release()

					tk.assertTakeArrays(valuesSliced, indices, expected)
				})
			}

			tk.Run("sliced indices", func() {
				zero, _ := scalar.MakeScalarParam(0, idxType)
				indicesFiller, _ := scalar.MakeArrayFromScalar(zero, 3, tk.mem)
				defer indicesFiller.Release()
				indicesSliced, _ := array.Concatenate([]arrow.Array{indicesFiller, indices, indicesFiller}, tk.mem)
				defer indicesSliced.Release()
				indicesSliced = array.NewSlice(indicesSliced, 3, int64(indices.Len()+3))
				defer indicesSliced.Release()

				tk.assertTakeArrays(values, indicesSliced, expected)
			})
		})
	}
}

func (tk *TakeKernelTestSuite) assertTakeNull(values, indices, expected string) {
	tk.checkTake(arrow.Null, values, indices, expected)
}

func (tk *TakeKernelTestSuite) assertTakeBool(values, indices, expected string) {
	tk.checkTake(arrow.FixedWidthTypes.Boolean, values, indices, expected)
}

func (tk *TakeKernelTestSuite) assertNoValidityBitmapButUnknownNullCount(values, indices arrow.Array) {
	tk.Zero(values.NullN())
	tk.Zero(indices.NullN())
	exp, err := compute.TakeArray(tk.ctx, values, indices)
	tk.Require().NoError(err)
	defer exp.Release()

	newValuesData := values.Data().(*array.Data).Copy()
	newValuesData.SetNullN(array.UnknownNullCount)
	newValuesData.Buffers()[0].Release()
	newValuesData.Buffers()[0] = nil
	defer newValuesData.Release()
	newValues := array.MakeFromData(newValuesData)

	newIndicesData := indices.Data().(*array.Data).Copy()
	newIndicesData.SetNullN(array.UnknownNullCount)
	newIndicesData.Buffers()[0].Release()
	newIndicesData.Buffers()[0] = nil
	defer newIndicesData.Release()
	newIndices := array.MakeFromData(newIndicesData)

	defer newValues.Release()
	defer newIndices.Release()

	result, err := compute.TakeArray(tk.ctx, newValues, newIndices)
	tk.Require().NoError(err)
	defer result.Release()

	assertArraysEqual(tk.T(), exp, result)
}

func (tk *TakeKernelTestSuite) assertNoValidityBitmapUnknownNullCountJSON(dt arrow.DataType, values, indices string) {
	vals, _, _ := array.FromJSON(tk.mem, dt, strings.NewReader(values), array.WithUseNumber())
	defer vals.Release()
	inds, _, _ := array.FromJSON(tk.mem, arrow.PrimitiveTypes.Int16, strings.NewReader(indices))
	defer inds.Release()
	tk.assertNoValidityBitmapButUnknownNullCount(vals, inds)
}

type TakeKernelTest struct {
	TakeKernelTestSuite
}

func (tk *TakeKernelTest) TestTakeNull() {
	tk.assertTakeNull(`[null, null, null]`, `[0, 1, 0]`, `[null, null, null]`)
	tk.assertTakeNull(`[null, null, null]`, `[0, 2]`, `[null, null]`)

	_, err := tk.takeJSON(arrow.Null, `[null, null, null]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeJSON(arrow.Null, `[null, null, null]`, arrow.PrimitiveTypes.Int8, `[0, -1, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
}

func (tk *TakeKernelTest) TestInvalidIndexType() {
	_, err := tk.takeJSON(arrow.Null, `[null, null, null]`, arrow.PrimitiveTypes.Float32, `[0.0, 1.0, 0.1]`)
	tk.ErrorIs(err, arrow.ErrNotImplemented)
}

func (tk *TakeKernelTest) TestDefaultOptions() {
	indArr, _, _ := array.FromJSON(tk.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[null, 2, 0, 3]`))
	defer indArr.Release()
	valArr, _, _ := array.FromJSON(tk.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[7, 8, 9, null]`))
	defer valArr.Release()

	indices, values := compute.NewDatum(indArr), compute.NewDatum(valArr)
	defer indices.Release()
	defer values.Release()

	noOptions, err := compute.CallFunction(tk.ctx, "take", nil, values, indices)
	tk.Require().NoError(err)
	defer noOptions.Release()

	explicitDefaults, err := compute.CallFunction(tk.ctx, "take", compute.DefaultTakeOptions(), values, indices)
	tk.Require().NoError(err)
	defer explicitDefaults.Release()

	assertDatumsEqual(tk.T(), explicitDefaults, noOptions, nil, nil)
}

func (tk *TakeKernelTest) TestTakeBoolean() {
	tk.assertTakeBool(`[true, true, true]`, `[]`, `[]`)
	tk.assertTakeBool(`[true, false, true]`, `[0, 1, 0]`, `[true, false, true]`)
	tk.assertTakeBool(`[null, false, true]`, `[0, 1, 0]`, `[null, false, null]`)
	tk.assertTakeBool(`[true, false, true]`, `[null, 1, 0]`, `[null, false, true]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(arrow.FixedWidthTypes.Boolean, `[true, false, true]`, `[1, 0, 0]`)
	_, err := tk.takeJSON(arrow.FixedWidthTypes.Boolean, `[true, false, true]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeJSON(arrow.FixedWidthTypes.Boolean, `[true, false, true]`, arrow.PrimitiveTypes.Int8, `[0, -1, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
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

	assertDatumsEqual(f.T(), defOpts, noOpts, nil, nil)
}

type FilterKernelExtension struct {
	FilterKernelTestSuite
}

func (f *FilterKernelExtension) TestExtension() {
	dt := types.NewSmallintType()
	arrow.RegisterExtensionType(dt)
	defer arrow.UnregisterExtensionType(dt.ExtensionName())

	f.assertFilterJSON(dt, `[]`, `[]`, `[]`)
	f.assertFilterJSON(dt, `[9]`, `[false]`, `[]`)
	f.assertFilterJSON(dt, `[9]`, `[true]`, `[9]`)
	f.assertFilterJSON(dt, `[9]`, `[null]`, `[null]`)
	f.assertFilterJSON(dt, `[null]`, `[false]`, `[]`)
	f.assertFilterJSON(dt, `[null]`, `[true]`, `[null]`)
	f.assertFilterJSON(dt, `[null]`, `[null]`, `[null]`)

	f.assertFilterJSON(dt, `[7, 8, 9]`, `[false, true, false]`, `[8]`)
	f.assertFilterJSON(dt, `[7, 8, 9]`, `[true, false, true]`, `[7, 9]`)
	f.assertFilterJSON(dt, `[null, 8, 9]`, `[false, true, false]`, `[8]`)
	f.assertFilterJSON(dt, `[7, 8, 9]`, `[null, true, false]`, `[null, 8]`)
	f.assertFilterJSON(dt, `[7, 8, 9]`, `[true, null, true]`, `[7, null, 9]`)

	val := f.getArr(dt, `[7, 8, 9]`)
	defer val.Release()
	filter := f.getArr(arrow.FixedWidthTypes.Boolean, `[false, true, true, true, false, true]`)
	defer filter.Release()
	filter = array.NewSlice(filter, 3, 6)
	defer filter.Release()
	exp := f.getArr(dt, `[7, 9]`)
	defer exp.Release()

	f.assertFilter(val, filter, exp)

	invalidFilter := f.getArr(arrow.FixedWidthTypes.Boolean, `[]`)
	defer invalidFilter.Release()

	_, err := compute.FilterArray(context.TODO(), val, invalidFilter, f.emitNulls)
	f.ErrorIs(err, arrow.ErrInvalid)
	_, err = compute.FilterArray(context.TODO(), val, invalidFilter, f.dropOpts)
	f.ErrorIs(err, arrow.ErrInvalid)
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

type FilterKernelWithList struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithList) TestListInt32() {
	dt := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	listJSON := `[[], [1, 2], null, [3]]`
	f.assertFilterJSON(dt, listJSON, `[false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, listJSON, `[false, true, true, null]`, `[[1, 2], null, null]`)
	f.assertFilterJSON(dt, listJSON, `[false, false, true, null]`, `[null, null]`)
	f.assertFilterJSON(dt, listJSON, `[true, false, false, true]`, `[[], [3]]`)
	f.assertFilterJSON(dt, listJSON, `[true, true, true, true]`, listJSON)
	f.assertFilterJSON(dt, listJSON, `[false, true, false, true]`, `[[1, 2], [3]]`)
}

func (f *FilterKernelWithList) TestListListInt32() {
	dt := arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))
	listJSON := `[
		[],
		[[1], [2, null, 2], []],
		null,
		[[3, null], null]
	]`

	f.assertFilterJSON(dt, listJSON, `[false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, listJSON, `[false, true, true, null]`, `[
		[[1], [2, null, 2], []],
		null,
		null
	]`)
	f.assertFilterJSON(dt, listJSON, `[false, false, true, null]`, `[null, null]`)
	f.assertFilterJSON(dt, listJSON, `[true, false, false, true]`, `[
		[],
		[[3, null], null]
	]`)
	f.assertFilterJSON(dt, listJSON, `[true, true, true, true]`, listJSON)
	f.assertFilterJSON(dt, listJSON, `[false, true, false, true]`, `[
		[[1], [2, null, 2], []],
		[[3, null], null]
	]`)
}

func (f *FilterKernelWithList) TestLargeListInt32() {
	dt := arrow.LargeListOf(arrow.PrimitiveTypes.Int32)
	listJSON := `[[], [1, 2], null, [3]]`
	f.assertFilterJSON(dt, listJSON, `[false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, listJSON, `[false, true, true, null]`, `[[1, 2], null, null]`)
}

func (f *FilterKernelWithList) TestFixedSizeListInt32() {
	dt := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32)
	listJSON := `[null, [1, null, 3], [4, 5, 6], [7, 8, null]]`
	f.assertFilterJSON(dt, listJSON, `[false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, listJSON, `[false, true, true, null]`, `[[1, null, 3], [4, 5, 6], null]`)
	f.assertFilterJSON(dt, listJSON, `[false, false, true, null]`, `[[4, 5, 6], null]`)
	f.assertFilterJSON(dt, listJSON, `[true, true, true, true]`, listJSON)
	f.assertFilterJSON(dt, listJSON, `[false, true, false, true]`, `[[1, null, 3], [7, 8, null]]`)
}

type FilterKernelWithUnion struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithUnion) TestDenseUnion() {
	dt := arrow.DenseUnionOf([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, []arrow.UnionTypeCode{2, 5})

	unionJSON := `[
		[2, null],
		[2, 222],
		[5, "hello"],
		[5, "eh"],
		[2, null],
		[2, 111],
		[5, null]
	]`

	f.assertFilterJSON(dt, unionJSON, `[false, false, false, false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, unionJSON, `[false, true, true, null, false, true, true]`, `[
		[2, 222],
		[5, "hello"],
		[2, null],
		[2, 111],
		[5, null]
	]`)
	f.assertFilterJSON(dt, unionJSON, `[true, false, true, false, true, false, false]`, `[
		[2, null],
		[5, "hello"],
		[2, null]
	]`)
	f.assertFilterJSON(dt, unionJSON, `[true, true, true, true, true, true, true]`, unionJSON)

	// sliced
	// (check this manually as concat of dense unions isn't supported)
	unionArr, _, _ := array.FromJSON(f.mem, dt, strings.NewReader(unionJSON))
	defer unionArr.Release()

	filterArr, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(`[false, true, true, null, false, true, true]`))
	defer filterArr.Release()

	expected, _, _ := array.FromJSON(f.mem, dt, strings.NewReader(`[[5, "hello"], [2, null], [2, 111]]`))
	defer expected.Release()

	values := array.NewSlice(unionArr, 2, 6)
	defer values.Release()
	filter := array.NewSlice(filterArr, 2, 6)
	defer filter.Release()
	f.assertFilter(values, filter, expected)
}

type FilterKernelWithStruct struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithStruct) TestStruct() {
	dt := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true})

	structJSON := `[
		null,
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`

	f.assertFilterJSON(dt, structJSON, `[false, false, false, false]`, `[]`)
	f.assertFilterJSON(dt, structJSON, `[false, true, true, null]`, `[
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		null
	]`)
	f.assertFilterJSON(dt, structJSON, `[true, true, true, true]`, structJSON)
	f.assertFilterJSON(dt, structJSON, `[true, false, true, false]`, `[null, {"a": 2, "b": "hello"}]`)
}

type FilterKernelWithRecordBatch struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithRecordBatch) doFilter(sc *arrow.Schema, batchJSON, selection string, opts compute.FilterOptions) (arrow.Record, error) {
	rec, _, err := array.RecordFromJSON(f.mem, sc, strings.NewReader(batchJSON), array.WithUseNumber())
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	batch := compute.NewDatum(rec)
	defer batch.Release()

	filter, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(selection))
	defer filter.Release()
	filterDatum := compute.NewDatum(filter)
	defer filterDatum.Release()

	outDatum, err := compute.Filter(context.TODO(), batch, filterDatum, opts)
	if err != nil {
		return nil, err
	}

	return outDatum.(*compute.RecordDatum).Value, nil
}

func (f *FilterKernelWithRecordBatch) assertFilter(sc *arrow.Schema, batchJSON, selection string, opts compute.FilterOptions, expectedBatch string) {
	actual, err := f.doFilter(sc, batchJSON, selection, opts)
	f.Require().NoError(err)
	defer actual.Release()

	expected, _, err := array.RecordFromJSON(f.mem, sc, strings.NewReader(expectedBatch), array.WithUseNumber())
	f.Require().NoError(err)
	defer expected.Release()

	f.Truef(array.RecordEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func (f *FilterKernelWithRecordBatch) TestFilterRecord() {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	sc := arrow.NewSchema(fields, nil)

	batchJSON := `[
		{"a": null, "b": "yo"},
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`

	for _, opts := range []compute.FilterOptions{f.emitNulls, f.dropOpts} {
		f.assertFilter(sc, batchJSON, `[false, false, false, false]`, opts, `[]`)
		f.assertFilter(sc, batchJSON, `[true, true, true, true]`, opts, batchJSON)
		f.assertFilter(sc, batchJSON, `[true, false, true, false]`, opts, `[
			{"a": null, "b": "yo"},
			{"a": 2, "b": "hello"}
		]`)
	}

	f.assertFilter(sc, batchJSON, `[false, true, true, null]`, f.dropOpts, `[
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"}
	]`)

	f.assertFilter(sc, batchJSON, `[false, true, true, null]`, f.emitNulls, `[
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": null, "b": null}
	]`)
}

type FilterKernelWithChunked struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithChunked) filterWithArray(dt arrow.DataType, values []string, filterStr string) (*arrow.Chunked, error) {
	chk, err := array.ChunkedFromJSON(f.mem, dt, values)
	f.Require().NoError(err)
	defer chk.Release()

	input := compute.NewDatum(chk)
	defer input.Release()

	filter, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(filterStr))
	defer filter.Release()

	filterDatum := compute.NewDatum(filter)
	defer filterDatum.Release()

	out, err := compute.Filter(context.TODO(), input, filterDatum, *compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}
	return out.(*compute.ChunkedDatum).Value, nil
}

func (f *FilterKernelWithChunked) filterWithChunked(dt arrow.DataType, values, filter []string) (*arrow.Chunked, error) {
	chk, err := array.ChunkedFromJSON(f.mem, dt, values)
	f.Require().NoError(err)
	defer chk.Release()

	input := compute.NewDatum(chk)
	defer input.Release()

	filtChk, err := array.ChunkedFromJSON(f.mem, arrow.FixedWidthTypes.Boolean, filter)
	f.Require().NoError(err)
	defer filtChk.Release()

	filtDatum := compute.NewDatum(filtChk)
	defer filtDatum.Release()

	out, err := compute.Filter(context.TODO(), input, filtDatum, *compute.DefaultFilterOptions())
	if err != nil {
		return nil, err
	}
	return out.(*compute.ChunkedDatum).Value, nil
}

func (f *FilterKernelWithChunked) assertFilter(dt arrow.DataType, values []string, filter string, expected []string) {
	actual, err := f.filterWithArray(dt, values, filter)
	f.Require().NoError(err)
	defer actual.Release()

	expectedResult, _ := array.ChunkedFromJSON(f.mem, dt, expected)
	defer expectedResult.Release()
	if !f.True(array.ChunkedEqual(expectedResult, actual)) {
		var s strings.Builder
		s.WriteString("expected: \n")
		for _, c := range expectedResult.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		s.WriteString("actual: \n")
		for _, c := range actual.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		f.T().Log(s.String())
	}
}

func (f *FilterKernelWithChunked) assertChunkedFilter(dt arrow.DataType, values, filter, expected []string) {
	actual, err := f.filterWithChunked(dt, values, filter)
	f.Require().NoError(err)
	defer actual.Release()

	expectedResult, _ := array.ChunkedFromJSON(f.mem, dt, expected)
	defer expectedResult.Release()
	if !f.True(array.ChunkedEqual(expectedResult, actual)) {
		var s strings.Builder
		s.WriteString("expected: \n")
		for _, c := range expectedResult.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		s.WriteString("actual: \n")
		for _, c := range actual.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		f.T().Log(s.String())
	}
}

func (f *FilterKernelWithChunked) TestFilterChunked() {
	f.assertFilter(arrow.PrimitiveTypes.Int8, []string{`[]`}, `[]`, []string{})
	f.assertChunkedFilter(arrow.PrimitiveTypes.Int8, []string{`[]`}, []string{`[]`}, []string{})

	f.assertFilter(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, `[false, true, false]`, []string{`[8]`})
	f.assertChunkedFilter(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, []string{`[false]`, `[true, false]`}, []string{`[8]`})
	f.assertChunkedFilter(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, []string{`[false, true]`, `[false]`}, []string{`[8]`})

	_, err := f.filterWithArray(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, `[false, true, false, true, true]`)
	f.ErrorIs(err, arrow.ErrInvalid)
	_, err = f.filterWithChunked(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, []string{`[ false, true, false]`, `[true, true]`})
	f.ErrorIs(err, arrow.ErrInvalid)
}

type FilterKernelWithTable struct {
	FilterKernelTestSuite
}

func (f *FilterKernelWithTable) filterWithArray(sc *arrow.Schema, values []string, filter string, opts compute.FilterOptions) (arrow.Table, error) {
	tbl, err := array.TableFromJSON(f.mem, sc, values)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filterArr, _, _ := array.FromJSON(f.mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(filter))
	defer filterArr.Release()

	out, err := compute.Filter(context.TODO(), &compute.TableDatum{Value: tbl}, &compute.ArrayDatum{Value: filterArr.Data()}, opts)
	if err != nil {
		return nil, err
	}
	return out.(*compute.TableDatum).Value, nil
}

func (f *FilterKernelWithTable) filterWithChunked(sc *arrow.Schema, values, filter []string, opts compute.FilterOptions) (arrow.Table, error) {
	tbl, err := array.TableFromJSON(f.mem, sc, values)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filtChk, err := array.ChunkedFromJSON(f.mem, arrow.FixedWidthTypes.Boolean, filter)
	f.Require().NoError(err)
	defer filtChk.Release()

	out, err := compute.Filter(context.TODO(), &compute.TableDatum{Value: tbl}, &compute.ChunkedDatum{Value: filtChk}, opts)
	if err != nil {
		return nil, err
	}
	return out.(*compute.TableDatum).Value, nil
}

func (f *FilterKernelWithTable) assertChunkedFilter(sc *arrow.Schema, tableJSON, filter []string, opts compute.FilterOptions, expTable []string) {
	actual, err := f.filterWithChunked(sc, tableJSON, filter, opts)
	f.Require().NoError(err)
	defer actual.Release()

	expected, err := array.TableFromJSON(f.mem, sc, expTable)
	f.Require().NoError(err)
	defer expected.Release()

	f.Truef(array.TableEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func (f *FilterKernelWithTable) assertFilter(sc *arrow.Schema, tableJSON []string, filter string, opts compute.FilterOptions, expectedTable []string) {
	actual, err := f.filterWithArray(sc, tableJSON, filter, opts)
	f.Require().NoError(err)
	defer actual.Release()

	expected, err := array.TableFromJSON(f.mem, sc, expectedTable)
	f.Require().NoError(err)
	defer expected.Release()

	f.Truef(array.TableEqual(expected, actual), "expected: %s\ngot: %s", expected, actual)
}

func (f *FilterKernelWithTable) TestFilterTable() {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	sc := arrow.NewSchema(fields, nil)
	tableJSON := []string{`[
		{"a": null, "b": "yo"},
		{"a": 1, "b": ""}
	]`, `[
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`}

	for _, opt := range []compute.FilterOptions{f.emitNulls, f.dropOpts} {
		f.assertFilter(sc, tableJSON, `[false, false, false, false]`, opt, []string{})
		f.assertChunkedFilter(sc, tableJSON, []string{`[false]`, `[false, false, false]`}, opt, []string{})
		f.assertFilter(sc, tableJSON, `[true, true, true, true]`, opt, tableJSON)
		f.assertChunkedFilter(sc, tableJSON, []string{`[true]`, `[true, true, true]`}, opt, tableJSON)
	}

	expectedEmitNull := []string{`[{"a": 1, "b": ""}]`, `[{"a": 2, "b": "hello"},{"a": null, "b": null}]`}
	f.assertFilter(sc, tableJSON, `[false, true, true, null]`, f.emitNulls, expectedEmitNull)
	f.assertChunkedFilter(sc, tableJSON, []string{`[false, true, true]`, `[null]`}, f.emitNulls, expectedEmitNull)

	expectedDrop := []string{`[{"a": 1, "b": ""}]`, `[{"a": 2, "b": "hello"}]`}
	f.assertFilter(sc, tableJSON, `[false, true, true, null]`, f.dropOpts, expectedDrop)
	f.assertChunkedFilter(sc, tableJSON, []string{`[false, true, true]`, `[null]`}, f.dropOpts, expectedDrop)
}

type TakeKernelTestTyped struct {
	TakeKernelTestSuite

	dt arrow.DataType
}

func (tk *TakeKernelTestTyped) assertTake(values, indices, expected string) {
	tk.checkTake(tk.dt, values, indices, expected)
}

type TakeKernelTestNumeric struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestNumeric) TestTakeNumeric() {
	tk.Run(tk.dt.String(), func() {
		tk.assertTake(`[7, 8, 9]`, `[]`, `[]`)
		tk.assertTake(`[7, 8, 9]`, `[0, 1, 0]`, `[7, 8, 7]`)
		tk.assertTake(`[null, 8, 9]`, `[0, 1, 0]`, `[null, 8, null]`)
		tk.assertTake(`[7, 8, 9]`, `[null, 1, 0]`, `[null, 8, 7]`)
		tk.assertTake(`[null, 8, 9]`, `[]`, `[]`)
		tk.assertTake(`[7, 8, 9]`, `[0, 0, 0, 0, 0, 0, 2]`, `[7, 7, 7, 7, 7, 7, 9]`)

		_, err := tk.takeJSON(tk.dt, `[7, 8, 9]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
		tk.ErrorIs(err, arrow.ErrIndex)
		_, err = tk.takeJSON(tk.dt, `[7, 8, 9]`, arrow.PrimitiveTypes.Int8, `[0, -1, 0]`)
		tk.ErrorIs(err, arrow.ErrIndex)
	})
}

type TakeKernelTestExtension struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestExtension) TestTakeExtension() {
	tk.dt = types.NewSmallintType()
	arrow.RegisterExtensionType(tk.dt.(arrow.ExtensionType))
	defer arrow.UnregisterExtensionType("smallint")

	tk.assertTake(`[7, 8, 9]`, `[]`, `[]`)
	tk.assertTake(`[7, 8, 9]`, `[0, 1, 0]`, `[7, 8, 7]`)
	tk.assertTake(`[null, 8, 9]`, `[0, 1, 0]`, `[null, 8, null]`)
	tk.assertTake(`[7, 8, 9]`, `[null, 1, 0]`, `[null, 8, 7]`)
	tk.assertTake(`[null, 8, 9]`, `[]`, `[]`)
	tk.assertTake(`[7, 8, 9]`, `[0, 0, 0, 0, 0, 0, 2]`, `[7, 7, 7, 7, 7, 7, 9]`)

	_, err := tk.takeJSON(tk.dt, `[7, 8, 9]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeJSON(tk.dt, `[7, 8, 9]`, arrow.PrimitiveTypes.Int8, `[0, -1, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
}

type TakeKernelTestFSB struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestFSB) SetupSuite() {
	tk.dt = &arrow.FixedSizeBinaryType{ByteWidth: 3}
}

func (tk *TakeKernelTestFSB) TestFixedSizeBinary() {
	// YWFh == base64("aaa")
	// YmJi == base64("bbb")
	// Y2Nj == base64("ccc")
	tk.assertTake(`["YWFh", "YmJi", "Y2Nj"]`, `[0, 1, 0]`, `["YWFh", "YmJi", "YWFh"]`)
	tk.assertTake(`[null, "YmJi", "Y2Nj"]`, `[0, 1, 0]`, `[null, "YmJi", null]`)
	tk.assertTake(`["YWFh", "YmJi", "Y2Nj"]`, `[null, 1, 0]`, `[null, "YmJi", "YWFh"]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `["YWFh", "YmJi", "Y2Nj"]`, `[0, 1, 0]`)

	_, err := tk.takeJSON(tk.dt, `["YWFh", "YmJi", "Y2Nj"]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeJSON(tk.dt, `["YWFh", "YmJi", "Y2Nj"]`, arrow.PrimitiveTypes.Int64, `[2, 5]`)
	tk.ErrorIs(err, arrow.ErrIndex)
}

type TakeKernelTestString struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestString) TestTakeString() {
	tk.Run(tk.dt.String(), func() {
		// base64 encoded so the binary non-utf8 arrays work
		// YQ== -> "a"
		// Yg== -> "b"
		// Yw== -> "c"
		tk.assertTake(`["YQ==", "Yg==", "Yw=="]`, `[0, 1, 0]`, `["YQ==", "Yg==", "YQ=="]`)
		tk.assertTake(`[null, "Yg==", "Yw=="]`, `[0, 1, 0]`, `[null, "Yg==", null]`)
		tk.assertTake(`["YQ==", "Yg==", "Yw=="]`, `[null, 1, 0]`, `[null, "Yg==", "YQ=="]`)

		tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `["YQ==", "Yg==", "Yw=="]`, `[0, 1, 0]`)

		_, err := tk.takeJSON(tk.dt, `["YQ==", "Yg==", "Yw=="]`, arrow.PrimitiveTypes.Int8, `[0, 9, 0]`)
		tk.ErrorIs(err, arrow.ErrIndex)
		_, err = tk.takeJSON(tk.dt, `["YQ==", "Yg==", "Yw=="]`, arrow.PrimitiveTypes.Int64, `[2, 5]`)
		tk.ErrorIs(err, arrow.ErrIndex)
	})
}

type TakeKernelLists struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelLists) TestListInt32() {
	tk.dt = arrow.ListOf(arrow.PrimitiveTypes.Int32)

	listJSON := `[[], [1, 2], null, [3]]`
	tk.checkTake(tk.dt, listJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, listJSON, `[3, 2, 1]`, `[[3], null, [1,2]]`)
	tk.checkTake(tk.dt, listJSON, `[null, 3, 0]`, `[null, [3], []]`)
	tk.checkTake(tk.dt, listJSON, `[null, null]`, `[null, null]`)
	tk.checkTake(tk.dt, listJSON, `[3, 0, 0, 3]`, `[[3], [], [], [3]]`)
	tk.checkTake(tk.dt, listJSON, `[0, 1, 2, 3]`, listJSON)
	tk.checkTake(tk.dt, listJSON, `[0, 0, 0, 0, 0, 0, 1]`, `[[], [], [], [], [], [], [1, 2]]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `[[], [1, 2], [3]]`, `[0, 1, 0]`)
}

func (tk *TakeKernelLists) TestListListInt32() {
	tk.dt = arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))

	listJSON := `[
		[],
		[[1], [2, null, 2], []],
		null,
		[[3, null], null]
	]`
	tk.checkTake(tk.dt, listJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, listJSON, `[3, 2, 1]`, `[
		[[3, null], null],
		null,
		[[1], [2, null, 2], []]
	]`)
	tk.checkTake(tk.dt, listJSON, `[null, 3, 0]`, `[
		null,
		[[3, null], null],
		[]
	]`)
	tk.checkTake(tk.dt, listJSON, `[null, null]`, `[null, null]`)
	tk.checkTake(tk.dt, listJSON, `[3, 0, 0, 3]`, `[[[3, null], null], [], [], [[3, null], null]]`)
	tk.checkTake(tk.dt, listJSON, `[0, 1, 2, 3]`, listJSON)
	tk.checkTake(tk.dt, listJSON, `[0, 0, 0, 0, 0, 0, 1]`,
		`[[], [], [], [], [], [], [[1], [2, null, 2], []]]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `[[[1], [2, null, 2], []], [[3, null]]]`, `[0, 1, 0]`)
}

func (tk *TakeKernelLists) TestLargeListInt32() {
	tk.dt = arrow.LargeListOf(arrow.PrimitiveTypes.Int32)
	listJSON := `[[], [1, 2], null, [3]]`
	tk.checkTake(tk.dt, listJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, listJSON, `[null, 1, 2, 0]`, `[null, [1, 2], null, []]`)
}

func (tk *TakeKernelLists) TestFixedSizeListInt32() {
	tk.dt = arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32)
	listJSON := `[null, [1, null, 3], [4, 5, 6], [7, 8, null]]`
	tk.checkTake(tk.dt, listJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, listJSON, `[3, 2, 1]`, `[[7, 8, null], [4, 5, 6], [1, null, 3]]`)
	tk.checkTake(tk.dt, listJSON, `[null, 2, 0]`, `[null, [4, 5, 6], null]`)
	tk.checkTake(tk.dt, listJSON, `[null, null]`, `[null, null]`)
	tk.checkTake(tk.dt, listJSON, `[3, 0, 0, 3]`, `[[7, 8, null], null, null, [7, 8, null]]`)
	tk.checkTake(tk.dt, listJSON, `[0, 1, 2, 3]`, listJSON)
	tk.checkTake(tk.dt, listJSON, `[2, 2, 2, 2, 2, 2, 1]`,
		`[[4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [1, null, 3]]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `[[1, null, 3], [4, 5, 6], [7, 8, null]]`, `[0, 1, 0]`)
}

type TakeKernelDenseUnion struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelDenseUnion) TestTakeUnion() {
	tk.dt = arrow.DenseUnionOf([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, []arrow.UnionTypeCode{2, 5})

	unionJSON := `[
		[2, null],
		[2, 222],
		[5, "hello"],
		[5, "eh"],
		[2, null],
		[2, 111],
		[5, null]
	]`
	tk.checkTake(tk.dt, unionJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, unionJSON, `[3, 1, 3, 1, 3]`, `[
		[5, "eh"],
		[2, 222],
		[5, "eh"],
		[2, 222],
		[5, "eh"]
	]`)
	tk.checkTake(tk.dt, unionJSON, `[4, 2, 1, 6]`, `[
		[2, null],
		[5, "hello"],
		[2, 222],
		[5, null]
	]`)
	tk.checkTake(tk.dt, unionJSON, `[0, 1, 2, 3, 4, 5, 6]`, unionJSON)
	tk.checkTake(tk.dt, unionJSON, `[0, 2, 2, 2, 2, 2, 2]`, `[
		[2, null],
		[5, "hello"],
		[5, "hello"],
		[5, "hello"],
		[5, "hello"],
		[5, "hello"],
		[5, "hello"]
	]`)
}

type TakeKernelStruct struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelStruct) TestStruct() {
	tk.dt = arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true})

	structJSON := `[
		null,
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`

	tk.checkTake(tk.dt, structJSON, `[]`, `[]`)
	tk.checkTake(tk.dt, structJSON, `[3, 1, 3, 1, 3]`, `[
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		{"a": 4, "b": "eh"}
	]`)
	tk.checkTake(tk.dt, structJSON, `[3, 1, 0]`, `[
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		null
	]`)
	tk.checkTake(tk.dt, structJSON, `[0, 1, 2, 3]`, structJSON)
	tk.checkTake(tk.dt, structJSON, `[0, 2, 2, 2, 2, 2, 2]`, `[
		null,
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"}
	]`)

	tk.assertNoValidityBitmapUnknownNullCountJSON(tk.dt, `[{"a": 1}, {"a": 2, "b": "hello"}]`, `[0, 1, 0]`)
}

type TakeKernelTestChunked struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestChunked) assertTake(dt arrow.DataType, values []string, indices string, expected []string) {
	actual, err := tk.takeWithArray(dt, values, indices)
	tk.Require().NoError(err)
	defer actual.Release()

	exp, err := array.ChunkedFromJSON(tk.mem, dt, expected)
	tk.Require().NoError(err)
	defer exp.Release()

	if !tk.True(array.ChunkedEqual(exp, actual)) {
		var s strings.Builder
		s.WriteString("expected: \n")
		for _, c := range exp.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		s.WriteString("actual: \n")
		for _, c := range actual.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		tk.T().Log(s.String())
	}
}

func (tk *TakeKernelTestChunked) assertChunkedTake(dt arrow.DataType, values, indices, expected []string) {
	actual, err := tk.takeWithChunked(dt, values, indices)
	tk.Require().NoError(err)
	defer actual.Release()

	exp, err := array.ChunkedFromJSON(tk.mem, dt, expected)
	tk.Require().NoError(err)
	defer exp.Release()

	if !tk.True(array.ChunkedEqual(exp, actual)) {
		var s strings.Builder
		s.WriteString("expected: \n")
		for _, c := range exp.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		s.WriteString("actual: \n")
		for _, c := range actual.Chunks() {
			fmt.Fprintf(&s, "%s\n", c)
		}
		tk.T().Log(s.String())
	}
}

func (tk *TakeKernelTestChunked) takeWithArray(dt arrow.DataType, values []string, indices string) (*arrow.Chunked, error) {
	chunked, err := array.ChunkedFromJSON(tk.mem, dt, values)
	tk.Require().NoError(err)
	defer chunked.Release()

	indicesArr, _, err := array.FromJSON(tk.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(indices))
	tk.Require().NoError(err)
	defer indicesArr.Release()

	result, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), &compute.ChunkedDatum{chunked}, &compute.ArrayDatum{indicesArr.Data()})
	if err != nil {
		return nil, err
	}
	return result.(*compute.ChunkedDatum).Value, nil

}

func (tk *TakeKernelTestChunked) takeWithChunked(dt arrow.DataType, values, indices []string) (*arrow.Chunked, error) {
	chunked, err := array.ChunkedFromJSON(tk.mem, dt, values)
	tk.Require().NoError(err)
	defer chunked.Release()

	chunkedIndices, err := array.ChunkedFromJSON(tk.mem, arrow.PrimitiveTypes.Int8, indices)
	tk.Require().NoError(err)
	defer chunkedIndices.Release()

	result, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), &compute.ChunkedDatum{chunked}, &compute.ChunkedDatum{chunkedIndices})
	if err != nil {
		return nil, err
	}
	return result.(*compute.ChunkedDatum).Value, nil
}

func (tk *TakeKernelTestChunked) TestChunkedArray() {
	tk.assertTake(arrow.PrimitiveTypes.Int8, []string{`[]`}, `[]`, []string{`[]`})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{}, []string{}, []string{})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{}, []string{`[]`}, []string{`[]`})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{}, []string{`[null]`}, []string{`[null]`})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{`[]`}, []string{}, []string{})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{`[]`}, []string{`[]`}, []string{`[]`})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{`[]`}, []string{`[null]`}, []string{`[null]`})

	tk.assertTake(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, `[0, 1, 0, 2]`, []string{`[7, 8, 7, 9]`})
	tk.assertChunkedTake(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, []string{`[0, 1, 0]`, `[]`, `[2]`}, []string{`[7, 8, 7]`, `[]`, `[9]`})
	tk.assertTake(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, `[2, 1]`, []string{`[9, 8]`})

	tk.assertChunkedTake(arrow.FixedWidthTypes.Boolean, []string{`[true]`, `[false, true]`}, []string{`[0, 1, 0]`, `[]`, `[2]`},
		[]string{`[true, false, true]`, `[]`, `[true]`})

	tk.assertChunkedTake(arrow.PrimitiveTypes.Int32,
		[]string{`[7, null]`, `[8, 9, 10]`, `[21, null, 42]`}, []string{`[2, 1]`, `[7, 6, 6, 4]`},
		[]string{`[8, null]`, `[42, null, null, 10]`})

	tk.assertChunkedTake(arrow.BinaryTypes.String,
		[]string{`["hello", "world", null]`, `["foo", "bar", "baz"]`},
		[]string{`[3]`, `[null, 2]`, `[0, 1]`, `[4, 5]`},
		[]string{`["foo"]`, `[null, null]`, `["hello", "world"]`, `["bar", "baz"]`})

	_, err := tk.takeWithArray(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, `[0, 5]`)
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeWithChunked(arrow.PrimitiveTypes.Int8, []string{`[7]`, `[8, 9]`}, []string{`[0, 1, 0]`, `[5, 1]`})
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeWithChunked(arrow.PrimitiveTypes.Int8, []string{}, []string{`[0]`})
	tk.ErrorIs(err, arrow.ErrIndex)
	_, err = tk.takeWithChunked(arrow.PrimitiveTypes.Int8, []string{`[]`}, []string{`[0]`})
	tk.ErrorIs(err, arrow.ErrIndex)
}

type TakeKernelTestRecord struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestRecord) takeJSON(schm *arrow.Schema, batchJSON string, indexType arrow.DataType, indices string) (arrow.Record, error) {
	batch, _, err := array.RecordFromJSON(tk.mem, schm, strings.NewReader(batchJSON))
	tk.Require().NoError(err)
	defer batch.Release()
	indexArr, _, err := array.FromJSON(tk.mem, indexType, strings.NewReader(indices))
	tk.Require().NoError(err)
	defer indexArr.Release()
	result, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(),
		&compute.RecordDatum{Value: batch}, &compute.ArrayDatum{Value: indexArr.Data()})
	if err != nil {
		return nil, err
	}
	return result.(*compute.RecordDatum).Value, nil
}

func (tk *TakeKernelTestRecord) assertTake(schm *arrow.Schema, batchJSON, indices, exp string) {
	expected, _, err := array.RecordFromJSON(tk.mem, schm, strings.NewReader(exp))
	tk.Require().NoError(err)
	defer expected.Release()

	for _, idxType := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint32} {
		result, err := tk.takeJSON(schm, batchJSON, idxType, indices)
		tk.NoError(err)
		defer result.Release()
		tk.Truef(array.RecordEqual(expected, result), "expected: %s\ngot: %s", expected, result)
	}
}

func (tk *TakeKernelTestRecord) TestTakeRecordBatch() {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}

	schm := arrow.NewSchema(fields, nil)
	batchJSON := `[
		{"a": null, "b": "yo"},
		{"a": 1, "b": ""},
		{"a": 2, "b": "hello"},
		{"a": 4, "b": "eh"}
	]`

	tk.assertTake(schm, batchJSON, `[]`, `[]`)
	tk.assertTake(schm, batchJSON, `[3, 1, 3, 1, 3]`, `[
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		{"a": 4, "b": "eh"}
	]`)
	tk.assertTake(schm, batchJSON, `[3, 1, 0]`, `[
		{"a": 4, "b": "eh"},
		{"a": 1, "b": ""},
		{"a": null, "b": "yo"}
	]`)
	tk.assertTake(schm, batchJSON, `[0, 1, 2, 3]`, batchJSON)
	tk.assertTake(schm, batchJSON, `[0, 2, 2, 2, 2, 2, 2]`, `[
		{"a": null, "b": "yo"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"},
		{"a": 2, "b": "hello"}
	]`)
}

type TakeKernelTestTable struct {
	TakeKernelTestTyped
}

func (tk *TakeKernelTestTable) assertTake(schm *arrow.Schema, tableJSON []string, filter string, exptable []string) {
	tbl, err := tk.takeWithArray(schm, tableJSON, filter)
	tk.Require().NoError(err)
	defer tbl.Release()

	exptbl, err := array.TableFromJSON(tk.mem, schm, exptable)
	tk.Require().NoError(err)
	defer exptbl.Release()

	tk.Truef(array.TableEqual(exptbl, tbl), "expected: %s\ngot: %s", exptbl, tbl)
}

func (tk *TakeKernelTestTable) assertChunkedTake(schm *arrow.Schema, tableJSON, filter, expTable []string) {
	tbl, err := tk.takeWithChunked(schm, tableJSON, filter)
	tk.Require().NoError(err)
	defer tbl.Release()

	exptbl, err := array.TableFromJSON(tk.mem, schm, expTable)
	tk.Require().NoError(err)
	defer exptbl.Release()

	tk.Truef(array.TableEqual(exptbl, tbl), "expected: %s\ngot: %s", exptbl, tbl)
}

func (tk *TakeKernelTestTable) takeWithArray(schm *arrow.Schema, values []string, indices string) (arrow.Table, error) {
	tbl, err := array.TableFromJSON(tk.mem, schm, values)
	tk.NoError(err)
	defer tbl.Release()

	indicesArr, _, err := array.FromJSON(tk.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(indices))
	tk.NoError(err)
	defer indicesArr.Release()

	result, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), &compute.TableDatum{Value: tbl},
		&compute.ArrayDatum{Value: indicesArr.Data()})
	if err != nil {
		return nil, err
	}
	return result.(*compute.TableDatum).Value, nil
}

func (tk *TakeKernelTestTable) takeWithChunked(schm *arrow.Schema, values, indices []string) (arrow.Table, error) {
	tbl, err := array.TableFromJSON(tk.mem, schm, values)
	tk.NoError(err)
	defer tbl.Release()

	chunkedIndices, err := array.ChunkedFromJSON(tk.mem, arrow.PrimitiveTypes.Int8, indices)
	tk.NoError(err)
	defer chunkedIndices.Release()

	result, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), &compute.TableDatum{Value: tbl},
		&compute.ChunkedDatum{Value: chunkedIndices})
	if err != nil {
		return nil, err
	}
	return result.(*compute.TableDatum).Value, nil
}

func (tk *TakeKernelTestTable) TestTakeTable() {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schm := arrow.NewSchema(fields, nil)

	tblJSON := []string{
		`[{"a": null, "b": "yo"}, {"a": 1, "b": ""}]`,
		`[{"a": 2, "b": "hello"}, {"a": 4, "b": "eh"}]`}

	tk.assertTake(schm, tblJSON, `[]`, []string{`[]`})
	expected310 := []string{
		`[{"a": 4, "b": "eh"}, {"a": 1, "b": ""}, {"a": null, "b": "yo"}]`}

	tk.assertTake(schm, tblJSON, `[3, 1, 0]`, expected310)
	tk.assertChunkedTake(schm, tblJSON, []string{`[0, 1]`, `[2, 3]`}, tblJSON)
}

func TestTakeKernels(t *testing.T) {
	suite.Run(t, new(TakeKernelTest))
	for _, dt := range numericTypes {
		suite.Run(t, &TakeKernelTestNumeric{TakeKernelTestTyped: TakeKernelTestTyped{dt: dt}})
	}
	suite.Run(t, new(TakeKernelTestFSB))
	for _, dt := range baseBinaryTypes {
		suite.Run(t, &TakeKernelTestString{TakeKernelTestTyped: TakeKernelTestTyped{dt: dt}})
	}
	suite.Run(t, new(TakeKernelLists))
	suite.Run(t, new(TakeKernelDenseUnion))
	suite.Run(t, new(TakeKernelTestExtension))
	suite.Run(t, new(TakeKernelStruct))
	suite.Run(t, new(TakeKernelTestRecord))
	suite.Run(t, new(TakeKernelTestChunked))
	suite.Run(t, new(TakeKernelTestTable))
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
	suite.Run(t, new(FilterKernelWithList))
	suite.Run(t, new(FilterKernelWithUnion))
	suite.Run(t, new(FilterKernelExtension))
	suite.Run(t, new(FilterKernelWithStruct))
	suite.Run(t, new(FilterKernelWithRecordBatch))
	suite.Run(t, new(FilterKernelWithChunked))
	suite.Run(t, new(FilterKernelWithTable))
}

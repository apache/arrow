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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/compute/exec"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/constraints"
)

func checkUniqueDict[I exec.IntTypes | exec.UintTypes](t *testing.T, input compute.ArrayLikeDatum, expected arrow.Array) {
	out, err := compute.Unique(context.TODO(), input)
	require.NoError(t, err)
	defer out.Release()

	result := out.(*compute.ArrayDatum).MakeArray().(*array.Dictionary)
	defer result.Release()

	require.Truef(t, arrow.TypeEqual(result.DataType(), expected.DataType()),
		"wanted: %s\ngot: %s", expected.DataType(), result.DataType())

	exDict := expected.(*array.Dictionary).Dictionary()
	resultDict := result.Dictionary()

	require.Truef(t, array.Equal(exDict, resultDict), "wanted: %s\ngot: %s", exDict, resultDict)

	want := exec.GetValues[I](expected.(*array.Dictionary).Indices().Data(), 1)
	got := exec.GetValues[I](result.Indices().Data(), 1)
	assert.ElementsMatchf(t, got, want, "wanted: %s\ngot: %s", want, got)
}

func checkDictionaryUnique(t *testing.T, input compute.ArrayLikeDatum, expected arrow.Array) {
	require.Truef(t, arrow.TypeEqual(input.Type(), expected.DataType()),
		"wanted: %s\ngot: %s", expected.DataType(), input.Type())

	switch input.Type().(*arrow.DictionaryType).IndexType.ID() {
	case arrow.INT8:
		checkUniqueDict[int8](t, input, expected)
	case arrow.INT16:
		checkUniqueDict[int16](t, input, expected)
	case arrow.INT32:
		checkUniqueDict[int32](t, input, expected)
	case arrow.INT64:
		checkUniqueDict[int64](t, input, expected)
	case arrow.UINT8:
		checkUniqueDict[uint8](t, input, expected)
	case arrow.UINT16:
		checkUniqueDict[uint16](t, input, expected)
	case arrow.UINT32:
		checkUniqueDict[uint32](t, input, expected)
	case arrow.UINT64:
		checkUniqueDict[uint64](t, input, expected)
	}
}

func checkUniqueFixedWidth[T exec.FixedWidthTypes](t *testing.T, input, expected arrow.Array) {
	result, err := compute.UniqueArray(context.TODO(), input)
	require.NoError(t, err)
	defer result.Release()

	require.Truef(t, arrow.TypeEqual(result.DataType(), expected.DataType()),
		"wanted: %s\ngot: %s", expected.DataType(), result.DataType())
	want := exec.GetValues[T](expected.Data(), 1)
	got := exec.GetValues[T](expected.Data(), 1)

	assert.ElementsMatchf(t, got, want, "wanted: %s\ngot: %s", want, got)
}

func checkUniqueVariableWidth[OffsetType int32 | int64](t *testing.T, input, expected arrow.Array) {
	result, err := compute.UniqueArray(context.TODO(), input)
	require.NoError(t, err)
	defer result.Release()

	require.Truef(t, arrow.TypeEqual(result.DataType(), expected.DataType()),
		"wanted: %s\ngot: %s", expected.DataType(), result.DataType())

	require.EqualValues(t, expected.Len(), result.Len())

	createSlice := func(v arrow.Array) [][]byte {
		var (
			offsets = exec.GetOffsets[OffsetType](v.Data(), 1)
			data    = v.Data().Buffers()[2].Bytes()
			out     = make([][]byte, v.Len())
		)

		for i := 0; i < v.Len(); i++ {
			out[i] = data[offsets[i]:offsets[i+1]]
		}
		return out
	}

	want := createSlice(expected)
	got := createSlice(result)

	assert.ElementsMatch(t, want, got)
}

type ArrowType interface {
	exec.FixedWidthTypes | string | []byte
}

type builder[T ArrowType] interface {
	AppendValues([]T, []bool)
}

func makeArray[T ArrowType](mem memory.Allocator, dt arrow.DataType, values []T, isValid []bool) arrow.Array {
	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()

	bldr.(builder[T]).AppendValues(values, isValid)
	return bldr.NewArray()
}

func checkUniqueFixedSizeBinary(t *testing.T, mem memory.Allocator, dt *arrow.FixedSizeBinaryType, inValues, outValues [][]byte, inValid, outValid []bool) {
	input := makeArray(mem, dt, inValues, inValid)
	defer input.Release()
	expected := makeArray(mem, dt, outValues, outValid)
	defer expected.Release()

	result, err := compute.UniqueArray(context.TODO(), input)
	require.NoError(t, err)
	defer result.Release()

	require.Truef(t, arrow.TypeEqual(result.DataType(), expected.DataType()),
		"wanted: %s\ngot: %s", expected.DataType(), result.DataType())

	slice := func(v arrow.Array) [][]byte {
		data := v.Data().Buffers()[1].Bytes()
		out := make([][]byte, v.Len())
		for i := range out {
			out[i] = data[i*dt.ByteWidth : (i+1)*dt.ByteWidth]
		}
		return out
	}

	want := slice(expected)
	got := slice(result)
	assert.ElementsMatch(t, want, got)
}

func checkUniqueFW[T exec.FixedWidthTypes](t *testing.T, mem memory.Allocator, dt arrow.DataType, inValues, outValues []T, inValid, outValid []bool) {
	input := makeArray(mem, dt, inValues, inValid)
	defer input.Release()
	expected := makeArray(mem, dt, outValues, outValid)
	defer expected.Release()

	checkUniqueFixedWidth[T](t, input, expected)
}

func checkUniqueVW[T string | []byte](t *testing.T, mem memory.Allocator, dt arrow.DataType, inValues, outValues []T, inValid, outValid []bool) {
	input := makeArray(mem, dt, inValues, inValid)
	defer input.Release()
	expected := makeArray(mem, dt, outValues, outValid)
	defer expected.Release()

	switch dt.(arrow.BinaryDataType).Layout().Buffers[1].ByteWidth {
	case 4:
		checkUniqueVariableWidth[int32](t, input, expected)
	case 8:
		checkUniqueVariableWidth[int64](t, input, expected)
	}
}

type PrimitiveHashKernelSuite[T exec.IntTypes | exec.UintTypes | constraints.Float] struct {
	suite.Suite

	mem *memory.CheckedAllocator
	dt  arrow.DataType
}

func (ps *PrimitiveHashKernelSuite[T]) SetupSuite() {
	ps.dt = exec.GetDataType[T]()
}

func (ps *PrimitiveHashKernelSuite[T]) SetupTest() {
	ps.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (ps *PrimitiveHashKernelSuite[T]) TearDownTest() {
	ps.mem.AssertSize(ps.T(), 0)
}

func (ps *PrimitiveHashKernelSuite[T]) TestUnique() {
	ps.Run(ps.dt.String(), func() {
		if ps.dt.ID() == arrow.DATE64 {
			checkUniqueFW(ps.T(), ps.mem, ps.dt,
				[]arrow.Date64{172800000, 864000000, 172800000, 864000000},
				[]arrow.Date64{172800000, 0, 864000000},
				[]bool{true, false, true, true}, []bool{true, false, true})

			checkUniqueFW(ps.T(), ps.mem, ps.dt,
				[]arrow.Date64{172800000, 864000000, 259200000, 864000000},
				[]arrow.Date64{0, 259200000, 864000000},
				[]bool{false, false, true, true}, []bool{false, true, true})

			arr, _, err := array.FromJSON(ps.mem, ps.dt, strings.NewReader(`[86400000, 172800000, null, 259200000, 172800000, null]`))
			ps.Require().NoError(err)
			defer arr.Release()
			input := array.NewSlice(arr, 1, 5)
			defer input.Release()
			expected, _, err := array.FromJSON(ps.mem, ps.dt, strings.NewReader(`[172800000, null, 259200000]`))
			ps.Require().NoError(err)
			defer expected.Release()
			checkUniqueFixedWidth[arrow.Date64](ps.T(), input, expected)
			return
		}

		checkUniqueFW(ps.T(), ps.mem, ps.dt,
			[]T{2, 1, 2, 1}, []T{2, 0, 1},
			[]bool{true, false, true, true}, []bool{true, false, true})
		checkUniqueFW(ps.T(), ps.mem, ps.dt,
			[]T{2, 1, 3, 1}, []T{0, 3, 1},
			[]bool{false, false, true, true}, []bool{false, true, true})

		arr, _, err := array.FromJSON(ps.mem, ps.dt, strings.NewReader(`[1, 2, null, 3, 2, null]`))
		ps.Require().NoError(err)
		defer arr.Release()
		input := array.NewSlice(arr, 1, 5)
		defer input.Release()

		expected, _, err := array.FromJSON(ps.mem, ps.dt, strings.NewReader(`[2, null, 3]`))
		ps.Require().NoError(err)
		defer expected.Release()

		checkUniqueFixedWidth[T](ps.T(), input, expected)
	})
}

type BinaryTypeHashKernelSuite[T string | []byte] struct {
	suite.Suite

	mem *memory.CheckedAllocator
	dt  arrow.DataType
}

func (ps *BinaryTypeHashKernelSuite[T]) SetupTest() {
	ps.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (ps *BinaryTypeHashKernelSuite[T]) TearDownTest() {
	ps.mem.AssertSize(ps.T(), 0)
}

func (ps *BinaryTypeHashKernelSuite[T]) TestUnique() {
	ps.Run(ps.dt.String(), func() {
		checkUniqueVW(ps.T(), ps.mem, ps.dt,
			[]T{T("test"), T(""), T("test2"), T("test")}, []T{T("test"), T(""), T("test2")},
			[]bool{true, false, true, true}, []bool{true, false, true})
	})
}

func TestHashKernels(t *testing.T) {
	suite.Run(t, &PrimitiveHashKernelSuite[int8]{})
	suite.Run(t, &PrimitiveHashKernelSuite[uint8]{})
	suite.Run(t, &PrimitiveHashKernelSuite[int16]{})
	suite.Run(t, &PrimitiveHashKernelSuite[uint16]{})
	suite.Run(t, &PrimitiveHashKernelSuite[int32]{})
	suite.Run(t, &PrimitiveHashKernelSuite[uint32]{})
	suite.Run(t, &PrimitiveHashKernelSuite[int64]{})
	suite.Run(t, &PrimitiveHashKernelSuite[uint64]{})
	suite.Run(t, &PrimitiveHashKernelSuite[float32]{})
	suite.Run(t, &PrimitiveHashKernelSuite[float64]{})
	suite.Run(t, &PrimitiveHashKernelSuite[arrow.Date32]{})
	suite.Run(t, &PrimitiveHashKernelSuite[arrow.Date64]{})

	suite.Run(t, &BinaryTypeHashKernelSuite[string]{dt: arrow.BinaryTypes.String})
	suite.Run(t, &BinaryTypeHashKernelSuite[string]{dt: arrow.BinaryTypes.LargeString})
	suite.Run(t, &BinaryTypeHashKernelSuite[[]byte]{dt: arrow.BinaryTypes.Binary})
	suite.Run(t, &BinaryTypeHashKernelSuite[[]byte]{dt: arrow.BinaryTypes.LargeBinary})
}

func TestUniqueTimeTimestamp(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.Time32s,
		[]arrow.Time32{2, 1, 2, 1}, []arrow.Time32{2, 0, 1},
		[]bool{true, false, true, true}, []bool{true, false, true})

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.Time64ns,
		[]arrow.Time64{2, 1, 2, 1}, []arrow.Time64{2, 0, 1},
		[]bool{true, false, true, true}, []bool{true, false, true})

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.Timestamp_ns,
		[]arrow.Timestamp{2, 1, 2, 1}, []arrow.Timestamp{2, 0, 1},
		[]bool{true, false, true, true}, []bool{true, false, true})

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.Duration_ns,
		[]arrow.Duration{2, 1, 2, 1}, []arrow.Duration{2, 0, 1},
		[]bool{true, false, true, true}, []bool{true, false, true})
}

func TestUniqueFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	dt := &arrow.FixedSizeBinaryType{ByteWidth: 3}
	checkUniqueFixedSizeBinary(t, mem, dt,
		[][]byte{[]byte("aaa"), nil, []byte("bbb"), []byte("aaa")},
		[][]byte{[]byte("aaa"), nil, []byte("bbb")},
		[]bool{true, false, true, true}, []bool{true, false, true})
}

func TestUniqueDecimal(t *testing.T) {
	t.Run("decimal128", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		values := []decimal128.Num{
			decimal128.FromI64(12),
			decimal128.FromI64(12),
			decimal128.FromI64(11),
			decimal128.FromI64(12)}
		expected := []decimal128.Num{
			decimal128.FromI64(12),
			decimal128.FromI64(0),
			decimal128.FromI64(11)}

		checkUniqueFW(t, mem, &arrow.Decimal128Type{Precision: 2, Scale: 0},
			values, expected, []bool{true, false, true, true}, []bool{true, false, true})
	})

	t.Run("decimal256", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)

		values := []decimal256.Num{
			decimal256.FromI64(12),
			decimal256.FromI64(12),
			decimal256.FromI64(11),
			decimal256.FromI64(12)}
		expected := []decimal256.Num{
			decimal256.FromI64(12),
			decimal256.FromI64(0),
			decimal256.FromI64(11)}

		checkUniqueFW(t, mem, &arrow.Decimal256Type{Precision: 2, Scale: 0},
			values, expected, []bool{true, false, true, true}, []bool{true, false, true})
	})
}

func TestUniqueIntervalMonth(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.MonthInterval,
		[]arrow.MonthInterval{2, 1, 2, 1}, []arrow.MonthInterval{2, 0, 1},
		[]bool{true, false, true, true}, []bool{true, false, true})

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.DayTimeInterval,
		[]arrow.DayTimeInterval{
			{Days: 2, Milliseconds: 1}, {Days: 3, Milliseconds: 2},
			{Days: 2, Milliseconds: 1}, {Days: 1, Milliseconds: 2}},
		[]arrow.DayTimeInterval{{Days: 2, Milliseconds: 1},
			{Days: 1, Milliseconds: 1}, {Days: 1, Milliseconds: 2}},
		[]bool{true, false, true, true}, []bool{true, false, true})

	checkUniqueFW(t, mem, arrow.FixedWidthTypes.MonthDayNanoInterval,
		[]arrow.MonthDayNanoInterval{
			{Months: 2, Days: 1, Nanoseconds: 1},
			{Months: 3, Days: 2, Nanoseconds: 1},
			{Months: 2, Days: 1, Nanoseconds: 1},
			{Months: 1, Days: 2, Nanoseconds: 1}},
		[]arrow.MonthDayNanoInterval{
			{Months: 2, Days: 1, Nanoseconds: 1},
			{Months: 1, Days: 1, Nanoseconds: 1},
			{Months: 1, Days: 2, Nanoseconds: 1}},
		[]bool{true, false, true, true}, []bool{true, false, true})
}

func TestUniqueChunkedArrayInvoke(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var (
		values1    = []string{"foo", "bar", "foo"}
		values2    = []string{"bar", "baz", "quuux", "foo"}
		dictValues = []string{"foo", "bar", "baz", "quuux"}
		typ        = arrow.BinaryTypes.String
		a1         = makeArray(mem, typ, values1, nil)
		a2         = makeArray(mem, typ, values2, nil)
		exDict     = makeArray(mem, typ, dictValues, nil)
	)

	defer a1.Release()
	defer a2.Release()
	defer exDict.Release()

	carr := arrow.NewChunked(typ, []arrow.Array{a1, a2})
	defer carr.Release()

	result, err := compute.Unique(context.TODO(), &compute.ChunkedDatum{Value: carr})
	require.NoError(t, err)
	defer result.Release()

	require.Equal(t, compute.KindArray, result.Kind())
	out := result.(*compute.ArrayDatum).MakeArray()
	defer out.Release()

	assertArraysEqual(t, exDict, out)
}

func TestDictionaryUnique(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const dictJSON = `[10, 20, 30, 40]`
	dict, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(dictJSON))
	require.NoError(t, err)
	defer dict.Release()

	for _, idxTyp := range integerTypes {
		t.Run("index_type="+idxTyp.Name(), func(t *testing.T) {
			scope := memory.NewCheckedAllocatorScope(mem)
			defer scope.CheckSize(t)

			indices, _, _ := array.FromJSON(mem, idxTyp, strings.NewReader(`[3, 0, 0, 0, 1, 1, 3, 0, 1, 3, 0, 1]`))
			defer indices.Release()
			dictType := &arrow.DictionaryType{
				IndexType: idxTyp, ValueType: arrow.PrimitiveTypes.Int64}
			exIndices, _, _ := array.FromJSON(mem, idxTyp, strings.NewReader(`[3, 0, 1]`))
			defer exIndices.Release()

			input := array.NewDictionaryArray(dictType, indices, dict)
			defer input.Release()
			exUniques := array.NewDictionaryArray(dictType, exIndices, dict)
			defer exUniques.Release()

			checkDictionaryUnique(t, &compute.ArrayDatum{Value: input.Data()}, exUniques)

			t.Run("empty array", func(t *testing.T) {
				scope := memory.NewCheckedAllocatorScope(mem)
				defer scope.CheckSize(t)

				// executor never gives the kernel any batches
				// so result dictionary is empty
				emptyInput, _ := array.DictArrayFromJSON(mem, dictType, `[]`, dictJSON)
				defer emptyInput.Release()
				exEmpty, _ := array.DictArrayFromJSON(mem, dictType, `[]`, `[]`)
				defer exEmpty.Release()
				checkDictionaryUnique(t, &compute.ArrayDatum{Value: emptyInput.Data()}, exEmpty)
			})

			t.Run("different chunk dictionaries", func(t *testing.T) {
				scope := memory.NewCheckedAllocatorScope(mem)
				defer scope.CheckSize(t)

				input2, _ := array.DictArrayFromJSON(mem, dictType, `[1, null, 2, 3]`, `[30, 40, 50, 60]`)
				defer input2.Release()

				diffCarr := arrow.NewChunked(dictType, []arrow.Array{input, input2})
				defer diffCarr.Release()

				exUnique2, _ := array.DictArrayFromJSON(mem, dictType, `[3, 0, 1, null, 4, 5]`, `[10, 20, 30, 40, 50, 60]`)
				defer exUnique2.Release()

				checkDictionaryUnique(t, &compute.ChunkedDatum{Value: diffCarr}, exUnique2)
			})

			t.Run("encoded nulls", func(t *testing.T) {
				scope := memory.NewCheckedAllocatorScope(mem)
				defer scope.CheckSize(t)

				dictWithNull, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[10, null, 30, 40]`))
				defer dictWithNull.Release()
				input := array.NewDictionaryArray(dictType, indices, dictWithNull)
				defer input.Release()
				exUniques := array.NewDictionaryArray(dictType, exIndices, dictWithNull)
				defer exUniques.Release()
				checkDictionaryUnique(t, &compute.ArrayDatum{Value: input.Data()}, exUniques)
			})

			t.Run("masked nulls", func(t *testing.T) {
				scope := memory.NewCheckedAllocatorScope(mem)
				defer scope.CheckSize(t)

				indicesWithNull, _, _ := array.FromJSON(mem, idxTyp, strings.NewReader(`[3, 0, 0, 0, null, null, 3, 0, null, 3, 0, null]`))
				defer indicesWithNull.Release()
				exIndicesWithNull, _, _ := array.FromJSON(mem, idxTyp, strings.NewReader(`[3, 0, null]`))
				defer exIndicesWithNull.Release()
				exUniques := array.NewDictionaryArray(dictType, exIndicesWithNull, dict)
				defer exUniques.Release()
				input := array.NewDictionaryArray(dictType, indicesWithNull, dict)
				defer input.Release()

				checkDictionaryUnique(t, &compute.ArrayDatum{Value: input.Data()}, exUniques)
			})
		})
	}
}

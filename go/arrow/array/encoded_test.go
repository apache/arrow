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

package array_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	stringValues, _, _ = array.FromJSON(memory.DefaultAllocator, arrow.BinaryTypes.String, strings.NewReader(`["Hello", "World", null]`))
	int32Values, _, _  = array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[10, 20, 30]`))
	int32OnlyNull      = array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, 3)
)

func TestMakeRLEArray(t *testing.T) {
	rleArr := array.NewRunEndEncodedArray(int32Values, stringValues, 3, 0)
	defer rleArr.Release()

	arrData := rleArr.Data()
	newArr := array.MakeFromData(arrData)
	defer newArr.Release()

	assert.Same(t, newArr.Data(), arrData)
	assert.IsType(t, (*array.RunEndEncoded)(nil), newArr)
}

func TestRLEFromRunEndsAndValues(t *testing.T) {
	rleArray := array.NewRunEndEncodedArray(int32Values, int32Values, 3, 0)
	defer rleArray.Release()

	assert.EqualValues(t, 3, rleArray.Len())
	assert.Truef(t, array.Equal(int32Values, rleArray.Values()), "expected: %s\ngot: %s", int32Values, rleArray.Values())
	assert.Truef(t, array.Equal(int32Values, rleArray.RunEndsArr()), "expected: %s\ngot: %s", int32Values, rleArray.RunEndsArr())
	assert.Zero(t, rleArray.Offset())
	assert.Zero(t, rleArray.Data().NullN())
	// one dummy buffer, since code may assume there's at least one nil buffer
	assert.Len(t, rleArray.Data().Buffers(), 1)

	// explicit offset
	rleArray = array.NewRunEndEncodedArray(int32Values, stringValues, 2, 1)
	defer rleArray.Release()

	assert.EqualValues(t, 2, rleArray.Len())
	assert.Truef(t, array.Equal(stringValues, rleArray.Values()), "expected: %s\ngot: %s", stringValues, rleArray.Values())
	assert.Truef(t, array.Equal(int32Values, rleArray.RunEndsArr()), "expected: %s\ngot: %s", int32Values, rleArray.RunEndsArr())
	assert.EqualValues(t, 1, rleArray.Offset())
	assert.Zero(t, rleArray.Data().NullN())

	assert.PanicsWithError(t, "invalid: arrow/array: run ends array must be int16, int32, or int64", func() {
		array.NewRunEndEncodedArray(stringValues, int32Values, 3, 0)
	})
	assert.PanicsWithError(t, "invalid: arrow/array: run ends array cannot contain nulls", func() {
		array.NewRunEndEncodedArray(int32OnlyNull, int32Values, 3, 0)
	})
}

func TestRunLengthEncodedOffsetLength(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	runEnds, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[100, 200, 300, 400, 500]`))
	defer runEnds.Release()

	values, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["Hello", "beautiful", "world", "of", "RLE"]`))
	defer values.Release()

	rleArray := array.NewRunEndEncodedArray(runEnds, values, 500, 0)
	defer rleArray.Release()

	assert.EqualValues(t, 5, rleArray.GetPhysicalLength())
	assert.EqualValues(t, 0, rleArray.GetPhysicalOffset())

	slice := array.NewSlice(rleArray, 199, 204).(*array.RunEndEncoded)
	defer slice.Release()

	assert.EqualValues(t, 2, slice.GetPhysicalLength())
	assert.EqualValues(t, 1, slice.GetPhysicalOffset())

	slice2 := array.NewSlice(rleArray, 199, 300).(*array.RunEndEncoded)
	defer slice2.Release()

	assert.EqualValues(t, 2, slice2.GetPhysicalLength())
	assert.EqualValues(t, 1, slice2.GetPhysicalOffset())

	slice3 := array.NewSlice(rleArray, 400, 500).(*array.RunEndEncoded)
	defer slice3.Release()

	assert.EqualValues(t, 1, slice3.GetPhysicalLength())
	assert.EqualValues(t, 4, slice3.GetPhysicalOffset())

	slice4 := array.NewSlice(rleArray, 0, 150).(*array.RunEndEncoded)
	defer slice4.Release()

	assert.EqualValues(t, 2, slice4.GetPhysicalLength())
	assert.EqualValues(t, 0, slice4.GetPhysicalOffset())

	zeroLengthAtEnd := array.NewSlice(rleArray, 500, 500).(*array.RunEndEncoded)
	defer zeroLengthAtEnd.Release()

	assert.EqualValues(t, 0, zeroLengthAtEnd.GetPhysicalLength())
	assert.EqualValues(t, 5, zeroLengthAtEnd.GetPhysicalOffset())
}

func TestRLECompare(t *testing.T) {
	rleArray := array.NewRunEndEncodedArray(int32Values, stringValues, 30, 0)
	// second that is a copy of the first
	standardEquals := array.MakeFromData(rleArray.Data().(*array.Data).Copy())

	defer rleArray.Release()
	defer standardEquals.Release()

	assert.Truef(t, array.Equal(rleArray, standardEquals), "left: %s\nright: %s", rleArray, standardEquals)
	assert.False(t, array.Equal(array.NewSlice(rleArray, 0, 29), array.NewSlice(rleArray, 1, 30)))

	// array that is logically the same as our rleArray, but has 2 small
	// runs for the first value instead of one large run
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("logical duplicate", func(t *testing.T) {
		dupRunEnds, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[5, 10, 20, 30]`))
		defer dupRunEnds.Release()
		strValues, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String,
			strings.NewReader(`["Hello", "Hello", "World", null]`))
		defer strValues.Release()

		dupArr := array.NewRunEndEncodedArray(dupRunEnds, strValues, 30, 0)
		defer dupArr.Release()

		assert.Truef(t, array.Equal(rleArray, dupArr), "expected: %sgot: %s", rleArray, dupArr)
	})

	t.Run("emptyArr", func(t *testing.T) {
		emptyRuns, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[]`))
		emptyVals, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[]`))
		defer emptyRuns.Release()
		defer emptyVals.Release()

		emptyArr := array.NewRunEndEncodedArray(emptyRuns, emptyVals, 0, 0)
		defer emptyArr.Release()

		dataCopy := emptyArr.Data().(*array.Data).Copy()
		defer dataCopy.Release()
		emptyArr2 := array.MakeFromData(dataCopy)
		defer emptyArr2.Release()

		assert.Truef(t, array.Equal(emptyArr, emptyArr2), "expected: %sgot: %s", emptyArr, emptyArr2)
	})

	t.Run("different offsets", func(t *testing.T) {
		// three different slices that have the value [3, 3, 3, 4, 4, 4, 4]
		offsetsa, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
			strings.NewReader(`[2, 5, 12, 58, 60]`))
		offsetsb, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
			strings.NewReader(`[81, 86, 99, 100]`))
		offsetsc, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
			strings.NewReader(`[3, 7]`))
		valsa, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64,
			strings.NewReader(`[1, 2, 3, 4, 5]`))
		valsb, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64,
			strings.NewReader(`[2, 3, 4, 5]`))
		valsc, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64,
			strings.NewReader(`[3, 4]`))
		defer func() {
			offsetsa.Release()
			offsetsb.Release()
			offsetsc.Release()
			valsa.Release()
			valsb.Release()
			valsc.Release()
		}()

		differentOffsetsA := array.NewRunEndEncodedArray(offsetsa, valsa, 60, 0)
		defer differentOffsetsA.Release()
		differentOffsetsB := array.NewRunEndEncodedArray(offsetsb, valsb, 100, 0)
		defer differentOffsetsB.Release()
		differentOffsetsC := array.NewRunEndEncodedArray(offsetsc, valsc, 7, 0)
		defer differentOffsetsC.Release()

		sliceA := array.NewSlice(differentOffsetsA, 9, 16)
		defer sliceA.Release()
		sliceB := array.NewSlice(differentOffsetsB, 83, 90)
		defer sliceB.Release()

		assert.True(t, array.Equal(sliceA, sliceB))
		assert.True(t, array.Equal(sliceA, differentOffsetsC))
		assert.True(t, array.Equal(sliceB, differentOffsetsC))
	})
}

func TestRunEndEncodedBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewBuilder(mem, arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String))
	defer bldr.Release()

	assert.IsType(t, (*array.RunEndEncodedBuilder)(nil), bldr)
	reeBldr := bldr.(*array.RunEndEncodedBuilder)

	valBldr := reeBldr.ValueBuilder().(*array.StringBuilder)

	reeBldr.Append(100)
	valBldr.Append("Hello")
	reeBldr.Append(100)
	valBldr.Append("beautiful")
	reeBldr.Append(50)
	valBldr.Append("world")
	reeBldr.ContinueRun(50)
	reeBldr.Append(100)
	valBldr.Append("of")
	reeBldr.Append(100)
	valBldr.Append("RLE")
	reeBldr.AppendNull()

	rleArray := reeBldr.NewRunEndEncodedArray()
	defer rleArray.Release()

	assert.EqualValues(t, 501, rleArray.Len())
	assert.EqualValues(t, 6, rleArray.GetPhysicalLength())
	assert.Equal(t, arrow.INT16, rleArray.RunEndsArr().DataType().ID())
	assert.Equal(t, []int16{100, 200, 300, 400, 500, 501}, rleArray.RunEndsArr().(*array.Int16).Int16Values())

	strValues := rleArray.Values().(*array.String)
	assert.Equal(t, "Hello", strValues.Value(0))
	assert.Equal(t, "beautiful", strValues.Value(1))
	assert.Equal(t, "world", strValues.Value(2))
	assert.Equal(t, "of", strValues.Value(3))
	assert.Equal(t, "RLE", strValues.Value(4))
	assert.True(t, strValues.IsNull(5))
	assert.Equal(t, "Hello", strValues.ValueStr(0))
}

func TestRunEndEncodedStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	defer b.Release()

	valBldr := b.ValueBuilder().(*array.StringBuilder)

	b.Append(100)
	valBldr.Append("Hello")
	b.Append(100)
	valBldr.Append("beautiful")
	b.Append(50)
	valBldr.Append("world")
	b.ContinueRun(50)
	b.Append(100)
	valBldr.Append("of")
	b.Append(100)
	valBldr.Append("RLE")
	b.AppendNull()

	arr := b.NewArray().(*array.RunEndEncoded)
	defer arr.Release()
	logical := arr.LogicalValuesArray()
	defer logical.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.RunEndEncoded)
	defer arr1.Release()
	logical1 := arr1.LogicalValuesArray()
	defer logical1.Release()

	assert.True(t, array.Equal(arr, arr1))
	assert.True(t, array.Equal(logical, logical1))
}

func TestREEBuilderOverflow(t *testing.T) {
	for _, typ := range []arrow.DataType{arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64} {
		t.Run("run_ends="+typ.String(), func(t *testing.T) {

			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			bldr := array.NewRunEndEncodedBuilder(mem, typ, arrow.BinaryTypes.String)
			defer bldr.Release()

			valBldr := bldr.ValueBuilder().(*array.StringBuilder)
			assert.Panics(t, func() {
				valBldr.Append("Foo")

				maxVal := uint64(1<<typ.(arrow.FixedWidthDataType).BitWidth()) - 1

				bldr.Append(uint64(maxVal))
			})
		})
	}
}

func TestLogicalRunEndsValuesArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	defer bldr.Release()

	valBldr := bldr.ValueBuilder().(*array.StringBuilder)
	// produces run-ends 1, 2, 4, 6, 10, 1000, 1750, 2000
	bldr.AppendRuns([]uint64{1, 1, 2, 2, 4, 990, 750, 250})
	valBldr.AppendValues([]string{"a", "b", "c", "d", "e", "f", "g", "h"}, nil)

	arr := bldr.NewRunEndEncodedArray()
	defer arr.Release()

	sl := array.NewSlice(arr, 150, 1650)
	defer sl.Release()

	assert.EqualValues(t, 150, sl.Data().Offset())
	assert.EqualValues(t, 1500, sl.Len())

	logicalValues := sl.(*array.RunEndEncoded).LogicalValuesArray()
	defer logicalValues.Release()
	logicalRunEnds := sl.(*array.RunEndEncoded).LogicalRunEndsArray(mem)
	defer logicalRunEnds.Release()

	expectedValues, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["f", "g"]`))
	require.NoError(t, err)
	defer expectedValues.Release()
	expectedRunEnds := []int16{850, 1500}

	assert.Truef(t, array.Equal(logicalValues, expectedValues), "expected: %s\ngot: %s", expectedValues, logicalValues)
	assert.Equal(t, expectedRunEnds, logicalRunEnds.(*array.Int16).Int16Values())
}

func TestLogicalRunEndsValuesArrayEmpty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	defer bldr.Release()

	valBldr := bldr.ValueBuilder().(*array.StringBuilder)
	// produces run-ends 1, 2, 4, 6, 10, 1000, 1750, 2000
	bldr.AppendRuns([]uint64{1, 1, 2, 2, 4, 990, 750, 250})
	valBldr.AppendValues([]string{"a", "b", "c", "d", "e", "f", "g", "h"}, nil)

	arr := bldr.NewRunEndEncodedArray()
	defer arr.Release()

	emptySlice := array.NewSlice(arr, 2000, 2000)
	defer emptySlice.Release()

	assert.EqualValues(t, 2000, emptySlice.Data().Offset())
	assert.EqualValues(t, 0, emptySlice.Len())

	logicalValues := emptySlice.(*array.RunEndEncoded).LogicalValuesArray()
	defer logicalValues.Release()
	logicalRunEnds := emptySlice.(*array.RunEndEncoded).LogicalRunEndsArray(mem)
	defer logicalRunEnds.Release()

	assert.Zero(t, logicalValues.Len())
	assert.Zero(t, logicalRunEnds.Len())

	empty := bldr.NewRunEndEncodedArray()
	defer empty.Release()

	assert.EqualValues(t, 0, empty.Data().Offset())
	assert.EqualValues(t, 0, empty.Len())

	logicalValues = empty.LogicalValuesArray()
	defer logicalValues.Release()
	logicalRunEnds = empty.LogicalRunEndsArray(mem)
	defer logicalRunEnds.Release()

	assert.Zero(t, logicalValues.Len())
	assert.Zero(t, logicalRunEnds.Len())
}

func TestRunEndEncodedUnmarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	defer bldr.Release()

	const testJSON = `
		[ null, "a", "a", "a", "b", "b", "b", null, null, "c", "d", "d", "d", null, null, null, "e", "e"]`

	require.NoError(t, json.Unmarshal([]byte(testJSON), bldr))
	arr := bldr.NewRunEndEncodedArray()
	defer arr.Release()

	expectedValues, _, err := array.FromJSON(mem, arrow.BinaryTypes.String,
		strings.NewReader(`[null, "a", "b", null, "c", "d", null, "e"]`))
	require.NoError(t, err)
	defer expectedValues.Release()

	assert.EqualValues(t, 18, arr.Len())
	assert.Equal(t, []int16{1, 4, 7, 9, 10, 13, 16, 18}, arr.RunEndsArr().(*array.Int16).Int16Values())
	logicalValues := arr.LogicalValuesArray()
	defer logicalValues.Release()

	assert.Truef(t, array.Equal(logicalValues, expectedValues), "expected: %s\ngot: %s", expectedValues, logicalValues)
}

func TestRunEndEncodedUnmarshalNestedJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewRunEndEncodedBuilder(mem, arrow.PrimitiveTypes.Int16,
		arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer bldr.Release()

	const testJSON = `
		[null, [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, null, 3], [4, 5, null], null, null, 
		[4, 5, null], [4, 5, null], [4, 5, null]]
	`

	require.NoError(t, json.Unmarshal([]byte(testJSON), bldr))
	arr := bldr.NewRunEndEncodedArray()
	defer arr.Release()

	assert.EqualValues(t, 11, arr.Len())
	assert.Equal(t, []int16{1, 4, 5, 6, 8, 11}, arr.RunEndsArr().(*array.Int16).Int16Values())

	expectedValues, _, err := array.FromJSON(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32),
		strings.NewReader(`[null, [1, 2, 3], [1, null, 3], [4, 5, null], null, [4, 5, null]]`))
	require.NoError(t, err)
	defer expectedValues.Release()

	logicalValues := arr.LogicalValuesArray()
	defer logicalValues.Release()

	assert.Truef(t, array.Equal(logicalValues, expectedValues), "expected: %s\ngot: %s", expectedValues, logicalValues)
}

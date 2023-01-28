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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/assert"
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

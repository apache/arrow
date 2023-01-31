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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/constraints"
)

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
			offsets = exec.GetValues[OffsetType](v.Data(), 1)
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
	checkUniqueVW(ps.T(), ps.mem, ps.dt,
		[]T{T("test"), T(""), T("test2"), T("test")}, []T{T("test"), T(""), T("test2")},
		[]bool{true, false, true, true}, []bool{true, false, true})
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

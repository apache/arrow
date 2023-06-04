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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func uint8ArrFromSlice(ids ...uint8) arrow.Array {
	data := array.NewData(arrow.PrimitiveTypes.Uint8, len(ids),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Uint8Traits.CastToBytes(ids))}, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func int32ArrFromSlice(offsets ...int32) arrow.Array {
	data := array.NewData(arrow.PrimitiveTypes.Int32, len(offsets),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))}, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func TestUnionSliceEquals(t *testing.T) {
	unionFields := []arrow.Field{
		{Name: "u0", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "u1", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
	}

	typeCodes := []arrow.UnionTypeCode{5, 10}
	sparseType := arrow.SparseUnionOf(unionFields, typeCodes)
	denseType := arrow.DenseUnionOf(unionFields, typeCodes)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sparse", Type: sparseType, Nullable: true},
		{Name: "dense", Type: denseType, Nullable: true},
	}, nil)

	sparseChildren := make([]arrow.Array, 2)
	denseChildren := make([]arrow.Array, 2)

	const length = 7

	typeIDsBuffer := memory.NewBufferBytes(arrow.Uint8Traits.CastToBytes([]uint8{5, 10, 5, 5, 10, 10, 5}))
	sparseChildren[0] = int32ArrFromSlice(0, 1, 2, 3, 4, 5, 6)
	defer sparseChildren[0].Release()
	sparseChildren[1] = uint8ArrFromSlice(10, 11, 12, 13, 14, 15, 16)
	defer sparseChildren[1].Release()

	denseChildren[0] = int32ArrFromSlice(0, 2, 3, 7)
	defer denseChildren[0].Release()
	denseChildren[1] = uint8ArrFromSlice(11, 14, 15)
	defer denseChildren[1].Release()

	offsetsBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 0, 1, 2, 1, 2, 3}))
	sparse := array.NewSparseUnion(sparseType, length, sparseChildren, typeIDsBuffer, 0)
	dense := array.NewDenseUnion(denseType, length, denseChildren, typeIDsBuffer, offsetsBuffer, 0)

	defer sparse.Release()
	defer dense.Release()

	batch := array.NewRecord(schema, []arrow.Array{sparse, dense}, -1)
	defer batch.Release()

	checkUnion := func(arr arrow.Array) {
		size := arr.Len()
		slice := array.NewSlice(arr, 2, int64(size))
		defer slice.Release()
		assert.EqualValues(t, size-2, slice.Len())

		slice2 := array.NewSlice(arr, 2, int64(arr.Len()))
		defer slice2.Release()
		assert.EqualValues(t, size-2, slice2.Len())

		assert.True(t, array.Equal(slice, slice2))
		assert.True(t, array.SliceEqual(arr, 2, int64(arr.Len()), slice, 0, int64(slice.Len())))

		// chain slices
		slice2 = array.NewSlice(arr, 1, int64(arr.Len()))
		defer slice2.Release()
		slice2 = array.NewSlice(slice2, 1, int64(slice2.Len()))
		defer slice2.Release()
		assert.True(t, array.Equal(slice, slice2))

		slice, slice2 = array.NewSlice(arr, 1, 6), array.NewSlice(arr, 1, 6)
		defer slice.Release()
		defer slice2.Release()
		assert.EqualValues(t, 5, slice.Len())

		assert.True(t, array.Equal(slice, slice2))
		assert.True(t, array.SliceEqual(arr, 1, 6, slice, 0, 5))
	}

	checkUnion(batch.Column(0))
	checkUnion(batch.Column(1))
}

func TestSparseUnionGetFlattenedField(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ty := arrow.SparseUnionOf([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "strs", Type: arrow.BinaryTypes.String, Nullable: true},
	}, []arrow.UnionTypeCode{2, 7})
	ints, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[0, 1, 2, 3]`))
	defer ints.Release()
	strs, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["a", null, "c", "d"]`))
	defer strs.Release()
	idsArr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[2, 7, 2, 7]`))
	defer idsArr.Release()
	ids := idsArr.Data().Buffers()[1]

	const length = 4

	t.Run("flattened", func(t *testing.T) {
		scoped := memory.NewCheckedAllocatorScope(mem)
		defer scoped.CheckSize(t)

		arr := array.NewSparseUnion(ty, length, []arrow.Array{ints, strs}, ids, 0)
		defer arr.Release()

		flattened, err := arr.GetFlattenedField(mem, 0)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[0, null, 2, null]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		flattened, err = arr.GetFlattenedField(mem, 1)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[null, null, null, "d"]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		sliced := array.NewSlice(arr, 1, 3).(*array.SparseUnion)
		defer sliced.Release()

		flattened, err = sliced.GetFlattenedField(mem, 0)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[null, 2]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		flattened, err = sliced.GetFlattenedField(mem, 1)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[null, null]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		_, err = arr.GetFlattenedField(mem, -1)
		assert.Error(t, err)
		_, err = arr.GetFlattenedField(mem, 2)
		assert.Error(t, err)
	})

	t.Run("offset children", func(t *testing.T) {
		scoped := memory.NewCheckedAllocatorScope(mem)
		defer scoped.CheckSize(t)

		strSlice, intSlice := array.NewSlice(strs, 1, 3), array.NewSlice(ints, 1, 3)
		defer strSlice.Release()
		defer intSlice.Release()

		arr := array.NewSparseUnion(ty, length-2, []arrow.Array{intSlice, strSlice}, ids, 0)
		defer arr.Release()

		flattened, err := arr.GetFlattenedField(mem, 0)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1, null]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		flattened, err = arr.GetFlattenedField(mem, 1)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[null, "c"]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		sliced := array.NewSlice(arr, 1, 2).(*array.SparseUnion)
		defer sliced.Release()

		flattened, err = sliced.GetFlattenedField(mem, 0)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[null]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		flattened, err = sliced.GetFlattenedField(mem, 1)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["c"]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)
	})

	t.Run("empty flattened", func(t *testing.T) {
		scoped := memory.NewCheckedAllocatorScope(mem)
		defer scoped.CheckSize(t)

		strSlice, intSlice := array.NewSlice(strs, length, length), array.NewSlice(ints, length, length)
		defer strSlice.Release()
		defer intSlice.Release()

		arr := array.NewSparseUnion(ty, 0, []arrow.Array{intSlice, strSlice}, ids, 0)
		defer arr.Release()

		flattened, err := arr.GetFlattenedField(mem, 0)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)

		flattened, err = arr.GetFlattenedField(mem, 1)
		assert.NoError(t, err)
		defer flattened.Release()
		expected, _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`[]`))
		defer expected.Release()

		assert.Truef(t, array.Equal(flattened, expected), "expected: %s, got: %s", expected, flattened)
	})
}

func TestSparseUnionValidate(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	a, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[4, 5]`))
	defer a.Release()
	dt := arrow.SparseUnionOf([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, []arrow.UnionTypeCode{0})
	children := []arrow.Array{a}

	typeIDsArr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[0, 0, 0]`))
	defer typeIDsArr.Release()
	typeIDs := typeIDsArr.Data().Buffers()[1]

	arr := array.NewSparseUnion(dt, 2, children, typeIDs, 0)
	assert.NoError(t, arr.ValidateFull())
	arr.Release()

	arr = array.NewSparseUnion(dt, 1, children, typeIDs, 1)
	assert.NoError(t, arr.ValidateFull())
	arr.Release()

	arr = array.NewSparseUnion(dt, 0, children, typeIDs, 2)
	assert.NoError(t, arr.ValidateFull())
	arr.Release()

	// length + offset < child length but that's ok!
	arr = array.NewSparseUnion(dt, 1, children, typeIDs, 0)
	assert.NoError(t, arr.ValidateFull())
	arr.Release()

	// length + offset > child length! BAD!
	assert.Panics(t, func() {
		arr = array.NewSparseUnion(dt, 1, children, typeIDs, 2)
	})

	// offset > child length
	assert.Panics(t, func() {
		arr = array.NewSparseUnion(dt, 0, children, typeIDs, 3)
	})
}

type UnionFactorySuite struct {
	suite.Suite

	mem             *memory.CheckedAllocator
	codes           []arrow.UnionTypeCode
	typeIDs         arrow.Array
	logicalTypeIDs  arrow.Array
	invalidTypeIDs  arrow.Array
	invalidTypeIDs2 arrow.Array
}

func (s *UnionFactorySuite) typeidsFromSlice(ids ...int8) arrow.Array {
	data := array.NewData(arrow.PrimitiveTypes.Int8, len(ids),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int8Traits.CastToBytes(ids))}, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func (s *UnionFactorySuite) offsetsFromSlice(offsets ...int32) arrow.Array {
	data := array.NewData(arrow.PrimitiveTypes.Int32, len(offsets),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))}, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func (s *UnionFactorySuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.codes = []arrow.UnionTypeCode{1, 2, 4, 127}
	s.typeIDs = s.typeidsFromSlice(0, 1, 2, 0, 1, 3, 2, 0, 2, 1)
	s.logicalTypeIDs = s.typeidsFromSlice(1, 2, 4, 1, 2, 127, 4, 1, 4, 2)
	s.invalidTypeIDs = s.typeidsFromSlice(1, 2, 4, 1, -2, 127, 4, 1, 4, 2)
	s.invalidTypeIDs2 = s.typeidsFromSlice(1, 2, 4, 1, 3, 127, 4, 1, 4, 2)
}

func (s *UnionFactorySuite) TearDownTest() {
	s.typeIDs.Release()
	s.logicalTypeIDs.Release()
	s.invalidTypeIDs.Release()
	s.invalidTypeIDs2.Release()
	s.mem.AssertSize(s.T(), 0)
}

func (s *UnionFactorySuite) checkFields(arr array.Union, fields []string) {
	ty := arr.DataType().(arrow.UnionType)
	s.Len(ty.Fields(), len(fields))
	for i, f := range ty.Fields() {
		s.Equal(fields[i], f.Name)
	}
}

func (s *UnionFactorySuite) checkCodes(arr array.Union, codes []arrow.UnionTypeCode) {
	ty := arr.DataType().(arrow.UnionType)
	s.Equal(codes, ty.TypeCodes())
}

func (s *UnionFactorySuite) checkUnion(arr array.Union, mode arrow.UnionMode, fields []string, codes []arrow.UnionTypeCode) {
	s.Equal(mode, arr.Mode())
	s.checkFields(arr, fields)
	s.checkCodes(arr, codes)
	typeIDs := s.typeIDs.(*array.Int8)
	for i := 0; i < typeIDs.Len(); i++ {
		s.EqualValues(typeIDs.Value(i), arr.ChildID(i))
	}
	s.Nil(arr.Field(-1))
	s.Nil(arr.Field(typeIDs.Len()))
}

func (s *UnionFactorySuite) TestMakeDenseUnions() {
	// typeIDs:                  {0, 1, 2, 0, 1, 3, 2, 0, 2, 1}
	offsets := s.offsetsFromSlice(0, 0, 0, 1, 1, 0, 1, 2, 1, 2)
	defer offsets.Release()

	children := make([]arrow.Array, 4)
	children[0], _, _ = array.FromJSON(s.mem, arrow.BinaryTypes.String, strings.NewReader(`["abc", "def", "xyz"]`))
	defer children[0].Release()
	children[1], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Uint8, strings.NewReader(`[10, 20, 30]`))
	defer children[1].Release()
	children[2], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.618, 2.718, 3.142]`))
	defer children[2].Release()
	children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[-12]`))
	defer children[3].Release()

	fieldNames := []string{"str", "int1", "real", "int2"}

	s.Run("without fields and codes", func() {
		result, err := array.NewDenseUnionFromArrays(s.typeIDs, offsets, children)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.DenseMode, []string{"0", "1", "2", "3"}, []arrow.UnionTypeCode{0, 1, 2, 3})
	})

	s.Run("with fields", func() {
		_, err := array.NewDenseUnionFromArraysWithFields(s.typeIDs, offsets, children, []string{"one"})
		s.Error(err)
		result, err := array.NewDenseUnionFromArraysWithFields(s.typeIDs, offsets, children, fieldNames)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.DenseMode, fieldNames, []arrow.UnionTypeCode{0, 1, 2, 3})
	})

	s.Run("with codes", func() {
		_, err := array.NewDenseUnionFromArrays(s.logicalTypeIDs, offsets, children, 0)
		s.Error(err)
		result, err := array.NewDenseUnionFromArrays(s.logicalTypeIDs, offsets, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.DenseMode, []string{"0", "1", "2", "3"}, s.codes)
	})

	s.Run("with fields and codes", func() {
		_, err := array.NewDenseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, offsets, children, []string{"one"}, s.codes)
		s.Error(err)
		result, err := array.NewDenseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, offsets, children, fieldNames, s.codes)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.DenseMode, fieldNames, s.codes)
	})

	s.Run("invalid type codes", func() {
		result, err := array.NewDenseUnionFromArrays(s.invalidTypeIDs, offsets, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())
		result, err = array.NewDenseUnionFromArrays(s.invalidTypeIDs2, offsets, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())
	})

	s.Run("invalid offsets", func() {
		// offset out of bounds at index 5
		invalidOffsets := s.offsetsFromSlice(0, 0, 0, 1, 1, 1, 1, 2, 1, 2)
		defer invalidOffsets.Release()
		result, err := array.NewDenseUnionFromArrays(s.typeIDs, invalidOffsets, children)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())

		// negative offset at index 5
		invalidOffsets = s.offsetsFromSlice(0, 0, 0, 1, 1, -1, 1, 2, 1, 2)
		defer invalidOffsets.Release()
		result, err = array.NewDenseUnionFromArrays(s.typeIDs, invalidOffsets, children)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())

		// non-monotonic offset at index 3
		invalidOffsets = s.offsetsFromSlice(1, 0, 0, 0, 1, 0, 1, 2, 1, 2)
		defer invalidOffsets.Release()
		result, err = array.NewDenseUnionFromArrays(s.typeIDs, invalidOffsets, children)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())
	})
}

func (s *UnionFactorySuite) TestDenseUnionStringRoundTrip() {
	// typeIDs:                  {0, 1, 2, 0, 1, 3, 2, 0, 2, 1}
	offsets := s.offsetsFromSlice(0, 0, 0, 1, 1, 0, 1, 2, 1, 2)
	defer offsets.Release()

	children := make([]arrow.Array, 4)
	children[0], _, _ = array.FromJSON(s.mem, arrow.BinaryTypes.String, strings.NewReader(`["abc", "def", "xyz"]`))
	defer children[0].Release()
	children[1], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Uint8, strings.NewReader(`[10, 20, 30]`))
	defer children[1].Release()
	children[2], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.618, 2.718, 3.142]`))
	defer children[2].Release()
	children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[-12]`))
	defer children[3].Release()

	fields := []string{"str", "int1", "real", "int2"}

	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(s.T(), 0)

	dt := arrow.DenseUnionFromArrays(children, fields, s.codes)
	arr, err := array.NewDenseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, offsets, children, fields, s.codes)
	s.NoError(err)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewDenseUnionBuilder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		s.NoError(b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.DenseUnion)
	defer arr1.Release()

	s.True(array.Equal(arr, arr1))
}

func (s *UnionFactorySuite) TestMakeSparse() {
	children := make([]arrow.Array, 4)
	children[0], _, _ = array.FromJSON(s.mem, arrow.BinaryTypes.String,
		strings.NewReader(`["abc", "", "", "def", "", "", "", "xyz", "", ""]`))
	children[1], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Uint8,
		strings.NewReader(`[0, 10, 0, 0, 20, 0, 0, 0, 0, 30]`))
	children[2], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64,
		strings.NewReader(`[0.0, 0.0, 1.618, 0.0, 0.0, 0.0, 2.718, 0.0, 3.142, 0.0]`))
	children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[0, 0, 0, 0, 0, -12, 0, 0, 0, 0]`))
	for _, c := range children {
		defer c.Release()
	}

	fieldNames := []string{"str", "int1", "real", "int2"}

	s.Run("without fields and codes", func() {
		result, err := array.NewSparseUnionFromArrays(s.typeIDs, children)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.SparseMode, []string{"0", "1", "2", "3"}, []arrow.UnionTypeCode{0, 1, 2, 3})
	})

	s.Run("with fields", func() {
		_, err := array.NewSparseUnionFromArraysWithFields(s.typeIDs, children, []string{"one"})
		s.Error(err)
		result, err := array.NewSparseUnionFromArraysWithFields(s.typeIDs, children, fieldNames)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.SparseMode, fieldNames, []arrow.UnionTypeCode{0, 1, 2, 3})
	})

	s.Run("with codes", func() {
		_, err := array.NewSparseUnionFromArrays(s.logicalTypeIDs, children, 0)
		s.Error(err)
		result, err := array.NewSparseUnionFromArrays(s.logicalTypeIDs, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.SparseMode, []string{"0", "1", "2", "3"}, s.codes)
	})

	s.Run("with fields and codes", func() {
		_, err := array.NewSparseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, children, []string{"one"}, s.codes)
		s.Error(err)
		result, err := array.NewSparseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, children, fieldNames, s.codes)
		s.NoError(err)
		defer result.Release()
		s.NoError(result.ValidateFull())
		s.checkUnion(result, arrow.SparseMode, fieldNames, s.codes)
	})

	s.Run("invalid type codes", func() {
		result, err := array.NewSparseUnionFromArrays(s.invalidTypeIDs, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())
		result, err = array.NewSparseUnionFromArrays(s.invalidTypeIDs2, children, s.codes...)
		s.NoError(err)
		defer result.Release()
		s.Error(result.ValidateFull())
	})

	s.Run("invalid child length", func() {
		children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
			strings.NewReader(`[0, 0, 0, 0, 0, -12, 0, 0, 0]`))
		defer children[3].Release()

		_, err := array.NewSparseUnionFromArrays(s.typeIDs, children)
		s.Error(err)
	})
}

func (s *UnionFactorySuite) TestSparseUnionStringRoundTrip() {
	children := make([]arrow.Array, 4)
	children[0], _, _ = array.FromJSON(s.mem, arrow.BinaryTypes.String,
		strings.NewReader(`["abc", "", "", "def", "", "", "", "xyz", "", ""]`))
	defer children[0].Release()
	children[1], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Uint8,
		strings.NewReader(`[0, 10, 0, 0, 20, 0, 0, 0, 0, 30]`))
	defer children[1].Release()
	children[2], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64,
		strings.NewReader(`[0.0, 0.0, 1.618, 0.0, 0.0, 0.0, 2.718, 0.0, 3.142, 0.0]`))
	defer children[2].Release()
	children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[0, 0, 0, 0, 0, -12, 0, 0, 0, 0]`))
	defer children[3].Release()

	fields := []string{"str", "int1", "real", "int2"}

	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(s.T(), 0)

	dt := arrow.SparseUnionFromArrays(children, fields, s.codes)

	arr, err := array.NewSparseUnionFromArraysWithFieldCodes(s.logicalTypeIDs, children, fields, s.codes)
	s.NoError(err)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewSparseUnionBuilder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		s.NoError(b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.SparseUnion)
	defer arr1.Release()

	s.True(array.Equal(arr, arr1))
}

type UnionBuilderSuite struct {
	suite.Suite

	I8  arrow.UnionTypeCode
	STR arrow.UnionTypeCode
	DBL arrow.UnionTypeCode

	mem              *memory.CheckedAllocator
	expectedTypes    []arrow.UnionTypeCode
	expectedTypesArr arrow.Array
	i8Bldr           *array.Int8Builder
	strBldr          *array.StringBuilder
	dblBldr          *array.Float64Builder
	unionBldr        array.UnionBuilder
	actual           array.Union
}

func (s *UnionBuilderSuite) SetupTest() {
	s.I8, s.STR, s.DBL = 8, 13, 7

	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.expectedTypes = make([]arrow.UnionTypeCode, 0)

	s.i8Bldr = array.NewInt8Builder(s.mem)
	s.strBldr = array.NewStringBuilder(s.mem)
	s.dblBldr = array.NewFloat64Builder(s.mem)
}

func (s *UnionBuilderSuite) TearDownTest() {
	if s.expectedTypesArr != nil {
		s.expectedTypesArr.Release()
		s.expectedTypesArr = nil
	}
	s.i8Bldr.Release()
	s.strBldr.Release()
	s.dblBldr.Release()
	if s.actual != nil {
		s.actual.Release()
		s.actual = nil
	}

	s.mem.AssertSize(s.T(), 0)
}

func (s *UnionBuilderSuite) createExpectedTypesArr() {
	data := array.NewData(arrow.PrimitiveTypes.Int8, len(s.expectedTypes),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int8Traits.CastToBytes(s.expectedTypes))}, nil, 0, 0)
	defer data.Release()
	s.expectedTypesArr = array.MakeFromData(data)
}

func (s *UnionBuilderSuite) appendInt(i int8) {
	s.expectedTypes = append(s.expectedTypes, s.I8)
	s.unionBldr.Append(s.I8)
	s.i8Bldr.Append(i)
	if s.unionBldr.Mode() == arrow.SparseMode {
		s.strBldr.AppendEmptyValue()
		s.dblBldr.AppendEmptyValue()
	}
}

func (s *UnionBuilderSuite) appendString(str string) {
	s.expectedTypes = append(s.expectedTypes, s.STR)
	s.unionBldr.Append(s.STR)
	s.strBldr.Append(str)
	if s.unionBldr.Mode() == arrow.SparseMode {
		s.i8Bldr.AppendEmptyValue()
		s.dblBldr.AppendEmptyValue()
	}
}

func (s *UnionBuilderSuite) appendDbl(dbl float64) {
	s.expectedTypes = append(s.expectedTypes, s.DBL)
	s.unionBldr.Append(s.DBL)
	s.dblBldr.Append(dbl)
	if s.unionBldr.Mode() == arrow.SparseMode {
		s.strBldr.AppendEmptyValue()
		s.i8Bldr.AppendEmptyValue()
	}
}

func (s *UnionBuilderSuite) appendBasics() {
	s.appendInt(33)
	s.appendString("abc")
	s.appendDbl(1.0)
	s.appendDbl(-1.0)
	s.appendString("")
	s.appendInt(10)
	s.appendString("def")
	s.appendInt(-10)
	s.appendDbl(0.5)

	s.Equal(9, s.unionBldr.Len())

	s.actual = s.unionBldr.NewArray().(array.Union)
	s.NoError(s.actual.ValidateFull())
	s.createExpectedTypesArr()
}

func (s *UnionBuilderSuite) appendNullsAndEmptyValues() {
	s.appendString("abc")
	s.unionBldr.AppendNull()
	s.unionBldr.AppendEmptyValue()
	s.expectedTypes = append(s.expectedTypes, s.I8, s.I8, s.I8)
	s.appendInt(42)
	s.unionBldr.AppendNulls(2)
	s.unionBldr.AppendEmptyValues(2)
	s.expectedTypes = append(s.expectedTypes, s.I8, s.I8, s.I8)

	s.Equal(8, s.unionBldr.Len())

	s.actual = s.unionBldr.NewArray().(array.Union)
	s.NoError(s.actual.ValidateFull())
	s.createExpectedTypesArr()
}

func (s *UnionBuilderSuite) appendInferred() {
	s.I8 = s.unionBldr.AppendChild(s.i8Bldr, "i8")
	s.EqualValues(0, s.I8)
	s.appendInt(33)
	s.appendInt(10)

	s.STR = s.unionBldr.AppendChild(s.strBldr, "str")
	s.EqualValues(1, s.STR)
	s.appendString("abc")
	s.appendString("")
	s.appendString("def")
	s.appendInt(-10)

	s.DBL = s.unionBldr.AppendChild(s.dblBldr, "dbl")
	s.EqualValues(2, s.DBL)
	s.appendDbl(1.0)
	s.appendDbl(-1.0)
	s.appendDbl(0.5)

	s.Equal(9, s.unionBldr.Len())

	s.actual = s.unionBldr.NewArray().(array.Union)
	s.NoError(s.actual.ValidateFull())
	s.createExpectedTypesArr()

	s.EqualValues(0, s.I8)
	s.EqualValues(1, s.STR)
	s.EqualValues(2, s.DBL)
}

func (s *UnionBuilderSuite) appendListOfInferred(utyp arrow.UnionType) *array.List {
	listBldr := array.NewListBuilder(s.mem, utyp)
	defer listBldr.Release()

	s.unionBldr = listBldr.ValueBuilder().(array.UnionBuilder)

	listBldr.Append(true)
	s.I8 = s.unionBldr.AppendChild(s.i8Bldr, "i8")
	s.EqualValues(0, s.I8)
	s.appendInt(10)

	listBldr.Append(true)
	s.STR = s.unionBldr.AppendChild(s.strBldr, "str")
	s.EqualValues(1, s.STR)
	s.appendString("abc")
	s.appendInt(-10)

	listBldr.Append(true)
	s.DBL = s.unionBldr.AppendChild(s.dblBldr, "dbl")
	s.EqualValues(2, s.DBL)
	s.appendDbl(0.5)

	s.Equal(4, s.unionBldr.Len())

	s.createExpectedTypesArr()
	return listBldr.NewListArray()
}

func (s *UnionBuilderSuite) assertArraysEqual(expected, actual arrow.Array) {
	s.Truef(array.Equal(expected, actual), "expected: %s, got: %s", expected, actual)
}

func (s *UnionBuilderSuite) TestDenseUnionBasics() {
	s.unionBldr = array.NewDenseUnionBuilderWithBuilders(s.mem,
		arrow.DenseUnionOf([]arrow.Field{
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "dbl", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, []arrow.UnionTypeCode{s.I8, s.STR, s.DBL}),
		[]array.Builder{s.i8Bldr, s.strBldr, s.dblBldr})
	defer s.unionBldr.Release()

	s.appendBasics()

	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[33, 10, -10]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String, strings.NewReader(`["abc", "", "def"]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.0, -1.0, 0.5]`))
	expectedOffsets, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0, 0, 1, 1, 1, 2, 2, 2]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
		expectedOffsets.Release()
	}()

	expected, err := array.NewDenseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		expectedOffsets,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)
}

func (s *UnionBuilderSuite) TestDenseBuilderNullsAndEmpty() {
	s.unionBldr = array.NewDenseUnionBuilderWithBuilders(s.mem,
		arrow.DenseUnionOf([]arrow.Field{
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "dbl", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, []arrow.UnionTypeCode{s.I8, s.STR, s.DBL}),
		[]array.Builder{s.i8Bldr, s.strBldr, s.dblBldr})
	defer s.unionBldr.Release()

	s.appendNullsAndEmptyValues()

	// four null / empty values (the latter implementation-defined) appended to I8
	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[null, 0, 42, null, 0]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String, strings.NewReader(`["abc"]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[]`))
	expectedOffsets, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0, 1, 2, 3, 3, 4, 4]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
		expectedOffsets.Release()
	}()

	expected, err := array.NewDenseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		expectedOffsets,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)

	// physical arrays must be as expected
	s.assertArraysEqual(expectedI8, s.actual.Field(0))
	s.assertArraysEqual(expectedStr, s.actual.Field(1))
	s.assertArraysEqual(expectedDbl, s.actual.Field(2))
}

func (s *UnionBuilderSuite) TestDenseUnionInferredTyped() {
	s.unionBldr = array.NewEmptyDenseUnionBuilder(s.mem)
	defer s.unionBldr.Release()

	s.appendInferred()

	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[33, 10, -10]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String, strings.NewReader(`["abc", "", "def"]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.0, -1.0, 0.5]`))
	expectedOffsets, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 1, 0, 1, 2, 2, 0, 1, 2]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
		expectedOffsets.Release()
	}()

	expected, err := array.NewDenseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		expectedOffsets,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)
}

func (s *UnionBuilderSuite) TestDenseUnionListOfInferredType() {
	actual := s.appendListOfInferred(arrow.DenseUnionOf([]arrow.Field{}, []arrow.UnionTypeCode{}))
	defer actual.Release()

	expectedType := arrow.ListOf(arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "dbl", Type: arrow.PrimitiveTypes.Float64, Nullable: true}},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL}))
	s.Equal(expectedType.String(), actual.DataType().String())
}

func (s *UnionBuilderSuite) TestSparseUnionBasics() {
	s.unionBldr = array.NewSparseUnionBuilderWithBuilders(s.mem,
		arrow.SparseUnionOf([]arrow.Field{
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "dbl", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, []arrow.UnionTypeCode{s.I8, s.STR, s.DBL}),
		[]array.Builder{s.i8Bldr, s.strBldr, s.dblBldr})
	defer s.unionBldr.Release()

	s.appendBasics()

	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[33, null, null, null, null, 10, null, -10, null]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String,
		strings.NewReader(`[null, "abc", null, null, "", null, "def", null, null]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64,
		strings.NewReader(`[null, null, 1.0, -1.0, null, null, null, null, 0.5]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
	}()

	expected, err := array.NewSparseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)
}

func (s *UnionBuilderSuite) TestSparseBuilderNullsAndEmpty() {
	s.unionBldr = array.NewSparseUnionBuilderWithBuilders(s.mem,
		arrow.SparseUnionOf([]arrow.Field{
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "dbl", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, []arrow.UnionTypeCode{s.I8, s.STR, s.DBL}),
		[]array.Builder{s.i8Bldr, s.strBldr, s.dblBldr})
	defer s.unionBldr.Release()

	s.appendNullsAndEmptyValues()

	// "abc", null, 0, 42, null, null, 0, 0
	// getting 0 for empty values is implementation-defined
	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[0, null, 0, 42, null, null, 0, 0]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String,
		strings.NewReader(`["abc", "", "", "", "", "", "", ""]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64,
		strings.NewReader(`[0, 0, 0, 0, 0, 0, 0, 0]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
	}()

	expected, err := array.NewSparseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)

	// physical arrays must be as expected
	s.assertArraysEqual(expectedI8, s.actual.Field(0))
	s.assertArraysEqual(expectedStr, s.actual.Field(1))
	s.assertArraysEqual(expectedDbl, s.actual.Field(2))
}

func (s *UnionBuilderSuite) TestSparseUnionInferredType() {
	s.unionBldr = array.NewEmptySparseUnionBuilder(s.mem)
	defer s.unionBldr.Release()

	s.appendInferred()

	expectedI8, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8,
		strings.NewReader(`[33, 10, null, null, null, -10, null, null, null]`))
	expectedStr, _, _ := array.FromJSON(s.mem, arrow.BinaryTypes.String,
		strings.NewReader(`[null, null, "abc", "", "def", null, null, null, null]`))
	expectedDbl, _, _ := array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64,
		strings.NewReader(`[null, null, null, null, null, null,1.0, -1.0, 0.5]`))

	defer func() {
		expectedI8.Release()
		expectedStr.Release()
		expectedDbl.Release()
	}()

	expected, err := array.NewSparseUnionFromArraysWithFieldCodes(s.expectedTypesArr,
		[]arrow.Array{expectedI8, expectedStr, expectedDbl},
		[]string{"i8", "str", "dbl"},
		[]arrow.UnionTypeCode{s.I8, s.STR, s.DBL})
	s.NoError(err)
	defer expected.Release()

	s.Equal(expected.DataType().String(), s.actual.DataType().String())
	s.assertArraysEqual(expected, s.actual)
}

func (s *UnionBuilderSuite) TestSparseUnionStructWithUnion() {
	bldr := array.NewStructBuilder(s.mem, arrow.StructOf(arrow.Field{Name: "u", Type: arrow.SparseUnionFromArrays(nil, nil, nil)}))
	defer bldr.Release()

	unionBldr := bldr.FieldBuilder(0).(array.UnionBuilder)
	int32Bldr := array.NewInt32Builder(s.mem)
	defer int32Bldr.Release()

	s.EqualValues(0, unionBldr.AppendChild(int32Bldr, "i"))
	expectedType := arrow.StructOf(arrow.Field{Name: "u",
		Type: arrow.SparseUnionOf([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, []arrow.UnionTypeCode{0})})
	s.Truef(arrow.TypeEqual(expectedType, bldr.Type()), "expected: %s, got: %s", expectedType, bldr.Type())
}

func ExampleSparseUnionBuilder() {
	dt1 := arrow.SparseUnionOf([]arrow.Field{
		{Name: "c", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}},
	}, []arrow.UnionTypeCode{0})
	dt2 := arrow.StructOf(arrow.Field{Name: "a", Type: dt1})

	pool := memory.DefaultAllocator
	bldr := array.NewStructBuilder(pool, dt2)
	defer bldr.Release()

	bldrDt1 := bldr.FieldBuilder(0).(*array.SparseUnionBuilder)
	binDictBldr := bldrDt1.Child(0).(*array.BinaryDictionaryBuilder)

	bldr.Append(true)
	bldrDt1.Append(0)
	binDictBldr.AppendString("foo")

	bldr.Append(true)
	bldrDt1.Append(0)
	binDictBldr.AppendString("bar")

	out := bldr.NewArray().(*array.Struct)
	defer out.Release()

	fmt.Println(out)

	// Output:
	// {[{c=foo} {c=bar}]}
}

func TestUnions(t *testing.T) {
	suite.Run(t, new(UnionFactorySuite))
	suite.Run(t, new(UnionBuilderSuite))
}

func TestNestedUnionStructDict(t *testing.T) {
	// ARROW-18274
	dt1 := arrow.SparseUnionOf([]arrow.Field{
		{Name: "c", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint16,
			ValueType: arrow.BinaryTypes.String,
			Ordered:   false,
		}},
	}, []arrow.UnionTypeCode{0})
	dt2 := arrow.StructOf(
		arrow.Field{Name: "b", Type: dt1},
	)
	dt3 := arrow.SparseUnionOf([]arrow.Field{
		{Name: "a", Type: dt2},
	}, []arrow.UnionTypeCode{0})
	pool := memory.NewGoAllocator()

	builder := array.NewSparseUnionBuilder(pool, dt3)
	defer builder.Release()
	arr := builder.NewArray()
	defer arr.Release()
	assert.Equal(t, 0, arr.Len())
}

func TestNestedUnionDictUnion(t *testing.T) {
	dt1 := arrow.SparseUnionOf([]arrow.Field{
		{Name: "c", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint16,
			ValueType: arrow.BinaryTypes.String,
			Ordered:   false,
		}},
	}, []arrow.UnionTypeCode{0})
	dt2 := arrow.SparseUnionOf([]arrow.Field{
		{Name: "a", Type: dt1},
	}, []arrow.UnionTypeCode{0})
	pool := memory.NewGoAllocator()

	builder := array.NewSparseUnionBuilder(pool, dt2)
	defer builder.Release()
	arr := builder.NewArray()
	defer arr.Release()
	assert.Equal(t, 0, arr.Len())
}

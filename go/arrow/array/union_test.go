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

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestUnionSliceEquals(t *testing.T) {
	batch := arrdata.Records["union"][0]

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
	children[1], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Uint8, strings.NewReader(`[10, 20, 30]`))
	children[2], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.618, 2.718, 3.142]`))
	children[3], _, _ = array.FromJSON(s.mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[-12]`))
	for _, c := range children {
		defer c.Release()
	}

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

func TestUnionFactories(t *testing.T) {
	suite.Run(t, new(UnionFactorySuite))
}

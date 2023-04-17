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
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFieldPathBasics(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f2 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f3 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}

	s := arrow.NewSchema([]arrow.Field{f0, f1, f2, f3}, nil)

	for i := range s.Fields() {
		f, err := compute.FieldPath{i}.Get(s)
		assert.NoError(t, err)
		assert.Equal(t, s.Field(i), *f)
	}

	f, err := compute.FieldPath{}.Get(s)
	assert.Nil(t, f)
	assert.ErrorIs(t, err, compute.ErrEmpty)

	f, err = compute.FieldPath{len(s.Fields()) * 2}.Get(s)
	assert.Nil(t, f)
	assert.ErrorIs(t, err, compute.ErrIndexRange)
}

func TestFieldRefBasics(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f2 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f3 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}

	s := arrow.NewSchema([]arrow.Field{f0, f1, f2, f3}, nil)

	// lookup by index returns Indices{index}
	for i := range s.Fields() {
		assert.ElementsMatch(t, []compute.FieldPath{{i}}, compute.FieldRefIndex(i).FindAll(s.Fields()))
	}

	// out of range index results in failure to match
	assert.Empty(t, compute.FieldRefIndex(len(s.Fields())*2).FindAll(s.Fields()))

	// lookup by name returns the indices of both matching fields
	assert.Equal(t, []compute.FieldPath{{0}, {2}}, compute.FieldRefName("alpha").FindAll(s.Fields()))
	assert.Equal(t, []compute.FieldPath{{1}, {3}}, compute.FieldRefName("beta").FindAll(s.Fields()))
}

func TestFieldRefDotPath(t *testing.T) {
	ref, err := compute.NewFieldRefFromDotPath(`.alpha`)
	assert.True(t, ref.IsName())
	assert.Equal(t, "alpha", ref.Name())
	assert.False(t, ref.IsFieldPath())
	assert.False(t, ref.IsNested())
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefName("alpha"), ref)
	assert.True(t, ref.Equals(compute.FieldRefName("alpha")))

	ref, err = compute.NewFieldRefFromDotPath(`..`)
	assert.Empty(t, ref.Name())
	assert.False(t, ref.IsName())
	assert.False(t, ref.IsFieldPath())
	assert.Nil(t, ref.FieldPath())
	assert.True(t, ref.IsNested())
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefList("", ""), ref)

	ref, err = compute.NewFieldRefFromDotPath(`[2]`)
	assert.False(t, ref.IsName())
	assert.True(t, ref.IsFieldPath())
	assert.Equal(t, compute.FieldPath{2}, ref.FieldPath())
	assert.False(t, ref.IsNested())
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefIndex(2), ref)

	ref, err = compute.NewFieldRefFromDotPath(`.beta[3]`)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefList("beta", 3), ref)

	ref, err = compute.NewFieldRefFromDotPath(`[5].gamma.delta[7]`)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefList(5, "gamma", "delta", 7), ref)

	ref, err = compute.NewFieldRefFromDotPath(`.hello world`)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefName("hello world"), ref)

	ref, err = compute.NewFieldRefFromDotPath(`.\[y\]\\tho\.\`)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldRefName(`[y]\tho.\`), ref)

	_, err = compute.NewFieldRefFromDotPath(``)
	assert.ErrorIs(t, err, compute.ErrInvalid)

	_, err = compute.NewFieldRefFromDotPath(`alpha`)
	assert.ErrorIs(t, err, compute.ErrInvalid)

	_, err = compute.NewFieldRefFromDotPath(`[134234`)
	assert.ErrorIs(t, err, compute.ErrInvalid)

	_, err = compute.NewFieldRefFromDotPath(`[1stuf]`)
	assert.ErrorIs(t, err, compute.ErrInvalid)
}

func TestFieldPathNested(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1_0 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "beta", Type: arrow.StructOf(f1_0)}
	f2_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_1 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f2_1 := arrow.Field{Name: "gamma", Type: arrow.StructOf(f2_1_0, f2_1_1)}
	f2 := arrow.Field{Name: "beta", Type: arrow.StructOf(f2_0, f2_1)}
	s := arrow.NewSchema([]arrow.Field{f0, f1, f2}, nil)

	f, err := compute.FieldPath{0}.Get(s)
	assert.NoError(t, err)
	assert.Equal(t, f0, *f)

	f, err = compute.FieldPath{0, 0}.Get(s)
	assert.ErrorIs(t, err, compute.ErrNoChildren)
	assert.Nil(t, f)

	f, err = compute.FieldPath{1, 0}.Get(s)
	assert.NoError(t, err)
	assert.Equal(t, f1_0, *f)

	f, err = compute.FieldPath{2, 0}.Get(s)
	assert.NoError(t, err)
	assert.Equal(t, f2_0, *f)

	f, err = compute.FieldPath{2, 1, 0}.Get(s)
	assert.NoError(t, err)
	assert.Equal(t, f2_1_0, *f)

	f, err = compute.FieldPath{1, 0}.GetField(s.Field(2))
	assert.NoError(t, err)
	assert.Equal(t, f2_1_0, *f)

	f, err = compute.FieldPath{2, 1, 1}.Get(s)
	assert.NoError(t, err)
	assert.Equal(t, f2_1_1, *f)
}

func TestFindFuncs(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1_0 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "alpha", Type: arrow.StructOf(f1_0)}
	f2_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_1 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f2_1 := arrow.Field{Name: "gamma", Type: arrow.StructOf(f2_1_0, f2_1_1)}
	f2 := arrow.Field{Name: "beta", Type: arrow.StructOf(f2_0, f2_1)}
	s := arrow.NewSchema([]arrow.Field{f0, f1, f2}, nil)

	assert.Equal(t, []compute.FieldPath{{1}}, compute.FieldRefName("gamma").FindAllField(f2))
	fp, err := compute.FieldRefName("alpha").FindOneOrNone(s)
	assert.ErrorIs(t, err, compute.ErrMultipleMatches)
	assert.Len(t, fp, 0)
	fp, err = compute.FieldRefName("alpha").FindOne(s)
	assert.ErrorIs(t, err, compute.ErrMultipleMatches)
	assert.Len(t, fp, 0)

	fp, err = compute.FieldRefName("beta").FindOneOrNone(s)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldPath{2}, fp)
	fp, err = compute.FieldRefName("beta").FindOne(s)
	assert.NoError(t, err)
	assert.Equal(t, compute.FieldPath{2}, fp)

	fp, err = compute.FieldRefName("gamma").FindOneOrNone(s)
	assert.NoError(t, err)
	assert.Len(t, fp, 0)

	fp, err = compute.FieldRefName("gamma").FindOne(s)
	assert.ErrorIs(t, err, compute.ErrNoMatch)
	assert.Nil(t, fp)
}

func TestGetFieldFuncs(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1_0 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "alpha", Type: arrow.StructOf(f1_0)}
	f2_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_1 := arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32}
	f2_1 := arrow.Field{Name: "gamma", Type: arrow.StructOf(f2_1_0, f2_1_1)}
	f2 := arrow.Field{Name: "beta", Type: arrow.StructOf(f2_0, f2_1)}
	s := arrow.NewSchema([]arrow.Field{f0, f1, f2}, nil)

	ref, err := compute.NewFieldRefFromDotPath(`[2].alpha`)
	assert.NoError(t, err)

	f, err := ref.GetOneField(s)
	assert.NoError(t, err)
	assert.Equal(t, f2_0, *f)
	f, err = ref.GetOneOrNone(s)
	assert.NoError(t, err)
	assert.Equal(t, f2_0, *f)

	ref = compute.FieldRefList("beta", "gamma", 2)
	f, err = ref.GetOneField(s)
	assert.ErrorIs(t, err, compute.ErrNoMatch)
	assert.Nil(t, f)
	f, err = ref.GetOneOrNone(s)
	assert.NoError(t, err)
	assert.Nil(t, f)

	f, err = compute.FieldRefName("alpha").GetOneOrNone(s)
	assert.ErrorIs(t, err, compute.ErrMultipleMatches)
	assert.Nil(t, f)
}

func TestFieldRefRecord(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	alphaBldr := array.NewInt32Builder(mem)
	defer alphaBldr.Release()

	betaBldr := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer betaBldr.Release()

	gammaBldr := array.NewStructBuilder(mem, arrow.StructOf(
		arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "beta", Type: arrow.PrimitiveTypes.Int32, Nullable: true}))
	defer gammaBldr.Release()

	alphaBldr.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	betaBldr.AppendValues([]int32{0, 3, 7, 8, 8, 10, 13, 14, 17, 20, 22}, []bool{true, true, true, false, true, true, true, true, true, true})
	for i := 0; i < 22; i++ {
		betaBldr.ValueBuilder().(*array.Int32Builder).Append(int32(i * 2))
	}

	gammaBldr.AppendValues([]bool{true, true, true, true, true, true, true, true, true, true})
	gammaBldr.FieldBuilder(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, nil)
	gammaBldr.FieldBuilder(1).(*array.Int32Builder).AppendValues([]int32{-10, -20, -30, -40, -50, -60, -70, -80, -90, -100}, nil)

	alpha := alphaBldr.NewInt32Array()
	defer alpha.Release()
	beta := betaBldr.NewListArray()
	defer beta.Release()
	gamma := gammaBldr.NewStructArray()
	defer gamma.Release()

	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "alpha", Type: alpha.DataType(), Nullable: true},
		{Name: "alpha", Type: beta.DataType(), Nullable: true},
		{Name: "alpha", Type: gamma.DataType(), Nullable: true},
	}, nil), []arrow.Array{alpha, beta, gamma}, 10)
	defer rec.Release()

	arr, err := compute.FieldPath{2, 0}.GetColumn(rec)
	assert.NoError(t, err)
	assert.Same(t, gamma.Field(0), arr)

	arr, err = compute.FieldPath{}.GetColumn(rec)
	assert.ErrorIs(t, err, compute.ErrEmpty)
	assert.Nil(t, arr)

	arr, err = compute.FieldPath{1, 0}.GetColumn(rec)
	assert.NoError(t, err)
	assert.Same(t, beta.ListValues(), arr)

	arr, err = compute.FieldPath{1, 0, 0}.GetColumn(rec)
	assert.ErrorIs(t, err, compute.ErrNoChildren)
	assert.Nil(t, arr)

	arr, err = compute.FieldPath{2, 2}.GetColumn(rec)
	assert.ErrorIs(t, err, compute.ErrIndexRange)
	assert.Nil(t, arr)

	arrs, err := compute.FieldRefName("alpha").GetAllColumns(rec)
	assert.NoError(t, err)
	assert.Equal(t, []arrow.Array{alpha, beta, gamma}, arrs)

	arrs, err = compute.FieldRefName("delta").GetAllColumns(rec)
	assert.NoError(t, err)
	assert.Len(t, arrs, 0)

	arr, err = compute.FieldRefName("delta").GetOneColumnOrNone(rec)
	assert.NoError(t, err)
	assert.Nil(t, arr)

	arr, err = compute.FieldRefName("alpha").GetOneColumnOrNone(rec)
	assert.ErrorIs(t, err, compute.ErrMultipleMatches)
	assert.Nil(t, arr)

	arr, err = compute.FieldRefList("alpha", "beta").GetOneColumnOrNone(rec)
	assert.NoError(t, err)
	assert.Same(t, gamma.Field(1), arr)
}

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

package exprs_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/compute/exprs"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
)

var (
	extSet               = exprs.NewDefaultExtensionSet()
	u32ID, u32TypeRef, _ = extSet.EncodeType(arrow.PrimitiveTypes.Uint32)

	boringSchema = types.NamedStruct{
		Names: []string{
			"bool", "i8", "i32", "i32_req",
			"u32", "i64", "f32", "f32_req",
			"f64", "date32", "str", "bin"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types: []types.Type{
				&types.BooleanType{},
				&types.Int8Type{},
				&types.Int32Type{},
				&types.Int32Type{Nullability: types.NullabilityRequired},
				&types.UserDefinedType{
					TypeReference: u32TypeRef,
				},
				&types.Int64Type{},
				&types.Float32Type{},
				&types.Float32Type{Nullability: types.NullabilityRequired},
				&types.Float64Type{},
				&types.DateType{},
				&types.StringType{},
				&types.BinaryType{},
			},
		},
	}
)

func TestToArrowSchema(t *testing.T) {
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i32_req", Type: arrow.PrimitiveTypes.Int32},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f32_req", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	sc, err := exprs.ToArrowSchema(boringSchema, extSet)
	assert.NoError(t, err)

	assert.Truef(t, expectedSchema.Equal(sc), "expected: %s\ngot: %s", expectedSchema, sc)
}

func TestComparisons(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var (
		ctx  = compute.WithAllocator(context.Background(), mem)
		zero = scalar.MakeScalar(int32(0))
		one  = scalar.MakeScalar(int32(1))
		two  = scalar.MakeScalar(int32(2))

		str = scalar.MakeScalar("hello")
		bin = scalar.MakeScalar([]byte("hello"))
	)

	getArgType := func(dt arrow.DataType) types.Type {
		switch dt.ID() {
		case arrow.INT32:
			return &types.Int32Type{}
		case arrow.STRING:
			return &types.StringType{}
		case arrow.BINARY:
			return &types.BinaryType{}
		}
		panic("wtf")
	}

	expect := func(t *testing.T, fn string, arg1, arg2 scalar.Scalar, res bool) {
		baseStruct := types.NamedStruct{
			Names: []string{"arg1", "arg2"},
			Struct: types.StructType{
				Types: []types.Type{getArgType(arg1.DataType()), getArgType(arg2.DataType())},
			},
		}

		ex, err := exprs.NewScalarCall(extSet, fn, nil,
			expr.MustExpr(expr.NewRootFieldRef(expr.NewStructFieldRef(0), &baseStruct.Struct)),
			expr.MustExpr(expr.NewRootFieldRef(expr.NewStructFieldRef(1), &baseStruct.Struct)))
		require.NoError(t, err)

		expression := &expr.Extended{
			Extensions: extSet.GetSubstraitRegistry().Set,
			ReferredExpr: []expr.ExpressionReference{
				expr.NewExpressionReference([]string{"out"}, ex),
			},
			BaseSchema: baseStruct,
		}

		input, _ := scalar.NewStructScalarWithNames([]scalar.Scalar{arg1, arg2}, []string{"arg1", "arg2"})
		out, err := exprs.ExecuteScalarSubstrait(ctx, expression, compute.NewDatum(input))
		require.NoError(t, err)
		require.Equal(t, compute.KindScalar, out.Kind())

		result := out.(*compute.ScalarDatum).Value
		assert.Equal(t, res, result.(*scalar.Boolean).Value)
	}

	expect(t, "equal", one, one, true)
	expect(t, "equal", one, two, false)
	expect(t, "less", one, two, true)
	expect(t, "less", one, zero, false)
	expect(t, "greater", one, zero, true)
	expect(t, "greater", one, two, false)

	expect(t, "equal", str, bin, true)
	expect(t, "equal", bin, str, true)
}

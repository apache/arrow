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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/compute/exprs"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
)

var (
	extSet           = exprs.NewDefaultExtensionSet()
	_, u32TypeRef, _ = extSet.EncodeTypeVariation(arrow.PrimitiveTypes.Uint32)

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
				&types.Int32Type{
					TypeVariationRef: u32TypeRef,
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

	boringArrowSchema = arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
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

func assertEqual(t *testing.T, expected, actual any) bool {
	switch e := expected.(type) {
	case compute.Datum:
		return assert.Truef(t, e.Equals(compute.NewDatumWithoutOwning(actual)),
			"expected: %s\ngot: %s", e, actual)
	case arrow.Array:
		switch a := actual.(type) {
		case compute.Datum:
			if a.Kind() == compute.KindArray {
				actual := a.(*compute.ArrayDatum).MakeArray()
				defer actual.Release()
				return assert.Truef(t, array.Equal(e, actual), "expected: %s\ngot: %s",
					e, actual)
			}
		case arrow.Array:
			return assert.Truef(t, array.Equal(e, a), "expected: %s\ngot: %s",
				e, actual)
		}
		t.Errorf("expected arrow Array, got %s", actual)
		return false
	}
	panic("unimplemented comparison")
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

func TestExecuteFieldRef(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	fromJSON := func(ty arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	scalarFromJSON := func(ty arrow.DataType, json string) scalar.Scalar {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		defer arr.Release()
		s, err := scalar.GetScalar(arr, 0)
		require.NoError(t, err)
		return s
	}

	tests := []struct {
		testName string
		ref      compute.FieldRef
		input    compute.Datum
		expected compute.Datum
	}{
		{"basic ref", compute.FieldRefName("a"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
			 	{"a": 6.125},
				{"a": 0.0},
				{"a": -1}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"ref one field", compute.FieldRefName("a"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
				{"a": 6.125, "b": 7.5},
				{"a": 0.0, "b": 2.125},
				{"a": -1, "b": 4.0}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"second field", compute.FieldRefName("b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
			`[
					{"a": 6.125, "b": 7.5},
					{"a": 0.0, "b": 2.125},
					{"a": -1, "b": 4.0}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[7.5, 2.125, 4.0]`))},
		{"nested field by path", compute.FieldRefPath(compute.FieldPath{0, 0}), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
				{"a": {"b": 6.125}},
				{"a": {"b": 0.0}},
				{"a": {"b": -1}}
			 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"nested field by name", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
					{"a": {"b": 6.125}},
					{"a": {"b": 0.0}},
					{"a": {"b": -1}}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, 0.0, -1]`))},
		{"nested field with nulls", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(fromJSON(
			arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}),
			`[
						{"a": {"b": 6.125}},
						{"a": null},
						{"a": {"b": null}}
					 ]`)), compute.NewDatumWithoutOwning(fromJSON(
			arrow.PrimitiveTypes.Float64, `[6.125, null, null]`))},
		{"nested scalar", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": {"b": 64.0}}]`)),
			compute.NewDatum(scalar.NewFloat64Scalar(64.0))},
		{"nested scalar with null", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": {"b": null}}]`)),
			compute.NewDatum(scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64))},
		{"nested scalar null", compute.FieldRefList("a", "b"), compute.NewDatumWithoutOwning(
			scalarFromJSON(arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.StructOf(
					arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true}),
					Nullable: true}), `[{"a": null}]`)),
			compute.NewDatum(scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64))},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			scoped := memory.NewCheckedAllocatorScope(mem)
			defer scoped.CheckSize(t)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			dt := tt.input.(compute.ArrayLikeDatum).Type().(arrow.NestedType)
			schema := arrow.NewSchema(dt.Fields(), nil)
			ref, err := exprs.NewFieldRef(tt.ref, schema, extSet)
			require.NoError(t, err)
			assert.NotNil(t, ref)

			actual, err := exprs.ExecuteScalarExpression(ctx, schema, ref, tt.input)
			require.NoError(t, err)
			defer actual.Release()

			assert.Truef(t, tt.expected.Equals(actual), "expected: %s\ngot: %s", tt.expected, actual)
		})
	}
}

func TestExecuteScalarFuncCall(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	fromJSON := func(ty arrow.DataType, json string) arrow.Array {
		arr, _, err := array.FromJSON(mem, ty, strings.NewReader(json))
		require.NoError(t, err)
		return arr
	}

	basicSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	nestedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.StructOf(basicSchema.Fields()...), Nullable: false},
	}, nil)

	bldr := exprs.NewExprBuilder(extSet)

	tests := []struct {
		name     string
		ex       exprs.Builder
		sc       *arrow.Schema
		input    compute.Datum
		expected compute.Datum
	}{
		{"add", bldr.MustCallScalar("add", nil, bldr.FieldRef("a"),
			bldr.Literal(expr.NewPrimitiveLiteral(float64(3.5), false))),
			basicSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(basicSchema.Fields()...),
				`[
				{"a": 6.125, "b": 3.375},
				{"a": 0.0, "b": 1},
				{"a": -1, "b": 4.75}
			]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[9.625, 3.5, 2.5]`))},
		{"add sub", bldr.MustCallScalar("add", nil, bldr.FieldRef("a"),
			bldr.MustCallScalar("subtract", nil,
				bldr.WrapLiteral(expr.NewLiteral(float64(3.5), false)),
				bldr.FieldRef("b"))),
			basicSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(basicSchema.Fields()...),
				`[
				{"a": 6.125, "b": 3.375},
				{"a": 0.0, "b": 1},
				{"a": -1, "b": 4.75}
			]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[6.25, 2.5, -2.25]`))},
		{"add nested", bldr.MustCallScalar("add", nil,
			bldr.FieldRefList("a", "a"), bldr.FieldRefList("a", "b")), nestedSchema,
			compute.NewDatumWithoutOwning(fromJSON(arrow.StructOf(nestedSchema.Fields()...),
				`[
					{"a": {"a": 6.125, "b": 3.375}},
					{"a": {"a": 0.0, "b": 1}},
					{"a": {"a": -1, "b": 4.75}}
				 ]`)), compute.NewDatumWithoutOwning(fromJSON(arrow.PrimitiveTypes.Float64,
				`[9.5, 1, 3.75]`))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scoped := memory.NewCheckedAllocatorScope(mem)
			defer scoped.CheckSize(t)

			bldr.SetInputSchema(tt.sc)
			ex, err := tt.ex.BuildExpr()
			require.NoError(t, err)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)
			dt := tt.input.(compute.ArrayLikeDatum).Type().(arrow.NestedType)
			schema := arrow.NewSchema(dt.Fields(), nil)

			actual, err := exprs.ExecuteScalarExpression(ctx, schema, ex, tt.input)
			require.NoError(t, err)
			defer actual.Release()

			assert.Truef(t, tt.expected.Equals(actual), "expected: %s\ngot: %s", tt.expected, actual)
		})
	}
}

func TestGenerateMask(t *testing.T) {
	sc, err := boringArrowSchema.AddField(0, arrow.Field{
		Name: "in", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
	require.NoError(t, err)

	bldr := exprs.NewExprBuilder(extSet)
	require.NoError(t, bldr.SetInputSchema(sc))

	tests := []struct {
		name   string
		json   string
		filter exprs.Builder
	}{
		{"simple", `[
			{"i32": 0, "f32": -0.1, "in": true},
			{"i32": 0, "f32":  0.3, "in": true},
			{"i32": 1, "f32":  0.2, "in": false},
			{"i32": 2, "f32": -0.1, "in": false},
			{"i32": 0, "f32":  0.1, "in": true},
			{"i32": 0, "f32": null, "in": true},
			{"i32": 0, "f32":  1.0, "in": true}
		]`, bldr.MustCallScalar("equal", nil,
			bldr.FieldRef("i32"), bldr.Literal(expr.NewPrimitiveLiteral(int32(0), false)))},
		{"complex", `[
			{"f64":  0.3, "f32":  0.1, "in": true},
			{"f64": -0.1, "f32":  0.3, "in": false},
			{"f64":  0.1, "f32":  0.2, "in": true},
			{"f64":  0.0, "f32": -0.1, "in": false},
			{"f64":  1.0, "f32":  0.1, "in": true},
			{"f64": -2.0, "f32": null, "in": null},
			{"f64":  3.0, "f32":  1.0, "in": true}
		]`, bldr.MustCallScalar("greater", nil,
			bldr.MustCallScalar("multiply", nil,
				bldr.Must(bldr.Cast(bldr.FieldRef("f32"), arrow.PrimitiveTypes.Float64)),
				bldr.FieldRef("f64")),
			bldr.Literal(expr.NewPrimitiveLiteral(float64(0), false)))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), mem), extSet)

			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(tt.json))
			require.NoError(t, err)
			defer rec.Release()

			input := compute.NewDatumWithoutOwning(rec)
			expectedMask := rec.Column(0)

			mask, err := exprs.ExecuteScalarExpression(ctx, sc,
				expr.MustExpr(tt.filter.BuildExpr()), input)
			require.NoError(t, err)
			defer mask.Release()

			assertEqual(t, expectedMask, mask)
		})
	}
}

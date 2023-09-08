// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package compute_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/assert"
)

func TestExpressionToString(t *testing.T) {
	ts, _ := scalar.MakeScalar("1990-10-23 10:23:33.123456").CastTo(arrow.FixedWidthTypes.Timestamp_ns)

	add := compute.NewCall("add", []compute.Expression{compute.NewFieldRef("beta"), compute.NewLiteral(3)}, &compute.ArithmeticOptions{})

	tests := []struct {
		expr     compute.Expression
		expected string
	}{
		{compute.NewFieldRef("alpha"), "alpha"},
		{compute.NewLiteral(3), "3"},
		{compute.NewLiteral("a"), `"a"`},
		{compute.NewLiteral("a\nb"), `"a\nb"`},
		{compute.NewLiteral(&scalar.Boolean{}), "null"},
		{compute.NewLiteral(&scalar.Int64{}), "null"},
		{compute.NewLiteral(scalar.NewBinaryScalar(memory.NewBufferBytes([]byte("az")),
			arrow.BinaryTypes.Binary)), `"617A"`},
		{compute.NewLiteral(ts), "1990-10-23 10:23:33.123456"},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewFieldRef("beta")}, nil), "add(3, beta)"},
		{compute.And(compute.NewFieldRef("a"), compute.NewFieldRef("b")), "(a and b)"},
		{compute.Or(compute.NewFieldRef("a"), compute.NewFieldRef("b")), "(a or b)"},
		{compute.Not(compute.NewFieldRef("a")), "invert(a)"},
		{compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int32),
			"cast(a, {to_type=int32, allow_int_overflow=false, allow_time_truncate=false, " +
				"allow_time_overflow=false, allow_decimal_truncate=false, " +
				"allow_float_truncate=false, allow_invalid_utf8=false})"},
		{compute.Cast(compute.NewFieldRef("a"), nil),
			"cast(a, {to_type=null, allow_int_overflow=false, allow_time_truncate=false, " +
				"allow_time_overflow=false, allow_decimal_truncate=false, " +
				"allow_float_truncate=false, allow_invalid_utf8=false})"},
		{compute.Equal(compute.NewFieldRef("a"), compute.NewLiteral(1)), "(a == 1)"},
		{compute.Less(compute.NewFieldRef("a"), compute.NewLiteral(2)), "(a < 2)"},
		{compute.Greater(compute.NewFieldRef("a"), compute.NewLiteral(3)), "(a > 3)"},
		{compute.NotEqual(compute.NewFieldRef("a"), compute.NewLiteral("a")), `(a != "a")`},
		{compute.LessEqual(compute.NewFieldRef("a"), compute.NewLiteral("b")), `(a <= "b")`},
		{compute.GreaterEqual(compute.NewFieldRef("a"), compute.NewLiteral("c")), `(a >= "c")`},
		{compute.Project(
			[]compute.Expression{
				compute.NewFieldRef("a"), compute.NewFieldRef("a"), compute.NewLiteral(3), add,
			}, []string{"a", "renamed_a", "three", "b"}),
			"{a=a, renamed_a=a, three=3, b=" + add.String() + "}"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.expr.String())
		})
	}
}

func TestExpressionEquality(t *testing.T) {
	tests := []struct {
		exp1  compute.Expression
		exp2  compute.Expression
		equal bool
	}{
		{compute.NewLiteral(1), compute.NewLiteral(1), true},
		{compute.NewLiteral(1), compute.NewLiteral(2), false},
		{compute.NewFieldRef("a"), compute.NewFieldRef("a"), true},
		{compute.NewFieldRef("a"), compute.NewFieldRef("b"), false},
		{compute.NewFieldRef("a"), compute.NewLiteral(2), false},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, nil),
			compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, nil), true},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, nil),
			compute.NewCall("add", []compute.Expression{compute.NewLiteral(2), compute.NewLiteral("a")}, nil), false},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, nil),
			compute.NewCall("add", []compute.Expression{compute.NewFieldRef("a"), compute.NewLiteral(3)}, nil), false},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, &compute.ArithmeticOptions{true}),
			compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, &compute.ArithmeticOptions{true}), true},
		{compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, &compute.ArithmeticOptions{true}),
			compute.NewCall("add", []compute.Expression{compute.NewLiteral(3), compute.NewLiteral("a")}, &compute.ArithmeticOptions{false}), false},
		{compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int32), compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int32), true},
		{compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int32), compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int64), false},
		{compute.Cast(compute.NewFieldRef("a"), arrow.PrimitiveTypes.Int32), compute.NewCall("cast", []compute.Expression{compute.NewFieldRef("a")}, compute.NewCastOptions(arrow.PrimitiveTypes.Int32, false)), false},
	}

	for _, tt := range tests {
		t.Run(tt.exp1.String(), func(t *testing.T) {
			assert.Equal(t, tt.equal, tt.exp1.Equals(tt.exp2))
		})
	}
}

func TestExpressionHashing(t *testing.T) {
	set := make(map[uint64]compute.Expression)

	e := compute.NewFieldRef("alpha")
	set[e.Hash()] = e

	e = compute.NewFieldRef("beta")
	_, ok := set[e.Hash()]
	assert.False(t, ok)
	set[e.Hash()] = e

	e = compute.NewFieldRef("beta")
	ex, ok := set[e.Hash()]
	assert.True(t, ok)
	assert.True(t, e.Equals(ex))

	e = compute.NewLiteral(1)
	set[e.Hash()] = e
	_, ok = set[compute.NewLiteral(1).Hash()]
	assert.True(t, ok)
	_, ok = set[compute.NewLiteral(3).Hash()]
	assert.False(t, ok)
	set[compute.NewLiteral(3).Hash()] = compute.NewLiteral(3)

	e = compute.NullLiteral(arrow.PrimitiveTypes.Int32)
	set[e.Hash()] = e
	_, ok = set[compute.NullLiteral(arrow.PrimitiveTypes.Int32).Hash()]
	assert.True(t, ok)
	e = compute.NullLiteral(arrow.PrimitiveTypes.Float32)
	_, ok = set[e.Hash()]
	assert.False(t, ok)
	set[e.Hash()] = e

	e = compute.NewCall("add", []compute.Expression{}, nil)
	set[e.Hash()] = e
	_, ok = set[compute.NewCall("add", nil, nil).Hash()]
	assert.True(t, ok)
	e = compute.NewCall("widgetify", nil, nil)
	_, ok = set[e.Hash()]
	assert.False(t, ok)
	set[e.Hash()] = e

	assert.Len(t, set, 8)
}

func TestIsScalarExpression(t *testing.T) {
	assert.True(t, compute.NewLiteral(true).IsScalarExpr())
	arr := array.MakeFromData(array.NewData(arrow.PrimitiveTypes.Int8, 0, []*memory.Buffer{nil, nil}, nil, 0, 0))
	defer arr.Release()

	assert.False(t, compute.NewLiteral(arr).IsScalarExpr())
	assert.True(t, compute.NewFieldRef("a").IsScalarExpr())
}

func TestExpressionIsSatisfiable(t *testing.T) {
	assert.True(t, compute.NewLiteral(true).IsSatisfiable())
	assert.False(t, compute.NewLiteral(false).IsSatisfiable())

	null := scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean)
	assert.False(t, compute.NewLiteral(null).IsSatisfiable())
	assert.True(t, compute.NewFieldRef("a").IsSatisfiable())
	assert.True(t, compute.Equal(compute.NewFieldRef("a"), compute.NewLiteral(1)).IsSatisfiable())
	// no constant folding here
	assert.True(t, compute.Equal(compute.NewLiteral(0), compute.NewLiteral(1)).IsSatisfiable())

	// when a top level conjunction contains an Expression which is certain to
	// evaluate to null, it can only evaluate to null or false
	neverTrue := compute.And(compute.NewLiteral(null), compute.NewFieldRef("a"))
	// this may appear in satisfiable filters if coalesced (for example, wrapped in fill_na)
	assert.True(t, compute.NewCall("is_null", []compute.Expression{neverTrue}, nil).IsSatisfiable())
}

func TestExpressionSerializationRoundTrip(t *testing.T) {
	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.AppendValues([]int32{1, 2, 3}, nil)
	lookupArr := bldr.NewArray()
	defer lookupArr.Release()

	intvalueset := compute.NewDatum(lookupArr)
	defer intvalueset.Release()

	bldr2 := array.NewFloat64Builder(memory.DefaultAllocator)
	defer bldr2.Release()

	bldr2.AppendValues([]float64{0.5, 1.0, 2.0}, nil)
	lookupArr = bldr2.NewArray()
	defer lookupArr.Release()

	fltvalueset := compute.NewDatum(lookupArr)
	defer fltvalueset.Release()

	tests := []struct {
		name string
		expr compute.Expression
	}{
		{"null literal", compute.NewLiteral(scalar.MakeNullScalar(arrow.Null))},
		{"null int32 literal", compute.NewLiteral(scalar.MakeNullScalar(arrow.PrimitiveTypes.Int32))},
		{"null struct literal", compute.NewLiteral(scalar.MakeNullScalar(arrow.StructOf(
			arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			arrow.Field{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		)))},
		{"literal true", compute.NewLiteral(true)},
		{"literal false", compute.NewLiteral(false)},
		{"literal int", compute.NewLiteral(1)},
		{"literal float", compute.NewLiteral(1.125)},
		{"stringy strings", compute.NewLiteral("stringy strings")},
		{"field ref", compute.NewFieldRef("field")},
		{"greater", compute.Greater(compute.NewFieldRef("a"), compute.NewLiteral(0.25))},
		{"or", compute.Or(
			compute.Equal(compute.NewFieldRef("a"), compute.NewLiteral(1)),
			compute.NotEqual(compute.NewFieldRef("b"), compute.NewLiteral("hello")),
			compute.Equal(compute.NewFieldRef("b"), compute.NewLiteral("foo bar")))},
		{"not", compute.Not(compute.NewFieldRef("alpha"))},
		{"is_in", compute.NewCall("is_in", []compute.Expression{compute.NewLiteral(1)}, &compute.SetLookupOptions{ValueSet: intvalueset})},
		{"is_in cast", compute.NewCall("is_in", []compute.Expression{
			compute.NewCall("cast", []compute.Expression{compute.NewFieldRef("version")}, compute.NewCastOptions(arrow.PrimitiveTypes.Float64, true))},
			&compute.SetLookupOptions{ValueSet: fltvalueset})},
		{"is valid", compute.IsValid(compute.NewFieldRef("validity"))},
		{"lots and", compute.And(
			compute.And(
				compute.GreaterEqual(compute.NewFieldRef("x"), compute.NewLiteral(-1.5)),
				compute.Less(compute.NewFieldRef("x"), compute.NewLiteral(0.0))),
			compute.And(compute.GreaterEqual(compute.NewFieldRef("y"), compute.NewLiteral(0.0)),
				compute.Less(compute.NewFieldRef("y"), compute.NewLiteral(1.5))),
			compute.And(compute.Greater(compute.NewFieldRef("z"), compute.NewLiteral(1.5)),
				compute.LessEqual(compute.NewFieldRef("z"), compute.NewLiteral(3.0))))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			serialized, err := compute.SerializeExpr(tt.expr, mem)
			assert.NoError(t, err)
			defer serialized.Release()
			roundTripped, err := compute.DeserializeExpr(mem, serialized)
			assert.NoError(t, err)
			defer roundTripped.Release()
			assert.Truef(t, tt.expr.Equals(roundTripped), "started with: %s, got: %s", tt.expr, roundTripped)
		})
	}
}

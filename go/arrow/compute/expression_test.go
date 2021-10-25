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

package compute_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/compute"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/arrow/scalar"
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

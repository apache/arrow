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
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/compute/exprs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
)

func TestNewScalarFunc(t *testing.T) {
	reg := exprs.NewDefaultExtensionSet()

	fn, err := exprs.NewScalarCall(reg, "add", nil,
		expr.NewPrimitiveLiteral(int32(1), false),
		expr.NewPrimitiveLiteral(int32(10), false))
	require.NoError(t, err)

	assert.Equal(t, "add(i32(1), i32(10), {overflow: [ERROR]}) => i32", fn.String())
	assert.Equal(t, "add:i32_i32", fn.CompoundName())
}

func TestFieldRefDotPath(t *testing.T) {
	f0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f1_0 := arrow.Field{Name: "be.ta", Type: arrow.PrimitiveTypes.Int32}
	f1 := arrow.Field{Name: "beta", Type: arrow.StructOf(f1_0)}
	f2_0 := arrow.Field{Name: "alpha", Type: arrow.PrimitiveTypes.Int32}
	f2_1_0 := arrow.Field{Name: "[alpha]", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)}
	f2_1_1 := arrow.Field{Name: "beta", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)}
	f2_1 := arrow.Field{Name: "gamma", Type: arrow.StructOf(f2_1_0, f2_1_1)}
	f2 := arrow.Field{Name: "gamma", Type: arrow.StructOf(f2_0, f2_1)}
	s := arrow.NewSchema([]arrow.Field{f0, f1, f2}, nil)

	tests := []struct {
		dotpath   string
		shouldErr bool
		expected  expr.ReferenceSegment
	}{
		{".alpha", false, &expr.StructFieldRef{Field: 0}},
		{"[2]", false, &expr.StructFieldRef{Field: 2}},
		{".beta[0]", false, &expr.StructFieldRef{Field: 1, Child: &expr.StructFieldRef{Field: 0}}},
		{"[2].gamma[1][5]", false, &expr.StructFieldRef{Field: 2,
			Child: &expr.StructFieldRef{Field: 1,
				Child: &expr.StructFieldRef{Field: 1,
					Child: &expr.ListElementRef{Offset: 5}}}}},
		{"[2].gamma[0].foobar", false, &expr.StructFieldRef{Field: 2,
			Child: &expr.StructFieldRef{Field: 1,
				Child: &expr.StructFieldRef{Field: 0,
					Child: &expr.MapKeyRef{MapKey: expr.NewPrimitiveLiteral("foobar", false)}}}}},
		{`[1].be\.ta`, false, &expr.StructFieldRef{Field: 1, Child: &expr.StructFieldRef{Field: 0}}},
		{`[2].gamma.\[alpha\]`, false, &expr.StructFieldRef{Field: 2,
			Child: &expr.StructFieldRef{Field: 1,
				Child: &expr.StructFieldRef{Field: 0}}}},
		{`[5]`, true, nil},     // bad struct index
		{``, true, nil},        // empty
		{`delta`, true, nil},   // not found
		{`[1234`, true, nil},   // bad syntax
		{`[1stuf]`, true, nil}, // bad syntax
	}

	for _, tt := range tests {
		t.Run(tt.dotpath, func(t *testing.T) {
			ref, err := exprs.NewFieldRefFromDotPath(tt.dotpath, s)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Truef(t, tt.expected.Equals(ref), "expected: %s\ngot: %s", tt.expected, ref)
			}
		})
	}
}

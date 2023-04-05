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
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArityBasics(t *testing.T) {
	nullary := compute.Nullary()
	assert.Equal(t, 0, nullary.NArgs)
	assert.False(t, nullary.IsVarArgs)

	unary := compute.Unary()
	assert.Equal(t, 1, unary.NArgs)
	assert.False(t, unary.IsVarArgs)

	binary := compute.Binary()
	assert.Equal(t, 2, binary.NArgs)
	assert.False(t, binary.IsVarArgs)

	ternary := compute.Ternary()
	assert.Equal(t, 3, ternary.NArgs)
	assert.False(t, ternary.IsVarArgs)

	varargs := compute.VarArgs(2)
	assert.Equal(t, 2, varargs.NArgs)
	assert.True(t, varargs.IsVarArgs)
}

func CheckDispatchBest(t *testing.T, funcName string, originalTypes, expected []arrow.DataType) {
	fn, exists := compute.GetFunctionRegistry().GetFunction(funcName)
	require.True(t, exists)

	vals := make([]arrow.DataType, len(originalTypes))
	copy(vals, originalTypes)

	actualKernel, err := fn.DispatchBest(vals...)
	require.NoError(t, err)
	expKernel, err := fn.DispatchExact(expected...)
	require.NoError(t, err)

	assert.Same(t, expKernel, actualKernel)
	assert.Equal(t, len(expected), len(vals))
	for i, v := range vals {
		assert.True(t, arrow.TypeEqual(v, expected[i]), v.String(), expected[i].String())
	}
}

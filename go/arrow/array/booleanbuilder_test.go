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
	"testing"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBooleanBuilder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewBooleanBuilder(mem)

	exp := tools.Bools(1, 1, 0, 1, 1, 0)
	got := make([]bool, len(exp)+2)

	b.AppendValues(exp, nil)
	assert.NoError(t, b.AppendValueFromString("true"))
	assert.NoError(t, b.AppendValueFromString("false"))
	exp = tools.Bools(1, 1, 0, 1, 1, 0, 1, 0)
	a := b.NewBooleanArray()
	b.Release()
	for i := 0; i < a.Len(); i++ {
		got[i] = a.Value(i)
	}
	assert.Equal(t, exp, got)

	a.Release()
}

func TestBooleanBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBooleanBuilder(mem)
	defer ab.Release()

	want := tools.Bools(1, 1, 0, 1, 1, 0, 1, 0)

	boolValues := func(a *array.Boolean) []bool {
		vs := make([]bool, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	ab.AppendValues([]bool{}, nil)
	a := ab.NewBooleanArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewBooleanArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(want, nil)
	a = ab.NewBooleanArray()
	assert.Equal(t, want, boolValues(a))
	a.Release()

	ab.AppendValues([]bool{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewBooleanArray()
	assert.Equal(t, want, boolValues(a))
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]bool{}, nil)
	a = ab.NewBooleanArray()
	assert.Equal(t, want, boolValues(a))
	a.Release()
}

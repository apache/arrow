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

package exec_test

import (
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestRechunkConsistentArraysTrivial(t *testing.T) {
	var groups [][]arrow.Array
	rechunked := exec.RechunkArraysConsistently(groups)
	assert.Zero(t, rechunked)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	a1 := exec.ArrayFromSlice(mem, []int16{})
	defer a1.Release()
	a2 := exec.ArrayFromSlice(mem, []int16{})
	defer a2.Release()
	b1 := exec.ArrayFromSlice(mem, []int32{})
	defer b1.Release()
	groups = [][]arrow.Array{{a1, a2}, {}, {b1}}
	rechunked = exec.RechunkArraysConsistently(groups)
	assert.Len(t, rechunked, 3)

	for _, arrvec := range rechunked {
		for _, arr := range arrvec {
			assert.Zero(t, arr.Len())
		}
	}
}

func assertEqual[T exec.NumericTypes](t *testing.T, mem memory.Allocator, arr arrow.Array, data []T) {
	exp := exec.ArrayFromSlice(mem, data)
	defer exp.Release()
	assert.Truef(t, array.Equal(exp, arr), "expected: %s\ngot: %s", exp, arr)
}

func TestRechunkArraysConsistentlyPlain(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	a1 := exec.ArrayFromSlice(mem, []int16{1, 2, 3})
	defer a1.Release()
	a2 := exec.ArrayFromSlice(mem, []int16{4, 5})
	defer a2.Release()
	a3 := exec.ArrayFromSlice(mem, []int16{6, 7, 8, 9})
	defer a3.Release()

	b1 := exec.ArrayFromSlice(mem, []int32{41, 42})
	defer b1.Release()
	b2 := exec.ArrayFromSlice(mem, []int32{43, 44, 45})
	defer b2.Release()
	b3 := exec.ArrayFromSlice(mem, []int32{46, 47})
	defer b3.Release()
	b4 := exec.ArrayFromSlice(mem, []int32{48, 49})
	defer b4.Release()

	groups := [][]arrow.Array{{a1, a2, a3}, {b1, b2, b3, b4}}
	rechunked := exec.RechunkArraysConsistently(groups)
	assert.Len(t, rechunked, 2)
	ra := rechunked[0]
	rb := rechunked[1]

	assert.Len(t, ra, 5)
	assertEqual(t, mem, ra[0], []int16{1, 2})
	ra[0].Release()
	assertEqual(t, mem, ra[1], []int16{3})
	ra[1].Release()
	assertEqual(t, mem, ra[2], []int16{4, 5})
	ra[2].Release()
	assertEqual(t, mem, ra[3], []int16{6, 7})
	ra[3].Release()
	assertEqual(t, mem, ra[4], []int16{8, 9})
	ra[4].Release()

	assert.Len(t, rb, 5)
	assertEqual(t, mem, rb[0], []int32{41, 42})
	rb[0].Release()
	assertEqual(t, mem, rb[1], []int32{43})
	rb[1].Release()
	assertEqual(t, mem, rb[2], []int32{44, 45})
	rb[2].Release()
	assertEqual(t, mem, rb[3], []int32{46, 47})
	rb[3].Release()
	assertEqual(t, mem, rb[4], []int32{48, 49})
	rb[4].Release()
}

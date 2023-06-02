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

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNullArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	b := array.NewNullBuilder(pool)
	defer b.Release()

	b.AppendNull()
	b.AppendNull()

	arr1 := b.NewArray().(*array.Null)
	defer arr1.Release()

	if got, want := arr1.Len(), 2; got != want {
		t.Fatalf("invalid null array length: got=%d, want=%d", got, want)
	}

	if got, want := arr1.NullN(), 2; got != want {
		t.Fatalf("invalid number of nulls: got=%d, want=%d", got, want)
	}

	if got, want := arr1.DataType(), arrow.Null; got != want {
		t.Fatalf("invalid null data type: got=%v, want=%v", got, want)
	}

	arr1.Retain()
	arr1.Release()

	if arr1.Data() == nil {
		t.Fatalf("invalid null data")
	}

	arr2 := b.NewNullArray()
	defer arr2.Release()

	if got, want := arr2.Len(), 0; got != want {
		t.Fatalf("invalid null array length: got=%d, want=%d", got, want)
	}

	arr3 := array.NewNull(10)
	defer arr3.Release()

	if got, want := arr3.Len(), 10; got != want {
		t.Fatalf("invalid null array length: got=%d, want=%d", got, want)
	}

	if got, want := arr3.NullN(), 10; got != want {
		t.Fatalf("invalid number of nulls: got=%d, want=%d", got, want)
	}

}

func TestNullStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewNullBuilder(mem)
	defer b.Release()

	b.AppendNull()
	b.AppendNull()

	arr := b.NewArray().(*array.Null)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewNullBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Null)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

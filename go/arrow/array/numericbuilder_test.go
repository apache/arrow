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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewFloat64Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewFloat64Builder(mem)

	ab.Append(1)
	ab.Append(2)
	ab.Append(3)
	ab.AppendNull()
	ab.Append(5)
	ab.Append(6)
	ab.AppendNull()
	ab.Append(8)
	ab.Append(9)
	ab.Append(10)

	// check state of builder before NewFloat64Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewFloat64Array()

	// check state of builder after NewFloat64Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewFloat64Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewFloat64Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewFloat64Array did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")
	assert.Equal(t, []float64{1, 2, 3, 0, 5, 6, 0, 8, 9, 10}, a.Float64Values(), "unexpected Float64Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.Float64Values(), 10, "unexpected length of Float64Values")

	a.Release()

	ab.Append(7)
	ab.Append(8)

	a = ab.NewFloat64Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []float64{7, 8}, a.Float64Values())
	assert.Len(t, a.Float64Values(), 2)

	a.Release()
}

func TestFloat64Builder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewFloat64Builder(mem)

	exp := []float64{1.0, 1.1, 1.2, 1.3}
	ab.AppendValues(exp, nil)
	a := ab.NewFloat64Array()
	assert.Equal(t, exp, a.Float64Values())

	a.Release()
	ab.Release()
}

func TestFloat64Builder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewFloat64Builder(mem)

	exp := []float64{1.0, 1.1, 1.2, 1.3}
	ab.AppendValues(exp, nil)
	a := ab.NewFloat64Array()
	assert.Equal(t, exp, a.Float64Values())
	a.Release()

	a = ab.NewFloat64Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.Release()
}

func TestFloat64Builder_Resize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewFloat64Builder(mem)

	assert.Equal(t, 0, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	ab.Reserve(63)
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	for i := 0; i < 63; i++ {
		ab.Append(0)
	}
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 63, ab.Len())

	ab.Resize(5)
	assert.Equal(t, 5, ab.Len())

	ab.Resize(32)
	assert.Equal(t, 5, ab.Len())

	ab.Release()
}

func TestNewTime32Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time32Type{Unit: arrow.Second}
	ab := array.NewTime32Builder(mem, dtype)

	ab.Append(1)
	ab.Append(2)
	ab.Append(3)
	ab.AppendNull()
	ab.Append(5)
	ab.Append(6)
	ab.AppendNull()
	ab.Append(8)
	ab.Append(9)
	ab.Append(10)

	// check state of builder before NewTime32Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewTime32Array()

	// check state of builder after NewTime32Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewTime32Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewTime32Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewTime32Array did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")
	assert.Equal(t, []arrow.Time32{1, 2, 3, 0, 5, 6, 0, 8, 9, 10}, a.Time32Values(), "unexpected Time32Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.Time32Values(), 10, "unexpected length of Time32Values")

	a.Release()

	ab.Append(7)
	ab.Append(8)

	a = ab.NewTime32Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []arrow.Time32{7, 8}, a.Time32Values())
	assert.Len(t, a.Time32Values(), 2)

	a.Release()
}

func TestTime32Builder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time32Type{Unit: arrow.Second}
	ab := array.NewTime32Builder(mem, dtype)

	exp := []arrow.Time32{0, 1, 2, 3}
	ab.AppendValues(exp, nil)
	a := ab.NewTime32Array()
	assert.Equal(t, exp, a.Time32Values())

	a.Release()
	ab.Release()
}

func TestTime32Builder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time32Type{Unit: arrow.Second}
	ab := array.NewTime32Builder(mem, dtype)

	exp := []arrow.Time32{0, 1, 2, 3}
	ab.AppendValues(exp, nil)
	a := ab.NewTime32Array()
	assert.Equal(t, exp, a.Time32Values())
	a.Release()

	a = ab.NewTime32Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.Release()
}

func TestTime32Builder_Resize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time32Type{Unit: arrow.Second}
	ab := array.NewTime32Builder(mem, dtype)

	assert.Equal(t, 0, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	ab.Reserve(63)
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	for i := 0; i < 63; i++ {
		ab.Append(0)
	}
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 63, ab.Len())

	ab.Resize(5)
	assert.Equal(t, 5, ab.Len())

	ab.Resize(32)
	assert.Equal(t, 5, ab.Len())

	ab.Release()
}

func TestNewTime64Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time64Type{Unit: arrow.Second}
	ab := array.NewTime64Builder(mem, dtype)

	ab.Append(1)
	ab.Append(2)
	ab.Append(3)
	ab.AppendNull()
	ab.Append(5)
	ab.Append(6)
	ab.AppendNull()
	ab.Append(8)
	ab.Append(9)
	ab.Append(10)

	// check state of builder before NewTime64Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewTime64Array()

	// check state of builder after NewTime64Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewTime64Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewTime64Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewTime64Array did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")
	assert.Equal(t, []arrow.Time64{1, 2, 3, 0, 5, 6, 0, 8, 9, 10}, a.Time64Values(), "unexpected Time64Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.Time64Values(), 10, "unexpected length of Time64Values")

	a.Release()

	ab.Append(7)
	ab.Append(8)

	a = ab.NewTime64Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []arrow.Time64{7, 8}, a.Time64Values())
	assert.Len(t, a.Time64Values(), 2)

	a.Release()
}

func TestTime64Builder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time64Type{Unit: arrow.Second}
	ab := array.NewTime64Builder(mem, dtype)

	exp := []arrow.Time64{0, 1, 2, 3}
	ab.AppendValues(exp, nil)
	a := ab.NewTime64Array()
	assert.Equal(t, exp, a.Time64Values())

	a.Release()
	ab.Release()
}

func TestTime64Builder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time64Type{Unit: arrow.Second}
	ab := array.NewTime64Builder(mem, dtype)

	exp := []arrow.Time64{0, 1, 2, 3}
	ab.AppendValues(exp, nil)
	a := ab.NewTime64Array()
	assert.Equal(t, exp, a.Time64Values())
	a.Release()

	a = ab.NewTime64Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.Release()
}

func TestTime64Builder_Resize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Time64Type{Unit: arrow.Second}
	ab := array.NewTime64Builder(mem, dtype)

	assert.Equal(t, 0, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	ab.Reserve(63)
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 0, ab.Len())

	for i := 0; i < 63; i++ {
		ab.Append(0)
	}
	assert.Equal(t, 64, ab.Cap())
	assert.Equal(t, 63, ab.Len())

	ab.Resize(5)
	assert.Equal(t, 5, ab.Len())

	ab.Resize(32)
	assert.Equal(t, 5, ab.Len())

	ab.Release()
}

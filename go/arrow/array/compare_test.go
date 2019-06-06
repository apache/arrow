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

package array

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestBaseArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b1 := NewBooleanBuilder(mem)
	defer b1.Release()
	b1.Append(true)
	a1 := b1.NewBooleanArray()
	defer a1.Release()

	b2 := NewBooleanBuilder(mem)
	defer b2.Release()
	a2 := b2.NewBooleanArray()
	defer a2.Release()

	if baseArrayEquals(a1, a2) {
		t.Errorf("two arrays with different lengths must not be equal")
	}

	b3 := NewBooleanBuilder(mem)
	defer b3.Release()
	b3.AppendNull()
	a3 := b3.NewBooleanArray()
	defer a3.Release()

	if baseArrayEquals(a1, a3) {
		t.Errorf("two arrays with different number of null values must not be equal")
	}

	b4 := NewInt32Builder(mem)
	defer b4.Release()
	b4.Append(0)
	a4 := b4.NewInt32Array()
	defer a4.Release()

	if baseArrayEquals(a1, a4) {
		t.Errorf("two arrays with different types must not be equal")
	}

	b5 := NewBooleanBuilder(mem)
	defer b5.Release()
	b5.AppendNull()
	b5.Append(true)
	a5 := b5.NewBooleanArray()
	defer a5.Release()
	b1.AppendNull()

	if baseArrayEquals(a1, a5) {
		t.Errorf("two arrays with different validity bitmaps must not be equal")
	}
}

func TestBooleanArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	v1 := []bool{true, false, true}
	v2 := []bool{false, false, true}

	b1 := NewBooleanBuilder(mem)
	b2 := NewBooleanBuilder(mem)
	defer b1.Release()
	defer b2.Release()

	for _, v := range v1 {
		b1.Append(v)
	}

	for _, v := range v2 {
		b2.Append(v)
	}

	a1 := b1.NewBooleanArray()
	a2 := b2.NewBooleanArray()
	defer a1.Release()
	defer a2.Release()

	if !ArrayEquals(a1, a1) || !ArrayEquals(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if ArrayEquals(a1, a2) {
		t.Errorf("%q is not equal to %q", a1, a2)
	}
}

func TestFixedSizeBinaryArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	v1 := [][]byte{
		[]byte("QWERTY"),
		nil,
		[]byte("123456"),
	}
	vv1 := []bool{true, false, true}

	v2 := [][]byte{
		[]byte("AZERTY"),
		nil,
		[]byte("123456"),
	}
	vv2 := []bool{true, false, true}

	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 7}
	b1 := NewFixedSizeBinaryBuilder(mem, dtype)
	defer b1.Release()
	b2 := NewFixedSizeBinaryBuilder(mem, dtype)
	defer b2.Release()

	b1.AppendValues(v1, vv1)
	b2.AppendValues(v2, vv2)

	a1 := b1.NewFixedSizeBinaryArray()
	defer a1.Release()
	a2 := b2.NewFixedSizeBinaryArray()
	defer a2.Release()

	if !ArrayEquals(a1, a1) || !ArrayEquals(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if ArrayEquals(a1, a2) {
		t.Errorf("%v is not equal to %v", a1, a2)
	}
}

func TestBinaryArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	v1 := [][]byte{
		[]byte("Apache"),
		nil,
		[]byte("Arrow"),
	}
	vv1 := []bool{true, false, true}

	v2 := [][]byte{
		[]byte("Apache"),
		nil,
		[]byte("Foundation"),
	}
	vv2 := []bool{true, false, true}

	b1 := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b1.Release()
	b2 := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b2.Release()

	b1.AppendValues(v1, vv1)
	b2.AppendValues(v2, vv2)

	a1 := b1.NewBinaryArray()
	defer a1.Release()
	a2 := b2.NewBinaryArray()
	defer a2.Release()

	if !ArrayEquals(a1, a1) || !ArrayEquals(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if ArrayEquals(a1, a2) {
		t.Errorf("%v is not equal to %v", a1, a2)
	}
}

func TestNullArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b1 := NewNullBuilder(mem)
	defer b1.Release()
	b2 := NewNullBuilder(mem)
	defer b2.Release()
	b3 := NewNullBuilder(mem)
	defer b3.Release()

	b1.AppendNull()
	b1.AppendNull()

	b2.AppendNull()
	b2.AppendNull()

	b3.AppendNull()

	a1 := b1.NewNullArray()
	defer a1.Release()
	a2 := b2.NewNullArray()
	defer a2.Release()
	a3 := b3.NewNullArray()
	defer a3.Release()

	if !ArrayEquals(a1, a1) || !ArrayEquals(a2, a2) || !ArrayEquals(a3, a3) {
		t.Errorf("an array must be equal to itself")
	}

	if !ArrayEquals(a1, a2) {
		t.Errorf("%v must be equal to %v", a1, a2)
	}

	if ArrayEquals(a1, a3) {
		t.Errorf("%v must not be equal to %v", a1, a3)
	}
}

func TestStringArrayEquals(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b1 := NewStringBuilder(mem)
	defer b1.Release()
	b2 := NewStringBuilder(mem)
	defer b2.Release()

	b1.Append("Apache")
	b1.AppendNull()
	b1.Append("Arrow")

	b2.Append("Apache")
	b2.AppendNull()
	b2.Append("Foundation")

	a1 := b1.NewStringArray()
	defer a1.Release()
	a2 := b2.NewStringArray()
	defer a2.Release()

	if !ArrayEquals(a1, a1) || !ArrayEquals(a2, a2) {
		t.Errorf("an array must be equal to itself")
	}

	if ArrayEquals(a1, a2) {
		t.Errorf("%v must not be equal to %v", a1, a2)
	}
}

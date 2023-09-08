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

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewDecimal256Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 10, Scale: 1})
	defer ab.Release()

	ab.Retain()
	ab.Release()

	want := []decimal256.Num{
		decimal256.New(1, 1, 1, 1),
		decimal256.New(2, 2, 2, 2),
		decimal256.New(3, 3, 3, 3),
		{},
		decimal256.FromI64(-5),
		decimal256.FromI64(-6),
		{},
		decimal256.FromI64(8),
		decimal256.FromI64(9),
		decimal256.FromI64(10),
	}
	valids := []bool{true, true, true, false, true, true, false, true, true, true}

	for i, valid := range valids {
		switch {
		case valid:
			ab.Append(want[i])
		default:
			ab.AppendNull()
		}
	}

	// check state of builder before NewDecimal256Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewArray().(*array.Decimal256)
	a.Retain()
	a.Release()

	// check state of builder after NewDecimal256Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewDecimal256Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewDecimal256Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewDecimal256Array did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")

	assert.Equal(t, want, a.Values(), "unexpected Decimal256Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Equal(t, 4, a.Data().Buffers()[0].Len(), "should be 4 bytes due to minBuilderCapacity")
	assert.Len(t, a.Values(), 10, "unexpected length of Decimal256Values")
	assert.Equal(t, 10*arrow.Decimal256SizeBytes, a.Data().Buffers()[1].Len())

	a.Release()
	ab.Append(decimal256.FromI64(7))
	ab.Append(decimal256.FromI64(8))

	a = ab.NewDecimal256Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, 4, a.Data().Buffers()[0].Len(), "should be 4 bytes due to minBuilderCapacity")
	assert.Equal(t, []decimal256.Num{decimal256.FromI64(7), decimal256.FromI64(8)}, a.Values())
	assert.Len(t, a.Values(), 2)
	assert.Equal(t, 2*arrow.Decimal256SizeBytes, a.Data().Buffers()[1].Len())

	a.Release()
}

func TestDecimal256Builder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 10, Scale: 1})
	defer ab.Release()

	want := []decimal256.Num{decimal256.FromI64(3), decimal256.FromI64(4)}

	ab.AppendValues([]decimal256.Num{}, nil)
	a := ab.NewDecimal256Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewDecimal256Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(want, nil)
	a = ab.NewDecimal256Array()
	assert.Equal(t, want, a.Values())
	a.Release()

	ab.AppendValues([]decimal256.Num{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewDecimal256Array()
	assert.Equal(t, want, a.Values())
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]decimal256.Num{}, nil)
	a = ab.NewDecimal256Array()
	assert.Equal(t, want, a.Values())
	a.Release()
}

func TestDecimal256Slice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Decimal256Type{Precision: 10, Scale: 1}
	b := array.NewDecimal256Builder(mem, dtype)
	defer b.Release()

	var data = []decimal256.Num{
		decimal256.FromI64(-1),
		decimal256.FromI64(+0),
		decimal256.FromI64(+1),
		decimal256.New(4, 4, 4, 4),
	}
	b.AppendValues(data[:2], nil)
	b.AppendNull()
	b.Append(data[3])

	arr := b.NewDecimal256Array()
	defer arr.Release()

	if got, want := arr.Len(), len(data); got != want {
		t.Fatalf("invalid array length: got=%d, want=%d", got, want)
	}

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.Decimal256)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) {[4 4 4 4]}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
	assert.Equal(t, array.NullValueStr, v.ValueStr(0))
	assert.Equal(t, "2.510840694e+57", v.ValueStr(1))

	if got, want := v.NullN(), 1; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if got, want := v.Data().Offset(), 2; got != want {
		t.Fatalf("invalid offset: got=%d, want=%d", got, want)
	}
}

func TestDecimal256StringRoundTrip(t *testing.T) {
	dt := &arrow.Decimal256Type{Precision: 70, Scale: 10}
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewDecimal256Builder(mem, dt)
	defer b.Release()

	values := []decimal256.Num{
		decimal256.New(1, 1, 1, 1),
		decimal256.New(2, 2, 2, 2),
		decimal256.New(3, 3, 3, 3),
		{},
		decimal256.FromI64(-5),
		decimal256.FromI64(-6),
		{},
		decimal256.FromI64(8),
		decimal256.FromI64(9),
		decimal256.FromI64(10),
	}
	valid := []bool{true, true, true, false, true, true, false, true, true, true}

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.Decimal256)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewDecimal256Builder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Decimal256)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

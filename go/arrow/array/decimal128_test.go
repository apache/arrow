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
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewDecimal128Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 1})
	defer ab.Release()

	ab.Retain()
	ab.Release()

	want := []decimal128.Num{
		decimal128.New(1, 1),
		decimal128.New(2, 2),
		decimal128.New(3, 3),
		{},
		decimal128.FromI64(-5),
		decimal128.FromI64(-6),
		{},
		decimal128.FromI64(8),
		decimal128.FromI64(9),
		decimal128.FromI64(10),
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

	// check state of builder before NewDecimal128Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewArray().(*array.Decimal128)
	a.Retain()
	a.Release()

	// check state of builder after NewDecimal128Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewDecimal128Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewDecimal128Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewDecimal128Array did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")

	assert.Equal(t, want, a.Values(), "unexpected Decimal128Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.Values(), 10, "unexpected length of Decimal128Values")

	a.Release()
	ab.Append(decimal128.FromI64(7))
	ab.Append(decimal128.FromI64(8))

	a = ab.NewDecimal128Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []decimal128.Num{decimal128.FromI64(7), decimal128.FromI64(8)}, a.Values())
	assert.Len(t, a.Values(), 2)

	a.Release()
}

func TestDecimal128Builder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 1})
	defer ab.Release()

	want := []decimal128.Num{decimal128.FromI64(3), decimal128.FromI64(4)}

	ab.AppendValues([]decimal128.Num{}, nil)
	a := ab.NewDecimal128Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewDecimal128Array()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(want, nil)
	a = ab.NewDecimal128Array()
	assert.Equal(t, want, a.Values())
	a.Release()

	ab.AppendValues([]decimal128.Num{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewDecimal128Array()
	assert.Equal(t, want, a.Values())
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]decimal128.Num{}, nil)
	a = ab.NewDecimal128Array()
	assert.Equal(t, want, a.Values())
	a.Release()
}

func TestDecimal128Slice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.Decimal128Type{Precision: 10, Scale: 1}
	b := array.NewDecimal128Builder(mem, dtype)
	defer b.Release()

	var data = []decimal128.Num{
		decimal128.FromI64(-1),
		decimal128.FromI64(+0),
		decimal128.FromI64(+1),
		decimal128.New(-4, 4),
	}
	b.AppendValues(data[:2], nil)
	b.AppendNull()
	b.Append(data[3])

	arr := b.NewDecimal128Array()
	defer arr.Release()

	if got, want := arr.Len(), len(data); got != want {
		t.Fatalf("invalid array length: got=%d, want=%d", got, want)
	}

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.Decimal128)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) {4 -4}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if got, want := v.NullN(), 1; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if got, want := v.Data().Offset(), 2; got != want {
		t.Fatalf("invalid offset: got=%d, want=%d", got, want)
	}
}

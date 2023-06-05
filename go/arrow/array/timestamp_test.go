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
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestTimestampStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dt := &arrow.TimestampType{Unit: arrow.Second}
	b := array.NewTimestampBuilder(mem, dt)
	defer b.Release()

	b.Append(1)
	b.Append(2)
	b.Append(3)
	b.AppendNull()
	b.Append(5)
	b.Append(6)
	b.AppendNull()
	b.Append(8)
	b.Append(9)
	b.Append(10)

	arr := b.NewArray().(*array.Timestamp)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewTimestampBuilder(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Timestamp)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestNewTimestampBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	timestamp := time.Now()
	dtype := &arrow.TimestampType{Unit: arrow.Second}
	ab := array.NewTimestampBuilder(mem, dtype)
	defer ab.Release()

	ab.Retain()
	ab.Release()

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
	ab.AppendTime(timestamp)

	// check state of builder before NewTimestampArray
	assert.Equal(t, 11, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewTimestampArray()

	// check state of builder after NewTimestampArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewTimestampArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewTimestampArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewTimestampArray did not reset state")

	// check state of array
	assert.Equal(t, 2, a.NullN(), "unexpected null count")
	assert.Equal(t, []arrow.Timestamp{1, 2, 3, 0, 5, 6, 0, 8, 9, 10, arrow.Timestamp(timestamp.Unix())}, a.TimestampValues(), "unexpected TimestampValues")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.TimestampValues(), 11, "unexpected length of TimestampValues")

	a.Release()

	ab.Append(7)
	ab.Append(8)

	a = ab.NewTimestampArray()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []arrow.Timestamp{7, 8}, a.TimestampValues())
	assert.Len(t, a.TimestampValues(), 2)

	a.Release()

	var (
		want   = []arrow.Timestamp{1, 2, 3, 4}
		valids = []bool{true, true, false, true}
	)

	ab.AppendValues(want, valids)
	a = ab.NewTimestampArray()

	sub := array.MakeFromData(a.Data())
	defer sub.Release()

	if got, want := sub.DataType().ID(), a.DataType().ID(); got != want {
		t.Fatalf("invalid type: got=%q, want=%q", got, want)
	}

	if _, ok := sub.(*array.Timestamp); !ok {
		t.Fatalf("could not type-assert to array.Timestamp")
	}

	if got, want := a.String(), `[1 2 (null) 4]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	slice := array.NewSliceData(a.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.Timestamp)
	if !ok {
		t.Fatalf("could not type-assert to array.Timestamp")
	}

	if got, want := v.String(), `[(null) 4]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	a.Release()
}

func TestTimestampBuilder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.TimestampType{Unit: arrow.Second}
	ab := array.NewTimestampBuilder(mem, dtype)
	defer ab.Release()

	exp := []arrow.Timestamp{0, 1, 2, 3}
	ab.AppendValues(exp, nil)
	a := ab.NewTimestampArray()
	assert.Equal(t, exp, a.TimestampValues())

	a.Release()
}

func TestTimestampBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.TimestampType{Unit: arrow.Second}
	ab := array.NewTimestampBuilder(mem, dtype)
	defer ab.Release()

	exp := []arrow.Timestamp{0, 1, 2, 3}

	ab.AppendValues([]arrow.Timestamp{}, nil)
	a := ab.NewTimestampArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewTimestampArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues([]arrow.Timestamp{}, nil)
	ab.AppendValues(exp, nil)
	a = ab.NewTimestampArray()
	assert.Equal(t, exp, a.TimestampValues())
	a.Release()

	ab.AppendValues(exp, nil)
	ab.AppendValues([]arrow.Timestamp{}, nil)
	a = ab.NewTimestampArray()
	assert.Equal(t, exp, a.TimestampValues())
	a.Release()
}

func TestTimestampBuilder_Resize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.TimestampType{Unit: arrow.Second}
	ab := array.NewTimestampBuilder(mem, dtype)
	defer ab.Release()

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
}

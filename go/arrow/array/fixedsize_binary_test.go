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

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := array.NewFixedSizeBinaryBuilder(mem, &dtype)

	zero := make([]byte, dtype.ByteWidth)

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	valid := []bool{true, false, true}
	b.AppendValues(values, valid)

	b.Retain()
	b.Release()

	a := b.NewFixedSizeBinaryArray()
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, zero, a.Value(1))
	assert.Equal(t, true, a.IsNull(1))
	assert.Equal(t, false, a.IsValid(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*array.FixedSizeBinary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, zero, a.Value(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	b.Release()
}

func TestFixedSizeBinarySlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 4}
	b := array.NewFixedSizeBinaryBuilder(mem, dtype)
	defer b.Release()

	var data = [][]byte{
		[]byte("ABCD"),
		[]byte("1234"),
		nil,
		[]byte("AZER"),
	}
	b.AppendValues(data[:2], nil)
	b.AppendNull()
	b.Append(data[3])

	arr := b.NewFixedSizeBinaryArray()
	defer arr.Release()

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.FixedSizeBinary)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) "AZER"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if got, want := v.NullN(), 1; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

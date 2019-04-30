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
	"github.com/stretchr/testify/assert"
)

func TestFixedSizeBinaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := NewFixedSizeBinaryBuilder(mem, &dtype)

	b.Append([]byte("1234567"))
	b.AppendNull()
	b.Append([]byte("ABCDEFG"))
	b.AppendNull()

	assert.Equal(t, 4, b.Len(), "unexpected Len()")
	assert.Equal(t, 2, b.NullN(), "unexpected NullN()")

	assert.Equal(t, b.Value(0), []byte("1234567"))
	assert.Equal(t, b.Value(1), []byte{})
	assert.Equal(t, b.Value(2), []byte("ABCDEFG"))
	assert.Equal(t, b.Value(3), []byte{})

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	b.AppendValues(values, []bool{true, false, true})

	assert.Equal(t, 7, b.Len(), "unexpected Len()")
	assert.Equal(t, 3, b.NullN(), "unexpected NullN()")

	assert.Equal(t, []byte("7654321"), b.Value(4))
	assert.Equal(t, []byte{}, b.Value(5))
	assert.Equal(t, []byte("AZERTYU"), b.Value(6))

	a := b.NewFixedSizeBinaryArray()

	// check state of builder after NewFixedSizeBinaryArray
	assert.Zero(t, b.Len(), "unexpected ArrayBuilder.Len(), NewFixedSizeBinaryArray did not reset state")
	assert.Zero(t, b.Cap(), "unexpected ArrayBuilder.Cap(), NewFixedSizeBinaryArray did not reset state")
	assert.Zero(t, b.NullN(), "unexpected ArrayBuilder.NullN(), NewFixedSizeBinaryArray did not reset state")
	assert.Equal(t, a.String(), `["1234567" (null) "ABCDEFG" (null) "7654321" (null) "AZERTYU"]`)

	b.Release()
	a.Release()
}

func TestFixedSizeBinaryBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	ab := NewFixedSizeBinaryBuilder(mem, &dtype)
	defer ab.Release()

	want := [][]byte{
		[]byte("1234567"),
		[]byte("AZERTYU"),
		[]byte("7654321"),
	}

	fixedSizeValues := func(a *FixedSizeBinary) [][]byte {
		vs := make([][]byte, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	ab.AppendValues([][]byte{}, nil)
	a := ab.NewFixedSizeBinaryArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewFixedSizeBinaryArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues([][]byte{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewFixedSizeBinaryArray()
	assert.Equal(t, want, fixedSizeValues(a))
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([][]byte{}, nil)
	a = ab.NewFixedSizeBinaryArray()
	assert.Equal(t, want, fixedSizeValues(a))
	a.Release()
}

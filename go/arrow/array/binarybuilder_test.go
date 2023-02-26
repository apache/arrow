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
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBinaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)

	exp := [][]byte{[]byte("foo"), []byte("bar"), nil, []byte("sydney"), []byte("cameron")}
	for _, v := range exp {
		if v == nil {
			ab.AppendNull()
		} else {
			ab.Append(v)
		}
	}

	assert.Equal(t, len(exp), ab.Len(), "unexpected Len()")
	assert.Equal(t, 1, ab.NullN(), "unexpected NullN()")

	for i, v := range exp {
		if v == nil {
			v = []byte{}
		}
		assert.Equal(t, v, ab.Value(i), "unexpected BinaryArrayBuilder.Value(%d)", i)
	}

	ar := ab.NewBinaryArray()
	ab.Release()
	ar.Release()

	// check state of builder after NewBinaryArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewBinaryArray did not reset state")
}

func TestBinaryBuilder_ReserveData(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)

	// call ReserveData and ensure the capacity doesn't change
	// when appending entries until that count.
	ab.ReserveData(256)
	expCap := ab.DataCap()
	for i := 0; i < 256/8; i++ {
		ab.Append(bytes.Repeat([]byte("a"), 8))
	}
	assert.Equal(t, expCap, ab.DataCap(), "unexpected BinaryArrayBuilder.DataCap()")

	ar := ab.NewBinaryArray()
	ab.Release()
	ar.Release()

	// check state of builder after NewBinaryArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewBinaryArray did not reset state")
}

func TestBinaryBuilderLarge(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)

	exp := [][]byte{[]byte("foo"), []byte("bar"), nil, []byte("sydney"), []byte("cameron")}
	for _, v := range exp {
		if v == nil {
			ab.AppendNull()
		} else {
			ab.Append(v)
		}
	}

	assert.Equal(t, len(exp), ab.Len(), "unexpected Len()")
	assert.Equal(t, 1, ab.NullN(), "unexpected NullN()")

	for i, v := range exp {
		if v == nil {
			v = []byte{}
		}
		assert.Equal(t, v, ab.Value(i), "unexpected BinaryArrayBuilder.Value(%d)", i)
	}

	ar := ab.NewLargeBinaryArray()
	ab.Release()
	ar.Release()

	// check state of builder after NewBinaryArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewBinaryArray did not reset state")
}

func TestBinaryBuilderLarge_ReserveData(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)

	// call ReserveData and ensure the capacity doesn't change
	// when appending entries until that count.
	ab.ReserveData(256)
	expCap := ab.DataCap()
	for i := 0; i < 256/8; i++ {
		ab.Append(bytes.Repeat([]byte("a"), 8))
	}
	assert.Equal(t, expCap, ab.DataCap(), "unexpected BinaryArrayBuilder.DataCap()")

	ar := ab.NewLargeBinaryArray()
	ab.Release()
	ar.Release()

	// check state of builder after NewBinaryArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewBinaryArray did not reset state")
}

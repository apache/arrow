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

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := NewFixedSizeBinaryBuilder(mem, &dtype)

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
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	// Test builder reset and NewArray API.
	b.AppendValues(values, valid)
	a = b.NewArray().(*FixedSizeBinary)
	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())
	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))
	a.Release()

	b.Release()
}

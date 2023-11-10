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

	"github.com/apache/arrow/go/v14/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Init(t *testing.T) {
	type exp struct{ size int }
	tests := []struct {
		name string
		cap  int

		exp exp
	}{
		{"07 bits", 07, exp{size: 1}},
		{"19 bits", 19, exp{size: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ab := &builder{mem: memory.NewGoAllocator()}
			ab.init(test.cap)
			assert.Equal(t, test.cap, ab.Cap(), "invalid capacity")
			assert.Equal(t, test.exp.size, ab.nullBitmap.Len(), "invalid length")
		})
	}
}

func TestBuilder_UnsafeSetValid(t *testing.T) {
	ab := &builder{mem: memory.NewGoAllocator()}
	ab.init(32)
	ab.unsafeAppendBoolsToBitmap(tools.Bools(0, 0, 0, 0, 0), 5)
	assert.Equal(t, 5, ab.Len())
	assert.Equal(t, []byte{0, 0, 0, 0}, ab.nullBitmap.Bytes())

	ab.unsafeSetValid(17)
	assert.Equal(t, []byte{0xe0, 0xff, 0x3f, 0}, ab.nullBitmap.Bytes())
}

func TestBuilder_resize(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 64

	b.init(n)
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, 0, b.Len())

	b.UnsafeAppendBoolToBitmap(true)
	for i := 1; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(false)
	}
	assert.Equal(t, n, b.Cap())
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	n = 5
	b.resize(n, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())

	b.resize(32, b.init)
	assert.Equal(t, n, b.Len())
	assert.Equal(t, n-1, b.NullN())
}

func TestBuilder_IsNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	assert.True(t, b.IsNull(0))
	assert.True(t, b.IsNull(1))

	for i := 0; i < n; i++ {
		b.UnsafeAppendBoolToBitmap(i%2 == 0)
	}
	for i := 0; i < n; i++ {
		assert.Equal(t, i%2 != 0, b.IsNull(i))
	}
}

func TestBuilder_SetNull(t *testing.T) {
	b := &builder{mem: memory.NewGoAllocator()}
	n := 32
	b.init(n)

	for i := 0; i < n; i++ {
		// Set everything to true
		b.UnsafeAppendBoolToBitmap(true)
	}
	for i := 0; i < n; i++ {
		if i%2 == 0 { // Set all even numbers to null
			b.SetNull(i)
		}
	}

	for i := 0; i < n; i++ {
		if i%2 == 0 {
			assert.True(t, b.IsNull(i))
		} else {
			assert.False(t, b.IsNull(i))
		}
	}
}

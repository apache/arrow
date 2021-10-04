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

// +build cgo
// +build ccalloc

package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCgoArrowAllocator_Allocate(t *testing.T) {
	tests := []struct {
		name string
		sz   int
	}{
		{"lt alignment", 33},
		{"gt alignment unaligned", 65},
		{"eq alignment", 64},
		{"large unaligned", 4097},
		{"large aligned", 8192},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alloc := NewCgoArrowAllocator()
			buf := alloc.Allocate(test.sz)
			assert.NotNil(t, buf)
			assert.Len(t, buf, test.sz)

			alloc.AssertSize(t, test.sz)
			defer alloc.AssertSize(t, 0)
			defer alloc.Free(buf)
		})
	}
}

func TestCgoArrowAllocator_Reallocate(t *testing.T) {
	tests := []struct {
		name     string
		sz1, sz2 int
	}{
		{"smaller", 200, 100},
		{"same", 200, 200},
		{"larger", 200, 300},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alloc := NewCgoArrowAllocator()
			buf := alloc.Allocate(test.sz1)
			for i := range buf {
				buf[i] = byte(i & 0xFF)
			}

			exp := make([]byte, test.sz2)
			copy(exp, buf)

			newBuf := alloc.Reallocate(test.sz2, buf)
			assert.Equal(t, exp, newBuf)

			alloc.AssertSize(t, test.sz2)
			defer alloc.AssertSize(t, 0)
			defer alloc.Free(newBuf)
		})
	}
}

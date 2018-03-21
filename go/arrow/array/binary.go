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
	"unsafe"

	"github.com/apache/arrow/go/arrow"
)

// A type which represents an immutable sequence of variable-length binary strings.
type Binary struct {
	array
	valueOffsets []int32
	valueBytes   []byte
}

// NewBinaryData constructs a new Binary array from data.
func NewBinaryData(data *Data) *Binary {
	a := &Binary{}
	a.refCount = 1
	a.setData(data)
	return a
}

// Value returns the slice at index i. This value should not be mutated.
func (a *Binary) Value(i int) []byte { return a.valueBytes[a.valueOffsets[i]:a.valueOffsets[i+1]] }

// ValueString returns the string at index i without performing additional allocations.
// The string is only valid for the lifetime of the Binary array.
func (a *Binary) ValueString(i int) string {
	b := a.Value(i)
	return *(*string)(unsafe.Pointer(&b))
}

func (a *Binary) ValueOffset(i int) int { return int(a.valueOffsets[i]) }
func (a *Binary) ValueLen(i int) int    { return int(a.valueOffsets[i+1] - a.valueOffsets[i]) }
func (a *Binary) ValueOffsets() []int32 { return a.valueOffsets }
func (a *Binary) ValueBytes() []byte    { return a.valueBytes }

func (a *Binary) setData(data *Data) {
	if len(data.buffers) != 3 {
		panic("len(data.buffers) != 3")
	}

	a.array.setData(data)

	if valueData := data.buffers[2]; valueData != nil {
		a.valueBytes = valueData.Bytes()
	}

	if valueOffsets := data.buffers[1]; valueOffsets != nil {
		a.valueOffsets = arrow.Int32Traits.CastFromBytes(valueOffsets.Bytes())
	}
}

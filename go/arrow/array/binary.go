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
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
)

// A type which represents an immutable sequence of variable-length binary strings.
type Binary struct {
	array
	valueOffsets32 []int32
	valueOffsets64 []int64
	valueBytes     []byte
	is64Bit        bool
}

// NewBinaryData constructs a new Binary array from data. It uses 32-bit integer offsets.
func NewBinaryData(data *Data) *Binary {
	return newBinaryData(data, false)
}

// New64BitOffsetsBinaryData constructs a new Binary array from data. It uses 64-bit integer offsets.
func New64BitOffsetsBinaryData(data *Data) *Binary {
	return newBinaryData(data, true)
}

func newBinaryData(data *Data, is64Bit bool) *Binary {
	a := &Binary{
		is64Bit: is64Bit,
	}
	a.refCount = 1
	a.setData(data)
	return a
}

// Value returns the slice at index i. This value should not be mutated.
func (a *Binary) Value(i int) []byte {
	if i < 0 || i >= a.array.data.length {
		panic("arrow/array: index out of range")
	}
	idx := a.array.data.offset + i
	if a.valueOffsets32 != nil {
		return a.valueBytes[a.valueOffsets32[idx]:a.valueOffsets32[idx+1]]
	}
	return a.valueBytes[a.valueOffsets64[idx]:a.valueOffsets64[idx+1]]
}

// ValueString returns the string at index i without performing additional allocations.
// The string is only valid for the lifetime of the Binary array.
func (a *Binary) ValueString(i int) string {
	b := a.Value(i)
	return *(*string)(unsafe.Pointer(&b))
}

func (a *Binary) ValueOffset(i int) int {
	if i < 0 || i >= a.array.data.length {
		panic("arrow/array: index out of range")
	}
	if a.valueOffsets32 != nil {
		return int(a.valueOffsets32[a.array.data.offset+i])
	}
	return int(a.valueOffsets64[a.array.data.offset+i])
}

func (a *Binary) ValueLen(i int) int {
	if i < 0 || i >= a.array.data.length {
		panic("arrow/array: index out of range")
	}
	beg := a.array.data.offset + i
	if a.valueOffsets32 != nil {
		return int(a.valueOffsets32[beg+1] - a.valueOffsets32[beg])
	}
	return int(a.valueOffsets64[beg+1] - a.valueOffsets64[beg])
}

func (a *Binary) ValueOffsets() []int32 {
	beg := a.array.data.offset
	end := beg + a.array.data.length + 1
	if a.valueOffsets32 == nil {
		panic(fmt.Sprint("BinaryArray: ValueOffsets can only be called on 32-bit BinaryArray"))
	}
	return a.valueOffsets32[beg:end]
}

func (a *Binary) ValueOffsets64Bit() []int64 {
	beg := a.array.data.offset
	end := beg + a.array.data.length + 1
	if a.valueOffsets64 == nil {
		panic(fmt.Sprint("BinaryArray: ValueOffsets64Bit can only be called on 64-bit BinaryArray"))
	}
	return a.valueOffsets64[beg:end]
}

func (a *Binary) ValueBytes() []byte {
	beg := a.array.data.offset
	end := beg + a.array.data.length
	if a.valueOffsets32 != nil {
		return a.valueBytes[a.valueOffsets32[beg]:a.valueOffsets32[end]]
	}
	return a.valueBytes[a.valueOffsets64[beg]:a.valueOffsets64[end]]
}

func (a *Binary) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString("(null)")
		default:
			fmt.Fprintf(o, "%q", a.ValueString(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Binary) setData(data *Data) {
	if len(data.buffers) != 3 {
		panic("len(data.buffers) != 3")
	}

	a.array.setData(data)

	if valueData := data.buffers[2]; valueData != nil {
		a.valueBytes = valueData.Bytes()
	}

	if valueOffsets := data.buffers[1]; valueOffsets != nil {
		if a.is64Bit {
			a.valueOffsets64 = arrow.Int64Traits.CastFromBytes(valueOffsets.Bytes())
		} else {
			a.valueOffsets32 = arrow.Int32Traits.CastFromBytes(valueOffsets.Bytes())
		}
	}
}

func arrayEqualBinary(left, right *Binary) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if bytes.Compare(left.Value(i), right.Value(i)) != 0 {
			return false
		}
	}
	return true
}

var (
	_ Interface = (*Binary)(nil)
)

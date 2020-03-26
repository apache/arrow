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
	"fmt"
	"math"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	binaryArrayMaximumCapacity = math.MaxInt32
)

// A BinaryBuilder is used to build a Binary array using the Append methods.
type BinaryBuilder struct {
	builder

	dtype     arrow.BinaryDataType
	offsets32 *int32BufferBuilder
	offsets64 *int64BufferBuilder
	values    *byteBufferBuilder
}

// NewBinaryBuilder creates a new binary builder. It uses 32-bit integers for the offsets.
func NewBinaryBuilder(mem memory.Allocator, dtype arrow.BinaryDataType) *BinaryBuilder {
	return newBinaryBuilder(mem, dtype, false)
}

// New64BitOffsetsBinaryBuilder creates a new binary builder. It uses 64-bit integers for the offsets.
func New64BitOffsetsBinaryBuilder(mem memory.Allocator, dtype arrow.BinaryDataType) *BinaryBuilder {
	return newBinaryBuilder(mem, dtype, true)
}

func newBinaryBuilder(mem memory.Allocator, dtype arrow.BinaryDataType, int64Offsets bool) *BinaryBuilder {
	var (
		offsets32 *int32BufferBuilder
		offsets64 *int64BufferBuilder
	)
	if int64Offsets {
		offsets64 = newInt64BufferBuilder(mem)
	} else {
		offsets32 = newInt32BufferBuilder(mem)
	}
	b := &BinaryBuilder{
		builder:   builder{refCount: 1, mem: mem},
		dtype:     dtype,
		offsets32: offsets32,
		offsets64: offsets64,
		values:    newByteBufferBuilder(mem),
	}
	return b
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (b *BinaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		if b.offsets32 != nil {
			b.offsets32.Release()
			b.offsets32 = nil
		}
		if b.offsets64 != nil {
			b.offsets64.Release()
			b.offsets64 = nil
		}
		if b.values != nil {
			b.values.Release()
			b.values = nil
		}
	}
}

// Append appends the byte slice to the binary builder.
func (b *BinaryBuilder) Append(v []byte) {
	b.Reserve(1)
	b.appendNextOffset()
	b.values.Append(v)
	b.UnsafeAppendBoolToBitmap(true)
}

// AppendString appends the string to the binary builder. This method will
// allocate a new byte slice.
func (b *BinaryBuilder) AppendString(v string) {
	b.Append([]byte(v))
}

// AppendNull appends a null value to the binary builder.
func (b *BinaryBuilder) AppendNull() {
	b.Reserve(1)
	b.appendNextOffset()
	b.UnsafeAppendBoolToBitmap(false)
}

// AppendValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *BinaryBuilder) AppendValues(v [][]byte, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	for _, vv := range v {
		b.appendNextOffset()
		b.values.Append(vv)
	}

	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

// AppendStringValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *BinaryBuilder) AppendStringValues(v []string, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	for _, vv := range v {
		b.appendNextOffset()
		b.values.Append([]byte(vv))
	}

	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

// Value returns the byte slice at index i.
func (b *BinaryBuilder) Value(i int) []byte {
	var (
		start int64
		end   int64
	)
	if b.offsets32 != nil {
		offsets := b.offsets32.Values()
		start = int64(offsets[i])
		if i == (b.length - 1) {
			end = int64(b.values.Len())
		} else {
			end = int64(offsets[i+1])
		}
	} else {
		offsets := b.offsets64.Values()
		start = int64(offsets[i])
		if i == (b.length - 1) {
			end = int64(b.values.Len())
		} else {
			end = int64(offsets[i+1])
		}
	}

	return b.values.Bytes()[start:end]
}

func (b *BinaryBuilder) init(capacity int) {
	b.builder.init(capacity)
	if b.offsets32 != nil {
		b.offsets32.resize((capacity + 1) * arrow.Int32SizeBytes)
	} else {
		b.offsets64.resize((capacity + 1) * arrow.Int64SizeBytes)
	}
}

// DataLen returns the number of bytes in the data array.
func (b *BinaryBuilder) DataLen() int { return b.values.length }

// DataCap returns the total number of bytes that can be stored
// without allocating additional memory.
func (b *BinaryBuilder) DataCap() int { return b.values.capacity }

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *BinaryBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

// ReserveData ensures there is enough space for appending n bytes
// by checking the capacity and resizing the data buffer if necessary.
func (b *BinaryBuilder) ReserveData(n int) {
	if b.values.capacity < b.values.length+n {
		b.values.resize(b.values.Len() + n)
	}
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may be reduced.
func (b *BinaryBuilder) Resize(n int) {
	if b.offsets32 != nil {
		b.offsets32.resize((n + 1) * arrow.Int32SizeBytes)
	} else {
		b.offsets64.resize((n + 1) * arrow.Int64SizeBytes)
	}
	b.builder.resize(n, b.init)
}

// NewArray creates a Binary array from the memory buffers used by the builder and resets the BinaryBuilder
// so it can be used to build a new array.
func (b *BinaryBuilder) NewArray() Interface {
	return b.NewBinaryArray()
}

// NewBinaryArray creates a Binary array from the memory buffers used by the builder and resets the BinaryBuilder
// so it can be used to build a new array.
func (b *BinaryBuilder) NewBinaryArray() (a *Binary) {
	data := b.newData()
	a = NewBinaryData(data)
	data.Release()
	return
}

func (b *BinaryBuilder) newData() (data *Data) {
	b.appendNextOffset()

	var (
		offsets *memory.Buffer
		values  = b.values.Finish()
	)
	if b.offsets32 != nil {
		offsets = b.offsets32.Finish()
	} else {
		offsets = b.offsets64.Finish()
	}
	data = NewData(b.dtype, b.length, []*memory.Buffer{b.nullBitmap, offsets, values}, nil, b.nulls, 0)
	if offsets != nil {
		offsets.Release()
	}

	if values != nil {
		values.Release()
	}

	b.builder.reset()

	return
}

func (b *BinaryBuilder) appendNextOffset() {
	numBytes := b.values.Len()
	if b.offsets32 != nil {
		if numBytes > binaryArrayMaximumCapacity {
			panic(fmt.Sprintf("BinaryBuilder: append would overflow offsets"))
		}
		b.offsets32.AppendValue(int32(numBytes))
	} else {
		b.offsets64.AppendValue(int64(numBytes))
	}
}

var (
	_ Builder = (*BinaryBuilder)(nil)
)

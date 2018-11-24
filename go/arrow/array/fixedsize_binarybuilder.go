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
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
)

// A FixedSizeBinaryBuilder is used to build a FixedSizeBinary array using the Append methods.
type FixedSizeBinaryBuilder struct {
	builder

	dtype   *arrow.FixedSizeBinaryType
	offsets *int32BufferBuilder
	values  *byteBufferBuilder
}

func NewFixedSizeBinaryBuilder(mem memory.Allocator, dtype *arrow.FixedSizeBinaryType) *FixedSizeBinaryBuilder {
	b := &FixedSizeBinaryBuilder{
		builder: builder{refCount: 1, mem: mem},
		dtype:   dtype,
		offsets: newInt32BufferBuilder(mem),
		values:  newByteBufferBuilder(mem),
	}
	return b
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (b *FixedSizeBinaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		if b.offsets != nil {
			b.offsets.Release()
			b.offsets = nil
		}
		if b.values != nil {
			b.values.Release()
			b.values = nil
		}
	}
}

func (b *FixedSizeBinaryBuilder) Append(v []byte) {
	if len(v) != b.dtype.ByteWidth {
		// TODO(alexandre): should we return an error instead?
		panic("len(v) != b.dtype.ByteWidth")
	}

	b.Reserve(1)
	b.appendNextOffset()
	b.values.Append(v)
	b.UnsafeAppendBoolToBitmap(true)
}

func (b *FixedSizeBinaryBuilder) AppendNull() {
	b.Reserve(1)
	b.appendNextOffset()
	b.UnsafeAppendBoolToBitmap(false)
}

// AppendValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *FixedSizeBinaryBuilder) AppendValues(v [][]byte, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	b.Reserve(len(v))
	for _, vv := range v {
		b.appendNextOffset()
		b.values.Append(vv)
	}

	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

func (b *FixedSizeBinaryBuilder) Value(i int) []byte {
	offsets := b.offsets.Values()
	start := int(offsets[i])
	var end int
	if i == (b.length - 1) {
		end = b.values.Len()
	} else {
		end = int(offsets[i+1])
	}
	return b.values.Bytes()[start:end]
}

func (b *FixedSizeBinaryBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.values.resize(capacity * b.dtype.ByteWidth)
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *FixedSizeBinaryBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *FixedSizeBinaryBuilder) Resize(n int) {
	b.offsets.resize((n + 1) * arrow.Int32SizeBytes)
	b.builder.resize(n, b.init)
}

// NewArray creates a FixedSizeBinary array from the memory buffers used by the
// builder and resets the FixedSizeBinaryBuilder so it can be used to build a new array.
func (b *FixedSizeBinaryBuilder) NewArray() Interface {
	return b.NewFixedSizeBinaryArray()
}

// NewFixedSizeBinaryArray creates a FixedSizeBinary array from the memory buffers used by the builder and resets the FixedSizeBinaryBuilder
// so it can be used to build a new array.
func (b *FixedSizeBinaryBuilder) NewFixedSizeBinaryArray() (a *FixedSizeBinary) {
	data := b.newData()
	a = NewFixedSizeBinaryData(data)
	data.Release()
	return
}

func (b *FixedSizeBinaryBuilder) newData() (data *Data) {
	b.appendNextOffset()
	values := b.values.Finish()
	offsets := b.offsets.Finish()
	data = NewData(b.dtype, b.length, []*memory.Buffer{b.nullBitmap, offsets, values}, nil, b.nulls, 0)

	if values != nil {
		values.Release()
	}
	if offsets != nil {
		offsets.Release()
	}

	b.builder.reset()

	return
}

func (b *FixedSizeBinaryBuilder) appendNextOffset() {
	numBytes := b.values.Len()
	// TODO(alexandre): check binaryArrayMaximumCapacity?
	b.offsets.AppendValue(int32(numBytes))
}

var (
	_ Builder = (*FixedSizeBinaryBuilder)(nil)
)

// Code generated by array/bufferbuilder_numeric.gen.go.tmpl. DO NOT EDIT.

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
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

type int64BufferBuilder struct {
	bufferBuilder
}

func newInt64BufferBuilder(mem memory.Allocator) *int64BufferBuilder {
	return &int64BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *int64BufferBuilder) AppendValues(v []int64) { b.Append(arrow.Int64Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *int64BufferBuilder) Values() []int64 { return arrow.Int64Traits.CastFromBytes(b.Bytes()) }

// Value returns the int64 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *int64BufferBuilder) Value(i int) int64 { return b.Values()[i] }

// Len returns the number of int64 elements in the buffer.
func (b *int64BufferBuilder) Len() int { return b.length / arrow.Int64SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *int64BufferBuilder) AppendValue(v int64) {
	if b.capacity < b.length+arrow.Int64SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int64SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int64Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int64SizeBytes
}

type int32BufferBuilder struct {
	bufferBuilder
}

func newInt32BufferBuilder(mem memory.Allocator) *int32BufferBuilder {
	return &int32BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *int32BufferBuilder) AppendValues(v []int32) { b.Append(arrow.Int32Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *int32BufferBuilder) Values() []int32 { return arrow.Int32Traits.CastFromBytes(b.Bytes()) }

// Value returns the int32 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *int32BufferBuilder) Value(i int) int32 { return b.Values()[i] }

// Len returns the number of int32 elements in the buffer.
func (b *int32BufferBuilder) Len() int { return b.length / arrow.Int32SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *int32BufferBuilder) AppendValue(v int32) {
	if b.capacity < b.length+arrow.Int32SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int32SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int32Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int32SizeBytes
}

type int8BufferBuilder struct {
	bufferBuilder
}

func newInt8BufferBuilder(mem memory.Allocator) *int8BufferBuilder {
	return &int8BufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

// AppendValues appends the contents of v to the buffer, growing the buffer as needed.
func (b *int8BufferBuilder) AppendValues(v []int8) { b.Append(arrow.Int8Traits.CastToBytes(v)) }

// Values returns a slice of length b.Len().
// The slice is only valid for use until the next buffer modification. That is, until the next call
// to Advance, Reset, Finish or any Append function. The slice aliases the buffer content at least until the next
// buffer modification.
func (b *int8BufferBuilder) Values() []int8 { return arrow.Int8Traits.CastFromBytes(b.Bytes()) }

// Value returns the int8 element at the index i. Value will panic if i is negative or ≥ Len.
func (b *int8BufferBuilder) Value(i int) int8 { return b.Values()[i] }

// Len returns the number of int8 elements in the buffer.
func (b *int8BufferBuilder) Len() int { return b.length / arrow.Int8SizeBytes }

// AppendValue appends v to the buffer, growing the buffer as needed.
func (b *int8BufferBuilder) AppendValue(v int8) {
	if b.capacity < b.length+arrow.Int8SizeBytes {
		newCapacity := bitutil.NextPowerOf2(b.length + arrow.Int8SizeBytes)
		b.resize(newCapacity)
	}
	arrow.Int8Traits.PutValue(b.bytes[b.length:], v)
	b.length += arrow.Int8SizeBytes
}

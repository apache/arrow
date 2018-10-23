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
)

// Chunked manages a collection of primitives arrays as one logical large array.
type Chunked struct {
	chunks []Interface

	refCount int64

	length int
	nulls  int
	dtype  arrow.DataType
}

// NewChunked returns a new chunked array from the slice of arrays.
//
// NewChunked panics if the chunks do not have the same data type.
func NewChunked(dtype arrow.DataType, chunks []Interface) *Chunked {
	arr := &Chunked{
		chunks:   make([]Interface, len(chunks)),
		refCount: 1,
		dtype:    dtype,
	}
	for i, chunk := range chunks {
		if chunk.DataType() != dtype {
			panic("arrow/array: mismatch data type")
		}
		chunk.Retain()
		arr.chunks[i] = chunk
		arr.length += chunk.Len()
		arr.nulls += chunk.NullN()
	}
	return arr
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (a *Chunked) Retain() {
	atomic.AddInt64(&a.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (a *Chunked) Release() {
	debug.Assert(atomic.LoadInt64(&a.refCount) > 0, "too many releases")

	if atomic.AddInt64(&a.refCount, -1) == 0 {
		for _, arr := range a.chunks {
			arr.Release()
		}
		a.chunks = nil
		a.length = 0
		a.nulls = 0
	}
}

func (a *Chunked) Len() int                 { return a.length }
func (a *Chunked) NullN() int               { return a.nulls }
func (a *Chunked) DataType() arrow.DataType { return a.dtype }
func (a *Chunked) Chunks() []Interface      { return a.chunks }
func (a *Chunked) Chunk(i int) Interface    { return a.chunks[i] }

// NewSlice constructs a zero-copy slice of the chunked array with the indicated
// indices i and j, corresponding to array[i:j].
// The returned chunked array must be Release()'d after use.
//
// NewSlice panics if the slice is outside the valid range of the input array.
// NewSlice panics if j < i.
func (a *Chunked) NewSlice(i, j int64) *Chunked {
	if j > int64(a.length) || i > j || i > int64(a.length) {
		panic("arrow/array: index out of range")
	}

	var (
		cur    = 0
		beg    = i
		sz     = j - i
		chunks = make([]Interface, 0, len(a.chunks))
	)

	for cur < len(a.chunks) && beg >= int64(a.chunks[cur].Len()) {
		beg -= int64(a.chunks[cur].Len())
		cur++
	}

	for cur < len(a.chunks) && sz > 0 {
		arr := a.chunks[cur]
		end := beg + sz
		if end > int64(arr.Len()) {
			end = int64(arr.Len())
		}
		chunks = append(chunks, NewSlice(arr, beg, end))
		sz -= int64(arr.Len()) - beg
		beg = 0
		cur++
	}
	chunks = chunks[:len(chunks):len(chunks)]
	defer func() {
		for _, chunk := range chunks {
			chunk.Release()
		}
	}()

	return NewChunked(a.dtype, chunks)
}

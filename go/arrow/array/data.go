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

// A type which represents the memory and metadata for an Arrow array.
type Data struct {
	refCount  int64
	typE      arrow.DataType
	nullN     int
	length    int
	buffers   []*memory.Buffer // TODO(sgc): should this be an interface?
	childData []*Data          // TODO(sgc): managed by ListArray, StructArray and UnionArray types
}

func NewData(typE arrow.DataType, length int, buffers []*memory.Buffer, nullN int) *Data {
	for _, b := range buffers {
		if b != nil {
			b.Retain()
		}
	}

	return &Data{
		refCount: 1,
		typE:     typE,
		nullN:    nullN,
		length:   length,
		buffers:  buffers,
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (d *Data) Retain() {
	atomic.AddInt64(&d.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (d *Data) Release() {
	debug.Assert(atomic.LoadInt64(&d.refCount) > 0, "too many releases")

	if atomic.AddInt64(&d.refCount, -1) == 0 {
		for _, b := range d.buffers {
			if b != nil {
				b.Release()
			}
		}

		for _, b := range d.childData {
			b.Release()
		}
		d.buffers, d.childData = nil, nil
	}
}

func (d *Data) DataType() arrow.DataType { return d.typE }
func (d *Data) NullN() int               { return d.nullN }
func (d *Data) Len() int                 { return d.length }

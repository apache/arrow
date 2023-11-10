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

// Package tensor provides types that implement n-dimensional arrays.
package tensor

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/internal/debug"
)

// Interface represents an n-dimensional array of numerical data.
type Interface interface {
	// Retain increases the reference count by 1.
	// Retain may be called simultaneously from multiple goroutines.
	Retain()

	// Release decreases the reference count by 1.
	// Release may be called simultaneously from multiple goroutines.
	// When the reference count goes to zero, the memory is freed.
	Release()

	// Len returns the number of elements in the tensor.
	Len() int

	// Shape returns the size - in each dimension - of the tensor.
	Shape() []int64

	// Strides returns the number of bytes to step in each dimension when traversing the tensor.
	Strides() []int64

	// NumDims returns the number of dimensions of the tensor.
	NumDims() int

	// DimName returns the name of the i-th dimension.
	DimName(i int) string

	// DimNames returns the names for all dimensions
	DimNames() []string

	DataType() arrow.DataType
	Data() arrow.ArrayData

	// IsMutable returns whether the underlying data buffer is mutable.
	IsMutable() bool
	IsContiguous() bool
	IsRowMajor() bool
	IsColMajor() bool
}

type tensorBase struct {
	refCount int64
	dtype    arrow.DataType
	bw       int64 // bytes width
	data     arrow.ArrayData
	shape    []int64
	strides  []int64
	names    []string
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (tb *tensorBase) Retain() {
	atomic.AddInt64(&tb.refCount, 1)
}

// Release decreases the reference count by 1.
// Release may be called simultaneously from multiple goroutines.
// When the reference count goes to zero, the memory is freed.
func (tb *tensorBase) Release() {
	debug.Assert(atomic.LoadInt64(&tb.refCount) > 0, "too many releases")

	if atomic.AddInt64(&tb.refCount, -1) == 0 {
		tb.data.Release()
		tb.data = nil
	}
}

func (tb *tensorBase) Len() int {
	o := int64(1)
	for _, v := range tb.shape {
		o *= v
	}
	return int(o)
}

func (tb *tensorBase) Shape() []int64           { return tb.shape }
func (tb *tensorBase) Strides() []int64         { return tb.strides }
func (tb *tensorBase) NumDims() int             { return len(tb.shape) }
func (tb *tensorBase) DimName(i int) string     { return tb.names[i] }
func (tb *tensorBase) DataType() arrow.DataType { return tb.dtype }
func (tb *tensorBase) Data() arrow.ArrayData    { return tb.data }
func (tb *tensorBase) DimNames() []string       { return tb.names }

// IsMutable returns whether the underlying data buffer is mutable.
func (tb *tensorBase) IsMutable() bool { return false } // FIXME(sbinet): implement it at the array.Data level

func (tb *tensorBase) IsContiguous() bool {
	return tb.IsRowMajor() || tb.IsColMajor()
}

func (tb *tensorBase) IsRowMajor() bool {
	strides := rowMajorStrides(tb.dtype, tb.shape)
	return equalInt64s(strides, tb.strides)
}

func (tb *tensorBase) IsColMajor() bool {
	strides := colMajorStrides(tb.dtype, tb.shape)
	return equalInt64s(strides, tb.strides)
}

func (tb *tensorBase) offset(index []int64) int64 {
	var offset int64
	for i, v := range index {
		offset += v * tb.strides[i]
	}
	return offset / tb.bw
}

// New returns a new n-dim array from the provided backing data and the shape and strides.
// If strides is nil, row-major strides will be inferred.
// If names is nil, a slice of empty strings will be created.
//
// New panics if the backing data is not a numerical type.
func New(data arrow.ArrayData, shape, strides []int64, names []string) Interface {
	dt := data.DataType()
	switch dt.ID() {
	case arrow.INT8:
		return NewInt8(data, shape, strides, names)
	case arrow.INT16:
		return NewInt16(data, shape, strides, names)
	case arrow.INT32:
		return NewInt32(data, shape, strides, names)
	case arrow.INT64:
		return NewInt64(data, shape, strides, names)
	case arrow.UINT8:
		return NewUint8(data, shape, strides, names)
	case arrow.UINT16:
		return NewUint16(data, shape, strides, names)
	case arrow.UINT32:
		return NewUint32(data, shape, strides, names)
	case arrow.UINT64:
		return NewUint64(data, shape, strides, names)
	case arrow.FLOAT32:
		return NewFloat32(data, shape, strides, names)
	case arrow.FLOAT64:
		return NewFloat64(data, shape, strides, names)
	case arrow.DATE32:
		return NewDate32(data, shape, strides, names)
	case arrow.DATE64:
		return NewDate64(data, shape, strides, names)
	default:
		panic(fmt.Errorf("arrow/tensor: invalid data type %s", dt.Name()))
	}
}

func newTensor(dtype arrow.DataType, data arrow.ArrayData, shape, strides []int64, names []string) *tensorBase {
	tb := tensorBase{
		refCount: 1,
		dtype:    dtype,
		bw:       int64(dtype.(arrow.FixedWidthDataType).BitWidth()) / 8,
		data:     data,
		shape:    shape,
		strides:  strides,
		names:    names,
	}
	tb.data.Retain()

	if len(tb.shape) > 0 && len(tb.strides) == 0 {
		tb.strides = rowMajorStrides(dtype, shape)
	}
	return &tb
}

func rowMajorStrides(dtype arrow.DataType, shape []int64) []int64 {
	dt := dtype.(arrow.FixedWidthDataType)
	rem := int64(dt.BitWidth() / 8)
	for _, v := range shape {
		rem *= v
	}

	if rem == 0 {
		strides := make([]int64, len(shape))
		rem := int64(dt.BitWidth() / 8)
		for i := range strides {
			strides[i] = rem
		}
		return strides
	}

	var strides []int64
	for _, v := range shape {
		rem /= v
		strides = append(strides, rem)
	}
	return strides
}

func colMajorStrides(dtype arrow.DataType, shape []int64) []int64 {
	dt := dtype.(arrow.FixedWidthDataType)
	total := int64(dt.BitWidth() / 8)
	for _, v := range shape {
		if v == 0 {
			strides := make([]int64, len(shape))
			for i := range strides {
				strides[i] = total
			}
			return strides
		}
	}

	var strides []int64
	for _, v := range shape {
		strides = append(strides, total)
		total *= v
	}
	return strides
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

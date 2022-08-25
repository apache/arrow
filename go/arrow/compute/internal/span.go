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

package internal

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
)

// BufferSpan is a lightweight Buffer holder for ArraySpans that does not
// take ownership of the underlying memory.Buffer at all or could be
// used to reference raw byte slices instead.
type BufferSpan struct {
	// Buf should be the byte slice representing this buffer, if this is
	// nil then this bufferspan should be considered empty.
	Buf []byte
	// Owner should point to an underlying parent memory.Buffer if this
	// memory is owned by a different, existing, buffer. Retain is not
	// called on this buffer, so it must not be released as long as
	// this BufferSpan refers to it.
	Owner *memory.Buffer
	// SelfAlloc tracks whether or not this bufferspan is the only owner
	// of the Owning memory.Buffer. This happens when preallocating
	// memory or if a kernel allocates it's own buffer for a result.
	// In these cases, we have to know so we can properly maintain the
	// refcount if this is later turned into an ArrayData object.
	SelfAlloc bool
}

// SetBuffer sets the given buffer into this BufferSpan and marks
// SelfAlloc as false. This should be called when setting a buffer
// that is externally owned/created.
func (b *BufferSpan) SetBuffer(buf *memory.Buffer) {
	b.Buf = buf.Bytes()
	b.Owner = buf
	b.SelfAlloc = false
}

// WrapBuffer wraps this bufferspan around a buffer and marks
// SelfAlloc as true. This should be called when setting a buffer
// that was allocated as part of an execution rather than just
// re-using an existing buffer from an input array.
func (b *BufferSpan) WrapBuffer(buf *memory.Buffer) {
	b.Buf = buf.Bytes()
	b.Owner = buf
	b.SelfAlloc = true
}

// ArraySpan is a light-weight, non-owning version of arrow.ArrayData
// for more efficient handling with computation and engines. We use
// explicit go Arrays to define the buffers and some scratch space
// for easily populating and shifting around pointers to memory without
// having to worry about and deal with retain/release during calculations.
type ArraySpan struct {
	Type    arrow.DataType
	Len     int64
	Nulls   int64
	Offset  int64
	Buffers [3]BufferSpan

	// Scratch is a holding spot for things such as
	// offsets or union type codes when converting from scalars
	Scratch [2]uint64

	Children []ArraySpan
}

// UpdateNullCount will count the bits in the null bitmap and update the
// number of nulls if the current null count is unknown, otherwise it just
// returns the value of a.Nulls
func (a *ArraySpan) UpdateNullCount() int64 {
	if a.Nulls != array.UnknownNullCount {
		return a.Nulls
	}

	a.Nulls = a.Len - int64(bitutil.CountSetBits(a.Buffers[0].Buf, int(a.Offset), int(a.Len)))
	return a.Nulls
}

// Dictionary returns a pointer to the array span for the dictionary which
// we will always place as the first (and only) child if it exists.
func (a *ArraySpan) Dictionary() *ArraySpan { return &a.Children[0] }

// NumBuffers returns the number of expected buffers for this type
func (a *ArraySpan) NumBuffers() int { return getNumBuffers(a.Type) }

// MakeData generates an arrow.ArrayData object for this ArraySpan,
// properly updating the buffer ref count if necessary.
func (a *ArraySpan) MakeData() arrow.ArrayData {
	bufs := make([]*memory.Buffer, a.NumBuffers())
	for i := range bufs {
		b := a.GetBuffer(i)
		bufs[i] = b
		if b != nil && a.Buffers[i].SelfAlloc {
			// if this buffer is just a pointer to another existing buffer
			// then we never bumped the refcount for that buffer.
			// As a result, we won't call release here so that the call
			// to array.NewData properly updates the ref counts of the buffers.
			// If instead this buffer was allocated during calculation
			// (such as during prealloc or by a kernel itself)
			// then we need to release after we create the ArrayData so that it
			// maintains the correct refcount of 1, giving the resulting
			// ArrayData object ownership of this buffer.
			defer b.Release()
		}
	}

	var (
		nulls    = int(a.Nulls)
		length   = int(a.Len)
		off      = int(a.Offset)
		dt       = a.Type
		children []arrow.ArrayData
	)

	if a.Type.ID() == arrow.NULL {
		nulls = int(length)
	} else if len(a.Buffers[0].Buf) == 0 {
		nulls = 0
	}

	// we use a.Type for the NewData call at the end, so we can
	// handle extension types by using dt to point to the storage type
	// and let the proper extension type get set into the ArrayData
	// object we return.
	if dt.ID() == arrow.EXTENSION {
		dt = dt.(arrow.ExtensionType).StorageType()
	}

	if dt.ID() == arrow.DICTIONARY {
		result := array.NewData(a.Type, length, bufs, nil, nulls, off)
		dict := a.Dictionary().MakeData()
		defer dict.Release()
		result.SetDictionary(dict)
		return result
	}

	if len(a.Children) > 0 {
		children = make([]arrow.ArrayData, len(a.Children))
		for i, c := range a.Children {
			d := c.MakeData()
			defer d.Release()
			children[i] = d
		}
	}
	return array.NewData(a.Type, length, bufs, children, nulls, off)
}

// MakeArray is a convenience function for calling array.MakeFromData(a.MakeData())
func (a *ArraySpan) MakeArray() arrow.Array {
	d := a.MakeData()
	defer d.Release()
	return array.MakeFromData(d)
}

// SetSlice updates the offset and length of this ArraySpan to refer to
// a specific slice of the underlying buffers.
func (a *ArraySpan) SetSlice(off, length int64) {
	a.Offset, a.Len = off, length
	if a.Type.ID() != arrow.NULL {
		a.Nulls = array.UnknownNullCount
	} else {
		a.Nulls = a.Len
	}
}

// GetBuffer returns the buffer for the requested index. If this buffer
// is owned by another array/arrayspan the Owning buffer is returned,
// otherwise if this slice has no owning buffer, we call NewBufferBytes
// to wrap it as a memory.Buffer. Can also return nil if there is no
// buffer in this index.
func (a *ArraySpan) GetBuffer(idx int) *memory.Buffer {
	buf := a.Buffers[idx]
	switch {
	case buf.Owner != nil:
		return buf.Owner
	case buf.Buf != nil:
		return memory.NewBufferBytes(buf.Buf)
	}
	return nil
}

// convenience function to resize the children slice if necessary,
// or just shrink the slice without re-allocating if there's enough
// capacity already.
func (a *ArraySpan) resizeChildren(i int) {
	if cap(a.Children) >= i {
		a.Children = a.Children[:i]
	} else {
		a.Children = make([]ArraySpan, i)
	}
}

// convenience function for populating the offsets buffer from a scalar
// value's size.
func setOffsetsForScalar[T int32 | int64](span *ArraySpan, buf []T, valueSize int64, bufidx int) {
	buf[0] = 0
	buf[1] = T(valueSize)

	b := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	s := (*reflect.SliceHeader)(unsafe.Pointer(&span.Buffers[bufidx].Buf))
	s.Data = b.Data
	s.Len = 2 * int(unsafe.Sizeof(T(0)))
	s.Cap = s.Len

	span.Buffers[bufidx].Owner = nil
	span.Buffers[bufidx].SelfAlloc = false
}

// FillFromScalar populates this ArraySpan as if it were a 1 length array
// with the single value equal to the passed in Scalar.
func (a *ArraySpan) FillFromScalar(val scalar.Scalar) {
	var (
		trueBit  byte = 0x01
		falseBit byte = 0x00
	)

	a.Type = val.DataType()
	a.Len = 1
	typeID := a.Type.ID()
	if val.IsValid() {
		a.Nulls = 0
	} else {
		a.Nulls = 1
	}

	if !arrow.IsUnion(typeID) && typeID != arrow.NULL {
		if val.IsValid() {
			a.Buffers[0].Buf = []byte{trueBit}
		} else {
			a.Buffers[0].Buf = []byte{falseBit}
		}
		a.Buffers[0].Owner = nil
		a.Buffers[0].SelfAlloc = false
	}

	switch {
	case typeID == arrow.BOOL:
		if val.(*scalar.Boolean).Value {
			a.Buffers[1].Buf = []byte{trueBit}
		} else {
			a.Buffers[1].Buf = []byte{falseBit}
		}
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false
	case arrow.IsPrimitive(typeID) || arrow.IsDecimal(typeID):
		sc := val.(scalar.PrimitiveScalar)
		a.Buffers[1].Buf = sc.Data()
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false
	case typeID == arrow.DICTIONARY:
		sc := val.(scalar.PrimitiveScalar)
		a.Buffers[1].Buf = sc.Data()
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false
		a.resizeChildren(1)
		a.Children[0].SetMembers(val.(*scalar.Dictionary).Value.Dict.Data())
	case arrow.IsBaseBinary(typeID):
		sc := val.(scalar.BinaryScalar)
		a.Buffers[1].Buf = arrow.Uint64Traits.CastToBytes(a.Scratch[:])
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false

		var dataBuffer []byte
		if sc.IsValid() {
			dataBuffer = sc.Data()
			a.Buffers[2].Owner = sc.Buffer()
			a.Buffers[2].SelfAlloc = false
		}
		if arrow.IsBinaryLike(typeID) {
			setOffsetsForScalar(a,
				unsafe.Slice((*int32)(unsafe.Pointer(&a.Scratch[0])), 2),
				int64(len(dataBuffer)), 1)
		} else {
			// large_binary_like
			setOffsetsForScalar(a,
				unsafe.Slice((*int64)(unsafe.Pointer(&a.Scratch[0])), 2),
				int64(len(dataBuffer)), 1)
		}
		a.Buffers[2].Buf = dataBuffer
	case typeID == arrow.FIXED_SIZE_BINARY:
		sc := val.(scalar.BinaryScalar)
		a.Buffers[1].Buf = sc.Data()
		a.Buffers[1].Owner = sc.Buffer()
		a.Buffers[1].SelfAlloc = false
	case arrow.IsListLike(typeID):
		sc := val.(scalar.ListScalar)
		valueLen := 0
		a.resizeChildren(1)

		if sc.GetList() != nil {
			a.Children[0].SetMembers(sc.GetList().Data())
			valueLen = sc.GetList().Len()
		} else {
			// even when the value is null, we must populate
			// child data to yield a valid array. ugh
			fillZeroLength(sc.DataType().(arrow.NestedType).Fields()[0].Type, &a.Children[0])
		}

		switch typeID {
		case arrow.LIST, arrow.MAP:
			setOffsetsForScalar(a,
				unsafe.Slice((*int32)(unsafe.Pointer(&a.Scratch[0])), 2),
				int64(valueLen), 1)
		case arrow.LARGE_LIST:
			setOffsetsForScalar(a,
				unsafe.Slice((*int64)(unsafe.Pointer(&a.Scratch[0])), 2),
				int64(valueLen), 1)
		default:
			// fixed size list has no second buffer
			a.Buffers[1].Buf, a.Buffers[1].Owner = nil, nil
			a.Buffers[1].SelfAlloc = false
		}
	case typeID == arrow.STRUCT:
		sc := val.(*scalar.Struct)
		a.Buffers[1].Buf = nil
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false
		a.resizeChildren(len(sc.Value))
		for i, v := range sc.Value {
			a.Children[i].FillFromScalar(v)
		}
	case arrow.IsUnion(typeID):
		// first buffer is kept null since unions have no validity vector
		a.Buffers[0].Buf, a.Buffers[0].Owner = nil, nil
		a.Buffers[0].SelfAlloc = false

		a.Buffers[1].Buf = arrow.Uint64Traits.CastToBytes(a.Scratch[:])[:1]
		a.Buffers[1].Owner = nil
		a.Buffers[1].SelfAlloc = false
		codes := unsafe.Slice((*arrow.UnionTypeCode)(unsafe.Pointer(&a.Buffers[1].Buf[0])), 1)

		a.resizeChildren(len(a.Type.(arrow.UnionType).Fields()))
		switch sc := val.(type) {
		case *scalar.DenseUnion:
			codes[0] = sc.TypeCode
			// has offset, start 4 bytes in so it's aligned to the 32-bit boundaries
			off := unsafe.Slice((*int32)(unsafe.Add(unsafe.Pointer(&a.Scratch[0]), arrow.Int32SizeBytes)), 2)
			setOffsetsForScalar(a, off, 1, 2)
			// we can't "see" the other arrays in the union, but we put the "active"
			// union array in the right place and fill zero-length arrays for
			// the others.
			childIDS := a.Type.(arrow.UnionType).ChildIDs()
			for i, f := range a.Type.(arrow.UnionType).Fields() {
				if i == childIDS[sc.TypeCode] {
					a.Children[i].FillFromScalar(sc.Value)
				} else {
					fillZeroLength(f.Type, &a.Children[i])
				}
			}
		case *scalar.SparseUnion:
			codes[0] = sc.TypeCode
			// sparse union scalars have a full complement of child values
			// even though only one of them is relevant, so we just fill them
			// in here
			for i, v := range sc.Value {
				a.Children[i].FillFromScalar(v)
			}
		}
	case typeID == arrow.EXTENSION:
		// pass through storage
		sc := val.(*scalar.Extension)
		a.FillFromScalar(sc.Value)
		// restore the extension type
		a.Type = val.DataType()
	case typeID == arrow.NULL:
		for i := range a.Buffers {
			a.Buffers[i].Buf = nil
			a.Buffers[i].Owner = nil
			a.Buffers[i].SelfAlloc = false
		}
	}
}

// SetMembers populates this ArraySpan from the given ArrayData object.
// As this is a non-owning reference, the ArrayData object must not
// be fully released while this ArraySpan is in use, otherwise any buffers
// referenced will be released too
func (a *ArraySpan) SetMembers(data arrow.ArrayData) {
	a.Type = data.DataType()
	a.Len = int64(data.Len())
	if a.Type.ID() == arrow.NULL {
		a.Nulls = a.Len
	} else {
		a.Nulls = int64(data.NullN())
	}
	a.Offset = int64(data.Offset())

	for i, b := range data.Buffers() {
		if b != nil {
			a.Buffers[i].SetBuffer(b)
		} else {
			a.Buffers[i].Buf = nil
			a.Buffers[i].Owner = nil
			a.Buffers[i].SelfAlloc = false
		}
	}

	typeID := a.Type.ID()
	if a.Buffers[0].Buf == nil {
		switch typeID {
		case arrow.NULL, arrow.SPARSE_UNION, arrow.DENSE_UNION:
		default:
			// should already be zero, but we make sure
			a.Nulls = 0
		}
	}

	for i := len(data.Buffers()); i < 3; i++ {
		a.Buffers[i].Buf = nil
		a.Buffers[i].Owner = nil
		a.Buffers[i].SelfAlloc = false
	}

	if typeID == arrow.DICTIONARY {
		if cap(a.Children) >= 1 {
			a.Children = a.Children[:1]
		} else {
			a.Children = make([]ArraySpan, 1)
		}
		a.Children[0].SetMembers(data.Dictionary())
	} else {
		if cap(a.Children) >= len(data.Children()) {
			a.Children = a.Children[:len(data.Children())]
		} else {
			a.Children = make([]ArraySpan, len(data.Children()))
		}
		for i, c := range data.Children() {
			a.Children[i].SetMembers(c)
		}
	}
}

// ExecValue represents a single input to an execution which could
// be either an Array (ArraySpan) or a Scalar value
type ExecValue struct {
	Array  ArraySpan
	Scalar scalar.Scalar
}

func (e *ExecValue) IsArray() bool  { return e.Scalar == nil }
func (e *ExecValue) IsScalar() bool { return !e.IsArray() }

func (e *ExecValue) Type() arrow.DataType {
	if e.IsArray() {
		return e.Array.Type
	}
	return e.Scalar.DataType()
}

// ExecResult is the result of a kernel execution and should be populated
// by the execution functions and/or a kernel. For now we're just going to
// alias an ArraySpan.
type ExecResult = ArraySpan

// ExecSpan represents a slice of inputs and is used to provide slices
// of input values to iterate over.
//
// Len is the length of the span (all elements in Values should either
// be scalar or an array with a length + offset of at least Len).
type ExecSpan struct {
	Len    int64
	Values []ExecValue
}

func getNumBuffers(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.NULL, arrow.STRUCT, arrow.FIXED_SIZE_LIST:
		return 1
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.STRING, arrow.LARGE_STRING, arrow.DENSE_UNION:
		return 3
	case arrow.EXTENSION:
		return getNumBuffers(dt.(arrow.ExtensionType).StorageType())
	default:
		return 2
	}
}

func fillZeroLength(dt arrow.DataType, span *ArraySpan) {
	span.Scratch[0], span.Scratch[1] = 0, 0
	span.Type = dt
	span.Len = 0
	numBufs := getNumBuffers(dt)
	for i := 0; i < numBufs; i++ {
		span.Buffers[i].Buf = arrow.Uint64Traits.CastToBytes(span.Scratch[:])[:0]
		span.Buffers[i].Owner = nil
	}

	for i := numBufs; i < 3; i++ {
		span.Buffers[i].Buf, span.Buffers[i].Owner = nil, nil
	}

	nt, ok := dt.(arrow.NestedType)
	if !ok {
		if len(span.Children) > 0 {
			span.Children = span.Children[:0]
		}
		return
	}

	if cap(span.Children) >= len(nt.Fields()) {
		span.Children = span.Children[:len(nt.Fields())]
	} else {
		span.Children = make([]ArraySpan, len(nt.Fields()))
	}
	for i, f := range nt.Fields() {
		fillZeroLength(f.Type, &span.Children[i])
	}
}

// PromoteExecSpanScalars promotes the values of the passed in ExecSpan
// from scalars to Arrays of length 1 for each value.
func PromoteExecSpanScalars(span ExecSpan) {
	for i := range span.Values {
		if span.Values[i].Scalar != nil {
			span.Values[i].Array.FillFromScalar(span.Values[i].Scalar)
			span.Values[i].Scalar = nil
		}
	}
}

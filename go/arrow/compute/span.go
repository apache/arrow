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

package compute

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
)

type BufferSpan struct {
	Buf   []byte
	Owner *memory.Buffer
}

func (b *BufferSpan) SetBuffer(buf *memory.Buffer) {
	b.Buf = buf.Bytes()
	b.Owner = buf
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

func promoteExecSpanScalars(span ExecSpan) {
	for i := range span.Values {
		if span.Values[i].Scalar != nil {
			span.Values[i].Array.FillFromScalar(span.Values[i].Scalar)
			span.Values[i].Scalar = nil
		}
	}
}

type ArraySpan struct {
	Type    arrow.DataType
	Len     int64
	Nulls   int64
	Offset  int64
	Buffers [3]BufferSpan

	Scratch [2]uint64

	Children []ArraySpan
}

func (a *ArraySpan) UpdateNullCount() int64 {
	if a.Nulls != array.UnknownNullCount {
		return a.Nulls
	}

	a.Nulls = a.Len - int64(bitutil.CountSetBits(a.Buffers[0].Buf, int(a.Offset), int(a.Len)))
	return a.Nulls
}

func (a *ArraySpan) Dictionary() *ArraySpan { return &a.Children[0] }

func (a *ArraySpan) NumBuffers() int { return getNumBuffers(a.Type) }

func (a *ArraySpan) MakeData() arrow.ArrayData {
	bufs := make([]*memory.Buffer, a.NumBuffers())
	for i := range bufs {
		b := a.GetBuffer(i)
		if b != nil {
			defer b.Release()
		}
		bufs[i] = b
	}

	var (
		nulls  = int(a.Nulls)
		length = int(a.Len)
		off    = int(a.Offset)
		dt     = a.Type
	)

	if a.Type.ID() == arrow.NULL {
		nulls = int(length)
	} else if len(a.Buffers[0].Buf) == 0 {
		nulls = 0
	}

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

	children := make([]arrow.ArrayData, len(a.Children))
	for i, c := range a.Children {
		d := c.MakeData()
		defer d.Release()
		children[i] = d
	}
	return array.NewData(a.Type, length, bufs, children, nulls, off)
}

func (a *ArraySpan) MakeArray() arrow.Array {
	d := a.MakeData()
	defer d.Release()
	return array.MakeFromData(d)
}

func (a *ArraySpan) SetSlice(off, length int64) {
	a.Offset, a.Len = off, length
	if a.Type.ID() != arrow.NULL {
		a.Nulls = array.UnknownNullCount
	} else {
		a.Nulls = a.Len
	}
}

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

func (a *ArraySpan) resizeChildren(i int) {
	if cap(a.Children) >= i {
		a.Children = a.Children[:i]
	} else {
		a.Children = make([]ArraySpan, i)
	}
}

func setOffsetsForScalar[T int32 | int64](span *ArraySpan, buf []T, valueSize int64, bufidx int) {
	buf[0] = 0
	buf[1] = T(valueSize)

	b := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	s := (*reflect.SliceHeader)(unsafe.Pointer(&span.Buffers[bufidx].Buf))
	s.Data = b.Data
	s.Len = 2 * int(unsafe.Sizeof(T(0)))
	s.Cap = s.Len
}

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
	}

	switch {
	case typeID == arrow.BOOL:
		if val.(*scalar.Boolean).Value {
			a.Buffers[1].Buf = []byte{trueBit}
		} else {
			a.Buffers[1].Buf = []byte{falseBit}
		}
		a.Buffers[0].Owner = nil
	case arrow.IsPrimitive(typeID) || arrow.IsDecimal(typeID):
		sc := val.(scalar.PrimitiveScalar)
		a.Buffers[1].Buf = sc.Data()
		a.Buffers[1].Owner = nil
	case typeID == arrow.DICTIONARY:
		sc := val.(scalar.PrimitiveScalar)
		a.Buffers[1].Buf = sc.Data()
		a.Buffers[1].Owner = nil
		a.resizeChildren(1)
		a.Children[0].SetMembers(val.(*scalar.Dictionary).Value.Dict.Data())
	case arrow.IsBaseBinary(typeID):
		sc := val.(scalar.BinaryScalar)
		a.Buffers[1].Buf = arrow.Uint64Traits.CastToBytes(a.Scratch[:])
		a.Buffers[1].Owner = nil

		var dataBuffer []byte
		if sc.IsValid() {
			dataBuffer = sc.Data()
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
		}
	case typeID == arrow.STRUCT:
		sc := val.(*scalar.Struct)
		a.resizeChildren(len(sc.Value))
		for i, v := range sc.Value {
			a.Children[i].FillFromScalar(v)
		}
	case arrow.IsUnion(typeID):
		// first buffer is kept null since unions have no validity vector
		a.Buffers[0].Buf, a.Buffers[0].Owner = nil, nil

		a.Buffers[1].Buf = arrow.Uint64Traits.CastToBytes(a.Scratch[:])[:1]
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
	}
}

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

type ExecResult = ArraySpan

type ExecSpan struct {
	Len    int64
	Values []ExecValue
}

func ExecSpanFromBatch(batch *ExecBatch) *ExecSpan {
	out := &ExecSpan{Len: batch.Len, Values: make([]ExecValue, len(batch.Values))}
	for i, v := range batch.Values {
		outVal := &out.Values[i]
		if v.Kind() == KindScalar {
			outVal.Scalar = v.(*ScalarDatum).Value
		} else {
			outVal.Array.SetMembers(v.(*ArrayDatum).Value)
			outVal.Scalar = nil
		}
	}
	return out
}

func iterateExecSpans(batch *ExecBatch, maxChunkSize int64, promoteIfAllScalar bool) (bool, spanIterator, error) {
	if batch.NumValues() > 0 {
		inferred, allArgsSame := inferBatchLength(batch.Values)
		if inferred != batch.Len {
			return false, nil, fmt.Errorf("%w: value lengths differed from execbatch length", arrow.ErrInvalid)
		}
		if !allArgsSame {
			return false, nil, fmt.Errorf("%w: array args must all be the same length", arrow.ErrInvalid)
		}
	}

	var (
		args           []Datum = batch.Values
		haveChunked    bool    = false
		haveAllScalars bool    = checkIfAllScalar(batch)
		chunkIdxes     []int   = make([]int, len(args))
		valuePositions []int64 = make([]int64, len(args))

		valueOffsets []int64 = make([]int64, len(args))
		pos, length  int64   = 0, batch.Len
	)

	maxChunkSize = min(length, maxChunkSize)

	nextChunkSpan := func(iterSz int64, span ExecSpan) int64 {
		for i := 0; i < len(args) && iterSz > 0; i++ {
			// if the argument is not chunked, it's either a scalar or an array
			// in which case it doesn't influence the size of the span
			chunkedArg, ok := args[i].(*ChunkedDatum)
			if !ok {
				continue
			}

			arg := chunkedArg.Value
			if len(arg.Chunks()) == 0 {
				iterSz = 0
				continue
			}

			var curChunk arrow.Array
			for {
				curChunk = arg.Chunk(chunkIdxes[i])
				if valuePositions[i] == int64(curChunk.Len()) {
					// chunk is zero-length, or was exhausted in the previous
					// iteration, move to next chunk
					chunkIdxes[i]++
					curChunk = arg.Chunk(chunkIdxes[i])
					span.Values[i].Array.SetMembers(curChunk.Data())
					valuePositions[i] = 0
					valueOffsets[i] = int64(curChunk.Data().Offset())
					continue
				}
				break
			}
			iterSz = min(int64(curChunk.Len())-valuePositions[i], iterSz)
		}
		return iterSz
	}

	span := ExecSpan{Values: make([]ExecValue, len(args)), Len: 0}
	for i, a := range args {
		switch arg := a.(type) {
		case *ScalarDatum:
			span.Values[i].Scalar = arg.Value
		case *ArrayDatum:
			span.Values[i].Array.SetMembers(arg.Value)
			valueOffsets[i] = int64(arg.Value.Offset())
		case *ChunkedDatum:
			// populate from the first chunk
			carr := arg.Value
			if len(carr.Chunks()) > 0 {
				arr := carr.Chunk(0).Data()
				span.Values[i].Array.SetMembers(arr)
				valueOffsets[i] = int64(arr.Offset())
			} else {
				// fill as zero length
				fillZeroLength(carr.DataType(), &span.Values[i].Array)
				span.Values[i].Scalar = nil
			}
			haveChunked = true
		}
	}

	if haveAllScalars && promoteIfAllScalar {
		promoteExecSpanScalars(span)
	}

	return haveAllScalars, func() (*ExecSpan, int64, bool) {
		if pos == length {
			return nil, pos, false
		}

		iterationSize := min(length-pos, maxChunkSize)
		if haveChunked {
			iterationSize = nextChunkSpan(iterationSize, span)
		}

		span.Len = iterationSize
		for i, a := range args {
			if a.Kind() != KindScalar {
				span.Values[i].Array.SetSlice(valuePositions[i]+valueOffsets[i], iterationSize)
				valuePositions[i] += iterationSize
			}
		}

		pos += iterationSize
		debug.Assert(pos <= length, "bad state for iteration exec span")
		return &span, pos, true
	}, nil
}

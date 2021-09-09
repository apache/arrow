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
	"math"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

func Concatenate(arrs []Interface, mem memory.Allocator) (Interface, error) {
	if len(arrs) == 0 {
		return nil, xerrors.New("array/concat: must pass at least one array")
	}

	// gather Data of inputs
	data := make([]*Data, len(arrs))
	for i, ar := range arrs {
		if !arrow.TypeEqual(ar.DataType(), arrs[0].DataType()) {
			return nil, xerrors.Errorf("arrays to be concatenated must be identically typed, but %s and %s were encountered",
				arrs[0].DataType(), ar.DataType())
		}
		data[i] = ar.Data()
	}

	out, err := concat(data, mem)
	if err != nil {
		return nil, err
	}

	defer out.Release()
	return MakeFromData(out), nil
}

type rng struct {
	offset, len int
}

type bitmap struct {
	data []byte
	rng  rng
}

func gatherBitmaps(data []*Data, idx int) []bitmap {
	out := make([]bitmap, len(data))
	for i, d := range data {
		if d.buffers[idx] != nil {
			out[i].data = d.buffers[idx].Bytes()
		}
		out[i].rng.offset = d.offset
		out[i].rng.len = d.length
	}
	return out
}

func gatherBuffers(data []*Data, idx int) []*memory.Buffer {
	return gatherFixedBuffers(data, idx, 1)
}

func gatherFixedBuffers(data []*Data, idx, byteWidth int) []*memory.Buffer {
	out := make([]*memory.Buffer, 0, len(data))
	for _, d := range data {
		buf := d.buffers[idx]
		if buf == nil {
			continue
		}

		out = append(out, memory.NewBufferBytes(buf.Bytes()[d.offset*byteWidth:(d.offset+d.length)*byteWidth]))
	}
	return out
}

func gatherBuffersFixedWidthType(data []*Data, idx int, fixed arrow.FixedWidthDataType) []*memory.Buffer {
	return gatherFixedBuffers(data, idx, fixed.BitWidth()/8)
}

func gatherBufferRanges(data []*Data, idx int, ranges []rng) []*memory.Buffer {
	out := make([]*memory.Buffer, 0, len(data))
	for i, d := range data {
		buf := d.buffers[idx]
		if buf == nil {
			debug.Assert(ranges[i].len == 0, "misaligned buffer value ranges")
			continue
		}

		out = append(out, memory.NewBufferBytes(buf.Bytes()[ranges[i].offset:ranges[i].offset+ranges[i].len]))
	}
	return out
}

func gatherChildren(data []*Data, idx int) []*Data {
	return gatherChildrenMultiplier(data, idx, 1)
}

func gatherChildrenMultiplier(data []*Data, idx, multiplier int) []*Data {
	out := make([]*Data, len(data))
	for i, d := range data {
		out[i] = NewSliceData(d.childData[idx], int64(d.offset*multiplier), int64(d.offset+d.length)*int64(multiplier))
	}
	return out
}

func gatherChildrenRanges(data []*Data, idx int, ranges []rng) []*Data {
	debug.Assert(len(data) == len(ranges), "mismatched children ranges for concat")
	out := make([]*Data, len(data))
	for i, d := range data {
		out[i] = NewSliceData(d.childData[idx], int64(ranges[i].offset), int64(ranges[i].offset+ranges[i].len))
	}
	return out
}

func concatBuffers(bufs []*memory.Buffer, mem memory.Allocator) *memory.Buffer {
	outLen := 0
	for _, b := range bufs {
		outLen += b.Len()
	}
	out := memory.NewResizableBuffer(mem)
	out.Resize(outLen)

	data := out.Bytes()
	for _, b := range bufs {
		copy(data, b.Bytes())
		data = data[b.Len():]
	}
	return out
}

func concatOffsets(buffers []*memory.Buffer, mem memory.Allocator) (*memory.Buffer, []rng, error) {
	outLen := 0
	for _, b := range buffers {
		outLen += b.Len() / arrow.Int32SizeBytes
	}

	out := memory.NewResizableBuffer(mem)
	out.Resize(arrow.Int32Traits.BytesRequired(outLen + 1))

	dst := arrow.Int32Traits.CastFromBytes(out.Bytes())
	valuesRanges := make([]rng, len(buffers))
	nextOffset := int32(0)
	nextElem := int(0)
	for i, b := range buffers {
		if b.Len() == 0 {
			valuesRanges[i].offset = 0
			valuesRanges[i].len = 0
			continue
		}

		src := arrow.Int32Traits.CastFromBytes(b.Bytes())
		valuesRanges[i].offset = int(src[0])
		expand := src[:len(src)+1]
		valuesRanges[i].len = int(expand[len(src)]) - valuesRanges[i].offset

		if nextOffset > math.MaxInt32-int32(valuesRanges[i].len) {
			return nil, nil, xerrors.New("offset overflow while concatenating arrays")
		}

		adj := nextOffset - src[0]
		for j, o := range src {
			dst[nextElem+j] = adj + o
		}

		nextElem += b.Len() / arrow.Int32SizeBytes
		nextOffset += int32(valuesRanges[i].len)
	}

	dst[outLen] = nextOffset
	return out, valuesRanges, nil
}

func concat(data []*Data, mem memory.Allocator) (*Data, error) {
	out := &Data{refCount: 1, dtype: data[0].dtype, nulls: 0}
	for _, d := range data {
		out.length += d.length
		if out.nulls == UnknownNullCount || d.nulls == UnknownNullCount {
			out.nulls = UnknownNullCount
			continue
		}
		out.nulls += d.nulls
	}

	out.buffers = make([]*memory.Buffer, len(data[0].buffers))
	if out.nulls != 0 && out.dtype.ID() != arrow.NULL {
		bm, err := concatBitmaps(gatherBitmaps(data, 0), mem)
		if err != nil {
			return nil, err
		}
		out.buffers[0] = bm
	}

	switch dt := out.dtype.(type) {
	case *arrow.NullType:
	case *arrow.BooleanType:
		bm, err := concatBitmaps(gatherBitmaps(data, 1), mem)
		if err != nil {
			return nil, err
		}
		out.buffers[1] = bm
	case arrow.FixedWidthDataType:
		out.buffers[1] = concatBuffers(gatherBuffersFixedWidthType(data, 1, dt), mem)
	case arrow.BinaryDataType:
		offsetBuffer, valueRanges, err := concatOffsets(gatherFixedBuffers(data, 1, arrow.Int32SizeBytes), mem)
		if err != nil {
			return nil, err
		}
		out.buffers[2] = concatBuffers(gatherBufferRanges(data, 2, valueRanges), mem)
		out.buffers[1] = offsetBuffer
	case *arrow.ListType:
		offsetBuffer, valueRanges, err := concatOffsets(gatherFixedBuffers(data, 1, arrow.Int32SizeBytes), mem)
		if err != nil {
			return nil, err
		}
		childData := gatherChildrenRanges(data, 0, valueRanges)
		out.buffers[1] = offsetBuffer
		out.childData = make([]*Data, 1)
		out.childData[0], err = concat(childData, mem)
	case *arrow.FixedSizeListType:
		childData := gatherChildrenMultiplier(data, 0, int(dt.Len()))
		children, err := concat(childData, mem)
		if err != nil {
			return nil, err
		}
		out.childData = []*Data{children}
	case *arrow.StructType:
		out.childData = make([]*Data, len(dt.Fields()))
		for i := range dt.Fields() {
			childData, err := concat(gatherChildren(data, i), mem)
			if err != nil {
				return nil, err
			}
			out.childData[i] = childData
		}
	case *arrow.MapType:
		offsetBuffer, valueRanges, err := concatOffsets(gatherFixedBuffers(data, 1, arrow.Int32SizeBytes), mem)
		if err != nil {
			return nil, err
		}
		childData := gatherChildrenRanges(data, 0, valueRanges)
		out.buffers[1] = offsetBuffer
		out.childData = make([]*Data, 1)
		out.childData[0], err = concat(childData, mem)
	default:
		return nil, xerrors.Errorf("concatenate not implemented for type %s", dt)
	}

	return out, nil
}

func addOvf(x, y int) (int, bool) {
	sum := x + y
	return sum, ((x&y)|((x|y)&^sum))>>62 == 1
}

func concatBitmaps(bitmaps []bitmap, mem memory.Allocator) (*memory.Buffer, error) {
	var (
		outlen   int
		overflow bool
	)

	for _, bm := range bitmaps {
		if outlen, overflow = addOvf(outlen, bm.rng.len); overflow {
			return nil, xerrors.New("length overflow when concatenating arrays")
		}
	}

	out := memory.NewResizableBuffer(mem)
	out.Resize(int(bitutil.BytesForBits(int64(outlen))))
	dst := out.Bytes()

	offset := 0
	for _, bm := range bitmaps {
		if bm.data == nil {
			bitutil.SetBitsTo(out.Bytes(), int64(offset), int64(bm.rng.len), true)
		} else {
			bitutil.CopyBitmap(bm.data, bm.rng.offset, bm.rng.len, dst, offset)
		}
		offset += bm.rng.len
	}
	return out, nil
}

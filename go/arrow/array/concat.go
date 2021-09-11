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
	"math/bits"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

// Concatenate creates a new array.Interface which is the concatenation of the
// passed in arrays. Returns nil if an error is encountered.
//
// The passed in arrays still need to be released manually, and will not be
// released by this function.
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

// simple struct to hold ranges
type rng struct {
	offset, len int
}

// simple bitmap struct to reference a specific slice of a bitmap where the range
// offset and length are in bits
type bitmap struct {
	data []byte
	rng  rng
}

// gather up the bitmaps from the passed in data objects
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

// gatherFixedBuffers gathers up the buffer objects of the given index, specifically
// returning only the slices of the buffers which are relevant to the passed in arrays
// in case they are themselves slices of other arrays. nil buffers are ignored and not
// in the output slice.
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

// gatherBuffersFixedWidthType is like gatherFixedBuffers, but uses a datatype to determine the size
// to use for determining the byte slice rather than a passed in bytewidth.
func gatherBuffersFixedWidthType(data []*Data, idx int, fixed arrow.FixedWidthDataType) []*memory.Buffer {
	return gatherFixedBuffers(data, idx, fixed.BitWidth()/8)
}

// gatherBufferRanges requires that len(ranges) == len(data) and returns a list of buffers
// which represent the corresponding range of each buffer in the specified index of each
// data object.
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

// gatherChildren gathers the children data objects for child of index idx for all of the data objects.
func gatherChildren(data []*Data, idx int) []*Data {
	return gatherChildrenMultiplier(data, idx, 1)
}

// gatherChildrenMultiplier gathers the full data slice of the underlying values from the children data objects
// such as the values data for a list array so that it can return a slice of the buffer for a given
// index into the children.
func gatherChildrenMultiplier(data []*Data, idx, multiplier int) []*Data {
	out := make([]*Data, len(data))
	for i, d := range data {
		out[i] = NewSliceData(d.childData[idx], int64(d.offset*multiplier), int64(d.offset+d.length)*int64(multiplier))
	}
	return out
}

// gatherChildrenRanges returns a slice of Data objects which each represent slices of the given ranges from the
// child in the specified index from each data object.
func gatherChildrenRanges(data []*Data, idx int, ranges []rng) []*Data {
	debug.Assert(len(data) == len(ranges), "mismatched children ranges for concat")
	out := make([]*Data, len(data))
	for i, d := range data {
		out[i] = NewSliceData(d.childData[idx], int64(ranges[i].offset), int64(ranges[i].offset+ranges[i].len))
	}
	return out
}

// creates a single contiguous buffer which contains the concatenation of all of the passed
// in buffer objects.
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

// concatOffsets creates a single offset buffer which represents the concatenation of all of the
// offsets buffers, adjusting the offsets appropriately to their new relative locations.
//
// It also returns the list of ranges that need to be fetched for the corresponding value buffers
// to construct the final concatenated value buffer.
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

		// when we gather our buffers, we sliced off the last offset from the buffer
		// so that we could count the lengths accurately
		src := arrow.Int32Traits.CastFromBytes(b.Bytes())
		valuesRanges[i].offset = int(src[0])
		// expand our slice to see that final offset
		expand := src[:len(src)+1]
		// compute the length of this range by taking the final offset and subtracting where we started.
		valuesRanges[i].len = int(expand[len(src)]) - valuesRanges[i].offset

		if nextOffset > math.MaxInt32-int32(valuesRanges[i].len) {
			return nil, nil, xerrors.New("offset overflow while concatenating arrays")
		}

		// adjust each offset by the difference between our last ending point and our starting point
		adj := nextOffset - src[0]
		for j, o := range src {
			dst[nextElem+j] = adj + o
		}

		// the next index for an element in the output buffer
		nextElem += b.Len() / arrow.Int32SizeBytes
		// update our offset counter to be the total current length of our output
		nextOffset += int32(valuesRanges[i].len)
	}

	// final offset should point to the end of the data
	dst[outLen] = nextOffset
	return out, valuesRanges, nil
}

// concat is the implementation for actually performing the concatenation of the *array.Data
// objects that we can call internally for nested types.
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
		for _, c := range childData {
			defer c.Release()
		}

		out.buffers[1] = offsetBuffer
		out.childData = make([]*Data, 1)
		out.childData[0], err = concat(childData, mem)
		if err != nil {
			return nil, err
		}
	case *arrow.FixedSizeListType:
		childData := gatherChildrenMultiplier(data, 0, int(dt.Len()))
		for _, c := range childData {
			defer c.Release()
		}

		children, err := concat(childData, mem)
		if err != nil {
			return nil, err
		}
		out.childData = []*Data{children}
	case *arrow.StructType:
		out.childData = make([]*Data, len(dt.Fields()))
		for i := range dt.Fields() {
			children := gatherChildren(data, i)
			for _, c := range children {
				defer c.Release()
			}

			childData, err := concat(children, mem)
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
		for _, c := range childData {
			defer c.Release()
		}

		out.buffers[1] = offsetBuffer
		out.childData = make([]*Data, 1)
		out.childData[0], err = concat(childData, mem)
		if err != nil {
			return nil, err
		}
	default:
		return nil, xerrors.Errorf("concatenate not implemented for type %s", dt)
	}

	return out, nil
}

// check overflow in the addition, taken from bits.Add but adapted for signed integers
// rather than unsigned integers. bits.UintSize will be either 32 or 64 based on
// whether our architecture is 32 bit or 64. The operation is the same for both cases,
// the only difference is how much we need to shift by 30 for 32 bit and 62 for 64 bit.
// Thus, bits.UintSize - 2 is how much we shift right by to check if we had an overflow
// in the signed addition.
//
// First return is the result of the sum, the second return is true if there was an overflow
func addOvf(x, y int) (int, bool) {
	sum := x + y
	return sum, ((x&y)|((x|y)&^sum))>>(bits.UintSize-2) == 1
}

// concatenate bitmaps together and return a buffer with the combined bitmaps
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
		if bm.data == nil { // if the bitmap is nil, that implies that the value is true for all elements
			bitutil.SetBitsTo(out.Bytes(), int64(offset), int64(bm.rng.len), true)
		} else {
			bitutil.CopyBitmap(bm.data, bm.rng.offset, bm.rng.len, dst, offset)
		}
		offset += bm.rng.len
	}
	return out, nil
}

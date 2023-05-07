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

package gen

import (
	"math"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

// RandomArrayGenerator is a struct used for constructing Random Arrow arrays
// for use with testing.
type RandomArrayGenerator struct {
	seed     uint64
	extra    uint64
	src      rand.Source
	seedRand *rand.Rand
	mem      memory.Allocator
}

// NewRandomArrayGenerator constructs a new generator with the requested Seed
func NewRandomArrayGenerator(seed uint64, mem memory.Allocator) RandomArrayGenerator {
	src := rand.NewSource(seed)
	return RandomArrayGenerator{seed, 0, src, rand.New(src), mem}
}

// GenerateBitmap generates a bitmap of n bits and stores it into buffer. Prob is the probability
// that a given bit will be zero, with 1-prob being the probability it will be 1. The return value
// is the number of bits that were left unset. The assumption being that buffer is currently
// zero initialized as this function does not clear any bits, it only sets 1s.
func (r *RandomArrayGenerator) GenerateBitmap(buffer []byte, n int64, prob float64) int64 {
	count := int64(0)
	r.extra++

	// bernoulli distribution uses P to determine the probabitiliy of a 0 or a 1,
	// which we'll use to generate the bitmap.
	dist := distuv.Bernoulli{P: 1 - prob, Src: rand.NewSource(r.seed + r.extra)}
	for i := 0; int64(i) < n; i++ {
		if dist.Rand() != float64(0.0) {
			bitutil.SetBit(buffer, i)
		} else {
			count++
		}
	}

	return count
}

func (r *RandomArrayGenerator) Boolean(size int64, prob, nullProb float64) arrow.Array {
	buffers := make([]*memory.Buffer, 2)
	nullcount := int64(0)

	buffers[0] = memory.NewResizableBuffer(r.mem)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	defer buffers[0].Release()
	nullcount = r.GenerateBitmap(buffers[0].Bytes(), size, nullProb)

	buffers[1] = memory.NewResizableBuffer(r.mem)
	buffers[1].Resize(int(bitutil.BytesForBits(size)))
	defer buffers[1].Release()
	r.GenerateBitmap(buffers[1].Bytes(), size, prob)

	data := array.NewData(arrow.FixedWidthTypes.Boolean, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewBooleanData(data)
}

func (r *RandomArrayGenerator) baseGenPrimitive(size int64, prob float64, byteWidth int) ([]*memory.Buffer, int64) {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(r.mem)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.GenerateBitmap(buffers[0].Bytes(), size, prob)

	buffers[1] = memory.NewResizableBuffer(r.mem)
	buffers[1].Resize(int(size) * byteWidth)

	return buffers, nullCount
}

func (r *RandomArrayGenerator) Int8(size int64, min, max int8, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Int8SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int8Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = int8(dist.Intn(int(max)-int(min+1))) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Int8, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewInt8Data(data)
}

func (r *RandomArrayGenerator) Uint8(size int64, min, max uint8, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Uint8SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Uint8Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = uint8(dist.Intn(int(max)-int(min)+1)) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Uint8, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewUint8Data(data)
}

func (r *RandomArrayGenerator) Int16(size int64, min, max int16, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Int16SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int16Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = int16(dist.Intn(int(max)-int(min)+1)) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Int16, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewInt16Data(data)
}

func (r *RandomArrayGenerator) Uint16(size int64, min, max uint16, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Uint16SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Uint16Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = uint16(dist.Intn(int(max)-int(min)+1)) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Uint16, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewUint16Data(data)
}

func (r *RandomArrayGenerator) Int32(size int64, min, max int32, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Int32SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = int32(dist.Intn(int(max)-int(min)+1)) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Int32, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewInt32Data(data)
}

func (r *RandomArrayGenerator) Uint32(size int64, min, max uint32, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Uint32SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Uint32Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = uint32(dist.Uint64n(uint64(max)-uint64(min)+1)) + min
	}

	data := array.NewData(arrow.PrimitiveTypes.Uint32, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewUint32Data(data)
}

func (r *RandomArrayGenerator) Int64(size int64, min, max int64, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Int64SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
	if max == math.MaxInt64 && min == math.MinInt64 {
		for i := int64(0); i < size; i++ {
			out[i] = int64(dist.Uint64())
		}
	} else {
		for i := int64(0); i < size; i++ {
			out[i] = dist.Int63n(max-min+1) + min
		}
	}

	data := array.NewData(arrow.PrimitiveTypes.Int64, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewInt64Data(data)
}

func (r *RandomArrayGenerator) Uint64(size int64, min, max uint64, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Uint64SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Uint64Traits.CastFromBytes(buffers[1].Bytes())
	if max == math.MaxUint64 {
		for i := int64(0); i < size; i++ {
			out[i] = dist.Uint64() + min
		}
	} else {
		for i := int64(0); i < size; i++ {
			out[i] = dist.Uint64n(max-min+1) + min
		}
	}

	data := array.NewData(arrow.PrimitiveTypes.Uint64, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewUint64Data(data)
}

func (r *RandomArrayGenerator) Float32(size int64, min, max float32, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Float32SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Float32Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = min + dist.Float32()*(max+1-min)
	}

	data := array.NewData(arrow.PrimitiveTypes.Float32, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewFloat32Data(data)
}

func (r *RandomArrayGenerator) Float64(size int64, min, max float64, prob float64) arrow.Array {
	buffers, nullcount := r.baseGenPrimitive(size, prob, arrow.Float64SizeBytes)
	for _, b := range buffers {
		defer b.Release()
	}

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Float64Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = dist.NormFloat64() + (max - min)
	}

	data := array.NewData(arrow.PrimitiveTypes.Float64, int(size), buffers, nil, int(nullcount), 0)
	defer data.Release()
	return array.NewFloat64Data(data)
}

func (r *RandomArrayGenerator) String(size int64, minLength, maxLength int, nullprob float64) arrow.Array {
	lengths := r.Int32(size, int32(minLength), int32(maxLength), nullprob).(*array.Int32)
	defer lengths.Release()

	bldr := array.NewStringBuilder(r.mem)
	defer bldr.Release()

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))

	buf := make([]byte, 0, maxLength)
	gen := func(n int32) string {
		out := buf[:n]
		for i := range out {
			out[i] = uint8(dist.Int31n(int32('z')-int32('A')+1) + int32('A'))
		}
		return string(out)
	}

	for i := 0; i < lengths.Len(); i++ {
		if lengths.IsValid(i) {
			bldr.Append(gen(lengths.Value(i)))
		} else {
			bldr.AppendNull()
		}
	}

	return bldr.NewArray()
}

func (r *RandomArrayGenerator) LargeString(size int64, minLength, maxLength int64, nullprob float64) arrow.Array {
	lengths := r.Int64(size, minLength, maxLength, nullprob).(*array.Int64)
	defer lengths.Release()

	bldr := array.NewLargeStringBuilder(r.mem)
	defer bldr.Release()

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))

	buf := make([]byte, 0, maxLength)
	gen := func(n int64) string {
		out := buf[:n]
		for i := range out {
			out[i] = uint8(dist.Int63n(int64('z')-int64('A')+1) + int64('A'))
		}
		return string(out)
	}

	for i := 0; i < lengths.Len(); i++ {
		if lengths.IsValid(i) {
			bldr.Append(gen(lengths.Value(i)))
		} else {
			bldr.AppendNull()
		}
	}

	return bldr.NewArray()
}

func (r *RandomArrayGenerator) Numeric(dt arrow.Type, size int64, min, max int64, nullprob float64) arrow.Array {
	switch dt {
	case arrow.INT8:
		return r.Int8(size, int8(min), int8(max), nullprob)
	case arrow.UINT8:
		return r.Uint8(size, uint8(min), uint8(max), nullprob)
	case arrow.INT16:
		return r.Int16(size, int16(min), int16(max), nullprob)
	case arrow.UINT16:
		return r.Uint16(size, uint16(min), uint16(max), nullprob)
	case arrow.INT32:
		return r.Int32(size, int32(min), int32(max), nullprob)
	case arrow.UINT32:
		return r.Uint32(size, uint32(min), uint32(max), nullprob)
	case arrow.INT64:
		return r.Int64(size, int64(min), int64(max), nullprob)
	case arrow.UINT64:
		return r.Uint64(size, uint64(min), uint64(max), nullprob)
	case arrow.FLOAT32:
		return r.Float32(size, float32(min), float32(max), nullprob)
	case arrow.FLOAT64:
		return r.Float64(size, float64(min), float64(max), nullprob)
	}
	panic("invalid type for random numeric array")
}

func (r *RandomArrayGenerator) ArrayOf(dt arrow.Type, size int64, nullprob float64) arrow.Array {
	switch dt {
	case arrow.BOOL:
		return r.Boolean(size, 0.50, nullprob)
	case arrow.STRING:
		return r.String(size, 0, 20, nullprob)
	case arrow.LARGE_STRING:
		return r.LargeString(size, 0, 20, nullprob)
	case arrow.INT8:
		return r.Int8(size, math.MinInt8, math.MaxInt8, nullprob)
	case arrow.UINT8:
		return r.Uint8(size, 0, math.MaxUint8, nullprob)
	case arrow.INT16:
		return r.Int16(size, math.MinInt16, math.MaxInt16, nullprob)
	case arrow.UINT16:
		return r.Uint16(size, 0, math.MaxUint16, nullprob)
	case arrow.INT32:
		return r.Int32(size, math.MinInt32, math.MaxInt32, nullprob)
	case arrow.UINT32:
		return r.Uint32(size, 0, math.MaxUint32, nullprob)
	case arrow.INT64:
		return r.Int64(size, math.MinInt64, math.MaxInt64, nullprob)
	case arrow.UINT64:
		return r.Uint64(size, 0, math.MaxUint64, nullprob)
	case arrow.FLOAT32:
		return r.Float32(size, -math.MaxFloat32, math.MaxFloat32, nullprob)
	case arrow.FLOAT64:
		return r.Float64(size, -math.MaxFloat64, math.MaxFloat64, nullprob)
	}
	panic("unimplemented ArrayOf type")
}

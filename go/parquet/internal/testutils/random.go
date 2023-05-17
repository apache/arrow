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

// Package testutils contains utilities for generating random data and other
// helpers that are used for testing the various aspects of the parquet library.
package testutils

import (
	"encoding/binary"
	"math"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/endian"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"

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
}

// NewRandomArrayGenerator constructs a new generator with the requested Seed
func NewRandomArrayGenerator(seed uint64) RandomArrayGenerator {
	src := rand.NewSource(seed)
	return RandomArrayGenerator{seed, 0, src, rand.New(src)}
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
	dist := distuv.Bernoulli{P: prob, Src: rand.NewSource(r.seed + r.extra)}
	for i := 0; int64(i) < n; i++ {
		if dist.Rand() != float64(0.0) {
			bitutil.SetBit(buffer, i)
		} else {
			count++
		}
	}

	return count
}

// ByteArray creates an array.String for use of creating random ByteArray values for testing parquet
// writing/reading. minLen/maxLen are the min and max length for a given value in the resulting array,
// with nullProb being the probability of a given index being null.
//
// For this generation we only generate ascii values with a min of 'A' and max of 'z'.
func (r *RandomArrayGenerator) ByteArray(size int64, minLen, maxLen int32, nullProb float64) arrow.Array {
	if nullProb < 0 || nullProb > 1 {
		panic("null prob must be between 0 and 1")
	}

	lengths := r.Int32(size, minLen, maxLen, nullProb)
	defer lengths.Release()

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	bldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	strbuf := make([]byte, maxLen)

	for i := 0; int64(i) < size; i++ {
		if lengths.IsValid(i) {
			l := lengths.Value(i)
			for j := int32(0); j < l; j++ {
				strbuf[j] = byte(dist.Int31n(int32('z')-int32('A')+1) + int32('A'))
			}
			val := strbuf[:l]
			bldr.Append(*(*string)(unsafe.Pointer(&val)))
		} else {
			bldr.AppendNull()
		}
	}

	return bldr.NewArray()
}

// Uint8 generates a random array.Uint8 of the requested size whose values are between min and max
// with prob as the probability that a given index will be null.
func (r *RandomArrayGenerator) Uint8(size int64, min, max uint8, prob float64) arrow.Array {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.GenerateBitmap(buffers[0].Bytes(), size, prob)

	buffers[1] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[1].Resize(int(size * int64(arrow.Uint8SizeBytes)))

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Uint8Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = uint8(dist.Intn(int(max-min+1))) + min
	}

	return array.NewUint8Data(array.NewData(arrow.PrimitiveTypes.Uint8, int(size), buffers, nil, int(nullCount), 0))
}

// Int32 generates a random array.Int32 of the given size with each value between min and max,
// and pctNull as the probability that a given index will be null.
func (r *RandomArrayGenerator) Int32(size int64, min, max int32, pctNull float64) *array.Int32 {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.GenerateBitmap(buffers[0].Bytes(), size, 1-pctNull)

	buffers[1] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[1].Resize(arrow.Int32Traits.BytesRequired(int(size)))

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = dist.Int31n(max-min+1) + min
	}
	return array.NewInt32Data(array.NewData(arrow.PrimitiveTypes.Int32, int(size), buffers, nil, int(nullCount), 0))
}

// Int64 generates a random array.Int64 of the given size with each value between min and max,
// and pctNull as the probability that a given index will be null.
func (r *RandomArrayGenerator) Int64(size int64, min, max int64, pctNull float64) *array.Int64 {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.GenerateBitmap(buffers[0].Bytes(), size, 1-pctNull)

	buffers[1] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[1].Resize(arrow.Int64Traits.BytesRequired(int(size)))

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = dist.Int63n(max-min+1) + min
	}
	return array.NewInt64Data(array.NewData(arrow.PrimitiveTypes.Int64, int(size), buffers, nil, int(nullCount), 0))
}

// Float64 generates a random array.Float64 of the requested size with pctNull as the probability
// that a given index will be null.
func (r *RandomArrayGenerator) Float64(size int64, pctNull float64) *array.Float64 {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.GenerateBitmap(buffers[0].Bytes(), size, 1-pctNull)

	buffers[1] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[1].Resize(arrow.Float64Traits.BytesRequired(int(size)))

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Float64Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = dist.NormFloat64()
	}
	return array.NewFloat64Data(array.NewData(arrow.PrimitiveTypes.Float64, int(size), buffers, nil, int(nullCount), 0))
}

func (r *RandomArrayGenerator) StringWithRepeats(mem memory.Allocator, sz, unique int64, minLen, maxLen int32, nullProb float64) *array.String {
	if unique > sz {
		panic("invalid config for random StringWithRepeats")
	}

	// generate a random string dictionary without any nulls
	arr := r.ByteArray(unique, minLen, maxLen, 0)
	defer arr.Release()
	dict := arr.(*array.String)

	// generate random indices to sample dictionary with
	idArray := r.Int64(sz, 0, unique-1, nullProb)
	defer idArray.Release()

	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()

	for i := int64(0); i < sz; i++ {
		if idArray.IsValid(int(i)) {
			idx := idArray.Value(int(i))
			bldr.Append(dict.Value(int(idx)))
		} else {
			bldr.AppendNull()
		}
	}

	return bldr.NewStringArray()
}

// FillRandomInt8 populates the slice out with random int8 values between min and max using
// seed as the random see for generation to allow consistency for testing.
func FillRandomInt8(seed uint64, min, max int8, out []int8) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int8(r.Intn(int(max-min+1))) + min
	}
}

// FillRandomUint8 populates the slice out with random uint8 values between min and max using
// seed as the random see for generation to allow consistency for testing.
func FillRandomUint8(seed uint64, min, max uint8, out []uint8) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint8(r.Intn(int(max-min+1))) + min
	}
}

// FillRandomInt16 populates the slice out with random int16 values between min and max using
// seed as the random see for generation to allow consistency for testing.
func FillRandomInt16(seed uint64, min, max int16, out []int16) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int16(r.Intn(int(max-min+1))) + min
	}
}

// FillRandomUint16 populates the slice out with random uint16 values between min and max using
// seed as the random see for generation to allow consistency for testing.
func FillRandomUint16(seed uint64, min, max uint16, out []uint16) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint16(r.Intn(int(max-min+1))) + min
	}
}

// FillRandomInt32 populates out with random int32 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomInt32(seed uint64, out []int32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int32(r.Uint32())
	}
}

// FillRandomInt32Max populates out with random int32 values between 0 and max using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomInt32Max(seed uint64, max int32, out []int32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Int31n(max)
	}
}

// FillRandomUint32Max populates out with random uint32 values between 0 and max using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomUint32Max(seed uint64, max uint32, out []uint32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint32(r.Uint64n(uint64(max)))
	}
}

// FillRandomInt64Max populates out with random int64 values between 0 and max using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomInt64Max(seed uint64, max int64, out []int64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Int63n(max)
	}
}

// FillRandomUint32 populates out with random uint32 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomUint32(seed uint64, out []uint32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint32()
	}
}

// FillRandomUint64 populates out with random uint64 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomUint64(seed uint64, out []uint64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint64()
	}
}

// FillRandomUint64Max populates out with random uint64 values between 0 and max using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomUint64Max(seed uint64, max uint64, out []uint64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint64n(max)
	}
}

// FillRandomInt64 populates out with random int64 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomInt64(seed uint64, out []int64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int64(r.Uint64())
	}
}

// FillRandomInt96 populates out with random Int96 values using seed as the random
// seed for the generator to allow consistency for testing. It does this by generating
// three random uint32 values for each int96 value.
func FillRandomInt96(seed uint64, out []parquet.Int96) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		*(*int32)(unsafe.Pointer(&out[idx][0])) = int32(r.Uint32())
		*(*int32)(unsafe.Pointer(&out[idx][4])) = int32(r.Uint32())
		*(*int32)(unsafe.Pointer(&out[idx][8])) = int32(r.Uint32())
	}
}

// randFloat32 creates a random float value with a normal distribution
// to better spread the values out and ensure we do not return any NaN values.
func randFloat32(r *rand.Rand) float32 {
	for {
		f := math.Float32frombits(r.Uint32())
		if !math.IsNaN(float64(f)) {
			return f
		}
	}
}

// randFloat64 creates a random float value with a normal distribution
// to better spread the values out and ensure we do not return any NaN values.
func randFloat64(r *rand.Rand) float64 {
	for {
		f := math.Float64frombits(r.Uint64())
		if !math.IsNaN(f) {
			return f
		}
	}
}

// FillRandomFloat32 populates out with random float32 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomFloat32(seed uint64, out []float32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = randFloat32(r)
	}
}

// FillRandomFloat64 populates out with random float64 values using seed as the random
// seed for the generator to allow consistency for testing.
func FillRandomFloat64(seed uint64, out []float64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = randFloat64(r)
	}
}

// FillRandomByteArray populates out with random ByteArray values with lengths between 2 and 12
// using heap as the actual memory storage used for the bytes generated. Each element of
// out will be some slice of the bytes in heap, and as such heap must outlive the byte array slices.
func FillRandomByteArray(seed uint64, out []parquet.ByteArray, heap *memory.Buffer) {
	const (
		maxByteArrayLen = 12
		minByteArrayLen = 2
	)
	RandomByteArray(seed, out, heap, minByteArrayLen, maxByteArrayLen)
}

// FillRandomFixedByteArray populates out with random FixedLenByteArray values with of a length equal to size
// using heap as the actual memory storage used for the bytes generated. Each element of
// out will be a slice of size bytes in heap, and as such heap must outlive the byte array slices.
func FillRandomFixedByteArray(seed uint64, out []parquet.FixedLenByteArray, heap *memory.Buffer, size int) {
	heap.Resize(len(out) * size)

	buf := heap.Bytes()
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		r.Read(buf[:size])
		out[idx] = buf[:size]
		buf = buf[size:]
	}
}

// FillRandomBooleans populates out with random bools with the probability p of being false using
// seed as the random seed to the generator in order to allow consistency for testing. This uses
// a Bernoulli distribution of values.
func FillRandomBooleans(p float64, seed uint64, out []bool) {
	dist := distuv.Bernoulli{P: p, Src: rand.NewSource(seed)}
	for idx := range out {
		out[idx] = dist.Rand() != float64(0.0)
	}
}

// fillRandomIsValid populates out with random bools with the probability pctNull of being false using
// seed as the random seed to the generator in order to allow consistency for testing. This uses
// the default Golang random generator distribution of float64 values between 0 and 1 comparing against
// pctNull. If the random value is > pctNull, it is true.
func fillRandomIsValid(seed uint64, pctNull float64, out []bool) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Float64() > pctNull
	}
}

// InitValues is a convenience function for generating a slice of random values based on the type.
// If the type is parquet.ByteArray or parquet.FixedLenByteArray, heap must not be null.
//
// The default values are:
//  []bool uses the current time as the seed with only values of 1 being false, for use
//   of creating validity boolean slices.
//  all other types use 0 as the seed
//  a []parquet.ByteArray is populated with lengths between 2 and 12
//  a []parquet.FixedLenByteArray is populated with fixed size random byte arrays of length 12.
func InitValues(values interface{}, heap *memory.Buffer) {
	switch arr := values.(type) {
	case []bool:
		fillRandomIsValid(uint64(time.Now().Unix()), 1.0, arr)
	case []int32:
		FillRandomInt32(0, arr)
	case []int64:
		FillRandomInt64(0, arr)
	case []float32:
		FillRandomFloat32(0, arr)
	case []float64:
		FillRandomFloat64(0, arr)
	case []parquet.Int96:
		FillRandomInt96(0, arr)
	case []parquet.ByteArray:
		FillRandomByteArray(0, arr, heap)
	case []parquet.FixedLenByteArray:
		FillRandomFixedByteArray(0, arr, heap, 12)
	}
}

// RandomByteArray populates out with random ByteArray values with lengths between minlen and maxlen
// using heap as the actual memory storage used for the bytes generated. Each element of
// out will be some slice of the bytes in heap, and as such heap must outlive the byte array slices.
func RandomByteArray(seed uint64, out []parquet.ByteArray, heap *memory.Buffer, minlen, maxlen int) {
	heap.Resize(len(out) * (maxlen + arrow.Uint32SizeBytes))

	buf := heap.Bytes()
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		length := r.Intn(maxlen-minlen+1) + minlen
		r.Read(buf[:length])
		out[idx] = buf[:length]

		buf = buf[length:]
	}
}

// RandomDecimals generates n random decimal values with precision determining the byte width
// for the values and seed as the random generator seed to allow consistency for testing. The
// resulting values will be either 32 bytes or 16 bytes each depending on the precision.
func RandomDecimals(n int64, seed uint64, precision int32) []byte {
	r := rand.New(rand.NewSource(seed))
	nreqBytes := pqarrow.DecimalSize(precision)
	byteWidth := 32
	if precision <= 38 {
		byteWidth = 16
	}

	out := make([]byte, int(int64(byteWidth)*n))
	for i := int64(0); i < n; i++ {
		start := int(i) * byteWidth
		r.Read(out[start : start+int(nreqBytes)])
		// sign extend if the sign bit is set for the last generated byte
		// 0b10000000 == 0x80 == 128
		if out[start+int(nreqBytes)-1]&byte(0x80) != 0 {
			memory.Set(out[start+int(nreqBytes):start+byteWidth], 0xFF)
		}

		// byte swap for big endian
		if endian.IsBigEndian {
			for j := 0; j+8 <= byteWidth; j += 8 {
				v := binary.LittleEndian.Uint64(out[start+j : start+j+8])
				binary.BigEndian.PutUint64(out[start+j:start+j+8], v)
			}
		}
	}
	return out
}

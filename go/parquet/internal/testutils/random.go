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

package testutils

import (
	"math"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/parquet"

	// "github.factset.com/mtopol/parquet-go/pqarrow"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

type RandomArrayGenerator struct {
	seed     uint64
	extra    uint64
	src      rand.Source
	seedRand *rand.Rand
}

func NewRandomArrayGenerator(seed uint64) RandomArrayGenerator {
	src := rand.NewSource(seed)
	return RandomArrayGenerator{seed, 0, src, rand.New(src)}
}

func (r *RandomArrayGenerator) GenerateBitmap(buffer []byte, n int64, prob float64) int64 {
	count := int64(0)
	r.extra++

	dist := distuv.Bernoulli{P: prob, Src: rand.NewSource(r.seed + r.extra)}
	for i := int(0); int64(i) < n; i++ {
		if dist.Rand() != float64(0.0) {
			bitutil.SetBit(buffer, i)
		} else {
			count++
		}
	}

	return count
}

func (r *RandomArrayGenerator) ByteArray(size int64, minLen, maxLen int32, nullProb float64) array.Interface {
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

func (r *RandomArrayGenerator) Uint8(size int64, min, max uint8, prob float64) array.Interface {
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
		// out[i] = dist.Int31n(max-min+1) + min
	}
	return array.NewFloat64Data(array.NewData(arrow.PrimitiveTypes.Float64, int(size), buffers, nil, int(nullCount), 0))
}

func FillRandomInt8(seed uint64, min, max int8, out []int8) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int8(r.Intn(int(max-min+1))) + min
	}
}

func FillRandomUint8(seed uint64, min, max uint8, out []uint8) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint8(r.Intn(int(max-min+1))) + min
	}
}

func FillRandomInt16(seed uint64, min, max int16, out []int16) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int16(r.Intn(int(max-min+1))) + min
	}
}

func FillRandomUint16(seed uint64, min, max uint16, out []uint16) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint16(r.Intn(int(max-min+1))) + min
	}
}

func FillRandomInt32(seed uint64, out []int32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int32(r.Uint32())
	}
}

func FillRandomInt32Max(seed uint64, max int32, out []int32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Int31n(max)
	}
}

func FillRandomUint32Max(seed uint64, max uint32, out []uint32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = uint32(r.Uint64n(uint64(max)))
	}
}

func FillRandomInt64Max(seed uint64, max int64, out []int64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Int63n(max)
	}
}

func FillRandomUint32(seed uint64, out []uint32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint32()
	}
}

func FillRandomUint64(seed uint64, out []uint64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint64()
	}
}

func FillRandomUint64Max(seed uint64, max uint64, out []uint64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Uint64n(max)
	}
}

func FillRandomInt64(seed uint64, out []int64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = int64(r.Uint64())
	}
}

func FillRandomInt96(seed uint64, out []parquet.Int96) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		*(*int32)(unsafe.Pointer(&out[idx][0])) = int32(r.Uint32())
		*(*int32)(unsafe.Pointer(&out[idx][4])) = int32(r.Uint32())
		*(*int32)(unsafe.Pointer(&out[idx][8])) = int32(r.Uint32())
	}
}

func randFloat32(r *rand.Rand) float32 {
	for {
		f := math.Float32frombits(r.Uint32())
		if !math.IsNaN(float64(f)) {
			return f
		}
	}
}

func randFloat64(r *rand.Rand) float64 {
	for {
		f := math.Float64frombits(r.Uint64())
		if !math.IsNaN(f) {
			return f
		}
	}
}

func FillRandomFloat32(seed uint64, out []float32) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = randFloat32(r)
	}
}

func FillRandomFloat64(seed uint64, out []float64) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = randFloat64(r)
	}
}

func randomByteArray(seed uint64, out []parquet.ByteArray, heap *memory.Buffer, minlen, maxlen int) {
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

func FillRandomByteArray(seed uint64, out []parquet.ByteArray, heap *memory.Buffer) {
	const (
		maxByteArrayLen = 12
		minByteArrayLen = 2
	)
	randomByteArray(seed, out, heap, minByteArrayLen, maxByteArrayLen)
}

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

func FillRandomBooleans(p float64, seed uint64, out []bool) {
	dist := distuv.Bernoulli{P: p, Src: rand.NewSource(seed)}
	for idx := range out {
		out[idx] = dist.Rand() != float64(0.0)
	}
}

func fillRandomIsValid(seed uint64, pctNull float64, out []bool) {
	r := rand.New(rand.NewSource(seed))
	for idx := range out {
		out[idx] = r.Float64() > pctNull
	}
}

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

// func RandomDecimals(n int64, seed uint64, precision int32) []byte {
// 	r := rand.New(rand.NewSource(seed))
// 	nreqBytes := pqarrow.DecimalSize(precision)
// 	byteWidth := 32
// 	if precision <= 38 {
// 		byteWidth = 16
// 	}

// 	out := make([]byte, int(int64(byteWidth)*n))
// 	for i := int64(0); i < n; i++ {
// 		start := int(i) * byteWidth
// 		r.Read(out[start : start+int(nreqBytes)])
// 		// sign extend if the sign bit is set for the last generated byte
// 		// 0b10000000 == 0x80 == 128
// 		if out[start+int(nreqBytes)-1]&byte(0x80) != 0 {
// 			memory.Set(out[start+int(nreqBytes):start+byteWidth], 0xFF)
// 		}
// 	}
// 	return out
// }

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

package utils_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

func TestBitWriter(t *testing.T) {
	buf := make([]byte, 8)
	bw := utils.NewBitWriter(utils.NewWriterAtBuffer(buf))

	for i := 0; i < 8; i++ {
		assert.Nil(t, bw.WriteValue(uint64(i%2), 1))
	}
	bw.Flush(false)

	assert.Equal(t, byte(0xAA), buf[0])

	for i := 0; i < 8; i++ {
		switch i {
		case 0, 1, 4, 5:
			assert.Nil(t, bw.WriteValue(0, 1))
		default:
			assert.Nil(t, bw.WriteValue(1, 1))
		}
	}
	bw.Flush(false)

	assert.Equal(t, byte(0xAA), buf[0])
	assert.Equal(t, byte(0xCC), buf[1])
}

func TestBitReader(t *testing.T) {
	buf := []byte{0xAA, 0xCC} // 0b10101010 0b11001100

	reader := utils.NewBitReader(bytes.NewReader(buf))
	for i := 0; i < 8; i++ {
		val, ok := reader.GetValue(1)
		assert.True(t, ok)
		assert.Equalf(t, (i%2) != 0, val != 0, "val: %d, i: %d", val, i)
	}

	for i := 0; i < 8; i++ {
		val, ok := reader.GetValue(1)
		assert.True(t, ok)
		switch i {
		case 0, 1, 4, 5:
			assert.EqualValues(t, 0, val)
		default:
			assert.EqualValues(t, 1, val)
		}
	}
}

func TestBitArrayVals(t *testing.T) {
	tests := []struct {
		name  string
		nvals func(uint) int
	}{
		{"1 value", func(uint) int { return 1 }},
		{"2 values", func(uint) int { return 2 }},
		{"larger", func(w uint) int {
			if w < 12 {
				return 1 << w
			}
			return 4096
		}},
		{"1024 values", func(uint) int { return 1024 }},
	}

	for width := uint(1); width < 32; width++ {
		t.Run(fmt.Sprintf("BitWriter Width %d", width), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var (
						nvals        = tt.nvals(width)
						mod   uint64 = 1
					)
					l := bitutil.BytesForBits(int64(int(width) * nvals))
					assert.Greater(t, l, int64(0))

					if width != 64 {
						mod = uint64(1) << width
					}

					buf := make([]byte, l)
					bw := utils.NewBitWriter(utils.NewWriterAtBuffer(buf))
					for i := 0; i < nvals; i++ {
						assert.Nil(t, bw.WriteValue(uint64(i)%mod, width))
					}
					bw.Flush(false)
					assert.Equal(t, l, int64(bw.Written()))

					br := utils.NewBitReader(bytes.NewReader(buf))
					for i := 0; i < nvals; i++ {
						val, ok := br.GetValue(int(width))
						assert.True(t, ok)
						assert.Equal(t, uint64(i)%mod, val)
					}
				})
			}
		})
	}
}

func TestMixedValues(t *testing.T) {
	const buflen = 1024
	buf := make([]byte, buflen)
	parity := true

	bw := utils.NewBitWriter(utils.NewWriterAtBuffer(buf))
	for i := 0; i < buflen; i++ {
		if i%2 == 0 {
			v := uint64(1)
			if !parity {
				v = 0
			}
			assert.Nil(t, bw.WriteValue(v, 1))
			parity = !parity
		} else {
			assert.Nil(t, bw.WriteValue(uint64(i), 10))
		}
	}
	bw.Flush(false)

	parity = true
	br := utils.NewBitReader(bytes.NewReader(buf))
	for i := 0; i < buflen; i++ {
		if i%2 == 0 {
			val, ok := br.GetValue(1)
			assert.True(t, ok)
			exp := uint64(1)
			if !parity {
				exp = 0
			}
			assert.Equal(t, exp, val)
			parity = !parity
		} else {
			val, ok := br.GetValue(10)
			assert.True(t, ok)
			assert.Equal(t, uint64(i), val)
		}
	}
}

func TestZigZag(t *testing.T) {
	testvals := []struct {
		val int64
		exp [10]byte
	}{
		{0, [...]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{1, [...]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{1234, [...]byte{164, 19, 0, 0, 0, 0, 0, 0, 0, 0}},
		{-1, [...]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{-1234, [...]byte{163, 19, 0, 0, 0, 0, 0, 0, 0, 0}},
		{math.MaxInt32, [...]byte{254, 255, 255, 255, 15, 0, 0, 0, 0, 0}},
		{-math.MaxInt32, [...]byte{253, 255, 255, 255, 15, 0, 0, 0, 0, 0}},
		{math.MinInt32, [...]byte{255, 255, 255, 255, 15, 0, 0, 0, 0, 0}},
		{math.MaxInt64, [...]byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
		{-math.MaxInt64, [...]byte{253, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
		{math.MinInt64, [...]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
	}

	for _, v := range testvals {
		t.Run(strconv.Itoa(int(v.val)), func(t *testing.T) {
			var buf [binary.MaxVarintLen64]byte
			wrtr := utils.NewBitWriter(utils.NewWriterAtBuffer(buf[:]))
			assert.True(t, wrtr.WriteZigZagVlqInt(v.val))
			wrtr.Flush(false)

			assert.Equal(t, v.exp, buf)

			rdr := utils.NewBitReader(bytes.NewReader(buf[:]))
			val, ok := rdr.GetZigZagVlqInt()
			assert.True(t, ok)
			assert.EqualValues(t, v.val, val)
		})
	}
}

const buflen = 1024

type RLETestSuite struct {
	suite.Suite

	expectedBuf []byte
	values      []uint64
}

type RLERandomSuite struct {
	suite.Suite
}

func TestRLE(t *testing.T) {
	suite.Run(t, new(RLETestSuite))
}

func TestRleRandom(t *testing.T) {
	suite.Run(t, new(RLERandomSuite))
}

func (r *RLETestSuite) ValidateRle(vals []uint64, width int, expected []byte, explen int) {
	const buflen = 64 * 1024
	buf := make([]byte, buflen)

	r.Run("test encode", func() {
		r.LessOrEqual(explen, buflen)

		enc := utils.NewRleEncoder(utils.NewWriterAtBuffer(buf), width)
		for _, val := range vals {
			r.NoError(enc.Put(val))
		}
		encoded := enc.Flush()
		if explen != -1 {
			r.Equal(explen, encoded)
		}

		if expected != nil {
			r.Equal(expected, buf[:encoded])
		}
	})

	r.Run("decode read", func() {
		dec := utils.NewRleDecoder(bytes.NewReader(buf), width)
		for _, val := range vals {
			v, ok := dec.GetValue()
			r.True(ok)
			r.Equal(val, v)
		}
	})

	r.Run("decode batch read", func() {
		dec := utils.NewRleDecoder(bytes.NewReader(buf), width)
		check := make([]uint64, len(vals))
		r.Equal(len(vals), dec.GetBatch(check))
		r.Equal(vals, check)
	})
}

func (r *RLETestSuite) SetupTest() {
	r.expectedBuf = make([]byte, 0, buflen)
	r.values = make([]uint64, 100)
}

func (r *RLETestSuite) Test50Zeros50Ones() {
	for i := 0; i < 50; i++ {
		r.values[i] = 0
	}
	for i := 50; i < 100; i++ {
		r.values[i] = 1
	}

	r.expectedBuf = append(r.expectedBuf, []byte{50 << 1, 0, 50 << 1, 1}...)
	for width := 1; width <= 8; width++ {
		r.Run(fmt.Sprintf("bitwidth: %d", width), func() {
			r.ValidateRle(r.values, width, r.expectedBuf, 4)
		})
	}

	for width := 9; width <= 32; width++ {
		r.Run(fmt.Sprintf("bitwidth: %d", width), func() {
			r.ValidateRle(r.values, width, nil, int(2*(1+bitutil.BytesForBits(int64(width)))))
		})
	}
}

func (r *RLETestSuite) Test100ZerosOnesAlternating() {
	for idx := range r.values {
		r.values[idx] = uint64(idx % 2)
	}

	ngroups := bitutil.BytesForBits(100)
	r.expectedBuf = r.expectedBuf[:ngroups+1]
	r.expectedBuf[0] = byte(ngroups<<1) | 1
	for i := 1; i <= 100/8; i++ {
		r.expectedBuf[i] = 0xAA
	}
	r.expectedBuf[100/8+1] = 0x0A

	r.Run("width: 1", func() {
		r.ValidateRle(r.values, 1, r.expectedBuf, int(1+ngroups))
	})
	for width := 2; width < 32; width++ {
		r.Run(fmt.Sprintf("width: %d", width), func() {
			nvalues := bitutil.BytesForBits(100) * 8
			r.ValidateRle(r.values, width, nil, int(1+bitutil.BytesForBits(int64(width)*nvalues)))
		})
	}
}

func (r *RLETestSuite) Test16BitValues() {
	// confirm encoded values are little endian
	r.values = r.values[:28]
	for i := 0; i < 16; i++ {
		r.values[i] = 0x55aa
	}
	for i := 16; i < 28; i++ {
		r.values[i] = 0xaa55
	}

	r.expectedBuf = append(r.expectedBuf, []byte{
		16 << 1, 0xaa, 0x55, 12 << 1, 0x55, 0xaa,
	}...)

	r.ValidateRle(r.values, 16, r.expectedBuf, 6)
}

func (r *RLETestSuite) Test32BitValues() {
	// confirm encoded values are little endian
	r.values = r.values[:28]
	for i := 0; i < 16; i++ {
		r.values[i] = 0x555aaaa5
	}
	for i := 16; i < 28; i++ {
		r.values[i] = 0x5aaaa555
	}

	r.expectedBuf = append(r.expectedBuf, []byte{
		16 << 1, 0xa5, 0xaa, 0x5a, 0x55,
		12 << 1, 0x55, 0xa5, 0xaa, 0x5a,
	}...)

	r.ValidateRle(r.values, 32, r.expectedBuf, 10)
}

func (r *RLETestSuite) TestRleValues() {
	tests := []struct {
		name  string
		nvals int
		val   int
	}{
		{"1", 1, -1},
		{"1024", 1024, -1},
		{"1024 0", 1024, 0},
		{"1024 1", 1024, 1},
	}

	for width := 1; width <= 32; width++ {
		r.Run(fmt.Sprintf("width %d", width), func() {
			for _, tt := range tests {
				r.Run(tt.name, func() {

					var mod uint64 = 1
					if width != 64 {
						mod = uint64(1) << width
					}

					r.values = r.values[:0]

					for v := 0; v < tt.nvals; v++ {
						if tt.val != -1 {
							r.values = append(r.values, uint64(tt.val))
						} else {
							r.values = append(r.values, uint64(v)%mod)
						}
					}
					r.ValidateRle(r.values, width, nil, -1)
				})
			}
		})
	}
}

// Test that writes out a repeated group and then a literal group
// but flush before finishing
func (r *RLETestSuite) TestBitRleFlush() {
	vals := make([]uint64, 0, 16)
	for i := 0; i < 16; i++ {
		vals = append(vals, 1)
	}
	vals = append(vals, 0)
	r.ValidateRle(vals, 1, nil, -1)
	vals = append(vals, 1)
	r.ValidateRle(vals, 1, nil, -1)
	vals = append(vals, 1)
	r.ValidateRle(vals, 1, nil, -1)
	vals = append(vals, 1)
	r.ValidateRle(vals, 1, nil, -1)
}

func (r *RLETestSuite) TestRepeatedPattern() {
	r.values = r.values[:0]
	const minrun = 1
	const maxrun = 32

	for i := minrun; i <= maxrun; i++ {
		v := i % 2
		for j := 0; j < i; j++ {
			r.values = append(r.values, uint64(v))
		}
	}

	// and go back down again
	for i := maxrun; i >= minrun; i-- {
		v := i % 2
		for j := 0; j < i; j++ {
			r.values = append(r.values, uint64(v))
		}
	}

	r.ValidateRle(r.values, 1, nil, -1)
}

func TestBitWidthZeroRepeated(t *testing.T) {
	buf := make([]byte, 1)
	const nvals = 15
	buf[0] = nvals << 1 // repeated indicator byte
	dec := utils.NewRleDecoder(bytes.NewReader(buf), 0)
	for i := 0; i < nvals; i++ {
		val, ok := dec.GetValue()
		assert.True(t, ok)
		assert.Zero(t, val)
	}
	_, ok := dec.GetValue()
	assert.False(t, ok)
}

func TestBitWidthZeroLiteral(t *testing.T) {
	const ngroups = 4
	buf := []byte{4<<1 | 1}
	dec := utils.NewRleDecoder(bytes.NewReader(buf), 0)
	const nvals = ngroups * 8
	for i := 0; i < nvals; i++ {
		val, ok := dec.GetValue()
		assert.True(t, ok)
		assert.Zero(t, val)
	}
	_, ok := dec.GetValue()
	assert.False(t, ok)
}

func (r *RLERandomSuite) checkRoundTrip(vals []uint64, width int) bool {
	const buflen = 64 * 1024
	buf := make([]byte, buflen)
	var encoded int

	res := r.Run("encode values", func() {
		enc := utils.NewRleEncoder(utils.NewWriterAtBuffer(buf), width)
		for idx, val := range vals {
			r.Require().NoErrorf(enc.Put(val), "encoding idx: %d", idx)
		}
		encoded = enc.Flush()
	})

	res = res && r.Run("decode individual", func() {
		dec := utils.NewRleDecoder(bytes.NewReader(buf[:encoded]), width)
		for idx, val := range vals {
			out, ok := dec.GetValue()
			r.True(ok)
			r.Require().Equalf(out, val, "mismatch idx: %d", idx)
		}
	})

	res = res && r.Run("batch decode", func() {
		dec := utils.NewRleDecoder(bytes.NewReader(buf[:encoded]), width)
		read := make([]uint64, len(vals))
		r.Require().Equal(len(vals), dec.GetBatch(read))
		r.Equal(vals, read)
	})

	return res
}

func (r *RLERandomSuite) checkRoundTripSpaced(vals arrow.Array, width int) {
	nvalues := vals.Len()
	bufsize := utils.MaxBufferSize(width, nvalues)

	buffer := make([]byte, bufsize)
	encoder := utils.NewRleEncoder(utils.NewWriterAtBuffer(buffer), width)

	switch v := vals.(type) {
	case *array.Int32:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				r.Require().NoError(encoder.Put(uint64(v.Value(i))))
			}
		}
	}

	encodedSize := encoder.Flush()

	// verify batch read
	decoder := utils.NewRleDecoder(bytes.NewReader(buffer[:encodedSize]), width)
	valuesRead := make([]uint64, nvalues)
	val, err := decoder.GetBatchSpaced(valuesRead, vals.NullN(), vals.NullBitmapBytes(), int64(vals.Data().Offset()))
	r.NoError(err)
	r.EqualValues(nvalues, val)

	switch v := vals.(type) {
	case *array.Int32:
		for i := 0; i < nvalues; i++ {
			if vals.IsValid(i) {
				r.EqualValues(v.Value(i), valuesRead[i])
			}
		}
	}
}

func (r *RLERandomSuite) TestRandomSequences() {
	const niters = 50
	const ngroups = 1000
	const maxgroup = 16

	values := make([]uint64, ngroups+maxgroup)
	seed := rand.Uint64() ^ (rand.Uint64() << 32)
	gen := rand.New(rand.NewSource(seed))

	for itr := 0; itr < niters; itr++ {
		parity := false
		values = values[:0]

		for i := 0; i < ngroups; i++ {
			groupsize := gen.Intn(19) + 1
			if groupsize > maxgroup {
				groupsize = 1
			}

			v := uint64(0)
			if parity {
				v = 1
			}
			for j := 0; j < groupsize; j++ {
				values = append(values, v)
			}
			parity = !parity
		}
		r.Require().Truef(r.checkRoundTrip(values, bits.Len(uint(len(values)))), "failing seed: %d", seed)
	}
}

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

func (r *RandomArrayGenerator) generateBitmap(buffer []byte, n int64, prob float64) int64 {
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

func (r *RandomArrayGenerator) Int32(size int64, min, max int32, prob float64) arrow.Array {
	buffers := make([]*memory.Buffer, 2)
	nullCount := int64(0)

	buffers[0] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[0].Resize(int(bitutil.BytesForBits(size)))
	nullCount = r.generateBitmap(buffers[0].Bytes(), size, prob)

	buffers[1] = memory.NewResizableBuffer(memory.DefaultAllocator)
	buffers[1].Resize(int(size * int64(arrow.Int32SizeBytes)))

	r.extra++
	dist := rand.New(rand.NewSource(r.seed + r.extra))
	out := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
	for i := int64(0); i < size; i++ {
		out[i] = int32(dist.Int31n(max-min+1)) + min
	}

	return array.NewInt32Data(array.NewData(arrow.PrimitiveTypes.Int32, int(size), buffers, nil, int(nullCount), 0))
}

func (r *RLERandomSuite) TestGetBatchSpaced() {
	seed := uint64(1337)

	rng := NewRandomArrayGenerator(seed)

	tests := []struct {
		name     string
		max      int32
		size     int64
		nullProb float64
		bitWidth int
	}{
		{"all ones 0.01 nullprob width 1", 1, 100000, 0.01, 1},
		{"all ones 0.1 nullprob width 1", 1, 100000, 0.1, 1},
		{"all ones 0.5 nullprob width 1", 1, 100000, 0.5, 1},
		{"max 4 0.05 nullprob width 3", 4, 100000, 0.05, 3},
		{"max 100 0.05 nullprob width 7", 100, 100000, 0.05, 7},
	}

	for _, tt := range tests {
		r.Run(tt.name, func() {
			arr := rng.Int32(tt.size, 0, tt.max, tt.nullProb)
			r.checkRoundTripSpaced(arr, tt.bitWidth)
			r.checkRoundTripSpaced(array.NewSlice(arr, 1, int64(arr.Len())), tt.bitWidth)
		})
	}
}

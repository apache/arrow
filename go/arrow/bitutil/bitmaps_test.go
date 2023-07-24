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

package bitutil_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func bitmapFromSlice(vals []int, bitOffset int) []byte {
	out := make([]byte, int(bitutil.BytesForBits(int64(len(vals)+bitOffset))))
	writer := bitutil.NewBitmapWriter(out, bitOffset, len(vals))
	for _, val := range vals {
		if val == 1 {
			writer.Set()
		} else {
			writer.Clear()
		}
		writer.Next()
	}
	writer.Finish()

	return out
}

func assertReaderVals(t *testing.T, reader *bitutil.BitmapReader, vals []bool) {
	for _, v := range vals {
		if v {
			assert.True(t, reader.Set())
			assert.False(t, reader.NotSet())
		} else {
			assert.True(t, reader.NotSet())
			assert.False(t, reader.Set())
		}
		reader.Next()
	}
}

func TestNormalOperation(t *testing.T) {
	for _, offset := range []int{0, 1, 3, 5, 7, 8, 12, 13, 21, 38, 75, 120} {
		buf := bitmapFromSlice([]int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}, offset)

		reader := bitutil.NewBitmapReader(buf, offset, 14)
		assertReaderVals(t, reader, []bool{false, true, true, true, false, false, false, true, false, true, false, true, false, true})
	}
}

func TestDoesNotReadOutOfBounds(t *testing.T) {
	var bitmap [16]byte
	const length = 128

	reader := bitutil.NewBitmapReader(bitmap[:], 0, length)
	assert.EqualValues(t, length, reader.Len())
	assert.NotPanics(t, func() {
		for i := 0; i < length; i++ {
			assert.True(t, reader.NotSet())
			reader.Next()
		}
	})
	assert.EqualValues(t, length, reader.Pos())

	reader = bitutil.NewBitmapReader(bitmap[:], 5, length-5)
	assert.EqualValues(t, length-5, reader.Len())
	assert.NotPanics(t, func() {
		for i := 0; i < length-5; i++ {
			assert.True(t, reader.NotSet())
			reader.Next()
		}
	})
	assert.EqualValues(t, length-5, reader.Pos())

	assert.NotPanics(t, func() {
		reader = bitutil.NewBitmapReader(nil, 0, 0)
	})
}

func writeToWriter(vals []int, wr *bitutil.BitmapWriter) {
	for _, v := range vals {
		if v != 0 {
			wr.Set()
		} else {
			wr.Clear()
		}
		wr.Next()
	}
	wr.Finish()
}

func TestBitmapWriter(t *testing.T) {
	for _, fillByte := range []byte{0x00, 0xFF} {
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			wr := bitutil.NewBitmapWriter(bitmap, 0, 12)
			writeToWriter([]int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1}, wr)
			// {0b00110110, 0b....1010, ........, ........}
			assert.Equal(t, []byte{0x36, (0x0A | (fillByte & 0xF0)), fillByte, fillByte}, bitmap)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			wr := bitutil.NewBitmapWriter(bitmap, 0, 12)
			wr.AppendBools([]bool{false, true, true, false, true, true, false, false, false, true, false, true})
			assert.Equal(t, []byte{0x36, (0x0A | (fillByte & 0xF0)), fillByte, fillByte}, bitmap)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			wr := bitutil.NewBitmapWriter(bitmap, 3, 12)
			writeToWriter([]int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1}, wr)
			// {0b10110..., 0b.1010001, ........, ........}
			assert.Equal(t, []byte{0xb0 | (fillByte & 0x07), 0x51 | (fillByte & 0x80), fillByte, fillByte}, bitmap)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			wr := bitutil.NewBitmapWriter(bitmap, 3, 12)
			wr.AppendBools([]bool{false, true, true, false})
			wr.AppendBools([]bool{true, true, false, false})
			wr.AppendBools([]bool{false, true, false, true})
			assert.Equal(t, []byte{0xb0 | (fillByte & 0x07), 0x51 | (fillByte & 0x80), fillByte, fillByte}, bitmap)
		}
		{
			bitmap := []byte{fillByte, fillByte, fillByte, fillByte}
			wr := bitutil.NewBitmapWriter(bitmap, 20, 12)
			writeToWriter([]int{0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1}, wr)
			// {........, ........, 0b0110...., 0b10100011}
			assert.Equal(t, []byte{fillByte, fillByte, 0x60 | (fillByte & 0x0f), 0xa3}, bitmap)
		}
	}
}

func TestBitmapReader(t *testing.T) {
	assertReaderVals := func(vals []int, rdr *bitutil.BitmapReader) {
		for _, v := range vals {
			if v != 0 {
				assert.True(t, rdr.Set())
				assert.False(t, rdr.NotSet())
			} else {
				assert.False(t, rdr.Set())
				assert.True(t, rdr.NotSet())
			}
			rdr.Next()
		}
	}

	vals := []int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}

	for _, offset := range []int{0, 1, 3, 5, 7, 8, 12, 13, 21, 38, 75, 120} {
		bm := make([]byte, bitutil.BytesForBits(int64(len(vals)+offset)))
		wr := bitutil.NewBitmapWriter(bm, offset, len(vals))
		writeToWriter(vals, wr)

		rdr := bitutil.NewBitmapReader(bm, offset, 14)
		assertReaderVals(vals, rdr)
	}
}

func TestCopyBitmap(t *testing.T) {
	const bufsize = 1000
	lengths := []int{bufsize*8 - 4, bufsize * 8}
	offsets := []int{0, 12, 16, 32, 37, 63, 64, 128}

	buffer := make([]byte, bufsize)

	// random bytes
	r := rand.New(rand.NewSource(0))
	r.Read(buffer)

	// add 16 byte padding
	otherBuffer := make([]byte, bufsize+32)
	r.Read(otherBuffer)

	for _, nbits := range lengths {
		for _, offset := range offsets {
			for _, destOffset := range offsets {
				t.Run(fmt.Sprintf("bits %d off %d dst %d", nbits, offset, destOffset), func(t *testing.T) {
					copyLen := nbits - offset

					bmCopy := make([]byte, len(otherBuffer))
					copy(bmCopy, otherBuffer)

					bitutil.CopyBitmap(buffer, offset, copyLen, bmCopy, destOffset)

					for i := 0; i < int(destOffset); i++ {
						assert.Equalf(t, bitutil.BitIsSet(otherBuffer, i), bitutil.BitIsSet(bmCopy, i), "bit index: %d", i)
					}
					for i := 0; i < int(copyLen); i++ {
						assert.Equalf(t, bitutil.BitIsSet(buffer, i+int(offset)), bitutil.BitIsSet(bmCopy, i+int(destOffset)), "bit index: %d", i)
					}
					for i := int(destOffset + copyLen); i < len(otherBuffer); i++ {
						assert.Equalf(t, bitutil.BitIsSet(otherBuffer, i), bitutil.BitIsSet(bmCopy, i), "bit index: %d", i)
					}
				})
			}
		}
	}
}

func benchmarkCopyBitmapN(b *testing.B, offsetSrc, offsetDest, n int) {
	nbits := n * 8
	// random bytes
	r := rand.New(rand.NewSource(0))
	src := make([]byte, n)
	r.Read(src)

	length := nbits - offsetSrc

	dest := make([]byte, bitutil.BytesForBits(int64(length+offsetDest)))

	b.ResetTimer()
	b.SetBytes(int64(n))
	for i := 0; i < b.N; i++ {
		bitutil.CopyBitmap(src, offsetSrc, length, dest, offsetDest)
	}
}

// Fast path which is just a memcopy
func BenchmarkCopyBitmapWithoutOffset(b *testing.B) {
	for _, sz := range []int{32, 128, 1000, 1024} {
		b.Run(strconv.Itoa(sz), func(b *testing.B) {
			benchmarkCopyBitmapN(b, 0, 0, sz)
		})
	}
}

// slow path where the source buffer is not byte aligned
func BenchmarkCopyBitmapWithOffset(b *testing.B) {
	for _, sz := range []int{32, 128, 1000, 1024} {
		b.Run(strconv.Itoa(sz), func(b *testing.B) {
			benchmarkCopyBitmapN(b, 4, 0, sz)
		})
	}
}

// slow path where both source and dest are not byte aligned
func BenchmarkCopyBitmapWithOffsetBoth(b *testing.B) {
	for _, sz := range []int{32, 128, 1000, 1024} {
		b.Run(strconv.Itoa(sz), func(b *testing.B) {
			benchmarkCopyBitmapN(b, 3, 7, sz)
		})
	}
}

const bufferSize = 1024 * 8

// a naive bitmap reader for a baseline

type NaiveBitmapReader struct {
	bitmap []byte
	pos    int
}

func (n *NaiveBitmapReader) IsSet() bool    { return bitutil.BitIsSet(n.bitmap, n.pos) }
func (n *NaiveBitmapReader) IsNotSet() bool { return !n.IsSet() }
func (n *NaiveBitmapReader) Next()          { n.pos++ }

// naive bitmap writer for a baseline

type NaiveBitmapWriter struct {
	bitmap []byte
	pos    int
}

func (n *NaiveBitmapWriter) Set() {
	byteOffset := n.pos / 8
	bitOffset := n.pos % 8
	bitSetMask := uint8(1 << bitOffset)
	n.bitmap[byteOffset] |= bitSetMask
}

func (n *NaiveBitmapWriter) Clear() {
	byteOffset := n.pos / 8
	bitOffset := n.pos % 8
	bitClearMask := uint8(0xFF ^ (1 << bitOffset))
	n.bitmap[byteOffset] &= bitClearMask
}

func (n *NaiveBitmapWriter) Next()   { n.pos++ }
func (n *NaiveBitmapWriter) Finish() {}

func randomBuffer(nbytes int64) []byte {
	buf := make([]byte, nbytes)
	r := rand.New(rand.NewSource(0))
	r.Read(buf)
	return buf
}

func BenchmarkBitmapReader(b *testing.B) {
	buf := randomBuffer(bufferSize)
	nbits := bufferSize * 8

	b.Run("naive baseline", func(b *testing.B) {
		b.SetBytes(2 * bufferSize)
		for i := 0; i < b.N; i++ {
			{
				total := 0
				rdr := NaiveBitmapReader{buf, 0}
				for j := 0; j < nbits; j++ {
					if rdr.IsSet() {
						total++
					}
					rdr.Next()
				}
			}
			{
				total := 0
				rdr := NaiveBitmapReader{buf, 0}
				for j := 0; j < nbits; j++ {
					if rdr.IsSet() {
						total++
					}
					rdr.Next()
				}
			}
		}
	})
	b.Run("bitmap reader", func(b *testing.B) {
		b.SetBytes(2 * bufferSize)
		for i := 0; i < b.N; i++ {
			{
				total := 0
				rdr := bitutil.NewBitmapReader(buf, 0, nbits)
				for j := 0; j < nbits; j++ {
					if rdr.Set() {
						total++
					}
					rdr.Next()
				}
			}
			{
				total := 0
				rdr := bitutil.NewBitmapReader(buf, 0, nbits)
				for j := 0; j < nbits; j++ {
					if rdr.Set() {
						total++
					}
					rdr.Next()
				}
			}
		}
	})
}

type (
	noAllocFn func(left, right []byte, lOffset, rOffset int64, out []byte, outOffset, length int64)
	allocFn   func(mem memory.Allocator, left, right []byte, lOffset, rOffset int64, length, outOffset int64) *memory.Buffer
	bitmapOp  struct {
		noAlloc noAllocFn
		alloc   allocFn
	}
)

type BitmapOpSuite struct {
	suite.Suite
}

func (s *BitmapOpSuite) testAligned(op bitmapOp, leftBits, rightBits []int, resultBits []bool) {
	var (
		left, right []byte
		out         *memory.Buffer
		length      int64
	)
	for _, lOffset := range []int64{0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120, 65536} {
		s.Run(fmt.Sprintf("left offset %d", lOffset), func() {
			left = bitmapFromSlice(leftBits, int(lOffset))
			length = int64(len(leftBits))
			for _, rOffset := range []int64{lOffset, lOffset + 8, lOffset + 40} {
				s.Run(fmt.Sprintf("right offset %d", rOffset), func() {
					right = bitmapFromSlice(rightBits, int(rOffset))
					for _, outOffset := range []int64{lOffset, lOffset + 16, lOffset + 24} {
						s.Run(fmt.Sprintf("out offset %d", outOffset), func() {
							s.Run("zero-length", func() {
								out = op.alloc(memory.DefaultAllocator, left, right, lOffset, rOffset, 0, outOffset)
								s.EqualValues(bitutil.BytesForBits(outOffset), out.Len())
								expected := make([]byte, out.Len())
								if out.Len() > 0 {
									s.Equal(expected, out.Bytes())
								} else {
									s.Nil(out.Bytes())
								}

								memory.Set(out.Bytes(), 0xFF)
								op.noAlloc(left, right, lOffset, rOffset, out.Bytes(), outOffset, 0)
								if out.Len() > 0 {
									memory.Set(expected, 0xFF)
									s.Equal(expected, out.Bytes())
								} else {
									s.Nil(out.Bytes())
								}
								out.Release()
							})

							out = op.alloc(memory.DefaultAllocator, left, right, lOffset, rOffset, length, outOffset)
							defer out.Release()
							rdr := bitutil.NewBitmapReader(out.Bytes(), int(outOffset), int(length))
							assertReaderVals(s.T(), rdr, resultBits)

							memory.Set(out.Bytes(), 0x00)
							op.noAlloc(left, right, lOffset, rOffset, out.Bytes(), outOffset, length)
							rdr = bitutil.NewBitmapReader(out.Bytes(), int(outOffset), int(length))
							assertReaderVals(s.T(), rdr, resultBits)
						})
					}
				})
			}
		})
	}
}

func (s *BitmapOpSuite) testUnaligned(op bitmapOp, leftBits, rightBits []int, resultBits []bool) {
	var (
		left, right []byte
		out         *memory.Buffer
		length      int64
		offsets     = []int64{0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120, 65536}
	)

	for _, lOffset := range offsets {
		s.Run(fmt.Sprintf("left offset %d", lOffset), func() {
			left = bitmapFromSlice(leftBits, int(lOffset))
			length = int64(len(leftBits))
			for _, rOffset := range offsets {
				s.Run(fmt.Sprintf("right offset %d", rOffset), func() {
					right = bitmapFromSlice(rightBits, int(rOffset))
					for _, outOffset := range offsets {
						s.Run(fmt.Sprintf("out offset %d", outOffset), func() {
							s.Run("zero-length", func() {
								out = op.alloc(memory.DefaultAllocator, left, right, lOffset, rOffset, 0, outOffset)
								s.EqualValues(bitutil.BytesForBits(outOffset), out.Len())
								expected := make([]byte, out.Len())
								if out.Len() > 0 {
									s.Equal(expected, out.Bytes())
								} else {
									s.Nil(out.Bytes())
								}

								memory.Set(out.Bytes(), 0xFF)
								op.noAlloc(left, right, lOffset, rOffset, out.Bytes(), outOffset, 0)
								if out.Len() > 0 {
									memory.Set(expected, 0xFF)
									s.Equal(expected, out.Bytes())
								} else {
									s.Nil(out.Bytes())
								}
								out.Release()
							})
							s.Run("alloc", func() {
								out = op.alloc(memory.DefaultAllocator, left, right, lOffset, rOffset, length, outOffset)
								rdr := bitutil.NewBitmapReader(out.Bytes(), int(outOffset), int(length))
								assertReaderVals(s.T(), rdr, resultBits)
							})
							s.Run("noalloc", func() {
								memory.Set(out.Bytes(), 0x00)
								op.noAlloc(left, right, lOffset, rOffset, out.Bytes(), outOffset, length)
								rdr := bitutil.NewBitmapReader(out.Bytes(), int(outOffset), int(length))
								assertReaderVals(s.T(), rdr, resultBits)
							})
						})
					}
				})
			}
		})
	}
}

func (s *BitmapOpSuite) TestBitmapAnd() {
	op := bitmapOp{
		noAlloc: bitutil.BitmapAnd,
		alloc:   bitutil.BitmapAndAlloc,
	}

	leftBits := []int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}
	rightBits := []int{0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0}
	resultBits := []bool{false, false, true, false, false, false, false, false, false, true, false, false, false, false}

	s.Run("aligned", func() {
		s.testAligned(op, leftBits, rightBits, resultBits)
	})
	s.Run("unaligned", func() {
		s.testUnaligned(op, leftBits, rightBits, resultBits)
	})
}

func (s *BitmapOpSuite) TestBitmapOr() {
	op := bitmapOp{
		noAlloc: bitutil.BitmapOr,
		alloc:   bitutil.BitmapOrAlloc,
	}

	leftBits := []int{0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}
	rightBits := []int{0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0}
	resultBits := []bool{false, true, true, true, true, true, false, true, true, true, true, true, true, true}

	s.Run("aligned", func() {
		s.testAligned(op, leftBits, rightBits, resultBits)
	})
	s.Run("unaligned", func() {
		s.testUnaligned(op, leftBits, rightBits, resultBits)
	})
}

func TestBitmapOps(t *testing.T) {
	suite.Run(t, new(BitmapOpSuite))
}

func TestSmallBitmapOp(t *testing.T) {
	// 0b01111111 0b11001111
	left := [2]byte{127, 207}
	// 0b11111110 0b01111111
	right := [2]byte{254, 127}
	// 0b01111110 0b01001111
	results := [2]byte{126, 79}

	var out [2]byte
	bitutil.BitmapAnd(left[:], right[:], 0, 0, out[:], 0, 8)
	assert.Equal(t, results[:1], out[:1])

	bitutil.BitmapAnd(left[:], right[:], 0, 0, out[:], 0, 16)
	assert.Equal(t, results, out)
}

func createRandomBuffer(mem memory.Allocator, src *rand.Rand, nbytes int) []byte {
	buf := mem.Allocate(nbytes)
	src.Read(buf)
	return buf
}

func benchBitOpImpl(b *testing.B, nBytes, offset int, op noAllocFn) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	r := rand.New(rand.NewSource(0))

	buf1 := createRandomBuffer(mem, r, nBytes)
	buf2 := createRandomBuffer(mem, r, nBytes)
	buf3 := createRandomBuffer(mem, r, nBytes)
	b.Cleanup(func() {
		mem.Free(buf1)
		mem.Free(buf2)
		mem.Free(buf3)
	})

	numBits := nBytes*8 - offset
	b.ResetTimer()
	b.SetBytes(bitutil.BytesForBits(int64(numBits)) * 2)
	for i := 0; i < b.N; i++ {
		op(buf1, buf2, 0, int64(offset), buf3, 0, int64(numBits))
	}
}

func BenchmarkBitmapAnd(b *testing.B) {
	sizes := []int{bufferSize * 4, bufferSize * 16}
	offsets := []int{0, 1, 2}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("nbytes=%d", s), func(b *testing.B) {
			for _, o := range offsets {
				b.Run(fmt.Sprintf("%d", o), func(b *testing.B) {
					benchBitOpImpl(b, s, o, bitutil.BitmapAnd)
				})
			}
		})
	}
}

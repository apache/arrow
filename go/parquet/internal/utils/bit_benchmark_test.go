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
	"math/rand"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/parquet/internal/testutils"
	"github.com/apache/arrow/go/parquet/internal/utils"
)

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

func randomBitsBuffer(nbits, setPct int64) []byte {
	rag := testutils.NewRandomArrayGenerator(23)
	prob := float64(0)
	if setPct != -1 {
		prob = float64(setPct) / 100.0
	}
	buf := make([]byte, int(bitutil.BytesForBits(nbits)))
	rag.GenerateBitmap(buf, nbits, prob)

	if setPct == -1 {
		wr := utils.NewBitmapWriter(buf, 0, nbits)
		for i := int64(0); i < nbits; i++ {
			if i%2 == 0 {
				wr.Set()
			} else {
				wr.Clear()
			}
			wr.Next()
		}
	}
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
				rdr := utils.NewBitmapReader(buf, 0, int64(nbits))
				for j := 0; j < nbits; j++ {
					if rdr.Set() {
						total++
					}
					rdr.Next()
				}
			}
			{
				total := 0
				rdr := utils.NewBitmapReader(buf, 0, int64(nbits))
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

func testBitRunReader(rdr utils.BitRunReader) (setTotal int64) {
	for {
		br := rdr.NextRun()
		if br.Len == 0 {
			break
		}
		if br.Set {
			setTotal += br.Len
		}
	}
	return
}

func BenchmarkBitRunReader(b *testing.B) {
	const numBits = 4096
	for _, pct := range []int64{1, 0, 10, 25, 50, 60, 75, 99} {
		buf := randomBitsBuffer(numBits, pct)
		b.Run("set pct "+strconv.Itoa(int(pct)), func(b *testing.B) {
			b.Run("linear", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := linearBitRunReader{utils.NewBitmapReader(buf, 0, numBits)}
					testBitRunReader(rdr)
				}
			})
			b.Run("internal", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := utils.NewBitRunReader(buf, 0, numBits)
					testBitRunReader(rdr)
				}
			})
		})
	}
}

func testSetBitRunReader(rdr utils.SetBitRunReader) (setTotal int64) {
	for {
		br := rdr.NextRun()
		if br.Length == 0 {
			break
		}
		setTotal += br.Length
	}
	return
}

func BenchmarkSetBitRunReader(b *testing.B) {
	const numBits = 4096
	for _, pct := range []int64{1, 0, 10, 25, 50, 60, 75, 99} {
		buf := randomBitsBuffer(numBits, pct)
		b.Run("set pct "+strconv.Itoa(int(pct)), func(b *testing.B) {
			b.Run("reader", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := utils.NewSetBitRunReader(buf, 0, numBits)
					testSetBitRunReader(rdr)
				}
			})
			b.Run("reverse rdr", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := utils.NewReverseSetBitRunReader(buf, 0, numBits)
					testSetBitRunReader(rdr)
				}
			})
		})
	}
}

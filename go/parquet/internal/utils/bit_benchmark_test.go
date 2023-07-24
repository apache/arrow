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
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/internal/bitutils"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
)

type linearBitRunReader struct {
	reader *bitutil.BitmapReader
}

func (l linearBitRunReader) NextRun() bitutils.BitRun {
	r := bitutils.BitRun{0, l.reader.Set()}
	for l.reader.Pos() < l.reader.Len() && l.reader.Set() == r.Set {
		r.Len++
		l.reader.Next()
	}
	return r
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
		wr := bitutil.NewBitmapWriter(buf, 0, int(nbits))
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

func testBitRunReader(rdr bitutils.BitRunReader) (setTotal int64) {
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
					rdr := linearBitRunReader{bitutil.NewBitmapReader(buf, 0, numBits)}
					testBitRunReader(rdr)
				}
			})
			b.Run("internal", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := bitutils.NewBitRunReader(buf, 0, numBits)
					testBitRunReader(rdr)
				}
			})
		})
	}
}

func testSetBitRunReader(rdr bitutils.SetBitRunReader) (setTotal int64) {
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
					rdr := bitutils.NewSetBitRunReader(buf, 0, numBits)
					testSetBitRunReader(rdr)
				}
			})
			b.Run("reverse rdr", func(b *testing.B) {
				b.SetBytes(numBits / 8)
				for i := 0; i < b.N; i++ {
					rdr := bitutils.NewReverseSetBitRunReader(buf, 0, numBits)
					testSetBitRunReader(rdr)
				}
			})
		})
	}
}

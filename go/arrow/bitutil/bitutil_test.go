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
	"testing"

	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/internal/testing/tools"
	"github.com/stretchr/testify/assert"
)

func TestIsMultipleOf8(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want bool
	}{
		{-16, true},
		{-9, false},
		{-8, true},
		{-7, false},
		{-4, false},
		{-1, false},
		{-0, true},
		{0, true},
		{1, false},
		{4, false},
		{7, false},
		{8, true},
		{9, false},
		{16, true},
	} {
		t.Run(fmt.Sprintf("v=%d", tc.v), func(t *testing.T) {
			got := bitutil.IsMultipleOf8(tc.v)
			if got != tc.want {
				t.Fatalf("IsMultipleOf8(%d): got=%v, want=%v", tc.v, got, tc.want)
			}
		})
	}
}

func TestCeilByte(t *testing.T) {
	tests := []struct {
		name    string
		in, exp int
	}{
		{"zero", 0, 0},
		{"five", 5, 8},
		{"sixteen", 16, 16},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := bitutil.CeilByte(test.in)
			assert.Equal(t, test.exp, got)
		})
	}
}

func TestBitIsSet(t *testing.T) {
	buf := make([]byte, 2)
	buf[0] = 0xa1
	buf[1] = 0xc2
	exp := []bool{true, false, false, false, false, true, false, true, false, true, false, false, false, false, true, true}
	var got []bool
	for i := 0; i < 0x10; i++ {
		got = append(got, bitutil.BitIsSet(buf, i))
	}
	assert.Equal(t, exp, got)
}

func TestBitIsNotSet(t *testing.T) {
	buf := make([]byte, 2)
	buf[0] = 0xa1
	buf[1] = 0xc2
	exp := []bool{false, true, true, true, true, false, true, false, true, false, true, true, true, true, false, false}
	var got []bool
	for i := 0; i < 0x10; i++ {
		got = append(got, bitutil.BitIsNotSet(buf, i))
	}
	assert.Equal(t, exp, got)
}

func TestClearBit(t *testing.T) {
	buf := make([]byte, 2)
	buf[0] = 0xff
	buf[1] = 0xff
	for i, v := range []bool{false, true, true, true, true, false, true, false, true, false, true, true, true, true, false, false} {
		if v {
			bitutil.ClearBit(buf, i)
		}
	}
	assert.Equal(t, []byte{0xa1, 0xc2}, buf)
}

func TestSetBit(t *testing.T) {
	buf := make([]byte, 2)
	for i, v := range []bool{true, false, false, false, false, true, false, true, false, true, false, false, false, false, true, true} {
		if v {
			bitutil.SetBit(buf, i)
		}
	}
	assert.Equal(t, []byte{0xa1, 0xc2}, buf)
}

func TestSetBitTo(t *testing.T) {
	buf := make([]byte, 2)
	for i, v := range []bool{true, false, false, false, false, true, false, true, false, true, false, false, false, false, true, true} {
		bitutil.SetBitTo(buf, i, v)
	}
	assert.Equal(t, []byte{0xa1, 0xc2}, buf)
}

func TestCountSetBits(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
		off  int
		n    int
		exp  int
	}{
		{"some 03 bits", bbits(0x11000000), 0, 3, 2},
		{"some 11 bits", bbits(0x11000011, 0x01000000), 0, 11, 5},
		{"some 72 bits", bbits(0x11001010, 0x11110000, 0x00001111, 0x11000011, 0x11001010, 0x11110000, 0x00001111, 0x11000011, 0x10001001), 0, 9 * 8, 35},
		{"all  08 bits", bbits(0x11111110), 0, 8, 7},
		{"all  03 bits", bbits(0x11100001), 0, 3, 3},
		{"all  11 bits", bbits(0x11111111, 0x11111111), 0, 11, 11},
		{"all  72 bits", bbits(0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111), 0, 9 * 8, 72},
		{"none 03 bits", bbits(0x00000001), 0, 3, 0},
		{"none 11 bits", bbits(0x00000000, 0x00000000), 0, 11, 0},
		{"none 72 bits", bbits(0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000), 0, 9 * 8, 0},

		{"some 03 bits - offset+1", bbits(0x11000000), 1, 3, 1},
		{"some 03 bits - offset+2", bbits(0x11000000), 2, 3, 0},
		{"some 11 bits - offset+1", bbits(0x11000011, 0x01000000, 0x00000000), 1, 11, 4},
		{"some 11 bits - offset+2", bbits(0x11000011, 0x01000000, 0x00000000), 2, 11, 3},
		{"some 11 bits - offset+3", bbits(0x11000011, 0x01000000, 0x00000000), 3, 11, 3},
		{"some 11 bits - offset+6", bbits(0x11000011, 0x01000000, 0x00000000), 6, 11, 3},
		{"some 11 bits - offset+7", bbits(0x11000011, 0x01000000, 0x00000000), 7, 11, 2},
		{"some 11 bits - offset+8", bbits(0x11000011, 0x01000000, 0x00000000), 8, 11, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := bitutil.CountSetBits(test.buf, test.off, test.n)
			assert.Equal(t, test.exp, got)
		})
	}
}

func TestCountSetBitsOffset(t *testing.T) {
	slowCountSetBits := func(buf []byte, offset, n int) int {
		count := 0
		for i := offset; i < offset+n; i++ {
			if bitutil.BitIsSet(buf, i) {
				count++
			}
		}
		return count
	}

	const (
		bufSize = 1000
		nbits   = bufSize * 8
	)

	offsets := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 32, 37, 63, 64, 128, nbits - 30, nbits - 64}

	buf := make([]byte, bufSize)

	rng := rand.New(rand.NewSource(0))
	_, err := rng.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	for i, offset := range offsets {
		want := slowCountSetBits(buf, offset, nbits-offset)
		got := bitutil.CountSetBits(buf, offset, nbits-offset)
		if got != want {
			t.Errorf("offset[%2d/%2d]=%5d. got=%5d, want=%5d", i+1, len(offsets), offset, got, want)
		}
	}
}

func TestSetBitsTo(t *testing.T) {
	for _, fillByte := range []byte{0x00, 0xFF} {
		{
			// set within a byte
			bm := []byte{fillByte, fillByte, fillByte, fillByte}
			bitutil.SetBitsTo(bm, 2, 2, true)
			bitutil.SetBitsTo(bm, 4, 2, false)
			assert.Equal(t, []byte{(fillByte &^ 0x3C) | 0xC}, bm[:1])
		}
		{
			// test straddling a single byte boundary
			bm := []byte{fillByte, fillByte, fillByte, fillByte}
			bitutil.SetBitsTo(bm, 4, 7, true)
			bitutil.SetBitsTo(bm, 11, 7, false)
			assert.Equal(t, []byte{(fillByte & 0xF) | 0xF0, 0x7, fillByte &^ 0x3}, bm[:3])
		}
		{
			// test byte aligned end
			bm := []byte{fillByte, fillByte, fillByte, fillByte}
			bitutil.SetBitsTo(bm, 4, 4, true)
			bitutil.SetBitsTo(bm, 8, 8, false)
			assert.Equal(t, []byte{(fillByte & 0xF) | 0xF0, 0x00, fillByte}, bm[:3])
		}
		{
			// test byte aligned end, multiple bytes
			bm := []byte{fillByte, fillByte, fillByte, fillByte}
			bitutil.SetBitsTo(bm, 0, 24, false)
			falseByte := byte(0)
			assert.Equal(t, []byte{falseByte, falseByte, falseByte, fillByte}, bm)
		}
	}
}

func bbits(v ...int32) []byte {
	return tools.IntsToBitsLSB(v...)
}

func BenchmarkBitIsSet(b *testing.B) {
	buf := make([]byte, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bitutil.BitIsSet(buf, (i%32)&0x1a)
	}
}

func BenchmarkSetBit(b *testing.B) {
	buf := make([]byte, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bitutil.SetBit(buf, (i%32)&0x1a)
	}
}

func BenchmarkSetBitTo(b *testing.B) {
	vals := []bool{true, false, false, false, false, true, false, true, false, true, false, false, false, false, true, true}
	buf := make([]byte, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bitutil.SetBitTo(buf, i%32, vals[i%len(vals)])
	}
}

var (
	intval int
)

func benchmarkCountSetBitsN(b *testing.B, offset, n int) {
	nn := n/8 + 1
	buf := make([]byte, nn)
	//src := [4]byte{0x1f, 0xaa, 0xba, 0x11}
	src := [4]byte{0x01, 0x01, 0x01, 0x01}
	for i := 0; i < nn; i++ {
		buf[i] = src[i&0x3]
	}
	b.ResetTimer()
	var res int
	for i := 0; i < b.N; i++ {
		res = bitutil.CountSetBits(buf, offset, n-offset)
	}
	intval = res
}

func BenchmarkCountSetBits_3(b *testing.B) {
	benchmarkCountSetBitsN(b, 0, 3)
}

func BenchmarkCountSetBits_32(b *testing.B) {
	benchmarkCountSetBitsN(b, 0, 32)
}

func BenchmarkCountSetBits_128(b *testing.B) {
	benchmarkCountSetBitsN(b, 0, 128)
}

func BenchmarkCountSetBits_1000(b *testing.B) {
	benchmarkCountSetBitsN(b, 0, 1000)
}

func BenchmarkCountSetBits_1024(b *testing.B) {
	benchmarkCountSetBitsN(b, 0, 1024)
}

func BenchmarkCountSetBitsOffset_3(b *testing.B) {
	benchmarkCountSetBitsN(b, 1, 3)
}

func BenchmarkCountSetBitsOffset_32(b *testing.B) {
	benchmarkCountSetBitsN(b, 1, 32)
}

func BenchmarkCountSetBitsOffset_128(b *testing.B) {
	benchmarkCountSetBitsN(b, 1, 128)
}

func BenchmarkCountSetBitsOffset_1000(b *testing.B) {
	benchmarkCountSetBitsN(b, 1, 1000)
}

func BenchmarkCountSetBitsOffset_1024(b *testing.B) {
	benchmarkCountSetBitsN(b, 1, 1024)
}

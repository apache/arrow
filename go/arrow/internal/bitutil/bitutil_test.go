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
	"testing"

	"github.com/apache/arrow/go/arrow/internal/bitutil"
	"github.com/apache/arrow/go/arrow/internal/testing/tools"
	"github.com/stretchr/testify/assert"
)

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
		n    int
		exp  int
	}{
		{"some 03 bits", bbits(0x11000000), 3, 2},
		{"some 11 bits", bbits(0x11000011, 0x01000000), 11, 5},
		{"some 72 bits", bbits(0x11001010, 0x11110000, 0x00001111, 0x11000011, 0x11001010, 0x11110000, 0x00001111, 0x11000011, 0x10001001), 9 * 8, 35},
		{"all  03 bits", bbits(0x11100001), 3, 3},
		{"all  11 bits", bbits(0x11111111, 0x11111111), 11, 11},
		{"all  72 bits", bbits(0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111, 0x11111111), 9 * 8, 72},
		{"none 03 bits", bbits(0x00000001), 3, 0},
		{"none 11 bits", bbits(0x00000000, 0x00000000), 11, 0},
		{"none 72 bits", bbits(0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000), 9 * 8, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := bitutil.CountSetBits(test.buf, test.n)
			assert.Equal(t, test.exp, got)
		})
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

func benchmarkCountSetBitsN(b *testing.B, n int) {
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
		res = bitutil.CountSetBits(buf, n)
	}
	intval = res
}

func BenchmarkCountSetBits_3(b *testing.B) {
	benchmarkCountSetBitsN(b, 3)
}

func BenchmarkCountSetBits_32(b *testing.B) {
	benchmarkCountSetBitsN(b, 32)
}

func BenchmarkCountSetBits_128(b *testing.B) {
	benchmarkCountSetBitsN(b, 128)
}

func BenchmarkCountSetBits_1000(b *testing.B) {
	benchmarkCountSetBitsN(b, 1000)
}

func BenchmarkCountSetBits_1024(b *testing.B) {
	benchmarkCountSetBitsN(b, 1024)
}

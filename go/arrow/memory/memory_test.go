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

package memory_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func makeExpectedBuf(sz, lo, hi int, c byte) []byte {
	buf := make([]byte, sz)
	for i := lo; i < hi; i++ {
		buf[i] = c
	}
	return buf
}

func TestSet(t *testing.T) {
	tests := []struct {
		name   string
		sz     int
		lo, hi int
		c      byte
	}{
		{"sz=0", 0, 0, 0, 0x00},
		{"all,sz=7", 7, 0, 7, 0x1f},
		{"part,sz=7", 7, 3, 4, 0x1f},
		{"last,sz=7", 7, 6, 7, 0x1f},
		{"all,sz=25", 25, 0, 25, 0x1f},
		{"part,sz=25", 25, 13, 19, 0x1f},
		{"last,sz=25", 25, 24, 25, 0x1f},
		{"all,sz=4096", 4096, 0, 4096, 0x1f},
		{"part,sz=4096", 4096, 1000, 3000, 0x1f},
		{"last,sz=4096", 4096, 4095, 4096, 0x1f},
		{"all,sz=16384", 16384, 0, 16384, 0x1f},
		{"part,sz=16384", 16384, 3333, 10000, 0x1f},
		{"last,sz=16384", 16384, 16383, 16384, 0x1f},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := make([]byte, test.sz)
			memory.Set(buf[test.lo:test.hi], test.c)
			exp := makeExpectedBuf(test.sz, test.lo, test.hi, test.c)
			assert.Equal(t, exp, buf)
		})
	}
}

func benchmarkSet(b *testing.B, n int64) {
	buf := make([]byte, n)
	b.SetBytes(n)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		memory.Set(buf, 0x1f)
	}
}

func BenchmarkSet_8(b *testing.B) {
	benchmarkSet(b, 8)
}

func BenchmarkSet_32(b *testing.B) {
	benchmarkSet(b, 32)
}

func BenchmarkSet_64(b *testing.B) {
	benchmarkSet(b, 64)
}

func BenchmarkSet_500(b *testing.B) {
	benchmarkSet(b, 500)
}

func BenchmarkSet_512(b *testing.B) {
	benchmarkSet(b, 512)
}

func BenchmarkSet_1000(b *testing.B) {
	benchmarkSet(b, 1000)
}

func BenchmarkSet_1024(b *testing.B) {
	benchmarkSet(b, 1024)
}

func BenchmarkSet_2000(b *testing.B) {
	benchmarkSet(b, 2000)
}

func BenchmarkSet_2048(b *testing.B) {
	benchmarkSet(b, 2048)
}

func BenchmarkSet_4000(b *testing.B) {
	benchmarkSet(b, 4000)
}

func BenchmarkSet_4096(b *testing.B) {
	benchmarkSet(b, 4096)
}

func BenchmarkSet_8000(b *testing.B) {
	benchmarkSet(b, 8000)
}

func BenchmarkSet_8192(b *testing.B) {
	benchmarkSet(b, 8192)
}

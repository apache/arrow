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

package bitutils_test

import (
	"testing"

	"github.com/apache/arrow/go/v12/internal/bitutils"
	"golang.org/x/exp/rand"
)

const kBufferSize int64 = 1024 * 8

var pattern = []bool{false, false, false, true, true, true}

func runBench(b *testing.B, bitmap []byte, nbits int64, fn func([]byte, int64, int64, func() bool)) {
	for n := 0; n < b.N; n++ {
		patternIndex := 0
		gen := func() bool {
			b := pattern[patternIndex]
			patternIndex++
			if patternIndex == len(pattern) {
				patternIndex = 0
			}
			return b
		}

		fn(bitmap, 0, nbits, gen)
	}
}

func BenchmarkGenerateBits(b *testing.B) {
	nbits := kBufferSize * 8
	// random bytes
	r := rand.New(rand.NewSource(0))
	bitmap := make([]byte, kBufferSize)
	r.Read(bitmap)

	b.ResetTimer()
	b.SetBytes(kBufferSize)
	runBench(b, bitmap, nbits, bitutils.GenerateBits)
}

func BenchmarkGenerateBitsUnrolled(b *testing.B) {
	nbits := kBufferSize * 8
	// random bytes
	r := rand.New(rand.NewSource(0))
	bitmap := make([]byte, kBufferSize)
	r.Read(bitmap)

	b.ResetTimer()
	b.SetBytes(kBufferSize)
	runBench(b, bitmap, nbits, bitutils.GenerateBitsUnrolled)
}

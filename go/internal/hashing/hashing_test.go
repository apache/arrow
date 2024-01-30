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

package hashing

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func MakeDistinctIntegers(nvals int) map[int]bool {
	r := rand.New(rand.NewSource(42))
	values := make(map[int]bool)
	for len(values) < nvals {
		values[r.Int()] = true
	}
	return values
}

func MakeSequentialIntegers(nvals int) map[int]bool {
	values := make(map[int]bool)
	for i := 0; i < nvals; i++ {
		values[i] = true
	}
	return values
}

func MakeDistinctStrings(nvals int) map[string]bool {
	values := make(map[string]bool)

	r := rand.New(rand.NewSource(42))

	max := 'z'
	min := '0'
	for len(values) < nvals {
		data := make([]byte, r.Intn(24))
		for idx := range data {
			data[idx] = byte(r.Intn(int(max-min+1)) + int(min))
		}
		values[string(data)] = true
	}
	return values
}

func TestHashingQualityInt(t *testing.T) {
	const nvalues = 10000

	tests := []struct {
		name    string
		values  map[int]bool
		quality float64
	}{
		{"distinct", MakeDistinctIntegers(nvalues), 0.96},
		{"sequential", MakeSequentialIntegers(nvalues), 0.96},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashes := make(map[uint64]bool)
			for k := range tt.values {
				hashes[hashInt(uint64(k), 0)] = true
				hashes[hashInt(uint64(k), 1)] = true
			}
			assert.GreaterOrEqual(t, float64(len(hashes)), tt.quality*float64(2*len(tt.values)))
		})
	}
}

func TestHashingBoundsStrings(t *testing.T) {
	sizes := []int{1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21}
	for _, s := range sizes {
		str := make([]byte, s)
		for idx := range str {
			str[idx] = uint8(idx)
		}

		h := Hash(str, 1)
		diff := 0
		for i := 0; i < 120; i++ {
			str[len(str)-1] = uint8(i)
			if Hash(str, 1) != h {
				diff++
			}
		}
		assert.GreaterOrEqual(t, diff, 118)
	}
}

func TestHashingQualityString(t *testing.T) {
	const nvalues = 10000
	values := MakeDistinctStrings(nvalues)

	hashes := make(map[uint64]bool)
	for k := range values {
		hashes[hashString(k, 0)] = true
		hashes[hashString(k, 1)] = true
	}
	assert.GreaterOrEqual(t, float64(len(hashes)), 0.96*float64(2*len(values)))
}

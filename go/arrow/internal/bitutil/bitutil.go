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

package bitutil

import (
	"math/bits"
	"reflect"
	"unsafe"
)

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)

// NextPowerOf2 rounds x to the next power of two.
func NextPowerOf2(x int) int { return 1 << uint(bits.Len(uint(x))) }

// CeilByte rounds size to the next multiple of 8.
func CeilByte(size int) int { return (size + 7) &^ 7 }

// BitIsSet returns true if the bit at index i in buf is set (1).
func BitIsSet(buf []byte, i int) bool { return (buf[uint(i)/8] & BitMask[byte(i)%8]) != 0 }

// BitIsNotSet returns true if the bit at index i in buf is not set (0).
func BitIsNotSet(buf []byte, i int) bool { return (buf[uint(i)/8] & BitMask[byte(i)%8]) == 0 }

// SetBit sets the bit at index i in buf to 1.
func SetBit(buf []byte, i int) { buf[uint(i)/8] |= BitMask[byte(i)%8] }

// ClearBit sets the bit at index i in buf to 0.
func ClearBit(buf []byte, i int) { buf[uint(i)/8] &= FlippedBitMask[byte(i)%8] }

// SetBitTo sets the bit at index i in buf to val.
func SetBitTo(buf []byte, i int, val bool) {
	if val {
		SetBit(buf, i)
	} else {
		ClearBit(buf, i)
	}
}

// CountSetBits counts the number of 1's in buf up to n bits.
func CountSetBits(buf []byte, n int) int {
	count := 0

	uint64Bytes := n / uint64SizeBits * 8
	for _, v := range bytesToUint64(buf[:uint64Bytes]) {
		count += bits.OnesCount64(v)
	}

	for _, v := range buf[uint64Bytes : n/8] {
		count += bits.OnesCount8(v)
	}

	// tail bits
	for i := n &^ 0x7; i < n; i++ {
		if BitIsSet(buf, i) {
			count++
		}
	}

	return count
}

const (
	uint64SizeBytes = int(unsafe.Sizeof(uint64(0)))
	uint64SizeBits  = uint64SizeBytes * 8
)

func bytesToUint64(b []byte) []uint64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []uint64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / uint64SizeBytes
	s.Cap = h.Cap / uint64SizeBytes

	return res
}

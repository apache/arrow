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

package tools

import "math/bits"

// IntsToBitsLSB encodes ints as LSB 0 bit numbering per https://en.wikipedia.org/wiki/Bit_numbering#LSB_0_bit_numbering
// The low bit of each nibble is tested, therefore integers should be written as 8-digit
// hex numbers consisting of 1s or 0s.
//
//     IntsToBitsLSB(0x11001010) -> 0x35
func IntsToBitsLSB(v ...int32) []byte {
	res := make([]byte, 0, len(v))
	for _, b := range v {
		c := uint8(0)
		for i := uint(0); i < 8; i++ {
			if b&1 == 1 {
				c |= 1 << i
			}
			b >>= 4
		}
		c = bits.Reverse8(c)
		res = append(res, c)
	}
	return res
}

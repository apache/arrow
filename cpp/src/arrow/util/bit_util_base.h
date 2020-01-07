// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>

// A small subset of BitUtil routines that are used in heavily-included
// headers such as array.h

namespace arrow {
namespace BitUtil {

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the bitwise complement version of kBitmask
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

// Bitmask selecting the (k - 1) preceding bits in a byte
static constexpr uint8_t kPrecedingBitmask[] = {0, 1, 3, 7, 15, 31, 63, 127};
static constexpr uint8_t kPrecedingWrappingBitmask[] = {255, 1, 3, 7, 15, 31, 63, 127};

// the bitwise complement version of kPrecedingBitmask
static constexpr uint8_t kTrailingBitmask[] = {255, 254, 252, 248, 240, 224, 192, 128};

constexpr int64_t BytesForBits(int64_t bits) { return (bits + 7) >> 3; }

static inline bool GetBit(const uint8_t* bits, uint64_t i) {
  return (bits[i >> 3] >> (i & 0x07)) & 1;
}

static inline void SetBit(uint8_t* bits, int64_t i) { bits[i / 8] |= kBitmask[i % 8]; }

static inline void ClearBit(uint8_t* bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

}  // namespace BitUtil
}  // namespace arrow

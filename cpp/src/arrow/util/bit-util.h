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

#ifndef ARROW_UTIL_BIT_UTIL_H
#define ARROW_UTIL_BIT_UTIL_H

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class Status;

namespace util {

static inline int64_t ceil_byte(int64_t size) {
  return (size + 7) & ~7;
}

static inline int64_t bytes_for_bits(int64_t size) {
  return ceil_byte(size) / 8;
}

static inline int64_t ceil_2bytes(int64_t size) {
  return (size + 15) & ~15;
}

static constexpr uint8_t BITMASK[] = {1, 2, 4, 8, 16, 32, 64, 128};

static inline bool get_bit(const uint8_t* bits, int i) {
  return static_cast<bool>(bits[i / 8] & BITMASK[i % 8]);
}

static inline bool bit_not_set(const uint8_t* bits, int i) {
  return (bits[i / 8] & BITMASK[i % 8]) == 0;
}

static inline void clear_bit(uint8_t* bits, int i) {
  bits[i / 8] &= ~BITMASK[i % 8];
}

static inline void set_bit(uint8_t* bits, int i) {
  bits[i / 8] |= BITMASK[i % 8];
}

static inline int64_t next_power2(int64_t n) {
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

static inline bool is_multiple_of_64(int64_t n) {
  return (n & 63) == 0;
}

inline int64_t RoundUpToMultipleOf64(int64_t num) {
  // TODO(wesm): is this definitely needed?
  // DCHECK_GE(num, 0);
  constexpr int64_t round_to = 64;
  constexpr int64_t force_carry_addend = round_to - 1;
  constexpr int64_t truncate_bitmask = ~(round_to - 1);
  constexpr int64_t max_roundable_num = std::numeric_limits<int64_t>::max() - round_to;
  if (num <= max_roundable_num) { return (num + force_carry_addend) & truncate_bitmask; }
  // handle overflow case.  This should result in a malloc error upstream
  return num;
}

void bytes_to_bits(const std::vector<uint8_t>& bytes, uint8_t* bits);
ARROW_EXPORT Status bytes_to_bits(const std::vector<uint8_t>&, std::shared_ptr<Buffer>*);

}  // namespace util

}  // namespace arrow

#endif  // ARROW_UTIL_BIT_UTIL_H

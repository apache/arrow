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
#include <cstring>

#include "arrow/util/bit_util.h"
#include "arrow/util/ubsan.h"
#include "parquet/types.h"

namespace parquet {

struct float16 {
  constexpr static uint16_t min() { return 0b1111101111111111; }
  constexpr static uint16_t max() { return 0b0111101111111111; }
  constexpr static uint16_t positive_zero() { return 0b0000000000000000; }
  constexpr static uint16_t negative_zero() { return 0b1000000000000000; }

  static uint8_t* min_ptr() { return min_; }
  static uint8_t* max_ptr() { return max_; }
  static uint8_t* positive_zero_ptr() { return positive_zero_; }
  static uint8_t* negative_zero_ptr() { return negative_zero_; }

  static bool is_nan(uint16_t n) { return (n & 0x7c00) == 0x7c00 && (n & 0x03ff) != 0; }
  static bool is_zero(uint16_t n) { return (n & 0x7fff) == 0; }
  static bool signbit(uint16_t n) { return (n & 0x8000) != 0; }

  static uint16_t Pack(const uint8_t* src) {
    return ::arrow::bit_util::FromLittleEndian(::arrow::util::SafeLoadAs<uint16_t>(src));
  }
  static uint16_t Pack(const FLBA& src) { return Pack(src.ptr); }

  static uint8_t* Unpack(uint16_t src, uint8_t* dest) {
    src = ::arrow::bit_util::ToLittleEndian(src);
    return static_cast<uint8_t*>(std::memcpy(dest, &src, sizeof(src)));
  }

 private:
  static inline uint8_t min_[] = {0b11111111, 0b11111011};
  static inline uint8_t max_[] = {0b11111111, 0b01111011};
  static inline uint8_t positive_zero_[] = {0b00000000, 0b00000000};
  static inline uint8_t negative_zero_[] = {0b00000000, 0b10000000};
};

}  // namespace parquet

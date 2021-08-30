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

#if defined(ARROW_HAVE_AVX2)
#include <immintrin.h>
#endif

#include <cstdint>

#include "arrow/compute/exec/key_encode.h"
#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

// Implementations are based on xxh3 32-bit algorithm description from:
// https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md
//
class Hashing {
 public:
  static void hash_fixed(int64_t hardware_flags, uint32_t num_keys, uint32_t length_key,
                         const uint8_t* keys, uint32_t* hashes);

  static void hash_varlen(int64_t hardware_flags, uint32_t num_rows,
                          const uint32_t* offsets, const uint8_t* concatenated_keys,
                          uint32_t* temp_buffer,  // Needs to hold 4 x 32-bit per row
                          uint32_t* hashes);

  static void HashMultiColumn(const std::vector<KeyEncoder::KeyColumnArray>& cols,
                              KeyEncoder::KeyEncoderContext* ctx, uint32_t* out_hash);

 private:
  static const uint32_t PRIME32_1 = 0x9E3779B1;  // 0b10011110001101110111100110110001
  static const uint32_t PRIME32_2 = 0x85EBCA77;  // 0b10000101111010111100101001110111
  static const uint32_t PRIME32_3 = 0xC2B2AE3D;  // 0b11000010101100101010111000111101
  static const uint32_t PRIME32_4 = 0x27D4EB2F;  // 0b00100111110101001110101100101111
  static const uint32_t PRIME32_5 = 0x165667B1;  // 0b00010110010101100110011110110001

  static void HashCombine(KeyEncoder::KeyEncoderContext* ctx, uint32_t num_rows,
                          uint32_t* accumulated_hash, const uint32_t* next_column_hash);

#if defined(ARROW_HAVE_AVX2)
  static uint32_t HashCombine_avx2(uint32_t num_rows, uint32_t* accumulated_hash,
                                   const uint32_t* next_column_hash);
#endif

  // Avalanche
  static inline uint32_t avalanche_helper(uint32_t acc);
#if defined(ARROW_HAVE_AVX2)
  static void avalanche_avx2(uint32_t num_keys, uint32_t* hashes);
#endif
  static void avalanche(int64_t hardware_flags, uint32_t num_keys, uint32_t* hashes);

  // Accumulator combine
  static inline uint32_t combine_accumulators(const uint32_t acc1, const uint32_t acc2,
                                              const uint32_t acc3, const uint32_t acc4);
#if defined(ARROW_HAVE_AVX2)
  static inline uint64_t combine_accumulators_avx2(__m256i acc);
#endif

  // Helpers
  template <typename T>
  static inline void helper_8B(uint32_t key_length, uint32_t num_keys, const T* keys,
                               uint32_t* hashes);
  static inline void helper_stripe(uint32_t offset, uint64_t mask_hi, const uint8_t* keys,
                                   uint32_t& acc1, uint32_t& acc2, uint32_t& acc3,
                                   uint32_t& acc4);
  static inline uint32_t helper_tail(uint32_t offset, uint64_t mask, const uint8_t* keys,
                                     uint32_t acc);
#if defined(ARROW_HAVE_AVX2)
  static void helper_stripes_avx2(uint32_t num_keys, uint32_t key_length,
                                  const uint8_t* keys, uint32_t* hash);
  static void helper_tails_avx2(uint32_t num_keys, uint32_t key_length,
                                const uint8_t* keys, uint32_t* hash);
#endif
  static void helper_stripes(int64_t hardware_flags, uint32_t num_keys,
                             uint32_t key_length, const uint8_t* keys, uint32_t* hash);
  static void helper_tails(int64_t hardware_flags, uint32_t num_keys, uint32_t key_length,
                           const uint8_t* keys, uint32_t* hash);

#if defined(ARROW_HAVE_AVX2)
  static void hash_varlen_avx2(uint32_t num_rows, const uint32_t* offsets,
                               const uint8_t* concatenated_keys,
                               uint32_t* temp_buffer,  // Needs to hold 4 x 32-bit per row
                               uint32_t* hashes);
#endif
};

}  // namespace compute
}  // namespace arrow

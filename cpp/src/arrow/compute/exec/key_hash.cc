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

#include "arrow/compute/exec/key_hash.h"

#include <memory.h>

#include <algorithm>
#include <cstdint>

#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

inline uint32_t Hashing::avalanche_helper(uint32_t acc) {
  acc ^= (acc >> 15);
  acc *= PRIME32_2;
  acc ^= (acc >> 13);
  acc *= PRIME32_3;
  acc ^= (acc >> 16);
  return acc;
}

void Hashing::avalanche(int64_t hardware_flags, uint32_t num_keys, uint32_t* hashes) {
  uint32_t processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    int tail = num_keys % 8;
    avalanche_avx2(num_keys - tail, hashes);
    processed = num_keys - tail;
  }
#endif
  for (uint32_t i = processed; i < num_keys; ++i) {
    hashes[i] = avalanche_helper(hashes[i]);
  }
}

inline uint32_t Hashing::combine_accumulators(const uint32_t acc1, const uint32_t acc2,
                                              const uint32_t acc3, const uint32_t acc4) {
  return ROTL(acc1, 1) + ROTL(acc2, 7) + ROTL(acc3, 12) + ROTL(acc4, 18);
}

inline void Hashing::helper_8B(uint32_t key_length, uint32_t num_keys,
                               const uint8_t* keys, uint32_t* hashes) {
  ARROW_DCHECK(key_length <= 8);
  uint64_t mask = ~0ULL >> (8 * (8 - key_length));
  constexpr uint64_t multiplier = 14029467366897019727ULL;
  uint32_t offset = 0;
  for (uint32_t ikey = 0; ikey < num_keys; ++ikey) {
    uint64_t x = *reinterpret_cast<const uint64_t*>(keys + offset);
    x &= mask;
    hashes[ikey] = static_cast<uint32_t>(BYTESWAP(x * multiplier));
    offset += key_length;
  }
}

inline void Hashing::helper_stripe(uint32_t offset, uint64_t mask_hi, const uint8_t* keys,
                                   uint32_t& acc1, uint32_t& acc2, uint32_t& acc3,
                                   uint32_t& acc4) {
  uint64_t v1 = reinterpret_cast<const uint64_t*>(keys + offset)[0];
  // We do not need to mask v1, because we will not process a stripe
  // unless at least 9 bytes of it are part of the key.
  uint64_t v2 = reinterpret_cast<const uint64_t*>(keys + offset)[1];
  v2 &= mask_hi;
  uint32_t x1 = static_cast<uint32_t>(v1);
  uint32_t x2 = static_cast<uint32_t>(v1 >> 32);
  uint32_t x3 = static_cast<uint32_t>(v2);
  uint32_t x4 = static_cast<uint32_t>(v2 >> 32);
  acc1 += x1 * PRIME32_2;
  acc1 = ROTL(acc1, 13) * PRIME32_1;
  acc2 += x2 * PRIME32_2;
  acc2 = ROTL(acc2, 13) * PRIME32_1;
  acc3 += x3 * PRIME32_2;
  acc3 = ROTL(acc3, 13) * PRIME32_1;
  acc4 += x4 * PRIME32_2;
  acc4 = ROTL(acc4, 13) * PRIME32_1;
}

void Hashing::helper_stripes(int64_t hardware_flags, uint32_t num_keys,
                             uint32_t key_length, const uint8_t* keys, uint32_t* hash) {
  uint32_t processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    int tail = num_keys % 2;
    helper_stripes_avx2(num_keys - tail, key_length, keys, hash);
    processed = num_keys - tail;
  }
#endif

  // If length modulo stripe length is less than or equal 8, round down to the nearest 16B
  // boundary (8B ending will be processed in a separate function), otherwise round up.
  const uint32_t num_stripes = (key_length + 7) / 16;
  uint64_t mask_hi =
      ~0ULL >>
      (8 * ((num_stripes * 16 > key_length) ? num_stripes * 16 - key_length : 0));

  for (uint32_t i = processed; i < num_keys; ++i) {
    uint32_t acc1, acc2, acc3, acc4;
    acc1 = static_cast<uint32_t>(
        (static_cast<uint64_t>(PRIME32_1) + static_cast<uint64_t>(PRIME32_2)) &
        0xffffffff);
    acc2 = PRIME32_2;
    acc3 = 0;
    acc4 = static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1));
    uint32_t offset = i * key_length;
    for (uint32_t stripe = 0; stripe < num_stripes - 1; ++stripe) {
      helper_stripe(offset, ~0ULL, keys, acc1, acc2, acc3, acc4);
      offset += 16;
    }
    helper_stripe(offset, mask_hi, keys, acc1, acc2, acc3, acc4);
    hash[i] = combine_accumulators(acc1, acc2, acc3, acc4);
  }
}

inline uint32_t Hashing::helper_tail(uint32_t offset, uint64_t mask, const uint8_t* keys,
                                     uint32_t acc) {
  uint64_t v = reinterpret_cast<const uint64_t*>(keys + offset)[0];
  v &= mask;
  uint32_t x1 = static_cast<uint32_t>(v);
  uint32_t x2 = static_cast<uint32_t>(v >> 32);
  acc += x1 * PRIME32_3;
  acc = ROTL(acc, 17) * PRIME32_4;
  acc += x2 * PRIME32_3;
  acc = ROTL(acc, 17) * PRIME32_4;
  return acc;
}

void Hashing::helper_tails(int64_t hardware_flags, uint32_t num_keys, uint32_t key_length,
                           const uint8_t* keys, uint32_t* hash) {
  uint32_t processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    int tail = num_keys % 8;
    helper_tails_avx2(num_keys - tail, key_length, keys, hash);
    processed = num_keys - tail;
  }
#endif
  uint64_t mask = ~0ULL >> (8 * (((key_length % 8) == 0) ? 0 : 8 - (key_length % 8)));
  uint32_t offset = key_length / 16 * 16;
  offset += processed * key_length;
  for (uint32_t i = processed; i < num_keys; ++i) {
    hash[i] = helper_tail(offset, mask, keys, hash[i]);
    offset += key_length;
  }
}

void Hashing::hash_fixed(int64_t hardware_flags, uint32_t num_keys, uint32_t length_key,
                         const uint8_t* keys, uint32_t* hashes) {
  ARROW_DCHECK(length_key > 0);

  if (length_key <= 8) {
    helper_8B(length_key, num_keys, keys, hashes);
    return;
  }
  helper_stripes(hardware_flags, num_keys, length_key, keys, hashes);
  if ((length_key % 16) > 0 && (length_key % 16) <= 8) {
    helper_tails(hardware_flags, num_keys, length_key, keys, hashes);
  }
  avalanche(hardware_flags, num_keys, hashes);
}

void Hashing::hash_varlen_helper(uint32_t length, const uint8_t* key, uint32_t* acc) {
  for (uint32_t i = 0; i < length / 16; ++i) {
    for (int j = 0; j < 4; ++j) {
      uint32_t lane = reinterpret_cast<const uint32_t*>(key)[i * 4 + j];
      acc[j] += (lane * PRIME32_2);
      acc[j] = ROTL(acc[j], 13);
      acc[j] *= PRIME32_1;
    }
  }

  int tail = length % 16;
  if (tail) {
    uint64_t last_stripe[2];
    const uint64_t* last_stripe_base =
        reinterpret_cast<const uint64_t*>(key + length - (length % 16));
    last_stripe[0] = last_stripe_base[0];
    uint64_t mask = ~0ULL >> (8 * ((length + 7) / 8 * 8 - length));
    if (tail <= 8) {
      last_stripe[1] = 0;
      last_stripe[0] &= mask;
    } else {
      last_stripe[1] = last_stripe_base[1];
      last_stripe[1] &= mask;
    }
    for (int j = 0; j < 4; ++j) {
      uint32_t lane = reinterpret_cast<const uint32_t*>(last_stripe)[j];
      acc[j] += (lane * PRIME32_2);
      acc[j] = ROTL(acc[j], 13);
      acc[j] *= PRIME32_1;
    }
  }
}

void Hashing::hash_varlen(int64_t hardware_flags, uint32_t num_rows,
                          const uint32_t* offsets, const uint8_t* concatenated_keys,
                          uint32_t* temp_buffer,  // Needs to hold 4 x 32-bit per row
                          uint32_t* hashes) {
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    hash_varlen_avx2(num_rows, offsets, concatenated_keys, temp_buffer, hashes);
  } else {
#endif
    for (uint32_t i = 0; i < num_rows; ++i) {
      uint32_t acc[4];
      acc[0] = static_cast<uint32_t>(
          (static_cast<uint64_t>(PRIME32_1) + static_cast<uint64_t>(PRIME32_2)) &
          0xffffffff);
      acc[1] = PRIME32_2;
      acc[2] = 0;
      acc[3] = static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1));
      uint32_t length = offsets[i + 1] - offsets[i];
      hash_varlen_helper(length, concatenated_keys + offsets[i], acc);
      hashes[i] = combine_accumulators(acc[0], acc[1], acc[2], acc[3]);
    }
    avalanche(hardware_flags, num_rows, hashes);
#if defined(ARROW_HAVE_AVX2)
  }
#endif
}

}  // namespace compute
}  // namespace arrow

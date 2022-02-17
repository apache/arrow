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
#include "arrow/util/ubsan.h"

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

template <typename T>
inline void Hashing::helper_8B(uint32_t key_length, uint32_t num_keys, const T* keys,
                               uint32_t* hashes) {
  ARROW_DCHECK(key_length <= 8);
  constexpr uint64_t multiplier = 14029467366897019727ULL;
  for (uint32_t ikey = 0; ikey < num_keys; ++ikey) {
    uint64_t x = static_cast<uint64_t>(keys[ikey]);
    hashes[ikey] = static_cast<uint32_t>(BYTESWAP(x * multiplier));
  }
}

inline void Hashing::helper_stripe(uint32_t offset, uint64_t mask_hi, const uint8_t* keys,
                                   uint32_t& acc1, uint32_t& acc2, uint32_t& acc3,
                                   uint32_t& acc4) {
  uint64_t v1 = util::SafeLoadAs<const uint64_t>(keys + offset);
  // We do not need to mask v1, because we will not process a stripe
  // unless at least 9 bytes of it are part of the key.
  uint64_t v2 = util::SafeLoadAs<const uint64_t>(keys + offset + 8);
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

// Process tail data in a length of 8 bytes (uint64_t)
// The caller needs to ensure that the `keys` is not less than 8 bytes (uint64_t)
inline uint32_t Hashing::helper_tail(uint32_t offset, uint64_t mask, const uint8_t* keys,
                                     uint32_t acc) {
  return helper_tail(offset, mask, keys, acc, sizeof(uint64_t));
}

// Process tail data with a specific `keys_length`
inline uint32_t Hashing::helper_tail(uint32_t offset, uint64_t mask, const uint8_t* keys,
                                     uint32_t acc, uint32_t key_length) {
  uint64_t v = 0;
  std::memcpy(&v, keys + offset, key_length);
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

  if (length_key <= 8 && ARROW_POPCOUNT64(length_key) == 1) {
    switch (length_key) {
      case 1:
        helper_8B(length_key, num_keys, keys, hashes);
        break;
      case 2:
        helper_8B(length_key, num_keys, reinterpret_cast<const uint16_t*>(keys), hashes);
        break;
      case 4:
        helper_8B(length_key, num_keys, reinterpret_cast<const uint32_t*>(keys), hashes);
        break;
      case 8:
        helper_8B(length_key, num_keys, reinterpret_cast<const uint64_t*>(keys), hashes);
        break;
      default:
        ARROW_DCHECK(false);
    }
    return;
  }
  helper_stripes(hardware_flags, num_keys, length_key, keys, hashes);
  if ((length_key % 16) > 0 && (length_key % 16) <= 8) {
    helper_tails(hardware_flags, num_keys, length_key, keys, hashes);
  }
  avalanche(hardware_flags, num_keys, hashes);
}

void Hashing::hash_varlen(int64_t hardware_flags, uint32_t num_rows,
                          const uint32_t* offsets, const uint8_t* concatenated_keys,
                          uint32_t* temp_buffer,  // Needs to hold 4 x 32-bit per row
                          uint32_t* hashes) {
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    hash_varlen_avx2(num_rows, offsets, concatenated_keys, temp_buffer, hashes);
    return;
  }
#endif
  static const uint64_t masks[9] = {0,
                                    0xffULL,
                                    0xffffULL,
                                    0xffffffULL,
                                    0xffffffffULL,
                                    0xffffffffffULL,
                                    0xffffffffffffULL,
                                    0xffffffffffffffULL,
                                    ~0ULL};

  for (uint32_t i = 0; i < num_rows; ++i) {
    uint32_t offset = offsets[i];
    uint32_t key_length = offsets[i + 1] - offsets[i];
    const uint32_t num_stripes = key_length / 16;

    uint32_t acc1, acc2, acc3, acc4;
    acc1 = static_cast<uint32_t>(
        (static_cast<uint64_t>(PRIME32_1) + static_cast<uint64_t>(PRIME32_2)) &
        0xffffffff);
    acc2 = PRIME32_2;
    acc3 = 0;
    acc4 = static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1));

    for (uint32_t stripe = 0; stripe < num_stripes; ++stripe) {
      helper_stripe(offset, ~0ULL, concatenated_keys, acc1, acc2, acc3, acc4);
      offset += 16;
    }
    uint32_t key_length_remaining = key_length - num_stripes * 16;
    if (key_length_remaining > 8) {
      helper_stripe(offset, masks[key_length_remaining - 8], concatenated_keys, acc1,
                    acc2, acc3, acc4);
      hashes[i] = combine_accumulators(acc1, acc2, acc3, acc4);
    } else if (key_length > 0) {
      uint32_t acc_combined = combine_accumulators(acc1, acc2, acc3, acc4);
      hashes[i] = helper_tail(offset, masks[key_length_remaining], concatenated_keys,
                              acc_combined, key_length_remaining);
    } else {
      hashes[i] = combine_accumulators(acc1, acc2, acc3, acc4);
    }
  }
  avalanche(hardware_flags, num_rows, hashes);
}

// From:
// https://www.boost.org/doc/libs/1_37_0/doc/html/hash/reference.html#boost.hash_combine
// template <class T>
// inline void hash_combine(std::size_t& seed, const T& v)
//{
//    std::hash<T> hasher;
//    seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
//}
void Hashing::HashCombine(KeyEncoder::KeyEncoderContext* ctx, uint32_t num_rows,
                          uint32_t* accumulated_hash, const uint32_t* next_column_hash) {
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    num_processed = HashCombine_avx2(num_rows, accumulated_hash, next_column_hash);
  }
#endif
  for (uint32_t i = num_processed; i < num_rows; ++i) {
    uint32_t acc = accumulated_hash[i];
    uint32_t next = next_column_hash[i];
    next += 0x9e3779b9 + (acc << 6) + (acc >> 2);
    acc ^= next;
    accumulated_hash[i] = acc;
  }
}

void Hashing::HashMultiColumn(const std::vector<KeyEncoder::KeyColumnArray>& cols,
                              KeyEncoder::KeyEncoderContext* ctx, uint32_t* out_hash) {
  uint32_t num_rows = static_cast<uint32_t>(cols[0].length());

  auto hash_temp_buf = util::TempVectorHolder<uint32_t>(ctx->stack, num_rows);
  auto hash_null_index_buf = util::TempVectorHolder<uint16_t>(ctx->stack, num_rows);
  auto byte_temp_buf = util::TempVectorHolder<uint8_t>(ctx->stack, num_rows);
  auto varbin_temp_buf = util::TempVectorHolder<uint32_t>(ctx->stack, 4 * num_rows);

  bool is_first = true;

  for (size_t icol = 0; icol < cols.size(); ++icol) {
    // If the col is the first one, the hash value is stored in the out_hash buffer
    // Otherwise, the hash value of current column is stored in a temp buf, and then
    // combined into the out_hash buffer
    uint32_t* dst_hash = is_first ? out_hash : hash_temp_buf.mutable_data();

    // Set the hash value as zero for a null type col
    if (cols[icol].metadata().is_null_type) {
      memset(dst_hash, 0, sizeof(uint32_t) * cols[icol].length());
    } else {
      if (cols[icol].metadata().is_fixed_length) {
        uint32_t col_width = cols[icol].metadata().fixed_length;
        if (col_width == 0) {
          util::bit_util::bits_to_bytes(ctx->hardware_flags, num_rows, cols[icol].data(1),
                                        byte_temp_buf.mutable_data(),
                                        cols[icol].bit_offset(1));
        }
        Hashing::hash_fixed(
            ctx->hardware_flags, num_rows, col_width == 0 ? 1 : col_width,
            col_width == 0 ? byte_temp_buf.mutable_data() : cols[icol].data(1), dst_hash);
      } else {
        Hashing::hash_varlen(
            ctx->hardware_flags, num_rows, cols[icol].offsets(), cols[icol].data(2),
            varbin_temp_buf.mutable_data(),  // Needs to hold 4 x 32-bit per row
            dst_hash);
      }

      // Zero hash for nulls
      if (cols[icol].data(0)) {
        int num_nulls;
        util::bit_util::bits_to_indexes(
            0, ctx->hardware_flags, num_rows, cols[icol].data(0), &num_nulls,
            hash_null_index_buf.mutable_data(), cols[icol].bit_offset(0));
        for (int i = 0; i < num_nulls; ++i) {
          uint16_t row_id = hash_null_index_buf.mutable_data()[i];
          dst_hash[row_id] = 0;
        }
      }
    }

    if (!is_first) {
      HashCombine(ctx, num_rows, out_hash, hash_temp_buf.mutable_data());
    }
    is_first = false;
  }
}

}  // namespace compute
}  // namespace arrow

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

#include <immintrin.h>

#include "arrow/compute/exec/key_hash.h"

namespace arrow {
namespace compute {

#if defined(ARROW_HAVE_AVX2)

void Hashing::avalanche_avx2(uint32_t num_keys, uint32_t* hashes) {
  constexpr int unroll = 8;
  ARROW_DCHECK(num_keys % unroll == 0);
  for (uint32_t i = 0; i < num_keys / unroll; ++i) {
    __m256i hash = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 15));
    hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_2));
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 13));
    hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_3));
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 16));
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(hashes)) + i, hash);
  }
}

inline uint64_t Hashing::combine_accumulators_avx2(__m256i acc) {
  acc = _mm256_or_si256(
      _mm256_sllv_epi32(acc, _mm256_setr_epi32(1, 7, 12, 18, 1, 7, 12, 18)),
      _mm256_srlv_epi32(acc, _mm256_setr_epi32(32 - 1, 32 - 7, 32 - 12, 32 - 18, 32 - 1,
                                               32 - 7, 32 - 12, 32 - 18)));
  acc = _mm256_add_epi32(acc, _mm256_shuffle_epi32(acc, 0xee));  // 0b11101110
  acc = _mm256_add_epi32(acc, _mm256_srli_epi64(acc, 32));
  acc = _mm256_permutevar8x32_epi32(acc, _mm256_setr_epi32(0, 4, 0, 0, 0, 0, 0, 0));
  uint64_t result = _mm256_extract_epi64(acc, 0);
  return result;
}

void Hashing::helper_stripes_avx2(uint32_t num_keys, uint32_t key_length,
                                  const uint8_t* keys, uint32_t* hash) {
  constexpr int unroll = 2;
  ARROW_DCHECK(num_keys % unroll == 0);

  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
  constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;

  const __m256i mask_last_stripe =
      (key_length % 16) <= 8
          ? _mm256_set1_epi8(static_cast<char>(0xffU))
          : _mm256_cmpgt_epi8(_mm256_set1_epi8(key_length % 16),
                              _mm256_setr_epi64x(kByteSequence0To7, kByteSequence8To15,
                                                 kByteSequence0To7, kByteSequence8To15));

  // If length modulo stripe length is less than or equal 8, round down to the nearest 16B
  // boundary (8B ending will be processed in a separate function), otherwise round up.
  const uint32_t num_stripes = (key_length + 7) / 16;
  for (uint32_t i = 0; i < num_keys / unroll; ++i) {
    __m256i acc = _mm256_setr_epi32(
        static_cast<uint32_t>((static_cast<uint64_t>(PRIME32_1) + PRIME32_2) &
                              0xffffffff),
        PRIME32_2, 0, static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1)),
        static_cast<uint32_t>((static_cast<uint64_t>(PRIME32_1) + PRIME32_2) &
                              0xffffffff),
        PRIME32_2, 0, static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1)));
    auto key0 = reinterpret_cast<const __m128i*>(keys + key_length * 2 * i);
    auto key1 = reinterpret_cast<const __m128i*>(keys + key_length * 2 * i + key_length);
    for (uint32_t stripe = 0; stripe < num_stripes - 1; ++stripe) {
      auto key_stripe =
          _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128(key0 + stripe)),
                                  _mm_loadu_si128(key1 + stripe), 1);
      acc = _mm256_add_epi32(
          acc, _mm256_mullo_epi32(key_stripe, _mm256_set1_epi32(PRIME32_2)));
      acc = _mm256_or_si256(_mm256_slli_epi32(acc, 13), _mm256_srli_epi32(acc, 32 - 13));
      acc = _mm256_mullo_epi32(acc, _mm256_set1_epi32(PRIME32_1));
    }
    auto key_stripe = _mm256_inserti128_si256(
        _mm256_castsi128_si256(_mm_loadu_si128(key0 + num_stripes - 1)),
        _mm_loadu_si128(key1 + num_stripes - 1), 1);
    key_stripe = _mm256_and_si256(key_stripe, mask_last_stripe);
    acc = _mm256_add_epi32(acc,
                           _mm256_mullo_epi32(key_stripe, _mm256_set1_epi32(PRIME32_2)));
    acc = _mm256_or_si256(_mm256_slli_epi32(acc, 13), _mm256_srli_epi32(acc, 32 - 13));
    acc = _mm256_mullo_epi32(acc, _mm256_set1_epi32(PRIME32_1));
    uint64_t result = combine_accumulators_avx2(acc);
    reinterpret_cast<uint64_t*>(hash)[i] = result;
  }
}

void Hashing::helper_tails_avx2(uint32_t num_keys, uint32_t key_length,
                                const uint8_t* keys, uint32_t* hash) {
  constexpr int unroll = 8;
  ARROW_DCHECK(num_keys % unroll == 0);
  auto keys_i64 = reinterpret_cast<arrow::util::int64_for_gather_t*>(keys);

  // Process between 1 and 8 last bytes of each key, starting from 16B boundary.
  // The caller needs to make sure that there are no more than 8 bytes to process after
  // that 16B boundary.
  uint32_t first_offset = key_length - (key_length % 16);
  __m256i mask = _mm256_set1_epi64x((~0ULL) >> (8 * (8 - (key_length % 16))));
  __m256i offset =
      _mm256_setr_epi32(0, key_length, key_length * 2, key_length * 3, key_length * 4,
                        key_length * 5, key_length * 6, key_length * 7);
  offset = _mm256_add_epi32(offset, _mm256_set1_epi32(first_offset));
  __m256i offset_incr = _mm256_set1_epi32(key_length * 8);

  for (uint32_t i = 0; i < num_keys / unroll; ++i) {
    auto v1 = _mm256_i32gather_epi64(keys_i64, _mm256_castsi256_si128(offset), 1);
    auto v2 = _mm256_i32gather_epi64(keys_i64, _mm256_extracti128_si256(offset, 1), 1);
    v1 = _mm256_and_si256(v1, mask);
    v2 = _mm256_and_si256(v2, mask);
    v1 = _mm256_permutevar8x32_epi32(v1, _mm256_setr_epi32(0, 2, 4, 6, 1, 3, 5, 7));
    v2 = _mm256_permutevar8x32_epi32(v2, _mm256_setr_epi32(0, 2, 4, 6, 1, 3, 5, 7));
    auto x1 = _mm256_permute2x128_si256(v1, v2, 0x20);
    auto x2 = _mm256_permute2x128_si256(v1, v2, 0x31);
    __m256i acc = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(hash)) + i);

    acc = _mm256_add_epi32(acc, _mm256_mullo_epi32(x1, _mm256_set1_epi32(PRIME32_3)));
    acc = _mm256_or_si256(_mm256_slli_epi32(acc, 17), _mm256_srli_epi32(acc, 32 - 17));
    acc = _mm256_mullo_epi32(acc, _mm256_set1_epi32(PRIME32_4));

    acc = _mm256_add_epi32(acc, _mm256_mullo_epi32(x2, _mm256_set1_epi32(PRIME32_3)));
    acc = _mm256_or_si256(_mm256_slli_epi32(acc, 17), _mm256_srli_epi32(acc, 32 - 17));
    acc = _mm256_mullo_epi32(acc, _mm256_set1_epi32(PRIME32_4));

    _mm256_storeu_si256((reinterpret_cast<__m256i*>(hash)) + i, acc);

    offset = _mm256_add_epi32(offset, offset_incr);
  }
}

void Hashing::hash_varlen_avx2(uint32_t num_rows, const uint32_t* offsets,
                               const uint8_t* concatenated_keys,
                               uint32_t* temp_buffer,  // Needs to hold 4 x 32-bit per row
                               uint32_t* hashes) {
  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
  constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;

  const __m128i sequence = _mm_set_epi64x(kByteSequence8To15, kByteSequence0To7);
  const __m128i acc_init = _mm_setr_epi32(
      static_cast<uint32_t>((static_cast<uint64_t>(PRIME32_1) + PRIME32_2) & 0xffffffff),
      PRIME32_2, 0, static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1)));

  // Variable length keys are always processed as a sequence of 16B stripes,
  // with the last stripe, if extending past the end of the key, having extra bytes set to
  // 0 on the fly.
  for (uint32_t ikey = 0; ikey < num_rows; ++ikey) {
    uint32_t begin = offsets[ikey];
    uint32_t end = offsets[ikey + 1];
    uint32_t length = end - begin;
    const uint8_t* base = concatenated_keys + begin;

    __m128i acc = acc_init;

    if (length) {
      uint32_t i;
      for (i = 0; i < (length - 1) / 16; ++i) {
        __m128i key_stripe = _mm_loadu_si128(reinterpret_cast<const __m128i*>(base) + i);
        acc = _mm_add_epi32(acc, _mm_mullo_epi32(key_stripe, _mm_set1_epi32(PRIME32_2)));
        acc = _mm_or_si128(_mm_slli_epi32(acc, 13), _mm_srli_epi32(acc, 32 - 13));
        acc = _mm_mullo_epi32(acc, _mm_set1_epi32(PRIME32_1));
      }
      __m128i key_stripe = _mm_loadu_si128(reinterpret_cast<const __m128i*>(base) + i);
      __m128i mask = _mm_cmpgt_epi8(_mm_set1_epi8(((length - 1) % 16) + 1), sequence);
      key_stripe = _mm_and_si128(key_stripe, mask);
      acc = _mm_add_epi32(acc, _mm_mullo_epi32(key_stripe, _mm_set1_epi32(PRIME32_2)));
      acc = _mm_or_si128(_mm_slli_epi32(acc, 13), _mm_srli_epi32(acc, 32 - 13));
      acc = _mm_mullo_epi32(acc, _mm_set1_epi32(PRIME32_1));
    }

    _mm_storeu_si128(reinterpret_cast<__m128i*>(temp_buffer) + ikey, acc);
  }

  // Combine accumulators and perform avalanche
  constexpr int unroll = 8;
  for (uint32_t i = 0; i < num_rows / unroll; ++i) {
    __m256i accA =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(temp_buffer) + 4 * i + 0);
    __m256i accB =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(temp_buffer) + 4 * i + 1);
    __m256i accC =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(temp_buffer) + 4 * i + 2);
    __m256i accD =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(temp_buffer) + 4 * i + 3);
    // Transpose 2x 4x4 32-bit matrices
    __m256i r0 = _mm256_unpacklo_epi32(accA, accB);
    __m256i r1 = _mm256_unpackhi_epi32(accA, accB);
    __m256i r2 = _mm256_unpacklo_epi32(accC, accD);
    __m256i r3 = _mm256_unpackhi_epi32(accC, accD);
    accA = _mm256_unpacklo_epi64(r0, r2);
    accB = _mm256_unpackhi_epi64(r0, r2);
    accC = _mm256_unpacklo_epi64(r1, r3);
    accD = _mm256_unpackhi_epi64(r1, r3);
    // _rotl(accA, 1)
    // _rotl(accB, 7)
    // _rotl(accC, 12)
    // _rotl(accD, 18)
    accA = _mm256_or_si256(_mm256_slli_epi32(accA, 1), _mm256_srli_epi32(accA, 32 - 1));
    accB = _mm256_or_si256(_mm256_slli_epi32(accB, 7), _mm256_srli_epi32(accB, 32 - 7));
    accC = _mm256_or_si256(_mm256_slli_epi32(accC, 12), _mm256_srli_epi32(accC, 32 - 12));
    accD = _mm256_or_si256(_mm256_slli_epi32(accD, 18), _mm256_srli_epi32(accD, 32 - 18));
    accA = _mm256_add_epi32(_mm256_add_epi32(accA, accB), _mm256_add_epi32(accC, accD));
    // avalanche
    __m256i hash = accA;
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 15));
    hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_2));
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 13));
    hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_3));
    hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 16));
    // Store.
    // At this point, because of way 2x 4x4 transposition was done, output hashes are in
    // order: 0, 2, 4, 6, 1, 3, 5, 7. Bring back the original order.
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(hashes) + i,
        _mm256_permutevar8x32_epi32(hash, _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7)));
  }
  // Process the tail of up to 7 hashes
  for (uint32_t i = num_rows - num_rows % unroll; i < num_rows; ++i) {
    uint32_t* temp_buffer_base = temp_buffer + i * 4;
    uint32_t acc = ROTL(temp_buffer_base[0], 1) + ROTL(temp_buffer_base[1], 7) +
                   ROTL(temp_buffer_base[2], 12) + ROTL(temp_buffer_base[3], 18);

    // avalanche
    acc ^= (acc >> 15);
    acc *= PRIME32_2;
    acc ^= (acc >> 13);
    acc *= PRIME32_3;
    acc ^= (acc >> 16);

    hashes[i] = acc;
  }
}

uint32_t Hashing::HashCombine_avx2(uint32_t num_rows, uint32_t* accumulated_hash,
                                   const uint32_t* next_column_hash) {
  constexpr uint32_t unroll = 8;
  for (uint32_t i = 0; i < num_rows / unroll; ++i) {
    __m256i acc =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(accumulated_hash) + i);
    __m256i next =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(next_column_hash) + i);
    next = _mm256_add_epi32(next, _mm256_set1_epi32(0x9e3779b9));
    next = _mm256_add_epi32(next, _mm256_slli_epi32(acc, 6));
    next = _mm256_add_epi32(next, _mm256_srli_epi32(acc, 2));
    acc = _mm256_xor_si256(acc, next);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(accumulated_hash) + i, acc);
  }
  uint32_t num_processed = num_rows / unroll * unroll;
  return num_processed;
}

#endif

}  // namespace compute
}  // namespace arrow

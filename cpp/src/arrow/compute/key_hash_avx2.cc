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

#include "arrow/compute/key_hash.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

#if defined(ARROW_HAVE_AVX2)

inline __m256i Hashing32::Avalanche_avx2(__m256i hash) {
  hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 15));
  hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_2));
  hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 13));
  hash = _mm256_mullo_epi32(hash, _mm256_set1_epi32(PRIME32_3));
  hash = _mm256_xor_si256(hash, _mm256_srli_epi32(hash, 16));
  return hash;
}

inline __m256i Hashing32::CombineHashesImp_avx2(__m256i previous_hash, __m256i hash) {
  // previous_hash ^= acc + kCombineConst + (previous_hash << 6) +
  // (previous_hash >> 2);
  //
  __m256i x = _mm256_add_epi32(_mm256_slli_epi32(previous_hash, 6),
                               _mm256_srli_epi32(previous_hash, 2));
  __m256i y = _mm256_add_epi32(hash, _mm256_set1_epi32(kCombineConst));
  __m256i new_hash = _mm256_xor_si256(previous_hash, _mm256_add_epi32(x, y));
  return new_hash;
}

template <bool T_COMBINE_HASHES>
void Hashing32::AvalancheAll_avx2(uint32_t num_rows_to_process, uint32_t* hashes,
                                  const uint32_t* hashes_temp_for_combine) {
  constexpr int unroll = 8;
  for (uint32_t i = 0; i < num_rows_to_process / unroll; ++i) {
    __m256i acc;
    if (T_COMBINE_HASHES) {
      acc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes_temp_for_combine) +
                               i);
    } else {
      acc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
    }
    acc = Avalanche_avx2(acc);
    if (T_COMBINE_HASHES) {
      __m256i previous_hash =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
      acc = CombineHashesImp_avx2(previous_hash, acc);
    }
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(hashes) + i, acc);
  }
  for (uint32_t i = num_rows_to_process - (num_rows_to_process % unroll);
       i < num_rows_to_process; ++i) {
    if (T_COMBINE_HASHES) {
      hashes[i] = CombineHashesImp(hashes[i], Avalanche(hashes_temp_for_combine[i]));
    } else {
      hashes[i] = Avalanche(hashes[i]);
    }
  }
}

inline __m256i Hashing32::Round_avx2(__m256i acc, __m256i input) {
  acc = _mm256_add_epi32(acc, _mm256_mullo_epi32(input, _mm256_set1_epi32(PRIME32_2)));
  acc = _mm256_or_si256(_mm256_slli_epi32(acc, 13), _mm256_srli_epi32(acc, 32 - 13));
  acc = _mm256_mullo_epi32(acc, _mm256_set1_epi32(PRIME32_1));
  return acc;
}

inline uint64_t Hashing32::CombineAccumulators_avx2(__m256i acc) {
  // Each 128-bit lane of input represents a set of 4 accumulators related to
  // a single hash (we process here two hashes together).
  //
  __m256i rotate_const_left = _mm256_setr_epi32(1, 7, 12, 18, 1, 7, 12, 18);
  __m256i rotate_const_right = _mm256_setr_epi32(32 - 1, 32 - 7, 32 - 12, 32 - 18, 32 - 1,
                                                 32 - 7, 32 - 12, 32 - 18);

  acc = _mm256_or_si256(_mm256_sllv_epi32(acc, rotate_const_left),
                        _mm256_srlv_epi32(acc, rotate_const_right));
  acc = _mm256_add_epi32(acc, _mm256_shuffle_epi32(acc, 0xee));  // 0b11101110
  acc = _mm256_add_epi32(acc, _mm256_srli_epi64(acc, 32));
  acc = _mm256_permutevar8x32_epi32(acc, _mm256_setr_epi32(0, 4, 0, 0, 0, 0, 0, 0));
  uint64_t result = _mm256_extract_epi64(acc, 0);
  return result;
}

inline __m256i Hashing32::StripeMask_avx2(int i, int j) {
  // Return two 16 byte masks, where the first i/j bytes are 0xff and the
  // remaining ones are 0x00
  //
  ARROW_DCHECK(i >= 0 && i <= kStripeSize && j >= 0 && j <= kStripeSize);
  return _mm256_cmpgt_epi8(
      _mm256_blend_epi32(_mm256_set1_epi8(i), _mm256_set1_epi8(j), 0xf0),
      _mm256_setr_epi64x(0x0706050403020100ULL, 0x0f0e0d0c0b0a0908ULL,
                         0x0706050403020100ULL, 0x0f0e0d0c0b0a0908ULL));
}

template <bool two_equal_lengths>
inline __m256i Hashing32::ProcessStripes_avx2(int64_t num_stripes_A,
                                              int64_t num_stripes_B,
                                              __m256i mask_last_stripe,
                                              const uint8_t* keys, int64_t offset_A,
                                              int64_t offset_B) {
  ARROW_DCHECK(num_stripes_A > 0 && num_stripes_B > 0);

  __m256i acc = _mm256_setr_epi32(
      static_cast<uint32_t>((static_cast<uint64_t>(PRIME32_1) + PRIME32_2) & 0xffffffff),
      PRIME32_2, 0, static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1)),
      static_cast<uint32_t>((static_cast<uint64_t>(PRIME32_1) + PRIME32_2) & 0xffffffff),
      PRIME32_2, 0, static_cast<uint32_t>(-static_cast<int32_t>(PRIME32_1)));

  // Constant for permutexvar8x32 instruction that conditionally swaps two
  // 128-bit lanes if and only if num_stripes_B > num_stripes_A.
  //
  __m256i swap_permute = _mm256_setzero_si256();
  int64_t offset_shorter, offset_longer;
  int64_t num_stripes_shorter, num_stripes_longer;

  if (!two_equal_lengths) {
    int64_t swap_mask = num_stripes_B > num_stripes_A ? ~0LL : 0LL;
    swap_permute = _mm256_xor_si256(_mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7),
                                    _mm256_set1_epi32(swap_mask & 4));
    offset_shorter = (offset_A & swap_mask) | (offset_B & ~swap_mask);
    offset_longer = (offset_A & ~swap_mask) | (offset_B & swap_mask);
    num_stripes_shorter = (num_stripes_A & swap_mask) | (num_stripes_B & ~swap_mask);
    num_stripes_longer = (num_stripes_A & ~swap_mask) | (num_stripes_B & swap_mask);
  } else {
    ARROW_DCHECK(num_stripes_A == num_stripes_B);
    offset_longer = offset_A;
    offset_shorter = offset_B;
    num_stripes_longer = num_stripes_A;
    num_stripes_shorter = num_stripes_A;
  }

  int64_t istripe = 0;
  for (; istripe + 1 < num_stripes_shorter; ++istripe) {
    __m256i stripe = _mm256_inserti128_si256(
        _mm256_castsi128_si256(_mm_loadu_si128(
            reinterpret_cast<const __m128i*>(keys + offset_longer) + istripe)),
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(keys + offset_shorter) +
                        istripe),
        1);
    acc = Round_avx2(acc, stripe);
  }
  __m256i stripe = _mm256_inserti128_si256(
      _mm256_castsi128_si256(_mm_loadu_si128(
          reinterpret_cast<const __m128i*>(keys + offset_longer) + istripe)),
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(keys + offset_shorter) + istripe),
      1);
  if (!two_equal_lengths) {
    __m256i acc_copy = acc;
    for (; istripe + 1 < num_stripes_longer; ++istripe) {
      acc = Round_avx2(acc, stripe);
      stripe = _mm256_inserti128_si256(
          stripe,
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(keys + offset_longer) +
                          istripe + 1),
          0);
    }
    acc = _mm256_blend_epi32(acc, acc_copy, 0xf0);
    mask_last_stripe = _mm256_permutevar8x32_epi32(mask_last_stripe, swap_permute);
  }
  stripe = _mm256_and_si256(stripe, mask_last_stripe);
  acc = Round_avx2(acc, stripe);
  if (!two_equal_lengths) {
    acc = _mm256_permutevar8x32_epi32(acc, swap_permute);
  }
  return acc;
}

template <bool T_COMBINE_HASHES>
uint32_t Hashing32::HashFixedLenImp_avx2(uint32_t num_rows, uint64_t length,
                                         const uint8_t* keys, uint32_t* hashes,
                                         uint32_t* hashes_temp_for_combine) {
  constexpr int unroll = 2;

  // Do not process rows that could read past the end of the buffer using 16
  // byte loads. Round down number of rows to process to multiple of 2.
  //
  uint64_t num_rows_to_skip = bit_util::CeilDiv(length, kStripeSize);
  uint32_t num_rows_to_process =
      (num_rows_to_skip > num_rows)
          ? 0
          : (num_rows - static_cast<uint32_t>(num_rows_to_skip));
  num_rows_to_process -= (num_rows_to_process % unroll);

  uint64_t num_stripes = bit_util::CeilDiv(length, kStripeSize);
  int num_tail_bytes = ((length - 1) & (kStripeSize - 1)) + 1;
  __m256i mask_last_stripe = StripeMask_avx2(num_tail_bytes, num_tail_bytes);

  for (uint32_t i = 0; i < num_rows_to_process / unroll; ++i) {
    __m256i acc = ProcessStripes_avx2</*two_equal_lengths=*/true>(
        num_stripes, num_stripes, mask_last_stripe, keys,
        static_cast<int64_t>(i) * unroll * length,
        static_cast<int64_t>(i) * unroll * length + length);

    if (T_COMBINE_HASHES) {
      reinterpret_cast<uint64_t*>(hashes_temp_for_combine)[i] =
          CombineAccumulators_avx2(acc);
    } else {
      reinterpret_cast<uint64_t*>(hashes)[i] = CombineAccumulators_avx2(acc);
    }
  }

  AvalancheAll_avx2<T_COMBINE_HASHES>(num_rows_to_process, hashes,
                                      hashes_temp_for_combine);

  return num_rows_to_process;
}

uint32_t Hashing32::HashFixedLen_avx2(bool combine_hashes, uint32_t num_rows,
                                      uint64_t length, const uint8_t* keys,
                                      uint32_t* hashes,
                                      uint32_t* hashes_temp_for_combine) {
  if (combine_hashes) {
    return HashFixedLenImp_avx2<true>(num_rows, length, keys, hashes,
                                      hashes_temp_for_combine);
  } else {
    return HashFixedLenImp_avx2<false>(num_rows, length, keys, hashes,
                                       hashes_temp_for_combine);
  }
}

template <typename T, bool T_COMBINE_HASHES>
uint32_t Hashing32::HashVarLenImp_avx2(uint32_t num_rows, const T* offsets,
                                       const uint8_t* concatenated_keys, uint32_t* hashes,
                                       uint32_t* hashes_temp_for_combine) {
  constexpr int unroll = 2;

  // Do not process rows that could read past the end of the buffer using 16
  // byte loads. Round down number of rows to process to multiple of 2.
  //
  uint32_t num_rows_to_process = num_rows;
  while (num_rows_to_process > 0 &&
         offsets[num_rows_to_process] + kStripeSize > offsets[num_rows]) {
    --num_rows_to_process;
  }
  num_rows_to_process -= (num_rows_to_process % unroll);

  for (uint32_t i = 0; i < num_rows_to_process / unroll; ++i) {
    T offset_A = offsets[unroll * i + 0];
    T offset_B = offsets[unroll * i + 1];
    T offset_end = offsets[unroll * i + 2];

    T length = offset_B - offset_A;
    int is_non_empty = length == 0 ? 0 : 1;
    int64_t num_stripes_A =
        static_cast<int64_t>(bit_util::CeilDiv(length, kStripeSize)) + (1 - is_non_empty);
    int num_tail_bytes_A = ((length - is_non_empty) & (kStripeSize - 1)) + is_non_empty;

    length = offset_end - offset_B;
    is_non_empty = length == 0 ? 0 : 1;
    int64_t num_stripes_B =
        static_cast<int64_t>(bit_util::CeilDiv(length, kStripeSize)) + (1 - is_non_empty);
    int num_tail_bytes_B = ((length - is_non_empty) & (kStripeSize - 1)) + is_non_empty;

    __m256i mask_last_stripe = StripeMask_avx2(num_tail_bytes_A, num_tail_bytes_B);

    __m256i acc = ProcessStripes_avx2</*two_equal_lengths=*/false>(
        num_stripes_A, num_stripes_B, mask_last_stripe, concatenated_keys,
        static_cast<int64_t>(offset_A), static_cast<int64_t>(offset_B));

    if (T_COMBINE_HASHES) {
      reinterpret_cast<uint64_t*>(hashes_temp_for_combine)[i] =
          CombineAccumulators_avx2(acc);
    } else {
      reinterpret_cast<uint64_t*>(hashes)[i] = CombineAccumulators_avx2(acc);
    }
  }

  AvalancheAll_avx2<T_COMBINE_HASHES>(num_rows_to_process, hashes,
                                      hashes_temp_for_combine);

  return num_rows_to_process;
}

uint32_t Hashing32::HashVarLen_avx2(bool combine_hashes, uint32_t num_rows,
                                    const uint32_t* offsets,
                                    const uint8_t* concatenated_keys, uint32_t* hashes,
                                    uint32_t* hashes_temp_for_combine) {
  if (combine_hashes) {
    return HashVarLenImp_avx2<uint32_t, true>(num_rows, offsets, concatenated_keys,
                                              hashes, hashes_temp_for_combine);
  } else {
    return HashVarLenImp_avx2<uint32_t, false>(num_rows, offsets, concatenated_keys,
                                               hashes, hashes_temp_for_combine);
  }
}

uint32_t Hashing32::HashVarLen_avx2(bool combine_hashes, uint32_t num_rows,
                                    const uint64_t* offsets,
                                    const uint8_t* concatenated_keys, uint32_t* hashes,
                                    uint32_t* hashes_temp_for_combine) {
  if (combine_hashes) {
    return HashVarLenImp_avx2<uint64_t, true>(num_rows, offsets, concatenated_keys,
                                              hashes, hashes_temp_for_combine);
  } else {
    return HashVarLenImp_avx2<uint64_t, false>(num_rows, offsets, concatenated_keys,
                                               hashes, hashes_temp_for_combine);
  }
}

#endif

}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/exec/key_compare.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

#if defined(ARROW_HAVE_AVX2)

uint32_t KeyCompare::CompareFixedLength_UpTo8B_avx2(
    uint32_t num_rows, const uint32_t* left_to_right_map, uint8_t* match_bytevector,
    uint32_t length, const uint8_t* rows_left, const uint8_t* rows_right) {
  ARROW_DCHECK(length <= 8);
  __m256i offset_left = _mm256_setr_epi64x(0, length, length * 2, length * 3);
  __m256i offset_left_incr = _mm256_set1_epi64x(length * 4);
  __m256i mask = _mm256_set1_epi64x(~0ULL >> (8 * (8 - length)));

  constexpr uint32_t unroll = 4;
  for (uint32_t i = 0; i < num_rows / unroll; ++i) {
    auto key_left = _mm256_i64gather_epi64(
        reinterpret_cast<arrow::util::int64_for_gather_t*>(rows_left), offset_left, 1);
    offset_left = _mm256_add_epi64(offset_left, offset_left_incr);
    __m128i offset_right =
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(left_to_right_map) + i);
    offset_right = _mm_mullo_epi32(offset_right, _mm_set1_epi32(length));

    auto key_right = _mm256_i32gather_epi64(
        reinterpret_cast<arrow::util::int64_for_gather_t*>(rows_right), offset_right, 1);
    uint32_t cmp = _mm256_movemask_epi8(_mm256_cmpeq_epi64(
        _mm256_and_si256(key_left, mask), _mm256_and_si256(key_right, mask)));
    reinterpret_cast<uint32_t*>(match_bytevector)[i] &= cmp;
  }

  uint32_t num_rows_processed = num_rows - (num_rows % unroll);
  return num_rows_processed;
}

uint32_t KeyCompare::CompareFixedLength_UpTo16B_avx2(
    uint32_t num_rows, const uint32_t* left_to_right_map, uint8_t* match_bytevector,
    uint32_t length, const uint8_t* rows_left, const uint8_t* rows_right) {
  ARROW_DCHECK(length <= 16);

  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
  constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;

  __m256i mask =
      _mm256_cmpgt_epi8(_mm256_set1_epi8(length),
                        _mm256_setr_epi64x(kByteSequence0To7, kByteSequence8To15,
                                           kByteSequence0To7, kByteSequence8To15));
  const uint8_t* key_left_ptr = rows_left;

  constexpr uint32_t unroll = 2;
  for (uint32_t i = 0; i < num_rows / unroll; ++i) {
    auto key_left = _mm256_inserti128_si256(
        _mm256_castsi128_si256(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(key_left_ptr))),
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(key_left_ptr + length)), 1);
    key_left_ptr += length * 2;
    auto key_right = _mm256_inserti128_si256(
        _mm256_castsi128_si256(_mm_loadu_si128(reinterpret_cast<const __m128i*>(
            rows_right + length * left_to_right_map[2 * i]))),
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(
            rows_right + length * left_to_right_map[2 * i + 1])),
        1);
    __m256i cmp = _mm256_cmpeq_epi64(_mm256_and_si256(key_left, mask),
                                     _mm256_and_si256(key_right, mask));
    cmp = _mm256_and_si256(cmp, _mm256_shuffle_epi32(cmp, 0xee));  // 0b11101110
    cmp = _mm256_permute4x64_epi64(cmp, 0x08);                     // 0b00001000
    reinterpret_cast<uint16_t*>(match_bytevector)[i] &=
        (_mm256_movemask_epi8(cmp) & 0xffff);
  }

  uint32_t num_rows_processed = num_rows - (num_rows % unroll);
  return num_rows_processed;
}

uint32_t KeyCompare::CompareFixedLength_avx2(uint32_t num_rows,
                                             const uint32_t* left_to_right_map,
                                             uint8_t* match_bytevector, uint32_t length,
                                             const uint8_t* rows_left,
                                             const uint8_t* rows_right) {
  ARROW_DCHECK(length > 0);

  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
  constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;
  constexpr uint64_t kByteSequence16To23 = 0x1716151413121110ULL;
  constexpr uint64_t kByteSequence24To31 = 0x1f1e1d1c1b1a1918ULL;

  // Non-zero length guarantees no underflow
  int32_t num_loops_less_one = (static_cast<int32_t>(length) + 31) / 32 - 1;

  __m256i tail_mask =
      _mm256_cmpgt_epi8(_mm256_set1_epi8(length - num_loops_less_one * 32),
                        _mm256_setr_epi64x(kByteSequence0To7, kByteSequence8To15,
                                           kByteSequence16To23, kByteSequence24To31));

  for (uint32_t irow_left = 0; irow_left < num_rows; ++irow_left) {
    uint32_t irow_right = left_to_right_map[irow_left];
    uint32_t begin_left = length * irow_left;
    uint32_t begin_right = length * irow_right;
    const __m256i* key_left_ptr =
        reinterpret_cast<const __m256i*>(rows_left + begin_left);
    const __m256i* key_right_ptr =
        reinterpret_cast<const __m256i*>(rows_right + begin_right);
    __m256i result_or = _mm256_setzero_si256();
    int32_t i;
    // length cannot be zero
    for (i = 0; i < num_loops_less_one; ++i) {
      __m256i key_left = _mm256_loadu_si256(key_left_ptr + i);
      __m256i key_right = _mm256_loadu_si256(key_right_ptr + i);
      result_or = _mm256_or_si256(result_or, _mm256_xor_si256(key_left, key_right));
    }

    __m256i key_left = _mm256_loadu_si256(key_left_ptr + i);
    __m256i key_right = _mm256_loadu_si256(key_right_ptr + i);
    result_or = _mm256_or_si256(
        result_or, _mm256_and_si256(tail_mask, _mm256_xor_si256(key_left, key_right)));
    int result = _mm256_testz_si256(result_or, result_or) * 0xff;
    match_bytevector[irow_left] &= result;
  }

  uint32_t num_rows_processed = num_rows;
  return num_rows_processed;
}

void KeyCompare::CompareVaryingLength_avx2(
    uint32_t num_rows, const uint32_t* left_to_right_map, uint8_t* match_bytevector,
    const uint8_t* rows_left, const uint8_t* rows_right, const uint32_t* offsets_left,
    const uint32_t* offsets_right) {
  for (uint32_t irow_left = 0; irow_left < num_rows; ++irow_left) {
    uint32_t irow_right = left_to_right_map[irow_left];
    uint32_t begin_left = offsets_left[irow_left];
    uint32_t begin_right = offsets_right[irow_right];
    uint32_t length_left = offsets_left[irow_left + 1] - begin_left;
    uint32_t length_right = offsets_right[irow_right + 1] - begin_right;
    uint32_t length = std::min(length_left, length_right);
    auto key_left_ptr = reinterpret_cast<const __m256i*>(rows_left + begin_left);
    auto key_right_ptr = reinterpret_cast<const __m256i*>(rows_right + begin_right);
    __m256i result_or = _mm256_setzero_si256();
    int32_t i;
    // length can be zero
    for (i = 0; i < (static_cast<int32_t>(length) + 31) / 32 - 1; ++i) {
      __m256i key_left = _mm256_loadu_si256(key_left_ptr + i);
      __m256i key_right = _mm256_loadu_si256(key_right_ptr + i);
      result_or = _mm256_or_si256(result_or, _mm256_xor_si256(key_left, key_right));
    }

    constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
    constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;
    constexpr uint64_t kByteSequence16To23 = 0x1716151413121110ULL;
    constexpr uint64_t kByteSequence24To31 = 0x1f1e1d1c1b1a1918ULL;

    __m256i tail_mask =
        _mm256_cmpgt_epi8(_mm256_set1_epi8(length - i * 32),
                          _mm256_setr_epi64x(kByteSequence0To7, kByteSequence8To15,
                                             kByteSequence16To23, kByteSequence24To31));

    __m256i key_left = _mm256_loadu_si256(key_left_ptr + i);
    __m256i key_right = _mm256_loadu_si256(key_right_ptr + i);
    result_or = _mm256_or_si256(
        result_or, _mm256_and_si256(tail_mask, _mm256_xor_si256(key_left, key_right)));
    int result = _mm256_testz_si256(result_or, result_or) * 0xff;
    match_bytevector[irow_left] &= result;
  }
}

#endif

}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/row/row_internal.h"
#include "arrow/util/simd.h"

#if !defined(ARROW_HAVE_AVX2) && !defined(ARROW_HAVE_AVX512) && \
    !defined(ARROW_HAVE_RUNTIME_AVX2) && !defined(ARROW_HAVE_RUNTIME_AVX512)
#  error "This file should only be included when AVX2 or AVX512 is enabled"
#endif

namespace arrow::compute {

// Convert 8 64-bit comparision results, each being 0 or -1, to 8 bytes.
inline uint64_t Cmp64To8(__m256i cmp64_lo, __m256i cmp64_hi) {
  uint32_t cmp_lo = _mm256_movemask_epi8(cmp64_lo);
  uint32_t cmp_hi = _mm256_movemask_epi8(cmp64_hi);
  return cmp_lo | (static_cast<uint64_t>(cmp_hi) << 32);
}

// Convert 8 32-bit comparision results, each being 0 or -1, to 8 bytes.
inline uint64_t Cmp32To8(__m256i cmp32) {
  return Cmp64To8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(cmp32)),
                  _mm256_cvtepi32_epi64(_mm256_extracti128_si256(cmp32, 1)));
}

// Get null bits for 8 32-bit row ids in `row_id32` at `col_pos` as a vector of 32-bit
// integers. Note that the result integer is 0 if the corresponding column is not null, or
// 1 otherwise.
inline __m256i GetNullBitInt32(const RowTableImpl& rows, uint32_t col_pos,
                               __m256i row_id32) {
  const uint8_t* null_masks = rows.null_masks(/*row_id=*/0);
  __m256i null_mask_num_bits =
      _mm256_set1_epi64x(rows.metadata().null_masks_bytes_per_row * 8);
  __m256i row_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(row_id32));
  __m256i row_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(row_id32, 1));
  __m256i bit_id_lo = _mm256_mul_epi32(row_lo, null_mask_num_bits);
  __m256i bit_id_hi = _mm256_mul_epi32(row_hi, null_mask_num_bits);
  bit_id_lo = _mm256_add_epi64(bit_id_lo, _mm256_set1_epi64x(col_pos));
  bit_id_hi = _mm256_add_epi64(bit_id_hi, _mm256_set1_epi64x(col_pos));
  __m128i right_lo = _mm256_i64gather_epi32(reinterpret_cast<const int*>(null_masks),
                                            _mm256_srli_epi64(bit_id_lo, 3), 1);
  __m128i right_hi = _mm256_i64gather_epi32(reinterpret_cast<const int*>(null_masks),
                                            _mm256_srli_epi64(bit_id_hi, 3), 1);
  __m256i right = _mm256_set_m128i(right_hi, right_lo);
  return _mm256_and_si256(_mm256_set1_epi32(1), _mm256_srli_epi32(right, col_pos & 7));
}

}  // namespace arrow::compute

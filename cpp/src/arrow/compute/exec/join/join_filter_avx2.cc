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

#include "arrow/compute/exec/join/join_filter.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

template <typename hash_type>
void ApproximateMembershipTest::MayHaveHash_imp_avx2(int64_t num_rows,
                                                     const hash_type* hashes,
                                                     uint8_t* result) const {
  constexpr int unroll = 4;
  for (int64_t i = 0; i < num_rows / unroll; ++i) {
    __m256i hash;
    if (sizeof(hash_type) == sizeof(uint64_t)) {
      hash = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes) + i);
    } else {
      DCHECK(sizeof(hash_type) == sizeof(uint32_t));
      hash = _mm256_cvtepu32_epi64(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(hashes)));
    }
    __m256i bit_offset0 =
        _mm256_and_si256(hash, _mm256_set1_epi64x(BitMasksGenerator::num_masks_ - 1));
    __m256i mask =
        _mm256_i64gather_epi64(reinterpret_cast<const long long*>(bit_masks_.masks_),
                               _mm256_srli_epi64(bit_offset0, 3), 1);
    mask = _mm256_srlv_epi64(mask, _mm256_and_si256(bit_offset0, _mm256_set1_epi64x(7)));
    mask = _mm256_and_si256(
        mask, _mm256_set1_epi64x((1ULL << BitMasksGenerator::bit_width_) - 1));
    __m256i bit_offset1 =
        _mm256_and_si256(_mm256_srli_epi64(hash, BitMasksGenerator::log_num_masks_),
                         _mm256_set1_epi64x(hash_mask_num_bits_));
    mask = _mm256_sllv_epi64(mask, _mm256_and_si256(bit_offset1, _mm256_set1_epi64x(7)));
    __m256i byte_offset = _mm256_srli_epi64(bit_offset1, 3);
    __m256i word = _mm256_i64gather_epi64(
        reinterpret_cast<const long long*>(bits_.data()), byte_offset, 1);
    uint32_t found =
        _mm256_movemask_epi8(_mm256_cmpeq_epi64(mask, _mm256_and_si256(word, mask)));
    reinterpret_cast<uint32_t*>(result)[i] = found;
  }
  for (int64_t i = num_rows / unroll * unroll; i < num_rows; ++i) {
    result[i] = MayHaveHash(hashes[i]);
  }
}

void ApproximateMembershipTest::MayHaveHash_avx2(int64_t num_rows, const uint64_t* hashes,
                                                 uint8_t* result) const {
  MayHaveHash_imp_avx2<uint64_t>(num_rows, hashes, result);
}

void ApproximateMembershipTest::MayHaveHash_avx2(int64_t num_rows, const uint32_t* hashes,
                                                 uint8_t* result) const {
  MayHaveHash_imp_avx2<uint32_t>(num_rows, hashes, result);
}

}  // namespace compute
}  // namespace arrow
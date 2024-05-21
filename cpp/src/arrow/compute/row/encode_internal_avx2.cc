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

#include "arrow/compute/row/encode_internal.h"

namespace arrow {
namespace compute {

void EncoderBinary::DecodeHelper_avx2(bool is_row_fixed_length, uint32_t start_row,
                                      uint32_t num_rows, uint32_t offset_within_row,
                                      const RowTableImpl& rows, KeyColumnArray* col) {
  if (is_row_fixed_length) {
    DecodeImp_avx2<true>(start_row, num_rows, offset_within_row, rows, col);
  } else {
    DecodeImp_avx2<false>(start_row, num_rows, offset_within_row, rows, col);
  }
}

template <bool is_row_fixed_length>
void EncoderBinary::DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                                   uint32_t offset_within_row, const RowTableImpl& rows,
                                   KeyColumnArray* col) {
  DecodeHelper<is_row_fixed_length>(
      start_row, num_rows, offset_within_row, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < (length + 31) / 32; ++istripe) {
          __m256i* dst256 = reinterpret_cast<__m256i*>(dst);
          const __m256i* src256 = reinterpret_cast<const __m256i*>(src);
          _mm256_storeu_si256(dst256 + istripe, _mm256_loadu_si256(src256 + istripe));
        }
      });
}

uint32_t EncoderBinaryPair::DecodeHelper_avx2(
    bool is_row_fixed_length, uint32_t col_width, uint32_t start_row, uint32_t num_rows,
    uint32_t offset_within_row, const RowTableImpl& rows, KeyColumnArray* col1,
    KeyColumnArray* col2) {
  using DecodeImp_avx2_t =
      uint32_t (*)(uint32_t start_row, uint32_t num_rows, uint32_t offset_within_row,
                   const RowTableImpl& rows, KeyColumnArray* col1, KeyColumnArray* col2);
  static const DecodeImp_avx2_t DecodeImp_avx2_fn[] = {
      DecodeImp_avx2<false, 1>, DecodeImp_avx2<false, 2>, DecodeImp_avx2<false, 4>,
      DecodeImp_avx2<false, 8>, DecodeImp_avx2<true, 1>,  DecodeImp_avx2<true, 2>,
      DecodeImp_avx2<true, 4>,  DecodeImp_avx2<true, 8>};
  int log_col_width = col_width == 8 ? 3 : col_width == 4 ? 2 : col_width == 2 ? 1 : 0;
  int dispatch_const = log_col_width | (is_row_fixed_length ? 4 : 0);
  return DecodeImp_avx2_fn[dispatch_const](start_row, num_rows, offset_within_row, rows,
                                           col1, col2);
}

template <bool is_row_fixed_length, uint32_t col_width>
uint32_t EncoderBinaryPair::DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                                           uint32_t offset_within_row,
                                           const RowTableImpl& rows, KeyColumnArray* col1,
                                           KeyColumnArray* col2) {
  ARROW_DCHECK(col_width == 1 || col_width == 2 || col_width == 4 || col_width == 8);

  uint8_t* col_vals_A = col1->mutable_data(1);
  uint8_t* col_vals_B = col2->mutable_data(1);

  uint32_t fixed_length = rows.metadata().fixed_length;
  const uint32_t* offsets;
  const uint8_t* src_base;
  if (is_row_fixed_length) {
    src_base = rows.data(1) + fixed_length * start_row + offset_within_row;
    offsets = nullptr;
  } else {
    src_base = rows.data(2) + offset_within_row;
    offsets = rows.offsets() + start_row;
  }

  constexpr int unroll = 32 / col_width;

  uint32_t num_processed = num_rows / unroll * unroll;

  if (col_width == 8) {
    for (uint32_t i = 0; i < num_rows / unroll; ++i) {
      const __m128i *src0, *src1, *src2, *src3;
      if (is_row_fixed_length) {
        const uint8_t* src = src_base + (i * unroll) * fixed_length;
        src0 = reinterpret_cast<const __m128i*>(src);
        src1 = reinterpret_cast<const __m128i*>(src + fixed_length);
        src2 = reinterpret_cast<const __m128i*>(src + fixed_length * 2);
        src3 = reinterpret_cast<const __m128i*>(src + fixed_length * 3);
      } else {
        const uint32_t* row_offsets = offsets + i * unroll;
        const uint8_t* src = src_base;
        src0 = reinterpret_cast<const __m128i*>(src + row_offsets[0]);
        src1 = reinterpret_cast<const __m128i*>(src + row_offsets[1]);
        src2 = reinterpret_cast<const __m128i*>(src + row_offsets[2]);
        src3 = reinterpret_cast<const __m128i*>(src + row_offsets[3]);
      }

      __m256i r0 = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128(src0)),
                                           _mm_loadu_si128(src1), 1);
      __m256i r1 = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128(src2)),
                                           _mm_loadu_si128(src3), 1);

      r0 = _mm256_permute4x64_epi64(r0, 0xd8);  // 0b11011000
      r1 = _mm256_permute4x64_epi64(r1, 0xd8);

      // First 128-bit lanes from both inputs
      __m256i c1 = _mm256_permute2x128_si256(r0, r1, 0x20);
      // Second 128-bit lanes from both inputs
      __m256i c2 = _mm256_permute2x128_si256(r0, r1, 0x31);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(col_vals_A) + i, c1);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(col_vals_B) + i, c2);
    }
  } else {
    uint8_t buffer[64];
    for (uint32_t i = 0; i < num_rows / unroll; ++i) {
      if (is_row_fixed_length) {
        const uint8_t* src = src_base + (i * unroll) * fixed_length;
        for (int j = 0; j < unroll; ++j) {
          if (col_width == 1) {
            reinterpret_cast<uint16_t*>(buffer)[j] =
                *reinterpret_cast<const uint16_t*>(src + fixed_length * j);
          } else if (col_width == 2) {
            reinterpret_cast<uint32_t*>(buffer)[j] =
                *reinterpret_cast<const uint32_t*>(src + fixed_length * j);
          } else if (col_width == 4) {
            reinterpret_cast<uint64_t*>(buffer)[j] =
                *reinterpret_cast<const uint64_t*>(src + fixed_length * j);
          }
        }
      } else {
        const uint32_t* row_offsets = offsets + i * unroll;
        const uint8_t* src = src_base;
        for (int j = 0; j < unroll; ++j) {
          if (col_width == 1) {
            reinterpret_cast<uint16_t*>(buffer)[j] =
                *reinterpret_cast<const uint16_t*>(src + row_offsets[j]);
          } else if (col_width == 2) {
            reinterpret_cast<uint32_t*>(buffer)[j] =
                *reinterpret_cast<const uint32_t*>(src + row_offsets[j]);
          } else if (col_width == 4) {
            reinterpret_cast<uint64_t*>(buffer)[j] =
                *reinterpret_cast<const uint64_t*>(src + row_offsets[j]);
          }
        }
      }

      __m256i r0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer));
      __m256i r1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(buffer) + 1);

      constexpr uint64_t kByteSequence_0_2_4_6_8_10_12_14 = 0x0e0c0a0806040200ULL;
      constexpr uint64_t kByteSequence_1_3_5_7_9_11_13_15 = 0x0f0d0b0907050301ULL;
      constexpr uint64_t kByteSequence_0_1_4_5_8_9_12_13 = 0x0d0c090805040100ULL;
      constexpr uint64_t kByteSequence_2_3_6_7_10_11_14_15 = 0x0f0e0b0a07060302ULL;

      if (col_width == 1) {
        // Collect every second byte next to each other
        const __m256i shuffle_const = _mm256_setr_epi64x(
            kByteSequence_0_2_4_6_8_10_12_14, kByteSequence_1_3_5_7_9_11_13_15,
            kByteSequence_0_2_4_6_8_10_12_14, kByteSequence_1_3_5_7_9_11_13_15);
        r0 = _mm256_shuffle_epi8(r0, shuffle_const);
        r1 = _mm256_shuffle_epi8(r1, shuffle_const);
        // 0b11011000 swapping second and third 64-bit lane
        r0 = _mm256_permute4x64_epi64(r0, 0xd8);
        r1 = _mm256_permute4x64_epi64(r1, 0xd8);
      } else if (col_width == 2) {
        // Collect every second 16-bit word next to each other
        const __m256i shuffle_const = _mm256_setr_epi64x(
            kByteSequence_0_1_4_5_8_9_12_13, kByteSequence_2_3_6_7_10_11_14_15,
            kByteSequence_0_1_4_5_8_9_12_13, kByteSequence_2_3_6_7_10_11_14_15);
        r0 = _mm256_shuffle_epi8(r0, shuffle_const);
        r1 = _mm256_shuffle_epi8(r1, shuffle_const);
        // 0b11011000 swapping second and third 64-bit lane
        r0 = _mm256_permute4x64_epi64(r0, 0xd8);
        r1 = _mm256_permute4x64_epi64(r1, 0xd8);
      } else if (col_width == 4) {
        // Collect every second 32-bit word next to each other
        const __m256i permute_const = _mm256_setr_epi32(0, 2, 4, 6, 1, 3, 5, 7);
        r0 = _mm256_permutevar8x32_epi32(r0, permute_const);
        r1 = _mm256_permutevar8x32_epi32(r1, permute_const);
      }

      // First 128-bit lanes from both inputs
      __m256i c1 = _mm256_permute2x128_si256(r0, r1, 0x20);
      // Second 128-bit lanes from both inputs
      __m256i c2 = _mm256_permute2x128_si256(r0, r1, 0x31);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(col_vals_A) + i, c1);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(col_vals_B) + i, c2);
    }
  }

  return num_processed;
}

void EncoderVarBinary::DecodeHelper_avx2(uint32_t start_row, uint32_t num_rows,
                                         uint32_t varbinary_col_id,
                                         const RowTableImpl& rows, KeyColumnArray* col) {
  if (varbinary_col_id == 0) {
    DecodeImp_avx2<true>(start_row, num_rows, varbinary_col_id, rows, col);
  } else {
    DecodeImp_avx2<false>(start_row, num_rows, varbinary_col_id, rows, col);
  }
}

template <bool first_varbinary_col>
void EncoderVarBinary::DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                                      uint32_t varbinary_col_id, const RowTableImpl& rows,
                                      KeyColumnArray* col) {
  DecodeHelper<first_varbinary_col>(
      start_row, num_rows, varbinary_col_id, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < (length + 31) / 32; ++istripe) {
          __m256i* dst256 = reinterpret_cast<__m256i*>(dst);
          const __m256i* src256 = reinterpret_cast<const __m256i*>(src);
          _mm256_storeu_si256(dst256 + istripe, _mm256_loadu_si256(src256 + istripe));
        }
      });
}

}  // namespace compute
}  // namespace arrow

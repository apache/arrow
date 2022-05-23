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

#if defined(ARROW_HAVE_AVX2)

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

template <class PROCESS_8_VALUES_FN>
int RowTableAccessor::Visit_avx2(const RowTableImpl& rows, int column_id, int num_rows,
                                 const uint32_t* row_ids,
                                 PROCESS_8_VALUES_FN process_8_values_fn) {
  // Number of rows processed together in a single iteration of the loop (single
  // call to the provided processing lambda).
  //
  constexpr int unroll = 8;

  bool is_fixed_length_column =
      rows.metadata().column_metadatas[column_id].is_fixed_length;

  // There are 4 cases, each requiring different steps:
  // 1. Varying length column that is the first varying length column in a row
  // 2. Varying length column that is not the first varying length column in a
  // row
  // 3. Fixed length column in a fixed length row
  // 4. Fixed length column in a varying length row

  if (!is_fixed_length_column) {
    int varbinary_column_id = VarbinaryColumnId(rows.metadata(), column_id);
    const uint8_t* row_ptr_base = rows.data(2);
    const uint32_t* row_offsets = rows.offsets();

    if (varbinary_column_id == 0) {
      // Case 1: This is the first varbinary column
      //
      __m256i field_offset_within_row = _mm256_set1_epi32(rows.metadata().fixed_length);
      __m256i varbinary_end_array_offset =
          _mm256_set1_epi32(rows.metadata().varbinary_end_array_offset);
      for (int i = 0; i < num_rows / unroll; ++i) {
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        __m256i row_offset = _mm256_i32gather_epi32(
            reinterpret_cast<const int*>(row_offsets), row_id, sizeof(uint32_t));
        __m256i field_length = _mm256_sub_epi32(
            _mm256_i32gather_epi32(
                reinterpret_cast<const int*>(row_ptr_base),
                _mm256_add_epi32(row_offset, varbinary_end_array_offset), 1),
            field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base,
                            _mm256_add_epi32(row_offset, field_offset_within_row),
                            field_length);
      }
    } else {
      // Case 2: This is second or later varbinary column
      //
      __m256i varbinary_end_array_offset =
          _mm256_set1_epi32(rows.metadata().varbinary_end_array_offset +
                            sizeof(uint32_t) * (varbinary_column_id - 1));
      auto row_ptr_base_i64 =
          reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_ptr_base);
      for (int i = 0; i < num_rows / unroll; ++i) {
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        __m256i row_offset = _mm256_i32gather_epi32(
            reinterpret_cast<const int*>(row_offsets), row_id, sizeof(uint32_t));
        __m256i end_array_offset =
            _mm256_add_epi32(row_offset, varbinary_end_array_offset);

        __m256i field_offset_within_row_A = _mm256_i32gather_epi64(
            row_ptr_base_i64, _mm256_castsi256_si128(end_array_offset), 1);
        __m256i field_offset_within_row_B = _mm256_i32gather_epi64(
            row_ptr_base_i64, _mm256_extracti128_si256(end_array_offset, 1), 1);
        field_offset_within_row_A = _mm256_permutevar8x32_epi32(
            field_offset_within_row_A, _mm256_setr_epi32(0, 2, 4, 6, 1, 3, 5, 7));
        field_offset_within_row_B = _mm256_permutevar8x32_epi32(
            field_offset_within_row_B, _mm256_setr_epi32(1, 3, 5, 7, 0, 2, 4, 6));

        __m256i field_offset_within_row = _mm256_blend_epi32(
            field_offset_within_row_A, field_offset_within_row_B, 0xf0);

        __m256i alignment_padding =
            _mm256_andnot_si256(field_offset_within_row, _mm256_set1_epi8(0xff));
        alignment_padding = _mm256_add_epi32(alignment_padding, _mm256_set1_epi32(1));
        alignment_padding = _mm256_and_si256(
            alignment_padding, _mm256_set1_epi32(rows.metadata().string_alignment - 1));

        field_offset_within_row =
            _mm256_add_epi32(field_offset_within_row, alignment_padding);

        __m256i field_length = _mm256_blend_epi32(field_offset_within_row_A,
                                                  field_offset_within_row_B, 0x0f);
        field_length = _mm256_permute4x64_epi64(field_length,
                                                0x4e);  // Swapping low and high 128-bits
        field_length = _mm256_sub_epi32(field_length, field_offset_within_row);

        process_8_values_fn(i * unroll, row_ptr_base,
                            _mm256_add_epi32(row_offset, field_offset_within_row),
                            field_length);
      }
    }
  }

  if (is_fixed_length_column) {
    __m256i field_offset_within_row =
        _mm256_set1_epi32(rows.metadata().encoded_field_offset(
            rows.metadata().pos_after_encoding(column_id)));
    __m256i field_length =
        _mm256_set1_epi32(rows.metadata().column_metadatas[column_id].fixed_length);

    bool is_fixed_length_row = rows.metadata().is_fixed_length;
    if (is_fixed_length_row) {
      // Case 3: This is a fixed length column in fixed length row
      //
      const uint8_t* row_ptr_base = rows.data(1);
      for (int i = 0; i < num_rows / unroll; ++i) {
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        __m256i row_offset = _mm256_mullo_epi32(row_id, field_length);
        __m256i field_offset = _mm256_add_epi32(row_offset, field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base, field_offset, field_length);
      }
    } else {
      // Case 4: This is a fixed length column in varying length row
      //
      const uint8_t* row_ptr_base = rows.data(2);
      const uint32_t* row_offsets = rows.offsets();
      for (int i = 0; i < num_rows / unroll; ++i) {
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        __m256i row_offset = _mm256_i32gather_epi32(
            reinterpret_cast<const int*>(row_offsets), row_id, sizeof(uint32_t));
        __m256i field_offset = _mm256_add_epi32(row_offset, field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base, field_offset, field_length);
      }
    }
  }

  return num_rows - (num_rows % unroll);
}

template <class PROCESS_8_VALUES_FN>
int RowTableAccessor::VisitNulls_avx2(const RowTableImpl& rows, int column_id,
                                      int num_rows, const uint32_t* row_ids,
                                      PROCESS_8_VALUES_FN process_8_values_fn) {
  // Number of rows processed together in a single iteration of the loop (single
  // call to the provided processing lambda).
  //
  constexpr int unroll = 8;

  const uint8_t* null_masks = rows.null_masks();
  __m256i null_bits_per_row =
      _mm256_set1_epi32(8 * rows.metadata().null_masks_bytes_per_row);
  for (int i = 0; i < num_rows / unroll; ++i) {
    __m256i row_id = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
    __m256i bit_id = _mm256_mullo_epi32(row_id, null_bits_per_row);
    bit_id = _mm256_add_epi32(bit_id, _mm256_set1_epi32(column_id));
    __m256i bytes = _mm256_i32gather_epi32(reinterpret_cast<const int*>(null_masks),
                                           _mm256_srli_epi32(bit_id, 3), 1);
    __m256i bit_in_word = _mm256_sllv_epi32(
        _mm256_set1_epi32(1), _mm256_and_si256(bit_id, _mm256_set1_epi32(7)));
    __m256i result =
        _mm256_cmpeq_epi32(_mm256_and_si256(bytes, bit_in_word), bit_in_word);
    uint64_t null_bytes = static_cast<uint64_t>(
        _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(result))));
    null_bytes |= static_cast<uint64_t>(_mm256_movemask_epi8(
                      _mm256_cvtepi32_epi64(_mm256_extracti128_si256(result, 1))))
                  << 32;

    process_8_values_fn(i * unroll, null_bytes);
  }

  return num_rows - (num_rows % unroll);
}

#endif

}  // namespace compute
}  // namespace arrow

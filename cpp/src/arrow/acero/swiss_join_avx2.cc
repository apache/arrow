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

#include "arrow/acero/swiss_join_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/simd.h"

namespace arrow {
namespace acero {

// TODO(GH-43693): The functions in this file are not wired anywhere. We may consider
// actually utilizing them or removing them.

template <class PROCESS_8_VALUES_FN>
int RowArrayAccessor::Visit_avx2(const RowTableImpl& rows, int column_id, int num_rows,
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
    const RowTableImpl::offset_type* row_offsets = rows.offsets();
    static_assert(
        sizeof(RowTableImpl::offset_type) == sizeof(int64_t),
        "RowArrayAccessor::Visit_avx2 only supports 64-bit RowTableImpl::offset_type");

    if (varbinary_column_id == 0) {
      // Case 1: This is the first varbinary column
      //
      __m256i field_offset_within_row = _mm256_set1_epi32(rows.metadata().fixed_length);
      __m256i varbinary_end_array_offset =
          _mm256_set1_epi64x(rows.metadata().varbinary_end_array_offset);
      for (int i = 0; i < num_rows / unroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets, _mm256_extracti128_si256(row_id, 1),
                                   sizeof(RowTableImpl::offset_type));
        // Gather the lower/higher 4 32-bit field lengths based on the lower/higher 4
        // 64-bit row offsets.
        __m128i field_length_lo = _mm256_i64gather_epi32(
            reinterpret_cast<const int*>(row_ptr_base),
            _mm256_add_epi64(row_offset_lo, varbinary_end_array_offset), 1);
        __m128i field_length_hi = _mm256_i64gather_epi32(
            reinterpret_cast<const int*>(row_ptr_base),
            _mm256_add_epi64(row_offset_hi, varbinary_end_array_offset), 1);
        // The final 8 32-bit field lengths, subtracting the field offset within row.
        __m256i field_length = _mm256_sub_epi32(
            _mm256_set_m128i(field_length_hi, field_length_lo), field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base,
                            _mm256_add_epi64(row_offset_lo, field_offset_within_row),
                            _mm256_add_epi64(row_offset_hi, field_offset_within_row),
                            field_length);
      }
    } else {
      // Case 2: This is second or later varbinary column
      //
      __m256i varbinary_end_array_offset =
          _mm256_set1_epi64x(rows.metadata().varbinary_end_array_offset +
                             sizeof(uint32_t) * (varbinary_column_id - 1));
      auto row_ptr_base_i64 =
          reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_ptr_base);
      for (int i = 0; i < num_rows / unroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        // Gather the lower/higher 4 32-bit field lengths based on the lower/higher 4
        // 64-bit row offsets.
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets, _mm256_extracti128_si256(row_id, 1),
                                   sizeof(RowTableImpl::offset_type));
        // Prepare the lower/higher 4 64-bit end array offsets based on the lower/higher 4
        // 64-bit row offsets.
        __m256i end_array_offset_lo =
            _mm256_add_epi64(row_offset_lo, varbinary_end_array_offset);
        __m256i end_array_offset_hi =
            _mm256_add_epi64(row_offset_hi, varbinary_end_array_offset);

        __m256i field_offset_within_row_A =
            _mm256_i64gather_epi64(row_ptr_base_i64, end_array_offset_lo, 1);
        __m256i field_offset_within_row_B =
            _mm256_i64gather_epi64(row_ptr_base_i64, end_array_offset_hi, 1);
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

        field_offset_within_row_A =
            _mm256_add_epi32(field_offset_within_row_A, alignment_padding);
        field_offset_within_row_B =
            _mm256_add_epi32(field_offset_within_row_B, alignment_padding);

        process_8_values_fn(i * unroll, row_ptr_base,
                            _mm256_add_epi64(row_offset_lo, field_offset_within_row_A),
                            _mm256_add_epi64(row_offset_hi, field_offset_within_row_B),
                            field_length);
      }
    }
  }

  if (is_fixed_length_column) {
    __m256i field_offset_within_row =
        _mm256_set1_epi64x(rows.metadata().encoded_field_offset(
            rows.metadata().pos_after_encoding(column_id)));
    __m256i field_length =
        _mm256_set1_epi32(rows.metadata().column_metadatas[column_id].fixed_length);

    bool is_fixed_length_row = rows.metadata().is_fixed_length;
    if (is_fixed_length_row) {
      // Case 3: This is a fixed length column in fixed length row
      //
      const uint8_t* row_ptr_base = rows.data(1);
      for (int i = 0; i < num_rows / unroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Widen the 32-bit row ids to 64-bit and store the lower/higher 4 of them into 2
        // 256-bit registers.
        __m256i row_id_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(row_id));
        __m256i row_id_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(row_id, 1));
        // Calculate the lower/higher 4 64-bit row offsets based on the lower/higher 4
        // 64-bit row ids and the fixed field length.
        __m256i row_offset_lo = _mm256_mul_epi32(row_id_lo, field_length);
        __m256i row_offset_hi = _mm256_mul_epi32(row_id_hi, field_length);
        // Calculate the lower/higher 4 64-bit field offsets based on the lower/higher 4
        // 64-bit row offsets and field offset within row.
        __m256i field_offset_lo =
            _mm256_add_epi64(row_offset_lo, field_offset_within_row);
        __m256i field_offset_hi =
            _mm256_add_epi64(row_offset_hi, field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base, field_offset_lo, field_offset_hi,
                            field_length);
      }
    } else {
      // Case 4: This is a fixed length column in varying length row
      //
      const uint8_t* row_ptr_base = rows.data(2);
      const RowTableImpl::offset_type* row_offsets = rows.offsets();
      for (int i = 0; i < num_rows / unroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets, _mm256_extracti128_si256(row_id, 1),
                                   sizeof(RowTableImpl::offset_type));
        // Calculate the lower/higher 4 64-bit field offsets based on the lower/higher 4
        // 64-bit row offsets and field offset within row.
        __m256i field_offset_lo =
            _mm256_add_epi64(row_offset_lo, field_offset_within_row);
        __m256i field_offset_hi =
            _mm256_add_epi64(row_offset_hi, field_offset_within_row);
        process_8_values_fn(i * unroll, row_ptr_base, field_offset_lo, field_offset_hi,
                            field_length);
      }
    }
  }

  return num_rows - (num_rows % unroll);
}

template <class PROCESS_8_VALUES_FN>
int RowArrayAccessor::VisitNulls_avx2(const RowTableImpl& rows, int column_id,
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

}  // namespace acero
}  // namespace arrow

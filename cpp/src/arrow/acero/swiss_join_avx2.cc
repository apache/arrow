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

namespace {

inline void Decode8FixedLength0_avx2(uint8_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 8 bits interesting) rows based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_lo, 1);
  __m128i row_hi = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_hi, 1);
  // Pack the lower/higher 4 32-bit rows into 8 16-bit rows.
  __m128i row = _mm_packs_epi32(row_lo, row_hi);
  // Shift left by 7 bits to move the LSB into the MSB for each 16-bit row.
  row = _mm_slli_epi32(row, 7);
  // Pack the 8 16-bit rows into 8 8-bit rows.
  row = _mm_packs_epi16(row, _mm_setzero_si128());
  // Get the MSB for each 8-bit row.
  int bits = _mm_movemask_epi8(row);
  *output = static_cast<uint8_t>(bits);
}

inline void Decode8FixedLength1_avx2(uint8_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 8 bits interesting) rows based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_lo, 1);
  __m128i row_hi = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_hi, 1);
  // Pack the lower/higher 4 32-bit rows into 8 16-bit rows.
  __m128i row = _mm_packs_epi32(row_lo, row_hi);
  // Pack the 8 16-bit rows into 8 8-bit rows.
  row = _mm_packs_epi16(row, _mm_setzero_si128());
  _mm_storel_epi64(reinterpret_cast<__m128i*>(output), row);
}

inline void Decode8FixedLength2_avx2(uint16_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 16 bits interesting) rows based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_lo, 1);
  __m128i row_hi = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_hi, 1);
  // Pack the lower/higher 4 32-bit rows into 8 16-bit rows.
  __m128i row = _mm_packs_epi32(row_lo, row_hi);
  _mm_storeu_si128(reinterpret_cast<__m128i*>(output), row);
}

inline void Decode8FixedLength4_avx2(uint32_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit rows based on the lower/higher 4 64-bit row offsets.
  __m128i row_lo = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_lo, 1);
  __m128i row_hi = _mm256_i64gather_epi32((const int*)row_ptr_base, offset_hi, 1);
  __m256i row = _mm256_set_m128i(row_hi, row_lo);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), row);
}

inline void Decode8FixedLength8_avx2(uint64_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  auto row_ptr_base_i64 =
      reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_ptr_base);
  // Gather the lower/higher 4 64-bit rows based on the lower/higher 4 64-bit row offsets.
  __m256i row_lo = _mm256_i64gather_epi64(row_ptr_base_i64, offset_lo, 1);
  __m256i row_hi = _mm256_i64gather_epi64(row_ptr_base_i64, offset_hi, 1);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), row_lo);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + 4), row_hi);
}

inline void Decode1_avx2(uint8_t* output, const uint8_t* row_ptr, uint32_t num_bytes) {
  // Copy 32 bytes at a time.
  __m256i* output_i256 = reinterpret_cast<__m256i*>(output);
  const __m256i* row_ptr_i256 = reinterpret_cast<const __m256i*>(row_ptr);
  for (int istripe = 0; istripe < bit_util::CeilDiv(num_bytes, 32); ++istripe) {
    _mm256_storeu_si256(output_i256 + istripe,
                        _mm256_loadu_si256(row_ptr_i256 + istripe));
  }
}

}  // namespace

int RowArray::DecodeFixedLength_avx2(ResizableArrayData* output, int output_start_row,
                                     int column_id, uint32_t fixed_length,
                                     int num_rows_to_append,
                                     const uint32_t* row_ids) const {
  DCHECK_EQ(output_start_row % 8, 0);

  int num_rows_processed = 0;
  switch (fixed_length) {
    case 0:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength0_avx2(output->mutable_data(1) + (output_start_row + i) / 8,
                                     row_ptr_base, offset_lo, offset_hi);
          });
      break;
    case 1:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength1_avx2(output->mutable_data(1) + output_start_row + i,
                                     row_ptr_base, offset_lo, offset_hi);
          });
      break;
    case 2:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength2_avx2(
                reinterpret_cast<uint16_t*>(output->mutable_data(1)) + output_start_row +
                    i,
                row_ptr_base, offset_lo, offset_hi);
          });
      break;
    case 4:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength4_avx2(
                reinterpret_cast<uint32_t*>(output->mutable_data(1)) + output_start_row +
                    i,
                row_ptr_base, offset_lo, offset_hi);
          });
      break;
    case 8:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength8_avx2(
                reinterpret_cast<uint64_t*>(output->mutable_data(1)) + output_start_row +
                    i,
                row_ptr_base, offset_lo, offset_hi);
          });
      break;
    default:
      RowArrayAccessor::Visit(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr, uint32_t num_bytes) {
            Decode1_avx2(output->mutable_data(1) + num_bytes * (output_start_row + i),
                         row_ptr, num_bytes);
          });
      num_rows_processed = num_rows_to_append;
      break;
  }

  return num_rows_processed;
}

int RowArray::DecodeOffsets_avx2(ResizableArrayData* output, int output_start_row,
                                 int column_id, int num_rows_to_append,
                                 const uint32_t* row_ids) const {
  uint32_t* offsets =
      reinterpret_cast<uint32_t*>(output->mutable_data(1)) + output_start_row;
  uint32_t sum = (output_start_row == 0) ? 0 : offsets[0];
  int num_rows_processed = RowArrayAccessor::Visit_avx2(
      rows_, column_id, num_rows_to_append, row_ids,
      [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
          __m256i num_bytes) {
        Decode8FixedLength4_avx2(
            reinterpret_cast<uint32_t*>(output->mutable_data(1)) + output_start_row + i,
            row_ptr_base, offset_lo, offset_hi);
        // offsets[i] = num_bytes;
      });
  for (int i = 0; i < num_rows_processed; ++i) {
    uint32_t length = offsets[i];
    offsets[i] = sum;
    sum += length;
  }
  offsets[num_rows_processed] = sum;

  return num_rows_processed;
}

int RowArray::DecodeVarLength_avx2(ResizableArrayData* output, int output_start_row,
                                   int column_id, int num_rows_to_append,
                                   const uint32_t* row_ids) const {
  RowArrayAccessor::Visit(
      rows_, column_id, num_rows_to_append, row_ids,
      [&](int i, const uint8_t* row_ptr, uint32_t num_bytes) {
        Decode1_avx2(output->mutable_data(1) + num_bytes * (output_start_row + i),
                     row_ptr, num_bytes);
      });
  return num_rows_to_append;
}

int RowArray::DecodeNulls_avx2(ResizableArrayData* output, int output_start_row,
                               int column_id, int num_rows_to_append,
                               const uint32_t* row_ids) const {
  DCHECK_EQ(output_start_row % 8, 0);

  return RowArrayAccessor::VisitNulls_avx2(
      rows_, column_id, num_rows_to_append, row_ids, [&](int i, uint8_t value) {
        *(output->mutable_data(0) + (output_start_row + i) / 8) = value;
      });
}

}  // namespace acero
}  // namespace arrow

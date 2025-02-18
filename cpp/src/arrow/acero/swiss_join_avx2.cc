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
#include "arrow/compute/row/row_util_avx2_internal.h"
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
  constexpr int kUnroll = 8;

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
    const uint8_t* row_ptr_base = rows.var_length_rows();
    const RowTableImpl::offset_type* row_offsets = rows.offsets();
    auto row_offsets_i64 =
        reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_offsets);
    static_assert(
        sizeof(RowTableImpl::offset_type) == sizeof(int64_t),
        "RowArrayAccessor::Visit_avx2 only supports 64-bit RowTableImpl::offset_type");

    if (varbinary_column_id == 0) {
      // Case 1: This is the first varbinary column
      //
      __m256i field_offset_within_row = _mm256_set1_epi32(rows.metadata().fixed_length);
      __m256i varbinary_end_array_offset =
          _mm256_set1_epi64x(rows.metadata().varbinary_end_array_offset);
      for (int i = 0; i < num_rows / kUnroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_extracti128_si256(row_id, 1),
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
        process_8_values_fn(i * kUnroll, row_ptr_base,
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
      for (int i = 0; i < num_rows / kUnroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_extracti128_si256(row_id, 1),
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

        __m256i alignment_padding = _mm256_andnot_si256(
            field_offset_within_row, _mm256_set1_epi8(static_cast<char>(0xff)));
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

        process_8_values_fn(i * kUnroll, row_ptr_base,
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
    uint32_t actual_field_length =
        rows.metadata().column_metadatas[column_id].fixed_length;
    // Bit column is encoded as a single byte
    if (actual_field_length == 0) {
      actual_field_length = 1;
    }
    __m256i field_length = _mm256_set1_epi32(actual_field_length);
    __m256i row_length = _mm256_set1_epi64x(rows.metadata().fixed_length);

    bool is_fixed_length_row = rows.metadata().is_fixed_length;
    if (is_fixed_length_row) {
      // Case 3: This is a fixed length column in fixed length row
      //
      const uint8_t* row_ptr_base = rows.fixed_length_rows(/*row_id=*/0);
      for (int i = 0; i < num_rows / kUnroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Widen the 32-bit row ids to 64-bit and store the lower/higher 4 of them into 2
        // 256-bit registers.
        __m256i row_id_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(row_id));
        __m256i row_id_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(row_id, 1));
        // Calculate the lower/higher 4 64-bit row offsets based on the lower/higher 4
        // 64-bit row ids and the fixed field length.
        __m256i row_offset_lo = _mm256_mul_epi32(row_id_lo, row_length);
        __m256i row_offset_hi = _mm256_mul_epi32(row_id_hi, row_length);
        // Calculate the lower/higher 4 64-bit field offsets based on the lower/higher 4
        // 64-bit row offsets and field offset within row.
        __m256i field_offset_lo =
            _mm256_add_epi64(row_offset_lo, field_offset_within_row);
        __m256i field_offset_hi =
            _mm256_add_epi64(row_offset_hi, field_offset_within_row);
        process_8_values_fn(i * kUnroll, row_ptr_base, field_offset_lo, field_offset_hi,
                            field_length);
      }
    } else {
      // Case 4: This is a fixed length column in varying length row
      //
      const uint8_t* row_ptr_base = rows.var_length_rows();
      const RowTableImpl::offset_type* row_offsets = rows.offsets();
      auto row_offsets_i64 =
          reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_offsets);
      for (int i = 0; i < num_rows / kUnroll; ++i) {
        // Load 8 32-bit row ids.
        __m256i row_id =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
        // Gather the lower/higher 4 64-bit row offsets based on the lower/higher 4 32-bit
        // row ids.
        __m256i row_offset_lo =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_castsi256_si128(row_id),
                                   sizeof(RowTableImpl::offset_type));
        __m256i row_offset_hi =
            _mm256_i32gather_epi64(row_offsets_i64, _mm256_extracti128_si256(row_id, 1),
                                   sizeof(RowTableImpl::offset_type));
        // Calculate the lower/higher 4 64-bit field offsets based on the lower/higher 4
        // 64-bit row offsets and field offset within row.
        __m256i field_offset_lo =
            _mm256_add_epi64(row_offset_lo, field_offset_within_row);
        __m256i field_offset_hi =
            _mm256_add_epi64(row_offset_hi, field_offset_within_row);
        process_8_values_fn(i * kUnroll, row_ptr_base, field_offset_lo, field_offset_hi,
                            field_length);
      }
    }
  }

  return num_rows - (num_rows % kUnroll);
}

template <class PROCESS_8_VALUES_FN>
int RowArrayAccessor::VisitNulls_avx2(const RowTableImpl& rows, int column_id,
                                      int num_rows, const uint32_t* row_ids,
                                      PROCESS_8_VALUES_FN process_8_values_fn) {
  // Number of rows processed together in a single iteration of the loop (single
  // call to the provided processing lambda).
  //
  constexpr int kUnroll = 8;

  uint32_t pos_after_encoding = rows.metadata().pos_after_encoding(column_id);
  for (int i = 0; i < num_rows / kUnroll; ++i) {
    __m256i row_id = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row_ids) + i);
    __m256i null32 = GetNullBitInt32(rows, pos_after_encoding, row_id);
    null32 = _mm256_cmpeq_epi32(null32, _mm256_set1_epi32(1));
    uint64_t null_bytes = arrow::compute::Cmp32To8(null32);

    process_8_values_fn(i * kUnroll, null_bytes);
  }

  return num_rows - (num_rows % kUnroll);
}

namespace {

inline void Decode8FixedLength0_avx2(uint8_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 1 bit interesting) values based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_lo, 1);
  __m128i row_hi =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_hi, 1);
  // Extend to 64-bit.
  __m256i row_lo_64 = _mm256_cvtepi32_epi64(row_lo);
  __m256i row_hi_64 = _mm256_cvtepi32_epi64(row_hi);
  // Keep the first 8 bits in each 64-bit value, as the other bits belong to other
  // columns.
  row_lo_64 = _mm256_and_si256(row_lo_64, _mm256_set1_epi64x(0xFF));
  row_hi_64 = _mm256_and_si256(row_hi_64, _mm256_set1_epi64x(0xFF));
  // If a 64-bit value is zero, then we get 64 set bits.
  __m256i is_zero_lo_64 = _mm256_cmpeq_epi64(row_lo_64, _mm256_setzero_si256());
  __m256i is_zero_hi_64 = _mm256_cmpeq_epi64(row_hi_64, _mm256_setzero_si256());
  // 64 set bits per value to 8 set bits (one byte) per value.
  int is_zero_lo_8 = _mm256_movemask_epi8(is_zero_lo_64);
  int is_zero_hi_8 = _mm256_movemask_epi8(is_zero_hi_64);
  // 8 set bits to 1 set bit.
  uint8_t is_zero = static_cast<uint8_t>(
      _mm_movemask_epi8(_mm_set_epi32(0, 0, is_zero_hi_8, is_zero_lo_8)));
  *output = static_cast<uint8_t>(~is_zero);
}

inline void Decode8FixedLength1_avx2(uint8_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 8 bits interesting) values based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_lo, 1);
  __m128i row_hi =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_hi, 1);
  __m256i row = _mm256_set_m128i(row_hi, row_lo);
  // Shuffle the lower 8 bits of each 32-bit values to the lower 32 bits of each 128-bit
  // lane.
  constexpr uint64_t kByteSequence_0_4_8_12 = 0x0c080400ULL;
  const __m256i shuffle_const =
      _mm256_setr_epi64x(kByteSequence_0_4_8_12, -1, kByteSequence_0_4_8_12, -1);
  row = _mm256_shuffle_epi8(row, shuffle_const);
  // Get the lower 32-bits (4 8-bit values) from each 128-bit lane.
  // NB: Be careful about sign-extension when casting the return value of
  // _mm256_extract_epi32 (signed 32-bit) to unsigned 64-bit, which will pollute the
  // higher bits of the following OR.
  uint32_t compact_row_lo = static_cast<uint32_t>(_mm256_extract_epi32(row, 0));
  uint64_t compact_row_hi = static_cast<uint64_t>(_mm256_extract_epi32(row, 4)) << 32;
  *reinterpret_cast<uint64_t*>(output) = compact_row_lo | compact_row_hi;
}

inline void Decode8FixedLength2_avx2(uint16_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit (only lower 16 bits interesting) values based on the
  // lower/higher 4 64-bit row offsets.
  __m128i row_lo =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_lo, 1);
  __m128i row_hi =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_hi, 1);
  __m256i row = _mm256_set_m128i(row_hi, row_lo);
  // Shuffle the lower 16 bits of each 32-bit values to the lower 64 bits of each 128-bit
  // lane.
  constexpr uint64_t kByteSequence_0_1_4_5_8_9_12_13 = 0x0d0c090805040100ULL;
  const __m256i shuffle_const = _mm256_setr_epi64x(kByteSequence_0_1_4_5_8_9_12_13, -1,
                                                   kByteSequence_0_1_4_5_8_9_12_13, -1);
  row = _mm256_shuffle_epi8(row, shuffle_const);
  // Swap the second and the third 64-bit lane, so that all 16-bit values end up in the
  // lower half of `row`.
  // (0xd8 = 0b 11 01 10 00)
  row = _mm256_permute4x64_epi64(row, 0xd8);
  _mm_storeu_si128(reinterpret_cast<__m128i*>(output), _mm256_castsi256_si128(row));
}

inline void Decode8FixedLength4_avx2(uint32_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  // Gather the lower/higher 4 32-bit values based on the lower/higher 4 64-bit row
  // offsets.
  __m128i row_lo =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_lo, 1);
  __m128i row_hi =
      _mm256_i64gather_epi32(reinterpret_cast<const int*>(row_ptr_base), offset_hi, 1);
  __m256i row = _mm256_set_m128i(row_hi, row_lo);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), row);
}

inline void Decode8FixedLength8_avx2(uint64_t* output, const uint8_t* row_ptr_base,
                                     __m256i offset_lo, __m256i offset_hi) {
  auto row_ptr_base_i64 =
      reinterpret_cast<const arrow::util::int64_for_gather_t*>(row_ptr_base);
  // Gather the lower/higher 4 64-bit values based on the lower/higher 4 64-bit row
  // offsets.
  __m256i row_lo = _mm256_i64gather_epi64(row_ptr_base_i64, offset_lo, 1);
  __m256i row_hi = _mm256_i64gather_epi64(row_ptr_base_i64, offset_hi, 1);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), row_lo);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output + 4), row_hi);
}

inline void Decode1_avx2(uint8_t* output, const uint8_t* row_ptr, uint32_t num_bytes) {
  // Copy 32 bytes at a time.
  // Note that both `output` and `row_ptr` have been allocated with enough padding to
  // accommodate the memory overshoot. See the allocations for `ResizableArrayData` in
  // `JoinResultMaterialize` and `JoinResidualFilter` for `output`, and
  // `RowTableImpl::kPaddingForVectors` for `row_ptr`.
  __m256i* output_i256 = reinterpret_cast<__m256i*>(output);
  const __m256i* row_ptr_i256 = reinterpret_cast<const __m256i*>(row_ptr);
  for (int istripe = 0; istripe < bit_util::CeilDiv(num_bytes, 32); ++istripe) {
    _mm256_storeu_si256(output_i256 + istripe,
                        _mm256_loadu_si256(row_ptr_i256 + istripe));
  }
}

inline uint32_t Decode8Offset_avx2(uint32_t* output, uint32_t current_length,
                                   __m256i num_bytes) {
  uint32_t num_bytes_last = static_cast<uint32_t>(_mm256_extract_epi32(num_bytes, 7));
  // Init every offset with the current length.
  __m256i offsets = _mm256_set1_epi32(current_length);
  // We keep left-shifting the length and accumulate the offset by adding the length.
  __m256i length =
      _mm256_permutevar8x32_epi32(num_bytes, _mm256_setr_epi32(7, 0, 1, 2, 3, 4, 5, 6));
  length = _mm256_insert_epi32(length, 0, 0);
  // `length` is now a sequence of 32-bit words such as:
  //   - length[0] = 0
  //   - length[1] = num_bytes[0]
  //   ...
  //   - length[7] = num_bytes[6]
  // (note that num_bytes[7] is kept in `num_bytes_last`)
  for (int i = 0; i < 7; ++i) {
    offsets = _mm256_add_epi32(offsets, length);
    length =
        _mm256_permutevar8x32_epi32(length, _mm256_setr_epi32(7, 0, 1, 2, 3, 4, 5, 6));
    length = _mm256_insert_epi32(length, 0, 0);
  }
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(output), offsets);
  return _mm256_extract_epi32(offsets, 7) + num_bytes_last;
}

inline void Decode8Null_avx2(uint8_t* output, uint64_t null_bytes) {
  uint8_t null_bits =
      static_cast<uint8_t>(_mm256_movemask_epi8(_mm256_set1_epi64x(null_bytes)));
  *output = ~null_bits;
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
            DCHECK_EQ(i % 8, 0);
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
                output->mutable_data_as<uint16_t>(1) + output_start_row + i, row_ptr_base,
                offset_lo, offset_hi);
          });
      break;
    case 4:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength4_avx2(
                output->mutable_data_as<uint32_t>(1) + output_start_row + i, row_ptr_base,
                offset_lo, offset_hi);
          });
      break;
    case 8:
      num_rows_processed = RowArrayAccessor::Visit_avx2(
          rows_, column_id, num_rows_to_append, row_ids,
          [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
              __m256i num_bytes) {
            Decode8FixedLength8_avx2(
                output->mutable_data_as<uint64_t>(1) + output_start_row + i, row_ptr_base,
                offset_lo, offset_hi);
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
  uint32_t* offsets = output->mutable_data_as<uint32_t>(1) + output_start_row;
  uint32_t current_length = (output_start_row == 0) ? 0 : offsets[0];
  int num_rows_processed = RowArrayAccessor::Visit_avx2(
      rows_, column_id, num_rows_to_append, row_ids,
      [&](int i, const uint8_t* row_ptr_base, __m256i offset_lo, __m256i offset_hi,
          __m256i num_bytes) {
        current_length = Decode8Offset_avx2(offsets + i, current_length, num_bytes);
      });
  offsets[num_rows_processed] = current_length;
  return num_rows_processed;
}

int RowArray::DecodeVarLength_avx2(ResizableArrayData* output, int output_start_row,
                                   int column_id, int num_rows_to_append,
                                   const uint32_t* row_ids) const {
  RowArrayAccessor::Visit(
      rows_, column_id, num_rows_to_append, row_ids,
      [&](int i, const uint8_t* row_ptr, uint32_t num_bytes) {
        uint8_t* dst = output->mutable_data(2) +
                       output->mutable_data_as<uint32_t>(1)[output_start_row + i];
        Decode1_avx2(dst, row_ptr, num_bytes);
      });
  return num_rows_to_append;
}

int RowArray::DecodeNulls_avx2(ResizableArrayData* output, int output_start_row,
                               int column_id, int num_rows_to_append,
                               const uint32_t* row_ids) const {
  DCHECK_EQ(output_start_row % 8, 0);

  return RowArrayAccessor::VisitNulls_avx2(
      rows_, column_id, num_rows_to_append, row_ids, [&](int i, uint64_t null_bytes) {
        DCHECK_EQ(i % 8, 0);
        Decode8Null_avx2(output->mutable_data(0) + (output_start_row + i) / 8,
                         null_bytes);
      });
}

}  // namespace acero
}  // namespace arrow

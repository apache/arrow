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

#include "arrow/compute/row/compare_internal.h"
#include "arrow/compute/util.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

#if defined(ARROW_HAVE_AVX2)

inline __m256i set_first_n_bytes_avx2(int n) {
  constexpr uint64_t kByteSequence0To7 = 0x0706050403020100ULL;
  constexpr uint64_t kByteSequence8To15 = 0x0f0e0d0c0b0a0908ULL;
  constexpr uint64_t kByteSequence16To23 = 0x1716151413121110ULL;
  constexpr uint64_t kByteSequence24To31 = 0x1f1e1d1c1b1a1918ULL;

  return _mm256_cmpgt_epi8(_mm256_set1_epi8(n),
                           _mm256_setr_epi64x(kByteSequence0To7, kByteSequence8To15,
                                              kByteSequence16To23, kByteSequence24To31));
}

template <bool use_selection>
uint32_t KeyCompare::NullUpdateColumnToRowImp_avx2(
    uint32_t id_col, uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, LightContext* ctx, const KeyColumnArray& col,
    const RowTableImpl& rows, uint8_t* match_bytevector) {
  if (!rows.has_any_nulls(ctx) && !col.data(0)) {
    return num_rows_to_compare;
  }

  uint32_t null_bit_id = rows.metadata().pos_after_encoding(id_col);

  if (!col.data(0)) {
    // Remove rows from the result for which the column value is a null
    const uint8_t* null_masks = rows.null_masks();
    uint32_t null_mask_num_bytes = rows.metadata().null_masks_bytes_per_row;

    uint32_t num_processed = 0;
    constexpr uint32_t unroll = 8;
    for (uint32_t i = 0; i < num_rows_to_compare / unroll; ++i) {
      __m256i irow_right;
      if (use_selection) {
        __m256i irow_left = _mm256_cvtepu16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(sel_left_maybe_null) + i));
        irow_right = _mm256_i32gather_epi32((const int*)left_to_right_map, irow_left, 4);
      } else {
        irow_right =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left_to_right_map) + i);
      }
      __m256i bitid =
          _mm256_mullo_epi32(irow_right, _mm256_set1_epi32(null_mask_num_bytes * 8));
      bitid = _mm256_add_epi32(bitid, _mm256_set1_epi32(null_bit_id));
      __m256i right =
          _mm256_i32gather_epi32((const int*)null_masks, _mm256_srli_epi32(bitid, 3), 1);
      right = _mm256_and_si256(
          _mm256_set1_epi32(1),
          _mm256_srlv_epi32(right, _mm256_and_si256(bitid, _mm256_set1_epi32(7))));
      __m256i cmp = _mm256_cmpeq_epi32(right, _mm256_setzero_si256());
      uint32_t result_lo =
          _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(cmp)));
      uint32_t result_hi =
          _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_extracti128_si256(cmp, 1)));
      reinterpret_cast<uint64_t*>(match_bytevector)[i] &=
          result_lo | (static_cast<uint64_t>(result_hi) << 32);
    }
    num_processed = num_rows_to_compare / unroll * unroll;
    return num_processed;
  } else if (!rows.has_any_nulls(ctx)) {
    // Remove rows from the result for which the column value on left side is
    // null
    const uint8_t* non_nulls = col.data(0);
    ARROW_DCHECK(non_nulls);
    uint32_t num_processed = 0;
    constexpr uint32_t unroll = 8;
    for (uint32_t i = 0; i < num_rows_to_compare / unroll; ++i) {
      __m256i cmp;
      if (use_selection) {
        __m256i irow_left = _mm256_cvtepu16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(sel_left_maybe_null) + i));
        irow_left = _mm256_add_epi32(irow_left, _mm256_set1_epi32(col.bit_offset(0)));
        __m256i left = _mm256_i32gather_epi32((const int*)non_nulls,
                                              _mm256_srli_epi32(irow_left, 3), 1);
        left = _mm256_and_si256(
            _mm256_set1_epi32(1),
            _mm256_srlv_epi32(left, _mm256_and_si256(irow_left, _mm256_set1_epi32(7))));
        cmp = _mm256_cmpeq_epi32(left, _mm256_set1_epi32(1));
      } else {
        __m256i left = _mm256_cvtepu8_epi32(_mm_set1_epi8(static_cast<uint8_t>(
            reinterpret_cast<const uint16_t*>(non_nulls + i)[0] >> col.bit_offset(0))));
        __m256i bits = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
        cmp = _mm256_cmpeq_epi32(_mm256_and_si256(left, bits), bits);
      }
      uint32_t result_lo =
          _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(cmp)));
      uint32_t result_hi =
          _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_extracti128_si256(cmp, 1)));
      reinterpret_cast<uint64_t*>(match_bytevector)[i] &=
          result_lo | (static_cast<uint64_t>(result_hi) << 32);
      num_processed = num_rows_to_compare / unroll * unroll;
    }
    return num_processed;
  } else {
    const uint8_t* null_masks = rows.null_masks();
    uint32_t null_mask_num_bytes = rows.metadata().null_masks_bytes_per_row;
    const uint8_t* non_nulls = col.data(0);
    ARROW_DCHECK(non_nulls);

    uint32_t num_processed = 0;
    constexpr uint32_t unroll = 8;
    for (uint32_t i = 0; i < num_rows_to_compare / unroll; ++i) {
      __m256i left_null;
      __m256i irow_right;
      if (use_selection) {
        __m256i irow_left = _mm256_cvtepu16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(sel_left_maybe_null) + i));
        irow_right = _mm256_i32gather_epi32((const int*)left_to_right_map, irow_left, 4);
        irow_left = _mm256_add_epi32(irow_left, _mm256_set1_epi32(col.bit_offset(0)));
        __m256i left = _mm256_i32gather_epi32((const int*)non_nulls,
                                              _mm256_srli_epi32(irow_left, 3), 1);
        left = _mm256_and_si256(
            _mm256_set1_epi32(1),
            _mm256_srlv_epi32(left, _mm256_and_si256(irow_left, _mm256_set1_epi32(7))));
        left_null = _mm256_cmpeq_epi32(left, _mm256_setzero_si256());
      } else {
        irow_right =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left_to_right_map) + i);
        __m256i left = _mm256_cvtepu8_epi32(_mm_set1_epi8(static_cast<uint8_t>(
            reinterpret_cast<const uint16_t*>(non_nulls + i)[0] >> col.bit_offset(0))));
        __m256i bits = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
        left_null =
            _mm256_cmpeq_epi32(_mm256_and_si256(left, bits), _mm256_setzero_si256());
      }
      __m256i bitid =
          _mm256_mullo_epi32(irow_right, _mm256_set1_epi32(null_mask_num_bytes * 8));
      bitid = _mm256_add_epi32(bitid, _mm256_set1_epi32(null_bit_id));
      __m256i right =
          _mm256_i32gather_epi32((const int*)null_masks, _mm256_srli_epi32(bitid, 3), 1);
      right = _mm256_and_si256(
          _mm256_set1_epi32(1),
          _mm256_srlv_epi32(right, _mm256_and_si256(bitid, _mm256_set1_epi32(7))));
      __m256i right_null = _mm256_cmpeq_epi32(right, _mm256_set1_epi32(1));

      uint64_t left_null_64 =
          static_cast<uint32_t>(_mm256_movemask_epi8(
              _mm256_cvtepi32_epi64(_mm256_castsi256_si128(left_null)))) |
          (static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(
               _mm256_cvtepi32_epi64(_mm256_extracti128_si256(left_null, 1)))))
           << 32);

      uint64_t right_null_64 =
          static_cast<uint32_t>(_mm256_movemask_epi8(
              _mm256_cvtepi32_epi64(_mm256_castsi256_si128(right_null)))) |
          (static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(
               _mm256_cvtepi32_epi64(_mm256_extracti128_si256(right_null, 1)))))
           << 32);

      reinterpret_cast<uint64_t*>(match_bytevector)[i] |= left_null_64 & right_null_64;
      reinterpret_cast<uint64_t*>(match_bytevector)[i] &= ~(left_null_64 ^ right_null_64);
    }
    num_processed = num_rows_to_compare / unroll * unroll;
    return num_processed;
  }
}

template <bool use_selection, class COMPARE8_FN>
uint32_t KeyCompare::CompareBinaryColumnToRowHelper_avx2(
    uint32_t offset_within_row, uint32_t num_rows_to_compare,
    const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
    LightContext* ctx, const KeyColumnArray& col, const RowTableImpl& rows,
    uint8_t* match_bytevector, COMPARE8_FN compare8_fn) {
  bool is_fixed_length = rows.metadata().is_fixed_length;
  if (is_fixed_length) {
    uint32_t fixed_length = rows.metadata().fixed_length;
    const uint8_t* rows_left = col.data(1);
    const uint8_t* rows_right = rows.data(1);
    constexpr uint32_t unroll = 8;
    __m256i irow_left = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
    for (uint32_t i = 0; i < num_rows_to_compare / unroll; ++i) {
      if (use_selection) {
        irow_left = _mm256_cvtepu16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(sel_left_maybe_null) + i));
      }
      __m256i irow_right;
      if (use_selection) {
        irow_right = _mm256_i32gather_epi32((const int*)left_to_right_map, irow_left, 4);
      } else {
        irow_right =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left_to_right_map) + i);
      }

      __m256i offset_right =
          _mm256_mullo_epi32(irow_right, _mm256_set1_epi32(fixed_length));
      offset_right = _mm256_add_epi32(offset_right, _mm256_set1_epi32(offset_within_row));

      reinterpret_cast<uint64_t*>(match_bytevector)[i] =
          compare8_fn(rows_left, rows_right, i * unroll, irow_left, offset_right);

      if (!use_selection) {
        irow_left = _mm256_add_epi32(irow_left, _mm256_set1_epi32(8));
      }
    }
    return num_rows_to_compare - (num_rows_to_compare % unroll);
  } else {
    const uint8_t* rows_left = col.data(1);
    const uint32_t* offsets_right = rows.offsets();
    const uint8_t* rows_right = rows.data(2);
    constexpr uint32_t unroll = 8;
    __m256i irow_left = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
    for (uint32_t i = 0; i < num_rows_to_compare / unroll; ++i) {
      if (use_selection) {
        irow_left = _mm256_cvtepu16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(sel_left_maybe_null) + i));
      }
      __m256i irow_right;
      if (use_selection) {
        irow_right = _mm256_i32gather_epi32((const int*)left_to_right_map, irow_left, 4);
      } else {
        irow_right =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(left_to_right_map) + i);
      }
      __m256i offset_right =
          _mm256_i32gather_epi32((const int*)offsets_right, irow_right, 4);
      offset_right = _mm256_add_epi32(offset_right, _mm256_set1_epi32(offset_within_row));

      reinterpret_cast<uint64_t*>(match_bytevector)[i] =
          compare8_fn(rows_left, rows_right, i * unroll, irow_left, offset_right);

      if (!use_selection) {
        irow_left = _mm256_add_epi32(irow_left, _mm256_set1_epi32(8));
      }
    }
    return num_rows_to_compare - (num_rows_to_compare % unroll);
  }
}

template <int column_width>
inline uint64_t CompareSelected8_avx2(const uint8_t* left_base, const uint8_t* right_base,
                                      __m256i irow_left, __m256i offset_right,
                                      int bit_offset = 0) {
  __m256i left;
  switch (column_width) {
    case 0: {
      irow_left = _mm256_add_epi32(irow_left, _mm256_set1_epi32(bit_offset));
      left = _mm256_i32gather_epi32((const int*)left_base,
                                    _mm256_srli_epi32(irow_left, 5), 4);
      __m256i bit_selection = _mm256_sllv_epi32(
          _mm256_set1_epi32(1), _mm256_and_si256(irow_left, _mm256_set1_epi32(31)));
      left = _mm256_cmpeq_epi32(bit_selection, _mm256_and_si256(left, bit_selection));
      left = _mm256_and_si256(left, _mm256_set1_epi32(0xff));
    } break;
    case 1:
      left = _mm256_i32gather_epi32((const int*)left_base, irow_left, 1);
      left = _mm256_and_si256(left, _mm256_set1_epi32(0xff));
      break;
    case 2:
      left = _mm256_i32gather_epi32((const int*)left_base, irow_left, 2);
      left = _mm256_and_si256(left, _mm256_set1_epi32(0xffff));
      break;
    case 4:
      left = _mm256_i32gather_epi32((const int*)left_base, irow_left, 4);
      break;
    default:
      ARROW_DCHECK(false);
  }

  __m256i right = _mm256_i32gather_epi32((const int*)right_base, offset_right, 1);
  if (column_width != sizeof(uint32_t)) {
    constexpr uint32_t mask = column_width == 0 || column_width == 1 ? 0xff : 0xffff;
    right = _mm256_and_si256(right, _mm256_set1_epi32(mask));
  }

  __m256i cmp = _mm256_cmpeq_epi32(left, right);

  uint32_t result_lo =
      _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(cmp)));
  uint32_t result_hi =
      _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_extracti128_si256(cmp, 1)));

  return result_lo | (static_cast<uint64_t>(result_hi) << 32);
}

template <int column_width>
inline uint64_t Compare8_avx2(const uint8_t* left_base, const uint8_t* right_base,
                              uint32_t irow_left_first, __m256i offset_right,
                              int bit_offset = 0) {
  __m256i left;
  switch (column_width) {
    case 0: {
      __m256i bits = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
      uint32_t start_bit_index = irow_left_first + bit_offset;
      uint8_t left_bits_8 =
          (reinterpret_cast<const uint16_t*>(left_base + start_bit_index / 8)[0] >>
           (start_bit_index % 8)) &
          0xff;
      left =
          _mm256_cmpeq_epi32(_mm256_and_si256(bits, _mm256_set1_epi8(left_bits_8)), bits);
      left = _mm256_and_si256(left, _mm256_set1_epi32(0xff));
    } break;
    case 1:
      left = _mm256_cvtepu8_epi32(_mm_set1_epi64x(
          *reinterpret_cast<const uint64_t*>(left_base + irow_left_first)));
      break;
    case 2:
      left = _mm256_cvtepu16_epi32(_mm_loadu_si128(
          reinterpret_cast<const __m128i*>(left_base + 2 * irow_left_first)));
      break;
    case 4:
      left = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(left_base + 4 * irow_left_first));
      break;
    default:
      ARROW_DCHECK(false);
  }

  __m256i right = _mm256_i32gather_epi32((const int*)right_base, offset_right, 1);
  if (column_width != sizeof(uint32_t)) {
    constexpr uint32_t mask = column_width == 0 || column_width == 1 ? 0xff : 0xffff;
    right = _mm256_and_si256(right, _mm256_set1_epi32(mask));
  }

  __m256i cmp = _mm256_cmpeq_epi32(left, right);

  uint32_t result_lo =
      _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_castsi256_si128(cmp)));
  uint32_t result_hi =
      _mm256_movemask_epi8(_mm256_cvtepi32_epi64(_mm256_extracti128_si256(cmp, 1)));

  return result_lo | (static_cast<uint64_t>(result_hi) << 32);
}

template <bool use_selection>
inline uint64_t Compare8_64bit_avx2(const uint8_t* left_base, const uint8_t* right_base,
                                    __m256i irow_left, uint32_t irow_left_first,
                                    __m256i offset_right) {
  auto left_base_i64 =
      reinterpret_cast<const arrow::util::int64_for_gather_t*>(left_base);
  __m256i left_lo, left_hi;
  if (use_selection) {
    left_lo = _mm256_i32gather_epi64(left_base_i64, _mm256_castsi256_si128(irow_left), 8);
    left_hi =
        _mm256_i32gather_epi64(left_base_i64, _mm256_extracti128_si256(irow_left, 1), 8);
  } else {
    left_lo = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(left_base + irow_left_first * sizeof(uint64_t)));
    left_hi = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(left_base + irow_left_first * sizeof(uint64_t)) +
        1);
  }
  auto right_base_i64 =
      reinterpret_cast<const arrow::util::int64_for_gather_t*>(right_base);
  __m256i right_lo =
      _mm256_i32gather_epi64(right_base_i64, _mm256_castsi256_si128(offset_right), 1);
  __m256i right_hi = _mm256_i32gather_epi64(right_base_i64,
                                            _mm256_extracti128_si256(offset_right, 1), 1);
  uint32_t result_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi64(left_lo, right_lo));
  uint32_t result_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi64(left_hi, right_hi));
  return result_lo | (static_cast<uint64_t>(result_hi) << 32);
}

template <bool use_selection>
inline uint64_t Compare8_Binary_avx2(uint32_t length, const uint8_t* left_base,
                                     const uint8_t* right_base, __m256i irow_left,
                                     uint32_t irow_left_first, __m256i offset_right) {
  uint32_t irow_left_array[8];
  uint32_t offset_right_array[8];
  if (use_selection) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(irow_left_array), irow_left);
  }
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(offset_right_array), offset_right);

  // Non-zero length guarantees no underflow
  int32_t num_loops_less_one = (static_cast<int32_t>(length) + 31) / 32 - 1;

  __m256i tail_mask = set_first_n_bytes_avx2(length - num_loops_less_one * 32);

  uint64_t result = 0;
  for (uint32_t irow = 0; irow < 8; ++irow) {
    const __m256i* key_left_ptr = reinterpret_cast<const __m256i*>(
        left_base +
        (use_selection ? irow_left_array[irow] : irow_left_first + irow) * length);
    const __m256i* key_right_ptr =
        reinterpret_cast<const __m256i*>(right_base + offset_right_array[irow]);
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
    uint64_t result_single = _mm256_testz_si256(result_or, result_or) * 0xff;
    result |= result_single << (8 * irow);
  }
  return result;
}

template <bool use_selection>
uint32_t KeyCompare::CompareBinaryColumnToRowImp_avx2(
    uint32_t offset_within_row, uint32_t num_rows_to_compare,
    const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
    LightContext* ctx, const KeyColumnArray& col, const RowTableImpl& rows,
    uint8_t* match_bytevector) {
  uint32_t col_width = col.metadata().fixed_length;
  if (col_width == 0) {
    int bit_offset = col.bit_offset(1);
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [bit_offset](const uint8_t* left_base, const uint8_t* right_base,
                     uint32_t irow_left_base, __m256i irow_left, __m256i offset_right) {
          if (use_selection) {
            return CompareSelected8_avx2<0>(left_base, right_base, irow_left,
                                            offset_right, bit_offset);
          } else {
            return Compare8_avx2<0>(left_base, right_base, irow_left_base, offset_right,
                                    bit_offset);
          }
        });
  } else if (col_width == 1) {
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left_base,
           __m256i irow_left, __m256i offset_right) {
          if (use_selection) {
            return CompareSelected8_avx2<1>(left_base, right_base, irow_left,
                                            offset_right);
          } else {
            return Compare8_avx2<1>(left_base, right_base, irow_left_base, offset_right);
          }
        });
  } else if (col_width == 2) {
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left_base,
           __m256i irow_left, __m256i offset_right) {
          if (use_selection) {
            return CompareSelected8_avx2<2>(left_base, right_base, irow_left,
                                            offset_right);
          } else {
            return Compare8_avx2<2>(left_base, right_base, irow_left_base, offset_right);
          }
        });
  } else if (col_width == 4) {
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left_base,
           __m256i irow_left, __m256i offset_right) {
          if (use_selection) {
            return CompareSelected8_avx2<4>(left_base, right_base, irow_left,
                                            offset_right);
          } else {
            return Compare8_avx2<4>(left_base, right_base, irow_left_base, offset_right);
          }
        });
  } else if (col_width == 8) {
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left_base,
           __m256i irow_left, __m256i offset_right) {
          return Compare8_64bit_avx2<use_selection>(left_base, right_base, irow_left,
                                                    irow_left_base, offset_right);
        });
  } else {
    return CompareBinaryColumnToRowHelper_avx2<use_selection>(
        offset_within_row, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
        ctx, col, rows, match_bytevector,
        [&col](const uint8_t* left_base, const uint8_t* right_base,
               uint32_t irow_left_base, __m256i irow_left, __m256i offset_right) {
          uint32_t length = col.metadata().fixed_length;
          return Compare8_Binary_avx2<use_selection>(
              length, left_base, right_base, irow_left, irow_left_base, offset_right);
        });
  }
}

// Overwrites the match_bytevector instead of updating it
template <bool use_selection, bool is_first_varbinary_col>
void KeyCompare::CompareVarBinaryColumnToRowImp_avx2(
    uint32_t id_varbinary_col, uint32_t num_rows_to_compare,
    const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
    LightContext* ctx, const KeyColumnArray& col, const RowTableImpl& rows,
    uint8_t* match_bytevector) {
  const uint32_t* offsets_left = col.offsets();
  const uint32_t* offsets_right = rows.offsets();
  const uint8_t* rows_left = col.data(2);
  const uint8_t* rows_right = rows.data(2);
  for (uint32_t i = 0; i < num_rows_to_compare; ++i) {
    uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
    uint32_t irow_right = left_to_right_map[irow_left];
    uint32_t begin_left = offsets_left[irow_left];
    uint32_t length_left = offsets_left[irow_left + 1] - begin_left;
    uint32_t begin_right = offsets_right[irow_right];
    uint32_t length_right;
    uint32_t offset_within_row;
    if (!is_first_varbinary_col) {
      rows.metadata().nth_varbinary_offset_and_length(
          rows_right + begin_right, id_varbinary_col, &offset_within_row, &length_right);
    } else {
      rows.metadata().first_varbinary_offset_and_length(
          rows_right + begin_right, &offset_within_row, &length_right);
    }
    begin_right += offset_within_row;

    __m256i result_or = _mm256_setzero_si256();
    uint32_t length = std::min(length_left, length_right);
    if (length > 0) {
      const __m256i* key_left_ptr =
          reinterpret_cast<const __m256i*>(rows_left + begin_left);
      const __m256i* key_right_ptr =
          reinterpret_cast<const __m256i*>(rows_right + begin_right);
      int32_t j;
      // length is greater than zero
      for (j = 0; j < (static_cast<int32_t>(length) + 31) / 32 - 1; ++j) {
        __m256i key_left = _mm256_loadu_si256(key_left_ptr + j);
        __m256i key_right = _mm256_loadu_si256(key_right_ptr + j);
        result_or = _mm256_or_si256(result_or, _mm256_xor_si256(key_left, key_right));
      }

      __m256i tail_mask = set_first_n_bytes_avx2(length - j * 32);

      __m256i key_left = _mm256_loadu_si256(key_left_ptr + j);
      __m256i key_right = _mm256_loadu_si256(key_right_ptr + j);
      result_or = _mm256_or_si256(
          result_or, _mm256_and_si256(tail_mask, _mm256_xor_si256(key_left, key_right)));
    }
    int result = _mm256_testz_si256(result_or, result_or) * 0xff;
    result *= (length_left == length_right ? 1 : 0);
    match_bytevector[i] = result;
  }
}

uint32_t KeyCompare::AndByteVectors_avx2(uint32_t num_elements, uint8_t* bytevector_A,
                                         const uint8_t* bytevector_B) {
  constexpr int unroll = 32;
  for (uint32_t i = 0; i < num_elements / unroll; ++i) {
    __m256i result = _mm256_and_si256(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytevector_A) + i),
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytevector_B) + i));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(bytevector_A) + i, result);
  }
  return (num_elements - (num_elements % unroll));
}

uint32_t KeyCompare::NullUpdateColumnToRow_avx2(
    bool use_selection, uint32_t id_col, uint32_t num_rows_to_compare,
    const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
    LightContext* ctx, const KeyColumnArray& col, const RowTableImpl& rows,
    uint8_t* match_bytevector) {
  int64_t num_rows_safe =
      TailSkipForSIMD::FixBitAccess(sizeof(uint32_t), col.length(), col.bit_offset(0));
  if (sel_left_maybe_null) {
    num_rows_to_compare = static_cast<uint32_t>(TailSkipForSIMD::FixSelection(
        num_rows_safe, static_cast<int>(num_rows_to_compare), sel_left_maybe_null));
  } else {
    num_rows_to_compare = static_cast<uint32_t>(num_rows_safe);
  }

  if (use_selection) {
    return NullUpdateColumnToRowImp_avx2<true>(id_col, num_rows_to_compare,
                                               sel_left_maybe_null, left_to_right_map,
                                               ctx, col, rows, match_bytevector);
  } else {
    return NullUpdateColumnToRowImp_avx2<false>(id_col, num_rows_to_compare,
                                                sel_left_maybe_null, left_to_right_map,
                                                ctx, col, rows, match_bytevector);
  }
}

uint32_t KeyCompare::CompareBinaryColumnToRow_avx2(
    bool use_selection, uint32_t offset_within_row, uint32_t num_rows_to_compare,
    const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
    LightContext* ctx, const KeyColumnArray& col, const RowTableImpl& rows,
    uint8_t* match_bytevector) {
  uint32_t col_width = col.metadata().fixed_length;
  int64_t num_rows_safe = col.length();
  if (col_width == 0) {
    // In this case we will access left column memory 4B at a time
    num_rows_safe =
        TailSkipForSIMD::FixBitAccess(sizeof(uint32_t), col.length(), col.bit_offset(1));
  } else if (col_width == 1 || col_width == 2) {
    // In this case we will access left column memory 4B at a time
    num_rows_safe =
        TailSkipForSIMD::FixBinaryAccess(sizeof(uint32_t), col.length(), col_width);
  } else if (col_width != 4 && col_width != 8) {
    // In this case we will access left column memory 32B at a time
    num_rows_safe =
        TailSkipForSIMD::FixBinaryAccess(sizeof(__m256i), col.length(), col_width);
  }
  if (sel_left_maybe_null) {
    num_rows_to_compare = static_cast<uint32_t>(TailSkipForSIMD::FixSelection(
        num_rows_safe, static_cast<int>(num_rows_to_compare), sel_left_maybe_null));
  } else {
    num_rows_to_compare = static_cast<uint32_t>(
        std::min(num_rows_safe, static_cast<int64_t>(num_rows_to_compare)));
  }

  if (use_selection) {
    return CompareBinaryColumnToRowImp_avx2<true>(offset_within_row, num_rows_to_compare,
                                                  sel_left_maybe_null, left_to_right_map,
                                                  ctx, col, rows, match_bytevector);
  } else {
    return CompareBinaryColumnToRowImp_avx2<false>(offset_within_row, num_rows_to_compare,
                                                   sel_left_maybe_null, left_to_right_map,
                                                   ctx, col, rows, match_bytevector);
  }
}

uint32_t KeyCompare::CompareVarBinaryColumnToRow_avx2(
    bool use_selection, bool is_first_varbinary_col, uint32_t id_varlen_col,
    uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, LightContext* ctx, const KeyColumnArray& col,
    const RowTableImpl& rows, uint8_t* match_bytevector) {
  int64_t num_rows_safe =
      TailSkipForSIMD::FixVarBinaryAccess(sizeof(__m256i), col.length(), col.offsets());
  if (use_selection) {
    num_rows_to_compare = static_cast<uint32_t>(TailSkipForSIMD::FixSelection(
        num_rows_safe, static_cast<int>(num_rows_to_compare), sel_left_maybe_null));
  } else {
    num_rows_to_compare = static_cast<uint32_t>(num_rows_safe);
  }

  if (use_selection) {
    if (is_first_varbinary_col) {
      CompareVarBinaryColumnToRowImp_avx2<true, true>(
          id_varlen_col, num_rows_to_compare, sel_left_maybe_null, left_to_right_map, ctx,
          col, rows, match_bytevector);
    } else {
      CompareVarBinaryColumnToRowImp_avx2<true, false>(
          id_varlen_col, num_rows_to_compare, sel_left_maybe_null, left_to_right_map, ctx,
          col, rows, match_bytevector);
    }
  } else {
    if (is_first_varbinary_col) {
      CompareVarBinaryColumnToRowImp_avx2<false, true>(
          id_varlen_col, num_rows_to_compare, sel_left_maybe_null, left_to_right_map, ctx,
          col, rows, match_bytevector);
    } else {
      CompareVarBinaryColumnToRowImp_avx2<false, false>(
          id_varlen_col, num_rows_to_compare, sel_left_maybe_null, left_to_right_map, ctx,
          col, rows, match_bytevector);
    }
  }

  return num_rows_to_compare;
}

#endif

}  // namespace compute
}  // namespace arrow

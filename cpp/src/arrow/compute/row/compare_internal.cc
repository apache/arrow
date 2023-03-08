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

#include "arrow/compute/row/compare_internal.h"

#include <memory.h>

#include <algorithm>
#include <cstdint>

#include "arrow/compute/util.h"
#include "arrow/compute/util_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

template <bool use_selection>
void KeyCompare::NullUpdateColumnToRow(uint32_t id_col, uint32_t num_rows_to_compare,
                                       const uint16_t* sel_left_maybe_null,
                                       const uint32_t* left_to_right_map,
                                       LightContext* ctx, const KeyColumnArray& col,
                                       const RowTableImpl& rows,
                                       uint8_t* match_bytevector,
                                       bool are_cols_in_encoding_order) {
  if (!rows.has_any_nulls(ctx) && !col.data(0)) {
    return;
  }
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    num_processed = NullUpdateColumnToRow_avx2(use_selection, id_col, num_rows_to_compare,
                                               sel_left_maybe_null, left_to_right_map,
                                               ctx, col, rows, match_bytevector);
  }
#endif

  uint32_t null_bit_id =
      are_cols_in_encoding_order ? id_col : rows.metadata().pos_after_encoding(id_col);

  if (!col.data(0)) {
    // Remove rows from the result for which the column value is a null
    const uint8_t* null_masks = rows.null_masks();
    uint32_t null_mask_num_bytes = rows.metadata().null_masks_bytes_per_row;
    for (uint32_t i = num_processed; i < num_rows_to_compare; ++i) {
      uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
      uint32_t irow_right = left_to_right_map[irow_left];
      int64_t bitid = irow_right * null_mask_num_bytes * 8 + null_bit_id;
      match_bytevector[i] &= (bit_util::GetBit(null_masks, bitid) ? 0 : 0xff);
    }
  } else if (!rows.has_any_nulls(ctx)) {
    // Remove rows from the result for which the column value on left side is
    // null
    const uint8_t* non_nulls = col.data(0);
    ARROW_DCHECK(non_nulls);
    for (uint32_t i = num_processed; i < num_rows_to_compare; ++i) {
      uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
      match_bytevector[i] &=
          bit_util::GetBit(non_nulls, irow_left + col.bit_offset(0)) ? 0xff : 0;
    }
  } else {
    const uint8_t* null_masks = rows.null_masks();
    uint32_t null_mask_num_bytes = rows.metadata().null_masks_bytes_per_row;
    const uint8_t* non_nulls = col.data(0);
    ARROW_DCHECK(non_nulls);
    for (uint32_t i = num_processed; i < num_rows_to_compare; ++i) {
      uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
      uint32_t irow_right = left_to_right_map[irow_left];
      int64_t bitid_right = irow_right * null_mask_num_bytes * 8 + null_bit_id;
      int right_null = bit_util::GetBit(null_masks, bitid_right) ? 0xff : 0;
      int left_null =
          bit_util::GetBit(non_nulls, irow_left + col.bit_offset(0)) ? 0 : 0xff;
      match_bytevector[i] |= left_null & right_null;
      match_bytevector[i] &= ~(left_null ^ right_null);
    }
  }
}

template <bool use_selection, class COMPARE_FN>
void KeyCompare::CompareBinaryColumnToRowHelper(
    uint32_t offset_within_row, uint32_t first_row_to_compare,
    uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, LightContext* ctx, const KeyColumnArray& col,
    const RowTableImpl& rows, uint8_t* match_bytevector, COMPARE_FN compare_fn) {
  bool is_fixed_length = rows.metadata().is_fixed_length;
  if (is_fixed_length) {
    uint32_t fixed_length = rows.metadata().fixed_length;
    const uint8_t* rows_left = col.data(1);
    const uint8_t* rows_right = rows.data(1);
    for (uint32_t i = first_row_to_compare; i < num_rows_to_compare; ++i) {
      uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
      uint32_t irow_right = left_to_right_map[irow_left];
      uint32_t offset_right = irow_right * fixed_length + offset_within_row;
      match_bytevector[i] = compare_fn(rows_left, rows_right, irow_left, offset_right);
    }
  } else {
    const uint8_t* rows_left = col.data(1);
    const uint32_t* offsets_right = rows.offsets();
    const uint8_t* rows_right = rows.data(2);
    for (uint32_t i = first_row_to_compare; i < num_rows_to_compare; ++i) {
      uint32_t irow_left = use_selection ? sel_left_maybe_null[i] : i;
      uint32_t irow_right = left_to_right_map[irow_left];
      uint32_t offset_right = offsets_right[irow_right] + offset_within_row;
      match_bytevector[i] = compare_fn(rows_left, rows_right, irow_left, offset_right);
    }
  }
}

template <bool use_selection>
void KeyCompare::CompareBinaryColumnToRow(uint32_t offset_within_row,
                                          uint32_t num_rows_to_compare,
                                          const uint16_t* sel_left_maybe_null,
                                          const uint32_t* left_to_right_map,
                                          LightContext* ctx, const KeyColumnArray& col,
                                          const RowTableImpl& rows,
                                          uint8_t* match_bytevector) {
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    num_processed = CompareBinaryColumnToRow_avx2(
        use_selection, offset_within_row, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector);
  }
#endif

  uint32_t col_width = col.metadata().fixed_length;
  if (col_width == 0) {
    int bit_offset = col.bit_offset(1);
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [bit_offset](const uint8_t* left_base, const uint8_t* right_base,
                     uint32_t irow_left, uint32_t offset_right) {
          uint8_t left =
              bit_util::GetBit(left_base, irow_left + bit_offset) ? 0xff : 0x00;
          uint8_t right = right_base[offset_right];
          return left == right ? 0xff : 0;
        });
  } else if (col_width == 1) {
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left,
           uint32_t offset_right) {
          uint8_t left = left_base[irow_left];
          uint8_t right = right_base[offset_right];
          return left == right ? 0xff : 0;
        });
  } else if (col_width == 2) {
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left,
           uint32_t offset_right) {
          util::CheckAlignment<uint16_t>(left_base);
          util::CheckAlignment<uint16_t>(right_base + offset_right);
          uint16_t left = reinterpret_cast<const uint16_t*>(left_base)[irow_left];
          uint16_t right = *reinterpret_cast<const uint16_t*>(right_base + offset_right);
          return left == right ? 0xff : 0;
        });
  } else if (col_width == 4) {
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left,
           uint32_t offset_right) {
          util::CheckAlignment<uint32_t>(left_base);
          util::CheckAlignment<uint32_t>(right_base + offset_right);
          uint32_t left = reinterpret_cast<const uint32_t*>(left_base)[irow_left];
          uint32_t right = *reinterpret_cast<const uint32_t*>(right_base + offset_right);
          return left == right ? 0xff : 0;
        });
  } else if (col_width == 8) {
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left,
           uint32_t offset_right) {
          util::CheckAlignment<uint64_t>(left_base);
          util::CheckAlignment<uint64_t>(right_base + offset_right);
          uint64_t left = reinterpret_cast<const uint64_t*>(left_base)[irow_left];
          uint64_t right = *reinterpret_cast<const uint64_t*>(right_base + offset_right);
          return left == right ? 0xff : 0;
        });
  } else {
    CompareBinaryColumnToRowHelper<use_selection>(
        offset_within_row, num_processed, num_rows_to_compare, sel_left_maybe_null,
        left_to_right_map, ctx, col, rows, match_bytevector,
        [&col](const uint8_t* left_base, const uint8_t* right_base, uint32_t irow_left,
               uint32_t offset_right) {
          uint32_t length = col.metadata().fixed_length;

          // Non-zero length guarantees no underflow
          int32_t num_loops_less_one =
              static_cast<int32_t>(bit_util::CeilDiv(length, 8)) - 1;

          uint64_t tail_mask = ~0ULL >> (64 - 8 * (length - num_loops_less_one * 8));

          const uint64_t* key_left_ptr =
              reinterpret_cast<const uint64_t*>(left_base + irow_left * length);
          util::CheckAlignment<uint64_t>(right_base + offset_right);
          const uint64_t* key_right_ptr =
              reinterpret_cast<const uint64_t*>(right_base + offset_right);
          uint64_t result_or = 0;
          int32_t i;
          // length cannot be zero
          for (i = 0; i < num_loops_less_one; ++i) {
            uint64_t key_left = util::SafeLoad(key_left_ptr + i);
            uint64_t key_right = key_right_ptr[i];
            result_or |= key_left ^ key_right;
          }
          uint64_t key_left = util::SafeLoad(key_left_ptr + i);
          uint64_t key_right = key_right_ptr[i];
          result_or |= tail_mask & (key_left ^ key_right);
          return result_or == 0 ? 0xff : 0;
        });
  }
}

// Overwrites the match_bytevector instead of updating it
template <bool use_selection, bool is_first_varbinary_col>
void KeyCompare::CompareVarBinaryColumnToRowHelper(
    uint32_t id_varbinary_col, uint32_t first_row_to_compare,
    uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, LightContext* ctx, const KeyColumnArray& col,
    const RowTableImpl& rows, uint8_t* match_bytevector) {
  const uint32_t* offsets_left = col.offsets();
  const uint32_t* offsets_right = rows.offsets();
  const uint8_t* rows_left = col.data(2);
  const uint8_t* rows_right = rows.data(2);
  for (uint32_t i = first_row_to_compare; i < num_rows_to_compare; ++i) {
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
    uint32_t length = std::min(length_left, length_right);
    const uint64_t* key_left_ptr =
        reinterpret_cast<const uint64_t*>(rows_left + begin_left);
    util::CheckAlignment<uint64_t>(rows_right + begin_right);
    const uint64_t* key_right_ptr =
        reinterpret_cast<const uint64_t*>(rows_right + begin_right);
    uint64_t result_or = 0;
    if (length > 0) {
      int32_t j;
      // length can be zero
      for (j = 0; j < static_cast<int32_t>(bit_util::CeilDiv(length, 8)) - 1; ++j) {
        uint64_t key_left = util::SafeLoad(key_left_ptr + j);
        uint64_t key_right = key_right_ptr[j];
        result_or |= key_left ^ key_right;
      }
      int32_t tail_length = length - j * 8;
      uint64_t tail_mask = ~0ULL >> (64 - 8 * tail_length);
      uint64_t key_left = 0;
      std::memcpy(&key_left, key_left_ptr + j, tail_length);
      uint64_t key_right = key_right_ptr[j];
      result_or |= tail_mask & (key_left ^ key_right);
    }
    int result = result_or == 0 ? 0xff : 0;
    result *= (length_left == length_right ? 1 : 0);
    match_bytevector[i] = result;
  }
}

// Overwrites the match_bytevector instead of updating it
template <bool use_selection, bool is_first_varbinary_col>
void KeyCompare::CompareVarBinaryColumnToRow(uint32_t id_varbinary_col,
                                             uint32_t num_rows_to_compare,
                                             const uint16_t* sel_left_maybe_null,
                                             const uint32_t* left_to_right_map,
                                             LightContext* ctx, const KeyColumnArray& col,
                                             const RowTableImpl& rows,
                                             uint8_t* match_bytevector) {
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    num_processed = CompareVarBinaryColumnToRow_avx2(
        use_selection, is_first_varbinary_col, id_varbinary_col, num_rows_to_compare,
        sel_left_maybe_null, left_to_right_map, ctx, col, rows, match_bytevector);
  }
#endif

  CompareVarBinaryColumnToRowHelper<use_selection, is_first_varbinary_col>(
      id_varbinary_col, num_processed, num_rows_to_compare, sel_left_maybe_null,
      left_to_right_map, ctx, col, rows, match_bytevector);
}

void KeyCompare::AndByteVectors(LightContext* ctx, uint32_t num_elements,
                                uint8_t* bytevector_A, const uint8_t* bytevector_B) {
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    num_processed = AndByteVectors_avx2(num_elements, bytevector_A, bytevector_B);
  }
#endif

  for (uint32_t i = num_processed / 8; i < bit_util::CeilDiv(num_elements, 8); ++i) {
    uint64_t* a = reinterpret_cast<uint64_t*>(bytevector_A);
    const uint64_t* b = reinterpret_cast<const uint64_t*>(bytevector_B);
    a[i] &= b[i];
  }
}

void KeyCompare::CompareColumnsToRows(
    uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
    const uint32_t* left_to_right_map, LightContext* ctx, uint32_t* out_num_rows,
    uint16_t* out_sel_left_maybe_same, const std::vector<KeyColumnArray>& cols,
    const RowTableImpl& rows, bool are_cols_in_encoding_order,
    uint8_t* out_match_bitvector_maybe_null) {
  if (num_rows_to_compare == 0) {
    *out_num_rows = 0;
    return;
  }

  // Allocate temporary byte and bit vectors
  auto bytevector_A_holder =
      util::TempVectorHolder<uint8_t>(ctx->stack, num_rows_to_compare);
  auto bytevector_B_holder =
      util::TempVectorHolder<uint8_t>(ctx->stack, num_rows_to_compare);
  auto bitvector_holder =
      util::TempVectorHolder<uint8_t>(ctx->stack, num_rows_to_compare);

  uint8_t* match_bytevector_A = bytevector_A_holder.mutable_data();
  uint8_t* match_bytevector_B = bytevector_B_holder.mutable_data();
  uint8_t* match_bitvector = bitvector_holder.mutable_data();

  bool is_first_column = true;
  for (size_t icol = 0; icol < cols.size(); ++icol) {
    const KeyColumnArray& col = cols[icol];

    if (col.metadata().is_null_type) {
      // If this null type col is the first column, the match_bytevector_A needs to be
      // initialized with 0xFF. Otherwise, the calculation can be skipped
      if (is_first_column) {
        std::memset(match_bytevector_A, 0xFF, num_rows_to_compare * sizeof(uint8_t));
      }
      continue;
    }

    uint32_t offset_within_row = rows.metadata().encoded_field_offset(
        are_cols_in_encoding_order
            ? static_cast<uint32_t>(icol)
            : rows.metadata().pos_after_encoding(static_cast<uint32_t>(icol)));
    if (col.metadata().is_fixed_length) {
      if (sel_left_maybe_null) {
        CompareBinaryColumnToRow<true>(
            offset_within_row, num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B);
        NullUpdateColumnToRow<true>(
            static_cast<uint32_t>(icol), num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B,
            are_cols_in_encoding_order);
      } else {
        // Version without using selection vector
        CompareBinaryColumnToRow<false>(
            offset_within_row, num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B);
        NullUpdateColumnToRow<false>(
            static_cast<uint32_t>(icol), num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B,
            are_cols_in_encoding_order);
      }
      if (!is_first_column) {
        AndByteVectors(ctx, num_rows_to_compare, match_bytevector_A, match_bytevector_B);
      }
      is_first_column = false;
    }
  }

  uint32_t ivarbinary = 0;
  for (size_t icol = 0; icol < cols.size(); ++icol) {
    const KeyColumnArray& col = cols[icol];
    if (!col.metadata().is_fixed_length) {
      // Process varbinary and nulls
      if (sel_left_maybe_null) {
        if (ivarbinary == 0) {
          CompareVarBinaryColumnToRow<true, true>(
              ivarbinary, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
              ctx, col, rows, is_first_column ? match_bytevector_A : match_bytevector_B);
        } else {
          CompareVarBinaryColumnToRow<true, false>(ivarbinary, num_rows_to_compare,
                                                   sel_left_maybe_null, left_to_right_map,
                                                   ctx, col, rows, match_bytevector_B);
        }
        NullUpdateColumnToRow<true>(
            static_cast<uint32_t>(icol), num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B,
            are_cols_in_encoding_order);
      } else {
        if (ivarbinary == 0) {
          CompareVarBinaryColumnToRow<false, true>(
              ivarbinary, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
              ctx, col, rows, is_first_column ? match_bytevector_A : match_bytevector_B);
        } else {
          CompareVarBinaryColumnToRow<false, false>(
              ivarbinary, num_rows_to_compare, sel_left_maybe_null, left_to_right_map,
              ctx, col, rows, match_bytevector_B);
        }
        NullUpdateColumnToRow<false>(
            static_cast<uint32_t>(icol), num_rows_to_compare, sel_left_maybe_null,
            left_to_right_map, ctx, col, rows,
            is_first_column ? match_bytevector_A : match_bytevector_B,
            are_cols_in_encoding_order);
      }
      if (!is_first_column) {
        AndByteVectors(ctx, num_rows_to_compare, match_bytevector_A, match_bytevector_B);
      }
      is_first_column = false;
      ++ivarbinary;
    }
  }

  util::bit_util::bytes_to_bits(ctx->hardware_flags, num_rows_to_compare,
                                match_bytevector_A, match_bitvector);

  if (out_match_bitvector_maybe_null) {
    ARROW_DCHECK(out_num_rows == nullptr);
    ARROW_DCHECK(out_sel_left_maybe_same == nullptr);
    memcpy(out_match_bitvector_maybe_null, match_bitvector,
           bit_util::BytesForBits(num_rows_to_compare));
  } else {
    if (sel_left_maybe_null) {
      int out_num_rows_int;
      util::bit_util::bits_filter_indexes(0, ctx->hardware_flags, num_rows_to_compare,
                                          match_bitvector, sel_left_maybe_null,
                                          &out_num_rows_int, out_sel_left_maybe_same);
      *out_num_rows = out_num_rows_int;
    } else {
      int out_num_rows_int;
      util::bit_util::bits_to_indexes(0, ctx->hardware_flags, num_rows_to_compare,
                                      match_bitvector, &out_num_rows_int,
                                      out_sel_left_maybe_same);
      *out_num_rows = out_num_rows_int;
    }
  }
}

}  // namespace compute
}  // namespace arrow

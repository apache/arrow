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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/compute/key_map.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/row/row_internal.h"
#include "arrow/compute/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

/// Converts between Arrow's typical column representation to a row-based representation
///
/// Data is stored as a single array of rows.  Each row combines data from all columns.
/// The conversion is reversible.
///
/// Row-oriented storage is beneficial when there is a need for random access
/// of individual rows and at the same time all included columns are likely to
/// be accessed together, as in the case of hash table key.
///
/// Does not support nested types
class ARROW_EXPORT RowTableEncoder {
 public:
  void Init(const std::vector<KeyColumnMetadata>& cols, int row_alignment,
            int string_alignment);

  const RowTableMetadata& row_metadata() { return row_metadata_; }
  // GrouperFastImpl right now needs somewhat intrusive visibility into RowTableEncoder
  // This could be cleaned up at some point
  const std::vector<KeyColumnArray>& batch_all_cols() { return batch_all_cols_; }

  /// \brief Prepare to encode a collection of columns
  /// \param start_row The starting row to encode
  /// \param num_rows The number of rows to encode
  /// \param cols The columns to encode.  The order of the columns should
  ///             be consistent with the order used to create the RowTableMetadata
  void PrepareEncodeSelected(int64_t start_row, int64_t num_rows,
                             const std::vector<KeyColumnArray>& cols);
  /// \brief Encode selection of prepared rows into a row table
  /// \param rows The output row table
  /// \param num_selected The number of rows to encode
  /// \param selection indices of the rows to encode
  Status EncodeSelected(RowTableImpl* rows, uint32_t num_selected,
                        const uint16_t* selection);

  /// \brief Decode a window of row oriented data into a corresponding
  ///        window of column oriented storage.
  /// \param start_row_input The starting row to decode
  /// \param start_row_output An offset into the output array to write to
  /// \param num_rows The number of rows to decode
  /// \param rows The row table to decode from
  /// \param cols The columns to decode into, should be sized appropriately
  ///
  /// The output buffers need to be correctly allocated and sized before
  /// calling each method.  For that reason decoding is split into two functions.
  /// DecodeFixedLengthBuffers processes everything except for varying length
  /// buffers.
  /// The output can be used to find out required varying length buffers sizes
  /// for the call to DecodeVaryingLengthBuffers
  void DecodeFixedLengthBuffers(int64_t start_row_input, int64_t start_row_output,
                                int64_t num_rows, const RowTableImpl& rows,
                                std::vector<KeyColumnArray>* cols, int64_t hardware_flags,
                                util::TempVectorStack* temp_stack);

  /// \brief Decode the varlength columns of a row table into column storage
  /// \param start_row_input The starting row to decode
  /// \param start_row_output An offset into the output arrays
  /// \param num_rows The number of rows to decode
  /// \param rows The row table to decode from
  /// \param cols The column arrays to decode into
  void DecodeVaryingLengthBuffers(int64_t start_row_input, int64_t start_row_output,
                                  int64_t num_rows, const RowTableImpl& rows,
                                  std::vector<KeyColumnArray>* cols,
                                  int64_t hardware_flags,
                                  util::TempVectorStack* temp_stack);

 private:
  /// Prepare column array vectors.
  /// Output column arrays represent a range of input column arrays
  /// specified by starting row and number of rows.
  /// Three vectors are generated:
  /// - all columns
  /// - fixed-length columns only
  /// - varying-length columns only
  void PrepareKeyColumnArrays(int64_t start_row, int64_t num_rows,
                              const std::vector<KeyColumnArray>& cols_in);

  // Data initialized once, based on data types of key columns
  RowTableMetadata row_metadata_;

  // Data initialized for each input batch.
  // All elements are ordered according to the order of encoded fields in a row.
  std::vector<KeyColumnArray> batch_all_cols_;
  std::vector<KeyColumnArray> batch_varbinary_cols_;
  std::vector<uint32_t> batch_varbinary_cols_base_offsets_;
};

class EncoderInteger {
 public:
  static void Decode(uint32_t start_row, uint32_t num_rows, uint32_t offset_within_row,
                     const RowTableImpl& rows, KeyColumnArray* col, LightContext* ctx,
                     KeyColumnArray* temp);
  static bool UsesTransform(const KeyColumnArray& column);
  static KeyColumnArray ArrayReplace(const KeyColumnArray& column,
                                     const KeyColumnArray& temp);
  static void PostDecode(const KeyColumnArray& input, KeyColumnArray* output,
                         LightContext* ctx);

 private:
  static bool IsBoolean(const KeyColumnMetadata& metadata);
};

class EncoderBinary {
 public:
  static void EncodeSelected(uint32_t offset_within_row, RowTableImpl* rows,
                             const KeyColumnArray& col, uint32_t num_selected,
                             const uint16_t* selection);
  static void Decode(uint32_t start_row, uint32_t num_rows, uint32_t offset_within_row,
                     const RowTableImpl& rows, KeyColumnArray* col, LightContext* ctx,
                     KeyColumnArray* temp);
  static bool IsInteger(const KeyColumnMetadata& metadata);

 private:
  template <class COPY_FN, class SET_NULL_FN>
  static void EncodeSelectedImp(uint32_t offset_within_row, RowTableImpl* rows,
                                const KeyColumnArray& col, uint32_t num_selected,
                                const uint16_t* selection, COPY_FN copy_fn,
                                SET_NULL_FN set_null_fn);

  template <bool is_row_fixed_length, class COPY_FN>
  static inline void DecodeHelper(uint32_t start_row, uint32_t num_rows,
                                  uint32_t offset_within_row,
                                  const RowTableImpl* rows_const,
                                  RowTableImpl* rows_mutable_maybe_null,
                                  const KeyColumnArray* col_const,
                                  KeyColumnArray* col_mutable_maybe_null,
                                  COPY_FN copy_fn) {
    ARROW_DCHECK(col_const && col_const->metadata().is_fixed_length);
    uint32_t col_width = col_const->metadata().fixed_length;

    if (is_row_fixed_length) {
      uint32_t row_width = rows_const->metadata().fixed_length;
      for (uint32_t i = 0; i < num_rows; ++i) {
        const uint8_t* src;
        uint8_t* dst;
        src = rows_const->data(1) + row_width * (start_row + i) + offset_within_row;
        dst = col_mutable_maybe_null->mutable_data(1) + col_width * i;
        copy_fn(dst, src, col_width);
      }
    } else {
      const uint32_t* row_offsets = rows_const->offsets();
      for (uint32_t i = 0; i < num_rows; ++i) {
        const uint8_t* src;
        uint8_t* dst;
        src = rows_const->data(2) + row_offsets[start_row + i] + offset_within_row;
        dst = col_mutable_maybe_null->mutable_data(1) + col_width * i;
        copy_fn(dst, src, col_width);
      }
    }
  }

  template <bool is_row_fixed_length>
  static void DecodeImp(uint32_t start_row, uint32_t num_rows, uint32_t offset_within_row,
                        const RowTableImpl& rows, KeyColumnArray* col);
#if defined(ARROW_HAVE_AVX2)
  static void DecodeHelper_avx2(bool is_row_fixed_length, uint32_t start_row,
                                uint32_t num_rows, uint32_t offset_within_row,
                                const RowTableImpl& rows, KeyColumnArray* col);
  template <bool is_row_fixed_length>
  static void DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                             uint32_t offset_within_row, const RowTableImpl& rows,
                             KeyColumnArray* col);
#endif
};

class EncoderBinaryPair {
 public:
  static bool CanProcessPair(const KeyColumnMetadata& col1,
                             const KeyColumnMetadata& col2) {
    return EncoderBinary::IsInteger(col1) && EncoderBinary::IsInteger(col2);
  }
  static void Decode(uint32_t start_row, uint32_t num_rows, uint32_t offset_within_row,
                     const RowTableImpl& rows, KeyColumnArray* col1, KeyColumnArray* col2,
                     LightContext* ctx, KeyColumnArray* temp1, KeyColumnArray* temp2);

 private:
  template <bool is_row_fixed_length, typename col1_type, typename col2_type>
  static void DecodeImp(uint32_t num_rows_to_skip, uint32_t start_row, uint32_t num_rows,
                        uint32_t offset_within_row, const RowTableImpl& rows,
                        KeyColumnArray* col1, KeyColumnArray* col2);
#if defined(ARROW_HAVE_AVX2)
  static uint32_t DecodeHelper_avx2(bool is_row_fixed_length, uint32_t col_width,
                                    uint32_t start_row, uint32_t num_rows,
                                    uint32_t offset_within_row, const RowTableImpl& rows,
                                    KeyColumnArray* col1, KeyColumnArray* col2);
  template <bool is_row_fixed_length, uint32_t col_width>
  static uint32_t DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                                 uint32_t offset_within_row, const RowTableImpl& rows,
                                 KeyColumnArray* col1, KeyColumnArray* col2);
#endif
};

class EncoderOffsets {
 public:
  static void GetRowOffsetsSelected(RowTableImpl* rows,
                                    const std::vector<KeyColumnArray>& cols,
                                    uint32_t num_selected, const uint16_t* selection);
  static void EncodeSelected(RowTableImpl* rows, const std::vector<KeyColumnArray>& cols,
                             uint32_t num_selected, const uint16_t* selection);

  static void Decode(uint32_t start_row, uint32_t num_rows, const RowTableImpl& rows,
                     std::vector<KeyColumnArray>* varbinary_cols,
                     const std::vector<uint32_t>& varbinary_cols_base_offset,
                     LightContext* ctx);

 private:
  template <bool has_nulls, bool is_first_varbinary>
  static void EncodeSelectedImp(uint32_t ivarbinary, RowTableImpl* rows,
                                const std::vector<KeyColumnArray>& cols,
                                uint32_t num_selected, const uint16_t* selection);
};

class EncoderVarBinary {
 public:
  static void EncodeSelected(uint32_t ivarbinary, RowTableImpl* rows,
                             const KeyColumnArray& cols, uint32_t num_selected,
                             const uint16_t* selection);

  static void Decode(uint32_t start_row, uint32_t num_rows, uint32_t varbinary_col_id,
                     const RowTableImpl& rows, KeyColumnArray* col, LightContext* ctx);

 private:
  template <bool first_varbinary_col, class COPY_FN>
  static inline void DecodeHelper(uint32_t start_row, uint32_t num_rows,
                                  uint32_t varbinary_col_id,
                                  const RowTableImpl* rows_const,
                                  RowTableImpl* rows_mutable_maybe_null,
                                  const KeyColumnArray* col_const,
                                  KeyColumnArray* col_mutable_maybe_null,
                                  COPY_FN copy_fn) {
    // Column and rows need to be varying length
    ARROW_DCHECK(!rows_const->metadata().is_fixed_length &&
                 !col_const->metadata().is_fixed_length);

    const uint32_t* row_offsets_for_batch = rows_const->offsets() + start_row;
    const uint32_t* col_offsets = col_const->offsets();

    uint32_t col_offset_next = col_offsets[0];
    for (uint32_t i = 0; i < num_rows; ++i) {
      uint32_t col_offset = col_offset_next;
      col_offset_next = col_offsets[i + 1];

      uint32_t row_offset = row_offsets_for_batch[i];
      const uint8_t* row = rows_const->data(2) + row_offset;

      uint32_t offset_within_row;
      uint32_t length;
      if (first_varbinary_col) {
        rows_const->metadata().first_varbinary_offset_and_length(row, &offset_within_row,
                                                                 &length);
      } else {
        rows_const->metadata().nth_varbinary_offset_and_length(
            row, varbinary_col_id, &offset_within_row, &length);
      }

      row_offset += offset_within_row;

      const uint8_t* src;
      uint8_t* dst;
      src = rows_const->data(2) + row_offset;
      dst = col_mutable_maybe_null->mutable_data(2) + col_offset;
      copy_fn(dst, src, length);
    }
  }
  template <bool first_varbinary_col>
  static void DecodeImp(uint32_t start_row, uint32_t num_rows, uint32_t varbinary_col_id,
                        const RowTableImpl& rows, KeyColumnArray* col);
#if defined(ARROW_HAVE_AVX2)
  static void DecodeHelper_avx2(uint32_t start_row, uint32_t num_rows,
                                uint32_t varbinary_col_id, const RowTableImpl& rows,
                                KeyColumnArray* col);
  template <bool first_varbinary_col>
  static void DecodeImp_avx2(uint32_t start_row, uint32_t num_rows,
                             uint32_t varbinary_col_id, const RowTableImpl& rows,
                             KeyColumnArray* col);
#endif
};

class EncoderNulls {
 public:
  static void EncodeSelected(RowTableImpl* rows, const std::vector<KeyColumnArray>& cols,
                             uint32_t num_selected, const uint16_t* selection);

  static void Decode(uint32_t start_row, uint32_t num_rows, const RowTableImpl& rows,
                     std::vector<KeyColumnArray>* cols);
};

}  // namespace compute
}  // namespace arrow

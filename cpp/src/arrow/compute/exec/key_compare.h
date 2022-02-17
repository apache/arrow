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

#include "arrow/compute/exec/key_encode.h"
#include "arrow/compute/exec/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {

class KeyCompare {
 public:
  // Returns a single 16-bit selection vector of rows that failed comparison.
  // If there is input selection on the left, the resulting selection is a filtered image
  // of input selection.
  static void CompareColumnsToRows(uint32_t num_rows_to_compare,
                                   const uint16_t* sel_left_maybe_null,
                                   const uint32_t* left_to_right_map,
                                   KeyEncoder::KeyEncoderContext* ctx,
                                   uint32_t* out_num_rows,
                                   uint16_t* out_sel_left_maybe_same,
                                   const std::vector<KeyEncoder::KeyColumnArray>& cols,
                                   const KeyEncoder::KeyRowArray& rows);

 private:
  template <bool use_selection>
  static void NullUpdateColumnToRow(uint32_t id_col, uint32_t num_rows_to_compare,
                                    const uint16_t* sel_left_maybe_null,
                                    const uint32_t* left_to_right_map,
                                    KeyEncoder::KeyEncoderContext* ctx,
                                    const KeyEncoder::KeyColumnArray& col,
                                    const KeyEncoder::KeyRowArray& rows,
                                    uint8_t* match_bytevector);

  template <bool use_selection, class COMPARE_FN>
  static void CompareBinaryColumnToRowHelper(
      uint32_t offset_within_row, uint32_t first_row_to_compare,
      uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
      const uint32_t* left_to_right_map, KeyEncoder::KeyEncoderContext* ctx,
      const KeyEncoder::KeyColumnArray& col, const KeyEncoder::KeyRowArray& rows,
      uint8_t* match_bytevector, COMPARE_FN compare_fn);

  template <bool use_selection>
  static void CompareBinaryColumnToRow(
      uint32_t offset_within_row, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  template <bool use_selection, bool is_first_varbinary_col>
  static void CompareVarBinaryColumnToRow(
      uint32_t id_varlen_col, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  static void AndByteVectors(KeyEncoder::KeyEncoderContext* ctx, uint32_t num_elements,
                             uint8_t* bytevector_A, const uint8_t* bytevector_B);

#if defined(ARROW_HAVE_AVX2)

  template <bool use_selection>
  static uint32_t NullUpdateColumnToRowImp_avx2(
      uint32_t id_col, uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
      const uint32_t* left_to_right_map, KeyEncoder::KeyEncoderContext* ctx,
      const KeyEncoder::KeyColumnArray& col, const KeyEncoder::KeyRowArray& rows,
      uint8_t* match_bytevector);

  template <bool use_selection, class COMPARE8_FN>
  static uint32_t CompareBinaryColumnToRowHelper_avx2(
      uint32_t offset_within_row, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector,
      COMPARE8_FN compare8_fn);

  template <bool use_selection>
  static uint32_t CompareBinaryColumnToRowImp_avx2(
      uint32_t offset_within_row, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  template <bool use_selection, bool is_first_varbinary_col>
  static void CompareVarBinaryColumnToRowImp_avx2(
      uint32_t id_varlen_col, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  static uint32_t AndByteVectors_avx2(uint32_t num_elements, uint8_t* bytevector_A,
                                      const uint8_t* bytevector_B);

  static uint32_t NullUpdateColumnToRow_avx2(
      bool use_selection, uint32_t id_col, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  static uint32_t CompareBinaryColumnToRow_avx2(
      bool use_selection, uint32_t offset_within_row, uint32_t num_rows_to_compare,
      const uint16_t* sel_left_maybe_null, const uint32_t* left_to_right_map,
      KeyEncoder::KeyEncoderContext* ctx, const KeyEncoder::KeyColumnArray& col,
      const KeyEncoder::KeyRowArray& rows, uint8_t* match_bytevector);

  static void CompareVarBinaryColumnToRow_avx2(
      bool use_selection, bool is_first_varbinary_col, uint32_t id_varlen_col,
      uint32_t num_rows_to_compare, const uint16_t* sel_left_maybe_null,
      const uint32_t* left_to_right_map, KeyEncoder::KeyEncoderContext* ctx,
      const KeyEncoder::KeyColumnArray& col, const KeyEncoder::KeyRowArray& rows,
      uint8_t* match_bytevector);

#endif
};

}  // namespace compute
}  // namespace arrow

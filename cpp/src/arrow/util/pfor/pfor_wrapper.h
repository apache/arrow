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

// High-level wrapper interface for PFOR compression
//
// Handles page-level serialization: header, offset array, and vectors.

#pragma once

#include <cstddef>
#include <cstdint>

#include "arrow/status.h"
#include "arrow/util/pfor/pfor.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace pfor {

/// \class PforWrapper
/// \brief High-level interface for PFOR page-level compression
///
/// Manages the page layout: [Header 7B] [Offset Array] [Vector 0] [Vector 1] ...
///
/// \tparam T the integer type (int32_t or int64_t)
template <typename T>
class PforWrapper {
 public:
  /// \brief Encode integer values into a PFOR-compressed page
  ///
  /// \param[in] values pointer to input integers
  /// \param[in] num_values total number of values
  /// \param[out] comp pointer to output buffer (caller must ensure sufficient size)
  /// \param[in,out] comp_size input: available buffer size; output: bytes written
  static void Encode(const T* values, int32_t num_values, char* comp,
                     int64_t* comp_size);

  /// \brief Decode a PFOR-compressed page
  ///
  /// \param[out] values pointer to output buffer
  /// \param[in] num_values number of values to decode (from page context)
  /// \param[in] comp pointer to compressed data
  /// \param[in] comp_size size of compressed data
  /// \return Status::OK on success, or an error if the data is malformed
  static Status Decode(T* values, int32_t num_values, const char* comp,
                       int64_t comp_size);

  /// \brief Get the maximum compressed size for a given number of values
  ///
  /// \param[in] num_values number of integer values
  /// \return maximum possible compressed page size in bytes
  static int64_t GetMaxCompressedSize(int32_t num_values);

 private:
  /// \brief Page header structure (7 bytes)
  struct PforHeader {
    uint8_t packing_mode;      // 0 = FOR + bit-packing
    uint8_t log_vector_size;   // log2(vector_size)
    uint8_t value_byte_width;  // sizeof(T): 4 or 8
    int32_t num_elements;      // total element count
  };

  static constexpr int32_t kVectorSize =
      static_cast<int32_t>(PforConstants::kPforVectorSize);

  static void StoreHeader(arrow::util::span<uint8_t> dest, const PforHeader& header);
  static PforHeader LoadHeader(arrow::util::span<const uint8_t> src);
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow

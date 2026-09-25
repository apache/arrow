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

/// \file variant_internal_util.h
/// \brief Internal utilities shared by variant implementation files.
///
/// NOT part of the public API — not installed. Used only by variant.cc
/// and variant_shredding.cc to avoid duplicating low-level helpers.

#include <cstdint>
#include <cstring>

#include "arrow/util/endian.h"

namespace arrow::extension::variant::internal {

/// \brief Read an unsigned integer of 1-4 bytes in little-endian order.
///
/// Reads exactly \p num_bytes from \p data, interprets them as an unsigned
/// little-endian integer, and returns the result as uint32_t.
///
/// \pre num_bytes must be in [1, 4]
/// \pre data must point to at least num_bytes readable bytes
inline uint32_t ReadUnsignedLE(const uint8_t* data, int32_t num_bytes) {
  uint32_t result = 0;
  std::memcpy(&result, data, num_bytes);
  result = ::arrow::bit_util::FromLittleEndian(result);
  if (num_bytes < 4) {
    result &= (static_cast<uint32_t>(1) << (num_bytes * 8)) - 1;
  }
  return result;
}

/// \brief Read an unsigned integer of 1-8 bytes in little-endian order.
///
/// Extended version that supports reading up to 8-byte values (for int64
/// extraction in shredding paths). Returns int64_t for compatibility with
/// Arrow's signed-offset conventions.
///
/// \pre num_bytes must be in [1, 8]
/// \pre data must point to at least num_bytes readable bytes
inline int64_t ReadUnsignedLE64(const uint8_t* data, int32_t num_bytes) {
  if (num_bytes <= 4) {
    return static_cast<int64_t>(ReadUnsignedLE(data, num_bytes));
  }
  uint64_t result = 0;
  std::memcpy(&result, data, num_bytes);
  result = ::arrow::bit_util::FromLittleEndian(result);
  if (num_bytes < 8) {
    result &= (static_cast<uint64_t>(1) << (num_bytes * 8)) - 1;
  }
  return static_cast<int64_t>(result);
}

}  // namespace arrow::extension::variant::internal

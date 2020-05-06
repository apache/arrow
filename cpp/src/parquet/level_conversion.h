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

#include "parquet/platform.h"

namespace parquet {
namespace internal {

void PARQUET_EXPORT DefinitionLevelsToBitmap(
    const int16_t* def_levels, int64_t num_def_levels, const int16_t max_definition_level,
    const int16_t max_repetition_level, int64_t* values_read, int64_t* null_count,
    uint8_t* valid_bits, int64_t valid_bits_offset);

// These APIs are likely to be revised as part of ARROW-8494 to reduce duplicate code.
// They currently represent minimal functionality for vectorized computation of definition
// levels.

#if defined(ARROW_LITTLE_ENDIAN)
/// Builds a bitmap by applying predicate to the level vector provided.
///
/// \param[in] levels Rep or def level array.
/// \param[in] num_levels The number of levels to process (must be [0, 64])
/// \param[in] predicate The predicate to apply (must have the signature `bool
/// predicate(int16_t)`.
/// \returns The bitmap using least significant "bit" ordering.
///
/// N.B. Correct byte ordering is dependent on little-endian architectures.
///
template <typename Predicate>
uint64_t LevelsToBitmap(const int16_t* levels, int64_t num_levels, Predicate predicate) {
  // Both clang and GCC can vectorize this automatically with SSE4/AVX2.
  uint64_t mask = 0;
  for (int x = 0; x < num_levels; x++) {
    mask |= static_cast<uint64_t>(predicate(levels[x]) ? 1 : 0) << x;
  }
  return mask;
}

/// Builds a  bitmap where each set bit indicates the corresponding level is greater
/// than rhs.
static inline uint64_t GreaterThanBitmap(const int16_t* levels, int64_t num_levels,
                                         int16_t rhs) {
  return LevelsToBitmap(levels, num_levels, [rhs](int16_t value) { return value > rhs; });
}

#endif

}  // namespace internal
}  // namespace parquet

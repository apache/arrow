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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "parquet/platform.h"

namespace parquet::internal {

/// Builds a  bitmap where each set bit indicates the corresponding level is greater
/// than rhs.
uint64_t PARQUET_EXPORT GreaterThanBitmap(const int16_t* levels, int64_t num_levels,
                                          int16_t rhs);

struct MinMax {
  int16_t min;
  int16_t max;
};

MinMax PARQUET_EXPORT FindMinMax(const int16_t* levels, int64_t num_levels);

// Fallback for num_levels > uint16_t::max(), counting in chunks to avoid overflow.
template <typename Predicate>
ARROW_NOINLINE int64_t CountLevelsIfChunked(const int16_t* levels, int64_t num_levels,
                                            Predicate&& pred) {
  constexpr int64_t kMaxChunkSize = std::numeric_limits<uint16_t>::max();
  int64_t total = 0;
  while (num_levels > 0) {
    const int64_t chunk_size = std::min(num_levels, kMaxChunkSize);
    uint16_t chunk_count = 0;
    for (int64_t i = 0; i < chunk_size; ++i) {
      chunk_count += pred(levels[i]) ? 1 : 0;
    }
    total += chunk_count;
    levels += chunk_size;
    num_levels -= chunk_size;
  }
  return total;
}

/// \brief Count the levels that satisfy `pred`.
///
/// Accumulates matches in a `uint16_t` so the compiler can vectorize the loop
/// using 16-bit lanes without widening each comparison result to 64 bits.
/// Inputs longer than `uint16_t::max()` are counted in chunks to avoid overflow.
template <typename Predicate>
inline int64_t CountLevelsIf(const int16_t* levels, int64_t num_levels,
                             Predicate&& pred) {
  constexpr int64_t kMaxChunkSize = std::numeric_limits<uint16_t>::max();
  if (num_levels > kMaxChunkSize) {
    return CountLevelsIfChunked(levels, num_levels, std::forward<Predicate>(pred));
  }
  uint16_t count = 0;
  for (int64_t i = 0; i < num_levels; ++i) {
    count += pred(levels[i]) ? 1 : 0;
  }
  return count;
}

/// \brief Count the levels equal to `value`.
inline int64_t CountLevels(const int16_t* levels, int64_t num_levels, int16_t value) {
  return CountLevelsIf(levels, num_levels,
                       [value](int16_t level) { return level == value; });
}

/// \brief Count the levels greater than or equal to `min_level`.
inline int64_t CountLevelsGreaterEqual(const int16_t* levels, int64_t num_levels,
                                       int16_t min_level) {
  return CountLevelsIf(levels, num_levels,
                       [min_level](int16_t level) { return level >= min_level; });
}

}  // namespace parquet::internal

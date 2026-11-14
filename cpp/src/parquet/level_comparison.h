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

/// Counts the levels that satisfy `pred`, in chunks small enough that a
/// `uint16_t` accumulator cannot overflow.
///
/// This is the fallback for inputs that do not fit in a `uint16_t` accumulator.
/// It is kept out of line so that the common path (a single Parquet page chunk,
/// always short enough for one accumulator) is a plain loop without any chunk
/// bookkeeping in front of it.
template <typename Predicate>
ARROW_NOINLINE int64_t CountIf16Chunked(const int16_t* levels, int64_t num_levels,
                                        Predicate pred) {
  // The loop below accumulates one match per lane in a `uint16_t`, so a chunk
  // must not hold more levels than that can count.
  constexpr int64_t kMaxCount = std::numeric_limits<uint16_t>::max();
  int64_t total = 0;
  while (num_levels > 0) {
    const int64_t chunk_length = std::min(num_levels, kMaxCount);
    uint16_t chunk_count = 0;
    for (int64_t i = 0; i < chunk_length; ++i) {
      chunk_count += pred(levels[i]) ? 1 : 0;
    }
    total += chunk_count;
    levels += chunk_length;
    num_levels -= chunk_length;
  }
  return total;
}

/// Count the levels that satisfy the given predicate.
///
/// The matches are accumulated in a `uint16_t`, which cannot overflow as long
/// as at most `std::numeric_limits<uint16_t>::max()` levels are counted at a
/// time, so longer inputs are delegated to `CountIf16Chunked`. This is both
/// safe and much faster than `std::count`, as the loop can be vectorized with
/// 16-bit accumulators (one accumulated match per lane) instead of widening
/// every match to a 64-bit integer.
///
/// \param[in] levels The level values to inspect.
/// \param[in] num_levels The number of levels to inspect, must be non-negative.
/// \param[in] pred The predicate to apply, must be callable as
///   `bool(int16_t)`.
/// \return The number of levels for which `pred` returns true.
template <typename Predicate>
inline int64_t CountIf16(const int16_t* levels, int64_t num_levels, Predicate pred) {
  // Short inputs are counted first, before the wide loop and before the chunk
  // clamp, because a handful of levels cannot amortize either. Running the
  // small case first is also how folly::simd::detail::simdForEach orders its
  // single steps before the unrolled ones, so that short inputs never pay for
  // the unrolled set up.
  //
  // 32 is the widest per-iteration body this reduction gets: a packed int16_t
  // register holds 8 levels with SSE2/NEON and 16 with AVX2, but clang runs up
  // to 32 levels per iteration on NEON (four registers), so shorter inputs
  // never reach a full iteration on the narrow targets. The bound also has to
  // be a compile-time power of two, which is what turns the loop into an
  // unrolled ladder of 8/16/24-wide compares instead of scalar code.
  constexpr int64_t kSmallLevels = 32;
  constexpr int64_t kMaxCount = std::numeric_limits<uint16_t>::max();
  if (num_levels < kSmallLevels) {
    uint16_t count = 0;
    for (int64_t i = 0; i < num_levels; ++i) {
      count += pred(levels[i]) ? 1 : 0;
    }
    return count;
  }
  if (num_levels > kMaxCount) {
    return CountIf16Chunked(levels, num_levels, pred);
  }
  uint16_t count = 0;
  for (int64_t i = 0; i < num_levels; ++i) {
    count += pred(levels[i]) ? 1 : 0;
  }
  return count;
}

}  // namespace parquet::internal

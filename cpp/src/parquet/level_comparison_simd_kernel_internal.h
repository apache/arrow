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

#include <xsimd/xsimd.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>

#include "parquet/level_comparison.h"

namespace parquet::internal {

template <typename Arch>
MinMax FindMinMaxSimd(const int16_t* levels, int64_t num_levels) {
  using batch = xsimd::batch<int16_t, Arch>;
  constexpr int64_t kLanes = static_cast<int64_t>(batch::size);

  MinMax out{std::numeric_limits<int16_t>::max(), std::numeric_limits<int16_t>::min()};

  int64_t i = 0;
  if (num_levels >= kLanes) {
    batch vmin(out.min);
    batch vmax(out.max);
    for (; i + kLanes <= num_levels; i += kLanes) {
      const auto v = batch::load_unaligned(levels + i);
      vmin = xsimd::min(vmin, v);
      vmax = xsimd::max(vmax, v);
    }
    out.min = xsimd::reduce_min(vmin);
    out.max = xsimd::reduce_max(vmax);
  }
  for (; i < num_levels; ++i) {
    out.min = std::min(levels[i], out.min);
    out.max = std::max(levels[i], out.max);
  }
  return out;
}

}  // namespace parquet::internal

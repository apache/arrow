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

#include "arrow/util/int128_internal.h"

namespace arrow {
namespace compute {
namespace internal {

using arrow::internal::int128_t;

// Accumulate sum/squared sum (using naive summation)
// Shared implementation between scalar/hash aggregate variance/stddev kernels
template <typename ArrowType>
struct IntegerVarStd {
  using c_type = typename ArrowType::c_type;

  int64_t count = 0;
  int64_t sum = 0;
  int128_t square_sum = 0;

  void ConsumeOne(const c_type value) {
    sum += value;
    square_sum += static_cast<uint64_t>(value) * value;
    count++;
  }

  double mean() const { return static_cast<double>(sum) / count; }

  double m2() const {
    // calculate m2 = square_sum - sum * sum / count
    // decompose `sum * sum / count` into integers and fractions
    const int128_t sum_square = static_cast<int128_t>(sum) * sum;
    const int128_t integers = sum_square / count;
    const double fractions = static_cast<double>(sum_square % count) / count;
    return static_cast<double>(square_sum - integers) - fractions;
  }
};

static inline void MergeVarStd(int64_t count1, double mean1, int64_t count2, double mean2,
                               double m22, int64_t* out_count, double* out_mean,
                               double* out_m2) {
  double mean = (mean1 * count1 + mean2 * count2) / (count1 + count2);
  *out_m2 += m22 + count1 * (mean1 - mean) * (mean1 - mean) +
             count2 * (mean2 - mean) * (mean2 - mean);
  *out_count += count2;
  *out_mean = mean;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

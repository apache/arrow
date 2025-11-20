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

#include <cassert>
#include <cmath>
#include <initializer_list>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow::internal {

/// \brief Percent-point / quantile function (PPF) of the normal distribution.
///
/// Given p in [0, 1], return the corresponding quantile value in the normal
/// distribution. This is the reciprocal of the cumulative distribution function.
///
/// If p is not in [0, 1], behavior is undefined.
///
/// This function is sometimes also called the probit function.
ARROW_EXPORT
double NormalPPF(double p);

/// "Improved Kahan–Babuška algorithm" by Neumaier
///
/// https://en.wikipedia.org/wiki/Kahan_summation_algorithm#Further_enhancements
template <typename Range = std::initializer_list<double>>
double NeumaierSum(Range&& inputs) {
  double sum = 0, c = 0;
  for (const double v : inputs) {
    double t = sum + v;
    if (std::isfinite(t)) {
      if (std::abs(sum) >= std::abs(v)) {
        // If sum is bigger, low-order digits of v are lost...
        c += (sum - t) + v;
      } else {
        // ... else low-order digits of sum are lost.
        c += (v - t) + sum;
      }
    }
    sum = t;
  }
  return sum + c;
}

/// Based-2 logarithm that works only on powers of two.
template <typename T>
constexpr T ReversePow2(T x) {
  constexpr T kByteLen = 8;
  for (T n = 0, y = 1; n <= (kByteLen * static_cast<T>(sizeof(T))); ++n, y = y * 2) {
    if (y == x) {
      return n;
    }
  }
  return 0;
}

static_assert(ReversePow2(8) == 3);
static_assert(ReversePow2(4) == 2);
static_assert(ReversePow2(2) == 1);

}  // namespace arrow::internal

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
#include "arrow/util/logging.h"
#include "arrow/util/math_internal.h"

#include <cmath>
#include <type_traits>

namespace arrow::compute::internal {

using arrow::internal::int128_t;

// Accumulate sum/squared sum (using naive summation)
// Shared implementation between scalar/hash aggregate variance/stddev kernels
struct IntegerVarStd {
  int64_t count = 0;
  int64_t sum = 0;
  int128_t square_sum = 0;

  template <typename Integer>
  void ConsumeOne(Integer value) {
    static_assert(std::is_integral_v<Integer>);
    static_assert(sizeof(Integer) <= 4);
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

enum class StatisticType { Var, Std, Skew, Kurtosis };

constexpr int moments_level_for_statistic(StatisticType s) {
  switch (s) {
    case StatisticType::Skew:
      return 3;
    case StatisticType::Kurtosis:
      return 4;
    default:
      return 2;
  }
}

struct Moments {
  int64_t count = 0;
  double mean = 0;
  double m2 = 0;  // m2 = sum((X-mean)^2)
  double m3 = 0;  // m3 = sum((X-mean)^3)
  double m4 = 0;  // m4 = sum((X-mean)^4)

  Moments() = default;
  Moments(int64_t count, double mean, double m2, double m3 = 0, double m4 = 0)
      : count(count), mean(mean), m2(m2), m3(m3), m4(m4) {}

  double Variance(int ddof) const { return m2 / (count - ddof); }

  double Stddev(int ddof) const { return sqrt(Variance(ddof)); }

  double Skew(bool biased = true) const {
    double result;
    // This may return NaN for m2 == 0 and m3 == 0, which is expected.
    if (biased) {
      result = sqrt(count) * m3 / sqrt(m2 * m2 * m2);
    } else {
      auto m2_avg = m2 / count;
      result = sqrt(count * (count - 1)) / (count - 2) * (m3 / count) /
               sqrt(m2_avg * m2_avg * m2_avg);
    }
    return result;
  }

  double Kurtosis(bool biased = true) const {
    double result;
    // This may return NaN for m2 == 0 and m4 == 0, which is expected.
    if (biased) {
      result = count * m4 / (m2 * m2) - 3;
    } else {
      auto m2_avg = m2 / count;
      result = 1.0 / ((count - 2) * (count - 3)) *
               (((count * count) - 1.0) * (m4 / count) / (m2_avg * m2_avg) -
                3 * ((count - 1) * (count - 1)));
    }
    return result;
  }

  void MergeFrom(int level, const Moments& other) { *this = Merge(level, *this, other); }

  static Moments Merge(int level, const Moments& a, const Moments& b) {
    using ::arrow::internal::NeumaierSum;

    if (a.count == 0) {
      return b;
    } else if (b.count == 0) {
      return a;
    }

    // Shorter aliases for readability
    const int64_t na = a.count, nb = b.count;
    const int64_t n = na + nb;
    const double mean = (a.mean * na + b.mean * nb) / n;
    // NOTE: there is a more common formula:
    //   double delta = b.mean - a.mean;
    //   double m2 = a.m2 + b.m2 + delta * delta * na * nb / n;
    // but it gives worse results in TestVarStdKernelMergeStability.
    const double m2 = NeumaierSum({a.m2, b.m2, na * (a.mean - mean) * (a.mean - mean),
                                   nb * (b.mean - mean) * (b.mean - mean)});
    double m3 = 0;
    double m4 = 0;
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    if (level >= 3) {
      double delta = b.mean - a.mean;
      double delta2 = delta * delta;
      m3 = NeumaierSum({a.m3, b.m3, delta2 * delta * na * nb * (na - nb) / (n * n),
                        3 * delta * (na * b.m2 - nb * a.m2) / n});
      if (level >= 4) {
        m4 = NeumaierSum(
            {a.m4, b.m4,
             (delta2 * delta2) * na * nb * (na * na - na * nb + nb * nb) / (n * n * n),
             6 * delta2 * (na * na * b.m2 + nb * nb * a.m2) / (n * n),
             4 * delta * (na * b.m3 - nb * a.m3) / n});
      }
    }
    return Moments(n, mean, m2, m3, m4);
  }

  static Moments FromScalar(int level, double value, int64_t count) {
    return Moments(count, /*mean=*/value, /*m2=*/0, /*m3=*/0, /*m4=*/0);
  }
};

}  // namespace arrow::compute::internal

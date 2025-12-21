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

#include "arrow/testing/math.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <type_traits>

#include <gtest/gtest.h>

#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace {

template <typename Float>
struct FloatToUInt;

template <>
struct FloatToUInt<double> {
  using Type = uint64_t;
};

template <>
struct FloatToUInt<float> {
  using Type = uint32_t;
};

template <>
struct FloatToUInt<util::Float16> {
  using Type = uint16_t;
};

template <typename Float>
struct UlpDistanceUtil {
 public:
  using UIntType = typename FloatToUInt<Float>::Type;
  static constexpr UIntType kNumberOfBits = sizeof(Float) * 8;
  static constexpr UIntType kSignMask = static_cast<UIntType>(1) << (kNumberOfBits - 1);

  // This implementation is inspired by:
  // https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
  static UIntType UlpDistance(Float left, Float right) {
    auto unsigned_left = util::SafeCopy<UIntType>(left);
    auto unsigned_right = util::SafeCopy<UIntType>(right);
    auto biased_left = ConvertSignAndMagnitudeToBiased(unsigned_left);
    auto biased_right = ConvertSignAndMagnitudeToBiased(unsigned_right);
    if (biased_left > biased_right) {
      std::swap(biased_left, biased_right);
    }
    return biased_right - biased_left;
  }

 private:
  // Source reference (GoogleTest):
  // https://github.com/google/googletest/blob/1b96fa13f549387b7549cc89e1a785cf143a1a50/googletest/include/gtest/internal/gtest-internal.h#L345-L368
  static UIntType ConvertSignAndMagnitudeToBiased(UIntType value) {
    if (value & kSignMask) {
      return ~value + 1;
    } else {
      return value | kSignMask;
    }
  }
};

template <typename Float>
bool WithinUlpGeneric(Float left, Float right, int n_ulps) {
  if constexpr (std::is_same_v<Float, util::Float16>) {
    if (left.is_nan() || right.is_nan()) {
      return left.is_nan() == right.is_nan();
    } else if (left.is_infinity() || right.is_infinity()) {
      return left == right;
    }
  } else {
    if (std::isnan(left) || std::isnan(right)) {
      return std::isnan(left) == std::isnan(right);
    }
    if (!std::isfinite(left) || !std::isfinite(right)) {
      return left == right;
    }
  }

  if (n_ulps == 0) {
    return left == right;
  }

  DCHECK_GE(n_ulps, 1);
  return UlpDistanceUtil<Float>::UlpDistance(left, right) <=
         static_cast<uint64_t>(n_ulps);
}

template <typename Float>
void AssertWithinUlpGeneric(Float left, Float right, int n_ulps) {
  if (!WithinUlpGeneric(left, right, n_ulps)) {
    FAIL() << left << " and " << right << " are not within " << n_ulps << " ulps";
  }
}

}  // namespace

bool WithinUlp(util::Float16 left, util::Float16 right, int n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

bool WithinUlp(float left, float right, int n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

bool WithinUlp(double left, double right, int n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(util::Float16 left, util::Float16 right, int n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(float left, float right, int n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(double left, double right, int n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

}  // namespace arrow

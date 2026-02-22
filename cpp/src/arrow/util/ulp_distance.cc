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

#include "arrow/util/ulp_distance.h"

#include <algorithm>
#include <bit>
#include <cinttypes>
#include <cmath>
#include <type_traits>

#include "arrow/util/float16.h"

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
  using UIntType = FloatToUInt<Float>::Type;
  static constexpr UIntType kNumberOfBits = sizeof(Float) * 8;
  static constexpr UIntType kSignMask = static_cast<UIntType>(1) << (kNumberOfBits - 1);

  // This implementation is inspired by:
  // https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
  static UIntType UlpDistance(Float left, Float right) {
    auto unsigned_left = std::bit_cast<UIntType>(left);
    auto unsigned_right = std::bit_cast<UIntType>(right);
    auto biased_left = ConvertSignAndMagnitudeToBiased(unsigned_left);
    auto biased_right = ConvertSignAndMagnitudeToBiased(unsigned_right);
    if (biased_left > biased_right) {
      std::swap(biased_left, biased_right);
    }

    // Handling of NaN should be determined by the comparison policy.
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
bool UlpDistanceGeneric(Float left, Float right, uint16_t n_ulps) {
  return UlpDistanceUtil<Float>::UlpDistance(left, right) <=
         static_cast<uint64_t>(n_ulps);
}

}  // namespace

bool UlpDistance(util::Float16 left, util::Float16 right, uint16_t n_ulps) {
  return UlpDistanceGeneric(left, right, n_ulps);
}

bool UlpDistance(float left, float right, uint16_t n_ulps) {
  return UlpDistanceGeneric(left, right, n_ulps);
}

bool UlpDistance(double left, double right, uint16_t n_ulps) {
  return UlpDistanceGeneric(left, right, n_ulps);
}

}  // namespace arrow

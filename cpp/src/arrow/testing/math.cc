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
#include "arrow/util/ulp_distance.h"

namespace arrow {
namespace {

template <typename Float>
bool WithinUlpGeneric(Float left, Float right, uint16_t n_ulps) {
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

  return UlpDistance(left, right, n_ulps);
}

template <typename Float>
void AssertWithinUlpGeneric(Float left, Float right, uint16_t n_ulps) {
  if (!WithinUlpGeneric(left, right, n_ulps)) {
    FAIL() << left << " and " << right << " are not within " << n_ulps << " ulps";
  }
}

}  // namespace

bool WithinUlp(util::Float16 left, util::Float16 right, uint16_t n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

bool WithinUlp(float left, float right, uint16_t n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

bool WithinUlp(double left, double right, uint16_t n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(util::Float16 left, util::Float16 right, uint16_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(float left, float right, uint16_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(double left, double right, uint16_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

}  // namespace arrow

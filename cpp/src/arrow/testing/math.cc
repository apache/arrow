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
#include "arrow/util/ulp_distance_internal.h"

namespace arrow {
namespace {

template <typename Float>
bool WithinUlpGeneric(Float left, Float right, int32_t n_ulps) {
  if constexpr (std::is_same_v<Float, util::Float16>) {
    if (left.is_nan() || right.is_nan()) {
      return left.is_nan() == right.is_nan();
    }
  } else {
    if (std::isnan(left) || std::isnan(right)) {
      return std::isnan(left) == std::isnan(right);
    }
  }

  if (n_ulps == 0) {
    return left == right;
  }

  return internal::WithinUlp(left, right, n_ulps);
}

template <typename Float>
void AssertWithinUlpGeneric(Float left, Float right, int32_t n_ulps) {
  if (!WithinUlpGeneric(left, right, n_ulps)) {
    FAIL() << left << " and " << right << " are not within " << n_ulps << " ulps";
  }
}

}  // namespace

void AssertWithinUlp(util::Float16 left, util::Float16 right, int32_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(float left, float right, int32_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(double left, double right, int32_t n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

}  // namespace arrow

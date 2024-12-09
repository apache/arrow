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

#include <cmath>
#include <limits>

#include <gtest/gtest.h>

#include "arrow/util/logging.h"

namespace arrow {
namespace {

template <typename Float>
bool WithinUlpOneWay(Float left, Float right, int n_ulps) {
  // The delta between 1.0 and the FP value immediately before it.
  // We're using this value because `frexp` returns a mantissa between 0.5 and 1.0.
  static const Float kOneUlp = Float(1.0) - std::nextafter(Float(1.0), Float(0.0));

  DCHECK_GE(n_ulps, 1);

  if (left == 0) {
    return left == right;
  }
  if (left < 0) {
    left = -left;
    right = -right;
  }

  int left_exp;
  Float left_mant = std::frexp(left, &left_exp);
  Float delta = static_cast<Float>(n_ulps) * kOneUlp;
  Float lower_bound = std::ldexp(left_mant - delta, left_exp);
  Float upper_bound = std::ldexp(left_mant + delta, left_exp);
  return right >= lower_bound && right <= upper_bound;
}

template <typename Float>
bool WithinUlpGeneric(Float left, Float right, int n_ulps) {
  if (!std::isfinite(left) || !std::isfinite(right)) {
    return left == right;
  }
  return (std::abs(left) <= std::abs(right)) ? WithinUlpOneWay(left, right, n_ulps)
                                             : WithinUlpOneWay(right, left, n_ulps);
}

template <typename Float>
void AssertWithinUlpGeneric(Float left, Float right, int n_ulps) {
  if (!WithinUlpGeneric(left, right, n_ulps)) {
    FAIL() << left << " and " << right << " are not within " << n_ulps << " ulps";
  }
}

}  // namespace

bool WithinUlp(float left, float right, int n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

bool WithinUlp(double left, double right, int n_ulps) {
  return WithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(float left, float right, int n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

void AssertWithinUlp(double left, double right, int n_ulps) {
  AssertWithinUlpGeneric(left, right, n_ulps);
}

}  // namespace arrow

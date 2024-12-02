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

#include "arrow/util/logging.h"

namespace arrow {
namespace {

template <typename Float>
constexpr Float kOneUlp;
template <>
constexpr float kOneUlp<float> = 5.9604645e-08f;
template <>
constexpr float kOneUlp<double> = 1.1102230246251565e-16;

template <typename Float>
bool WithinUlpOneWay(Float left, Float right, int n_ulp) {
  // Ideally these would be compile-time (constexpr) checks
  DCHECK_LT(Float(1.0) - kOneUlp<Float>, Float(1.0));
  DCHECK_EQ(std::nextafter(Float(1.0) - kOneUlp<Float>, Float(1.0)), Float(1.0));

  DCHECK_GE(n_ulp, 1);

  if (left == 0) {
    return left == right;
  }
  if (left < 0) {
    left = -left;
    right = -right;
  }

  int left_exp;
  Float left_mant = std::frexp(left, &left_exp);
  Float delta = static_cast<Float>(n_ulp) * kOneUlp<Float>;
  Float lower_bound = std::ldexp(left_mant - delta, left_exp);
  Float upper_bound = std::ldexp(left_mant + delta, left_exp);
  return right >= lower_bound && right <= upper_bound;
}

template <typename Float>
bool WithinUlpGeneric(Float left, Float right, int n_ulp) {
  if (!std::isfinite(left) || !std::isfinite(right)) {
    return left == right;
  }
  return WithinUlpOneWay(left, right, n_ulp) || WithinUlpOneWay(right, left, n_ulp);
}

}  // namespace

bool WithinUlp(float left, float right, int n_ulp) {
  return WithinUlpGeneric(left, right, n_ulp);
}

bool WithinUlp(double left, double right, int n_ulp) {
  return WithinUlpGeneric(left, right, n_ulp);
}

}  // namespace arrow

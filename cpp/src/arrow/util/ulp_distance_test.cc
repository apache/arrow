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

#include "arrow/util/ulp_distance_internal.h"

#include <cinttypes>
#include <cmath>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/testing/gtest_util.h"
#include "arrow/util/float16.h"

namespace arrow::internal {

using util::Float16;

template <typename Float>
void CheckWithinUlpSingle(Float x, Float y, int32_t n_ulp) {
  ARROW_SCOPED_TRACE("x = ", x, ", y = ", y, ", n_ulp = ", n_ulp);
  ASSERT_TRUE(WithinUlp(x, y, n_ulp));
}

template <typename Float>
void CheckNotWithinUlpSingle(Float x, Float y, int n_ulp) {
  ARROW_SCOPED_TRACE("x = ", x, ", y = ", y, ", n_ulp = ", n_ulp);
  ASSERT_FALSE(WithinUlp(x, y, n_ulp));
}

template <typename Float>
void CheckWithinUlp(Float x, Float y, int n_ulp) {
  CheckWithinUlpSingle(x, y, n_ulp);
  CheckWithinUlpSingle(y, x, n_ulp);
  CheckWithinUlpSingle(x, y, n_ulp + 1);
  CheckWithinUlpSingle(y, x, n_ulp + 1);
  CheckWithinUlpSingle(-x, -y, n_ulp);
  CheckWithinUlpSingle(-y, -x, n_ulp);

  for (int exp : {1, -1, 10, -10}) {
    Float x_scaled(0);
    Float y_scaled(0);
    if constexpr (std::is_same_v<Float, Float16>) {
      x_scaled = Float16(std::ldexp(x.ToFloat(), exp));
      y_scaled = Float16(std::ldexp(y.ToFloat(), exp));
    } else {
      x_scaled = std::ldexp(x, exp);
      y_scaled = std::ldexp(y, exp);
    }
    CheckWithinUlpSingle(x_scaled, y_scaled, n_ulp);
    CheckWithinUlpSingle(y_scaled, x_scaled, n_ulp);
  }
}

template <typename Float>
void CheckNotWithinUlp(Float x, Float y, int n_ulp) {
  CheckNotWithinUlpSingle(x, y, n_ulp);
  CheckNotWithinUlpSingle(y, x, n_ulp);
  CheckNotWithinUlpSingle(-x, -y, n_ulp);
  CheckNotWithinUlpSingle(-y, -x, n_ulp);
  if (n_ulp > 1) {
    CheckNotWithinUlpSingle(x, y, n_ulp - 1);
    CheckNotWithinUlpSingle(y, x, n_ulp - 1);
    CheckNotWithinUlpSingle(-x, -y, n_ulp - 1);
    CheckNotWithinUlpSingle(-y, -x, n_ulp - 1);
  }

  for (int exp : {1, -1, 10, -10}) {
    Float x_scaled(0);
    Float y_scaled(0);
    if constexpr (std::is_same_v<Float, Float16>) {
      x_scaled = Float16(std::ldexp(x.ToFloat(), exp));
      y_scaled = Float16(std::ldexp(y.ToFloat(), exp));
    } else {
      x_scaled = std::ldexp(x, exp);
      y_scaled = std::ldexp(y, exp);
    }
    CheckNotWithinUlpSingle(x_scaled, y_scaled, n_ulp);
    CheckNotWithinUlpSingle(y_scaled, x_scaled, n_ulp);
  }
}

TEST(TestWithinUlp, Double) {
  for (double f : {0.0, 1e-20, 1.0, 2345678.9}) {
    CheckWithinUlp(f, f, 0);
    CheckWithinUlp(f, f, 1);
    CheckWithinUlp(f, f, 42);
  }
  CheckWithinUlp(-0.0, 0.0, 1);
  CheckWithinUlp(1.0, 1.0000000000000002, 1);
  CheckWithinUlp(1.0, 1.0000000000000007, 3);
  CheckNotWithinUlp(1.0, 1.0000000000000002, 0);
  CheckNotWithinUlp(1.0, 1.0000000000000007, 2);
  CheckNotWithinUlp(1.0, 1.0000000000000007, 1);
  // left and right have a different exponent but are still very close
  CheckWithinUlp(1.0, 0.9999999999999999, 1);
  CheckWithinUlp(1.0, 0.9999999999999988, 11);
  CheckNotWithinUlp(1.0, 0.9999999999999988, 10);
  CheckWithinUlp(1.0000000000000002, 0.9999999999999999, 2);
  CheckNotWithinUlp(1.0000000000000002, 0.9999999999999999, 1);
  CheckWithinUlp(0.9999999999999988, 1.0000000000000007, 14);
  CheckNotWithinUlp(0.9999999999999988, 1.0000000000000007, 13);

  CheckWithinUlp(123.4567, 123.45670000000015, 11);
  CheckNotWithinUlp(123.4567, 123.45670000000015, 10);

  CheckWithinUlp(HUGE_VAL, HUGE_VAL, 10);
  CheckWithinUlp(-HUGE_VAL, -HUGE_VAL, 10);
  CheckWithinUlp(std::nan(""), std::nan(""), 10);
  CheckNotWithinUlp(HUGE_VAL, -HUGE_VAL, 10);
  CheckNotWithinUlp(12.34, -HUGE_VAL, 10);
  CheckNotWithinUlp(12.34, std::nan(""), 10);
  CheckNotWithinUlp(12.34, -12.34, 10);
  CheckNotWithinUlp(0.0, 1e-20, 10);
}

TEST(TestWithinUlp, Float) {
  for (float f : {0.0f, 1e-8f, 1.0f, 123.456f}) {
    CheckWithinUlp(f, f, 0);
    CheckWithinUlp(f, f, 1);
    CheckWithinUlp(f, f, 42);
  }
  CheckWithinUlp(-0.0f, 0.0f, 1);
  CheckWithinUlp(1.0f, 1.0000001f, 1);
  CheckWithinUlp(1.0f, 1.0000013f, 11);
  CheckNotWithinUlp(1.0f, 1.0000001f, 0);
  CheckNotWithinUlp(1.0f, 1.0000013f, 10);
  // left and right have a different exponent but are still very close
  CheckWithinUlp(1.0f, 0.99999994f, 1);
  CheckWithinUlp(1.0f, 0.99999934f, 11);
  CheckNotWithinUlp(1.0f, 0.99999934f, 10);
  CheckWithinUlp(1.0000001f, 0.99999994f, 2);
  CheckNotWithinUlp(1.0000001f, 0.99999994f, 1);
  CheckWithinUlp(1.0000013f, 0.99999934f, 22);
  CheckNotWithinUlp(1.0000013f, 0.99999934f, 21);

  CheckWithinUlp(123.456f, 123.456085f, 11);
  CheckNotWithinUlp(123.456f, 123.456085f, 10);

  CheckWithinUlp(HUGE_VALF, HUGE_VALF, 10);
  CheckWithinUlp(-HUGE_VALF, -HUGE_VALF, 10);
  CheckWithinUlp(std::nanf(""), std::nanf(""), 10);
  CheckNotWithinUlp(HUGE_VALF, -HUGE_VALF, 10);
  CheckNotWithinUlp(12.34f, -HUGE_VALF, 10);
  CheckNotWithinUlp(12.34f, std::nanf(""), 10);
  CheckNotWithinUlp(12.34f, -12.34f, 10);
}

std::vector<Float16> ConvertToFloat16Vector(const std::vector<float>& float_values) {
  std::vector<Float16> float16_vector;
  float16_vector.reserve(float_values.size());
  for (auto& value : float_values) {
    float16_vector.emplace_back(value);
  }
  return float16_vector;
}

TEST(TestWithinUlp, Float16) {
  for (Float16 f : ConvertToFloat16Vector({0.0f, 1e-8f, 1.0f, 123.456f})) {
    CheckWithinUlp(f, f, 0);
    CheckWithinUlp(f, f, 1);
    CheckWithinUlp(f, f, 42);
  }
  CheckWithinUlp(Float16(-0.0f), Float16(0.0f), 1);
  CheckWithinUlp(Float16(1.0f), Float16(1.00097656f), 1);
  CheckWithinUlp(Float16(1.0f), Float16(1.01074219f), 11);
  CheckNotWithinUlp(Float16(1.0f), Float16(1.00097656f), 0);
  CheckNotWithinUlp(Float16(1.0f), Float16(1.01074219f), 10);
  // left and right have a different exponent but are still very close
  CheckWithinUlp(Float16(1.0f), Float16(0.999511719f), 1);
  CheckWithinUlp(Float16(1.0f), Float16(0.994628906f), 11);
  CheckNotWithinUlp(Float16(1.0f), Float16(0.994628906f), 10);
  CheckWithinUlp(Float16(1.00097656), Float16(0.999511719f), 2);
  CheckNotWithinUlp(Float16(1.00097656), Float16(0.999511719f), 1);
  CheckWithinUlp(Float16(1.01074219f), Float16(0.994628906f), 22);
  CheckNotWithinUlp(Float16(1.01074219f), Float16(0.994628906f), 21);

  CheckWithinUlp(Float16(123.456f), Float16(124.143501f), 11);
  // The assertion below does not work because ldexp(Float16(124.143501f), 10)
  // results in inf in Float16.
  // CheckNotWithinUlp(Float16(123.456f), Float16(124.143501f), 10);

  CheckWithinUlp(std::numeric_limits<Float16>::infinity(),
                 std::numeric_limits<Float16>::infinity(), 10);
  CheckWithinUlp(-std::numeric_limits<Float16>::infinity(),
                 -std::numeric_limits<Float16>::infinity(), 10);
  CheckWithinUlp(std::numeric_limits<Float16>::quiet_NaN(),
                 std::numeric_limits<Float16>::quiet_NaN(), 10);
  CheckNotWithinUlp(std::numeric_limits<Float16>::infinity(),
                    -std::numeric_limits<Float16>::infinity(), 10);
  CheckNotWithinUlp(Float16(12.34f), -std::numeric_limits<Float16>::infinity(), 10);
  CheckNotWithinUlp(Float16(12.34f), std::numeric_limits<Float16>::quiet_NaN(), 10);
  CheckNotWithinUlp(Float16(12.34f), Float16(-12.34f), 10);
}

}  // namespace arrow::internal

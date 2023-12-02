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

#include <array>
#include <cmath>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/float16.h"
#include "arrow/util/span.h"
#include "arrow/util/ubsan.h"

namespace arrow::util {
namespace {

template <typename T>
using Limits = std::numeric_limits<T>;

float F32(uint32_t bits) { return SafeCopy<float>(bits); }
double F64(uint64_t bits) { return SafeCopy<double>(bits); }

template <typename T>
class Float16ConversionTest : public ::testing::Test {
 public:
  struct RoundTripTestCase {
    T input;
    uint16_t bits;
    T output;
  };

  static void TestRoundTrip(span<const RoundTripTestCase> test_cases) {
    for (size_t index = 0; index < test_cases.size(); ++index) {
      ARROW_SCOPED_TRACE("i=", index);
      const auto& tc = test_cases[index];

      const auto f16 = Float16(tc.input);
      EXPECT_EQ(tc.bits, f16.bits());
      EXPECT_EQ(tc.output, static_cast<T>(f16));

      EXPECT_EQ(std::signbit(tc.output), f16.signbit());
      EXPECT_EQ(std::isnan(tc.output), f16.is_nan());
      EXPECT_EQ(std::isinf(tc.output), f16.is_infinity());
      EXPECT_EQ(std::isfinite(tc.output), f16.is_finite());
    }
  }

  static void TestRoundTripFromNaN(span<const T> test_cases) {
    for (size_t i = 0; i < test_cases.size(); ++i) {
      ARROW_SCOPED_TRACE("i=", i);
      const auto input = test_cases[i];

      ASSERT_TRUE(std::isnan(input));
      const bool sign = std::signbit(input);

      const auto f16 = Float16(input);
      EXPECT_TRUE(f16.is_nan());
      EXPECT_EQ(std::isinf(input), f16.is_infinity());
      EXPECT_EQ(std::isfinite(input), f16.is_finite());
      EXPECT_EQ(sign, f16.signbit());

      const auto output = static_cast<T>(f16);
      EXPECT_TRUE(std::isnan(output));
      EXPECT_EQ(sign, std::signbit(output));
    }
  }

  void TestRoundTripFromInf() {
    const T test_cases[] = {+Limits<T>::infinity(), -Limits<T>::infinity()};

    for (size_t i = 0; i < std::size(test_cases); ++i) {
      ARROW_SCOPED_TRACE("i=", i);
      const auto input = test_cases[i];

      ASSERT_TRUE(std::isinf(input));
      const bool sign = std::signbit(input);

      const auto f16 = Float16(input);
      EXPECT_TRUE(f16.is_infinity());
      EXPECT_EQ(std::isfinite(input), f16.is_finite());
      EXPECT_EQ(std::isnan(input), f16.is_nan());
      EXPECT_EQ(sign, f16.signbit());

      const auto output = static_cast<T>(f16);
      EXPECT_TRUE(std::isinf(output));
      EXPECT_EQ(sign, std::signbit(output));
    }
  }

  void TestRoundTrip();
  void TestRoundTripFromNaN();
};

template <>
void Float16ConversionTest<float>::TestRoundTrip() {
  // Expected values were also manually validated with numpy-1.24.3
  const RoundTripTestCase test_cases[] = {
      // +/-0.0f
      {F32(0x80000000u), 0b1000000000000000u, -0.0f},
      {F32(0x00000000u), 0b0000000000000000u, +0.0f},
      // 32-bit exp is 102 => 2^-25. Rounding to nearest.
      {F32(0xb3000001u), 0b1000000000000001u, -5.96046447754e-8f},
      // 32-bit exp is 102 => 2^-25. Rounding to even.
      {F32(0xb3000000u), 0b1000000000000000u, -0.0f},
      // 32-bit exp is 101 => 2^-26. Underflow to zero.
      {F32(0xb2800001u), 0b1000000000000000u, -0.0f},
      // 32-bit exp is 108 => 2^-19.
      {F32(0xb61a0000u), 0b1000000000100110u, -2.26497650146e-6f},
      // 32-bit exp is 108 => 2^-19.
      {F32(0xb61e0000u), 0b1000000000101000u, -2.38418579102e-6f},
      // 32-bit exp is 112 => 2^-15. Rounding to nearest.
      {F32(0xb87fa001u), 0b1000001111111111u, -6.09755516052e-5f},
      // 32-bit exp is 112 => 2^-15. Rounds to 16-bit exp of 1 => 2^-14
      {F32(0xb87fe001u), 0b1000010000000000u, -6.103515625e-5f},
      // 32-bit exp is 142 => 2^15. Rounding to nearest.
      {F32(0xc7001001u), 0b1111100000000001u, -32800.0f},
      // 32-bit exp is 142 => 2^15. Rounding to even.
      {F32(0xc7001000u), 0b1111100000000000u, -32768.0f},
      // 65520.0f rounds to inf
      {F32(0x477ff000u), 0b0111110000000000u, Limits<float>::infinity()},
      // 65488.0039062f rounds to 65504.0 (float16 max)
      {F32(0x477fd001u), 0b0111101111111111u, 65504.0f},
      // 32-bit exp is 127 => 2^0, rounds to 16-bit exp of 16 => 2^1.
      {F32(0xbffff000u), 0b1100000000000000u, -2.0f},
      // Extreme values should safely clamp to +/-inf
      {Limits<float>::max(), 0b0111110000000000u, +Limits<float>::infinity()},
      {Limits<float>::lowest(), 0b1111110000000000u, -Limits<float>::infinity()},
  };

  TestRoundTrip(span(test_cases, std::size(test_cases)));
}

template <>
void Float16ConversionTest<double>::TestRoundTrip() {
  // Expected values were also manually validated with numpy-1.24.3
  const RoundTripTestCase test_cases[] = {
      // +/-0.0
      {F64(0x8000000000000000u), 0b1000000000000000u, -0.0},
      {F64(0x0000000000000000u), 0b0000000000000000u, +0.0},
      // 64-bit exp is 998 => 2^-25. Rounding to nearest.
      {F64(0xbe60000000000001u), 0b1000000000000001u, -5.9604644775390625e-8},
      // 64-bit exp is 998 => 2^-25. Rounding to even.
      {F64(0xbe60000000000000u), 0b1000000000000000u, -0.0},
      // 64-bit exp is 997 => 2^-26. Underflow to zero.
      {F64(0xbe50000000000001u), 0b1000000000000000u, -0.0},
      // 64-bit exp is 1004 => 2^-19.
      {F64(0xbec3400000000000u), 0b1000000000100110u, -2.2649765014648438e-6},
      // 64-bit exp is 1004 => 2^-19.
      {F64(0xbec3c00000000000u), 0b1000000000101000u, -2.3841857910156250e-6},
      // 64-bit exp is 1008 => 2^-15. Rounding to nearest.
      {F64(0xbf0ff40000000001u), 0b1000001111111111u, -6.0975551605224609e-5},
      // 64-bit exp is 1008 => 2^-15. Rounds to 16-bit exp of 1 => 2^-14
      {F64(0xbf0ffc0000000001u), 0b1000010000000000u, -6.1035156250000000e-5},
      // 64-bit exp is 1038 => 2^15. Rounding to nearest.
      {F64(0xc0e0020000000001u), 0b1111100000000001u, -32800.0},
      // 64-bit exp is 1038 => 2^15. Rounding to even.
      {F64(0xc0e0020000000000u), 0b1111100000000000u, -32768.0},
      // 65520.0 rounds to inf
      {F64(0x40effe0000000000u), 0b0111110000000000u, Limits<double>::infinity()},
      // 65488.00000000001 rounds to 65504.0 (float16 max)
      {F64(0x40effa0000000001u), 0b0111101111111111u, 65504.0},
      // 64-bit exp is 1023 => 2^0, rounds to 16-bit exp of 16 => 2^1.
      {F64(0xbffffe0000000000u), 0b1100000000000000u, -2.0},
      // Extreme values should safely clamp to +/-inf
      {Limits<double>::max(), 0b0111110000000000u, +Limits<double>::infinity()},
      {Limits<double>::lowest(), 0b1111110000000000u, -Limits<double>::infinity()},
  };

  TestRoundTrip(span(test_cases, std::size(test_cases)));
}

template <>
void Float16ConversionTest<float>::TestRoundTripFromNaN() {
  const float test_cases[] = {
      Limits<float>::quiet_NaN(), F32(0x7f800001u), F32(0xff800001u), F32(0x7fc00000u),
      F32(0xffc00000u),           F32(0x7fffffffu), F32(0xffffffffu)};
  TestRoundTripFromNaN(span(test_cases, std::size(test_cases)));
}

template <>
void Float16ConversionTest<double>::TestRoundTripFromNaN() {
  const double test_cases[] = {Limits<double>::quiet_NaN(), F64(0x7ff0000000000001u),
                               F64(0xfff0000000000001u),    F64(0x7ff8000000000000u),
                               F64(0xfff8000000000000u),    F64(0x7fffffffffffffffu),
                               F64(0xffffffffffffffffu)};
  TestRoundTripFromNaN(span(test_cases, std::size(test_cases)));
}

using NativeFloatTypes = ::testing::Types<float, double>;

TYPED_TEST_SUITE(Float16ConversionTest, NativeFloatTypes);

TYPED_TEST(Float16ConversionTest, RoundTrip) { this->TestRoundTrip(); }
TYPED_TEST(Float16ConversionTest, RoundTripFromNaN) { this->TestRoundTripFromNaN(); }
TYPED_TEST(Float16ConversionTest, RoundTripFromInf) { this->TestRoundTripFromInf(); }

TEST(Float16Test, ConstexprFunctions) {
  constexpr auto a = Float16::FromBits(0xbc00);  // -1.0
  constexpr auto b = Float16::FromBits(0x3c00);  // +1.0

  static_assert(a.bits() == 0xbc00);
  static_assert(a.signbit() == true);
  static_assert(a.is_nan() == false);
  static_assert(a.is_infinity() == false);
  static_assert(a.is_finite() == true);
  static_assert(a.is_zero() == false);

  static_assert((a == b) == false);
  static_assert((a != b) == true);
  static_assert((a < b) == true);
  static_assert((a > b) == false);
  static_assert((a <= b) == true);
  static_assert((a >= b) == false);
  static_assert(-a == +b);

  constexpr auto v = Float16::FromBits(0xffff);
  static_assert(v.ToBytes()[0] == 0xff);
  static_assert(v.ToLittleEndian()[0] == 0xff);
  static_assert(v.ToBigEndian()[0] == 0xff);
}

TEST(Float16Test, Constructors) {
  // Construction from exact bits
  ASSERT_EQ(1, Float16::FromBits(1).bits());
  // Construction from floating point (including implicit conversions)
  int i = 0;
  for (auto f16 : {Float16(1.0f), Float16(1.0), Float16(1)}) {
    ARROW_SCOPED_TRACE("i=", i++);
    ASSERT_EQ(0x3c00, f16.bits());
  }
}

TEST(Float16Test, Compare) {
  constexpr float f32_inf = Limits<float>::infinity();
  constexpr float f32_nan = Limits<float>::quiet_NaN();

  const struct {
    Float16 f16;
    float f32;
  } test_values[] = {
      {Limits<Float16>::min(), +6.103515625e-05f},
      {Limits<Float16>::max(), +65504.0f},
      {Limits<Float16>::lowest(), -65504.0f},
      {+Limits<Float16>::infinity(), +f32_inf},
      {-Limits<Float16>::infinity(), -f32_inf},
      // Multiple (semantically equivalent) NaN representations
      {Float16::FromBits(0x7e00), f32_nan},
      {Float16::FromBits(0xfe00), f32_nan},
      {Float16::FromBits(0x7fff), f32_nan},
      {Float16::FromBits(0xffff), f32_nan},
      // Positive/negative zeros
      {Float16::FromBits(0x0000), +0.0f},
      {Float16::FromBits(0x8000), -0.0f},
      // Miscellaneous values. In general, they're chosen to test the sign/exponent and
      // exponent/mantissa boundaries
      {Float16::FromBits(0x101c), +0.00050163269043f},
      {Float16::FromBits(0x901c), -0.00050163269043f},
      {Float16::FromBits(0x101d), +0.000502109527588f},
      {Float16::FromBits(0x901d), -0.000502109527588f},
      {Float16::FromBits(0x121c), +0.00074577331543f},
      {Float16::FromBits(0x921c), -0.00074577331543f},
      {Float16::FromBits(0x141c), +0.00100326538086f},
      {Float16::FromBits(0x941c), -0.00100326538086f},
      {Float16::FromBits(0x501c), +32.875f},
      {Float16::FromBits(0xd01c), -32.875f},
      // A few subnormals for good measure
      {Float16::FromBits(0x001c), +1.66893005371e-06f},
      {Float16::FromBits(0x801c), -1.66893005371e-06f},
      {Float16::FromBits(0x021c), +3.21865081787e-05f},
      {Float16::FromBits(0x821c), -3.21865081787e-05f},
  };

  auto expect_op = [&](std::string op_name, auto op) {
    ARROW_SCOPED_TRACE(op_name);
    const auto num_values = static_cast<int>(std::size(test_values));

    // Check all combinations of operands in both directions
    for (int i = 0; i < num_values; ++i) {
      for (int j = 0; j < num_values; ++j) {
        auto [a16, a32] = test_values[i];
        auto [b16, b32] = test_values[j];
        ARROW_SCOPED_TRACE("[", i, ",", j, "] = ", a16, ",", b16);

        // Results for float16 and float32 should be the same
        ASSERT_EQ(op(a16, b16), op(a32, b32));
      }
    }
  };

  // Verify that our "equivalent" 16/32-bit values actually are
  for (const auto& v : test_values) {
    if (std::isnan(v.f32)) {
      ASSERT_TRUE(std::isnan(v.f16.ToFloat()));
    } else {
      ASSERT_EQ(v.f32, v.f16.ToFloat());
    }
  }

  expect_op("equal", [](auto l, auto r) { return l == r; });
  expect_op("not_equal", [](auto l, auto r) { return l != r; });
  expect_op("less", [](auto l, auto r) { return l < r; });
  expect_op("greater", [](auto l, auto r) { return l > r; });
  expect_op("less_equal", [](auto l, auto r) { return l <= r; });
  expect_op("greater_equal", [](auto l, auto r) { return l >= r; });
}

TEST(Float16Test, ToBytes) {
  constexpr auto f16 = Float16::FromBits(0xd01c);
  std::array<uint8_t, 2> bytes;
  auto load = [&bytes]() { return SafeLoadAs<uint16_t>(bytes.data()); };

  // Test native-endian
  f16.ToBytes(bytes.data());
  ASSERT_EQ(load(), 0xd01c);
  bytes = f16.ToBytes();
  ASSERT_EQ(load(), 0xd01c);

#if ARROW_LITTLE_ENDIAN
  constexpr uint16_t expected_le = 0xd01c;
  constexpr uint16_t expected_be = 0x1cd0;
#else
  constexpr uint16_t expected_le = 0x1cd0;
  constexpr uint16_t expected_be = 0xd01c;
#endif
  // Test little-endian
  f16.ToLittleEndian(bytes.data());
  ASSERT_EQ(load(), expected_le);
  bytes = f16.ToLittleEndian();
  ASSERT_EQ(load(), expected_le);
  // Test big-endian
  f16.ToBigEndian(bytes.data());
  ASSERT_EQ(load(), expected_be);
  bytes = f16.ToBigEndian();
  ASSERT_EQ(load(), expected_be);
}

TEST(Float16Test, FromBytes) {
  constexpr uint16_t u16 = 0xd01c;
  const auto* data = reinterpret_cast<const uint8_t*>(&u16);
  ASSERT_EQ(Float16::FromBytes(data), Float16::FromBits(0xd01c));
#if ARROW_LITTLE_ENDIAN
  ASSERT_EQ(Float16::FromLittleEndian(data), Float16::FromBits(0xd01c));
  ASSERT_EQ(Float16::FromBigEndian(data), Float16::FromBits(0x1cd0));
#else
  ASSERT_EQ(Float16::FromLittleEndian(data), Float16(0x1cd0));
  ASSERT_EQ(Float16::FromBigEndian(data), Float16(0xd01c));
#endif
}

}  // namespace
}  // namespace arrow::util

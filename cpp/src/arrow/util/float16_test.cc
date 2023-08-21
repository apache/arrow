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
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/float16.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace {

template <typename T>
using Limits = std::numeric_limits<T>;

float F32(uint32_t bits) { return SafeCopy<float>(bits); }

TEST(Float16Test, RoundTripFromFloat32) {
  struct TestCase {
    float f32;
    uint16_t b16;
    float f16_as_f32;
  };
  // Expected values were also manually validated with numpy-1.24.3
  const TestCase test_cases[] = {
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
  };

  for (size_t index = 0; index < std::size(test_cases); ++index) {
    ARROW_SCOPED_TRACE("index=", index);
    const auto& tc = test_cases[index];
    const auto f16 = Float16::FromFloat(tc.f32);
    EXPECT_EQ(tc.b16, f16.bits());
    EXPECT_EQ(tc.f16_as_f32, f16.ToFloat());
  }
}

TEST(Float16Test, RoundTripFromFloat32Nan) {
  const float nan_test_cases[] = {
      Limits<float>::quiet_NaN(), F32(0x7f800001u), F32(0xff800001u), F32(0x7fc00000u),
      F32(0xff800001u),           F32(0x7fffffffu), F32(0xffffffffu)};

  for (size_t i = 0; i < std::size(nan_test_cases); ++i) {
    ARROW_SCOPED_TRACE("i=", i);
    const auto f32 = nan_test_cases[i];

    ASSERT_TRUE(std::isnan(f32));
    const bool sign = std::signbit(f32);

    const auto f16 = Float16::FromFloat(f32);
    EXPECT_TRUE(f16.is_nan());
    EXPECT_EQ(sign, f16.signbit());

    const auto f16_as_f32 = f16.ToFloat();
    EXPECT_TRUE(std::isnan(f16_as_f32));
    EXPECT_EQ(sign, std::signbit(f16_as_f32));
  }
}

TEST(Float16Test, RoundTripFromFloat32Inf) {
  const float test_cases[] = {+Limits<float>::infinity(), -Limits<float>::infinity()};

  for (size_t i = 0; i < std::size(test_cases); ++i) {
    ARROW_SCOPED_TRACE("i=", i);
    const auto f32 = test_cases[i];

    ASSERT_TRUE(std::isinf(f32));
    const bool sign = std::signbit(f32);

    const auto f16 = Float16::FromFloat(f32);
    EXPECT_TRUE(f16.is_infinity());
    EXPECT_EQ(sign, f16.signbit());

    const auto f16_as_f32 = f16.ToFloat();
    EXPECT_TRUE(std::isinf(f16_as_f32));
    EXPECT_EQ(sign, std::signbit(f16_as_f32));
  }
}

// Holds a float16 and its equivalent float32
struct TestValue {
  TestValue(Float16 f16, float f32) : f16(f16), f32(f32) {}
  TestValue(uint16_t u16, float f32) : TestValue(Float16(u16), f32) {}

  Float16 f16;
  float f32;
};

#define GENERATE_OPERATOR(NAME, OP)                              \
  struct NAME {                                                  \
    std::pair<bool, bool> operator()(TestValue l, TestValue r) { \
      return std::make_pair((l.f32 OP r.f32), (l.f16 OP r.f16)); \
    }                                                            \
  }

GENERATE_OPERATOR(CompareEq, ==);
GENERATE_OPERATOR(CompareNe, !=);
GENERATE_OPERATOR(CompareLt, <);
GENERATE_OPERATOR(CompareGt, >);
GENERATE_OPERATOR(CompareLe, <=);
GENERATE_OPERATOR(CompareGe, >=);

#undef GENERATE_OPERATOR

const std::vector<TestValue> g_test_values = {
    TestValue(Limits<Float16>::min(), +0.00006104f),
    TestValue(Limits<Float16>::max(), +65504.0f),
    TestValue(Limits<Float16>::lowest(), -65504.0f),
    TestValue(+Limits<Float16>::infinity(), +Limits<float>::infinity()),
    TestValue(-Limits<Float16>::infinity(), -Limits<float>::infinity()),
    // Multiple (semantically equivalent) NaN representations
    TestValue(0x7fff, Limits<float>::quiet_NaN()),
    TestValue(0xffff, Limits<float>::quiet_NaN()),
    TestValue(0x7e00, Limits<float>::quiet_NaN()),
    TestValue(0xfe00, Limits<float>::quiet_NaN()),
    // Positive/negative zeroes
    TestValue(0x0000, +0.0f),
    TestValue(0x8000, -0.0f),
    // Miscellaneous values. In general, they're chosen to test the sign/exponent and
    // exponent/mantissa boundaries
    TestValue(0x101c, +0.000502f),
    TestValue(0x901c, -0.000502f),
    TestValue(0x101d, +0.0005022f),
    TestValue(0x901d, -0.0005022f),
    TestValue(0x121c, +0.000746f),
    TestValue(0x921c, -0.000746f),
    TestValue(0x141c, +0.001004f),
    TestValue(0x941c, -0.001004f),
    TestValue(0x501c, +32.9f),
    TestValue(0xd01c, -32.9f),
    // A few subnormals for good measure
    TestValue(0x001c, +0.0000017f),
    TestValue(0x801c, -0.0000017f),
    TestValue(0x021c, +0.0000332f),
    TestValue(0x821c, -0.0000332f),
};

template <typename Operator>
class Float16OperatorTest : public ::testing::Test {
 public:
  void TestCompare(const std::vector<TestValue>& test_values) {
    const auto num_values = static_cast<int>(test_values.size());

    // Check all combinations of operands in both directions
    for (int i = 0; i < num_values; ++i) {
      for (int j = 0; j < num_values; ++j) {
        ARROW_SCOPED_TRACE(i, ",", j);

        auto a = test_values[i];
        auto b = test_values[j];

        // Results for float16 and float32 should be the same
        auto ret = Operator{}(a, b);
        ASSERT_EQ(ret.first, ret.second);
      }
    }
  }
};

using OperatorTypes =
    ::testing::Types<CompareEq, CompareNe, CompareLt, CompareGt, CompareLe, CompareGe>;

TYPED_TEST_SUITE(Float16OperatorTest, OperatorTypes);

TYPED_TEST(Float16OperatorTest, Compare) { this->TestCompare(g_test_values); }

TEST(Float16Test, ToBytes) {
  constexpr auto f16 = Float16(0xd01c);
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
  ASSERT_EQ(Float16::FromBytes(data), Float16(0xd01c));
#if ARROW_LITTLE_ENDIAN
  ASSERT_EQ(Float16::FromLittleEndian(data), Float16(0xd01c));
  ASSERT_EQ(Float16::FromBigEndian(data), Float16(0x1cd0));
#else
  ASSERT_EQ(Float16::FromLittleEndian(data), Float16(0x1cd0));
  ASSERT_EQ(Float16::FromBigEndian(data), Float16(0xd01c));
#endif
}

}  // namespace
}  // namespace util
}  // namespace arrow

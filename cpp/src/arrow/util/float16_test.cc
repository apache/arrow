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
    for (int offset = 0; offset < num_values; ++offset) {
      int i = 0;
      int j = offset;
      while (i < num_values) {
        ARROW_SCOPED_TRACE(i, ",", j);

        auto a = test_values[i];
        auto b = test_values[j];

        // Results for float16 and float32 should be the same
        auto ret = Operator{}(a, b);
        ASSERT_EQ(ret.first, ret.second);

        ++i;
        j = (j + 1) % num_values;
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
  auto bytes = f16.ToBytes();
  ASSERT_EQ(SafeLoadAs<uint16_t>(bytes.data()), 0xd01c);
#if ARROW_LITTLE_ENDIAN
  bytes = f16.ToLittleEndian();
  ASSERT_EQ(SafeLoadAs<uint16_t>(bytes.data()), 0xd01c);
  bytes = f16.ToBigEndian();
  ASSERT_EQ(SafeLoadAs<uint16_t>(bytes.data()), 0x1cd0);
#else
  bytes = f16.ToLittleEndian();
  ASSERT_EQ(SafeLoadAs<uint16_t>(bytes.data()), 0x1cd0);
  bytes = f16.ToBigEndian();
  ASSERT_EQ(SafeLoadAs<uint16_t>(bytes.data()), 0xd01c);
#endif
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

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
//

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int128.h"

namespace arrow {
namespace decimal {

class DecimalTestFixture : public ::testing::Test {
 public:
  DecimalTestFixture() : integer_value_(23423445), string_value_("234.23445") {}
  Int128 integer_value_;
  std::string string_value_;
};

TEST_F(DecimalTestFixture, TestToString) {
  Int128 decimal(this->integer_value_);
  int precision = 8;
  int scale = 5;
  std::string result = ToString(decimal, precision, scale);
  ASSERT_EQ(result, this->string_value_);
}

TEST_F(DecimalTestFixture, TestFromString) {
  Int128 expected(this->integer_value_);
  Int128 result;
  int precision, scale;
  ASSERT_OK(FromString(this->string_value_, &result, &precision, &scale));
  ASSERT_EQ(result, expected);
  ASSERT_EQ(precision, 8);
  ASSERT_EQ(scale, 5);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus) {
  std::string plus_value("+234.234");
  Int128 out;
  int scale;
  int precision;
  ASSERT_OK(FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(234234, out);
  ASSERT_EQ(6, precision);
  ASSERT_EQ(3, scale);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus128) {
  std::string plus_value("+2342394230592.232349023094");
  Int128 expected_value("2342394230592232349023094");
  Int128 out;
  int scale;
  int precision;
  ASSERT_OK(FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(expected_value, out);
  ASSERT_EQ(25, precision);
  ASSERT_EQ(12, scale);
}

TEST(DecimalTest, TestStringToInt32) {
  Int128 value;
  StringToInteger("123", "456", 1, &value);
  ASSERT_EQ(value, 123456);
}

TEST(DecimalTest, TestStringToInt64) {
  Int128 value;
  StringToInteger("123456789", "456", -1, &value);
  ASSERT_EQ(value, -123456789456);
}

TEST(DecimalTest, TestStringToInt128) {
  Int128 value;
  StringToInteger("123456789", "456789123", 1, &value);
  ASSERT_EQ(value.high_bits(), 0);
  ASSERT_EQ(value.low_bits(), 123456789456789123);
}

TEST(DecimalTest, TestFromString128) {
  static const std::string string_value("-23049223942343532412");
  Int128 result(string_value);
  Int128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestFromDecimalString128) {
  std::string string_value("-23049223942343.532412");
  Int128 result;
  ASSERT_OK(FromString(string_value, &result));
  Int128 expected(static_cast<int64_t>(-230492239423435324));
  expected *= 100;
  expected -= 12;
  ASSERT_EQ(result, expected);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestDecimal32SignedRoundTrip) {
  Int128 expected("-3402692");

  std::array<uint8_t, 16> bytes;
  ASSERT_OK(expected.ToBytes(&bytes));

  Int128 result(bytes.data());
  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimal64SignedRoundTrip) {
  Int128 expected;
  std::string string_value("-34034293045.921");
  ASSERT_OK(FromString(string_value, &expected));

  std::array<uint8_t, 16> bytes;
  ASSERT_OK(expected.ToBytes(&bytes));

  Int128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimalStringAndBytesRoundTrip) {
  Int128 expected;
  std::string string_value("-340282366920938463463374607431.711455");
  ASSERT_OK(FromString(string_value, &expected));

  std::string expected_string_value("-340282366920938463463374607431711455");
  Int128 expected_underlying_value(expected_string_value);

  ASSERT_EQ(expected, expected_underlying_value);

  std::array<uint8_t, 16> bytes;
  ASSERT_OK(expected.ToBytes(&bytes));

  Int128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestInvalidInputMinus) {
  std::string invalid_value("-");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputDot) {
  std::string invalid_value("0.0.0");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputEmbeddedMinus) {
  std::string invalid_value("0-13-32");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputSingleChar) {
  std::string invalid_value("a");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithValidSubstring) {
  std::string invalid_value("-23092.235-");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  auto msg = status.message();
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithMinusPlus) {
  std::string invalid_value("-+23092.235");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithPlusMinus) {
  std::string invalid_value("+-23092.235");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalTest, TestInvalidInputWithLeadingZeros) {
  std::string invalid_value("00a");
  Int128 out;
  Status status = decimal::FromString(invalid_value, &out);
  ASSERT_RAISES(Invalid, status);
}

TEST(DecimalZerosTest, LeadingZerosNoDecimalPoint) {
  std::string string_value("0000000");
  Int128 d;
  int precision;
  int scale;
  ASSERT_OK(FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(precision, 7);
  ASSERT_EQ(scale, 0);
  ASSERT_EQ(d, 0);
}

TEST(DecimalZerosTest, LeadingZerosDecimalPoint) {
  std::string string_value("000.0000");
  Int128 d;
  int precision;
  int scale;
  ASSERT_OK(FromString(string_value, &d, &precision, &scale));
  // We explicitly do not support this for now, otherwise this would be ASSERT_EQ
  ASSERT_NE(precision, 7);

  ASSERT_EQ(scale, 4);
  ASSERT_EQ(d, 0);
}

TEST(DecimalZerosTest, NoLeadingZerosDecimalPoint) {
  std::string string_value(".00000");
  Int128 d;
  int precision;
  int scale;
  ASSERT_OK(FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(precision, 5);
  ASSERT_EQ(scale, 5);
  ASSERT_EQ(d, 0);
}

template <typename T>
class Int128Test : public ::testing::Test {
 public:
  Int128Test() : value_(42) {}
  const T value_;
};

using Int128Types =
    ::testing::Types<char, unsigned char, short, unsigned short,  // NOLINT
                     int, unsigned int, long, unsigned long,      // NOLINT
                     long long, unsigned long long                // NOLINT
                     >;

TYPED_TEST_CASE(Int128Test, Int128Types);

TYPED_TEST(Int128Test, ConstructibleFromAnyIntegerType) {
  Int128 value(this->value_);
  ASSERT_EQ(42, value.low_bits());
}

TEST(Int128TestTrue, ConstructibleFromBool) {
  Int128 value(true);
  ASSERT_EQ(1, value.low_bits());
}

TEST(Int128TestFalse, ConstructibleFromBool) {
  Int128 value(false);
  ASSERT_EQ(0, value.low_bits());
}

}  // namespace decimal
}  // namespace arrow

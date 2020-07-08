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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>
#include <boost/multiprecision/cpp_int.hpp>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"

using boost::multiprecision::int128_t;

namespace arrow {

class DecimalTestFixture : public ::testing::Test {
 public:
  DecimalTestFixture() : integer_value_(23423445), string_value_("234.23445") {}
  Decimal128 integer_value_;
  std::string string_value_;
};

TEST_F(DecimalTestFixture, TestToString) {
  Decimal128 decimal(this->integer_value_);
  int32_t scale = 5;
  std::string result = decimal.ToString(scale);
  ASSERT_EQ(result, this->string_value_);
}

TEST_F(DecimalTestFixture, TestFromString) {
  Decimal128 expected(this->integer_value_);
  Decimal128 result;
  int32_t precision, scale;
  ASSERT_OK(Decimal128::FromString(this->string_value_, &result, &precision, &scale));
  ASSERT_EQ(result, expected);
  ASSERT_EQ(precision, 8);
  ASSERT_EQ(scale, 5);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus) {
  std::string plus_value("+234.234");
  Decimal128 out;
  int32_t scale;
  int32_t precision;
  ASSERT_OK(Decimal128::FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(234234, out);
  ASSERT_EQ(6, precision);
  ASSERT_EQ(3, scale);
}

TEST_F(DecimalTestFixture, TestStringStartingWithPlus128) {
  std::string plus_value("+2342394230592.232349023094");
  Decimal128 expected_value("2342394230592232349023094");
  Decimal128 out;
  int32_t scale;
  int32_t precision;
  ASSERT_OK(Decimal128::FromString(plus_value, &out, &precision, &scale));
  ASSERT_EQ(expected_value, out);
  ASSERT_EQ(25, precision);
  ASSERT_EQ(12, scale);
}

TEST(DecimalTest, TestFromStringDecimal128) {
  std::string string_value("-23049223942343532412");
  Decimal128 result(string_value);
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestFromDecimalString128) {
  std::string string_value("-23049223942343.532412");
  Decimal128 result;
  ASSERT_OK_AND_ASSIGN(result, Decimal128::FromString(string_value));
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(DecimalTest, TestDecimal32SignedRoundTrip) {
  Decimal128 expected("-3402692");

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());
  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimal64SignedRoundTrip) {
  Decimal128 expected;
  std::string string_value("-34034293045.921");
  ASSERT_OK_AND_ASSIGN(expected, Decimal128::FromString(string_value));

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestDecimalStringAndBytesRoundTrip) {
  Decimal128 expected;
  std::string string_value("-340282366920938463463374607431.711455");
  ASSERT_OK_AND_ASSIGN(expected, Decimal128::FromString(string_value));

  std::string expected_string_value("-340282366920938463463374607431711455");
  Decimal128 expected_underlying_value(expected_string_value);

  ASSERT_EQ(expected, expected_underlying_value);

  auto bytes = expected.ToBytes();

  Decimal128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(DecimalTest, TestInvalidInputMinus) {
  std::string invalid_value("-");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputDot) {
  std::string invalid_value("0.0.0");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputEmbeddedMinus) {
  std::string invalid_value("0-13-32");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputSingleChar) {
  std::string invalid_value("a");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputWithValidSubstring) {
  std::string invalid_value("-23092.235-");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputWithMinusPlus) {
  std::string invalid_value("-+23092.235");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputWithPlusMinus) {
  std::string invalid_value("+-23092.235");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalTest, TestInvalidInputWithLeadingZeros) {
  std::string invalid_value("00a");
  ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
}

TEST(DecimalZerosTest, LeadingZerosNoDecimalPoint) {
  std::string string_value("0000000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(0, precision);
  ASSERT_EQ(0, scale);
  ASSERT_EQ(0, d);
}

TEST(DecimalZerosTest, LeadingZerosDecimalPoint) {
  std::string string_value("000.0000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(4, precision);
  ASSERT_EQ(4, scale);
  ASSERT_EQ(0, d);
}

TEST(DecimalZerosTest, NoLeadingZerosDecimalPoint) {
  std::string string_value(".00000");
  Decimal128 d;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(string_value, &d, &precision, &scale));
  ASSERT_EQ(5, precision);
  ASSERT_EQ(5, scale);
  ASSERT_EQ(0, d);
}

template <typename T>
class Decimal128Test : public ::testing::Test {
 public:
  Decimal128Test() : value_(42) {}
  const T value_;
};

using Decimal128Types =
    ::testing::Types<char, unsigned char, short, unsigned short,  // NOLINT
                     int, unsigned int, long, unsigned long,      // NOLINT
                     long long, unsigned long long                // NOLINT
                     >;

TYPED_TEST_SUITE(Decimal128Test, Decimal128Types);

TYPED_TEST(Decimal128Test, ConstructibleFromAnyIntegerType) {
  Decimal128 value(this->value_);
  ASSERT_EQ(42, value.low_bits());
}

TEST(Decimal128TestTrue, ConstructibleFromBool) {
  Decimal128 value(true);
  ASSERT_EQ(1, value.low_bits());
}

TEST(Decimal128TestFalse, ConstructibleFromBool) {
  Decimal128 value(false);
  ASSERT_EQ(0, value.low_bits());
}

TEST(Decimal128Test, Division) {
  const std::string expected_string_value("-23923094039234029");
  const Decimal128 value(expected_string_value);
  const Decimal128 result(value / 3);
  const Decimal128 expected_value("-7974364679744676");
  ASSERT_EQ(expected_value, result);
}

TEST(Decimal128Test, PrintLargePositiveValue) {
  const std::string string_value("99999999999999999999999999999999999999");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintLargeNegativeValue) {
  const std::string string_value("-99999999999999999999999999999999999999");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintMaxValue) {
  const std::string string_value("170141183460469231731687303715884105727");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

TEST(Decimal128Test, PrintMinValue) {
  const std::string string_value("-170141183460469231731687303715884105728");
  const Decimal128 value(string_value);
  const std::string printed_value = value.ToIntegerString();
  ASSERT_EQ(string_value, printed_value);
}

class Decimal128PrintingTest
    : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, std::string>> {};

TEST_P(Decimal128PrintingTest, Print) {
  int32_t test_value;
  int32_t scale;
  std::string expected_string;
  std::tie(test_value, scale, expected_string) = GetParam();
  const Decimal128 value(test_value);
  const std::string printed_value = value.ToString(scale);
  ASSERT_EQ(expected_string, printed_value);
}

INSTANTIATE_TEST_SUITE_P(Decimal128PrintingTest, Decimal128PrintingTest,
                         ::testing::Values(std::make_tuple(123, 1, "12.3"),
                                           std::make_tuple(123, 5, "0.00123"),
                                           std::make_tuple(123, 10, "1.23E-8"),
                                           std::make_tuple(123, -1, "1.23E+3"),
                                           std::make_tuple(-123, -1, "-1.23E+3"),
                                           std::make_tuple(123, -3, "1.23E+5"),
                                           std::make_tuple(-123, -3, "-1.23E+5"),
                                           std::make_tuple(12345, -3, "1.2345E+7")));

class Decimal128ParsingTest
    : public ::testing::TestWithParam<std::tuple<std::string, uint64_t, int32_t>> {};

TEST_P(Decimal128ParsingTest, Parse) {
  std::string test_string;
  uint64_t expected_low_bits;
  int32_t expected_scale;
  std::tie(test_string, expected_low_bits, expected_scale) = GetParam();
  Decimal128 value;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString(test_string, &value, nullptr, &scale));
  ASSERT_EQ(value.low_bits(), expected_low_bits);
  ASSERT_EQ(expected_scale, scale);
}

INSTANTIATE_TEST_SUITE_P(Decimal128ParsingTest, Decimal128ParsingTest,
                         ::testing::Values(std::make_tuple("12.3", 123ULL, 1),
                                           std::make_tuple("0.00123", 123ULL, 5),
                                           std::make_tuple("1.23E-8", 123ULL, 10),
                                           std::make_tuple("-1.23E-8", -123LL, 10),
                                           std::make_tuple("1.23E+3", 1230ULL, 0),
                                           std::make_tuple("-1.23E+3", -1230LL, 0),
                                           std::make_tuple("1.23E+5", 123000ULL, 0),
                                           std::make_tuple("1.2345E+7", 12345000ULL, 0),
                                           std::make_tuple("1.23e-8", 123ULL, 10),
                                           std::make_tuple("-1.23e-8", -123LL, 10),
                                           std::make_tuple("1.23e+3", 1230ULL, 0),
                                           std::make_tuple("-1.23e+3", -1230LL, 0),
                                           std::make_tuple("1.23e+5", 123000ULL, 0),
                                           std::make_tuple("1.2345e+7", 12345000ULL, 0)));

class Decimal128ParsingTestInvalid : public ::testing::TestWithParam<std::string> {};

TEST_P(Decimal128ParsingTestInvalid, Parse) {
  std::string test_string = GetParam();
  ASSERT_RAISES(Invalid, Decimal128::FromString(test_string));
}

INSTANTIATE_TEST_SUITE_P(Decimal128ParsingTestInvalid, Decimal128ParsingTestInvalid,
                         ::testing::Values("0.00123D/3", "1.23eA8", "1.23E+3A",
                                           "-1.23E--5", "1.2345E+++07"));

TEST(Decimal128ParseTest, WithExponentAndNullptrScale) {
  const Decimal128 expected_value(123);
  ASSERT_OK_AND_EQ(expected_value, Decimal128::FromString("1.23E-8"));
}

template <typename Real>
void CheckDecimalFromReal(Real real, int32_t precision, int32_t scale,
                          const std::string& expected) {
  ASSERT_OK_AND_ASSIGN(auto dec, Decimal128::FromReal(real, precision, scale));
  ASSERT_EQ(dec.ToString(scale), expected);
}

template <typename Real>
void CheckDecimalFromRealIntegerString(Real real, int32_t precision, int32_t scale,
                                       const std::string& expected) {
  ASSERT_OK_AND_ASSIGN(auto dec, Decimal128::FromReal(real, precision, scale));
  ASSERT_EQ(dec.ToIntegerString(), expected);
}

template <typename Real>
struct FromRealTestParam {
  Real real;
  int32_t precision;
  int32_t scale;
  std::string expected;

  // Weird, but we need to define this to avoid Valgrind issues
  // with the default GTest print routine.
  friend std::ostream& operator<<(std::ostream& os,
                                  const FromRealTestParam<Real>& param) {
    return os << "<real: " << param.real << ">";
  }
};

using FromFloatTestParam = FromRealTestParam<float>;
using FromDoubleTestParam = FromRealTestParam<double>;

// Common tests for Decimal128::FromReal(T, ...)
template <typename T>
class TestDecimalFromReal : public ::testing::Test {
 public:
  using ParamType = FromRealTestParam<T>;

  void TestSuccess() {
    const std::vector<ParamType> params{
        // clang-format off
        {0.0f, 1, 0, "0"},
        {-0.0f, 1, 0, "0"},
        {0.0f, 19, 4, "0.0000"},
        {-0.0f, 19, 4, "0.0000"},
        {123.0f, 7, 4, "123.0000"},
        {-123.0f, 7, 4, "-123.0000"},
        {456.78f, 7, 4, "456.7800"},
        {-456.78f, 7, 4, "-456.7800"},
        {456.784f, 5, 2, "456.78"},
        {-456.784f, 5, 2, "-456.78"},
        {456.786f, 5, 2, "456.79"},
        {-456.786f, 5, 2, "-456.79"},
        {999.99f, 5, 2, "999.99"},
        {-999.99f, 5, 2, "-999.99"},
        {123.0f, 19, 0, "123"},
        {-123.0f, 19, 0, "-123"},
        {123.4f, 19, 0, "123"},
        {-123.4f, 19, 0, "-123"},
        {123.6f, 19, 0, "124"},
        {-123.6f, 19, 0, "-124"},
        // 2**62
        {4.611686e+18f, 19, 0, "4611686018427387904"},
        {-4.611686e+18f, 19, 0, "-4611686018427387904"},
        // 2**63
        {9.223372e+18f, 19, 0, "9223372036854775808"},
        {-9.223372e+18f, 19, 0, "-9223372036854775808"},
        // 2**64
        {1.8446744e+19f, 20, 0, "18446744073709551616"},
        {-1.8446744e+19f, 20, 0, "-18446744073709551616"}
        // clang-format on
    };
    for (const ParamType& param : params) {
      CheckDecimalFromReal(param.real, param.precision, param.scale, param.expected);
    }
  }

  void TestErrors() {
    ASSERT_RAISES(Invalid, Decimal128::FromReal(INFINITY, 19, 4));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(-INFINITY, 19, 4));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(NAN, 19, 4));
    // Overflows
    ASSERT_RAISES(Invalid, Decimal128::FromReal(1000.0, 3, 0));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(-1000.0, 3, 0));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(1000.0, 5, 2));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(-1000.0, 5, 2));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(999.996, 5, 2));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(-999.996, 5, 2));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(1e+38, 38, 0));
    ASSERT_RAISES(Invalid, Decimal128::FromReal(-1e+38, 38, 0));
  }
};

using RealTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(TestDecimalFromReal, RealTypes);

TYPED_TEST(TestDecimalFromReal, TestSuccess) { this->TestSuccess(); }

TYPED_TEST(TestDecimalFromReal, TestErrors) { this->TestErrors(); }

// Custom test for Decimal128::FromReal(float, ...)
class TestDecimalFromRealFloat : public ::testing::TestWithParam<FromFloatTestParam> {};

TEST_P(TestDecimalFromRealFloat, SuccessConversion) {
  const auto param = GetParam();
  CheckDecimalFromReal(param.real, param.precision, param.scale, param.expected);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    TestDecimalFromRealFloat, TestDecimalFromRealFloat,
    ::testing::Values(
        // 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
        FromFloatTestParam{9.223373e+18f, 19, 0, "9223373136366403584"},
        FromFloatTestParam{-9.223373e+18f, 19, 0, "-9223373136366403584"},
        FromFloatTestParam{9.223373e+14f, 19, 4, "922337313636640.3584"},
        FromFloatTestParam{-9.223373e+14f, 19, 4, "-922337313636640.3584"},
        // 2**64 - 2**40 (exactly representable in a float)
        FromFloatTestParam{1.8446743e+19f, 20, 0, "18446742974197923840"},
        FromFloatTestParam{-1.8446743e+19f, 20, 0, "-18446742974197923840"},
        // 2**64 + 2**41 (exactly representable in a float)
        FromFloatTestParam{1.8446746e+19f, 20, 0, "18446746272732807168"},
        FromFloatTestParam{-1.8446746e+19f, 20, 0, "-18446746272732807168"},
        FromFloatTestParam{1.8446746e+15f, 20, 4, "1844674627273280.7168"},
        FromFloatTestParam{-1.8446746e+15f, 20, 4, "-1844674627273280.7168"},
        // Almost 10**38 (minus 2**103)
        FromFloatTestParam{9.999999e+37f, 38, 0,
                           "99999986661652122824821048795547566080"},
        FromFloatTestParam{-9.999999e+37f, 38, 0,
                           "-99999986661652122824821048795547566080"}
));
// clang-format on

TEST(TestDecimalFromRealFloat, LargeValues) {
  // Test the entire float range
  for (int32_t scale = -38; scale <= 38; ++scale) {
    float real = std::pow(10.0f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString(real, 1, -scale, "1");
  }
  for (int32_t scale = -37; scale <= 36; ++scale) {
    float real = 123.f * std::pow(10.f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString(real, 2, -scale - 1, "12");
    CheckDecimalFromRealIntegerString(real, 3, -scale, "123");
    CheckDecimalFromRealIntegerString(real, 4, -scale + 1, "1230");
  }
}

// Custom test for Decimal128::FromReal(double, ...)
class TestDecimalFromRealDouble : public ::testing::TestWithParam<FromDoubleTestParam> {};

TEST_P(TestDecimalFromRealDouble, SuccessConversion) {
  const auto param = GetParam();
  CheckDecimalFromReal(param.real, param.precision, param.scale, param.expected);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    TestDecimalFromRealDouble, TestDecimalFromRealDouble,
    ::testing::Values(
        // 2**63 + 2**11 (exactly representable in a double's 53 bits of precision)
        FromDoubleTestParam{9.223372036854778e+18, 19, 0, "9223372036854777856"},
        FromDoubleTestParam{-9.223372036854778e+18, 19, 0, "-9223372036854777856"},
        FromDoubleTestParam{9.223372036854778e+10, 19, 8, "92233720368.54777856"},
        FromDoubleTestParam{-9.223372036854778e+10, 19, 8, "-92233720368.54777856"},
        // 2**64 - 2**11 (exactly representable in a double)
        FromDoubleTestParam{1.844674407370955e+19, 20, 0, "18446744073709549568"},
        FromDoubleTestParam{-1.844674407370955e+19, 20, 0, "-18446744073709549568"},
        // 2**64 + 2**11 (exactly representable in a double)
        FromDoubleTestParam{1.8446744073709556e+19, 20, 0, "18446744073709555712"},
        FromDoubleTestParam{-1.8446744073709556e+19, 20, 0, "-18446744073709555712"},
        FromDoubleTestParam{1.8446744073709556e+15, 20, 4, "1844674407370955.5712"},
        FromDoubleTestParam{-1.8446744073709556e+15, 20, 4, "-1844674407370955.5712"},
        // Almost 10**38 (minus 2**73)
        FromDoubleTestParam{9.999999999999998e+37, 38, 0,
                            "99999999999999978859343891977453174784"},
        FromDoubleTestParam{-9.999999999999998e+37, 38, 0,
                            "-99999999999999978859343891977453174784"},
        FromDoubleTestParam{9.999999999999998e+27, 38, 10,
                            "9999999999999997885934389197.7453174784"},
        FromDoubleTestParam{-9.999999999999998e+27, 38, 10,
                            "-9999999999999997885934389197.7453174784"}
));
// clang-format on

TEST(TestDecimalFromRealDouble, LargeValues) {
  // Test the entire double range
  for (int32_t scale = -308; scale <= 308; ++scale) {
    double real = std::pow(10.0, static_cast<double>(scale));
    CheckDecimalFromRealIntegerString(real, 1, -scale, "1");
  }
  for (int32_t scale = -307; scale <= 306; ++scale) {
    double real = 123. * std::pow(10.0, static_cast<double>(scale));
    CheckDecimalFromRealIntegerString(real, 2, -scale - 1, "12");
    CheckDecimalFromRealIntegerString(real, 3, -scale, "123");
    CheckDecimalFromRealIntegerString(real, 4, -scale + 1, "1230");
  }
}

template <typename Real>
struct ToRealTestParam {
  std::string decimal_value;
  int32_t scale;
  Real expected;
};

using ToFloatTestParam = ToRealTestParam<float>;
using ToDoubleTestParam = ToRealTestParam<double>;

template <typename Real>
void CheckDecimalToReal(const std::string& decimal_value, int32_t scale, Real expected) {
  Decimal128 dec(decimal_value);
  ASSERT_EQ(dec.ToReal<Real>(scale), expected);
}

void CheckFloatToRealApprox(const std::string& decimal_value, int32_t scale,
                            float expected) {
  Decimal128 dec(decimal_value);
  ASSERT_FLOAT_EQ(dec.ToReal<float>(scale), expected);
}

void CheckDoubleToRealApprox(const std::string& decimal_value, int32_t scale,
                             double expected) {
  Decimal128 dec(decimal_value);
  ASSERT_DOUBLE_EQ(dec.ToReal<double>(scale), expected);
}

// Common tests for Decimal128::ToReal<T>
template <typename T>
class TestDecimalToReal : public ::testing::Test {
 public:
  using Real = T;
  using ParamType = ToRealTestParam<T>;

  Real Pow2(int exp) { return std::pow(static_cast<Real>(2), static_cast<Real>(exp)); }

  Real Pow10(int exp) { return std::pow(static_cast<Real>(10), static_cast<Real>(exp)); }

  void TestSuccess() {
    const std::vector<ParamType> params{
        // clang-format off
        {"0", 0, 0.0f},
        {"0", 10, 0.0f},
        {"0", -10, 0.0f},
        {"1", 0, 1.0f},
        {"12345", 0, 12345.f},
#ifndef __MINGW32__  // MinGW has precision issues
        {"12345", 1, 1234.5f},
#endif
        {"12345", -3, 12345000.f},
        // 2**62
        {"4611686018427387904", 0, Pow2(62)},
        // 2**63 + 2**62
        {"13835058055282163712", 0, Pow2(63) + Pow2(62)},
        // 2**64 + 2**62
        {"23058430092136939520", 0, Pow2(64) + Pow2(62)},
        // 10**38 - 2**103
#ifndef __MINGW32__  // MinGW has precision issues
        {"99999989858795198174164788026374356992", 0, Pow10(38) - Pow2(103)},
#endif
        // clang-format on
    };
    for (const ParamType& param : params) {
      CheckDecimalToReal<Real>(param.decimal_value, param.scale, param.expected);
      if (param.decimal_value != "0") {
        CheckDecimalToReal<Real>("-" + param.decimal_value, param.scale, -param.expected);
      }
    }
  }
};

TYPED_TEST_SUITE(TestDecimalToReal, RealTypes);

TYPED_TEST(TestDecimalToReal, TestSuccess) { this->TestSuccess(); }

// Custom test for Decimal128::ToReal<float>
class TestDecimalToRealFloat : public TestDecimalToReal<float> {};

TEST_F(TestDecimalToRealFloat, LargeValues) {
  // Note that exact comparisons would succeed on some platforms (Linux, macOS).
  // Nevertheless, power-of-ten factors are not all exactly representable
  // in binary floating point.
  for (int32_t scale = -38; scale <= 38; scale++) {
    CheckFloatToRealApprox("1", scale, Pow10(-scale));
  }
  for (int32_t scale = -38; scale <= 36; scale++) {
    const Real factor = static_cast<Real>(123);
    CheckFloatToRealApprox("123", scale, factor * Pow10(-scale));
  }
}

TEST_F(TestDecimalToRealFloat, Precision) {
  // 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
  CheckDecimalToReal<float>("9223373136366403584", 0, 9.223373e+18f);
  CheckDecimalToReal<float>("-9223373136366403584", 0, -9.223373e+18f);
  // 2**64 + 2**41 (exactly representable in a float)
  CheckDecimalToReal<float>("18446746272732807168", 0, 1.8446746e+19f);
  CheckDecimalToReal<float>("-18446746272732807168", 0, -1.8446746e+19f);
}

// ToReal<double> tests are disabled on MinGW because of precision issues in results
#ifndef __MINGW32__

// Custom test for Decimal128::ToReal<double>
class TestDecimalToRealDouble : public TestDecimalToReal<double> {};

TEST_F(TestDecimalToRealDouble, LargeValues) {
  // Note that exact comparisons would succeed on some platforms (Linux, macOS).
  // Nevertheless, power-of-ten factors are not all exactly representable
  // in binary floating point.
  for (int32_t scale = -308; scale <= 308; scale++) {
    CheckDoubleToRealApprox("1", scale, Pow10(-scale));
  }
  for (int32_t scale = -308; scale <= 306; scale++) {
    const Real factor = static_cast<Real>(123);
    CheckDoubleToRealApprox("123", scale, factor * Pow10(-scale));
  }
}

TEST_F(TestDecimalToRealDouble, Precision) {
  // 2**63 + 2**11 (exactly representable in a double's 53 bits of precision)
  CheckDecimalToReal<double>("9223372036854777856", 0, 9.223372036854778e+18);
  CheckDecimalToReal<double>("-9223372036854777856", 0, -9.223372036854778e+18);
  // 2**64 - 2**11 (exactly representable in a double)
  CheckDecimalToReal<double>("18446744073709549568", 0, 1.844674407370955e+19);
  CheckDecimalToReal<double>("-18446744073709549568", 0, -1.844674407370955e+19);
  // 2**64 + 2**11 (exactly representable in a double)
  CheckDecimalToReal<double>("18446744073709555712", 0, 1.8446744073709556e+19);
  CheckDecimalToReal<double>("-18446744073709555712", 0, -1.8446744073709556e+19);
  // Almost 10**38 (minus 2**73)
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", 0,
                             9.999999999999998e+37);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", 0,
                             -9.999999999999998e+37);
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", 10,
                             9.999999999999998e+27);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", 10,
                             -9.999999999999998e+27);
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", -10,
                             9.999999999999998e+47);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", -10,
                             -9.999999999999998e+47);
}

#endif  // __MINGW32__

TEST(Decimal128Test, TestSmallNumberFormat) {
  Decimal128 value("0.2");
  std::string expected("0.2");

  const int32_t scale = 1;
  std::string result = value.ToString(scale);
  ASSERT_EQ(expected, result);
}

TEST(Decimal128Test, TestNoDecimalPointExponential) {
  Decimal128 value;
  int32_t precision;
  int32_t scale;
  ASSERT_OK(Decimal128::FromString("1E1", &value, &precision, &scale));
  ASSERT_EQ(10, value.low_bits());
  ASSERT_EQ(2, precision);
  ASSERT_EQ(0, scale);
}

TEST(Decimal128Test, TestFromBigEndian) {
  // We test out a variety of scenarios:
  //
  // * Positive values that are left shifted
  //   and filled in with the same bit pattern
  // * Negated of the positive values
  // * Complement of the positive values
  //
  // For the positive values, we can call FromBigEndian
  // with a length that is less than 16, whereas we must
  // pass all 16 bytes for the negative and complement.
  //
  // We use a number of bit patterns to increase the coverage
  // of scenarios
  for (int32_t start : {1, 15, /* 00001111 */
                        85,    /* 01010101 */
                        127 /* 01111111 */}) {
    Decimal128 value(start);
    for (int ii = 0; ii < 16; ++ii) {
      auto native_endian = value.ToBytes();
#if ARROW_LITTLE_ENDIAN
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      // Limit the number of bytes we are passing to make
      // sure that it works correctly. That's why all of the
      // 'start' values don't have a 1 in the most significant
      // bit place
      ASSERT_OK_AND_EQ(value,
                       Decimal128::FromBigEndian(native_endian.data() + 15 - ii, ii + 1));

      // Negate it
      auto negated = -value;
      native_endian = negated.ToBytes();
#if ARROW_LITTLE_ENDIAN
      // convert to big endian
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      // The sign bit is looked up in the MSB
      ASSERT_OK_AND_EQ(negated,
                       Decimal128::FromBigEndian(native_endian.data() + 15 - ii, ii + 1));

      // Take the complement
      auto complement = ~value;
      native_endian = complement.ToBytes();
#if ARROW_LITTLE_ENDIAN
      // convert to big endian
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      ASSERT_OK_AND_EQ(complement, Decimal128::FromBigEndian(native_endian.data(), 16));

      value <<= 8;
      value += Decimal128(start);
    }
  }
}

TEST(Decimal128Test, TestFromBigEndianBadLength) {
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(0, -1));
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(0, 17));
}

TEST(Decimal128Test, TestToInteger) {
  Decimal128 value1("1234");
  int32_t out1;

  Decimal128 value2("-1234");
  int64_t out2;

  ASSERT_OK(value1.ToInteger(&out1));
  ASSERT_EQ(1234, out1);

  ASSERT_OK(value1.ToInteger(&out2));
  ASSERT_EQ(1234, out2);

  ASSERT_OK(value2.ToInteger(&out1));
  ASSERT_EQ(-1234, out1);

  ASSERT_OK(value2.ToInteger(&out2));
  ASSERT_EQ(-1234, out2);

  Decimal128 invalid_int32(static_cast<int64_t>(std::pow(2, 31)));
  ASSERT_RAISES(Invalid, invalid_int32.ToInteger(&out1));

  Decimal128 invalid_int64("12345678912345678901");
  ASSERT_RAISES(Invalid, invalid_int64.ToInteger(&out2));
}

template <typename ArrowType, typename CType = typename ArrowType::c_type>
std::vector<CType> GetRandomNumbers(int32_t size) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  auto x_array = rand.Numeric<ArrowType>(size, 0, std::numeric_limits<CType>::max(), 0);

  auto x_ptr = x_array->data()->template GetValues<CType>(1);
  std::vector<CType> ret;
  for (int i = 0; i < size; ++i) {
    ret.push_back(x_ptr[i]);
  }
  return ret;
}

TEST(Decimal128Test, Multiply) {
  ASSERT_EQ(Decimal128(60501), Decimal128(301) * Decimal128(201));

  ASSERT_EQ(Decimal128(-60501), Decimal128(-301) * Decimal128(201));

  ASSERT_EQ(Decimal128(-60501), Decimal128(301) * Decimal128(-201));

  ASSERT_EQ(Decimal128(60501), Decimal128(-301) * Decimal128(-201));

  // Test some random numbers.
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      Decimal128 result = Decimal128(x) * Decimal128(y);
      ASSERT_EQ(Decimal128(static_cast<int64_t>(x) * y), result)
          << " x: " << x << " y: " << y;
    }
  }

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y :
         std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 0, 1, 2, 32, INT32_MAX}) {
      Decimal128 result = Decimal128(x.str()) * Decimal128(y.str());
      ASSERT_EQ(Decimal128((x * y).str()), result) << " x: " << x << " y: " << y;
    }
  }
}

TEST(Decimal128Test, Divide) {
  ASSERT_EQ(Decimal128(66), Decimal128(20100) / Decimal128(301));

  ASSERT_EQ(Decimal128(-66), Decimal128(-20100) / Decimal128(301));

  ASSERT_EQ(Decimal128(-66), Decimal128(20100) / Decimal128(-301));

  ASSERT_EQ(Decimal128(66), Decimal128(-20100) / Decimal128(-301));

  // Test some random numbers.
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      if (y == 0) {
        continue;
      }

      Decimal128 result = Decimal128(x) / Decimal128(y);
      ASSERT_EQ(Decimal128(static_cast<int64_t>(x) / y), result)
          << " x: " << x << " y: " << y;
    }
  }

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y : std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 1, 2, 32, INT32_MAX}) {
      Decimal128 result = Decimal128(x.str()) * Decimal128(y.str());
      ASSERT_EQ(Decimal128((x * y).str()), result) << " x: " << x << " y: " << y;
    }
  }
}

TEST(Decimal128Test, Mod) {
  ASSERT_EQ(Decimal128(234), Decimal128(20100) % Decimal128(301));

  ASSERT_EQ(Decimal128(-234), Decimal128(-20100) % Decimal128(301));

  ASSERT_EQ(Decimal128(234), Decimal128(20100) % Decimal128(-301));

  ASSERT_EQ(Decimal128(-234), Decimal128(-20100) % Decimal128(-301));

  // Test some random numbers.
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      if (y == 0) {
        continue;
      }

      Decimal128 result = Decimal128(x) % Decimal128(y);
      ASSERT_EQ(Decimal128(static_cast<int64_t>(x) % y), result)
          << " x: " << x << " y: " << y;
    }
  }

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y : std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 1, 2, 32, INT32_MAX}) {
      Decimal128 result = Decimal128(x.str()) * Decimal128(y.str());
      ASSERT_EQ(Decimal128((x * y).str()), result) << " x: " << x << " y: " << y;
    }
  }
}

TEST(Decimal128Test, Sign) {
  ASSERT_EQ(1, Decimal128(999999).Sign());
  ASSERT_EQ(-1, Decimal128(-999999).Sign());
  ASSERT_EQ(1, Decimal128(0).Sign());
}

TEST(Decimal128Test, GetWholeAndFraction) {
  Decimal128 value("123456");
  Decimal128 whole;
  Decimal128 fraction;
  int32_t out;

  value.GetWholeAndFraction(0, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(123456, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(0, out);

  value.GetWholeAndFraction(1, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(12345, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(6, out);

  value.GetWholeAndFraction(5, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(1, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(23456, out);

  value.GetWholeAndFraction(7, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(0, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(123456, out);
}

TEST(Decimal128Test, GetWholeAndFractionNegative) {
  Decimal128 value("-123456");
  Decimal128 whole;
  Decimal128 fraction;
  int32_t out;

  value.GetWholeAndFraction(0, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-123456, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(0, out);

  value.GetWholeAndFraction(1, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-12345, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-6, out);

  value.GetWholeAndFraction(5, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(-1, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-23456, out);

  value.GetWholeAndFraction(7, &whole, &fraction);
  ASSERT_OK(whole.ToInteger(&out));
  ASSERT_EQ(0, out);
  ASSERT_OK(fraction.ToInteger(&out));
  ASSERT_EQ(-123456, out);
}

TEST(Decimal128Test, IncreaseScale) {
  Decimal128 result;
  int32_t out;

  result = Decimal128("1234").IncreaseScaleBy(0);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(1234, out);

  result = Decimal128("1234").IncreaseScaleBy(3);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(1234000, out);

  result = Decimal128("-1234").IncreaseScaleBy(3);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1234000, out);
}

TEST(Decimal128Test, ReduceScaleAndRound) {
  Decimal128 result;
  int32_t out;

  result = Decimal128("123456").ReduceScaleBy(0);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(123456, out);

  result = Decimal128("123456").ReduceScaleBy(1, false);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12345, out);

  result = Decimal128("123456").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12346, out);

  result = Decimal128("123451").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(12345, out);

  result = Decimal128("-123789").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);

  result = Decimal128("-123749").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1237, out);

  result = Decimal128("-123750").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);
}

TEST(Decimal128Test, FitsInPrecision) {
  ASSERT_TRUE(Decimal128("0").FitsInPrecision(1));
  ASSERT_TRUE(Decimal128("9").FitsInPrecision(1));
  ASSERT_TRUE(Decimal128("-9").FitsInPrecision(1));
  ASSERT_FALSE(Decimal128("10").FitsInPrecision(1));
  ASSERT_FALSE(Decimal128("-10").FitsInPrecision(1));

  ASSERT_TRUE(Decimal128("0").FitsInPrecision(2));
  ASSERT_TRUE(Decimal128("10").FitsInPrecision(2));
  ASSERT_TRUE(Decimal128("-10").FitsInPrecision(2));
  ASSERT_TRUE(Decimal128("99").FitsInPrecision(2));
  ASSERT_TRUE(Decimal128("-99").FitsInPrecision(2));
  ASSERT_FALSE(Decimal128("100").FitsInPrecision(2));
  ASSERT_FALSE(Decimal128("-100").FitsInPrecision(2));

  ASSERT_TRUE(Decimal128("99999999999999999999999999999999999999").FitsInPrecision(38));
  ASSERT_TRUE(Decimal128("-99999999999999999999999999999999999999").FitsInPrecision(38));
  ASSERT_FALSE(Decimal128("100000000000000000000000000000000000000").FitsInPrecision(38));
  ASSERT_FALSE(
      Decimal128("-100000000000000000000000000000000000000").FitsInPrecision(38));
}

}  // namespace arrow

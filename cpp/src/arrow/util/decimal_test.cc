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
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <boost/multiprecision/cpp_int.hpp>

#include "arrow/array.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::checked_cast;
using internal::int128_t;
using internal::uint128_t;

using DecimalTypes = ::testing::Types<Decimal128, Decimal256>;

static const int128_t kInt128Max =
    (static_cast<int128_t>(INT64_MAX) << 64) + static_cast<int128_t>(UINT64_MAX);

template <typename DecimalType>
void AssertDecimalFromString(const std::string& s, const DecimalType& expected,
                             int32_t expected_precision, int32_t expected_scale) {
  ARROW_SCOPED_TRACE("s = '", s, "'");
  DecimalType d;
  int32_t precision, scale;
  ASSERT_OK(DecimalType::FromString(s, &d, &precision, &scale));
  EXPECT_EQ(expected, d);
  EXPECT_EQ(expected_precision, precision);
  EXPECT_EQ(expected_scale, scale);
}

// Assert that the low bits of an array of integers are equal to `expected_low`,
// and that all other bits are equal to `expected_high`.
template <typename T, size_t N, typename U, typename V>
void AssertArrayBits(const std::array<T, N>& a, U expected_low, V expected_high) {
  EXPECT_EQ(a[0], expected_low);
  for (size_t i = 1; i < N; ++i) {
    EXPECT_EQ(a[i], expected_high);
  }
}

Decimal128 Decimal128FromLE(const std::array<uint64_t, 2>& a) {
  return Decimal128(Decimal128::LittleEndianArray, a);
}

Decimal256 Decimal256FromLE(const std::array<uint64_t, 4>& a) {
  return Decimal256(Decimal256::LittleEndianArray, a);
}

Decimal128 Decimal128FromInt128(int128_t value) {
  return Decimal128(static_cast<int64_t>(value >> 64),
                    static_cast<uint64_t>(value & 0xFFFFFFFFFFFFFFFFULL));
}

template <typename DecimalType>
struct DecimalTraits {};

template <>
struct DecimalTraits<Decimal128> {
  using ArrowType = Decimal128Type;
};

template <>
struct DecimalTraits<Decimal256> {
  using ArrowType = Decimal256Type;
};

template <typename DecimalType>
class DecimalFromStringTest : public ::testing::Test {
 public:
  using ArrowType = typename DecimalTraits<DecimalType>::ArrowType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  void TestBasics() { AssertDecimalFromString("234.23445", DecimalType(23423445), 8, 5); }

  void TestStringStartingWithPlus() {
    AssertDecimalFromString("+234.567", DecimalType(234567), 6, 3);
    AssertDecimalFromString("+2342394230592.232349023094",
                            DecimalType("2342394230592232349023094"), 25, 12);
  }

  void TestInvalidInput() {
    for (const std::string invalid_value :
         {"-", "0.0.0", "0-13-32", "a", "-23092.235-", "-+23092.235", "+-23092.235",
          "00a", "1e1a", "0.00123D/3", "1.23eA8", "1.23E+3A", "-1.23E--5",
          "1.2345E+++07"}) {
      ARROW_SCOPED_TRACE("invalid_value = '", invalid_value, "'");
      ASSERT_RAISES(Invalid, Decimal128::FromString(invalid_value));
    }
  }

  void TestLeadingZerosNoDecimalPoint() {
    AssertDecimalFromString("0000000", DecimalType(0), 0, 0);
  }

  void TestLeadingZerosDecimalPoint() {
    AssertDecimalFromString("000.0000", DecimalType(0), 4, 4);
  }

  void TestNoLeadingZerosDecimalPoint() {
    AssertDecimalFromString(".00000", DecimalType(0), 5, 5);
  }

  void TestNoDecimalPointExponent() {
    AssertDecimalFromString("1E1", DecimalType(10), 2, 0);
  }

  void TestWithExponentAndNullptrScale() {
    const DecimalType expected_value(123);
    ASSERT_OK_AND_EQ(expected_value, DecimalType::FromString("1.23E-8"));
  }

  void TestSmallValues() {
    struct TestValue {
      std::string s;
      int64_t expected;
      int32_t expected_precision;
      int32_t expected_scale;
    };
    for (const auto& tv : std::vector<TestValue>{{"12.3", 123LL, 3, 1},
                                                 {"0.00123", 123LL, 5, 5},
                                                 {"1.23E-8", 123LL, 3, 10},
                                                 {"-1.23E-8", -123LL, 3, 10},
                                                 {"1.23E+3", 1230LL, 4, 0},
                                                 {"-1.23E+3", -1230LL, 4, 0},
                                                 {"1.23E+5", 123000LL, 6, 0},
                                                 {"1.2345E+7", 12345000LL, 8, 0},
                                                 {"1.23e-8", 123LL, 3, 10},
                                                 {"-1.23e-8", -123LL, 3, 10},
                                                 {"1.23e+3", 1230LL, 4, 0},
                                                 {"-1.23e+3", -1230LL, 4, 0},
                                                 {"1.23e+5", 123000LL, 6, 0},
                                                 {"1.2345e+7", 12345000LL, 8, 0}}) {
      ARROW_SCOPED_TRACE("s = '", tv.s, "'");
      AssertDecimalFromString(tv.s, DecimalType(tv.expected), tv.expected_precision,
                              tv.expected_scale);
    }
  }

  void CheckRandomValuesRoundTrip(int32_t precision, int32_t scale) {
    auto rnd = random::RandomArrayGenerator(42);
    const auto ty = std::make_shared<ArrowType>(precision, scale);
    const auto array = rnd.ArrayOf(ty, 100, /*null_probability=*/0.0);
    for (int64_t i = 0; i < array->length(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto scalar, array->GetScalar(i));
      const DecimalType& dec_value = checked_cast<const ScalarType&>(*scalar).value;
      const auto s = dec_value.ToString(scale);
      ASSERT_OK_AND_ASSIGN(auto round_tripped, DecimalType::FromString(s));
      ASSERT_EQ(dec_value, round_tripped);
    }
  }

  void TestRandomSmallValuesRoundTrip() {
    for (int32_t scale : {0, 2, 9}) {
      ARROW_SCOPED_TRACE("scale = ", scale);
      CheckRandomValuesRoundTrip(9, scale);
    }
  }

  void TestRandomValuesRoundTrip() {
    const auto max_scale = DecimalType::kMaxScale;
    for (int32_t scale : {0, 3, max_scale / 2, max_scale}) {
      ARROW_SCOPED_TRACE("scale = ", scale);
      CheckRandomValuesRoundTrip(DecimalType::kMaxPrecision, scale);
    }
  }
};

TYPED_TEST_SUITE(DecimalFromStringTest, DecimalTypes);

TYPED_TEST(DecimalFromStringTest, Basics) { this->TestBasics(); }

TYPED_TEST(DecimalFromStringTest, StringStartingWithPlus) {
  this->TestStringStartingWithPlus();
}

TYPED_TEST(DecimalFromStringTest, InvalidInput) { this->TestInvalidInput(); }

TYPED_TEST(DecimalFromStringTest, LeadingZerosDecimalPoint) {
  this->TestLeadingZerosDecimalPoint();
}

TYPED_TEST(DecimalFromStringTest, LeadingZerosNoDecimalPoint) {
  this->TestLeadingZerosNoDecimalPoint();
}

TYPED_TEST(DecimalFromStringTest, NoLeadingZerosDecimalPoint) {
  this->TestNoLeadingZerosDecimalPoint();
}

TYPED_TEST(DecimalFromStringTest, NoDecimalPointExponent) {
  this->TestNoDecimalPointExponent();
}

TYPED_TEST(DecimalFromStringTest, WithExponentAndNullptrScale) {
  this->TestWithExponentAndNullptrScale();
}

TYPED_TEST(DecimalFromStringTest, SmallValues) { this->TestSmallValues(); }

TYPED_TEST(DecimalFromStringTest, RandomSmallValuesRoundTrip) {
  this->TestRandomSmallValuesRoundTrip();
}

TYPED_TEST(DecimalFromStringTest, RandomValuesRoundTrip) {
  this->TestRandomValuesRoundTrip();
}

TEST(Decimal128Test, TestFromStringDecimal128) {
  std::string string_value("-23049223942343532412");
  Decimal128 result(string_value);
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(Decimal128Test, TestFromDecimalString128) {
  std::string string_value("-23049223942343.532412");
  Decimal128 result;
  ASSERT_OK_AND_ASSIGN(result, Decimal128::FromString(string_value));
  Decimal128 expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);

  // Sanity check that our number is actually using more than 64 bits
  ASSERT_NE(result.high_bits(), 0);
}

TEST(Decimal128Test, TestStringRoundTrip) {
  static constexpr uint64_t kTestBits[] = {
      0,
      1,
      999,
      1000,
      std::numeric_limits<int32_t>::max(),
      (1ull << 31),
      std::numeric_limits<uint32_t>::max(),
      (1ull << 32),
      std::numeric_limits<int64_t>::max(),
      (1ull << 63),
      std::numeric_limits<uint64_t>::max(),
  };
  static constexpr int32_t kScales[] = {0, 1, 10};
  for (uint64_t high_bits : kTestBits) {
    for (uint64_t low_bits : kTestBits) {
      // When high_bits = 1ull << 63 or std::numeric_limits<uint64_t>::max(), decimal is
      // negative.
      Decimal128 decimal(high_bits, low_bits);
      for (int32_t scale : kScales) {
        std::string str = decimal.ToString(scale);
        ASSERT_OK_AND_ASSIGN(Decimal128 result, Decimal128::FromString(str));
        EXPECT_EQ(decimal, result);
      }
    }
  }
}

TEST(Decimal128Test, TestDecimal32SignedRoundTrip) {
  Decimal128 expected("-3402692");

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());
  ASSERT_EQ(expected, result);
}

TEST(Decimal128Test, TestDecimal64SignedRoundTrip) {
  Decimal128 expected;
  std::string string_value("-34034293045.921");
  ASSERT_OK_AND_ASSIGN(expected, Decimal128::FromString(string_value));

  auto bytes = expected.ToBytes();
  Decimal128 result(bytes.data());

  ASSERT_EQ(expected, result);
}

TEST(Decimal128Test, TestDecimalStringAndBytesRoundTrip) {
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

/*
  Note: generating a number of 64-bit decimal digits from a bigint:

  >>> def dec(x, n):
  ...:     sign = x < 0
  ...:     if sign:
  ...:         x = 2**(64*n) + x
  ...:     a = []
  ...:     for i in range(n-1):
  ...:         x, r = divmod(x, 2**64)
  ...:         a.append(r)
  ...:     assert x < 2**64
  ...:     a.append(x)
  ...:     return a
  ...:
  >>> dec(10**37, 2)
  [68739955140067328, 542101086242752217]
  >>> dec(-10**37, 2)
  [18378004118569484288, 17904642987466799398]
  >>> dec(10**75, 4)
  [0, 10084168908774762496, 12965995782233477362, 159309191113245227]
  >>> dec(-10**75, 4)
  [0, 8362575164934789120, 5480748291476074253, 18287434882596306388]
*/

TEST(Decimal128Test, FromStringLimits) {
  // Positive / zero exponent
  AssertDecimalFromString(
      "1e37", Decimal128FromLE({68739955140067328ULL, 542101086242752217ULL}), 38, 0);
  AssertDecimalFromString(
      "-1e37", Decimal128FromLE({18378004118569484288ULL, 17904642987466799398ULL}), 38,
      0);
  AssertDecimalFromString(
      "9.87e37", Decimal128FromLE({15251391175463010304ULL, 5350537721215964381ULL}), 38,
      0);
  AssertDecimalFromString(
      "-9.87e37", Decimal128FromLE({3195352898246541312ULL, 13096206352493587234ULL}), 38,
      0);
  AssertDecimalFromString(
      "12345678901234567890123456789012345678",
      Decimal128FromLE({14143994781733811022ULL, 669260594276348691ULL}), 38, 0);
  AssertDecimalFromString(
      "-12345678901234567890123456789012345678",
      Decimal128FromLE({4302749291975740594ULL, 17777483479433202924ULL}), 38, 0);

  // "9..9" (38 times)
  const auto dec38times9pos =
      Decimal128FromLE({687399551400673279ULL, 5421010862427522170ULL});
  // "-9..9" (38 times)
  const auto dec38times9neg =
      Decimal128FromLE({17759344522308878337ULL, 13025733211282029445ULL});

  AssertDecimalFromString("99999999999999999999999999999999999999", dec38times9pos, 38,
                          0);
  AssertDecimalFromString("-99999999999999999999999999999999999999", dec38times9neg, 38,
                          0);
  AssertDecimalFromString("9.9999999999999999999999999999999999999e37", dec38times9pos,
                          38, 0);
  AssertDecimalFromString("-9.9999999999999999999999999999999999999e37", dec38times9neg,
                          38, 0);

  // Positive / zero exponent, precision too large for a non-negative scale
  ASSERT_RAISES(Invalid, Decimal128::FromString("1e39"));
  ASSERT_RAISES(Invalid, Decimal128::FromString("-1e39"));
  ASSERT_RAISES(Invalid, Decimal128::FromString("9e39"));
  ASSERT_RAISES(Invalid, Decimal128::FromString("-9e39"));
  ASSERT_RAISES(Invalid, Decimal128::FromString("9.9e40"));
  ASSERT_RAISES(Invalid, Decimal128::FromString("-9.9e40"));
  // XXX conversion overflows are currently not detected
  //   ASSERT_RAISES(Invalid, Decimal128::FromString("99e38"));
  //   ASSERT_RAISES(Invalid, Decimal128::FromString("-99e38"));
  //   ASSERT_RAISES(Invalid,
  //   Decimal128::FromString("999999999999999999999999999999999999999e1"));
  //   ASSERT_RAISES(Invalid,
  //   Decimal128::FromString("-999999999999999999999999999999999999999e1"));
  //   ASSERT_RAISES(Invalid,
  //   Decimal128::FromString("999999999999999999999999999999999999999"));

  // No exponent, many fractional digits
  AssertDecimalFromString("9.9999999999999999999999999999999999999", dec38times9pos, 38,
                          37);
  AssertDecimalFromString("-9.9999999999999999999999999999999999999", dec38times9neg, 38,
                          37);
  AssertDecimalFromString("0.99999999999999999999999999999999999999", dec38times9pos, 38,
                          38);
  AssertDecimalFromString("-0.99999999999999999999999999999999999999", dec38times9neg, 38,
                          38);

  // Negative exponent
  AssertDecimalFromString("1e-38", Decimal128FromLE({1, 0}), 1, 38);
  AssertDecimalFromString(
      "-1e-38", Decimal128FromLE({18446744073709551615ULL, 18446744073709551615ULL}), 1,
      38);
  AssertDecimalFromString("9.99e-36", Decimal128FromLE({999, 0}), 3, 38);
  AssertDecimalFromString(
      "-9.99e-36", Decimal128FromLE({18446744073709550617ULL, 18446744073709551615ULL}),
      3, 38);
  AssertDecimalFromString("987e-38", Decimal128FromLE({987, 0}), 3, 38);
  AssertDecimalFromString(
      "-987e-38", Decimal128FromLE({18446744073709550629ULL, 18446744073709551615ULL}), 3,
      38);
  AssertDecimalFromString("99999999999999999999999999999999999999e-37", dec38times9pos,
                          38, 37);
  AssertDecimalFromString("-99999999999999999999999999999999999999e-37", dec38times9neg,
                          38, 37);
  AssertDecimalFromString("99999999999999999999999999999999999999e-38", dec38times9pos,
                          38, 38);
  AssertDecimalFromString("-99999999999999999999999999999999999999e-38", dec38times9neg,
                          38, 38);
}

TEST(Decimal256Test, FromStringLimits) {
  // Positive / zero exponent
  AssertDecimalFromString(
      "1e75",
      Decimal256FromLE(
          {0, 10084168908774762496ULL, 12965995782233477362ULL, 159309191113245227ULL}),
      76, 0);
  AssertDecimalFromString(
      "-1e75",
      Decimal256FromLE(
          {0, 8362575164934789120ULL, 5480748291476074253ULL, 18287434882596306388ULL}),
      76, 0);
  AssertDecimalFromString(
      "9.87e75",
      Decimal256FromLE(
          {0, 3238743064843046400ULL, 7886074450795240548ULL, 1572381716287730397ULL}),
      76, 0);
  AssertDecimalFromString(
      "-9.87e75",
      Decimal256FromLE(
          {0, 15208001008866505216ULL, 10560669622914311067ULL, 16874362357421821218ULL}),
      76, 0);

  AssertDecimalFromString(
      "1234567890123456789012345678901234567890123456789012345678901234567890123456",
      Decimal256FromLE({17877984925544397504ULL, 5352188884907840935ULL,
                        234631617561833724ULL, 196678011949953713ULL}),
      76, 0);
  AssertDecimalFromString(
      "-1234567890123456789012345678901234567890123456789012345678901234567890123456",
      Decimal256FromLE({568759148165154112ULL, 13094555188801710680ULL,
                        18212112456147717891ULL, 18250066061759597902ULL}),
      76, 0);

  // "9..9" (76 times)
  const auto dec76times9pos =
      Decimal256FromLE({18446744073709551615ULL, 8607968719199866879ULL,
                        532749306367912313ULL, 1593091911132452277ULL});
  // "-9..9" (76 times)
  const auto dec76times9neg = Decimal256FromLE(
      {1, 9838775354509684736ULL, 17913994767341639302ULL, 16853652162577099338ULL});

  AssertDecimalFromString(
      "9999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9pos, 76, 0);
  AssertDecimalFromString(
      "-9999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9neg, 76, 0);
  AssertDecimalFromString(
      "9.999999999999999999999999999999999999999999999999999999999999999999999999999e75",
      dec76times9pos, 76, 0);
  AssertDecimalFromString(
      "-9.999999999999999999999999999999999999999999999999999999999999999999999999999e75",
      dec76times9neg, 76, 0);

  // Positive / zero exponent, precision too large for a non-negative scale
  ASSERT_RAISES(Invalid, Decimal256::FromString("1e77"));
  ASSERT_RAISES(Invalid, Decimal256::FromString("-1e77"));
  ASSERT_RAISES(Invalid, Decimal256::FromString("9e77"));
  ASSERT_RAISES(Invalid, Decimal256::FromString("-9e77"));
  ASSERT_RAISES(Invalid, Decimal256::FromString("9.9e78"));
  ASSERT_RAISES(Invalid, Decimal256::FromString("-9.9e78"));

  // XXX conversion overflows are currently not detected
  //   ASSERT_RAISES(Invalid, Decimal256::FromString("99e76"));
  //   ASSERT_RAISES(Invalid, Decimal256::FromString("-99e76"));
  //   ASSERT_RAISES(Invalid,
  //     Decimal256::FromString("9999999999999999999999999999999999999999999999999999999999999999999999999999e1"));
  //   ASSERT_RAISES(Invalid,
  //     Decimal256::FromString("-9999999999999999999999999999999999999999999999999999999999999999999999999999e1"));
  //   ASSERT_RAISES(Invalid,
  //     Decimal256::FromString("99999999999999999999999999999999999999999999999999999999999999999999999999999"));

  // No exponent, many fractional digits
  AssertDecimalFromString(
      "9.999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9pos, 76, 75);
  AssertDecimalFromString(
      "-9.999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9neg, 76, 75);
  AssertDecimalFromString(
      "0.9999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9pos, 76, 76);
  AssertDecimalFromString(
      "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999",
      dec76times9neg, 76, 76);

  // Negative exponent
  AssertDecimalFromString("1e-76", Decimal256FromLE({1, 0, 0, 0}), 1, 76);
  AssertDecimalFromString(
      "-1e-76",
      Decimal256FromLE({18446744073709551615ULL, 18446744073709551615ULL,
                        18446744073709551615ULL, 18446744073709551615ULL}),
      1, 76);
  AssertDecimalFromString("9.99e-74", Decimal256FromLE({999, 0, 0, 0}), 3, 76);
  AssertDecimalFromString(
      "-9.99e-74",
      Decimal256FromLE({18446744073709550617ULL, 18446744073709551615ULL,
                        18446744073709551615ULL, 18446744073709551615ULL}),
      3, 76);
  AssertDecimalFromString("987e-76", Decimal256FromLE({987, 0, 0, 0}), 3, 76);
  AssertDecimalFromString(
      "-987e-76",
      Decimal256FromLE({18446744073709550629ULL, 18446744073709551615ULL,
                        18446744073709551615ULL, 18446744073709551615ULL}),
      3, 76);
  AssertDecimalFromString(
      "9999999999999999999999999999999999999999999999999999999999999999999999999999e-75",
      dec76times9pos, 76, 75);
  AssertDecimalFromString(
      "-9999999999999999999999999999999999999999999999999999999999999999999999999999e-75",
      dec76times9neg, 76, 75);
  AssertDecimalFromString(
      "9999999999999999999999999999999999999999999999999999999999999999999999999999e-76",
      dec76times9pos, 76, 76);
  AssertDecimalFromString(
      "-9999999999999999999999999999999999999999999999999999999999999999999999999999e-76",
      dec76times9neg, 76, 76);
}

template <typename DecimalType>
class DecimalFromIntegerTest : public ::testing::Test {
 public:
  template <typename IntegerType>
  void CheckConstructFrom() {
    DecimalType value(IntegerType{42});
    AssertArrayBits(value.little_endian_array(), 42, 0);

    DecimalType max_value(std::numeric_limits<IntegerType>::max());
    AssertArrayBits(max_value.little_endian_array(),
                    std::numeric_limits<IntegerType>::max(), 0);

    DecimalType min_value(std::numeric_limits<IntegerType>::min());
    AssertArrayBits(min_value.little_endian_array(),
                    std::numeric_limits<IntegerType>::min(),
                    (std::is_signed<IntegerType>::value ? -1 : 0));
  }

  void TestConstructibleFromAnyIntegerType() {
    CheckConstructFrom<char>();                // NOLINT
    CheckConstructFrom<signed char>();         // NOLINT
    CheckConstructFrom<unsigned char>();       // NOLINT
    CheckConstructFrom<short>();               // NOLINT
    CheckConstructFrom<unsigned short>();      // NOLINT
    CheckConstructFrom<int>();                 // NOLINT
    CheckConstructFrom<unsigned int>();        // NOLINT
    CheckConstructFrom<long>();                // NOLINT
    CheckConstructFrom<unsigned long>();       // NOLINT
    CheckConstructFrom<long long>();           // NOLINT
    CheckConstructFrom<unsigned long long>();  // NOLINT
  }

  void TestConstructibleFromBool() {
    {
      DecimalType value(true);
      AssertArrayBits(value.little_endian_array(), 1, 0);
    }
    {
      DecimalType value(false);
      AssertArrayBits(value.little_endian_array(), 0, 0);
    }
  }

  void TestNumericLimits() {
    TestNumericLimit<Int8Type>();
    TestNumericLimit<UInt8Type>();
    TestNumericLimit<Int16Type>();
    TestNumericLimit<UInt16Type>();
    TestNumericLimit<Int32Type>();
    TestNumericLimit<UInt32Type>();
    TestNumericLimit<Int64Type>();
    TestNumericLimit<UInt64Type>();
  }

  template <typename ArrowType>
  void TestNumericLimit() {
    using c_type = typename ArrowType::c_type;
    ASSERT_OK_AND_ASSIGN(const int32_t precision,
                         MaxDecimalDigitsForInteger(ArrowType::type_id));
    DecimalType min_value(std::numeric_limits<c_type>::min());
    ASSERT_TRUE(min_value.FitsInPrecision(precision));
    DecimalType max_value(std::numeric_limits<c_type>::max());
    ASSERT_TRUE(max_value.FitsInPrecision(precision));
  }
};

TYPED_TEST_SUITE(DecimalFromIntegerTest, DecimalTypes);

TYPED_TEST(DecimalFromIntegerTest, ConstructibleFromAnyIntegerType) {
  this->TestConstructibleFromAnyIntegerType();
}

TYPED_TEST(DecimalFromIntegerTest, ConstructibleFromBool) {
  this->TestConstructibleFromBool();
}

TYPED_TEST(DecimalFromIntegerTest, TestNumericLimits) { this->TestNumericLimits(); }

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

struct ToStringTestParam {
  int64_t test_value;
  int32_t scale;
  std::string expected_string;

  // Avoid Valgrind uninitialized memory reads with the default GTest print routine.
  friend std::ostream& operator<<(std::ostream& os, const ToStringTestParam& param) {
    return os << "<value: " << param.test_value << ">";
  }
};

static const ToStringTestParam kToStringTestData[] = {
    {0, -1, "0.E+1"},
    {0, 0, "0"},
    {0, 1, "0.0"},
    {0, 6, "0.000000"},
    {2, 7, "2.E-7"},
    {2, -1, "2.E+1"},
    {2, 0, "2"},
    {2, 1, "0.2"},
    {2, 6, "0.000002"},
    {-2, 7, "-2.E-7"},
    {-2, 7, "-2.E-7"},
    {-2, -1, "-2.E+1"},
    {-2, 0, "-2"},
    {-2, 1, "-0.2"},
    {-2, 6, "-0.000002"},
    {-2, 7, "-2.E-7"},
    {123, -3, "1.23E+5"},
    {123, -1, "1.23E+3"},
    {123, 1, "12.3"},
    {123, 0, "123"},
    {123, 5, "0.00123"},
    {123, 8, "0.00000123"},
    {123, 9, "1.23E-7"},
    {123, 10, "1.23E-8"},
    {-123, -3, "-1.23E+5"},
    {-123, -1, "-1.23E+3"},
    {-123, 1, "-12.3"},
    {-123, 0, "-123"},
    {-123, 5, "-0.00123"},
    {-123, 8, "-0.00000123"},
    {-123, 9, "-1.23E-7"},
    {-123, 10, "-1.23E-8"},
    {1000000000, -3, "1.000000000E+12"},
    {1000000000, -1, "1.000000000E+10"},
    {1000000000, 0, "1000000000"},
    {1000000000, 1, "100000000.0"},
    {1000000000, 5, "10000.00000"},
    {1000000000, 15, "0.000001000000000"},
    {1000000000, 16, "1.000000000E-7"},
    {1000000000, 17, "1.000000000E-8"},
    {-1000000000, -3, "-1.000000000E+12"},
    {-1000000000, -1, "-1.000000000E+10"},
    {-1000000000, 0, "-1000000000"},
    {-1000000000, 1, "-100000000.0"},
    {-1000000000, 5, "-10000.00000"},
    {-1000000000, 15, "-0.000001000000000"},
    {-1000000000, 16, "-1.000000000E-7"},
    {-1000000000, 17, "-1.000000000E-8"},
    {1234567890123456789LL, -3, "1.234567890123456789E+21"},
    {1234567890123456789LL, -1, "1.234567890123456789E+19"},
    {1234567890123456789LL, 0, "1234567890123456789"},
    {1234567890123456789LL, 1, "123456789012345678.9"},
    {1234567890123456789LL, 5, "12345678901234.56789"},
    {1234567890123456789LL, 24, "0.000001234567890123456789"},
    {1234567890123456789LL, 25, "1.234567890123456789E-7"},
    {-1234567890123456789LL, -3, "-1.234567890123456789E+21"},
    {-1234567890123456789LL, -1, "-1.234567890123456789E+19"},
    {-1234567890123456789LL, 0, "-1234567890123456789"},
    {-1234567890123456789LL, 1, "-123456789012345678.9"},
    {-1234567890123456789LL, 5, "-12345678901234.56789"},
    {-1234567890123456789LL, 24, "-0.000001234567890123456789"},
    {-1234567890123456789LL, 25, "-1.234567890123456789E-7"},
};

class Decimal128ToStringTest : public ::testing::TestWithParam<ToStringTestParam> {};

TEST_P(Decimal128ToStringTest, ToString) {
  const ToStringTestParam& param = GetParam();
  const Decimal128 value(param.test_value);
  const std::string printed_value = value.ToString(param.scale);
  ASSERT_EQ(param.expected_string, printed_value);
}

INSTANTIATE_TEST_SUITE_P(Decimal128ToStringTest, Decimal128ToStringTest,
                         ::testing::ValuesIn(kToStringTestData));

template <typename Decimal, typename Real>
void CheckDecimalFromReal(Real real, int32_t precision, int32_t scale,
                          const std::string& expected) {
  ASSERT_OK_AND_ASSIGN(auto dec, Decimal::FromReal(real, precision, scale));
  ASSERT_EQ(dec.ToString(scale), expected);
}

template <typename Decimal, typename Real>
void CheckDecimalFromRealIntegerString(Real real, int32_t precision, int32_t scale,
                                       const std::string& expected) {
  ASSERT_OK_AND_ASSIGN(auto dec, Decimal::FromReal(real, precision, scale));
  ASSERT_EQ(dec.ToIntegerString(), expected);
}

template <typename Real>
struct FromRealTestParam {
  Real real;
  int32_t precision;
  int32_t scale;
  std::string expected;

  // Avoid Valgrind uninitialized memory reads with the default GTest print routine.
  friend std::ostream& operator<<(std::ostream& os,
                                  const FromRealTestParam<Real>& param) {
    return os << "<real: " << param.real << ">";
  }
};

using FromFloatTestParam = FromRealTestParam<float>;
using FromDoubleTestParam = FromRealTestParam<double>;

// Common tests for Decimal128::FromReal(T, ...) and Decimal256::FromReal(T, ...)
template <typename T>
class TestDecimalFromReal : public ::testing::Test {
 public:
  using Decimal = typename T::first_type;
  using Real = typename T::second_type;
  using ParamType = FromRealTestParam<Real>;

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
        {4.6116860184273879e+18, 19, 0, "4611686018427387904"},
        {-4.6116860184273879e+18, 19, 0, "-4611686018427387904"},
        // 2**63
        {9.2233720368547758e+18, 19, 0, "9223372036854775808"},
        {-9.2233720368547758e+18, 19, 0, "-9223372036854775808"},
        // 2**64
        {1.8446744073709552e+19, 20, 0, "18446744073709551616"},
        {-1.8446744073709552e+19, 20, 0, "-18446744073709551616"}
        // clang-format on
    };
    for (const ParamType& param : params) {
      CheckDecimalFromReal<Decimal>(param.real, param.precision, param.scale,
                                    param.expected);
    }
  }

  void TestErrors() {
    ASSERT_RAISES(Invalid, Decimal::FromReal(INFINITY, 19, 4));
    ASSERT_RAISES(Invalid, Decimal::FromReal(-INFINITY, 19, 4));
    ASSERT_RAISES(Invalid, Decimal::FromReal(NAN, 19, 4));
    // Overflows
    ASSERT_RAISES(Invalid, Decimal::FromReal(1000.0, 3, 0));
    ASSERT_RAISES(Invalid, Decimal::FromReal(-1000.0, 3, 0));
    ASSERT_RAISES(Invalid, Decimal::FromReal(1000.0, 5, 2));
    ASSERT_RAISES(Invalid, Decimal::FromReal(-1000.0, 5, 2));
    ASSERT_RAISES(Invalid, Decimal::FromReal(999.996, 5, 2));
    ASSERT_RAISES(Invalid, Decimal::FromReal(-999.996, 5, 2));
    ASSERT_RAISES(Invalid, Decimal::FromReal(1e+38, 38, 0));
    ASSERT_RAISES(Invalid, Decimal::FromReal(-1e+38, 38, 0));
  }
};

using RealTypes =
    ::testing::Types<std::pair<Decimal128, float>, std::pair<Decimal128, double>,
                     std::pair<Decimal256, float>, std::pair<Decimal256, double>>;
TYPED_TEST_SUITE(TestDecimalFromReal, RealTypes);

TYPED_TEST(TestDecimalFromReal, TestSuccess) { this->TestSuccess(); }

TYPED_TEST(TestDecimalFromReal, TestErrors) { this->TestErrors(); }

// Tests for Decimal128::FromReal(float, ...) and Decimal256::FromReal(float, ...)
template <typename T>
class TestDecimalFromRealFloat : public ::testing::Test {
 protected:
  std::vector<FromFloatTestParam> GetValues() {
    return {// 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
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
                               "-99999986661652122824821048795547566080"}};
  }
};
TYPED_TEST_SUITE(TestDecimalFromRealFloat, DecimalTypes);

TYPED_TEST(TestDecimalFromRealFloat, SuccessConversion) {
  for (const auto& param : this->GetValues()) {
    CheckDecimalFromReal<TypeParam>(param.real, param.precision, param.scale,
                                    param.expected);
  }
}

TYPED_TEST(TestDecimalFromRealFloat, LargeValues) {
  // Test the entire float range
  for (int32_t scale = -38; scale <= 38; ++scale) {
    float real = std::pow(10.0f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString<TypeParam>(real, 1, -scale, "1");
  }
  for (int32_t scale = -37; scale <= 36; ++scale) {
    float real = 123.f * std::pow(10.f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString<TypeParam>(real, 2, -scale - 1, "12");
    CheckDecimalFromRealIntegerString<TypeParam>(real, 3, -scale, "123");
    CheckDecimalFromRealIntegerString<TypeParam>(real, 4, -scale + 1, "1230");
  }
}

// Tests for Decimal128::FromReal(double, ...) and Decimal256::FromReal(double, ...)
template <typename T>
class TestDecimalFromRealDouble : public ::testing::Test {
 protected:
  std::vector<FromDoubleTestParam> GetValues() {
    return {// 2**63 + 2**11 (exactly representable in a double's 53 bits of precision)
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
                                "-9999999999999997885934389197.7453174784"}};
  }
};
TYPED_TEST_SUITE(TestDecimalFromRealDouble, DecimalTypes);

TYPED_TEST(TestDecimalFromRealDouble, SuccessConversion) {
  for (const auto& param : this->GetValues()) {
    CheckDecimalFromReal<TypeParam>(param.real, param.precision, param.scale,
                                    param.expected);
  }
}

TYPED_TEST(TestDecimalFromRealDouble, LargeValues) {
  // Test the entire double range
  for (int32_t scale = -308; scale <= 308; ++scale) {
    double real = std::pow(10.0, static_cast<double>(scale));
    CheckDecimalFromRealIntegerString<TypeParam>(real, 1, -scale, "1");
  }
  for (int32_t scale = -307; scale <= 306; ++scale) {
    double real = 123. * std::pow(10.0, static_cast<double>(scale));
    CheckDecimalFromRealIntegerString<TypeParam>(real, 2, -scale - 1, "12");
    CheckDecimalFromRealIntegerString<TypeParam>(real, 3, -scale, "123");
    CheckDecimalFromRealIntegerString<TypeParam>(real, 4, -scale + 1, "1230");
  }
}

// Additional values that only apply to Decimal256
TEST(TestDecimal256FromRealDouble, ExtremeValues) {
  const std::vector<FromDoubleTestParam> values = {
      // Almost 10**76
      FromDoubleTestParam{9.999999999999999e+75, 76, 0,
                          "999999999999999886366330070006442034959750906670402"
                          "8242075715752105414230016"},
      FromDoubleTestParam{-9.999999999999999e+75, 76, 0,
                          "-999999999999999886366330070006442034959750906670402"
                          "8242075715752105414230016"},
      FromDoubleTestParam{9.999999999999999e+65, 76, 10,
                          "999999999999999886366330070006442034959750906670402"
                          "824207571575210.5414230016"},
      FromDoubleTestParam{-9.999999999999999e+65, 76, 10,
                          "-999999999999999886366330070006442034959750906670402"
                          "824207571575210.5414230016"}};
  for (const auto& param : values) {
    CheckDecimalFromReal<Decimal256>(param.real, param.precision, param.scale,
                                     param.expected);
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

template <typename Decimal, typename Real>
void CheckDecimalToReal(const std::string& decimal_value, int32_t scale, Real expected) {
  Decimal dec(decimal_value);
  ASSERT_EQ(dec.template ToReal<Real>(scale), expected)
      << "Decimal value: " << decimal_value << " Scale: " << scale;
}

template <typename Decimal>
void CheckDecimalToRealApprox(const std::string& decimal_value, int32_t scale,
                              float expected) {
  Decimal dec(decimal_value);
  ASSERT_FLOAT_EQ(dec.template ToReal<float>(scale), expected)
      << "Decimal value: " << decimal_value << " Scale: " << scale;
}

template <typename Decimal>
void CheckDecimalToRealApprox(const std::string& decimal_value, int32_t scale,
                              double expected) {
  Decimal dec(decimal_value);
  ASSERT_DOUBLE_EQ(dec.template ToReal<double>(scale), expected)
      << "Decimal value: " << decimal_value << " Scale: " << scale;
}

// Common tests for Decimal128::ToReal<T> and Decimal256::ToReal<T>
template <typename T>
class TestDecimalToReal : public ::testing::Test {
 public:
  using Decimal = typename T::first_type;
  using Real = typename T::second_type;
  using ParamType = ToRealTestParam<Real>;

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
      CheckDecimalToReal<Decimal, Real>(param.decimal_value, param.scale, param.expected);
      if (param.decimal_value != "0") {
        CheckDecimalToReal<Decimal, Real>("-" + param.decimal_value, param.scale,
                                          -param.expected);
      }
    }
  }

  // Test precision of conversions to float values
  void TestPrecision() {
    // 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
    CheckDecimalToReal<Decimal, Real>("9223373136366403584", 0, 9.223373e+18f);
    CheckDecimalToReal<Decimal, Real>("-9223373136366403584", 0, -9.223373e+18f);
    // 2**64 + 2**41 (exactly representable in a float)
    CheckDecimalToReal<Decimal, Real>("18446746272732807168", 0, 1.8446746e+19f);
    CheckDecimalToReal<Decimal, Real>("-18446746272732807168", 0, -1.8446746e+19f);
  }

  // Test conversions with a range of scales
  void TestLargeValues(int32_t max_scale) {
    // Note that exact comparisons would succeed on some platforms (Linux, macOS).
    // Nevertheless, power-of-ten factors are not all exactly representable
    // in binary floating point.
    for (int32_t scale = -max_scale; scale <= max_scale; scale++) {
#ifdef _WIN32
      // MSVC gives pow(10.f, -45.f) == 0 even though 1e-45f is nonzero
      if (scale == 45) continue;
#endif
      CheckDecimalToRealApprox<Decimal>("1", scale, Pow10(-scale));
    }
    for (int32_t scale = -max_scale; scale <= max_scale - 2; scale++) {
#ifdef _WIN32
      // MSVC gives pow(10.f, -45.f) == 0 even though 1e-45f is nonzero
      if (scale == 45) continue;
#endif
      const Real factor = static_cast<Real>(123);
      CheckDecimalToRealApprox<Decimal>("123", scale, factor * Pow10(-scale));
    }
  }
};

TYPED_TEST_SUITE(TestDecimalToReal, RealTypes);

TYPED_TEST(TestDecimalToReal, TestSuccess) { this->TestSuccess(); }

// Custom test for Decimal128::ToReal<float>
class TestDecimal128ToRealFloat : public TestDecimalToReal<std::pair<Decimal128, float>> {
};
TEST_F(TestDecimal128ToRealFloat, LargeValues) { TestLargeValues(/*max_scale=*/38); }
TEST_F(TestDecimal128ToRealFloat, Precision) { this->TestPrecision(); }
// Custom test for Decimal256::ToReal<float>
class TestDecimal256ToRealFloat : public TestDecimalToReal<std::pair<Decimal256, float>> {
};
TEST_F(TestDecimal256ToRealFloat, LargeValues) { TestLargeValues(/*max_scale=*/76); }
TEST_F(TestDecimal256ToRealFloat, Precision) { this->TestPrecision(); }

// ToReal<double> tests are disabled on MinGW because of precision issues in results
#ifndef __MINGW32__

// Custom test for Decimal128::ToReal<double>
template <typename DecimalType>
class TestDecimalToRealDouble : public TestDecimalToReal<std::pair<DecimalType, double>> {
};
TYPED_TEST_SUITE(TestDecimalToRealDouble, DecimalTypes);

TYPED_TEST(TestDecimalToRealDouble, LargeValues) {
  // Note that exact comparisons would succeed on some platforms (Linux, macOS).
  // Nevertheless, power-of-ten factors are not all exactly representable
  // in binary floating point.
  for (int32_t scale = -308; scale <= 308; scale++) {
    CheckDecimalToRealApprox<TypeParam>("1", scale, this->Pow10(-scale));
  }
  for (int32_t scale = -308; scale <= 306; scale++) {
    const double factor = 123.;
    CheckDecimalToRealApprox<TypeParam>("123", scale, factor * this->Pow10(-scale));
  }
}

TYPED_TEST(TestDecimalToRealDouble, Precision) {
  // 2**63 + 2**11 (exactly representable in a double's 53 bits of precision)
  CheckDecimalToReal<TypeParam, double>("9223372036854777856", 0, 9.223372036854778e+18);
  CheckDecimalToReal<TypeParam, double>("-9223372036854777856", 0,
                                        -9.223372036854778e+18);
  // 2**64 - 2**11 (exactly representable in a double)
  CheckDecimalToReal<TypeParam, double>("18446744073709549568", 0, 1.844674407370955e+19);
  CheckDecimalToReal<TypeParam, double>("-18446744073709549568", 0,
                                        -1.844674407370955e+19);
  // 2**64 + 2**11 (exactly representable in a double)
  CheckDecimalToReal<TypeParam, double>("18446744073709555712", 0,
                                        1.8446744073709556e+19);
  CheckDecimalToReal<TypeParam, double>("-18446744073709555712", 0,
                                        -1.8446744073709556e+19);
  // Almost 10**38 (minus 2**73)
  CheckDecimalToReal<TypeParam, double>("99999999999999978859343891977453174784", 0,
                                        9.999999999999998e+37);
  CheckDecimalToReal<TypeParam, double>("-99999999999999978859343891977453174784", 0,
                                        -9.999999999999998e+37);
  CheckDecimalToReal<TypeParam, double>("99999999999999978859343891977453174784", 10,
                                        9.999999999999998e+27);
  CheckDecimalToReal<TypeParam, double>("-99999999999999978859343891977453174784", 10,
                                        -9.999999999999998e+27);
  CheckDecimalToReal<TypeParam, double>("99999999999999978859343891977453174784", -10,
                                        9.999999999999998e+47);
  CheckDecimalToReal<TypeParam, double>("-99999999999999978859343891977453174784", -10,
                                        -9.999999999999998e+47);
}

#endif  // __MINGW32__

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
  auto x_array = rand.Numeric<ArrowType>(size, static_cast<CType>(0),
                                         std::numeric_limits<CType>::max(), 0);

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
      // Test by multiplying with an additional 32 bit factor, then additional
      // factor of 2^30 to test results in the range of -2^123 to 2^123 without overflow.
      for (auto z : GetRandomNumbers<Int32Type>(32)) {
        int128_t w = static_cast<int128_t>(x) * y * (1ull << 30);
        Decimal128 expected = Decimal128FromInt128(static_cast<int128_t>(w) * z);
        Decimal128 actual = Decimal128FromInt128(w) * Decimal128(z);
        ASSERT_EQ(expected, actual) << " w: " << x << " * " << y << " * 2^30 z: " << z;
      }
    }
  }

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y :
         std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 0, 1, 2, 32, INT32_MAX}) {
      Decimal128 decimal_x = Decimal128FromInt128(x);
      Decimal128 decimal_y = Decimal128FromInt128(y);
      Decimal128 result = decimal_x * decimal_y;
      EXPECT_EQ(Decimal128FromInt128(x * y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
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
      Decimal128 decimal_x = Decimal128FromInt128(x);
      Decimal128 decimal_y = Decimal128FromInt128(y);
      Decimal128 result = decimal_x / decimal_y;
      EXPECT_EQ(Decimal128FromInt128(x / y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(Decimal128Test, Rescale) {
  ASSERT_OK_AND_EQ(Decimal128(11100), Decimal128(111).Rescale(0, 2));
  ASSERT_OK_AND_EQ(Decimal128(111), Decimal128(11100).Rescale(2, 0));
  ASSERT_OK_AND_EQ(Decimal128(5), Decimal128(500000).Rescale(6, 1));
  ASSERT_OK_AND_EQ(Decimal128(500000), Decimal128(5).Rescale(1, 6));
  ASSERT_RAISES(Invalid, Decimal128(555555).Rescale(6, 1));

  // Test some random numbers.
  for (auto original_scale : GetRandomNumbers<Int16Type>(16)) {
    for (auto value : GetRandomNumbers<Int32Type>(16)) {
      Decimal128 unscaled_value = Decimal128(value);
      Decimal128 scaled_value = unscaled_value;
      for (int32_t new_scale = original_scale; new_scale < original_scale + 29;
           new_scale++, scaled_value *= Decimal128(10)) {
        ASSERT_OK_AND_EQ(scaled_value, unscaled_value.Rescale(original_scale, new_scale));
        ASSERT_OK_AND_EQ(unscaled_value, scaled_value.Rescale(new_scale, original_scale));
      }
    }
  }

  for (auto original_scale : GetRandomNumbers<Int16Type>(16)) {
    Decimal128 value(1);
    for (int32_t new_scale = original_scale; new_scale < original_scale + 39;
         new_scale++, value *= Decimal128(10)) {
      Decimal128 negative_value = value * -1;
      ASSERT_OK_AND_EQ(value, Decimal128(1).Rescale(original_scale, new_scale));
      ASSERT_OK_AND_EQ(negative_value, Decimal128(-1).Rescale(original_scale, new_scale));
      ASSERT_OK_AND_EQ(Decimal128(1), value.Rescale(new_scale, original_scale));
      ASSERT_OK_AND_EQ(Decimal128(-1), negative_value.Rescale(new_scale, original_scale));
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
      Decimal128 decimal_x = Decimal128FromInt128(x);
      Decimal128 decimal_y = Decimal128FromInt128(y);
      Decimal128 result = decimal_x % decimal_y;
      EXPECT_EQ(Decimal128FromInt128(x % y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
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

  result = Decimal128("5").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(1, out);

  result = Decimal128("0").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(0, out);

  result = Decimal128("-123789").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);

  result = Decimal128("-123749").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1237, out);

  result = Decimal128("-123750").ReduceScaleBy(2, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1238, out);

  result = Decimal128("-5").ReduceScaleBy(1, true);
  ASSERT_OK(result.ToInteger(&out));
  ASSERT_EQ(-1, out);
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

TEST(Decimal128Test, LeftShift) {
  auto check = [](int128_t x, uint32_t bits) {
    auto expected = Decimal128FromInt128(x << bits);
    auto actual = Decimal128FromInt128(x) << bits;
    ASSERT_EQ(actual.low_bits(), expected.low_bits());
    ASSERT_EQ(actual.high_bits(), expected.high_bits());
  };

  ASSERT_EQ(Decimal128("0"), Decimal128("0") << 0);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") << 1);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") << 63);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") << 127);

  check(123, 0);
  check(123, 1);
  check(123, 63);
  check(123, 64);
  check(123, 120);

  ASSERT_EQ(Decimal128("199999999999998"), Decimal128("99999999999999") << 1);
  ASSERT_EQ(Decimal128("3435973836799965640261632"), Decimal128("99999999999999") << 35);
  ASSERT_EQ(Decimal128("120892581961461708544797985370825293824"),
            Decimal128("99999999999999") << 80);

  ASSERT_EQ(Decimal128("1234567890123456789012"), Decimal128("1234567890123456789012")
                                                      << 0);
  ASSERT_EQ(Decimal128("2469135780246913578024"), Decimal128("1234567890123456789012")
                                                      << 1);
  ASSERT_EQ(Decimal128("88959991838777271103427858320412639232"),
            Decimal128("1234567890123456789012") << 56);

  check(-123, 0);
  check(-123, 1);
  check(-123, 63);
  check(-123, 64);
  check(-123, 120);

  ASSERT_EQ(Decimal128("-199999999999998"), Decimal128("-99999999999999") << 1);
  ASSERT_EQ(Decimal128("-3435973836799965640261632"), Decimal128("-99999999999999")
                                                          << 35);
  ASSERT_EQ(Decimal128("-120892581961461708544797985370825293824"),
            Decimal128("-99999999999999") << 80);

  ASSERT_EQ(Decimal128("-1234567890123456789012"), Decimal128("-1234567890123456789012")
                                                       << 0);
  ASSERT_EQ(Decimal128("-2469135780246913578024"), Decimal128("-1234567890123456789012")
                                                       << 1);
  ASSERT_EQ(Decimal128("-88959991838777271103427858320412639232"),
            Decimal128("-1234567890123456789012") << 56);
}

TEST(Decimal128Test, RightShift) {
  ASSERT_EQ(Decimal128("0"), Decimal128("0") >> 0);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") >> 1);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") >> 63);
  ASSERT_EQ(Decimal128("0"), Decimal128("0") >> 127);

  ASSERT_EQ(Decimal128("1"), Decimal128("1") >> 0);
  ASSERT_EQ(Decimal128("0"), Decimal128("1") >> 1);
  ASSERT_EQ(Decimal128("0"), Decimal128("1") >> 63);
  ASSERT_EQ(Decimal128("0"), Decimal128("1") >> 127);

  ASSERT_EQ(Decimal128("-1"), Decimal128("-1") >> 0);
  ASSERT_EQ(Decimal128("-1"), Decimal128("-1") >> 1);
  ASSERT_EQ(Decimal128("-1"), Decimal128("-1") >> 63);
  ASSERT_EQ(Decimal128("-1"), Decimal128("-1") >> 127);

  ASSERT_EQ(Decimal128("1096516"), Decimal128("1234567890123456789012") >> 50);
  ASSERT_EQ(Decimal128("66"), Decimal128("1234567890123456789012") >> 64);
  ASSERT_EQ(Decimal128("2"), Decimal128("1234567890123456789012") >> 69);
  ASSERT_EQ(Decimal128("0"), Decimal128("1234567890123456789012") >> 71);
  ASSERT_EQ(Decimal128("0"), Decimal128("1234567890123456789012") >> 127);

  ASSERT_EQ(Decimal128("-1096517"), Decimal128("-1234567890123456789012") >> 50);
  ASSERT_EQ(Decimal128("-67"), Decimal128("-1234567890123456789012") >> 64);
  ASSERT_EQ(Decimal128("-3"), Decimal128("-1234567890123456789012") >> 69);
  ASSERT_EQ(Decimal128("-1"), Decimal128("-1234567890123456789012") >> 71);
  ASSERT_EQ(Decimal128("-1"), Decimal128("-1234567890123456789012") >> 127);
}

TEST(Decimal128Test, Negate) {
  auto check = [](Decimal128 pos, Decimal128 neg) {
    EXPECT_EQ(-pos, neg);
    EXPECT_EQ(-neg, pos);
  };

  check(Decimal128(0, 0), Decimal128(0, 0));
  check(Decimal128(0, 1), Decimal128(-1, 0xFFFFFFFFFFFFFFFFULL));
  check(Decimal128(0, 2), Decimal128(-1, 0xFFFFFFFFFFFFFFFEULL));
  check(Decimal128(0, 0x8000000000000000ULL), Decimal128(-1, 0x8000000000000000ULL));
  check(Decimal128(0, 0xFFFFFFFFFFFFFFFFULL), Decimal128(-1, 1));
  check(Decimal128(12, 0), Decimal128(-12, 0));
  check(Decimal128(12, 1), Decimal128(-13, 0xFFFFFFFFFFFFFFFFULL));
  check(Decimal128(12, 0xFFFFFFFFFFFFFFFFULL), Decimal128(-13, 1));
}

static constexpr std::array<uint64_t, 4> kSortedDecimal256Bits[] = {
    {0, 0, 0, 0x8000000000000000ULL},  // min
    {0xFFFFFFFFFFFFFFFEULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL,
     0xFFFFFFFFFFFFFFFFULL},  // -2
    {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL,
     0xFFFFFFFFFFFFFFFFULL},  // -1
    {0, 0, 0, 0},
    {1, 0, 0, 0},
    {2, 0, 0, 0},
    {0xFFFFFFFFFFFFFFFFULL, 0, 0, 0},
    {0, 1, 0, 0},
    {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0, 0},
    {0, 0, 1, 0},
    {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0},
    {0, 0, 0, 1},
    {0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL,
     0x7FFFFFFFFFFFFFFFULL},  // max
};

TEST(Decimal256Test, TestComparators) {
  constexpr size_t num_values =
      sizeof(kSortedDecimal256Bits) / sizeof(kSortedDecimal256Bits[0]);
  for (size_t i = 0; i < num_values; ++i) {
    Decimal256 left(::arrow::bit_util::little_endian::ToNative(kSortedDecimal256Bits[i]));
    for (size_t j = 0; j < num_values; ++j) {
      Decimal256 right(
          ::arrow::bit_util::little_endian::ToNative(kSortedDecimal256Bits[j]));
      EXPECT_EQ(i == j, left == right);
      EXPECT_EQ(i != j, left != right);
      EXPECT_EQ(i < j, left < right);
      EXPECT_EQ(i > j, left > right);
      EXPECT_EQ(i <= j, left <= right);
      EXPECT_EQ(i >= j, left >= right);
    }
  }
}

TEST(Decimal256Test, TestToBytesRoundTrip) {
  for (const std::array<uint64_t, 4>& bits : kSortedDecimal256Bits) {
    Decimal256 decimal(::arrow::bit_util::little_endian::ToNative(bits));
    EXPECT_EQ(decimal, Decimal256(decimal.ToBytes().data()));
  }
}

template <typename T>
class Decimal256Test : public ::testing::Test {
 public:
  Decimal256Test() {}
};

using Decimal256Types =
    ::testing::Types<char, unsigned char, short, unsigned short,  // NOLINT
                     int, unsigned int, long, unsigned long,      // NOLINT
                     long long, unsigned long long                // NOLINT
                     >;

TYPED_TEST_SUITE(Decimal256Test, Decimal256Types);

TYPED_TEST(Decimal256Test, ConstructibleFromAnyIntegerType) {
  using UInt64Array = std::array<uint64_t, 4>;
  Decimal256 value(TypeParam{42});
  EXPECT_EQ(UInt64Array({42, 0, 0, 0}),
            ::arrow::bit_util::little_endian::FromNative(value.native_endian_array()));

  TypeParam max = std::numeric_limits<TypeParam>::max();
  Decimal256 max_value(max);
  EXPECT_EQ(
      UInt64Array({static_cast<uint64_t>(max), 0, 0, 0}),
      ::arrow::bit_util::little_endian::FromNative(max_value.native_endian_array()));

  TypeParam min = std::numeric_limits<TypeParam>::min();
  Decimal256 min_value(min);
  uint64_t high_bits = std::is_signed<TypeParam>::value ? ~uint64_t{0} : uint64_t{0};
  EXPECT_EQ(
      UInt64Array({static_cast<uint64_t>(min), high_bits, high_bits, high_bits}),
      ::arrow::bit_util::little_endian::FromNative(min_value.native_endian_array()));
}

TEST(Decimal256Test, ConstructibleFromBool) {
  EXPECT_EQ(Decimal256(0), Decimal256(false));
  EXPECT_EQ(Decimal256(1), Decimal256(true));
}

Decimal256 Decimal256FromInt128(int128_t value) {
  return Decimal256(Decimal128(static_cast<int64_t>(value >> 64),
                               static_cast<uint64_t>(value & 0xFFFFFFFFFFFFFFFFULL)));
}

TEST(Decimal256Test, Multiply) {
  using boost::multiprecision::int256_t;
  using boost::multiprecision::uint256_t;

  ASSERT_EQ(Decimal256(60501), Decimal256(301) * Decimal256(201));

  ASSERT_EQ(Decimal256(-60501), Decimal256(-301) * Decimal256(201));

  ASSERT_EQ(Decimal256(-60501), Decimal256(301) * Decimal256(-201));

  ASSERT_EQ(Decimal256(60501), Decimal256(-301) * Decimal256(-201));

  // Test some random numbers.
  std::vector<int128_t> left;
  std::vector<int128_t> right;
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      for (auto z : GetRandomNumbers<Int32Type>(16)) {
        for (auto w : GetRandomNumbers<Int32Type>(16)) {
          // Test two 128 bit numbers which have a large amount of bits set.
          int128_t l = static_cast<uint128_t>(x) << 96 | static_cast<uint128_t>(y) << 64 |
                       static_cast<uint128_t>(z) << 32 | static_cast<uint128_t>(w);
          int128_t r = static_cast<uint128_t>(w) << 96 | static_cast<uint128_t>(z) << 64 |
                       static_cast<uint128_t>(y) << 32 | static_cast<uint128_t>(x);
          int256_t expected = int256_t(l) * r;
          Decimal256 actual = Decimal256FromInt128(l) * Decimal256FromInt128(r);
          ASSERT_EQ(expected.str(), actual.ToIntegerString())
              << " " << int256_t(l).str() << " * " << int256_t(r).str();
          // Test a 96 bit number against a 160 bit number.
          int128_t s = l >> 32;
          uint256_t b = uint256_t(r) << 32;
          Decimal256 b_dec =
              Decimal256FromInt128(r) * Decimal256(static_cast<uint64_t>(1) << 32);
          ASSERT_EQ(b.str(), b_dec.ToIntegerString()) << int256_t(r).str();
          expected = int256_t(s) * b;
          actual = Decimal256FromInt128(s) * b_dec;
          ASSERT_EQ(expected.str(), actual.ToIntegerString())
              << " " << int256_t(s).str() << " * " << int256_t(b).str();
        }
      }
    }
  }

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y :
         std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 0, 1, 2, 32, INT32_MAX}) {
      Decimal256 decimal_x = Decimal256FromInt128(x);
      Decimal256 decimal_y = Decimal256FromInt128(y);
      Decimal256 result = decimal_x * decimal_y;
      EXPECT_EQ(Decimal256FromInt128(x * y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(Decimal256Test, Shift) {
  {
    // Values compared against python's implementation of shift.
    Decimal256 v(967);
    v <<= 16;
    ASSERT_EQ(v, Decimal256("63373312"));
    v <<= 66;
    ASSERT_EQ(v, Decimal256("4676125070269385647763488768"));
    v <<= 128;
    ASSERT_EQ(v,
              Decimal256(
                  "1591202906929606242763855199532957938318305582067671727858104926208"));
  }
  {
    // Values compared against python's implementation of shift.
    Decimal256 v(0xEFFACDA);
    v <<= 17;
    ASSERT_EQ(v, Decimal256("32982558834688"));
    v <<= 67;
    ASSERT_EQ(v, Decimal256("4867366573756459829801535578046464"));
    v <<= 129;
    ASSERT_EQ(
        v,
        Decimal256(
            "3312558036779413504434176328500812891073739806516698535430241719490183168"));
    v <<= 43;
    ASSERT_EQ(v, Decimal256(0));
  }

  {
    // Values compared against python's implementation of shift.
    Decimal256 v("-12346789123456789123456789");
    v <<= 15;
    ASSERT_EQ(v, Decimal256("-404579585997432065997432061952"))
        << std::hex << v.native_endian_array()[0] << " " << v.native_endian_array()[1]
        << " " << v.native_endian_array()[2] << " " << v.native_endian_array()[3] << "\n"
        << Decimal256("-404579585997432065997432061952").native_endian_array()[0] << " "
        << Decimal256("-404579585997432065997432061952").native_endian_array()[1] << " "
        << Decimal256("-404579585997432065997432061952").native_endian_array()[2] << " "
        << Decimal256("-404579585997432065997432061952").native_endian_array()[3];
    v <<= 30;
    ASSERT_EQ(v, Decimal256("-434414022622047565860171081516421480448"));
    v <<= 66;
    ASSERT_EQ(v,
              Decimal256("-32054097189358332105678889809255994470201895906771963215872"));
  }
}

TEST(Decimal256Test, Add) {
  EXPECT_EQ(Decimal256(103), Decimal256(100) + Decimal256(3));
  EXPECT_EQ(Decimal256(203), Decimal256(200) + Decimal256(3));
  EXPECT_EQ(Decimal256(20401), Decimal256(20100) + Decimal256(301));
  EXPECT_EQ(Decimal256(-19799), Decimal256(-20100) + Decimal256(301));
  EXPECT_EQ(Decimal256(19799), Decimal256(20100) + Decimal256(-301));
  EXPECT_EQ(Decimal256(-20401), Decimal256(-20100) + Decimal256(-301));
  EXPECT_EQ(Decimal256("100000000000000000000000000000000001"),
            Decimal256("99999999999999999999999999999999999") + Decimal256("2"));
  EXPECT_EQ(Decimal256("120200000000000000000000000000002019"),
            Decimal256("99999999999999999999999999999999999") +
                Decimal256("20200000000000000000000000000002020"));

  // Test some random numbers.
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      if (y == 0) {
        continue;
      }

      Decimal256 result = Decimal256(x) + Decimal256(y);
      ASSERT_EQ(Decimal256(static_cast<int64_t>(x) + y), result)
          << " x: " << x << " y: " << y;
    }
  }
}

TEST(Decimal256Test, Divide) {
  ASSERT_EQ(Decimal256(33), Decimal256(100) / Decimal256(3));
  ASSERT_EQ(Decimal256(66), Decimal256(200) / Decimal256(3));
  ASSERT_EQ(Decimal256(66), Decimal256(20100) / Decimal256(301));
  ASSERT_EQ(Decimal256(-66), Decimal256(-20100) / Decimal256(301));
  ASSERT_EQ(Decimal256(-66), Decimal256(20100) / Decimal256(-301));
  ASSERT_EQ(Decimal256(66), Decimal256(-20100) / Decimal256(-301));
  ASSERT_EQ(Decimal256("-5192296858534827628530496329343552"),
            Decimal256("-269599466671506397946670150910580797473777870509761363"
                       "24636208709184") /
                Decimal256("5192296858534827628530496329874417"));
  ASSERT_EQ(Decimal256("5192296858534827628530496329343552"),
            Decimal256("-269599466671506397946670150910580797473777870509761363"
                       "24636208709184") /
                Decimal256("-5192296858534827628530496329874417"));
  ASSERT_EQ(Decimal256("5192296858534827628530496329343552"),
            Decimal256("2695994666715063979466701509105807974737778705097613632"
                       "4636208709184") /
                Decimal256("5192296858534827628530496329874417"));
  ASSERT_EQ(Decimal256("-5192296858534827628530496329343552"),
            Decimal256("2695994666715063979466701509105807974737778705097613632"
                       "4636208709184") /
                Decimal256("-5192296858534827628530496329874417"));

  // Test some random numbers.
  for (auto x : GetRandomNumbers<Int32Type>(16)) {
    for (auto y : GetRandomNumbers<Int32Type>(16)) {
      if (y == 0) {
        continue;
      }

      Decimal256 result = Decimal256(x) / Decimal256(y);
      ASSERT_EQ(Decimal256(static_cast<int64_t>(x) / y), result)
          << " x: " << x << " y: " << y;
    }
  }

  // Test some edge cases
  for (auto x :
       std::vector<int128_t>{-kInt128Max, -INT64_MAX - 1, -INT64_MAX, -INT32_MAX - 1,
                             -INT32_MAX, 0, INT32_MAX, INT64_MAX, kInt128Max}) {
    for (auto y : std::vector<int128_t>{-INT64_MAX - 1, -INT64_MAX, -INT32_MAX, -32, -2,
                                        -1, 1, 2, 32, INT32_MAX, INT64_MAX}) {
      Decimal256 decimal_x = Decimal256FromInt128(x);
      Decimal256 decimal_y = Decimal256FromInt128(y);
      Decimal256 result = decimal_x / decimal_y;
      EXPECT_EQ(Decimal256FromInt128(x / y), result);
    }
  }
}

TEST(Decimal256Test, Rescale) {
  ASSERT_OK_AND_EQ(Decimal256(11100), Decimal256(111).Rescale(0, 2));
  ASSERT_OK_AND_EQ(Decimal256(111), Decimal256(11100).Rescale(2, 0));
  ASSERT_OK_AND_EQ(Decimal256(5), Decimal256(500000).Rescale(6, 1));
  ASSERT_OK_AND_EQ(Decimal256(500000), Decimal256(5).Rescale(1, 6));
  ASSERT_RAISES(Invalid, Decimal256(555555).Rescale(6, 1));

  // Test some random numbers.
  for (auto original_scale : GetRandomNumbers<Int16Type>(16)) {
    for (auto value : GetRandomNumbers<Int32Type>(16)) {
      Decimal256 unscaled_value = Decimal256(value);
      Decimal256 scaled_value = unscaled_value;
      for (int32_t new_scale = original_scale; new_scale < original_scale + 68;
           new_scale++, scaled_value *= Decimal256(10)) {
        ASSERT_OK_AND_EQ(scaled_value, unscaled_value.Rescale(original_scale, new_scale));
        ASSERT_OK_AND_EQ(unscaled_value, scaled_value.Rescale(new_scale, original_scale));
      }
    }
  }

  for (auto original_scale : GetRandomNumbers<Int16Type>(16)) {
    Decimal256 value(1);
    for (int32_t new_scale = original_scale; new_scale < original_scale + 77;
         new_scale++, value *= Decimal256(10)) {
      Decimal256 negative_value = value * -1;
      ASSERT_OK_AND_EQ(value, Decimal256(1).Rescale(original_scale, new_scale));
      ASSERT_OK_AND_EQ(negative_value, Decimal256(-1).Rescale(original_scale, new_scale));
      ASSERT_OK_AND_EQ(Decimal256(1), value.Rescale(new_scale, original_scale));
      ASSERT_OK_AND_EQ(Decimal256(-1), negative_value.Rescale(new_scale, original_scale));
    }
  }
}

TEST(Decimal256Test, IncreaseScale) {
  Decimal256 result;

  result = Decimal256("1234").IncreaseScaleBy(0);
  ASSERT_EQ("1234", result.ToIntegerString());

  result = Decimal256("1234").IncreaseScaleBy(3);
  ASSERT_EQ("1234000", result.ToIntegerString());

  result = Decimal256("-1234").IncreaseScaleBy(3);
  ASSERT_EQ("-1234000", result.ToIntegerString());
}

TEST(Decimal256Test, ReduceScaleAndRound) {
  Decimal256 result;

  result = Decimal256("123456").ReduceScaleBy(0);
  ASSERT_EQ("123456", result.ToIntegerString());

  result = Decimal256("123456").ReduceScaleBy(1, false);
  ASSERT_EQ("12345", result.ToIntegerString());

  result = Decimal256("123456").ReduceScaleBy(1, true);
  ASSERT_EQ("12346", result.ToIntegerString());

  result = Decimal256("123451").ReduceScaleBy(1, true);
  ASSERT_EQ("12345", result.ToIntegerString());

  result = Decimal256("5").ReduceScaleBy(1, true);
  ASSERT_EQ("1", result.ToIntegerString());

  result = Decimal256("0").ReduceScaleBy(1, true);
  ASSERT_EQ("0", result.ToIntegerString());

  result = Decimal256("-123789").ReduceScaleBy(2, true);
  ASSERT_EQ("-1238", result.ToIntegerString());

  result = Decimal256("-123749").ReduceScaleBy(2, true);
  ASSERT_EQ("-1237", result.ToIntegerString());

  result = Decimal256("-123750").ReduceScaleBy(2, true);
  ASSERT_EQ("-1238", result.ToIntegerString());

  result = Decimal256("-5").ReduceScaleBy(1, true);
  ASSERT_EQ("-1", result.ToIntegerString());
}

TEST(Decimal256, FromBigEndianTest) {
  // We test out a variety of scenarios:
  //
  // * Positive values that are left shifted
  //   and filled in with the same bit pattern
  // * Negated of the positive values
  // * Complement of the positive values
  //
  // For the positive values, we can call FromBigEndian
  // with a length that is less than 16, whereas we must
  // pass all 32 bytes for the negative and complement.
  //
  // We use a number of bit patterns to increase the coverage
  // of scenarios
  for (int32_t start : {1, 1, 15, /* 00001111 */
                        85,       /* 01010101 */
                        127 /* 01111111 */}) {
    Decimal256 value(start);
    for (int ii = 0; ii < 32; ++ii) {
      auto native_endian = value.ToBytes();
#if ARROW_LITTLE_ENDIAN
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      // Limit the number of bytes we are passing to make
      // sure that it works correctly. That's why all of the
      // 'start' values don't have a 1 in the most significant
      // bit place
      ASSERT_OK_AND_EQ(value,
                       Decimal256::FromBigEndian(native_endian.data() + 31 - ii, ii + 1));

      // Negate it
      auto negated = -value;
      native_endian = negated.ToBytes();
#if ARROW_LITTLE_ENDIAN
      // convert to big endian
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      // The sign bit is looked up in the MSB
      ASSERT_OK_AND_EQ(negated,
                       Decimal256::FromBigEndian(native_endian.data() + 31 - ii, ii + 1));

      // Take the complement
      auto complement = ~value;
      native_endian = complement.ToBytes();
#if ARROW_LITTLE_ENDIAN
      // convert to big endian
      std::reverse(native_endian.begin(), native_endian.end());
#endif
      ASSERT_OK_AND_EQ(complement, Decimal256::FromBigEndian(native_endian.data(), 32));

      value <<= 8;
      value += Decimal256(start);
    }
  }
}

TEST(Decimal256Test, TestFromBigEndianBadLength) {
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(nullptr, -1));
  ASSERT_RAISES(Invalid, Decimal128::FromBigEndian(nullptr, 33));
}

class Decimal256ToStringTest : public ::testing::TestWithParam<ToStringTestParam> {};

TEST_P(Decimal256ToStringTest, ToString) {
  const ToStringTestParam& data = GetParam();
  const Decimal256 value(data.test_value);
  const std::string printed_value = value.ToString(data.scale);
  ASSERT_EQ(data.expected_string, printed_value);
}

INSTANTIATE_TEST_SUITE_P(Decimal256ToStringTest, Decimal256ToStringTest,
                         ::testing::ValuesIn(kToStringTestData));

}  // namespace arrow

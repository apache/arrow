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

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace internal {

template <typename T>
void AssertConversion(const T& type, const std::string& s, typename T::c_type expected) {
  typename T::c_type out{};
  ASSERT_TRUE(ParseValue(type, s.data(), s.length(), &out))
      << "Conversion failed for '" << s << "' (expected to return " << expected << ")";
  ASSERT_EQ(out, expected) << "Conversion failed for '" << s << "'";
}

template <typename T>
void AssertConversion(const std::string& s, typename T::c_type expected) {
  auto type = checked_pointer_cast<T>(TypeTraits<T>::type_singleton());
  AssertConversion(*type, s, expected);
}

template <typename T>
void AssertConversionFails(const T& type, const std::string& s) {
  typename T::c_type out{};
  ASSERT_FALSE(ParseValue(type, s.data(), s.length(), &out))
      << "Conversion should have failed for '" << s << "' (returned " << out << ")";
}

template <typename T>
void AssertConversionFails(const std::string& s) {
  auto type = checked_pointer_cast<T>(TypeTraits<T>::type_singleton());
  AssertConversionFails(*type, s);
}

TEST(StringConversion, ToBoolean) {
  AssertConversion<BooleanType>("true", true);
  AssertConversion<BooleanType>("tRuE", true);
  AssertConversion<BooleanType>("FAlse", false);
  AssertConversion<BooleanType>("false", false);
  AssertConversion<BooleanType>("1", true);
  AssertConversion<BooleanType>("0", false);

  AssertConversionFails<BooleanType>("");
}

TEST(StringConversion, ToFloat) {
  AssertConversion<FloatType>("1.5", 1.5f);
  AssertConversion<FloatType>("0", 0.0f);
  // XXX ASSERT_EQ doesn't distinguish signed zeros
  AssertConversion<FloatType>("-0.0", -0.0f);
  AssertConversion<FloatType>("-1e20", -1e20f);

  AssertConversionFails<FloatType>("");
  AssertConversionFails<FloatType>("e");
}

TEST(StringConversion, ToDouble) {
  AssertConversion<DoubleType>("1.5", 1.5);
  AssertConversion<DoubleType>("0", 0);
  // XXX ASSERT_EQ doesn't distinguish signed zeros
  AssertConversion<DoubleType>("-0.0", -0.0);
  AssertConversion<DoubleType>("-1e100", -1e100);

  AssertConversionFails<DoubleType>("");
  AssertConversionFails<DoubleType>("e");
}

#if !defined(_WIN32) || defined(NDEBUG)

TEST(StringConversion, ToFloatLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<FloatType>("1.5", 1.5f);
}

TEST(StringConversion, ToDoubleLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<DoubleType>("1.5", 1.5f);
}

#endif  // _WIN32

TEST(StringConversion, ToInt8) {
  AssertConversion<Int8Type>("0", 0);
  AssertConversion<Int8Type>("127", 127);
  AssertConversion<Int8Type>("0127", 127);
  AssertConversion<Int8Type>("-128", -128);
  AssertConversion<Int8Type>("-00128", -128);

  // Non-representable values
  AssertConversionFails<Int8Type>("128");
  AssertConversionFails<Int8Type>("-129");

  AssertConversionFails<Int8Type>("");
  AssertConversionFails<Int8Type>("-");
  AssertConversionFails<Int8Type>("0.0");
  AssertConversionFails<Int8Type>("e");
}

TEST(StringConversion, ToUInt8) {
  AssertConversion<UInt8Type>("0", 0);
  AssertConversion<UInt8Type>("26", 26);
  AssertConversion<UInt8Type>("255", 255);
  AssertConversion<UInt8Type>("0255", 255);

  // Non-representable values
  AssertConversionFails<UInt8Type>("-1");
  AssertConversionFails<UInt8Type>("256");
  AssertConversionFails<UInt8Type>("260");
  AssertConversionFails<UInt8Type>("1234");

  AssertConversionFails<UInt8Type>("");
  AssertConversionFails<UInt8Type>("-");
  AssertConversionFails<UInt8Type>("0.0");
  AssertConversionFails<UInt8Type>("e");
}

TEST(StringConversion, ToInt16) {
  AssertConversion<Int16Type>("0", 0);
  AssertConversion<Int16Type>("32767", 32767);
  AssertConversion<Int16Type>("032767", 32767);
  AssertConversion<Int16Type>("-32768", -32768);
  AssertConversion<Int16Type>("-0032768", -32768);

  // Non-representable values
  AssertConversionFails<Int16Type>("32768");
  AssertConversionFails<Int16Type>("-32769");

  AssertConversionFails<Int16Type>("");
  AssertConversionFails<Int16Type>("-");
  AssertConversionFails<Int16Type>("0.0");
  AssertConversionFails<Int16Type>("e");
}

TEST(StringConversion, ToUInt16) {
  AssertConversion<UInt16Type>("0", 0);
  AssertConversion<UInt16Type>("6660", 6660);
  AssertConversion<UInt16Type>("65535", 65535);
  AssertConversion<UInt16Type>("065535", 65535);

  // Non-representable values
  AssertConversionFails<UInt16Type>("-1");
  AssertConversionFails<UInt16Type>("65536");
  AssertConversionFails<UInt16Type>("123456");

  AssertConversionFails<UInt16Type>("");
  AssertConversionFails<UInt16Type>("-");
  AssertConversionFails<UInt16Type>("0.0");
  AssertConversionFails<UInt16Type>("e");
}

TEST(StringConversion, ToInt32) {
  AssertConversion<Int32Type>("0", 0);
  AssertConversion<Int32Type>("2147483647", 2147483647);
  AssertConversion<Int32Type>("02147483647", 2147483647);
  AssertConversion<Int32Type>("-2147483648", -2147483648LL);
  AssertConversion<Int32Type>("-002147483648", -2147483648LL);

  // Non-representable values
  AssertConversionFails<Int32Type>("2147483648");
  AssertConversionFails<Int32Type>("-2147483649");

  AssertConversionFails<Int32Type>("");
  AssertConversionFails<Int32Type>("-");
  AssertConversionFails<Int32Type>("0.0");
  AssertConversionFails<Int32Type>("e");
}

TEST(StringConversion, ToUInt32) {
  AssertConversion<UInt32Type>("0", 0);
  AssertConversion<UInt32Type>("432198765", 432198765UL);
  AssertConversion<UInt32Type>("4294967295", 4294967295UL);
  AssertConversion<UInt32Type>("04294967295", 4294967295UL);

  // Non-representable values
  AssertConversionFails<UInt32Type>("-1");
  AssertConversionFails<UInt32Type>("4294967296");
  AssertConversionFails<UInt32Type>("12345678901");

  AssertConversionFails<UInt32Type>("");
  AssertConversionFails<UInt32Type>("-");
  AssertConversionFails<UInt32Type>("0.0");
  AssertConversionFails<UInt32Type>("e");
}

TEST(StringConversion, ToInt64) {
  AssertConversion<Int64Type>("0", 0);
  AssertConversion<Int64Type>("9223372036854775807", 9223372036854775807LL);
  AssertConversion<Int64Type>("09223372036854775807", 9223372036854775807LL);
  AssertConversion<Int64Type>("-9223372036854775808", -9223372036854775807LL - 1);
  AssertConversion<Int64Type>("-009223372036854775808", -9223372036854775807LL - 1);

  // Non-representable values
  AssertConversionFails<Int64Type>("9223372036854775808");
  AssertConversionFails<Int64Type>("-9223372036854775809");

  AssertConversionFails<Int64Type>("");
  AssertConversionFails<Int64Type>("-");
  AssertConversionFails<Int64Type>("0.0");
  AssertConversionFails<Int64Type>("e");
}

TEST(StringConversion, ToUInt64) {
  AssertConversion<UInt64Type>("0", 0);
  AssertConversion<UInt64Type>("18446744073709551615", 18446744073709551615ULL);

  // Non-representable values
  AssertConversionFails<UInt64Type>("-1");
  AssertConversionFails<UInt64Type>("18446744073709551616");

  AssertConversionFails<UInt64Type>("");
  AssertConversionFails<UInt64Type>("-");
  AssertConversionFails<UInt64Type>("0.0");
  AssertConversionFails<UInt64Type>("e");
}

TEST(StringConversion, ToTimestampDate_ISO8601) {
  {
    TimestampType type{TimeUnit::SECOND};

    AssertConversion(type, "1970-01-01", 0);
    AssertConversion(type, "1989-07-14", 616377600);
    AssertConversion(type, "2000-02-29", 951782400);
    AssertConversion(type, "3989-07-14", 63730281600LL);
    AssertConversion(type, "1900-02-28", -2203977600LL);

    AssertConversionFails(type, "");
    AssertConversionFails(type, "1970");
    AssertConversionFails(type, "19700101");
    AssertConversionFails(type, "1970/01/01");
    AssertConversionFails(type, "1970-01-01 ");
    AssertConversionFails(type, "1970-01-01Z");

    // Invalid dates
    AssertConversionFails(type, "1970-00-01");
    AssertConversionFails(type, "1970-13-01");
    AssertConversionFails(type, "1970-01-32");
    AssertConversionFails(type, "1970-02-29");
    AssertConversionFails(type, "2100-02-29");
  }
  {
    TimestampType type{TimeUnit::MILLI};

    AssertConversion(type, "1970-01-01", 0);
    AssertConversion(type, "1989-07-14", 616377600000LL);
    AssertConversion(type, "3989-07-14", 63730281600000LL);
    AssertConversion(type, "1900-02-28", -2203977600000LL);
  }
  {
    TimestampType type{TimeUnit::MICRO};

    AssertConversion(type, "1970-01-01", 0);
    AssertConversion(type, "1989-07-14", 616377600000000LL);
    AssertConversion(type, "3989-07-14", 63730281600000000LL);
    AssertConversion(type, "1900-02-28", -2203977600000000LL);
  }
  {
    TimestampType type{TimeUnit::NANO};

    AssertConversion(type, "1970-01-01", 0);
    AssertConversion(type, "1989-07-14", 616377600000000000LL);
    AssertConversion(type, "2018-11-13", 1542067200000000000LL);
    AssertConversion(type, "1900-02-28", -2203977600000000000LL);
  }
}

TEST(StringConversion, ToTimestampDateTime_ISO8601) {
  {
    TimestampType type{TimeUnit::SECOND};

    AssertConversion(type, "1970-01-01 00:00:00", 0);
    AssertConversion(type, "2018-11-13 17", 1542128400);
    AssertConversion(type, "2018-11-13T17", 1542128400);
    AssertConversion(type, "2018-11-13 17Z", 1542128400);
    AssertConversion(type, "2018-11-13T17Z", 1542128400);
    AssertConversion(type, "2018-11-13 17:11", 1542129060);
    AssertConversion(type, "2018-11-13T17:11", 1542129060);
    AssertConversion(type, "2018-11-13 17:11Z", 1542129060);
    AssertConversion(type, "2018-11-13T17:11Z", 1542129060);
    AssertConversion(type, "2018-11-13 17:11:10", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10", 1542129070);
    AssertConversion(type, "2018-11-13 17:11:10Z", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304LL);

    // No subseconds allowed
    AssertConversionFails(type, "1900-02-28 12:34:56.001");
    // Invalid dates
    AssertConversionFails(type, "1970-02-29 00:00:00");
    AssertConversionFails(type, "2100-02-29 00:00:00");
    // Invalid times
    AssertConversionFails(type, "1970-01-01 24");
    AssertConversionFails(type, "1970-01-01 00:60");
    AssertConversionFails(type, "1970-01-01 00,00");
    AssertConversionFails(type, "1970-01-01 24:00:00");
    AssertConversionFails(type, "1970-01-01 00:60:00");
    AssertConversionFails(type, "1970-01-01 00:00:60");
    AssertConversionFails(type, "1970-01-01 00:00,00");
    AssertConversionFails(type, "1970-01-01 00,00:00");
  }
  {
    TimestampType type{TimeUnit::MILLI};

    AssertConversion(type, "2018-11-13 17:11:10", 1542129070000LL);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070000LL);
    AssertConversion(type, "3989-07-14T11:22:33Z", 63730322553000LL);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304000LL);
    AssertConversion(type, "2018-11-13T17:11:10.777Z", 1542129070777LL);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1");
    AssertConversionFails(type, "1900-02-28 12:34:56.123456");
  }
  {
    TimestampType type{TimeUnit::MICRO};

    AssertConversion(type, "2018-11-13 17:11:10", 1542129070000000LL);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070000000LL);
    AssertConversion(type, "3989-07-14T11:22:33Z", 63730322553000000LL);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304000000LL);
    AssertConversion(type, "2018-11-13T17:11:10.777000", 1542129070777000LL);
    AssertConversion(type, "3989-07-14T11:22:33.000777Z", 63730322553000777LL);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1");
    AssertConversionFails(type, "2018-11-13T17:11:10.777");
    AssertConversionFails(type, "1900-02-28 12:34:56.123456789");
  }
  {
    TimestampType type{TimeUnit::NANO};

    AssertConversion(type, "2018-11-13 17:11:10", 1542129070000000000LL);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304000000000LL);
    AssertConversion(type, "2018-11-13 17:11:10.777000000", 1542129070777000000LL);
    AssertConversion(type, "2018-11-13T17:11:10.000777000Z", 1542129070000777000LL);
    AssertConversion(type, "1969-12-31 23:59:59.999999999", -1);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1");
    AssertConversionFails(type, "1900-02-28 12:34:56.123");
    AssertConversionFails(type, "1900-02-28 12:34:56.123456");
  }
}

TEST(TimestampParser, StrptimeParser) {
  std::string format = "%m/%d/%Y %H:%M:%S";
  auto parser = TimestampParser::MakeStrptime(format);

  struct Case {
    std::string value;
    std::string iso8601;
  };

  std::vector<Case> cases = {{"5/31/2000 12:34:56", "2000-05-31 12:34:56"},
                             {"5/31/2000 00:00:00", "2000-05-31 00:00:00"}};

  std::vector<TimeUnit::type> units = {TimeUnit::SECOND, TimeUnit::MILLI, TimeUnit::MICRO,
                                       TimeUnit::NANO};

  for (auto unit : units) {
    for (const auto& case_ : cases) {
      int64_t converted, expected;
      ASSERT_TRUE((*parser)(case_.value.c_str(), case_.value.size(), unit, &converted));
      ASSERT_TRUE(ParseTimestampISO8601(case_.iso8601.c_str(), case_.iso8601.size(), unit,
                                        &expected));
      ASSERT_EQ(expected, converted);
    }
  }

  // Unparseable strings
  std::vector<std::string> unparseables = {"foo", "5/1/2000", "5/1/2000 12:34:56:6"};
  for (auto& value : unparseables) {
    int64_t dummy;
    ASSERT_FALSE((*parser)(value.c_str(), value.size(), TimeUnit::SECOND, &dummy));
  }
}

}  // namespace internal
}  // namespace arrow

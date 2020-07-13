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

template <typename T, typename Context = void>
void AssertConversion(const std::string& s, typename T::c_type expected,
                      const Context* ctx = NULLPTR) {
  typename T::c_type out{};
  ASSERT_TRUE(ParseValue<T>(s.data(), s.length(), &out, ctx))
      << "Conversion failed for '" << s << "' (expected to return " << expected << ")";
  ASSERT_EQ(out, expected) << "Conversion failed for '" << s << "'";
}

template <typename T, typename Context = void>
void AssertConversionFails(const std::string& s, const Context* ctx = NULLPTR) {
  typename T::c_type out{};
  ASSERT_FALSE(ParseValue<T>(s.data(), s.length(), &out, ctx))
      << "Conversion should have failed for '" << s << "' (returned " << out << ")";
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
    ParseTimestampContext ctx{TimeUnit::SECOND};

    AssertConversion<TimestampType>("1970-01-01", 0, &ctx);
    AssertConversion<TimestampType>("1989-07-14", 616377600, &ctx);
    AssertConversion<TimestampType>("2000-02-29", 951782400, &ctx);
    AssertConversion<TimestampType>("3989-07-14", 63730281600LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28", -2203977600LL, &ctx);

    AssertConversionFails<TimestampType>("", &ctx);
    AssertConversionFails<TimestampType>("1970", &ctx);
    AssertConversionFails<TimestampType>("19700101", &ctx);
    AssertConversionFails<TimestampType>("1970/01/01", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 ", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01Z", &ctx);

    // Invalid dates
    AssertConversionFails<TimestampType>("1970-00-01", &ctx);
    AssertConversionFails<TimestampType>("1970-13-01", &ctx);
    AssertConversionFails<TimestampType>("1970-01-32", &ctx);
    AssertConversionFails<TimestampType>("1970-02-29", &ctx);
    AssertConversionFails<TimestampType>("2100-02-29", &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::MILLI};

    AssertConversion<TimestampType>("1970-01-01", 0, &ctx);
    AssertConversion<TimestampType>("1989-07-14", 616377600000LL, &ctx);
    AssertConversion<TimestampType>("3989-07-14", 63730281600000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28", -2203977600000LL, &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::MICRO};

    AssertConversion<TimestampType>("1970-01-01", 0, &ctx);
    AssertConversion<TimestampType>("1989-07-14", 616377600000000LL, &ctx);
    AssertConversion<TimestampType>("3989-07-14", 63730281600000000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28", -2203977600000000LL, &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::NANO};

    AssertConversion<TimestampType>("1970-01-01", 0, &ctx);
    AssertConversion<TimestampType>("1989-07-14", 616377600000000000LL, &ctx);
    AssertConversion<TimestampType>("2018-11-13", 1542067200000000000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28", -2203977600000000000LL, &ctx);
  }
}

TEST(StringConversion, ToTimestampDateTime_ISO8601) {
  {
    ParseTimestampContext ctx{TimeUnit::SECOND};

    AssertConversion<TimestampType>("1970-01-01 00:00:00", 0, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17", 1542128400, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17", 1542128400, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17Z", 1542128400, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17Z", 1542128400, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17:11", 1542129060, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11", 1542129060, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17:11Z", 1542129060, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11Z", 1542129060, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17:11:10", 1542129070, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11:10", 1542129070, &ctx);
    AssertConversion<TimestampType>("2018-11-13 17:11:10Z", 1542129070, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11:10Z", 1542129070, &ctx);
    AssertConversion<TimestampType>("1900-02-28 12:34:56", -2203932304LL, &ctx);

    // Invalid dates
    AssertConversionFails<TimestampType>("1970-02-29 00:00:00", &ctx);
    AssertConversionFails<TimestampType>("2100-02-29 00:00:00", &ctx);
    // Invalid times
    AssertConversionFails<TimestampType>("1970-01-01 24", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00:60", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00,00", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 24:00:00", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00:60:00", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00:00:60", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00:00,00", &ctx);
    AssertConversionFails<TimestampType>("1970-01-01 00,00:00", &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::MILLI};

    AssertConversion<TimestampType>("2018-11-13 17:11:10", 1542129070000LL, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11:10Z", 1542129070000LL, &ctx);
    AssertConversion<TimestampType>("3989-07-14T11:22:33Z", 63730322553000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28 12:34:56", -2203932304000LL, &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::MICRO};

    AssertConversion<TimestampType>("2018-11-13 17:11:10", 1542129070000000LL, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11:10Z", 1542129070000000LL, &ctx);
    AssertConversion<TimestampType>("3989-07-14T11:22:33Z", 63730322553000000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28 12:34:56", -2203932304000000LL, &ctx);
  }
  {
    ParseTimestampContext ctx{TimeUnit::NANO};

    AssertConversion<TimestampType>("2018-11-13 17:11:10", 1542129070000000000LL, &ctx);
    AssertConversion<TimestampType>("2018-11-13T17:11:10Z", 1542129070000000000LL, &ctx);
    AssertConversion<TimestampType>("1900-02-28 12:34:56", -2203932304000000000LL, &ctx);
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

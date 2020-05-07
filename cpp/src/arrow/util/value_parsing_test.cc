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

template <typename Converter, typename C_TYPE>
void AssertConversion(const std::string& s, C_TYPE expected) {
  typename Converter::value_type out;
  ASSERT_TRUE(Converter::Convert(s.data(), s.length(), &out))
      << "Conversion failed for '" << s << "' (expected to return " << expected << ")";
  ASSERT_EQ(out, expected) << "Conversion failed for '" << s << "'";
}

template <typename Converter>
void AssertConversionFails(const std::string& s) {
  typename Converter::value_type out;
  ASSERT_FALSE(Converter::Convert(s.data(), s.length(), &out))
      << "Conversion should have failed for '" << s << "' (returned " << out << ")";
}

TEST(StringConversion, ToBoolean) {
  using CType = StringConverter<BooleanType>;
  AssertConversion<CType>("true", true);
  AssertConversion<CType>("tRuE", true);
  AssertConversion<CType>("FAlse", false);
  AssertConversion<CType>("false", false);
  AssertConversion<CType>("1", true);
  AssertConversion<CType>("0", false);

  AssertConversionFails<CType>("");
}

TEST(StringConversion, ToFloat) {
  using CType = StringConverter<FloatType>;
  AssertConversion<CType>("1.5", 1.5f);
  AssertConversion<CType>("0", 0.0f);
  // XXX ASSERT_EQ doesn't distinguish signed zeros
  AssertConversion<CType>("-0.0", -0.0f);
  AssertConversion<CType>("-1e20", -1e20f);

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToDouble) {
  using CType = StringConverter<DoubleType>;
  AssertConversion<CType>("1.5", 1.5);
  AssertConversion<CType>("0", 0);
  // XXX ASSERT_EQ doesn't distinguish signed zeros
  AssertConversion<CType>("-0.0", -0.0);
  AssertConversion<CType>("-1e100", -1e100);

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("e");
}

#if !defined(_WIN32) || defined(NDEBUG)

TEST(StringConversion, ToFloatLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<StringConverter<FloatType>>("1.5", 1.5f);
}

TEST(StringConversion, ToDoubleLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<StringConverter<DoubleType>>("1.5", 1.5f);
}

#endif  // _WIN32

TEST(StringConversion, ToInt8) {
  using CType = StringConverter<Int8Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("127", 127);
  AssertConversion<CType>("0127", 127);
  AssertConversion<CType>("-128", -128);
  AssertConversion<CType>("-00128", -128);

  // Non-representable values
  AssertConversionFails<CType>("128");
  AssertConversionFails<CType>("-129");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToUInt8) {
  using CType = StringConverter<UInt8Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("26", 26);
  AssertConversion<CType>("255", 255);
  AssertConversion<CType>("0255", 255);

  // Non-representable values
  AssertConversionFails<CType>("-1");
  AssertConversionFails<CType>("256");
  AssertConversionFails<CType>("260");
  AssertConversionFails<CType>("1234");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToInt16) {
  using CType = StringConverter<Int16Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("32767", 32767);
  AssertConversion<CType>("032767", 32767);
  AssertConversion<CType>("-32768", -32768);
  AssertConversion<CType>("-0032768", -32768);

  // Non-representable values
  AssertConversionFails<CType>("32768");
  AssertConversionFails<CType>("-32769");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToUInt16) {
  using CType = StringConverter<UInt16Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("6660", 6660);
  AssertConversion<CType>("65535", 65535);
  AssertConversion<CType>("065535", 65535);

  // Non-representable values
  AssertConversionFails<CType>("-1");
  AssertConversionFails<CType>("65536");
  AssertConversionFails<CType>("123456");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToInt32) {
  using CType = StringConverter<Int32Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("2147483647", 2147483647);
  AssertConversion<CType>("02147483647", 2147483647);
  AssertConversion<CType>("-2147483648", -2147483648LL);
  AssertConversion<CType>("-002147483648", -2147483648LL);

  // Non-representable values
  AssertConversionFails<CType>("2147483648");
  AssertConversionFails<CType>("-2147483649");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToUInt32) {
  using CType = StringConverter<UInt32Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("432198765", 432198765UL);
  AssertConversion<CType>("4294967295", 4294967295UL);
  AssertConversion<CType>("04294967295", 4294967295UL);

  // Non-representable values
  AssertConversionFails<CType>("-1");
  AssertConversionFails<CType>("4294967296");
  AssertConversionFails<CType>("12345678901");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToInt64) {
  using CType = StringConverter<Int64Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("9223372036854775807", 9223372036854775807LL);
  AssertConversion<CType>("09223372036854775807", 9223372036854775807LL);
  AssertConversion<CType>("-9223372036854775808", -9223372036854775807LL - 1);
  AssertConversion<CType>("-009223372036854775808", -9223372036854775807LL - 1);

  // Non-representable values
  AssertConversionFails<CType>("9223372036854775808");
  AssertConversionFails<CType>("-9223372036854775809");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

TEST(StringConversion, ToUInt64) {
  using CType = StringConverter<UInt64Type>;

  AssertConversion<CType>("0", 0);
  AssertConversion<CType>("18446744073709551615", 18446744073709551615ULL);

  // Non-representable values
  AssertConversionFails<CType>("-1");
  AssertConversionFails<CType>("18446744073709551616");

  AssertConversionFails<CType>("");
  AssertConversionFails<CType>("-");
  AssertConversionFails<CType>("0.0");
  AssertConversionFails<CType>("e");
}

template <TimeUnit::type UNIT>
struct TimestampChecks {
  static void AssertPasses(const std::string& s, int64_t expected) {
    auto parser = TimestampParser::MakeISO8601();
    int64_t out;
    ASSERT_TRUE((*parser)(s.data(), s.length(), UNIT, &out))
        << "Conversion failed for '" << s << "' (expected to return " << expected << ")";
    ASSERT_EQ(out, expected) << "Conversion failed for '" << s << "'";
  }

  static void AssertFails(const std::string& s) {
    auto parser = TimestampParser::MakeISO8601();
    int64_t out;
    ASSERT_FALSE((*parser)(s.data(), s.length(), UNIT, &out))
        << "Conversion should have failed for '" << s << "' (returned " << out << ")";
  }
};

auto AssertSecond = TimestampChecks<TimeUnit::SECOND>::AssertPasses;
auto AssertSecondFails = TimestampChecks<TimeUnit::SECOND>::AssertFails;
auto AssertMilli = TimestampChecks<TimeUnit::MILLI>::AssertPasses;
auto AssertMilliFails = TimestampChecks<TimeUnit::MILLI>::AssertFails;
auto AssertMicro = TimestampChecks<TimeUnit::MICRO>::AssertPasses;
auto AssertMicroFails = TimestampChecks<TimeUnit::MICRO>::AssertFails;
auto AssertNano = TimestampChecks<TimeUnit::NANO>::AssertPasses;
auto AssertNanoFails = TimestampChecks<TimeUnit::NANO>::AssertFails;

TEST(StringConversion, ToTimestampDate_ISO8601) {
  {
    AssertSecond("1970-01-01", 0);
    AssertSecond("1989-07-14", 616377600);
    AssertSecond("2000-02-29", 951782400);
    AssertSecond("3989-07-14", 63730281600LL);
    AssertSecond("1900-02-28", -2203977600LL);

    AssertSecondFails("");
    AssertSecondFails("1970");
    AssertSecondFails("19700101");
    AssertSecondFails("1970/01/01");
    AssertSecondFails("1970-01-01 ");
    AssertSecondFails("1970-01-01Z");

    // Invalid dates
    AssertSecondFails("1970-00-01");
    AssertSecondFails("1970-13-01");
    AssertSecondFails("1970-01-32");
    AssertSecondFails("1970-02-29");
    AssertSecondFails("2100-02-29");
  }
  {
    AssertMilli("1970-01-01", 0);
    AssertMilli("1989-07-14", 616377600000LL);
    AssertMilli("3989-07-14", 63730281600000LL);
    AssertMilli("1900-02-28", -2203977600000LL);
  }
  {
    AssertMicro("1970-01-01", 0);
    AssertMicro("1989-07-14", 616377600000000LL);
    AssertMicro("3989-07-14", 63730281600000000LL);
    AssertMicro("1900-02-28", -2203977600000000LL);
  }
  {
    AssertNano("1970-01-01", 0);
    AssertNano("1989-07-14", 616377600000000000LL);
    AssertNano("2018-11-13", 1542067200000000000LL);
    AssertNano("1900-02-28", -2203977600000000000LL);
  }
}

TEST(StringConversion, ToTimestampDateTime_ISO8601) {
  {
    AssertSecond("1970-01-01 00:00:00", 0);
    AssertSecond("2018-11-13 17", 1542128400);
    AssertSecond("2018-11-13T17", 1542128400);
    AssertSecond("2018-11-13 17Z", 1542128400);
    AssertSecond("2018-11-13T17Z", 1542128400);
    AssertSecond("2018-11-13 17:11", 1542129060);
    AssertSecond("2018-11-13T17:11", 1542129060);
    AssertSecond("2018-11-13 17:11Z", 1542129060);
    AssertSecond("2018-11-13T17:11Z", 1542129060);
    AssertSecond("2018-11-13 17:11:10", 1542129070);
    AssertSecond("2018-11-13T17:11:10", 1542129070);
    AssertSecond("2018-11-13 17:11:10Z", 1542129070);
    AssertSecond("2018-11-13T17:11:10Z", 1542129070);
    AssertSecond("1900-02-28 12:34:56", -2203932304LL);

    // Invalid dates
    AssertSecondFails("1970-02-29 00:00:00");
    AssertSecondFails("2100-02-29 00:00:00");
    // Invalid times
    AssertSecondFails("1970-01-01 24");
    AssertSecondFails("1970-01-01 00:60");
    AssertSecondFails("1970-01-01 00,00");
    AssertSecondFails("1970-01-01 24:00:00");
    AssertSecondFails("1970-01-01 00:60:00");
    AssertSecondFails("1970-01-01 00:00:60");
    AssertSecondFails("1970-01-01 00:00,00");
    AssertSecondFails("1970-01-01 00,00:00");
  }
  {
    AssertMilli("2018-11-13 17:11:10", 1542129070000LL);
    AssertMilli("2018-11-13T17:11:10Z", 1542129070000LL);
    AssertMilli("3989-07-14T11:22:33Z", 63730322553000LL);
    AssertMilli("1900-02-28 12:34:56", -2203932304000LL);
  }
  {
    AssertMicro("2018-11-13 17:11:10", 1542129070000000LL);
    AssertMicro("2018-11-13T17:11:10Z", 1542129070000000LL);
    AssertMicro("3989-07-14T11:22:33Z", 63730322553000000LL);
    AssertMicro("1900-02-28 12:34:56", -2203932304000000LL);
  }
  {
    AssertNano("2018-11-13 17:11:10", 1542129070000000000LL);
    AssertNano("2018-11-13T17:11:10Z", 1542129070000000000LL);
    AssertNano("1900-02-28 12:34:56", -2203932304000000000LL);
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

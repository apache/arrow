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

#include <cmath>
#include <locale>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/formatting.h"

namespace arrow {

using internal::FormatValue;
using internal::FormatValueTraits;

template <typename ARROW_TYPE, typename Formatter = FormatValueTraits<ARROW_TYPE>,
          typename V = typename Formatter::value_type>
void AssertFormat(const ARROW_TYPE& type, V value, const std::string& expected) {
  ASSERT_EQ(FormatValue(type, value), expected)
      << "Formatting failed (value = " << value << ")";
}

TEST(Formatting, Boolean) {
  BooleanType type;

  AssertFormat(type, true, "true");
  AssertFormat(type, false, "false");
}

template <typename ARROW_TYPE>
void TestAnyIntUpTo8(const ARROW_TYPE& type) {
  AssertFormat(type, 0, "0");
  AssertFormat(type, 1, "1");
  AssertFormat(type, 9, "9");
  AssertFormat(type, 10, "10");
  AssertFormat(type, 99, "99");
  AssertFormat(type, 100, "100");
  AssertFormat(type, 127, "127");
}

template <typename ARROW_TYPE>
void TestAnyIntUpTo16(const ARROW_TYPE& type) {
  TestAnyIntUpTo8(type);
  AssertFormat(type, 999, "999");
  AssertFormat(type, 1000, "1000");
  AssertFormat(type, 9999, "9999");
  AssertFormat(type, 10000, "10000");
  AssertFormat(type, 32767, "32767");
}

template <typename ARROW_TYPE>
void TestAnyIntUpTo32(const ARROW_TYPE& type) {
  TestAnyIntUpTo16(type);
  AssertFormat(type, 99999, "99999");
  AssertFormat(type, 100000, "100000");
  AssertFormat(type, 999999, "999999");
  AssertFormat(type, 1000000, "1000000");
  AssertFormat(type, 9999999, "9999999");
  AssertFormat(type, 10000000, "10000000");
  AssertFormat(type, 99999999, "99999999");
  AssertFormat(type, 100000000, "100000000");
  AssertFormat(type, 999999999, "999999999");
  AssertFormat(type, 1000000000, "1000000000");
  AssertFormat(type, 1234567890, "1234567890");
  AssertFormat(type, 2147483647, "2147483647");
}

template <typename ARROW_TYPE>
void TestAnyIntUpTo64(const ARROW_TYPE& type) {
  TestAnyIntUpTo32(type);
  AssertFormat(type, 9999999999ULL, "9999999999");
  AssertFormat(type, 10000000000ULL, "10000000000");
  AssertFormat(type, 99999999999ULL, "99999999999");
  AssertFormat(type, 100000000000ULL, "100000000000");
  AssertFormat(type, 999999999999ULL, "999999999999");
  AssertFormat(type, 1000000000000ULL, "1000000000000");
  AssertFormat(type, 9999999999999ULL, "9999999999999");
  AssertFormat(type, 10000000000000ULL, "10000000000000");
  AssertFormat(type, 99999999999999ULL, "99999999999999");
  AssertFormat(type, 1000000000000000000ULL, "1000000000000000000");
  AssertFormat(type, 9223372036854775807ULL, "9223372036854775807");
}

template <typename ARROW_TYPE>
void TestUIntUpTo8(const ARROW_TYPE& type) {
  TestAnyIntUpTo8(type);
  AssertFormat(type, 128, "128");
  AssertFormat(type, 255, "255");
}

template <typename ARROW_TYPE>
void TestUIntUpTo16(const ARROW_TYPE& type) {
  TestAnyIntUpTo16(type);
  AssertFormat(type, 32768, "32768");
  AssertFormat(type, 65535, "65535");
}

template <typename ARROW_TYPE>
void TestUIntUpTo32(const ARROW_TYPE& type) {
  TestAnyIntUpTo32(type);
  AssertFormat(type, 2147483648U, "2147483648");
  AssertFormat(type, 4294967295U, "4294967295");
}

template <typename ARROW_TYPE>
void TestUIntUpTo64(const ARROW_TYPE& type) {
  TestAnyIntUpTo64(type);
  AssertFormat(type, 9999999999999999999ULL, "9999999999999999999");
  AssertFormat(type, 10000000000000000000ULL, "10000000000000000000");
  AssertFormat(type, 12345678901234567890ULL, "12345678901234567890");
  AssertFormat(type, 18446744073709551615ULL, "18446744073709551615");
}

TEST(Formatting, UInt8) { TestUIntUpTo8(UInt8Type{}); }

TEST(Formatting, UInt16) { TestUIntUpTo16(UInt16Type{}); }

TEST(Formatting, UInt32) { TestUIntUpTo32(UInt32Type{}); }

TEST(Formatting, UInt64) { TestUIntUpTo64(UInt64Type{}); }

template <typename ARROW_TYPE>
void TestIntUpTo8(const ARROW_TYPE& type) {
  TestAnyIntUpTo8(type);
  AssertFormat(type, -1, "-1");
  AssertFormat(type, -9, "-9");
  AssertFormat(type, -10, "-10");
  AssertFormat(type, -99, "-99");
  AssertFormat(type, -100, "-100");
  AssertFormat(type, -127, "-127");
  AssertFormat(type, -128, "-128");
}

template <typename ARROW_TYPE>
void TestIntUpTo16(const ARROW_TYPE& type) {
  TestAnyIntUpTo16(type);
  TestIntUpTo8(type);
  AssertFormat(type, -129, "-129");
  AssertFormat(type, -999, "-999");
  AssertFormat(type, -1000, "-1000");
  AssertFormat(type, -9999, "-9999");
  AssertFormat(type, -10000, "-10000");
  AssertFormat(type, -32768, "-32768");
}

template <typename ARROW_TYPE>
void TestIntUpTo32(const ARROW_TYPE& type) {
  TestAnyIntUpTo32(type);
  TestIntUpTo16(type);
  AssertFormat(type, -32769, "-32769");
  AssertFormat(type, -99999, "-99999");
  AssertFormat(type, -1000000000, "-1000000000");
  AssertFormat(type, -1234567890, "-1234567890");
  AssertFormat(type, -2147483647, "-2147483647");
  AssertFormat(type, -2147483647 - 1, "-2147483648");
}

template <typename ARROW_TYPE>
void TestIntUpTo64(const ARROW_TYPE& type) {
  TestAnyIntUpTo64(type);
  TestIntUpTo32(type);
  AssertFormat(type, -2147483649LL, "-2147483649");
  AssertFormat(type, -9999999999LL, "-9999999999");
  AssertFormat(type, -1000000000000000000LL, "-1000000000000000000");
  AssertFormat(type, -9012345678901234567LL, "-9012345678901234567");
  AssertFormat(type, -9223372036854775807LL, "-9223372036854775807");
  AssertFormat(type, -9223372036854775807LL - 1, "-9223372036854775808");
}

TEST(Formatting, Int8) { TestIntUpTo8(Int8Type{}); }

TEST(Formatting, Int16) { TestIntUpTo16(Int16Type{}); }

TEST(Formatting, Int32) { TestIntUpTo32(Int32Type{}); }

TEST(Formatting, Int64) { TestIntUpTo64(Int64Type{}); }

TEST(Formatting, Float) {
  FloatType type;

  AssertFormat(type, 0.0f, "0");
  AssertFormat(type, -0.0f, "-0");
  AssertFormat(type, 1.5f, "1.5");
  AssertFormat(type, 0.0001f, "0.0001");
  AssertFormat(type, 1234.567f, "1234.567");
  AssertFormat(type, 1e9f, "1000000000");
  AssertFormat(type, 1e10f, "1e+10");
  AssertFormat(type, 1e20f, "1e+20");
  AssertFormat(type, 1e-6f, "0.000001");
  AssertFormat(type, 1e-7f, "1e-7");
  AssertFormat(type, 1e-20f, "1e-20");

  AssertFormat(type, std::nanf(""), "nan");
  AssertFormat(type, HUGE_VALF, "inf");
  AssertFormat(type, -HUGE_VALF, "-inf");
}

TEST(Formatting, Double) {
  DoubleType type;

  AssertFormat(type, 0.0, "0");
  AssertFormat(type, -0.0, "-0");
  AssertFormat(type, 1.5, "1.5");
  AssertFormat(type, 0.0001, "0.0001");
  AssertFormat(type, 1234.567, "1234.567");
  AssertFormat(type, 1e9, "1000000000");
  AssertFormat(type, 1e10, "1e+10");
  AssertFormat(type, 1e20, "1e+20");
  AssertFormat(type, 1e-6, "0.000001");
  AssertFormat(type, 1e-7, "1e-7");
  AssertFormat(type, 1e-20, "1e-20");

  AssertFormat(type, std::nan(""), "nan");
  AssertFormat(type, HUGE_VAL, "inf");
  AssertFormat(type, -HUGE_VAL, "-inf");
}

TEST(Formatting, Date32) {
  Date32Type type;

  AssertFormat(type, 0, "1970-01-01");
  AssertFormat(type, 1, "1970-01-02");
  AssertFormat(type, 30, "1970-01-31");
  AssertFormat(type, 30 + 1, "1970-02-01");
  AssertFormat(type, 30 + 28, "1970-02-28");
  AssertFormat(type, 30 + 28 + 1, "1970-03-01");
  AssertFormat(type, -1, "1969-12-31");
  AssertFormat(type, 365, "1971-01-01");
  AssertFormat(type, 2 * 365, "1972-01-01");
  AssertFormat(type, 2 * 365 + 30 + 28 + 1, "1972-02-29");
}

TEST(Formatting, Date64) {
  constexpr int64_t kMillisInDay = 24 * 60 * 60 * 1000;

  Date64Type type;
  AssertFormat(type, kMillisInDay * (0), "1970-01-01");
  AssertFormat(type, kMillisInDay * (1), "1970-01-02");
  AssertFormat(type, kMillisInDay * (30), "1970-01-31");
  AssertFormat(type, kMillisInDay * (30 + 1), "1970-02-01");
  AssertFormat(type, kMillisInDay * (30 + 28), "1970-02-28");
  AssertFormat(type, kMillisInDay * (30 + 28 + 1), "1970-03-01");
  AssertFormat(type, kMillisInDay * (-1), "1969-12-31");
  AssertFormat(type, kMillisInDay * (365), "1971-01-01");
  AssertFormat(type, kMillisInDay * (2 * 365), "1972-01-01");
  AssertFormat(type, kMillisInDay * (2 * 365 + 30 + 28 + 1), "1972-02-29");
}

TEST(Formatting, Time32) {
  {
    Time32Type type(TimeUnit::SECOND);

    AssertFormat(type, 0, "00:00:00");
    AssertFormat(type, 1, "00:00:01");
    AssertFormat(type, ((12) * 60 + 34) * 60 + 56, "12:34:56");
    AssertFormat(type, 24 * 60 * 60 - 1, "23:59:59");
  }

  {
    Time32Type type(TimeUnit::MILLI);

    AssertFormat(type, 0, "00:00:00.000");
    AssertFormat(type, 1, "00:00:00.001");
    AssertFormat(type, 1000, "00:00:01.000");
    AssertFormat(type, (((12) * 60 + 34) * 60 + 56) * 1000 + 789, "12:34:56.789");
    AssertFormat(type, 24 * 60 * 60 * 1000 - 1, "23:59:59.999");
  }
}

TEST(Formatting, Time64) {
  {
    Time64Type type(TimeUnit::MICRO);

    AssertFormat(type, 0, "00:00:00.000000");
    AssertFormat(type, 1, "00:00:00.000001");
    AssertFormat(type, 1000000, "00:00:01.000000");
    AssertFormat(type, (((12) * 60 + 34) * 60 + 56) * 1000000LL + 789000,
                 "12:34:56.789000");
    AssertFormat(type, (24 * 60 * 60) * 1000000LL - 1, "23:59:59.999999");
  }

  {
    Time64Type type(TimeUnit::NANO);

    AssertFormat(type, 0, "00:00:00.000000000");
    AssertFormat(type, 1, "00:00:00.000000001");
    AssertFormat(type, 1000000000LL, "00:00:01.000000000");
    AssertFormat(type, (((12) * 60 + 34) * 60 + 56) * 1000000000LL + 789000000LL,
                 "12:34:56.789000000");
    AssertFormat(type, (24 * 60 * 60) * 1000000000LL - 1, "23:59:59.999999999");
  }
}

TEST(Formatting, Timestamp) {
  {
    TimestampType type(TimeUnit::SECOND);

    AssertFormat(type, 0, "1970-01-01 00:00:00");
    AssertFormat(type, 1, "1970-01-01 00:00:01");
    AssertFormat(type, 24 * 60 * 60, "1970-01-02 00:00:00");
    AssertFormat(type, 616377600, "1989-07-14 00:00:00");
    AssertFormat(type, 951782400, "2000-02-29 00:00:00");
    AssertFormat(type, 63730281600LL, "3989-07-14 00:00:00");
    AssertFormat(type, -2203977600LL, "1900-02-28 00:00:00");

    AssertFormat(type, 1542129070, "2018-11-13 17:11:10");
    AssertFormat(type, -2203932304LL, "1900-02-28 12:34:56");
  }

  {
    TimestampType type(TimeUnit::MILLI);

    AssertFormat(type, 0, "1970-01-01 00:00:00.000");
    AssertFormat(type, 1000L + 1, "1970-01-01 00:00:01.001");
    AssertFormat(type, 24 * 60 * 60 * 1000LL + 2, "1970-01-02 00:00:00.002");
    AssertFormat(type, 616377600 * 1000LL + 3, "1989-07-14 00:00:00.003");
    AssertFormat(type, 951782400 * 1000LL + 4, "2000-02-29 00:00:00.004");
    AssertFormat(type, 63730281600LL * 1000LL + 5, "3989-07-14 00:00:00.005");
    AssertFormat(type, -2203977600LL * 1000LL + 6, "1900-02-28 00:00:00.006");

    AssertFormat(type, 1542129070LL * 1000LL + 7, "2018-11-13 17:11:10.007");
    AssertFormat(type, -2203932304LL * 1000LL + 8, "1900-02-28 12:34:56.008");
  }

  {
    TimestampType type(TimeUnit::MICRO);

    AssertFormat(type, 0, "1970-01-01 00:00:00.000000");
    AssertFormat(type, 1000000LL + 1, "1970-01-01 00:00:01.000001");
    AssertFormat(type, 24 * 60 * 60 * 1000000LL + 2, "1970-01-02 00:00:00.000002");
    AssertFormat(type, 616377600 * 1000000LL + 3, "1989-07-14 00:00:00.000003");
    AssertFormat(type, 951782400 * 1000000LL + 4, "2000-02-29 00:00:00.000004");
    AssertFormat(type, 63730281600LL * 1000000LL + 5, "3989-07-14 00:00:00.000005");
    AssertFormat(type, -2203977600LL * 1000000LL + 6, "1900-02-28 00:00:00.000006");

    AssertFormat(type, 1542129070 * 1000000LL + 7, "2018-11-13 17:11:10.000007");
    AssertFormat(type, -2203932304LL * 1000000LL + 8, "1900-02-28 12:34:56.000008");
  }

  {
    TimestampType type(TimeUnit::NANO);

    AssertFormat(type, 0, "1970-01-01 00:00:00.000000000");
    AssertFormat(type, 1000000000LL + 1, "1970-01-01 00:00:01.000000001");
    AssertFormat(type, 24 * 60 * 60 * 1000000000LL + 2, "1970-01-02 00:00:00.000000002");
    AssertFormat(type, 616377600 * 1000000000LL + 3, "1989-07-14 00:00:00.000000003");
    AssertFormat(type, 951782400 * 1000000000LL + 4, "2000-02-29 00:00:00.000000004");
    AssertFormat(type, -2203977600LL * 1000000000LL + 6, "1900-02-28 00:00:00.000000006");

    AssertFormat(type, 1542129070 * 1000000000LL + 7, "2018-11-13 17:11:10.000000007");
    AssertFormat(type, -2203932304LL * 1000000000LL + 8, "1900-02-28 12:34:56.000000008");
  }
}

}  // namespace arrow

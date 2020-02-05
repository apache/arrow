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

using internal::StringFormatter;

class StringAppender {
 public:
  Status operator()(util::string_view v) {
    string_.append(v.data(), v.size());
    return Status::OK();
  }

  std::string string() const { return string_; }

 protected:
  std::string string_;
};

template <typename FormatterType, typename C_TYPE = typename FormatterType::value_type>
void AssertFormatting(FormatterType& formatter, C_TYPE value,
                      const std::string& expected) {
  StringAppender appender;
  ASSERT_OK(formatter(value, appender));
  ASSERT_EQ(appender.string(), expected) << "Formatting failed (value = " << value << ")";
}

TEST(Formatting, Boolean) {
  StringFormatter<BooleanType> formatter;

  AssertFormatting(formatter, true, "true");
  AssertFormatting(formatter, false, "false");
}

template <typename FormatterType>
void TestAnyIntUpTo8(FormatterType& formatter) {
  AssertFormatting(formatter, 0, "0");
  AssertFormatting(formatter, 1, "1");
  AssertFormatting(formatter, 9, "9");
  AssertFormatting(formatter, 10, "10");
  AssertFormatting(formatter, 99, "99");
  AssertFormatting(formatter, 100, "100");
  AssertFormatting(formatter, 127, "127");
}

template <typename FormatterType>
void TestAnyIntUpTo16(FormatterType& formatter) {
  TestAnyIntUpTo8(formatter);
  AssertFormatting(formatter, 999, "999");
  AssertFormatting(formatter, 1000, "1000");
  AssertFormatting(formatter, 9999, "9999");
  AssertFormatting(formatter, 10000, "10000");
  AssertFormatting(formatter, 32767, "32767");
}

template <typename FormatterType>
void TestAnyIntUpTo32(FormatterType& formatter) {
  TestAnyIntUpTo16(formatter);
  AssertFormatting(formatter, 99999, "99999");
  AssertFormatting(formatter, 100000, "100000");
  AssertFormatting(formatter, 999999, "999999");
  AssertFormatting(formatter, 1000000, "1000000");
  AssertFormatting(formatter, 9999999, "9999999");
  AssertFormatting(formatter, 10000000, "10000000");
  AssertFormatting(formatter, 99999999, "99999999");
  AssertFormatting(formatter, 100000000, "100000000");
  AssertFormatting(formatter, 999999999, "999999999");
  AssertFormatting(formatter, 1000000000, "1000000000");
  AssertFormatting(formatter, 1234567890, "1234567890");
  AssertFormatting(formatter, 2147483647, "2147483647");
}

template <typename FormatterType>
void TestAnyIntUpTo64(FormatterType& formatter) {
  TestAnyIntUpTo32(formatter);
  AssertFormatting(formatter, 9999999999ULL, "9999999999");
  AssertFormatting(formatter, 10000000000ULL, "10000000000");
  AssertFormatting(formatter, 99999999999ULL, "99999999999");
  AssertFormatting(formatter, 100000000000ULL, "100000000000");
  AssertFormatting(formatter, 999999999999ULL, "999999999999");
  AssertFormatting(formatter, 1000000000000ULL, "1000000000000");
  AssertFormatting(formatter, 9999999999999ULL, "9999999999999");
  AssertFormatting(formatter, 10000000000000ULL, "10000000000000");
  AssertFormatting(formatter, 99999999999999ULL, "99999999999999");
  AssertFormatting(formatter, 1000000000000000000ULL, "1000000000000000000");
  AssertFormatting(formatter, 9223372036854775807ULL, "9223372036854775807");
}

template <typename FormatterType>
void TestUIntUpTo8(FormatterType& formatter) {
  TestAnyIntUpTo8(formatter);
  AssertFormatting(formatter, 128, "128");
  AssertFormatting(formatter, 255, "255");
}

template <typename FormatterType>
void TestUIntUpTo16(FormatterType& formatter) {
  TestAnyIntUpTo16(formatter);
  AssertFormatting(formatter, 32768, "32768");
  AssertFormatting(formatter, 65535, "65535");
}

template <typename FormatterType>
void TestUIntUpTo32(FormatterType& formatter) {
  TestAnyIntUpTo32(formatter);
  AssertFormatting(formatter, 2147483648U, "2147483648");
  AssertFormatting(formatter, 4294967295U, "4294967295");
}

template <typename FormatterType>
void TestUIntUpTo64(FormatterType& formatter) {
  TestAnyIntUpTo64(formatter);
  AssertFormatting(formatter, 9999999999999999999ULL, "9999999999999999999");
  AssertFormatting(formatter, 10000000000000000000ULL, "10000000000000000000");
  AssertFormatting(formatter, 12345678901234567890ULL, "12345678901234567890");
  AssertFormatting(formatter, 18446744073709551615ULL, "18446744073709551615");
}

TEST(Formatting, UInt8) {
  StringFormatter<UInt8Type> formatter;

  TestUIntUpTo8(formatter);
}

TEST(Formatting, UInt16) {
  StringFormatter<UInt16Type> formatter;

  TestUIntUpTo16(formatter);
}

TEST(Formatting, UInt32) {
  StringFormatter<UInt32Type> formatter;

  TestUIntUpTo32(formatter);
}

TEST(Formatting, UInt64) {
  StringFormatter<UInt64Type> formatter;

  TestUIntUpTo64(formatter);
}

template <typename FormatterType>
void TestIntUpTo8(FormatterType& formatter) {
  TestAnyIntUpTo8(formatter);
  AssertFormatting(formatter, -1, "-1");
  AssertFormatting(formatter, -9, "-9");
  AssertFormatting(formatter, -10, "-10");
  AssertFormatting(formatter, -99, "-99");
  AssertFormatting(formatter, -100, "-100");
  AssertFormatting(formatter, -127, "-127");
  AssertFormatting(formatter, -128, "-128");
}

template <typename FormatterType>
void TestIntUpTo16(FormatterType& formatter) {
  TestAnyIntUpTo16(formatter);
  TestIntUpTo8(formatter);
  AssertFormatting(formatter, -129, "-129");
  AssertFormatting(formatter, -999, "-999");
  AssertFormatting(formatter, -1000, "-1000");
  AssertFormatting(formatter, -9999, "-9999");
  AssertFormatting(formatter, -10000, "-10000");
  AssertFormatting(formatter, -32768, "-32768");
}

template <typename FormatterType>
void TestIntUpTo32(FormatterType& formatter) {
  TestAnyIntUpTo32(formatter);
  TestIntUpTo16(formatter);
  AssertFormatting(formatter, -32769, "-32769");
  AssertFormatting(formatter, -99999, "-99999");
  AssertFormatting(formatter, -1000000000, "-1000000000");
  AssertFormatting(formatter, -1234567890, "-1234567890");
  AssertFormatting(formatter, -2147483647, "-2147483647");
  AssertFormatting(formatter, -2147483647 - 1, "-2147483648");
}

template <typename FormatterType>
void TestIntUpTo64(FormatterType& formatter) {
  TestAnyIntUpTo64(formatter);
  TestIntUpTo32(formatter);
  AssertFormatting(formatter, -2147483649LL, "-2147483649");
  AssertFormatting(formatter, -9999999999LL, "-9999999999");
  AssertFormatting(formatter, -1000000000000000000LL, "-1000000000000000000");
  AssertFormatting(formatter, -9012345678901234567LL, "-9012345678901234567");
  AssertFormatting(formatter, -9223372036854775807LL, "-9223372036854775807");
  AssertFormatting(formatter, -9223372036854775807LL - 1, "-9223372036854775808");
}

TEST(Formatting, Int8) {
  StringFormatter<Int8Type> formatter;

  TestIntUpTo8(formatter);
}

TEST(Formatting, Int16) {
  StringFormatter<Int16Type> formatter;

  TestIntUpTo16(formatter);
}

TEST(Formatting, Int32) {
  StringFormatter<Int32Type> formatter;

  TestIntUpTo32(formatter);
}

TEST(Formatting, Int64) {
  StringFormatter<Int64Type> formatter;

  TestIntUpTo64(formatter);
}

TEST(Formatting, Float) {
  StringFormatter<FloatType> formatter;

  AssertFormatting(formatter, 0.0f, "0");
  AssertFormatting(formatter, -0.0f, "-0");
  AssertFormatting(formatter, 1.5f, "1.5");
  AssertFormatting(formatter, 0.0001f, "0.0001");
  AssertFormatting(formatter, 1234.567f, "1234.567");
  AssertFormatting(formatter, 1e9f, "1000000000");
  AssertFormatting(formatter, 1e10f, "1e+10");
  AssertFormatting(formatter, 1e20f, "1e+20");
  AssertFormatting(formatter, 1e-6f, "0.000001");
  AssertFormatting(formatter, 1e-7f, "1e-7");
  AssertFormatting(formatter, 1e-20f, "1e-20");

  AssertFormatting(formatter, std::nanf(""), "nan");
  AssertFormatting(formatter, HUGE_VALF, "inf");
  AssertFormatting(formatter, -HUGE_VALF, "-inf");
}

TEST(Formatting, Double) {
  StringFormatter<DoubleType> formatter;

  AssertFormatting(formatter, 0.0, "0");
  AssertFormatting(formatter, -0.0, "-0");
  AssertFormatting(formatter, 1.5, "1.5");
  AssertFormatting(formatter, 0.0001, "0.0001");
  AssertFormatting(formatter, 1234.567, "1234.567");
  AssertFormatting(formatter, 1e9, "1000000000");
  AssertFormatting(formatter, 1e10, "1e+10");
  AssertFormatting(formatter, 1e20, "1e+20");
  AssertFormatting(formatter, 1e-6, "0.000001");
  AssertFormatting(formatter, 1e-7, "1e-7");
  AssertFormatting(formatter, 1e-20, "1e-20");

  AssertFormatting(formatter, std::nan(""), "nan");
  AssertFormatting(formatter, HUGE_VAL, "inf");
  AssertFormatting(formatter, -HUGE_VAL, "-inf");
}

}  // namespace arrow

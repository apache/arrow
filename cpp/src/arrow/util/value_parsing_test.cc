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
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/float16.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using util::Float16;

namespace internal {

template <typename T>
void AssertValueEquals(T a, T b) {
  ASSERT_EQ(a, b);
}

template <>
void AssertValueEquals<float>(float a, float b) {
  ASSERT_EQ(a, b);
  ASSERT_EQ(std::signbit(a), std::signbit(b));
}

template <>
void AssertValueEquals<double>(double a, double b) {
  ASSERT_EQ(a, b);
  ASSERT_EQ(std::signbit(a), std::signbit(b));
}

template <typename T>
void AssertConversion(StringConverter<T>* converter, const T& type, const std::string& s,
                      typename T::c_type expected) {
  ARROW_SCOPED_TRACE("When converting: '", s, "', expecting: ", expected);
  typename T::c_type out{};
  ASSERT_TRUE(converter->Convert(type, s.data(), s.length(), &out));
  AssertValueEquals(out, expected);
}

template <typename T>
void AssertConversion(StringConverter<T>* converter, const std::string& s,
                      typename T::c_type expected) {
  auto type = checked_pointer_cast<T>(TypeTraits<T>::type_singleton());
  AssertConversion(converter, *type, s, expected);
}

template <typename T>
void AssertConversion(const T& type, const std::string& s, typename T::c_type expected) {
  ARROW_SCOPED_TRACE("When converting: '", s, "', expecting: ", expected);
  typename T::c_type out{};
  ASSERT_TRUE(ParseValue(type, s.data(), s.length(), &out));
  AssertValueEquals(out, expected);
}

template <typename T>
void AssertConversion(const std::string& s, typename T::c_type expected) {
  auto type = checked_pointer_cast<T>(TypeTraits<T>::type_singleton());
  AssertConversion(*type, s, expected);
}

template <typename T>
void AssertConversionFails(StringConverter<T>* converter, const T& type,
                           const std::string& s) {
  typename T::c_type out{};
  ASSERT_FALSE(converter->Convert(type, s.data(), s.length(), &out))
      << "Conversion should have failed for '" << s << "' (returned " << out << ")";
}

template <typename T>
void AssertConversionFails(StringConverter<T>* converter, const std::string& s) {
  auto type = checked_pointer_cast<T>(TypeTraits<T>::type_singleton());
  AssertConversionFails(converter, *type, s);
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
  AssertConversion<FloatType>("-0.0", -0.0f);
  AssertConversion<FloatType>("-1e20", -1e20f);
  AssertConversion<FloatType>("+Infinity", std::numeric_limits<float>::infinity());
  AssertConversion<FloatType>("-Infinity", -std::numeric_limits<float>::infinity());
  AssertConversion<FloatType>("Infinity", std::numeric_limits<float>::infinity());

  AssertConversionFails<FloatType>("");
  AssertConversionFails<FloatType>("e");
  AssertConversionFails<FloatType>("1,5");

  StringConverter<FloatType> converter(/*decimal_point=*/',');
  AssertConversion(&converter, "1,5", 1.5f);
  AssertConversion(&converter, "0", 0.0f);
  AssertConversionFails(&converter, "1.5");
}

TEST(StringConversion, ToDouble) {
  AssertConversion<DoubleType>("1.5", 1.5);
  AssertConversion<DoubleType>("0", 0);
  AssertConversion<DoubleType>("-0.0", -0.0);
  AssertConversion<DoubleType>("-1e100", -1e100);
  AssertConversion<DoubleType>("+Infinity", std::numeric_limits<double>::infinity());
  AssertConversion<DoubleType>("-Infinity", -std::numeric_limits<double>::infinity());
  AssertConversion<DoubleType>("Infinity", std::numeric_limits<double>::infinity());

  AssertConversionFails<DoubleType>("");
  AssertConversionFails<DoubleType>("e");
  AssertConversionFails<DoubleType>("1,5");

  StringConverter<DoubleType> converter(/*decimal_point=*/',');
  AssertConversion(&converter, "1,5", 1.5);
  AssertConversion(&converter, "0", 0.0);
  AssertConversionFails(&converter, "1.5");
}

TEST(StringConversion, ToHalfFloat) {
  AssertConversion<HalfFloatType>("1.5", Float16(1.5f).bits());
  AssertConversion<HalfFloatType>("0", Float16(0.0f).bits());
  AssertConversion<HalfFloatType>("-0.0", Float16(-0.0f).bits());
  AssertConversion<HalfFloatType>("-1e15", Float16(-1e15).bits());
  AssertConversion<HalfFloatType>("+Infinity", 0x7c00);
  AssertConversion<HalfFloatType>("-Infinity", 0xfc00);
  AssertConversion<HalfFloatType>("Infinity", 0x7c00);

  AssertConversionFails<HalfFloatType>("");
  AssertConversionFails<HalfFloatType>("e");
  AssertConversionFails<HalfFloatType>("1,5");

  StringConverter<HalfFloatType> converter(/*decimal_point=*/',');
  AssertConversion(&converter, "1,5", Float16(1.5f).bits());
  AssertConversion(&converter, "0", Float16(0.0f).bits());
  AssertConversionFails(&converter, "1.5");
}

#if !defined(_WIN32) || defined(NDEBUG)

TEST(StringConversion, ToFloatLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<FloatType>("1.5", 1.5f);
  AssertConversionFails<FloatType>("1,5");

  StringConverter<FloatType> converter(/*decimal_point=*/'#');
  AssertConversion(&converter, "1#5", 1.5f);
  AssertConversionFails(&converter, "1.5");
  AssertConversionFails(&converter, "1,5");
}

TEST(StringConversion, ToDoubleLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<DoubleType>("1.5", 1.5);
  AssertConversionFails<DoubleType>("1,5");

  StringConverter<DoubleType> converter(/*decimal_point=*/'#');
  AssertConversion(&converter, "1#5", 1.5);
  AssertConversionFails(&converter, "1.5");
  AssertConversionFails(&converter, "1,5");
}

TEST(StringConversion, ToHalfFloatLocale) {
  // French locale uses the comma as decimal point
  LocaleGuard locale_guard("fr_FR.UTF-8");

  AssertConversion<HalfFloatType>("1.5", Float16(1.5).bits());
  AssertConversionFails<HalfFloatType>("1,5");

  StringConverter<HalfFloatType> converter(/*decimal_point=*/'#');
  AssertConversion(&converter, "1#5", Float16(1.5).bits());
  AssertConversionFails(&converter, "1.5");
  AssertConversionFails(&converter, "1,5");
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

  // Hex
  AssertConversion<Int8Type>("0x0", 0);
  AssertConversion<Int8Type>("0X1A", 26);
  AssertConversion<Int8Type>("0xb", 11);
  AssertConversion<Int8Type>("0x7F", 127);
  AssertConversion<Int8Type>("0xFF", -1);
  AssertConversionFails<Int8Type>("0x");
  AssertConversionFails<Int8Type>("0x100");
  AssertConversionFails<Int8Type>("0x1g");
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

  // Hex
  AssertConversion<UInt8Type>("0x0", 0);
  AssertConversion<UInt8Type>("0x1A", 26);
  AssertConversion<UInt8Type>("0xb", 11);
  AssertConversion<UInt8Type>("0x7F", 127);
  AssertConversion<UInt8Type>("0xFF", 255);
  AssertConversionFails<UInt8Type>("0x");
  AssertConversionFails<UInt8Type>("0x100");
  AssertConversionFails<UInt8Type>("0x1g");
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

  // Hex
  AssertConversion<Int16Type>("0x0", 0);
  AssertConversion<Int16Type>("0X1aA", 426);
  AssertConversion<Int16Type>("0xb", 11);
  AssertConversion<Int16Type>("0x7ffF", 32767);
  AssertConversion<Int16Type>("0XfffF", -1);
  AssertConversionFails<Int16Type>("0x");
  AssertConversionFails<Int16Type>("0x10000");
  AssertConversionFails<Int16Type>("0x1g");
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

  // Hex
  AssertConversion<UInt16Type>("0x0", 0);
  AssertConversion<UInt16Type>("0x1aA", 426);
  AssertConversion<UInt16Type>("0xb", 11);
  AssertConversion<UInt16Type>("0x7ffF", 32767);
  AssertConversion<UInt16Type>("0xFffF", 65535);
  AssertConversionFails<UInt16Type>("0x");
  AssertConversionFails<UInt16Type>("0x10000");
  AssertConversionFails<UInt16Type>("0x1g");
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

  // Hex
  AssertConversion<Int32Type>("0x0", 0);
  AssertConversion<Int32Type>("0x123ABC", 1194684);
  AssertConversion<Int32Type>("0xA4B35", 674613);
  AssertConversion<Int32Type>("0x7FFFFFFF", 2147483647);
  AssertConversion<Int32Type>("0x123abc", 1194684);
  AssertConversion<Int32Type>("0xA4b35", 674613);
  AssertConversion<Int32Type>("0x7FFFfFfF", 2147483647);
  AssertConversion<Int32Type>("0XFFFFfFfF", -1);
  AssertConversionFails<Int32Type>("0X");
  AssertConversionFails<Int32Type>("0x23512ak");
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

  // Hex
  AssertConversion<UInt32Type>("0x0", 0);
  AssertConversion<UInt32Type>("0x123ABC", 1194684);
  AssertConversion<UInt32Type>("0xA4B35", 674613);
  AssertConversion<UInt32Type>("0x7FFFFFFF", 2147483647);
  AssertConversion<UInt32Type>("0x123abc", 1194684);
  AssertConversion<UInt32Type>("0xA4b35", 674613);
  AssertConversion<UInt32Type>("0x7FFFfFfF", 2147483647);
  AssertConversion<UInt32Type>("0XFFFFfFfF", 4294967295);
  AssertConversionFails<UInt32Type>("0X");
  AssertConversionFails<UInt32Type>("0x23512ak");
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

  // Hex
  AssertConversion<Int64Type>("0x0", 0);
  AssertConversion<Int64Type>("0x5415a123ABC123cb", 6058926048274359243);
  AssertConversion<Int64Type>("0xA4B35", 674613);
  AssertConversion<Int64Type>("0x7FFFFFFFFFFFFFFf", 9223372036854775807);
  AssertConversion<Int64Type>("0XF000000000000001", -1152921504606846975);
  AssertConversion<Int64Type>("0xfFFFFFFFFFFFFFFf", -1);
  AssertConversionFails<Int64Type>("0X");
  AssertConversionFails<Int64Type>("0x12345678901234567");
  AssertConversionFails<Int64Type>("0x23512ak");
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

  // Hex
  AssertConversion<UInt64Type>("0x0", 0);
  AssertConversion<UInt64Type>("0x5415a123ABC123cb", 6058926048274359243);
  AssertConversion<UInt64Type>("0xA4B35", 674613);
  AssertConversion<UInt64Type>("0x7FFFFFFFFFFFFFFf", 9223372036854775807);
  AssertConversion<UInt64Type>("0XF000000000000001", 17293822569102704641ULL);
  AssertConversion<UInt64Type>("0xfFFFFFFFFFFFFFFf", 18446744073709551615ULL);
  AssertConversionFails<UInt64Type>("0x");
  AssertConversionFails<UInt64Type>("0x12345678901234567");
  AssertConversionFails<UInt64Type>("0x23512ak");
}

TEST(StringConversion, ToDate32) {
  AssertConversion<Date32Type>("1970-01-01", 0);
  AssertConversion<Date32Type>("1970-01-02", 1);
  AssertConversion<Date32Type>("2020-03-15", 18336);
  AssertConversion<Date32Type>("1945-05-08", -9004);
  AssertConversion<Date32Type>("4707-11-28", 999999);
  AssertConversion<Date32Type>("0001-01-01", -719162);

  // Invalid format
  AssertConversionFails<Date32Type>("");
  AssertConversionFails<Date32Type>("1970");
  AssertConversionFails<Date32Type>("1970-01");
  AssertConversionFails<Date32Type>("1970-01-01 00:00:00");
  AssertConversionFails<Date32Type>("1970/01/01");
}

TEST(StringConversion, ToDate64) {
  AssertConversion<Date64Type>("1970-01-01", 0);
  AssertConversion<Date64Type>("1970-01-02", 86400000);
  AssertConversion<Date64Type>("2020-03-15", 1584230400000LL);
  AssertConversion<Date64Type>("1945-05-08", -777945600000LL);
  AssertConversion<Date64Type>("4707-11-28", 86399913600000LL);
  AssertConversion<Date64Type>("0001-01-01", -62135596800000LL);
}

template <typename T>
void AssertInvalidTimes(const T& type) {
  // Invalid time format
  AssertConversionFails(type, "");
  AssertConversionFails(type, "00");
  AssertConversionFails(type, "00:");
  AssertConversionFails(type, "00:00:");
  AssertConversionFails(type, "00:00:00:");
  AssertConversionFails(type, "000000");
  AssertConversionFails(type, "000000.000");

  // Invalid time value
  AssertConversionFails(type, "24:00:00");
  AssertConversionFails(type, "00:60:00");
  AssertConversionFails(type, "00:00:60");
}

TEST(StringConversion, ToTime32) {
  {
    Time32Type type{TimeUnit::SECOND};

    AssertConversion(type, "00:00", 0);
    AssertConversion(type, "01:23", 4980);
    AssertConversion(type, "23:59", 86340);

    AssertConversion(type, "00:00:00", 0);
    AssertConversion(type, "01:23:45", 5025);
    AssertConversion(type, "23:45:43", 85543);
    AssertConversion(type, "23:59:59", 86399);

    AssertInvalidTimes(type);
    // No subseconds allowed
    AssertConversionFails(type, "00:00:00.123");
  }
  {
    Time32Type type{TimeUnit::MILLI};

    AssertConversion(type, "00:00", 0);
    AssertConversion(type, "01:23", 4980000);
    AssertConversion(type, "23:59", 86340000);

    AssertConversion(type, "00:00:00", 0);
    AssertConversion(type, "01:23:45", 5025000);
    AssertConversion(type, "23:45:43", 85543000);
    AssertConversion(type, "23:59:59", 86399000);

    AssertConversion(type, "00:00:00.123", 123);
    AssertConversion(type, "01:23:45.000", 5025000);
    AssertConversion(type, "01:23:45.1", 5025100);
    AssertConversion(type, "01:23:45.123", 5025123);
    AssertConversion(type, "01:23:45.999", 5025999);

    AssertInvalidTimes(type);
    // Invalid subseconds
    AssertConversionFails(type, "00:00:00.1234");
  }
}

TEST(StringConversion, ToTime64) {
  {
    Time64Type type{TimeUnit::MICRO};

    AssertConversion(type, "00:00:00", 0LL);
    AssertConversion(type, "01:23:45", 5025000000LL);
    AssertConversion(type, "23:45:43", 85543000000LL);
    AssertConversion(type, "23:59:59", 86399000000LL);

    AssertConversion(type, "00:00:00.123456", 123456LL);
    AssertConversion(type, "01:23:45.000000", 5025000000LL);
    AssertConversion(type, "01:23:45.1", 5025100000LL);
    AssertConversion(type, "01:23:45.123", 5025123000LL);
    AssertConversion(type, "01:23:45.999999", 5025999999LL);

    AssertInvalidTimes(type);
    // Invalid subseconds
    AssertConversionFails(type, "00:00:00.1234567");
  }
  {
    Time64Type type{TimeUnit::NANO};

    AssertConversion(type, "00:00:00", 0LL);
    AssertConversion(type, "01:23:45", 5025000000000LL);
    AssertConversion(type, "23:45:43", 85543000000000LL);
    AssertConversion(type, "23:59:59", 86399000000000LL);

    AssertConversion(type, "00:00:00.123456789", 123456789LL);
    AssertConversion(type, "01:23:45.000000000", 5025000000000LL);
    AssertConversion(type, "01:23:45.1", 5025100000000LL);
    AssertConversion(type, "01:23:45.1234", 5025123400000LL);
    AssertConversion(type, "01:23:45.999999999", 5025999999999LL);

    AssertInvalidTimes(type);
    // Invalid subseconds
    AssertConversionFails(type, "00:00:00.1234567891");
  }
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
    AssertConversion(type, "2018-11-13 17+00", 1542128400);
    AssertConversion(type, "2018-11-13 17+0000", 1542128400);
    AssertConversion(type, "2018-11-13 17+00:00", 1542128400);
    AssertConversion(type, "2018-11-13 17+01", 1542124800);
    AssertConversion(type, "2018-11-13 17+0117", 1542123780);
    AssertConversion(type, "2018-11-13 17+01:17", 1542123780);
    AssertConversion(type, "2018-11-13 17-01", 1542132000);
    AssertConversion(type, "2018-11-13 17-0117", 1542133020);
    AssertConversion(type, "2018-11-13 17-01:17", 1542133020);
    AssertConversion(type, "2018-11-13T17", 1542128400);
    AssertConversion(type, "2018-11-13 17Z", 1542128400);
    AssertConversion(type, "2018-11-13T17Z", 1542128400);
    AssertConversion(type, "2018-11-13 17:11", 1542129060);
    AssertConversion(type, "2018-11-13T17:11", 1542129060);
    AssertConversion(type, "2018-11-13 17:11Z", 1542129060);
    AssertConversion(type, "2018-11-13T17:11Z", 1542129060);
    AssertConversion(type, "2018-11-13 17:11+00", 1542129060);
    AssertConversion(type, "2018-11-13 17:11+0000", 1542129060);
    AssertConversion(type, "2018-11-13 17:11+00:00", 1542129060);
    AssertConversion(type, "2018-11-13 17:11+01", 1542125460);
    AssertConversion(type, "2018-11-13 17:11+0117", 1542124440);
    AssertConversion(type, "2018-11-13 17:11+01:17", 1542124440);
    AssertConversion(type, "2018-11-13 17:11-01", 1542132660);
    AssertConversion(type, "2018-11-13 17:11-0117", 1542133680);
    AssertConversion(type, "2018-11-13 17:11-01:17", 1542133680);
    AssertConversion(type, "2018-11-13 17:11:10", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10", 1542129070);
    AssertConversion(type, "2018-11-13 17:11:10Z", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10+00", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10+0000", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10+00:00", 1542129070);
    AssertConversion(type, "2018-11-13T17:11:10+01", 1542125470);
    AssertConversion(type, "2018-11-13T17:11:10+0117", 1542124450);
    AssertConversion(type, "2018-11-13T17:11:10+01:17", 1542124450);
    AssertConversion(type, "2018-11-13T17:11:10-01", 1542132670);
    AssertConversion(type, "2018-11-13T17:11:10-0117", 1542133690);
    AssertConversion(type, "2018-11-13T17:11:10-01:17", 1542133690);
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
    // Invalid zone offsets
    AssertConversionFails(type, "1970-01-01 00:00+0");
    AssertConversionFails(type, "1970-01-01 00:00+000");
    AssertConversionFails(type, "1970-01-01 00:00+00000");
    AssertConversionFails(type, "1970-01-01 00:00+2400");
    AssertConversionFails(type, "1970-01-01 00:00+0060");
    AssertConversionFails(type, "1970-01-01 00-0");
    AssertConversionFails(type, "1970-01-01 00-000");
    AssertConversionFails(type, "1970-01-01 00+00000");
    AssertConversionFails(type, "1970-01-01 00+2400");
    AssertConversionFails(type, "1970-01-01 00+0060");
    AssertConversionFails(type, "1970-01-01 00:00:00+0");
    AssertConversionFails(type, "1970-01-01 00:00:00-000");
    AssertConversionFails(type, "1970-01-01 00:00:00-00000");
    AssertConversionFails(type, "1970-01-01 00:00:00+2400");
    AssertConversionFails(type, "1970-01-01 00:00:00+00:99");
  }
  {
    TimestampType type{TimeUnit::MILLI};

    AssertConversion(type, "2018-11-13 17:11:10", 1542129070000LL);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070000LL);
    AssertConversion(type, "3989-07-14T11:22:33Z", 63730322553000LL);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304000LL);
    AssertConversion(type, "2018-11-13T17:11:10.777Z", 1542129070777LL);

    AssertConversion(type, "1900-02-28 12:34:56.1", -2203932304000LL + 100LL);
    AssertConversion(type, "1900-02-28 12:34:56.12", -2203932304000LL + 120LL);
    AssertConversion(type, "1900-02-28 12:34:56.123", -2203932304000LL + 123LL);

    AssertConversion(type, "2018-11-13 17:11:10.123+01", 1542129070123LL - 3600000LL);
    AssertConversion(type, "2018-11-13 17:11:10.123+0117", 1542129070123LL - 4620000LL);
    AssertConversion(type, "2018-11-13 17:11:10.123+01:17", 1542129070123LL - 4620000LL);
    AssertConversion(type, "2018-11-13 17:11:10.123-01", 1542129070123LL + 3600000LL);
    AssertConversion(type, "2018-11-13 17:11:10.123-0117", 1542129070123LL + 4620000LL);
    AssertConversion(type, "2018-11-13 17:11:10.123-01:17", 1542129070123LL + 4620000LL);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1234");
    AssertConversionFails(type, "1900-02-28 12:34:56.12345");
    AssertConversionFails(type, "1900-02-28 12:34:56.123456");
    AssertConversionFails(type, "1900-02-28 12:34:56.1234567");
    AssertConversionFails(type, "1900-02-28 12:34:56.12345678");
    AssertConversionFails(type, "1900-02-28 12:34:56.123456789");
  }
  {
    TimestampType type{TimeUnit::MICRO};

    AssertConversion(type, "2018-11-13 17:11:10", 1542129070000000LL);
    AssertConversion(type, "2018-11-13T17:11:10Z", 1542129070000000LL);
    AssertConversion(type, "3989-07-14T11:22:33Z", 63730322553000000LL);
    AssertConversion(type, "1900-02-28 12:34:56", -2203932304000000LL);
    AssertConversion(type, "2018-11-13T17:11:10.777000", 1542129070777000LL);
    AssertConversion(type, "3989-07-14T11:22:33.000777Z", 63730322553000777LL);

    AssertConversion(type, "1900-02-28 12:34:56.1", -2203932304000000LL + 100000LL);
    AssertConversion(type, "1900-02-28 12:34:56.12", -2203932304000000LL + 120000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123", -2203932304000000LL + 123000LL);
    AssertConversion(type, "1900-02-28 12:34:56.1234", -2203932304000000LL + 123400LL);
    AssertConversion(type, "1900-02-28 12:34:56.12345", -2203932304000000LL + 123450LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456", -2203932304000000LL + 123456LL);

    AssertConversion(type, "1900-02-28 12:34:56.123456+01",
                     -2203932304000000LL + 123456LL - 3600000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456+0117",
                     -2203932304000000LL + 123456LL - 4620000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456+01:17",
                     -2203932304000000LL + 123456LL - 4620000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456-01",
                     -2203932304000000LL + 123456LL + 3600000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456-0117",
                     -2203932304000000LL + 123456LL + 4620000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456-01:17",
                     -2203932304000000LL + 123456LL + 4620000000LL);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1234567");
    AssertConversionFails(type, "1900-02-28 12:34:56.12345678");
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

    AssertConversion(type, "1900-02-28 12:34:56.1", -2203932304000000000LL + 100000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.12",
                     -2203932304000000000LL + 120000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123",
                     -2203932304000000000LL + 123000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.1234",
                     -2203932304000000000LL + 123400000LL);
    AssertConversion(type, "1900-02-28 12:34:56.12345",
                     -2203932304000000000LL + 123450000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456",
                     -2203932304000000000LL + 123456000LL);
    AssertConversion(type, "1900-02-28 12:34:56.1234567",
                     -2203932304000000000LL + 123456700LL);
    AssertConversion(type, "1900-02-28 12:34:56.12345678",
                     -2203932304000000000LL + 123456780LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789",
                     -2203932304000000000LL + 123456789LL);

    AssertConversion(type, "1900-02-28 12:34:56.123456789+01",
                     -2203932304000000000LL + 123456789LL - 3600000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789+0117",
                     -2203932304000000000LL + 123456789LL - 4620000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789+01:17",
                     -2203932304000000000LL + 123456789LL - 4620000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789-01",
                     -2203932304000000000LL + 123456789LL + 3600000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789-0117",
                     -2203932304000000000LL + 123456789LL + 4620000000000LL);
    AssertConversion(type, "1900-02-28 12:34:56.123456789-01:17",
                     -2203932304000000000LL + 123456789LL + 4620000000000LL);

    // Invalid subseconds
    AssertConversionFails(type, "1900-02-28 12:34:56.1234567890");
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

  for (auto unit : TimeUnit::values()) {
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

TEST(TimestampParser, StrptimeZoneOffset) {
  if (!kStrptimeSupportsZone) {
    GTEST_SKIP() << "strptime does not support %z on this platform";
  }
#ifdef __EMSCRIPTEN__
  GTEST_SKIP() << "Test temporarily disabled due to emscripten bug "
                  "https://github.com/emscripten-core/emscripten/issues/20467 ";
#endif

  std::string format = "%Y-%d-%m %H:%M:%S%z";
  auto parser = TimestampParser::MakeStrptime(format);

  // N.B. GNU %z supports ISO8601 format while BSD %z supports only
  // +HHMM or -HHMM and POSIX doesn't appear to define %z at all
  for (auto unit : TimeUnit::values()) {
    for (const std::string value :
         {"2018-01-01 00:00:00+0000", "2018-01-01 00:00:00+0100",
          "2018-01-01 00:00:00+0130", "2018-01-01 00:00:00-0117"}) {
      SCOPED_TRACE(value);
      int64_t converted = 0;
      int64_t expected = 0;
      ASSERT_TRUE((*parser)(value.c_str(), value.size(), unit, &converted));
      ASSERT_TRUE(ParseTimestampISO8601(value.c_str(), value.size(), unit, &expected));
      ASSERT_EQ(expected, converted);
    }
    for (const std::string value : {"2018-01-01 00:00:00", "2018-01-01 00:00:00EST"}) {
      SCOPED_TRACE(value);
      int64_t converted = 0;
      ASSERT_FALSE((*parser)(value.c_str(), value.size(), unit, &converted));
    }
  }
}

}  // namespace internal
}  // namespace arrow

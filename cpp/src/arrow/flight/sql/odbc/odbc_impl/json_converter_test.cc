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

#include "arrow/flight/sql/odbc/odbc_impl/json_converter.h"
#include "arrow/scalar.h"
#include "arrow/testing/builder.h"
#include "arrow/type.h"

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

TEST(ConvertToJson, String) {
  ASSERT_EQ("\"\"", ConvertToJson(StringScalar("")));
  ASSERT_EQ("\"string\"", ConvertToJson(StringScalar("string")));
  ASSERT_EQ("\"string\\\"\"", ConvertToJson(StringScalar("string\"")));
}

TEST(ConvertToJson, LargeString) {
  ASSERT_EQ("\"\"", ConvertToJson(LargeStringScalar("")));
  ASSERT_EQ("\"string\"", ConvertToJson(LargeStringScalar("string")));
  ASSERT_EQ("\"string\\\"\"", ConvertToJson(LargeStringScalar("string\"")));
}

TEST(ConvertToJson, Binary) {
  ASSERT_EQ("\"\"", ConvertToJson(BinaryScalar("")));
  ASSERT_EQ("\"c3RyaW5n\"", ConvertToJson(BinaryScalar("string")));
  ASSERT_EQ("\"c3RyaW5nIg==\"", ConvertToJson(BinaryScalar("string\"")));
}

TEST(ConvertToJson, LargeBinary) {
  ASSERT_EQ("\"\"", ConvertToJson(LargeBinaryScalar("")));
  ASSERT_EQ("\"c3RyaW5n\"", ConvertToJson(LargeBinaryScalar("string")));
  ASSERT_EQ("\"c3RyaW5nIg==\"", ConvertToJson(LargeBinaryScalar("string\"")));
}

TEST(ConvertToJson, FixedSizeBinary) {
  ASSERT_EQ("\"\"", ConvertToJson(FixedSizeBinaryScalar("")));
  ASSERT_EQ("\"c3RyaW5n\"", ConvertToJson(FixedSizeBinaryScalar("string")));
  ASSERT_EQ("\"c3RyaW5nIg==\"", ConvertToJson(FixedSizeBinaryScalar("string\"")));
}

TEST(ConvertToJson, Int8) {
  ASSERT_EQ("127", ConvertToJson(Int8Scalar(127)));
  ASSERT_EQ("-128", ConvertToJson(Int8Scalar(-128)));
}

TEST(ConvertToJson, Int16) {
  ASSERT_EQ("32767", ConvertToJson(Int16Scalar(32767)));
  ASSERT_EQ("-32768", ConvertToJson(Int16Scalar(-32768)));
}

TEST(ConvertToJson, Int32) {
  ASSERT_EQ("2147483647", ConvertToJson(Int32Scalar(2147483647)));
  // 2147483648 is not valid as a signed int, using workaround
  ASSERT_EQ("-2147483648",
            ConvertToJson(Int32Scalar(static_cast<int32_t>(-2147483647 - 1))));
}

TEST(ConvertToJson, Int64) {
  ASSERT_EQ("9223372036854775807", ConvertToJson(Int64Scalar(9223372036854775807LL)));
  // 9223372036854775808ULL is not valid as a signed int64, using workaround
  ASSERT_EQ("-9223372036854775808",
            ConvertToJson(Int64Scalar(static_cast<int64_t>(-9223372036854775807LL - 1))));
}

TEST(ConvertToJson, UInt8) {
  ASSERT_EQ("127", ConvertToJson(UInt8Scalar(127)));
  ASSERT_EQ("255", ConvertToJson(UInt8Scalar(255)));
}

TEST(ConvertToJson, UInt16) {
  ASSERT_EQ("32767", ConvertToJson(UInt16Scalar(32767)));
  ASSERT_EQ("65535", ConvertToJson(UInt16Scalar(65535)));
}

TEST(ConvertToJson, UInt32) {
  ASSERT_EQ("2147483647", ConvertToJson(UInt32Scalar(2147483647)));
  ASSERT_EQ("4294967295", ConvertToJson(UInt32Scalar(4294967295)));
}

TEST(ConvertToJson, UInt64) {
  ASSERT_EQ("9223372036854775807", ConvertToJson(UInt64Scalar(9223372036854775807LL)));
  ASSERT_EQ("18446744073709551615", ConvertToJson(UInt64Scalar(18446744073709551615ULL)));
}

TEST(ConvertToJson, Float) {
  ASSERT_EQ("1.5", ConvertToJson(FloatScalar(1.5)));
  ASSERT_EQ("-1.5", ConvertToJson(FloatScalar(-1.5)));
}

TEST(ConvertToJson, Double) {
  ASSERT_EQ("1.5", ConvertToJson(DoubleScalar(1.5)));
  ASSERT_EQ("-1.5", ConvertToJson(DoubleScalar(-1.5)));
}

TEST(ConvertToJson, Boolean) {
  ASSERT_EQ("true", ConvertToJson(BooleanScalar(true)));
  ASSERT_EQ("false", ConvertToJson(BooleanScalar(false)));
}

TEST(ConvertToJson, Null) { ASSERT_EQ("null", ConvertToJson(NullScalar())); }

TEST(ConvertToJson, Date32) {
  ASSERT_EQ("\"1969-12-31\"", ConvertToJson(Date32Scalar(-1)));
  ASSERT_EQ("\"1970-01-01\"", ConvertToJson(Date32Scalar(0)));
  ASSERT_EQ("\"2022-01-01\"", ConvertToJson(Date32Scalar(18993)));
}

TEST(ConvertToJson, Date64) {
  ASSERT_EQ("\"1969-12-31\"", ConvertToJson(Date64Scalar(-86400000)));
  ASSERT_EQ("\"1970-01-01\"", ConvertToJson(Date64Scalar(0)));
  ASSERT_EQ("\"2022-01-01\"", ConvertToJson(Date64Scalar(1640995200000)));
}

TEST(ConvertToJson, Time32) {
  ASSERT_EQ("\"00:00:00\"", ConvertToJson(Time32Scalar(0, TimeUnit::SECOND)));
  ASSERT_EQ("\"01:02:03\"", ConvertToJson(Time32Scalar(3723, TimeUnit::SECOND)));
  ASSERT_EQ("\"00:00:00.123\"", ConvertToJson(Time32Scalar(123, TimeUnit::MILLI)));
}

TEST(ConvertToJson, Time64) {
  ASSERT_EQ("\"00:00:00.123456\"", ConvertToJson(Time64Scalar(123456, TimeUnit::MICRO)));
  ASSERT_EQ("\"00:00:00.123456789\"",
            ConvertToJson(Time64Scalar(123456789, TimeUnit::NANO)));
}

TEST(ConvertToJson, Timestamp) {
  ASSERT_EQ("\"1969-12-31 00:00:00.000\"",
            ConvertToJson(TimestampScalar(-86400000, TimeUnit::MILLI)));
  ASSERT_EQ("\"1970-01-01 00:00:00.000\"",
            ConvertToJson(TimestampScalar(0, TimeUnit::MILLI)));
  ASSERT_EQ("\"2022-01-01 00:00:00.000\"",
            ConvertToJson(TimestampScalar(1640995200000, TimeUnit::MILLI)));
  ASSERT_EQ("\"2022-01-01 00:00:01.234\"",
            ConvertToJson(TimestampScalar(1640995201234, TimeUnit::MILLI)));
}

TEST(ConvertToJson, DayTimeInterval) {
  ASSERT_EQ("\"123d0ms\"", ConvertToJson(DayTimeIntervalScalar({123, 0})));
  ASSERT_EQ("\"1d234ms\"", ConvertToJson(DayTimeIntervalScalar({1, 234})));
}

TEST(ConvertToJson, MonthDayNanoInterval) {
  ASSERT_EQ("\"12M34d56ns\"", ConvertToJson(MonthDayNanoIntervalScalar({12, 34, 56})));
}

TEST(ConvertToJson, MonthInterval) {
  ASSERT_EQ("\"1M\"", ConvertToJson(MonthIntervalScalar(1)));
}

TEST(ConvertToJson, Duration) {
  // TODO: Append TimeUnit on conversion
  ASSERT_EQ("\"123\"", ConvertToJson(DurationScalar(123, TimeUnit::SECOND)));
  ASSERT_EQ("\"123\"", ConvertToJson(DurationScalar(123, TimeUnit::MILLI)));
  ASSERT_EQ("\"123\"", ConvertToJson(DurationScalar(123, TimeUnit::MICRO)));
  ASSERT_EQ("\"123\"", ConvertToJson(DurationScalar(123, TimeUnit::NANO)));
}

TEST(ConvertToJson, Lists) {
  std::vector<std::string> values = {"ABC", "DEF", "XYZ"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  const char* expected_string = R"(["ABC","DEF","XYZ"])";
  ASSERT_EQ(expected_string, ConvertToJson(ListScalar{array}));
  ASSERT_EQ(expected_string, ConvertToJson(FixedSizeListScalar{array}));
  ASSERT_EQ(expected_string, ConvertToJson(LargeListScalar{array}));

  StringBuilder builder;
  ASSERT_OK(builder.AppendNull());
  ASSERT_EQ("[null]", ConvertToJson(ListScalar{builder.Finish().ValueOrDie()}));
  ASSERT_EQ("[]", ConvertToJson(ListScalar{StringBuilder().Finish().ValueOrDie()}));
}

TEST(ConvertToJson, Struct) {
  auto i32 = MakeScalar(1);
  auto f64 = MakeScalar(2.5);
  auto str = MakeScalar("yo");
  ASSERT_OK_AND_ASSIGN(
      auto scalar,
      StructScalar::Make(
          {i32, f64, str, MakeNullScalar(std::shared_ptr<DataType>(new Date32Type()))},
          {"i", "f", "s", "null"}));
  ASSERT_EQ("{\"i\":1,\"f\":2.5,\"s\":\"yo\",\"null\":null}", ConvertToJson(*scalar));
}

}  // namespace arrow::flight::sql::odbc

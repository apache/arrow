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

#include "arrow/pretty_print.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestPrettyPrint : public ::testing::Test {
 public:
  void SetUp() {}

  void Print(const Array& array) {}

 private:
  std::ostringstream sink_;
};

template <typename T>
void CheckStream(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::ostringstream sink;
  ASSERT_OK(PrettyPrint(obj, options, &sink));
  std::string result = sink.str();
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

void CheckArray(const Array& arr, const PrettyPrintOptions& options, const char* expected,
                bool check_operator = true) {
  ARROW_SCOPED_TRACE("For datatype: ", arr.type()->ToString());
  CheckStream(arr, options, expected);

  if (options.indent == 0 && check_operator) {
    std::stringstream ss;
    ss << arr;
    std::string result = std::string(expected, strlen(expected));
    ASSERT_EQ(result, ss.str());
  }
}

template <typename T>
void Check(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::string result;
  ASSERT_OK(PrettyPrint(obj, options, &result));
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const std::shared_ptr<DataType>& type,
                    const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  std::shared_ptr<Array> array;
  ArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, &array);
  CheckArray(*array, options, expected, check_operator);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  CheckPrimitive<TYPE, C_TYPE>(TypeTraits<TYPE>::type_singleton(), options, is_valid,
                               values, expected, check_operator);
}

TEST_F(TestPrettyPrint, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10}, is_valid, values, expected);

  static const char* expected_na = R"expected([
  0,
  1,
  NA,
  3,
  NA
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10, 2, "NA"}, is_valid, values, expected_na,
                                     false);

  static const char* ex_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 10}, is_valid, values, ex_in2);

  static const char* ex_in2_w2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 2}, is_valid, values, ex_in2_w2);

  static const char* ex_in2_w1 = R"expected(  [
    0,
    ...
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 1}, is_valid, values, ex_in2_w1);

  std::vector<double> values2 = {0., 1., 2., 3., 4.};
  static const char* ex2 = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<DoubleType, double>({0, 10}, is_valid, values2, ex2);
  static const char* ex2_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<DoubleType, double>({2, 10}, is_valid, values2, ex2_in2);

  std::vector<std::string> values3 = {"foo", "bar", "", "baz", ""};
  static const char* ex3 = R"expected([
  "foo",
  "bar",
  null,
  "baz",
  null
])expected";
  CheckPrimitive<StringType, std::string>({0, 10}, is_valid, values3, ex3);
  CheckPrimitive<LargeStringType, std::string>({0, 10}, is_valid, values3, ex3);
  static const char* ex3_in2 = R"expected(  [
    "foo",
    "bar",
    null,
    "baz",
    null
  ])expected";
  CheckPrimitive<StringType, std::string>({2, 10}, is_valid, values3, ex3_in2);
  CheckPrimitive<LargeStringType, std::string>({2, 10}, is_valid, values3, ex3_in2);
}

TEST_F(TestPrettyPrint, PrimitiveTypeNoNewlines) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<int32_t> values = {0, 1, 2, 3, 4};

  PrettyPrintOptions options{};
  options.skip_new_lines = true;
  options.window = 4;

  const char* expected = "[0,1,null,3,null]";
  CheckPrimitive<Int32Type, int32_t>(options, is_valid, values, expected, false);

  // With ellipsis
  is_valid.insert(is_valid.end(), 20, true);
  is_valid.insert(is_valid.end(), {true, false, true});
  values.insert(values.end(), 20, 99);
  values.insert(values.end(), {44, 43, 42});

  expected = "[0,1,null,3,...,99,44,null,42]";
  CheckPrimitive<Int32Type, int32_t>(options, is_valid, values, expected, false);
}

TEST_F(TestPrettyPrint, ArrayCustomElementDelimiter) {
  PrettyPrintOptions options{};
  // Use a custom array element delimiter of " | ",
  // rather than the default delimiter (i.e. ",").
  options.array_delimiters.element = " | ";

  // Short array without ellipsis
  {
    std::vector<bool> is_valid = {true, true, false, true, false};
    std::vector<int32_t> values = {1, 2, 3, 4, 5};
    static const char* expected = R"expected([
  1 | 
  2 | 
  null | 
  4 | 
  null
])expected";
    CheckPrimitive<Int32Type, int32_t>(options, is_valid, values, expected, false);
  }

  // Longer array with ellipsis
  {
    std::vector<bool> is_valid = {true, false, true};
    std::vector<int32_t> values = {1, 2, 3};
    // Append 20 copies of the value "10" to the end of the values vector.
    values.insert(values.end(), 20, 10);
    // Append 20 copies of the value "true" to the end of the validity bitmap vector.
    is_valid.insert(is_valid.end(), 20, true);
    // Append the values 4, 5, and 6 to the end of the values vector.
    values.insert(values.end(), {4, 5, 6});
    // Append the values true, false, and true to the end of the validity bitmap vector.
    is_valid.insert(is_valid.end(), {true, false, true});
    static const char* expected = R"expected([
  1 | 
  null | 
  3 | 
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  ...
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  10 | 
  4 | 
  null | 
  6
])expected";
    CheckPrimitive<Int32Type, int32_t>(options, is_valid, values, expected, false);
  }
}

TEST_F(TestPrettyPrint, ArrayCustomOpenCloseDelimiter) {
  PrettyPrintOptions options{};
  // Use a custom opening Array delimiter of "{", rather than the default "]".
  options.array_delimiters.open = "{";
  // Use a custom closing Array delimiter of "}", rather than the default "]".
  options.array_delimiters.close = "}";

  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<int32_t> values = {1, 2, 3, 4, 5};
  static const char* expected = R"expected({
  1,
  2,
  null,
  4,
  null
})expected";
  CheckPrimitive<Int32Type, int32_t>(options, is_valid, values, expected, false);
}

TEST_F(TestPrettyPrint, Int8) {
  static const char* expected = R"expected([
  0,
  127,
  -128
])expected";
  CheckPrimitive<Int8Type, int8_t>({0, 10}, {true, true, true}, {0, 127, -128}, expected);
}

TEST_F(TestPrettyPrint, UInt8) {
  static const char* expected = R"expected([
  0,
  255
])expected";
  CheckPrimitive<UInt8Type, uint8_t>({0, 10}, {true, true}, {0, 255}, expected);
}

TEST_F(TestPrettyPrint, Int64) {
  static const char* expected = R"expected([
  0,
  9223372036854775807,
  -9223372036854775808
])expected";
  CheckPrimitive<Int64Type, int64_t>(
      {0, 10}, {true, true, true}, {0, 9223372036854775807LL, -9223372036854775807LL - 1},
      expected);
}

TEST_F(TestPrettyPrint, UInt64) {
  static const char* expected = R"expected([
  0,
  9223372036854775803,
  18446744073709551615
])expected";
  CheckPrimitive<UInt64Type, uint64_t>(
      {0, 10}, {true, true, true}, {0, 9223372036854775803ULL, 18446744073709551615ULL},
      expected);
}

TEST_F(TestPrettyPrint, DateTimeTypes) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  {
    std::vector<int32_t> values = {0, 1, 2, 31, 4};
    static const char* expected = R"expected([
  1970-01-01,
  1970-01-02,
  null,
  1970-02-01,
  null
])expected";
    CheckPrimitive<Date32Type, int32_t>({0, 10}, is_valid, values, expected);
  }

  {
    constexpr int64_t ms_per_day = 24 * 60 * 60 * 1000;
    std::vector<int64_t> values = {0 * ms_per_day, 1 * ms_per_day, 2 * ms_per_day,
                                   31 * ms_per_day, 4 * ms_per_day};
    static const char* expected = R"expected([
  1970-01-01,
  1970-01-02,
  null,
  1970-02-01,
  null
])expected";
    CheckPrimitive<Date64Type, int64_t>({0, 10}, is_valid, values, expected);
  }

  {
    std::vector<int64_t> values = {
        0, 1, 2, 678 + 1000000 * (5 + 60 * (4 + 60 * (3 + 24 * int64_t(1)))), 4};
    static const char* expected = R"expected([
  1970-01-01 00:00:00.000000Z,
  1970-01-01 00:00:00.000001Z,
  null,
  1970-01-02 03:04:05.000678Z,
  null
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::MICRO, "Transylvania"),
                                           {0, 10}, is_valid, values, expected);
  }

  {
    std::vector<int32_t> values = {1, 62, 2, 3 + 60 * (2 + 60 * 1), 4};
    static const char* expected = R"expected([
  00:00:01,
  00:01:02,
  null,
  01:02:03,
  null
])expected";
    CheckPrimitive<Time32Type, int32_t>(time32(TimeUnit::SECOND), {0, 10}, is_valid,
                                        values, expected);
  }

  {
    std::vector<int64_t> values = {
        0, 1, 2, 678 + int64_t(1000000000) * (5 + 60 * (4 + 60 * 3)), 4};
    static const char* expected = R"expected([
  00:00:00.000000000,
  00:00:00.000000001,
  null,
  03:04:05.000000678,
  null
])expected";
    CheckPrimitive<Time64Type, int64_t>(time64(TimeUnit::NANO), {0, 10}, is_valid, values,
                                        expected);
  }
}

TEST_F(TestPrettyPrint, TestIntervalTypes) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  {
    std::vector<DayTimeIntervalType::DayMilliseconds> values = {
        {1, 2}, {-3, 4}, {}, {}, {}};
    static const char* expected = R"expected([
  1d2ms,
  -3d4ms,
  null,
  0d0ms,
  null
])expected";
    CheckPrimitive<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
        {0, 10}, is_valid, values, expected);
  }
  {
    std::vector<MonthDayNanoIntervalType::MonthDayNanos> values = {
        {1, 2, 3}, {-3, 4, -5}, {}, {}, {}};
    static const char* expected = R"expected([
  1M2d3ns,
  -3M4d-5ns,
  null,
  0M0d0ns,
  null
])expected";
    CheckPrimitive<MonthDayNanoIntervalType, MonthDayNanoIntervalType::MonthDayNanos>(
        {0, 10}, is_valid, values, expected);
  }
}

TEST_F(TestPrettyPrint, DateTimeTypesWithOutOfRangeValues) {
  // Our vendored date library allows years within [-32767, 32767],
  // which limits the range of values which can be displayed.
  const int32_t min_int32 = std::numeric_limits<int32_t>::min();
  const int32_t max_int32 = std::numeric_limits<int32_t>::max();
  const int64_t min_int64 = std::numeric_limits<int64_t>::min();
  const int64_t max_int64 = std::numeric_limits<int64_t>::max();

  const int32_t min_date32 = -12687428;
  const int32_t max_date32 = 11248737;
  const int64_t min_date64 = 86400000LL * min_date32;
  const int64_t max_date64 = 86400000LL * (max_date32 + 1) - 1;

  const int32_t min_time32_seconds = 0;
  const int32_t max_time32_seconds = 86399;
  const int32_t min_time32_millis = 0;
  const int32_t max_time32_millis = 86399999;
  const int64_t min_time64_micros = 0;
  const int64_t max_time64_micros = 86399999999LL;
  const int64_t min_time64_nanos = 0;
  const int64_t max_time64_nanos = 86399999999999LL;

  const int64_t min_timestamp_seconds = -1096193779200LL;
  const int64_t max_timestamp_seconds = 971890963199LL;
  const int64_t min_timestamp_millis = min_timestamp_seconds * 1000;
  const int64_t max_timestamp_millis = max_timestamp_seconds * 1000 + 999;
  const int64_t min_timestamp_micros = min_timestamp_millis * 1000;
  const int64_t max_timestamp_micros = max_timestamp_millis * 1000 + 999;

  std::vector<bool> is_valid = {false, false, false, false, true,
                                true,  true,  true,  true,  true};

  // Dates
  {
    std::vector<int32_t> values = {min_int32,  max_int32, min_date32 - 1, max_date32 + 1,
                                   min_int32,  max_int32, min_date32 - 1, max_date32 + 1,
                                   min_date32, max_date32};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -2147483648>,
  <value out of range: 2147483647>,
  <value out of range: -12687429>,
  <value out of range: 11248738>,
  -32767-01-01,
  32767-12-31
])expected";
    CheckPrimitive<Date32Type, int32_t>({0, 10}, is_valid, values, expected);
  }
  {
    std::vector<int64_t> values = {min_int64,  max_int64, min_date64 - 1, max_date64 + 1,
                                   min_int64,  max_int64, min_date64 - 1, max_date64 + 1,
                                   min_date64, max_date64};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1096193779200001>,
  <value out of range: 971890963200000>,
  -32767-01-01,
  32767-12-31
])expected";
    CheckPrimitive<Date64Type, int64_t>({0, 10}, is_valid, values, expected);
  }

  // Times
  {
    std::vector<int32_t> values = {min_int32,
                                   max_int32,
                                   min_time32_seconds - 1,
                                   max_time32_seconds + 1,
                                   min_int32,
                                   max_int32,
                                   min_time32_seconds - 1,
                                   max_time32_seconds + 1,
                                   min_time32_seconds,
                                   max_time32_seconds};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -2147483648>,
  <value out of range: 2147483647>,
  <value out of range: -1>,
  <value out of range: 86400>,
  00:00:00,
  23:59:59
])expected";
    CheckPrimitive<Time32Type, int32_t>(time32(TimeUnit::SECOND), {0, 10}, is_valid,
                                        values, expected);
  }
  {
    std::vector<int32_t> values = {
        min_int32,         max_int32,        min_time32_millis - 1, max_time32_millis + 1,
        min_int32,         max_int32,        min_time32_millis - 1, max_time32_millis + 1,
        min_time32_millis, max_time32_millis};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -2147483648>,
  <value out of range: 2147483647>,
  <value out of range: -1>,
  <value out of range: 86400000>,
  00:00:00.000,
  23:59:59.999
])expected";
    CheckPrimitive<Time32Type, int32_t>(time32(TimeUnit::MILLI), {0, 10}, is_valid,
                                        values, expected);
  }
  {
    std::vector<int64_t> values = {
        min_int64,         max_int64,        min_time64_micros - 1, max_time64_micros + 1,
        min_int64,         max_int64,        min_time64_micros - 1, max_time64_micros + 1,
        min_time64_micros, max_time64_micros};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1>,
  <value out of range: 86400000000>,
  00:00:00.000000,
  23:59:59.999999
])expected";
    CheckPrimitive<Time64Type, int64_t>(time64(TimeUnit::MICRO), {0, 10}, is_valid,
                                        values, expected);
  }
  {
    std::vector<int64_t> values = {
        min_int64,        max_int64,       min_time64_nanos - 1, max_time64_nanos + 1,
        min_int64,        max_int64,       min_time64_nanos - 1, max_time64_nanos + 1,
        min_time64_nanos, max_time64_nanos};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1>,
  <value out of range: 86400000000000>,
  00:00:00.000000000,
  23:59:59.999999999
])expected";
    CheckPrimitive<Time64Type, int64_t>(time64(TimeUnit::NANO), {0, 10}, is_valid, values,
                                        expected);
  }

  // Timestamps
  {
    std::vector<int64_t> values = {min_int64,
                                   max_int64,
                                   min_timestamp_seconds - 1,
                                   max_timestamp_seconds + 1,
                                   min_int64,
                                   max_int64,
                                   min_timestamp_seconds - 1,
                                   max_timestamp_seconds + 1,
                                   min_timestamp_seconds,
                                   max_timestamp_seconds};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1096193779201>,
  <value out of range: 971890963200>,
  -32767-01-01 00:00:00,
  32767-12-31 23:59:59
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::SECOND), {0, 10}, is_valid,
                                           values, expected);
  }
  {
    std::vector<int64_t> values = {min_int64,
                                   max_int64,
                                   min_timestamp_millis - 1,
                                   max_timestamp_millis + 1,
                                   min_int64,
                                   max_int64,
                                   min_timestamp_millis - 1,
                                   max_timestamp_millis + 1,
                                   min_timestamp_millis,
                                   max_timestamp_millis};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1096193779200001>,
  <value out of range: 971890963200000>,
  -32767-01-01 00:00:00.000,
  32767-12-31 23:59:59.999
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::MILLI), {0, 10}, is_valid,
                                           values, expected);
  }
  {
    std::vector<int64_t> values = {min_int64,
                                   max_int64,
                                   min_timestamp_micros - 1,
                                   max_timestamp_micros + 1,
                                   min_int64,
                                   max_int64,
                                   min_timestamp_micros - 1,
                                   max_timestamp_micros + 1,
                                   min_timestamp_micros,
                                   max_timestamp_micros};
    static const char* expected = R"expected([
  null,
  null,
  null,
  null,
  <value out of range: -9223372036854775808>,
  <value out of range: 9223372036854775807>,
  <value out of range: -1096193779200000001>,
  <value out of range: 971890963200000000>,
  -32767-01-01 00:00:00.000000,
  32767-12-31 23:59:59.999999
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::MICRO), {0, 10}, is_valid,
                                           values, expected);
  }
  // Note that while the values below are legal and correct, they used to
  // trigger an internal signed overflow inside the vendored "date" library
  // (https://github.com/HowardHinnant/date/issues/696).
  {
    std::vector<int64_t> values = {min_int64, max_int64};
    static const char* expected = R"expected([
  1677-09-21 00:12:43.145224192,
  2262-04-11 23:47:16.854775807
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::NANO), {0, 10},
                                           {true, true}, values, expected);
  }
}

TEST_F(TestPrettyPrint, StructTypeBasic) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22]]");

  static const char* ex = R"expected(-- is_valid: all not null
-- child 0 type: int32
  [
    11
  ]
-- child 1 type: int32
  [
    22
  ])expected";
  CheckStream(*array, {0, 10}, ex);

  static const char* ex_2 = R"expected(  -- is_valid: all not null
  -- child 0 type: int32
    [
      11
    ]
  -- child 1 type: int32
    [
      22
    ])expected";
  CheckStream(*array, {2, 10}, ex_2);
}

TEST_F(TestPrettyPrint, StructTypeAdvanced) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22], null, [null, 33]]");

  static const char* ex = R"expected(-- is_valid:
  [
    true,
    false,
    true
  ]
-- child 0 type: int32
  [
    11,
    0,
    null
  ]
-- child 1 type: int32
  [
    22,
    0,
    33
  ])expected";
  CheckStream(*array, {0, 10}, ex);
}

TEST_F(TestPrettyPrint, StructTypeNoNewLines) {
  // Struct types will at least have new lines for arrays
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22], null, [null, 33]]");
  auto options = PrettyPrintOptions();
  options.skip_new_lines = true;

  static const char* ex = R"expected(-- is_valid:[true,false,true]
-- child 0 type: int32
[11,0,null]
-- child 1 type: int32
[22,0,33])expected";
  CheckStream(*array, options, ex);
}

TEST_F(TestPrettyPrint, BinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};
  std::vector<std::string> values = {"foo", "bar", "", "baz", "", "\xff"};
  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A,\n  ,\n  FF\n]";
  CheckPrimitive<BinaryType, std::string>({0}, is_valid, values, ex);
  CheckPrimitive<LargeBinaryType, std::string>({0}, is_valid, values, ex);
  static const char* ex_in2 =
      "  [\n    666F6F,\n    626172,\n    null,\n    62617A,\n    ,\n    FF\n  ]";
  CheckPrimitive<BinaryType, std::string>({2}, is_valid, values, ex_in2);
  CheckPrimitive<LargeBinaryType, std::string>({2}, is_valid, values, ex_in2);
}

TEST_F(TestPrettyPrint, BinaryNoNewlines) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};
  std::vector<std::string> values = {"foo", "bar", "", "baz", "", "\xff"};

  PrettyPrintOptions options{};
  options.skip_new_lines = true;

  const char* expected = "[666F6F,626172,null,62617A,,FF]";
  CheckPrimitive<BinaryType, std::string>(options, is_valid, values, expected, false);

  // With ellipsis
  options.window = 2;
  expected = "[666F6F,626172,...,,FF]";
  CheckPrimitive<BinaryType, std::string>(options, is_valid, values, expected, false);
}

template <typename TypeClass>
void TestPrettyPrintVarLengthListLike() {
  using LargeTypeClass = typename TypeTraits<TypeClass>::LargeType;
  auto var_list_type = std::make_shared<TypeClass>(int64());
  auto var_large_list_type = std::make_shared<LargeTypeClass>(int64());

  static const char* ex = R"expected([
  [
    null
  ],
  [],
  null,
  [
    4,
    6,
    7
  ],
  [
    2,
    3
  ]
])expected";
  static const char* ex_2 = R"expected(  [
    [
      null
    ],
    [],
    null,
    [
      4,
      6,
      7
    ],
    [
      2,
      3
    ]
  ])expected";
  static const char* ex_3 = R"expected([
  [
    null
  ],
  ...
  [
    2,
    3
  ]
])expected";
  static const char* ex_4 = R"expected([
  [
    null
  ],
  [],
  null,
  [
    4,
    6,
    7
  ],
  [
    2,
    3
  ]
])expected";

  auto array = ArrayFromJSON(var_list_type, "[[null], [], null, [4, 6, 7], [2, 3]]");
  auto make_options = [](int indent, int window, int container_window) {
    auto options = PrettyPrintOptions(indent, window);
    options.container_window = container_window;
    return options;
  };
  CheckStream(*array, make_options(/*indent=*/0, /*window=*/10, /*container_window=*/5),
              ex);
  CheckStream(*array, make_options(/*indent=*/2, /*window=*/10, /*container_window=*/5),
              ex_2);
  CheckStream(*array, make_options(/*indent=*/0, /*window=*/10, /*container_window=*/1),
              ex_3);
  CheckArray(*array, {0, 10}, ex_4);

  array = ArrayFromJSON(var_large_list_type, "[[null], [], null, [4, 6, 7], [2, 3]]");
  CheckStream(*array, make_options(/*indent=*/0, /*window=*/10, /*container_window=*/5),
              ex);
  CheckStream(*array, make_options(/*indent=*/2, /*window=*/10, /*container_window=*/5),
              ex_2);
  CheckStream(*array, make_options(/*indent=*/0, /*window=*/10, /*container_window=*/1),
              ex_3);
  CheckArray(*array, {0, 10}, ex_4);
}

TEST_F(TestPrettyPrint, ListType) { TestPrettyPrintVarLengthListLike<arrow::ListType>(); }

template <typename ListViewType>
void TestListViewSpecificPrettyPrinting() {
  using ArrayType = typename TypeTraits<ListViewType>::ArrayType;
  using OffsetType = typename TypeTraits<ListViewType>::OffsetType;

  auto string_values = ArrayFromJSON(utf8(), R"(["Hello", "World", null])");
  auto int32_values = ArrayFromJSON(int32(), "[1, 20, 3]");
  auto int16_values = ArrayFromJSON(int16(), "[10, 2, 30]");

  auto Offsets = [](std::string_view json) {
    return ArrayFromJSON(TypeTraits<OffsetType>::type_singleton(), json);
  };
  auto Sizes = Offsets;

  ASSERT_OK_AND_ASSIGN(auto int_list_view_array,
                       ArrayType::FromArrays(*Offsets("[0, 0, 1, 2]"),
                                             *Sizes("[2, 1, 1, 1]"), *int32_values));
  ASSERT_OK(int_list_view_array->ValidateFull());
  static const char* ex1 =
      "[\n"
      "  [\n"
      "    1,\n"
      "    20\n"
      "  ],\n"
      "  [\n"
      "    1\n"
      "  ],\n"
      "  [\n"
      "    20\n"
      "  ],\n"
      "  [\n"
      "    3\n"
      "  ]\n"
      "]";
  CheckStream(*int_list_view_array, {}, ex1);

  ASSERT_OK_AND_ASSIGN(auto string_list_view_array,
                       ArrayType::FromArrays(*Offsets("[0, 0, 1, 2]"),
                                             *Sizes("[2, 1, 1, 1]"), *string_values));
  ASSERT_OK(string_list_view_array->ValidateFull());
  static const char* ex2 =
      "[\n"
      "  [\n"
      "    \"Hello\",\n"
      "    \"World\"\n"
      "  ],\n"
      "  [\n"
      "    \"Hello\"\n"
      "  ],\n"
      "  [\n"
      "    \"World\"\n"
      "  ],\n"
      "  [\n"
      "    null\n"
      "  ]\n"
      "]";
  CheckStream(*string_list_view_array, {}, ex2);

  auto sliced_array = string_list_view_array->Slice(1, 2);
  static const char* ex3 =
      "[\n"
      "  [\n"
      "    \"Hello\"\n"
      "  ],\n"
      "  [\n"
      "    \"World\"\n"
      "  ]\n"
      "]";
  CheckStream(*sliced_array, {}, ex3);

  ASSERT_OK_AND_ASSIGN(
      auto empty_array,
      ArrayType::FromArrays(*Offsets("[]"), *Sizes("[]"), *int16_values));
  ASSERT_OK(empty_array->ValidateFull());
  static const char* ex4 = "[]";
  CheckStream(*empty_array, {}, ex4);
}

TEST_F(TestPrettyPrint, ListViewType) {
  TestPrettyPrintVarLengthListLike<arrow::ListViewType>();

  TestListViewSpecificPrettyPrinting<arrow::ListViewType>();
  TestListViewSpecificPrettyPrinting<arrow::LargeListViewType>();
}

TEST_F(TestPrettyPrint, ListTypeNoNewlines) {
  auto list_type = list(int64());
  auto empty_array = ArrayFromJSON(list_type, "[]");
  auto array = ArrayFromJSON(list_type, "[[null], [], null, [4, 5, 6, 7, 8], [2, 3]]");

  PrettyPrintOptions options{};
  options.skip_new_lines = true;
  options.null_rep = "NA";
  options.container_window = 10;
  CheckArray(*empty_array, options, "[]", false);
  CheckArray(*array, options, "[[NA],[],NA,[4,5,6,7,8],[2,3]]", false);

  options.window = 2;
  options.container_window = 2;
  CheckArray(*empty_array, options, "[]", false);
  CheckArray(*array, options, "[[NA],[],NA,[4,5,6,7,8],[2,3]]", false);

  options.window = 1;
  options.container_window = 2;
  CheckArray(*empty_array, options, "[]", false);
  CheckArray(*array, options, "[[NA],[],NA,[4,...,8],[2,3]]", false);
}

TEST_F(TestPrettyPrint, MapType) {
  auto map_type = map(utf8(), int64());
  auto array = ArrayFromJSON(map_type, R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])");

  static const char* ex = R"expected([
  keys:
  [
    "joe",
    "mark"
  ]
  values:
  [
    0,
    null
  ],
  null,
  keys:
  [
    "cap"
  ]
  values:
  [
    8
  ],
  keys:
  []
  values:
  []
])expected";
  CheckArray(*array, {0, 10}, ex);

  PrettyPrintOptions options{};
  options.skip_new_lines = true;

  static const char* ex_flat =
      R"expected([keys:["joe","mark"]values:[0,null],null,)expected"
      R"expected(keys:["cap"]values:[8],keys:[]values:[]])expected";
  CheckArray(*array, options, ex_flat, false);
}

TEST_F(TestPrettyPrint, FixedSizeListType) {
  auto list_type = fixed_size_list(int32(), 3);
  auto array = ArrayFromJSON(list_type,
                             "[[null, 0, 1], [2, 3, null], null, [4, 6, 7], [8, 9, 5]]");

  CheckArray(*array, {0, 10}, R"expected([
  [
    null,
    0,
    1
  ],
  [
    2,
    3,
    null
  ],
  null,
  [
    4,
    6,
    7
  ],
  [
    8,
    9,
    5
  ]
])expected");

  auto make_options = [](int indent, int window, int container_window) {
    auto options = PrettyPrintOptions(indent, window);
    options.container_window = container_window;
    return options;
  };
  CheckStream(*array, make_options(/*indent=*/0, /*window=*/1, /*container_window=*/3),
              R"expected([
  [
    null,
    0,
    1
  ],
  [
    2,
    3,
    null
  ],
  null,
  [
    4,
    6,
    7
  ],
  [
    8,
    9,
    5
  ]
])expected");

  CheckStream(*array, make_options(/*indent=*/0, /*window=*/1, /*container_window=*/1),
              R"expected([
  [
    null,
    0,
    1
  ],
  ...
  [
    8,
    9,
    5
  ]
])expected");
}

TEST_F(TestPrettyPrint, FixedSizeBinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  auto type = fixed_size_binary(3);
  auto array = ArrayFromJSON(type, "[\"foo\", \"bar\", null, \"baz\"]");

  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A\n]";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = "  [\n    666F6F,\n    ...\n    62617A\n  ]";
  CheckArray(*array, {2, 1}, ex_2);
}

TEST_F(TestPrettyPrint, DecimalTypes) {
  int32_t p = 19;
  int32_t s = 4;

  for (auto type : {decimal128(p, s), decimal256(p, s)}) {
    auto array = ArrayFromJSON(type, "[\"123.4567\", \"456.7891\", null]");

    static const char* ex = "[\n  123.4567,\n  456.7891,\n  null\n]";
    CheckArray(*array, {0}, ex);
  }
}

TEST_F(TestPrettyPrint, DictionaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), utf8());

  std::shared_ptr<Array> indices;
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);
  auto arr = std::make_shared<DictionaryArray>(dict_type, indices, dict);

  static const char* expected = R"expected(
-- dictionary:
  [
    "foo",
    "bar",
    "baz"
  ]
-- indices:
  [
    1,
    2,
    null,
    0,
    2,
    0
  ])expected";

  CheckArray(*arr, {0}, expected);
}

TEST_F(TestPrettyPrint, ChunkedArrayPrimitiveType) {
  auto array = ArrayFromJSON(int32(), "[0, 1, null, 3, null]");
  ChunkedArray chunked_array(array);

  static const char* expected = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";
  CheckStream(chunked_array, {0}, expected);

  ChunkedArray chunked_array_2({array, array});

  static const char* expected_2 = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ],
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";

  CheckStream(chunked_array_2, {0}, expected_2);
}

TEST_F(TestPrettyPrint, ChunkedArrayCustomElementDelimiter) {
  PrettyPrintOptions options{};
  // Use a custom ChunkedArray element delimiter of ";",
  // rather than the default delimiter (i.e. ",").
  options.chunked_array_delimiters.element = ";";
  // Use a custom Array element delimiter of " | ",
  // rather than the default delimiter (i.e. ",").
  options.array_delimiters.element = " | ";

  const auto chunk = ArrayFromJSON(int32(), "[1, 2, null, 4, null]");

  // ChunkedArray with 1 chunk
  {
    const ChunkedArray chunked_array(chunk);

    static const char* expected = R"expected([
  [
    1 | 
    2 | 
    null | 
    4 | 
    null
  ]
])expected";
    CheckStream(chunked_array, options, expected);
  }

  // ChunkedArray with 2 chunks
  {
    const ChunkedArray chunked_array({chunk, chunk});

    static const char* expected = R"expected([
  [
    1 | 
    2 | 
    null | 
    4 | 
    null
  ];
  [
    1 | 
    2 | 
    null | 
    4 | 
    null
  ]
])expected";

    CheckStream(chunked_array, options, expected);
  }
}

TEST_F(TestPrettyPrint, ChunkedArrayCustomOpenCloseDelimiter) {
  PrettyPrintOptions options{};
  // Use a custom opening Array delimiter of "{", rather than the default "]".
  options.array_delimiters.open = "{";
  // Use a custom closing Array delimiter of "}", rather than the default "]".
  options.array_delimiters.close = "}";
  // Use a custom opening ChunkedArray delimiter of "<", rather than the default "]".
  options.chunked_array_delimiters.open = "<";
  // Use a custom closing ChunkedArray delimiter of ">", rather than the default "]".
  options.chunked_array_delimiters.close = ">";

  const auto chunk = ArrayFromJSON(int32(), "[1, 2, null, 4, null]");

  // ChunkedArray with 1 chunk
  {
    const ChunkedArray chunked_array(chunk);

    static const char* expected = R"expected(<
  {
    1,
    2,
    null,
    4,
    null
  }
>)expected";
    CheckStream(chunked_array, options, expected);
  }

  // ChunkedArray with 2 chunks
  {
    const ChunkedArray chunked_array({chunk, chunk});

    static const char* expected = R"expected(<
  {
    1,
    2,
    null,
    4,
    null
  },
  {
    1,
    2,
    null,
    4,
    null
  }
>)expected";

    CheckStream(chunked_array, options, expected);
  }
}

TEST_F(TestPrettyPrint, TablePrimitive) {
  std::shared_ptr<Field> int_field = field("column", int32());
  auto array = ArrayFromJSON(int_field->type(), "[0, 1, null, 3, null]");
  auto column = std::make_shared<ChunkedArray>(ArrayVector({array}));
  std::shared_ptr<Schema> table_schema = schema({int_field});
  std::shared_ptr<Table> table = Table::Make(table_schema, {column});

  static const char* expected = R"expected(column: int32
----
column:
  [
    [
      0,
      1,
      null,
      3,
      null
    ]
  ]
)expected";
  CheckStream(*table, {0}, expected);
}

TEST_F(TestPrettyPrint, SchemaWithDictionary) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);

  auto simple = field("one", int32());
  auto simple_dict = field("two", dictionary(int16(), utf8()));
  auto list_of_dict = field("three", list(simple_dict));
  auto struct_with_dict = field("four", struct_({simple, simple_dict}));

  auto sch = schema({simple, simple_dict, list_of_dict, struct_with_dict});

  static const char* expected = R"expected(one: int32
two: dictionary<values=string, indices=int16, ordered=0>
three: list<two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, two: dictionary<values=string, indices=int16, ordered=0>
four: struct<one: int32, two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, one: int32
  child 1, two: dictionary<values=string, indices=int16, ordered=0>)expected";

  PrettyPrintOptions options;
  Check(*sch, options, expected);
}

TEST_F(TestPrettyPrint, SchemaWithNotNull) {
  auto simple = field("one", int32());
  auto non_null = field("two", int32(), false);
  auto list_simple = field("three", list(int32()));
  auto list_non_null = field("four", list(int32()), false);
  auto list_non_null2 = field("five", list(field("item", int32(), false)));

  auto sch = schema({simple, non_null, list_simple, list_non_null, list_non_null2});

  static const char* expected = R"expected(one: int32
two: int32 not null
three: list<item: int32>
  child 0, item: int32
four: list<item: int32> not null
  child 0, item: int32
five: list<item: int32 not null>
  child 0, item: int32 not null)expected";

  PrettyPrintOptions options;
  Check(*sch, options, expected);
}

TEST_F(TestPrettyPrint, SchemaWithMetadata) {
  // ARROW-7063
  auto metadata1 = key_value_metadata({"foo1"}, {"bar1"});
  auto metadata2 = key_value_metadata({"foo2"}, {"bar2"});
  auto metadata3 = key_value_metadata(
      {"foo3", "lorem"},
      {"bar3",
       R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla accumsan vel
          turpis et mollis. Aliquam tincidunt arcu id tortor blandit blandit. Donec
          eget leo quis lectus scelerisque varius. Class aptent taciti sociosqu ad
          litora torquent per conubia nostra, per inceptos himenaeos. Praesent
          faucibus, diam eu volutpat iaculis, tellus est porta ligula, a efficitur
          turpis nulla facilisis quam. Aliquam vitae lorem erat. Proin a dolor ac libero
          dignissim mollis vitae eu mauris. Quisque posuere tellus vitae massa
          pellentesque sagittis. Aenean feugiat, diam ac dignissim fermentum, lorem
          sapien commodo massa, vel volutpat orci nisi eu justo. Nulla non blandit
          sapien. Quisque pretium vestibulum urna eu vehicula.)"});
  auto my_schema = schema(
      {field("one", int32(), true, metadata1), field("two", utf8(), false, metadata2)},
      metadata3);

  PrettyPrintOptions options;
  static const char* expected = R"(one: int32
  -- field metadata --
  foo1: 'bar1'
two: string not null
  -- field metadata --
  foo2: 'bar2'
-- schema metadata --
foo3: 'bar3'
lorem: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla ac' + 737)";
  Check(*my_schema, options, expected);

  static const char* expected_verbose = R"(one: int32
  -- field metadata --
  foo1: 'bar1'
two: string not null
  -- field metadata --
  foo2: 'bar2'
-- schema metadata --
foo3: 'bar3'
lorem: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla accumsan vel
          turpis et mollis. Aliquam tincidunt arcu id tortor blandit blandit. Donec
          eget leo quis lectus scelerisque varius. Class aptent taciti sociosqu ad
          litora torquent per conubia nostra, per inceptos himenaeos. Praesent
          faucibus, diam eu volutpat iaculis, tellus est porta ligula, a efficitur
          turpis nulla facilisis quam. Aliquam vitae lorem erat. Proin a dolor ac libero
          dignissim mollis vitae eu mauris. Quisque posuere tellus vitae massa
          pellentesque sagittis. Aenean feugiat, diam ac dignissim fermentum, lorem
          sapien commodo massa, vel volutpat orci nisi eu justo. Nulla non blandit
          sapien. Quisque pretium vestibulum urna eu vehicula.')";
  options.truncate_metadata = false;
  Check(*my_schema, options, expected_verbose);

  // Metadata that exactly fits
  auto metadata4 =
      key_value_metadata({"key"}, {("valuexxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                                    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")});
  my_schema = schema({field("f0", int32())}, metadata4);
  static const char* expected_fits = R"(f0: int32
-- schema metadata --
key: 'valuexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')";
  options.truncate_metadata = false;
  Check(*my_schema, options, expected_fits);

  // A large key
  auto metadata5 = key_value_metadata({"0123456789012345678901234567890123456789"},
                                      {("valuexxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                                        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")});
  my_schema = schema({field("f0", int32())}, metadata5);
  static const char* expected_big_key = R"(f0: int32
-- schema metadata --
0123456789012345678901234567890123456789: 'valuexxxxxxxxxxxxxxxxxxxxxxxxx' + 40)";
  options.truncate_metadata = true;
  Check(*my_schema, options, expected_big_key);
}

TEST_F(TestPrettyPrint, SchemaIndentation) {
  // ARROW-6159
  auto simple = field("one", int32());
  auto non_null = field("two", int32(), false);
  auto sch = schema({simple, non_null});

  static const char* expected = R"expected(    one: int32
    two: int32 not null)expected";

  PrettyPrintOptions options(/*indent=*/4);
  Check(*sch, options, expected);
}

}  // namespace arrow

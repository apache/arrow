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

#include <gtest/gtest.h>
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"

namespace arrow {

using internal::StringFormatter;

class ScalarTemporalTest : public ::testing::Test {};

namespace compute {

TEST(ScalarTemporalTest, TestSimpleTemporalComponentExtraction) {
  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1900-01-01T01:59:20.001001001","2033-05-18T03:33:20.000000000"])";
  auto unit = timestamp(TimeUnit::NANO);
  auto timestamps = ArrayFromJSON(unit, times);

  auto year = "[1970, 2000, 1900, 2033]";
  auto month = "[1, 2, 1, 5]";
  auto day = "[1, 29, 1, 18]";
  auto day_of_week = "[4, 2, 1, 3]";
  auto day_of_year = "[1, 60, 1, 138]";
  auto week = "[1, 9, 1, 20]";
  auto quarter = "[1, 1, 1, 2]";
  auto hour = "[0, 23, 1, 3]";
  auto minute = "[0, 23, 59, 33]";
  auto second = "[59, 23, 20, 20]";
  auto millisecond = "[123, 999, 1, 0]";
  auto microsecond = "[456, 999, 1, 0]";
  auto nanosecond = "[789, 999, 1, 0]";

  ASSERT_OK_AND_ASSIGN(Datum actual_year, Year(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_month, Month(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day, Day(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_year, DayOfYear(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_week, Week(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_quarter, Quarter(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_week, DayOfWeek(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_hour, Hour(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_minute, Minute(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_second, Second(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_millisecond, Millisecond(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_microsecond, Microsecond(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_nanosecond, Nanosecond(timestamps));

  ASSERT_EQ(actual_year, ArrayFromJSON(int32(), year));
  ASSERT_EQ(actual_month, ArrayFromJSON(uint32(), month));
  ASSERT_EQ(actual_day, ArrayFromJSON(uint32(), day));
  ASSERT_EQ(actual_day_of_week, ArrayFromJSON(uint8(), day_of_week));
  ASSERT_EQ(actual_day_of_year, ArrayFromJSON(uint16(), day_of_year));
  ASSERT_EQ(actual_week, ArrayFromJSON(uint8(), week));
  ASSERT_EQ(actual_quarter, ArrayFromJSON(uint32(), quarter));
  ASSERT_EQ(actual_hour, ArrayFromJSON(uint8(), hour));
  ASSERT_EQ(actual_minute, ArrayFromJSON(uint8(), minute));
  ASSERT_EQ(actual_second, ArrayFromJSON(uint8(), second));
  ASSERT_EQ(actual_millisecond, ArrayFromJSON(uint16(), millisecond));
  ASSERT_EQ(actual_microsecond, ArrayFromJSON(uint16(), microsecond));
  ASSERT_EQ(actual_nanosecond, ArrayFromJSON(uint16(), nanosecond));

  //  CheckScalarUnary("year", unit, times, int32(), year);
  //  CheckScalarUnary("month", unit, times, uint32(), month);
  //  CheckScalarUnary("day", unit, times, uint32(), day);
  //  CheckScalarUnary("day_of_week", unit, times, uint8(), day_of_week);
  //  CheckScalarUnary("day_of_year", unit, times, uint16(), day_of_year);
  //  CheckScalarUnary("week", unit, times, uint8(), week);
  //  CheckScalarUnary("quarter", unit, times, uint32(), quarter);
  //  CheckScalarUnary("hour", unit, times, uint8(), hour);
  //  CheckScalarUnary("minute", unit, times, uint8(), minute);
  //  CheckScalarUnary("second", unit, times, uint8(), second);
  //  CheckScalarUnary("millisecond", unit, times, uint16(), millisecond);
  //  CheckScalarUnary("microsecond", unit, times, uint16(), microsecond);
  //  CheckScalarUnary("nanosecond", unit, times, uint16(), nanosecond);
}

TEST(ScalarTemporalTest, TestSimpleZonedTemporalComponentExtraction) {
  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1900-01-01T01:59:20.001001001","2033-05-18T03:33:20.000000000"])";
  auto unit = timestamp(TimeUnit::NANO, "CET");
  auto timestamps = ArrayFromJSON(unit, times);

  auto year = "[1970, 2000, 1900, 2033]";
  auto month = "[1, 2, 1, 5]";
  auto day = "[1, 29, 1, 18]";
  auto day_of_week = "[4, 2, 1, 3]";
  auto day_of_year = "[1, 60, 1, 138]";
  auto week = "[1, 9, 1, 20]";
  auto quarter = "[1, 1, 1, 2]";
  auto hour = "[1, 0, 2, 5]";
  auto minute = "[0, 23, 59, 33]";
  auto second = "[59, 23, 20, 20]";
  auto millisecond = "[123, 999, 1, 0]";
  auto microsecond = "[456, 999, 1, 0]";
  auto nanosecond = "[789, 999, 1, 0]";

  ASSERT_OK_AND_ASSIGN(Datum actual_year, Year(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_month, Month(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day, Day(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_year, DayOfYear(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_week, Week(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_quarter, Quarter(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_week, DayOfWeek(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_hour, Hour(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_minute, Minute(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_second, Second(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_millisecond, Millisecond(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_microsecond, Microsecond(timestamps));
  ASSERT_OK_AND_ASSIGN(Datum actual_nanosecond, Nanosecond(timestamps));

  ASSERT_EQ(actual_year, ArrayFromJSON(int32(), year));
  ASSERT_EQ(actual_month, ArrayFromJSON(uint32(), month));
  ASSERT_EQ(actual_day, ArrayFromJSON(uint32(), day));
  ASSERT_EQ(actual_day_of_week, ArrayFromJSON(uint8(), day_of_week));
  ASSERT_EQ(actual_day_of_year, ArrayFromJSON(uint16(), day_of_year));
  ASSERT_EQ(actual_week, ArrayFromJSON(uint8(), week));
  ASSERT_EQ(actual_quarter, ArrayFromJSON(uint32(), quarter));
  ASSERT_EQ(actual_hour, ArrayFromJSON(uint8(), hour));
  ASSERT_EQ(actual_minute, ArrayFromJSON(uint8(), minute));
  ASSERT_EQ(actual_second, ArrayFromJSON(uint8(), second));
  ASSERT_EQ(actual_millisecond, ArrayFromJSON(uint16(), millisecond));
  ASSERT_EQ(actual_microsecond, ArrayFromJSON(uint16(), microsecond));
  ASSERT_EQ(actual_nanosecond, ArrayFromJSON(uint16(), nanosecond));

  //  CheckScalarUnary("year", unit, times, int32(), year);
  //  CheckScalarUnary("month", unit, times, uint32(), month);
  //  CheckScalarUnary("day", unit, times, uint32(), day);
  //  CheckScalarUnary("day_of_week", unit, times, uint8(), day_of_week);
  //  CheckScalarUnary("day_of_year", unit, times, uint16(), day_of_year);
  //  CheckScalarUnary("week", unit, times, uint8(), week);
  //  CheckScalarUnary("quarter", unit, times, uint32(), quarter);
  //  CheckScalarUnary("hour", unit, times, uint8(), hour);
  //  CheckScalarUnary("minute", unit, times, uint8(), minute);
  //  CheckScalarUnary("second", unit, times, uint8(), second);
  //  CheckScalarUnary("millisecond", unit, times, uint16(), millisecond);
  //  CheckScalarUnary("microsecond", unit, times, uint16(), microsecond);
  //  CheckScalarUnary("nanosecond", unit, times, uint16(), nanosecond);
}

TEST(ScalarTemporalTest, TestZonedTemporalComponentExtraction) {
  std::string timezone = "CET";
  const char* json_second = "[59, 951866603, -2208981640, 2000000000]";
  const char* json_milli = "[59000, 951866603000, -2208981640000, 2000000000000]";
  const char* json_micro =
      "[59000000, 951866603000000, -2208981640000000, 2000000000000000]";
  const char* json_nano =
      "[59000000000, 951866603000000000, -2208981640000000000, 2000000000000000000]";

  auto year = ArrayFromJSON(int32(), "[1970, 2000, 1900, 2033]");
  auto month = ArrayFromJSON(uint32(), "[1, 2, 1, 5]");
  auto day = ArrayFromJSON(uint32(), "[1, 29, 1, 18]");
  auto day_of_week = ArrayFromJSON(uint8(), "[4, 2, 1, 3]");
  auto day_of_year = ArrayFromJSON(uint16(), "[1, 60, 1, 138]");
  auto week = ArrayFromJSON(uint8(), "[1, 9, 1, 20]");
  auto quarter = ArrayFromJSON(uint32(), "[1, 1, 1, 2]");
  auto hour = ArrayFromJSON(uint8(), "[1, 0, 2, 5]");
  auto minute = ArrayFromJSON(uint8(), "[0, 23, 59, 33]");
  auto second = ArrayFromJSON(uint8(), "[59, 23, 20, 20]");

  auto all_time_points = {
      ArrayFromJSON(timestamp(TimeUnit::SECOND, timezone), json_second),
      ArrayFromJSON(timestamp(TimeUnit::MILLI, timezone), json_milli),
      ArrayFromJSON(timestamp(TimeUnit::MICRO, timezone), json_micro),
      ArrayFromJSON(timestamp(TimeUnit::NANO, timezone), json_nano)};

  //  for (auto time_points : all_time_points) {
  //    CheckScalarUnary("year", time_points, year);
  //    CheckScalarUnary("month", time_points, month);
  //    CheckScalarUnary("day", time_points, day);
  //    CheckScalarUnary("day_of_year", time_points, day_of_year);
  //    CheckScalarUnary("week", time_points, week);
  //    CheckScalarUnary("quarter", time_points, quarter);
  //    CheckScalarUnary("day_of_week", time_points, day_of_week);
  //    CheckScalarUnary("hour", time_points, hour);
  //    CheckScalarUnary("minute", time_points, minute);
  //    CheckScalarUnary("second", time_points, second);
  //  }
}
}  // namespace compute
}  // namespace arrow

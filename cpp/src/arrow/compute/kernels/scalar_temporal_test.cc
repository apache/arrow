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
  const char* json =
      R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "3989-07-14T18:04:01","1900-01-01T01:59:20","2033-05-18T03:33:20"])";
  auto time_points = ArrayFromJSON(timestamp(TimeUnit::SECOND), json);

  auto year = ArrayFromJSON(int64(), "[1970, 2000, 3989, 1900, 2033]");
  auto month = ArrayFromJSON(int64(), "[1, 2, 7, 1, 5]");
  auto day = ArrayFromJSON(int64(), "[1, 29, 14, 1, 18]");
  auto day_of_year = ArrayFromJSON(int64(), "[1, 60, 195, 1, 138]");
  auto week = ArrayFromJSON(int64(), "[1, 9, 28, 1, 20]");
  auto quarter = ArrayFromJSON(int64(), "[1, 1, 3, 1, 2]");
  auto day_of_week = ArrayFromJSON(int64(), "[4, 2, 5, 1, 3]");
  auto hour = ArrayFromJSON(int64(), "[0, 23, 18, 1, 3]");
  auto minute = ArrayFromJSON(int64(), "[0, 23, 4, 59, 33]");
  auto second = ArrayFromJSON(int64(), "[59, 23, 1, 20, 20]");
  auto millisecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");
  auto microsecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");
  auto nanosecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");

  ASSERT_OK_AND_ASSIGN(Datum actual_year, Year(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_month, Month(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_day, Day(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_year, DayOfYear(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_week, Week(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_quarter, Quarter(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_day_of_week, DayOfWeek(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_hour, Hour(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_minute, Minute(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_second, Second(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_millisecond, Millisecond(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_microsecond, Microsecond(time_points));
  ASSERT_OK_AND_ASSIGN(Datum actual_nanosecond, Nanosecond(time_points));

  ASSERT_EQ(actual_year, year);
  ASSERT_EQ(actual_month, month);
  ASSERT_EQ(actual_day, day);
  ASSERT_EQ(actual_day_of_year, day_of_year);
  ASSERT_EQ(actual_week, week);
  ASSERT_EQ(actual_quarter, quarter);
  ASSERT_EQ(actual_day_of_week, day_of_week);
  ASSERT_EQ(actual_hour, hour);
  ASSERT_EQ(actual_minute, minute);
  ASSERT_EQ(actual_second, second);
  ASSERT_EQ(actual_millisecond, millisecond);
  ASSERT_EQ(actual_microsecond, microsecond);
  ASSERT_EQ(actual_nanosecond, nanosecond);

  CheckScalarUnary("year", time_points, year);
  CheckScalarUnary("month", time_points, month);
  CheckScalarUnary("day", time_points, day);
  CheckScalarUnary("day_of_year", time_points, day_of_year);
  CheckScalarUnary("week", time_points, week);
  CheckScalarUnary("quarter", time_points, quarter);
  CheckScalarUnary("day_of_week", time_points, day_of_week);
  CheckScalarUnary("hour", time_points, hour);
  CheckScalarUnary("minute", time_points, minute);
  CheckScalarUnary("second", time_points, second);
  CheckScalarUnary("millisecond", time_points, millisecond);
  CheckScalarUnary("microsecond", time_points, microsecond);
  CheckScalarUnary("nanosecond", time_points, nanosecond);
}

TEST(ScalarTemporalTest, TestTemporalComponentExtraction) {
  const char* json_second = "[59, 951866603, -2208981640, 2000000000]";
  const char* json_milli = "[59000, 951866603000, -2208981640000, 2000000000000]";
  const char* json_micro =
      "[59000000, 951866603000000, -2208981640000000, 2000000000000000]";
  const char* json_nano =
      "[59000000000, 951866603000000000, -2208981640000000000, 2000000000000000000]";

  auto time_points_second = ArrayFromJSON(timestamp(TimeUnit::SECOND), json_second);
  auto time_points_milli = ArrayFromJSON(timestamp(TimeUnit::MILLI), json_milli);
  auto time_points_micro = ArrayFromJSON(timestamp(TimeUnit::MICRO), json_micro);
  auto time_points_nano = ArrayFromJSON(timestamp(TimeUnit::NANO), json_nano);

  auto year = ArrayFromJSON(int64(), "[1970, 2000, 1900, 2033]");
  auto month = ArrayFromJSON(int64(), "[1, 2, 1, 5]");
  auto day = ArrayFromJSON(int64(), "[1, 29, 1, 18]");
  auto day_of_year = ArrayFromJSON(int64(), "[1, 60, 1, 138]");
  auto week = ArrayFromJSON(int64(), "[1, 9, 1, 20]");
  auto quarter = ArrayFromJSON(int64(), "[1, 1, 1, 2]");
  auto day_of_week = ArrayFromJSON(int64(), "[4, 2, 1, 3]");
  auto hour = ArrayFromJSON(int64(), "[0, 23, 1, 3]");
  auto minute = ArrayFromJSON(int64(), "[0, 23, 59, 33]");
  auto second = ArrayFromJSON(int64(), "[59, 23, 20, 20]");
  auto millisecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");
  auto microsecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");
  auto nanosecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");

  for (auto time_points :
       {time_points_second, time_points_milli, time_points_micro, time_points_nano}) {
    CheckScalarUnary("year", time_points, year);
    CheckScalarUnary("month", time_points, month);
    CheckScalarUnary("day", time_points, day);
    CheckScalarUnary("day_of_year", time_points, day_of_year);
    CheckScalarUnary("week", time_points, week);
    CheckScalarUnary("quarter", time_points, quarter);
    CheckScalarUnary("day_of_week", time_points, day_of_week);
    CheckScalarUnary("hour", time_points, hour);
    CheckScalarUnary("minute", time_points, minute);
    CheckScalarUnary("second", time_points, second);
    CheckScalarUnary("millisecond", time_points, millisecond);
    CheckScalarUnary("microsecond", time_points, microsecond);
    CheckScalarUnary("nanosecond", time_points, nanosecond);
  }

  std::string in = "[123, 999, 1, 31231000]";
  auto out = ArrayFromJSON(int64(), "[123, 999, 1, 0]");

  auto tp_milli = ArrayFromJSON(timestamp(TimeUnit::MILLI), in);
  auto tp_milli_zoned = ArrayFromJSON(timestamp(TimeUnit::MILLI, "Etc/GMT+2"), in);
  CheckScalarUnary("millisecond", tp_milli, out);
  CheckScalarUnary("millisecond", tp_milli, out);

  auto tp_micro = ArrayFromJSON(timestamp(TimeUnit::MICRO), in);
  auto tp_micro_zoned = ArrayFromJSON(timestamp(TimeUnit::MICRO, "Etc/GMT+2"), in);
  CheckScalarUnary("microsecond", tp_micro, out);
  CheckScalarUnary("microsecond", tp_micro_zoned, out);

  auto tp_nano = ArrayFromJSON(timestamp(TimeUnit::NANO), in);
  auto tp_nano_zoned = ArrayFromJSON(timestamp(TimeUnit::NANO, "Etc/GMT+2"), in);
  CheckScalarUnary("nanosecond", tp_nano, out);
  CheckScalarUnary("nanosecond", tp_nano_zoned, out);
}

TEST(ScalarTemporalTest, TestSimpleZonedTemporalComponentExtraction) {
  const char* json =
      R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "3989-07-14T18:04:01","1900-01-01T01:59:20","2033-05-18T03:33:20"])";
  auto time_points = ArrayFromJSON(timestamp(TimeUnit::SECOND, "Etc/GMT+2"), json);

  auto year = ArrayFromJSON(int64(), "[1969, 2000, 3989, 1899, 2033]");
  auto month = ArrayFromJSON(int64(), "[12, 2, 7, 12, 5]");
  auto day = ArrayFromJSON(int64(), "[31, 29, 14, 31, 18]");
  auto day_of_year = ArrayFromJSON(int64(), "[365, 60, 195, 365, 138]");
  auto week = ArrayFromJSON(int64(), "[1, 9, 28, 52, 20]");
  auto quarter = ArrayFromJSON(int64(), "[4, 1, 3, 4, 2]");
  auto day_of_week = ArrayFromJSON(int64(), "[3, 2, 5, 7, 3]");
  auto hour = ArrayFromJSON(int64(), "[22, 21, 16, 23, 1]");
  auto minute = ArrayFromJSON(int64(), "[0, 23, 4, 59, 33]");
  auto second = ArrayFromJSON(int64(), "[59, 23, 1, 20, 20]");
  auto millisecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");
  auto microsecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");
  auto nanosecond = ArrayFromJSON(int64(), "[0, 0, 0, 0, 0]");

  CheckScalarUnary("year", time_points, year);
  CheckScalarUnary("month", time_points, month);
  CheckScalarUnary("day", time_points, day);
  CheckScalarUnary("day_of_year", time_points, day_of_year);
  CheckScalarUnary("week", time_points, week);
  CheckScalarUnary("quarter", time_points, quarter);
  CheckScalarUnary("day_of_week", time_points, day_of_week);
  CheckScalarUnary("hour", time_points, hour);
  CheckScalarUnary("minute", time_points, minute);
  CheckScalarUnary("second", time_points, second);
  CheckScalarUnary("millisecond", time_points, millisecond);
  CheckScalarUnary("microsecond", time_points, microsecond);
  CheckScalarUnary("nanosecond", time_points, nanosecond);
}

TEST(ScalarTemporalTest, TestZonedTemporalComponentExtraction) {
  std::string timezone = "Etc/GMT+2";
  const char* json_second = "[59, 951866603, -2208981640, 2000000000]";
  const char* json_milli = "[59000, 951866603000, -2208981640000, 2000000000000]";
  const char* json_micro =
      "[59000000, 951866603000000, -2208981640000000, 2000000000000000]";
  const char* json_nano =
      "[59000000000, 951866603000000000, -2208981640000000000, 2000000000000000000]";

  auto year = ArrayFromJSON(int64(), "[1969, 2000, 1899, 2033]");
  auto month = ArrayFromJSON(int64(), "[12, 2, 12, 5]");
  auto day = ArrayFromJSON(int64(), "[31, 29, 31, 18]");
  auto day_of_year = ArrayFromJSON(int64(), "[365, 60, 365, 138]");
  auto week = ArrayFromJSON(int64(), "[1, 9, 52, 20]");
  auto quarter = ArrayFromJSON(int64(), "[4, 1, 4, 2]");
  auto day_of_week = ArrayFromJSON(int64(), "[3, 2, 7, 3]");
  auto hour = ArrayFromJSON(int64(), "[22, 21, 23, 1]");
  auto minute = ArrayFromJSON(int64(), "[0, 23, 59, 33]");
  auto second = ArrayFromJSON(int64(), "[59, 23, 20, 20]");
  auto millisecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");
  auto microsecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");
  auto nanosecond = ArrayFromJSON(int64(), "[0, 0, 0, 0]");

  auto all_time_points = {
      ArrayFromJSON(timestamp(TimeUnit::SECOND, timezone), json_second),
      ArrayFromJSON(timestamp(TimeUnit::MILLI, timezone), json_milli),
      ArrayFromJSON(timestamp(TimeUnit::MICRO, timezone), json_micro),
      ArrayFromJSON(timestamp(TimeUnit::NANO, timezone), json_nano)};

  for (auto time_points : all_time_points) {
    CheckScalarUnary("year", time_points, year);
    CheckScalarUnary("month", time_points, month);
    CheckScalarUnary("day", time_points, day);
    CheckScalarUnary("day_of_year", time_points, day_of_year);
    CheckScalarUnary("week", time_points, week);
    CheckScalarUnary("quarter", time_points, quarter);
    CheckScalarUnary("day_of_week", time_points, day_of_week);
    CheckScalarUnary("hour", time_points, hour);
    CheckScalarUnary("minute", time_points, minute);
    CheckScalarUnary("second", time_points, second);
    CheckScalarUnary("millisecond", time_points, millisecond);
    CheckScalarUnary("microsecond", time_points, microsecond);
    CheckScalarUnary("nanosecond", time_points, nanosecond);
  }
}
}  // namespace compute
}  // namespace arrow

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
          "1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000", null])";
  auto unit = timestamp(TimeUnit::NANO);
  auto timestamps = ArrayFromJSON(unit, times);
  auto iso_calendar_type =
      struct_({field("iso_year", int64()), field("iso_week", int64()),
               field("day_of_week", int64())});

  auto year = "[1970, 2000, 1899, 2033, null]";
  auto month = "[1, 2, 1, 5, null]";
  auto day = "[1, 29, 1, 18, null]";
  auto day_of_week = "[4, 2, 7, 3, null]";
  auto day_of_year = "[1, 60, 1, 138, null]";
  auto iso_year = "[1970, 2000, 1899, 2033, null]";
  auto iso_week = "[1, 9, 52, 20, null]";
  auto iso_calendar =
      ArrayFromJSON(iso_calendar_type,
                    R"([{"iso_year": 1970, "iso_week": 1, "day_of_week": 4},
                        {"iso_year": 2000, "iso_week": 9, "day_of_week": 2},
                        {"iso_year": 1899, "iso_week": 52, "day_of_week": 7},
                        {"iso_year": 2033, "iso_week": 20, "day_of_week": 3}, null])");
  auto quarter = "[1, 1, 1, 2, null]";
  auto hour = "[0, 23, 0, 3, null]";
  auto minute = "[0, 23, 59, 33, null]";
  auto second = "[59.123456789, 23.999999999, 20.001001001, 20.0, null]";
  auto millisecond = "[123, 999, 1, 0, null]";
  auto microsecond = "[456, 999, 1, 0, null]";
  auto nanosecond = "[789, 999, 1, 0, null]";
  auto subsecond = "[123456789, 999999999, 1001001, 0, null]";

  CheckScalarUnary("year", unit, times, int64(), year);
  CheckScalarUnary("month", unit, times, int64(), month);
  CheckScalarUnary("day", unit, times, int64(), day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("iso_year", unit, times, int64(), iso_year);
  CheckScalarUnary("iso_week", unit, times, int64(), iso_week);
  CheckScalarUnary("iso_calendar", timestamps, iso_calendar);
  CheckScalarUnary("quarter", unit, times, int64(), quarter);
  CheckScalarUnary("hour", unit, times, int64(), hour);
  CheckScalarUnary("minute", unit, times, int64(), minute);
  CheckScalarUnary("second", unit, times, float64(), second);
  CheckScalarUnary("millisecond", unit, times, int64(), millisecond);
  CheckScalarUnary("microsecond", unit, times, int64(), microsecond);
  CheckScalarUnary("nanosecond", unit, times, int64(), nanosecond);
  CheckScalarUnary("subsecond", unit, times, int64(), subsecond);
}

TEST(ScalarTemporalTest, TestZonedTemporalComponentExtraction) {
  std::string timezone = "UTC-2";
  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000", null])";
  auto unit = timestamp(TimeUnit::NANO, timezone);
  auto timestamps = ArrayFromJSON(unit, times);

  ASSERT_RAISES(Invalid, Year(timestamps));
  ASSERT_RAISES(Invalid, Month(timestamps));
  ASSERT_RAISES(Invalid, Day(timestamps));
  ASSERT_RAISES(Invalid, DayOfWeek(timestamps));
  ASSERT_RAISES(Invalid, DayOfYear(timestamps));
  ASSERT_RAISES(Invalid, ISOYear(timestamps));
  ASSERT_RAISES(Invalid, ISOWeek(timestamps));
  ASSERT_RAISES(Invalid, ISOCalendar(timestamps));
  ASSERT_RAISES(Invalid, Quarter(timestamps));
  ASSERT_RAISES(Invalid, Hour(timestamps));
  ASSERT_RAISES(Invalid, Minute(timestamps));
  ASSERT_RAISES(Invalid, Second(timestamps));
  ASSERT_RAISES(Invalid, Millisecond(timestamps));
  ASSERT_RAISES(Invalid, Microsecond(timestamps));
  ASSERT_RAISES(Invalid, Nanosecond(timestamps));
  ASSERT_RAISES(Invalid, Subsecond(timestamps));
}
}  // namespace compute
}  // namespace arrow

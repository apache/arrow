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
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"

namespace arrow {

using internal::StringFormatter;

class ScalarTemporalTest : public ::testing::Test {
 public:
  const char* date32s =
      R"([0,
      11016,
      -25932,
      23148,
      18262,
      18261,
      18260,
      14609,
      14610,
      14612,
      14613,
      13149,
      13148,
      14241,
      14242,
      15340,
      null])";

  const char* date64s =
      R"([0,
      951782400000,
      -2240524800000,
      1999987200000,
      1577836800000,
      1577750400000,
      1577664000000,
      1262217600000,
      1262304000000,
      1262476800000,
      1262563200000,
      1136073600000,
      1135987200000,
      1230422400000,
      1230508800000,
      1325376000000,
      null])";

  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000",
          "2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
          "2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
          "2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])";
  const char* times_seconds_precision =
      R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "1899-01-01T00:59:20","2033-05-18T03:33:20",
          "2020-01-01T01:05:05", "2019-12-31T02:10:10",
          "2019-12-30T03:15:15", "2009-12-31T04:20:20",
          "2010-01-01T05:25:25", "2010-01-03T06:30:30",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40",
          "2005-12-31T09:45:45", "2008-12-28", "2008-12-29",
          "2012-01-01 01:02:03", null])";
  std::shared_ptr<arrow::DataType> iso_calendar_type =
      struct_({field("iso_year", int64()), field("iso_week", int64()),
               field("iso_day_of_week", int64())});
  std::shared_ptr<arrow::Array> iso_calendar =
      ArrayFromJSON(iso_calendar_type,
                    R"([{"iso_year": 1970, "iso_week": 1, "iso_day_of_week": 4},
                      {"iso_year": 2000, "iso_week": 9, "iso_day_of_week": 2},
                      {"iso_year": 1898, "iso_week": 52, "iso_day_of_week": 7},
                      {"iso_year": 2033, "iso_week": 20, "iso_day_of_week": 3},
                      {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 3},
                      {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 2},
                      {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 1},
                      {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 4},
                      {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 5},
                      {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 7},
                      {"iso_year": 2010, "iso_week": 1, "iso_day_of_week": 1},
                      {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 7},
                      {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 6},
                      {"iso_year": 2008, "iso_week": 52, "iso_day_of_week": 7},
                      {"iso_year": 2009, "iso_week": 1, "iso_day_of_week": 1},
                      {"iso_year": 2011, "iso_week": 52, "iso_day_of_week": 7}, null])");
  std::string year =
      "[1970, 2000, 1899, 2033, 2020, 2019, 2019, 2009, 2010, 2010, 2010, 2006, "
      "2005, 2008, 2008, 2012, null]";
  std::string month = "[1, 2, 1, 5, 1, 12, 12, 12, 1, 1, 1, 1, 12, 12, 12, 1, null]";
  std::string day = "[1, 29, 1, 18, 1, 31, 30, 31, 1, 3, 4, 1, 31, 28, 29, 1, null]";
  std::string day_of_week = "[3, 1, 6, 2, 2, 1, 0, 3, 4, 6, 0, 6, 5, 6, 0, 6, null]";
  std::string day_of_year =
      "[1, 60, 1, 138, 1, 365, 364, 365, 1, 3, 4, 1, 365, 363, 364, 1, null]";
  std::string iso_year =
      "[1970, 2000, 1898, 2033, 2020, 2020, 2020, 2009, 2009, 2009, 2010, 2005, "
      "2005, 2008, 2009, 2011, null]";
  std::string iso_week =
      "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";

  std::string quarter = "[1, 1, 1, 2, 1, 4, 4, 4, 1, 1, 1, 1, 4, 4, 4, 1, null]";
  std::string hour = "[0, 23, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 1, null]";
  std::string minute =
      "[0, 23, 59, 33, 5, 10, 15, 20, 25, 30, 35, 40, 45, 0, 0, 2, null]";
  std::string second =
      "[59, 23, 20, 20, 5, 10, 15, 20, 25, 30, 35, 40, 45, 0, 0, 3, null]";
  std::string millisecond = "[123, 999, 1, 0, 1, 2, 3, 4, 5, 6, 0, 0, 0, 0, 0, 0, null]";
  std::string microsecond =
      "[456, 999, 1, 0, 0, 0, 0, 132, 321, 163, 0, 0, 0, 0, 0, 0, null]";
  std::string nanosecond = "[789, 999, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null]";
  std::string subsecond =
      "[0.123456789, 0.999999999, 0.001001001, 0, 0.001, 0.002, 0.003, 0.004132, "
      "0.005321, 0.006163, 0, 0, 0, 0, 0, 0, null]";
  std::string zeros = "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null]";
};

namespace compute {

TEST_F(ScalarTemporalTest, TestTemporalComponentExtractionAllTemporalTypes) {
  std::vector<std::shared_ptr<DataType>> units = {date32(), date64(),
                                                  timestamp(TimeUnit::NANO)};
  std::vector<const char*> samples = {date32s, date64s, times};
  DCHECK_EQ(units.size(), samples.size());
  for (size_t i = 0; i < samples.size(); ++i) {
    auto unit = units[i];
    auto sample = samples[i];
    CheckScalarUnary("year", unit, sample, int64(), year);
    CheckScalarUnary("month", unit, sample, int64(), month);
    CheckScalarUnary("day", unit, sample, int64(), day);
    CheckScalarUnary("day_of_week", unit, sample, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, sample, int64(), day_of_year);
    CheckScalarUnary("iso_year", unit, sample, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, sample, int64(), iso_week);
    CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, sample), iso_calendar);
    CheckScalarUnary("quarter", unit, sample, int64(), quarter);
    if (unit->id() == Type::TIMESTAMP) {
      CheckScalarUnary("hour", unit, sample, int64(), hour);
      CheckScalarUnary("minute", unit, sample, int64(), minute);
      CheckScalarUnary("second", unit, sample, int64(), second);
      CheckScalarUnary("millisecond", unit, sample, int64(), millisecond);
      CheckScalarUnary("microsecond", unit, sample, int64(), microsecond);
      CheckScalarUnary("nanosecond", unit, sample, int64(), nanosecond);
      CheckScalarUnary("subsecond", unit, sample, float64(), subsecond);
    }
  }
}

TEST_F(ScalarTemporalTest, TestTemporalComponentExtractionWithDifferentUnits) {
  for (auto u : internal::AllTimeUnits()) {
    auto unit = timestamp(u);
    CheckScalarUnary("year", unit, times_seconds_precision, int64(), year);
    CheckScalarUnary("month", unit, times_seconds_precision, int64(), month);
    CheckScalarUnary("day", unit, times_seconds_precision, int64(), day);
    CheckScalarUnary("day_of_week", unit, times_seconds_precision, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, times_seconds_precision, int64(), day_of_year);
    CheckScalarUnary("iso_year", unit, times_seconds_precision, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, times_seconds_precision, int64(), iso_week);
    CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, times_seconds_precision),
                     iso_calendar);
    CheckScalarUnary("quarter", unit, times_seconds_precision, int64(), quarter);
    CheckScalarUnary("hour", unit, times_seconds_precision, int64(), hour);
    CheckScalarUnary("minute", unit, times_seconds_precision, int64(), minute);
    CheckScalarUnary("second", unit, times_seconds_precision, int64(), second);
    CheckScalarUnary("millisecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("microsecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("nanosecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("subsecond", unit, times_seconds_precision, float64(), zeros);
  }
}

TEST_F(ScalarTemporalTest, TestOutsideNanosecondRange) {
  const char* times = R"(["1677-09-20T00:00:59.123456", "2262-04-13T23:23:23.999999"])";

  auto unit = timestamp(TimeUnit::MICRO);
  auto iso_calendar_type =
      struct_({field("iso_year", int64()), field("iso_week", int64()),
               field("iso_day_of_week", int64())});

  auto year = "[1677, 2262]";
  auto month = "[9, 4]";
  auto day = "[20, 13]";
  auto day_of_week = "[0, 6]";
  auto day_of_year = "[263, 103]";
  auto iso_year = "[1677, 2262]";
  auto iso_week = "[38, 15]";
  auto iso_calendar =
      ArrayFromJSON(iso_calendar_type,
                    R"([{"iso_year": 1677, "iso_week": 38, "iso_day_of_week": 1},
                          {"iso_year": 2262, "iso_week": 15, "iso_day_of_week": 7}])");
  auto quarter = "[3, 2]";
  auto hour = "[0, 23]";
  auto minute = "[0, 23]";
  auto second = "[59, 23]";
  auto millisecond = "[123, 999]";
  auto microsecond = "[456, 999]";
  auto nanosecond = "[0, 0]";
  auto subsecond = "[0.123456, 0.999999]";

  CheckScalarUnary("year", unit, times, int64(), year);
  CheckScalarUnary("month", unit, times, int64(), month);
  CheckScalarUnary("day", unit, times, int64(), day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("iso_year", unit, times, int64(), iso_year);
  CheckScalarUnary("iso_week", unit, times, int64(), iso_week);
  CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, times), iso_calendar);
  CheckScalarUnary("quarter", unit, times, int64(), quarter);
  CheckScalarUnary("hour", unit, times, int64(), hour);
  CheckScalarUnary("minute", unit, times, int64(), minute);
  CheckScalarUnary("second", unit, times, int64(), second);
  CheckScalarUnary("millisecond", unit, times, int64(), millisecond);
  CheckScalarUnary("microsecond", unit, times, int64(), microsecond);
  CheckScalarUnary("nanosecond", unit, times, int64(), nanosecond);
  CheckScalarUnary("subsecond", unit, times, float64(), subsecond);
}

#ifndef _WIN32
// TODO: We should test on windows once ARROW-13168 is resolved.
TEST_F(ScalarTemporalTest, TestZoned1) {
  auto unit = timestamp(TimeUnit::NANO, "Pacific/Marquesas");
  auto iso_calendar_type =
      struct_({field("iso_year", int64()), field("iso_week", int64()),
               field("iso_day_of_week", int64())});
  auto year =
      "[1969, 2000, 1898, 2033, 2019, 2019, 2019, 2009, 2009, 2010, 2010, 2005, 2005, "
      "2008, 2008, 2011, null]";
  auto month = "[12, 2, 12, 5, 12, 12, 12, 12, 12, 1, 1, 12, 12, 12, 12, 12, null]";
  auto day = "[31, 29, 31, 17, 31, 30, 29, 30, 31, 2, 3, 31, 31, 27, 28, 31, null]";
  auto day_of_week = "[2, 1, 5, 1, 1, 0, 6, 2, 3, 5, 6, 5, 5, 5, 6, 5, null]";
  auto day_of_year =
      "[365, 60, 365, 137, 365, 364, 363, 364, 365, 2, 3, 365, 365, 362, 363, 365, null]";
  auto iso_year =
      "[1970, 2000, 1898, 2033, 2020, 2020, 2019, 2009, 2009, 2009, 2009, 2005, 2005, "
      "2008, 2008, 2011, null]";
  auto iso_week = "[1, 9, 52, 20, 1, 1, 52, 53, 53, 53, 53, 52, 52, 52, 52, 52, null]";
  auto iso_calendar =
      ArrayFromJSON(iso_calendar_type,
                    R"([{"iso_year": 1970, "iso_week": 1, "iso_day_of_week": 3},
                        {"iso_year": 2000, "iso_week": 9, "iso_day_of_week": 2},
                        {"iso_year": 1898, "iso_week": 52, "iso_day_of_week": 6},
                        {"iso_year": 2033, "iso_week": 20, "iso_day_of_week": 2},
                        {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 2},
                        {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 1},
                        {"iso_year": 2019, "iso_week": 52, "iso_day_of_week": 7},
                        {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 3},
                        {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 4},
                        {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 6},
                        {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 7},
                        {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 6},
                        {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 6},
                        {"iso_year": 2008, "iso_week": 52, "iso_day_of_week": 6},
                        {"iso_year": 2008, "iso_week": 52, "iso_day_of_week": 7},
                        {"iso_year": 2011, "iso_week": 52, "iso_day_of_week": 6}, null])");
  auto quarter = "[4, 1, 4, 2, 4, 4, 4, 4, 4, 1, 1, 4, 4, 4, 4, 4, null]";
  auto hour = "[14, 13, 15, 18, 15, 16, 17, 18, 19, 21, 22, 23, 0, 14, 14, 15, null]";
  auto minute = "[30, 53, 41, 3, 35, 40, 45, 50, 55, 0, 5, 10, 15, 30, 30, 32, null]";

  CheckScalarUnary("year", unit, times, int64(), year);
  CheckScalarUnary("month", unit, times, int64(), month);
  CheckScalarUnary("day", unit, times, int64(), day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("iso_year", unit, times, int64(), iso_year);
  CheckScalarUnary("iso_week", unit, times, int64(), iso_week);
  CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, times), iso_calendar);
  CheckScalarUnary("quarter", unit, times, int64(), quarter);
  CheckScalarUnary("hour", unit, times, int64(), hour);
  CheckScalarUnary("minute", unit, times, int64(), minute);
  CheckScalarUnary("second", unit, times, int64(), second);
  CheckScalarUnary("millisecond", unit, times, int64(), millisecond);
  CheckScalarUnary("microsecond", unit, times, int64(), microsecond);
  CheckScalarUnary("nanosecond", unit, times, int64(), nanosecond);
  CheckScalarUnary("subsecond", unit, times, float64(), subsecond);
}

TEST_F(ScalarTemporalTest, TestZoned2) {
  for (auto u : internal::AllTimeUnits()) {
    auto unit = timestamp(u, "Australia/Broken_Hill");
    auto iso_calendar_type =
        struct_({field("iso_year", int64()), field("iso_week", int64()),
                 field("iso_day_of_week", int64())});
    auto year =
        "[1970, 2000, 1899, 2033, 2020, 2019, 2019, 2009, 2010, 2010, 2010, 2006, 2005, "
        "2008, 2008, 2012, null]";
    auto month = "[1, 3, 1, 5, 1, 12, 12, 12, 1, 1, 1, 1, 12, 12, 12, 1, null]";
    auto day = "[1, 1, 1, 18, 1, 31, 30, 31, 1, 3, 4, 1, 31, 28, 29, 1, null]";
    auto day_of_week = "[3, 2, 6, 2, 2, 1, 0, 3, 4, 6, 0, 6, 5, 6, 0, 6, null]";
    auto day_of_year =
        "[1, 61, 1, 138, 1, 365, 364, 365, 1, 3, 4, 1, 365, 363, 364, 1, null]";
    auto iso_year =
        "[1970, 2000, 1898, 2033, 2020, 2020, 2020, 2009, 2009, 2009, 2010, 2005, 2005, "
        "2008, 2009, 2011, null]";
    auto iso_week = "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";
    auto iso_calendar =
        ArrayFromJSON(iso_calendar_type,
                      R"([{"iso_year": 1970, "iso_week": 1, "iso_day_of_week": 4},
                          {"iso_year": 2000, "iso_week": 9, "iso_day_of_week": 3},
                          {"iso_year": 1898, "iso_week": 52, "iso_day_of_week": 7},
                          {"iso_year": 2033, "iso_week": 20, "iso_day_of_week": 3},
                          {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 3},
                          {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 2},
                          {"iso_year": 2020, "iso_week": 1, "iso_day_of_week": 1},
                          {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 4},
                          {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 5},
                          {"iso_year": 2009, "iso_week": 53, "iso_day_of_week": 7},
                          {"iso_year": 2010, "iso_week": 1, "iso_day_of_week": 1},
                          {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 7},
                          {"iso_year": 2005, "iso_week": 52, "iso_day_of_week": 6},
                          {"iso_year": 2008, "iso_week": 52, "iso_day_of_week": 7},
                          {"iso_year": 2009, "iso_week": 1, "iso_day_of_week": 1},
                          {"iso_year": 2011, "iso_week": 52, "iso_day_of_week": 7}, null])");
    auto quarter = "[1, 1, 1, 2, 1, 4, 4, 4, 1, 1, 1, 1, 4, 4, 4, 1, null]";
    auto hour = "[9, 9, 9, 13, 11, 12, 13, 14, 15, 17, 18, 19, 20, 10, 10, 11, null]";
    auto minute = "[30, 53, 59, 3, 35, 40, 45, 50, 55, 0, 5, 10, 15, 30, 30, 32, null]";

    CheckScalarUnary("year", unit, times_seconds_precision, int64(), year);
    CheckScalarUnary("month", unit, times_seconds_precision, int64(), month);
    CheckScalarUnary("day", unit, times_seconds_precision, int64(), day);
    CheckScalarUnary("day_of_week", unit, times_seconds_precision, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, times_seconds_precision, int64(), day_of_year);
    CheckScalarUnary("iso_year", unit, times_seconds_precision, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, times_seconds_precision, int64(), iso_week);
    CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, times_seconds_precision),
                     iso_calendar);
    CheckScalarUnary("quarter", unit, times_seconds_precision, int64(), quarter);
    CheckScalarUnary("hour", unit, times_seconds_precision, int64(), hour);
    CheckScalarUnary("minute", unit, times_seconds_precision, int64(), minute);
    CheckScalarUnary("second", unit, times_seconds_precision, int64(), second);
    CheckScalarUnary("millisecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("microsecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("nanosecond", unit, times_seconds_precision, int64(), zeros);
    CheckScalarUnary("subsecond", unit, times_seconds_precision, float64(), zeros);
  }
}

TEST_F(ScalarTemporalTest, TestNonexistentTimezone) {
  auto data_buffer = Buffer::Wrap(std::vector<int32_t>{1, 2, 3});
  auto null_buffer = Buffer::FromString("\xff");

  for (auto u : internal::AllTimeUnits()) {
    auto ts_type = timestamp(u, "Mars/Mariner_Valley");
    auto timestamp_array = std::make_shared<NumericArray<TimestampType>>(
        ts_type, 2, data_buffer, null_buffer, 0);
    ASSERT_RAISES(Invalid, Year(timestamp_array));
    ASSERT_RAISES(Invalid, Month(timestamp_array));
    ASSERT_RAISES(Invalid, Day(timestamp_array));
    ASSERT_RAISES(Invalid, DayOfWeek(timestamp_array));
    ASSERT_RAISES(Invalid, DayOfYear(timestamp_array));
    ASSERT_RAISES(Invalid, ISOYear(timestamp_array));
    ASSERT_RAISES(Invalid, ISOWeek(timestamp_array));
    ASSERT_RAISES(Invalid, ISOCalendar(timestamp_array));
    ASSERT_RAISES(Invalid, Quarter(timestamp_array));
    ASSERT_RAISES(Invalid, Hour(timestamp_array));
    ASSERT_RAISES(Invalid, Minute(timestamp_array));
    ASSERT_RAISES(Invalid, Second(timestamp_array));
    ASSERT_RAISES(Invalid, Millisecond(timestamp_array));
    ASSERT_RAISES(Invalid, Microsecond(timestamp_array));
    ASSERT_RAISES(Invalid, Nanosecond(timestamp_array));
    ASSERT_RAISES(Invalid, Subsecond(timestamp_array));
  }
}
#endif

TEST_F(ScalarTemporalTest, DayOfWeek) {
  auto unit = timestamp(TimeUnit::NANO);

  auto timestamps = ArrayFromJSON(unit, times);
  auto day_of_week_week_start_7_zero_based =
      "[4, 2, 0, 3, 3, 2, 1, 4, 5, 0, 1, 0, 6, 0, 1, 0, null]";
  auto day_of_week_week_start_2_zero_based =
      "[2, 0, 5, 1, 1, 0, 6, 2, 3, 5, 6, 5, 4, 5, 6, 5, null]";
  auto day_of_week_week_start_7_one_based =
      "[5, 3, 1, 4, 4, 3, 2, 5, 6, 1, 2, 1, 7, 1, 2, 1, null]";
  auto day_of_week_week_start_2_one_based =
      "[3, 1, 6, 2, 2, 1, 7, 3, 4, 6, 7, 6, 5, 6, 7, 6, null]";

  auto expected_70 = ArrayFromJSON(int64(), day_of_week_week_start_7_zero_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_70,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*one_based_numbering=*/false, /*week_start=*/7)));
  ASSERT_TRUE(result_70.Equals(expected_70));

  auto expected_20 = ArrayFromJSON(int64(), day_of_week_week_start_2_zero_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_20,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*one_based_numbering=*/false, /*week_start=*/2)));
  ASSERT_TRUE(result_20.Equals(expected_20));

  auto expected_71 = ArrayFromJSON(int64(), day_of_week_week_start_7_one_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_71,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*one_based_numbering=*/true, /*week_start=*/7)));
  ASSERT_TRUE(result_71.Equals(expected_71));

  auto expected_21 = ArrayFromJSON(int64(), day_of_week_week_start_2_one_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_21,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*one_based_numbering=*/true, /*week_start=*/2)));
  ASSERT_TRUE(result_21.Equals(expected_21));

  ASSERT_RAISES(Invalid,
                DayOfWeek(timestamps, DayOfWeekOptions(/*one_based_numbering=*/true,
                                                       /*week_start=*/0)));
  ASSERT_RAISES(Invalid,
                DayOfWeek(timestamps, DayOfWeekOptions(/*one_based_numbering=*/false,
                                                       /*week_start=*/8)));
}

// TODO: We should test on windows once ARROW-13168 is resolved.
#ifndef _WIN32
TEST_F(ScalarTemporalTest, TestAssumeTimezone) {
  std::string timezone_utc = "UTC";
  std::string timezone_kolkata = "Asia/Kolkata";
  std::string timezone_us_central = "US/Central";
  const char* times_utc = R"(["1970-01-01T00:00:00", null])";
  const char* times_kolkata = R"(["1970-01-01T05:30:00", null])";
  const char* times_us_central = R"(["1969-12-31T18:00:00", null])";
  auto options_utc = AssumeTimezoneOptions(timezone_utc);
  auto options_kolkata = AssumeTimezoneOptions(timezone_kolkata);
  auto options_us_central = AssumeTimezoneOptions(timezone_us_central);
  auto options_invalid = AssumeTimezoneOptions("Europe/Brusselsss");

  for (auto u : internal::AllTimeUnits()) {
    auto unit = timestamp(u);
    auto unit_utc = timestamp(u, timezone_utc);
    auto unit_kolkata = timestamp(u, timezone_kolkata);
    auto unit_us_central = timestamp(u, timezone_us_central);

    CheckScalarUnary("assume_timezone", unit, times_utc, unit_utc, times_utc,
                     &options_utc);
    CheckScalarUnary("assume_timezone", unit, times_kolkata, unit_kolkata, times_utc,
                     &options_kolkata);
    CheckScalarUnary("assume_timezone", unit, times_us_central, unit_us_central,
                     times_utc, &options_us_central);
    ASSERT_RAISES(Invalid,
                  AssumeTimezone(ArrayFromJSON(unit_kolkata, times_utc), options_utc));
    ASSERT_RAISES(Invalid,
                  AssumeTimezone(ArrayFromJSON(unit, times_utc), options_invalid));
  }
}

TEST_F(ScalarTemporalTest, TestAssumeTimezoneAmbiguous) {
  std::string timezone = "CET";
  const char* times = R"(["2018-10-28 01:20:00",
                          "2018-10-28 02:36:00",
                          "2018-10-28 03:46:00"])";
  const char* times_earliest = R"(["2018-10-27 23:20:00",
                                   "2018-10-28 00:36:00",
                                   "2018-10-28 02:46:00"])";
  const char* times_latest = R"(["2018-10-27 23:20:00",
                                 "2018-10-28 01:36:00",
                                 "2018-10-28 02:46:00"])";

  auto options_earliest =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_EARLIEST);
  auto options_latest =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_LATEST);
  auto options_raise =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_RAISE);

  for (auto u : internal::AllTimeUnits()) {
    auto unit = timestamp(u);
    auto unit_local = timestamp(u, timezone);
    ASSERT_RAISES(Invalid, AssumeTimezone(ArrayFromJSON(unit, times), options_raise));
    CheckScalarUnary("assume_timezone", unit, times, unit_local, times_earliest,
                     &options_earliest);
    CheckScalarUnary("assume_timezone", unit, times, unit_local, times_latest,
                     &options_latest);
  }
}

TEST_F(ScalarTemporalTest, TestAssumeTimezoneNonexistent) {
  std::string timezone = "Europe/Warsaw";
  const char* times = R"(["2015-03-29 02:30:00", "2015-03-29 03:30:00"])";
  const char* times_latest = R"(["2015-03-29 01:00:00", "2015-03-29 01:30:00"])";
  const char* times_earliest = R"(["2015-03-29 00:59:59", "2015-03-29 01:30:00"])";
  const char* times_earliest_milli =
      R"(["2015-03-29 00:59:59.999", "2015-03-29 01:30:00"])";
  const char* times_earliest_micro =
      R"(["2015-03-29 00:59:59.999999", "2015-03-29 01:30:00"])";
  const char* times_earliest_nano =
      R"(["2015-03-29 00:59:59.999999999", "2015-03-29 01:30:00"])";

  auto options_raise =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_RAISE,
                            AssumeTimezoneOptions::NONEXISTENT_RAISE);
  auto options_latest =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_RAISE,
                            AssumeTimezoneOptions::NONEXISTENT_LATEST);
  auto options_earliest =
      AssumeTimezoneOptions(timezone, AssumeTimezoneOptions::AMBIGUOUS_RAISE,
                            AssumeTimezoneOptions::NONEXISTENT_EARLIEST);

  for (auto u : internal::AllTimeUnits()) {
    auto unit = timestamp(u);
    auto unit_local = timestamp(u, timezone);
    ASSERT_RAISES(Invalid, AssumeTimezone(ArrayFromJSON(unit, times), options_raise));
    CheckScalarUnary("assume_timezone", unit, times, unit_local, times_latest,
                     &options_latest);
  }
  CheckScalarUnary("assume_timezone", timestamp(TimeUnit::SECOND), times,
                   timestamp(TimeUnit::SECOND, timezone), times_earliest,
                   &options_earliest);
  CheckScalarUnary("assume_timezone", timestamp(TimeUnit::MILLI), times,
                   timestamp(TimeUnit::MILLI, timezone), times_earliest_milli,
                   &options_earliest);
  CheckScalarUnary("assume_timezone", timestamp(TimeUnit::MICRO), times,
                   timestamp(TimeUnit::MICRO, timezone), times_earliest_micro,
                   &options_earliest);
  CheckScalarUnary("assume_timezone", timestamp(TimeUnit::NANO), times,
                   timestamp(TimeUnit::NANO, timezone), times_earliest_nano,
                   &options_earliest);
}

TEST_F(ScalarTemporalTest, Strftime) {
  auto options_default = StrftimeOptions();
  auto options = StrftimeOptions("%Y-%m-%dT%H:%M:%S%z");

  const char* seconds = R"(["1970-01-01T00:00:59", "2021-08-18T15:11:50", null])";
  const char* milliseconds = R"(["1970-01-01T00:00:59.123", null])";
  const char* microseconds = R"(["1970-01-01T00:00:59.123456", null])";
  const char* nanoseconds = R"(["1970-01-01T00:00:59.123456789", null])";

  const char* default_seconds = R"(
      ["1970-01-01T00:00:59", "2021-08-18T15:11:50", null])";
  const char* string_seconds = R"(
      ["1970-01-01T00:00:59+0000", "2021-08-18T15:11:50+0000", null])";
  const char* string_milliseconds = R"(["1970-01-01T00:00:59.123+0000", null])";
  const char* string_microseconds = R"(["1970-01-01T05:30:59.123456+0530", null])";
  const char* string_nanoseconds = R"(["1969-12-31T14:00:59.123456789-1000", null])";

  CheckScalarUnary("strftime", timestamp(TimeUnit::SECOND, "UTC"), seconds, utf8(),
                   default_seconds, &options_default);
  CheckScalarUnary("strftime", timestamp(TimeUnit::SECOND, "UTC"), seconds, utf8(),
                   string_seconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::MILLI, "GMT"), milliseconds, utf8(),
                   string_milliseconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::MICRO, "Asia/Kolkata"), microseconds,
                   utf8(), string_microseconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::NANO, "US/Hawaii"), nanoseconds,
                   utf8(), string_nanoseconds, &options);
}

TEST_F(ScalarTemporalTest, StrftimeNoTimezone) {
  auto options_default = StrftimeOptions();
  const char* seconds = R"(["1970-01-01T00:00:59", null])";
  auto arr = ArrayFromJSON(timestamp(TimeUnit::SECOND), seconds);

  CheckScalarUnary("strftime", timestamp(TimeUnit::SECOND), seconds, utf8(), seconds,
                   &options_default);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr, StrftimeOptions("%Y-%m-%dT%H:%M:%S%z")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr, StrftimeOptions("%Y-%m-%dT%H:%M:%S%Z")));
}

TEST_F(ScalarTemporalTest, StrftimeInvalidTimezone) {
  const char* seconds = R"(["1970-01-01T00:00:59", null])";
  auto arr = ArrayFromJSON(timestamp(TimeUnit::SECOND, "non-existent"), seconds);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Cannot locate timezone 'non-existent'"),
      Strftime(arr, StrftimeOptions()));
}

TEST_F(ScalarTemporalTest, StrftimeCLocale) {
  auto options_default = StrftimeOptions();
  auto options = StrftimeOptions("%Y-%m-%dT%H:%M:%S%z", "C");
  auto options_locale_specific = StrftimeOptions("%a", "C");

  const char* seconds = R"(["1970-01-01T00:00:59", null])";
  const char* milliseconds = R"(["1970-01-01T00:00:59.123", null])";
  const char* microseconds = R"(["1970-01-01T00:00:59.123456", null])";
  const char* nanoseconds = R"(["1970-01-01T00:00:59.123456789", null])";

  const char* default_seconds = R"(["1970-01-01T00:00:59", null])";
  const char* string_seconds = R"(["1970-01-01T00:00:59+0000", null])";
  const char* string_milliseconds = R"(["1970-01-01T00:00:59.123+0000", null])";
  const char* string_microseconds = R"(["1970-01-01T05:30:59.123456+0530", null])";
  const char* string_nanoseconds = R"(["1969-12-31T14:00:59.123456789-1000", null])";

  const char* string_locale_specific = R"(["Wed", null])";

  CheckScalarUnary("strftime", timestamp(TimeUnit::SECOND, "UTC"), seconds, utf8(),
                   default_seconds, &options_default);
  CheckScalarUnary("strftime", timestamp(TimeUnit::SECOND, "UTC"), seconds, utf8(),
                   string_seconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::MILLI, "GMT"), milliseconds, utf8(),
                   string_milliseconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::MICRO, "Asia/Kolkata"), microseconds,
                   utf8(), string_microseconds, &options);
  CheckScalarUnary("strftime", timestamp(TimeUnit::NANO, "US/Hawaii"), nanoseconds,
                   utf8(), string_nanoseconds, &options);

  CheckScalarUnary("strftime", timestamp(TimeUnit::NANO, "US/Hawaii"), nanoseconds,
                   utf8(), string_locale_specific, &options_locale_specific);
}

TEST_F(ScalarTemporalTest, StrftimeOtherLocale) {
  if (!LocaleExists("fr_FR.UTF-8")) {
    GTEST_SKIP() << "locale 'fr_FR.UTF-8' doesn't exist on this system";
  }

  auto options = StrftimeOptions("%d %B %Y %H:%M:%S", "fr_FR.UTF-8");
  const char* milliseconds = R"(
      ["1970-01-01T00:00:59.123", "2021-08-18T15:11:50.456", null])";
  const char* expected = R"(
      ["01 janvier 1970 00:00:59,123", "18 août 2021 15:11:50,456", null])";
  CheckScalarUnary("strftime", timestamp(TimeUnit::MILLI, "UTC"), milliseconds, utf8(),
                   expected, &options);
}

TEST_F(ScalarTemporalTest, StrftimeInvalidLocale) {
  auto options = StrftimeOptions("%d %B %Y %H:%M:%S", "non-existent");
  const char* seconds = R"(["1970-01-01T00:00:59", null])";
  auto arr = ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), seconds);

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Cannot find locale 'non-existent'"),
                                  Strftime(arr, options));
}
#endif  // !_WIN32

}  // namespace compute
}  // namespace arrow

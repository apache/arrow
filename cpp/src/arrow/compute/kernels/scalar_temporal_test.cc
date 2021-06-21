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
  std::shared_ptr<arrow::Array> iso_calendar = ArrayFromJSON(iso_calendar_type,
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

TEST_F(ScalarTemporalTest, TestTemporalComponentExtraction) {
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

TEST(ScalarTemporalTest, TestOutsideNanosecondRange) {
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
TEST(ScalarTemporalTest, TestZoned1) {
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

TEST(ScalarTemporalTest, TestZoned2) {
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
    // TODO: pandas expects [9, 9, 10, ...
    auto hour = "[9, 9, 9, 13, 11, 12, 13, 14, 15, 17, 18, 19, 20, 10, 10, 11, null]";
    // TODO: pandas expects [30, 53, 25, ...
    auto minute = "[30, 53, 59, 3, 35, 40, 45, 50, 55, 0, 5, 10, 15, 30, 30, 32, null]";

    CheckScalarUnary("year", unit, times_seconds_precision, int64(), year);
    CheckScalarUnary("month", unit, times_seconds_precision, int64(), month);
    CheckScalarUnary("day", unit, times_seconds_precision, int64(), day);
    CheckScalarUnary("day_of_week", unit, times_seconds_precision, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, times_seconds_precision, int64(), day_of_year);
    CheckScalarUnary("iso_year", unit, times_seconds_precision, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, times_seconds_precision, int64(), iso_week);
    CheckScalarUnary("iso_calendar", ArrayFromJSON(unit, times_seconds_precision), iso_calendar);
    CheckScalarUnary("quarter", unit, times_seconds_precision, int64(), quarter);
    CheckScalarUnary("hour", unit, times_seconds_precision, int64(), hour);
    CheckScalarUnary("minute", unit, times_seconds_precision, int64(), minute);
    CheckScalarUnary("second", unit, times_seconds_precision, float64(), second);
    CheckScalarUnary("millisecond", unit, times_seconds_precision, int64(), millisecond);
    CheckScalarUnary("microsecond", unit, times_seconds_precision, int64(), microsecond);
    CheckScalarUnary("nanosecond", unit, times_seconds_precision, int64(), nanosecond);
    CheckScalarUnary("subsecond", unit, times_seconds_precision, float64(), subsecond);
  }
}

TEST_F(ScalarTemporalTest, DayOfWeek) {
  auto unit = timestamp(TimeUnit::NANO);

  auto timestamps = ArrayFromJSON(unit, times);
  auto day_of_week_week_start_7_zero_based =
      "[4, 2, 0, 3, null, 3, 2, 1, 4, 5, 0, 1, 0, 6, 0, 1, 0]";
  auto day_of_week_week_start_2_zero_based =
      "[2, 0, 5, 1, null, 1, 0, 6, 2, 3, 5, 6, 5, 4, 5, 6, 5]";
  auto day_of_week_week_start_7_one_based =
      "[5, 3, 1, 4, null, 4, 3, 2, 5, 6, 1, 2, 1, 7, 1, 2, 1]";
  auto day_of_week_week_start_2_one_based =
      "[3, 1, 6, 2, null, 2, 1, 7, 3, 4, 6, 7, 6, 5, 6, 7, 6]";

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
#endif
}  // namespace compute
}  // namespace arrow

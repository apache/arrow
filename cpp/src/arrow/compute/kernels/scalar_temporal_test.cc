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

#include <tuple>

#include <gtest/gtest.h>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::StringFormatter;

namespace compute {

class ScalarTemporalTest : public ::testing::Test {
 public:
  const char* date32s =
      R"([0, 11016, -25932, 23148, 18262, 18261, 18260, 14609, 14610, 14612,
          14613, 13149, 13148, 14241, 14242, 15340, null])";
  const char* date32s2 =
      R"([365, 10650, -25901, 23118, 18263, 18259, 18260, 14609, 14610, 14612,
          14613, 13149, 13148, 14240, 13937, 15400, null])";
  const char* date64s =
      R"([0, 951782400000, -2240524800000, 1999987200000, 1577836800000,
          1577750400000, 1577664000000, 1262217600000, 1262304000000, 1262476800000,
          1262563200000, 1136073600000, 1135987200000, 1230422400000, 1230508800000,
          1325376000000, null])";
  const char* date64s2 =
      R"([31536000000, 920160000000, -2237846400000, 1997395200000,
          1577923200000, 1577577600000, 1577664000000, 1262217600000, 1262304000000,
          1262476800000, 1262563200000, 1136073600000, 1135987200000, 1230336000000,
          1204156800000, 1330560000000, null])";
  const char* times_s =
      R"([59, 84203, 3560, 12800, 3905, 7810, 11715, 15620, 19525, 23430, 27335,
          31240, 35145, 0, 0, 3723, null])";
  const char* times_s2 =
      R"([59, 84203, 12642, 7182, 68705, 7390, 915, 16820, 19525, 5430, 84959,
          31207, 35145, 0, 0, 3723, null])";
  const char* times_ms =
      R"([59123, 84203999, 3560001, 12800000, 3905001, 7810002, 11715003, 15620004,
          19525005, 23430006, 27335000, 31240000, 35145000, 0, 0, 3723000, null])";
  const char* times_ms2 =
      R"([59103, 84203999, 12642001, 7182000, 68705005, 7390000, 915003, 16820004,
          19525005, 5430006, 84959000, 31207000, 35145000, 0, 0, 3723000, null])";
  const char* times_us =
      R"([59123456, 84203999999, 3560001001, 12800000000, 3905001000, 7810002000,
          11715003000, 15620004132, 19525005321, 23430006163, 27335000000,
          31240000000, 35145000000, 0, 0, 3723000000, null])";
  const char* times_us2 =
      R"([59103476, 84203999999, 12642001001, 7182000000, 68705005000, 7390000000,
          915003000, 16820004432, 19525005021, 5430006163, 84959000000,
          31207000000, 35145000000, 0, 0, 3723000000, null])";
  const char* times_ns =
      R"([59123456789, 84203999999999, 3560001001001, 12800000000000, 3905001000000,
          7810002000000, 11715003000000, 15620004132000, 19525005321000,
          23430006163000, 27335000000000, 31240000000000, 35145000000000, 0, 0,
          3723000000000, null])";
  const char* times_ns2 =
      R"([59103476799, 84203999999909, 12642001001001, 7182000000000, 68705005000000,
          7390000000000, 915003000000, 16820004432000, 19525005021000, 5430006163000,
          84959000000000, 31207000000000, 35145000000000, 0, 0, 3723000000000, null])";
  const char* times =
      R"(["1970-01-01T00:00:59.123456789","2000-02-29T23:23:23.999999999",
          "1899-01-01T00:59:20.001001001","2033-05-18T03:33:20.000000000",
          "2020-01-01T01:05:05.001", "2019-12-31T02:10:10.002",
          "2019-12-30T03:15:15.003", "2009-12-31T04:20:20.004132",
          "2010-01-01T05:25:25.005321", "2010-01-03T06:30:30.006163",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])";
  const char* times2 =
      R"(["1970-01-01T00:00:59.103476799","2000-02-29T23:23:23.999999909",
          "1899-01-01T03:30:42.001001001","2033-05-18T01:59:42.000000000",
          "2020-01-01T19:05:05.005", "2019-12-31T02:03:10.000",
          "2019-12-30T00:15:15.003", "2009-12-31T04:40:20.004432",
          "2010-01-01T05:25:25.005021", "2010-01-03T01:30:30.006163",
          "2010-01-04T23:35:59", "2006-01-01T08:40:07",
          "2005-12-31T09:45:45", "2008-12-28", "2008-12-29",
          "2012-01-01 01:02:03", null])";
  const char* times_seconds_precision =
      R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "1899-01-01T00:59:20","2033-05-18T03:33:20",
          "2020-01-01T01:05:05", "2019-12-31T02:10:10",
          "2019-12-30T03:15:15", "2009-12-31T04:20:20",
          "2010-01-01T05:25:25", "2010-01-03T06:30:30",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40",
          "2005-12-31T09:45:45", "2008-12-28", "2008-12-29",
          "2012-01-01 01:02:03", null])";
  const char* times_seconds_precision2 =
      R"(["1971-01-01T00:00:59","1999-02-28T23:23:23",
          "1899-02-01T00:59:20","2033-04-18T03:33:20",
          "2020-01-02T01:05:05", "2019-12-29T02:10:10",
          "2019-12-30T04:15:15", "2009-12-31T03:20:20",
          "2010-01-01T05:26:25", "2010-01-03T06:29:30",
          "2010-01-04T07:35:36", "2006-01-01T08:40:39",
          "2005-12-31T09:45:45", "2008-12-27T23:59:59",
          "2008-02-28", "2012-03-01", null])";
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
  std::string is_leap_year =
      "[false, true, false, false, true, false, false, false, false, false, false, "
      "false, false, true, true, true, null]";
  std::string month = "[1, 2, 1, 5, 1, 12, 12, 12, 1, 1, 1, 1, 12, 12, 12, 1, null]";
  std::string day = "[1, 29, 1, 18, 1, 31, 30, 31, 1, 3, 4, 1, 31, 28, 29, 1, null]";
  std::shared_ptr<arrow::DataType> year_month_day_type =
      struct_({field("year", int64()), field("month", int64()), field("day", int64())});
  std::shared_ptr<arrow::Array> year_month_day =
      ArrayFromJSON(year_month_day_type,
                    R"([{"year": 1970, "month": 1, "day": 1},
                      {"year": 2000, "month": 2, "day": 29},
                      {"year": 1899, "month": 1, "day": 1},
                      {"year": 2033, "month": 5, "day": 18},
                      {"year": 2020, "month": 1, "day": 1},
                      {"year": 2019, "month": 12, "day": 31},
                      {"year": 2019, "month": 12, "day": 30},
                      {"year": 2009, "month": 12, "day": 31},
                      {"year": 2010, "month": 1, "day": 1},
                      {"year": 2010, "month": 1, "day": 3},
                      {"year": 2010, "month": 1, "day": 4},
                      {"year": 2006, "month": 1, "day": 1},
                      {"year": 2005, "month": 12, "day": 31},
                      {"year": 2008, "month": 12, "day": 28},
                      {"year": 2008, "month": 12, "day": 29},
                      {"year": 2012, "month": 1, "day": 1}, null])");
  std::string day_of_week = "[3, 1, 6, 2, 2, 1, 0, 3, 4, 6, 0, 6, 5, 6, 0, 6, null]";
  std::string day_of_year =
      "[1, 60, 1, 138, 1, 365, 364, 365, 1, 3, 4, 1, 365, 363, 364, 1, null]";
  std::string us_year =
      "[1969, 2000, 1899, 2033, 2020, 2020, 2020, 2009, 2009, 2010, 2010, 2006, 2005, "
      "2008, 2008, 2012, null]";
  std::string iso_year =
      "[1970, 2000, 1898, 2033, 2020, 2020, 2020, 2009, 2009, 2009, 2010, 2005, "
      "2005, 2008, 2009, 2011, null]";
  std::string iso_week =
      "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";
  std::string us_week = "[53, 9, 1, 20, 1, 1, 1, 52, 52, 1, 1, 1, 52, 53, 53, 1, null]";
  std::string week = "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";

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
  std::string subsecond_ms =
      "[0.123, 0.999, 0.001, 0, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0, 0, 0, "
      "0, 0, 0, null]";
  std::string subsecond_us =
      "[0.123456, 0.999999, 0.001001, 0, 0.001, 0.002, 0.003, 0.004132, 0.005321, "
      "0.006163, 0, 0, 0, 0, 0, 0, null]";
  std::string zeros = "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null]";
  std::string years_between = "[1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null]";
  std::string years_between_tz =
      "[1, -1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, null]";
  std::string quarters_between =
      "[4, -4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -3, 0, null]";
  std::string quarters_between_tz =
      "[4, -4, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -3, 1, null]";
  std::string months_between =
      "[12, -12, 1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -10, 2, null]";
  std::string months_between_tz =
      "[12, -12, 1, -1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -10, 2, null]";
  std::string month_day_nano_interval_between_zeros =
      "[[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], "
      "[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], "
      "[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], null]";
  std::string month_day_nano_interval_between =
      "[[12, 0, 0], [-12, -1, 0], [1, 0, 0], [-1, 0, 0], [0, 1, 0], [0, -2, 0], "
      "[0, 0, 3600000000000], [0, 0, -3600000000000], "
      "[0, 0, 60000000000], [0, 0, -60000000000], "
      "[0, 0, 1000000000], [0, 0, -1000000000], "
      "[0, 0, 0], [0, -1, 86399000000000], [-10, -1, 0], [2, 0, -3723000000000], null]";
  std::string month_day_nano_interval_between_tz =
      "[[12, 0, 0], [-12, -1, 0], [1, 0, 0], [-1, 0, 0], [1, -30, 0], [0, -2, 0], "
      "[0, 0, 3600000000000], [0, 0, -3600000000000], "
      "[0, 0, 60000000000], [0, 0, -60000000000], "
      "[0, 0, 1000000000], [0, 0, -1000000000], "
      "[0, 0, 0], [0, 0, -1000000000], [-10, -1, 0], [2, -2, -3723000000000], null]";
  std::string month_day_nano_interval_between_date =
      "[[12, 0, 0], [-12, -1, 0], [1, 0, 0], [-1, 0, 0], [0, 1, 0], [0, -2, 0], "
      "[0, 0, 0], [0, 0, 0], "
      "[0, 0, 0], [0, 0, 0], "
      "[0, 0, 0], [0, 0, 0], "
      "[0, 0, 0], [0, -1, 0], [-10, -1, 0], [2, 0, 0], null]";
  std::string month_day_nano_interval_between_time =
      "[[0, 0, -19979990], [0, 0, -90], [0, 0, 9082000000000], [0, 0, -5618000000000], "
      "[0, 0, 64800004000000], [0, 0, -420002000000], [0, 0, -10800000000000], "
      "[0, 0, 1200000300000], [0, 0, -300000], [0, 0, -18000000000000], [0, 0, "
      "57624000000000], "
      "[0, 0, -33000000000], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], null]";
  std::string month_day_nano_interval_between_time_s =
      "[[0, 0, 0], [0, 0, 0], [0, 0, 9082000000000], [0, 0, -5618000000000], "
      "[0, 0, 64800000000000], [0, 0, -420000000000], [0, 0, -10800000000000], "
      "[0, 0, 1200000000000], [0, 0, 0], [0, 0, -18000000000000], [0, 0, "
      "57624000000000], "
      "[0, 0, -33000000000], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], null]";
  std::string month_day_nano_interval_between_time_ms =
      "[[0, 0, -20000000], [0, 0, 0], [0, 0, 9082000000000], [0, 0, -5618000000000], "
      "[0, 0, 64800004000000], [0, 0, -420002000000], [0, 0, -10800000000000], "
      "[0, 0, 1200000000000], [0, 0, 0], [0, 0, -18000000000000], [0, 0, "
      "57624000000000], "
      "[0, 0, -33000000000], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], null]";
  std::string month_day_nano_interval_between_time_us =
      "[[0, 0, -19980000], [0, 0, 0], [0, 0, 9082000000000], [0, 0, -5618000000000], "
      "[0, 0, 64800004000000], [0, 0, -420002000000], [0, 0, -10800000000000], "
      "[0, 0, 1200000300000], [0, 0, -300000], [0, 0, -18000000000000], [0, 0, "
      "57624000000000], "
      "[0, 0, -33000000000], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], null]";
  std::string day_time_interval_between_zeros =
      "[[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], "
      "[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], null]";
  std::string day_time_interval_between =
      "[[365, 0], [-366, 0], [31, 0], [-30, 0], [1, 0], [-2, 0], [0, 3600000], "
      "[0, -3600000], [0, 60000], [0, -60000], [0, 1000], [0, -1000], [0, 0], "
      "[-1, 86399000], [-305, 0], [60, -3723000], null]";
  std::string day_time_interval_between_date =
      "[[365, 0], [-366, 0], [31, 0], [-30, 0], [1, 0], [-2, 0], [0, 0], "
      "[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], "
      "[-1, 0], [-305, 0], [60, 0], null]";
  std::string day_time_interval_between_tz =
      "[[365, 0], [-366, 0], [31, 0], [-30, 0], [1, 0], [-2, 0], [0, 3600000], "
      "[0, -3600000], [0, 60000], [0, -60000], [0, 1000], [0, -1000], [0, 0], "
      "[0, -1000], [-305, 0], [60, -3723000], null]";
  std::string day_time_interval_between_time =
      "[[0, -20], [0, 0], [0, 9082000], [0, -5618000], [0, 64800004], [0, -420002], "
      "[0, -10800000], [0, 1200000], [0, 0], [0, -18000000], [0, 57624000], "
      "[0, -33000], [0, 0], [0, 0], [0, 0], [0, 0], null]";
  std::string day_time_interval_between_time_s =
      "[[0, 0], [0, 0], [0, 9082000], [0, -5618000], [0, 64800000], [0, -420000], "
      "[0, -10800000], [0, 1200000], [0, 0], [0, -18000000], [0, 57624000], "
      "[0, -33000], [0, 0], [0, 0], [0, 0], [0, 0], null]";
  std::string weeks_between =
      "[52, -53, 5, -4, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, -44, 9, null]";
  std::string weeks_between_tz =
      "[52, -53, 5, -5, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, -43, 9, null]";
  std::string days_between =
      "[365, -366, 31, -30, 1, -2, 0, 0, 0, 0, 0, 0, 0, -1, -305, 60, null]";
  std::string days_between_tz =
      "[365, -366, 31, -30, 1, -2, 0, 0, 0, 0, 0, 0, 0, 0, -305, 60, null]";
  std::string hours_between =
      "[8760, -8784, 744, -720, 24, -48, 1, -1, 0, 0, 0, 0, 0, -1, -7320, 1439, null]";
  std::string hours_between_date =
      "[8760, -8784, 744, -720, 24, -48, 0, 0, 0, 0, 0, 0, 0, -24, -7320, 1440, null]";
  std::string hours_between_tz =
      "[8760, -8784, 744, -720, 24, -48, 1, -1, 0, -1, 0, 0, 0, 0, -7320, 1439, null]";
  std::string hours_between_time =
      "[0, 0, 3, -2, 18, 0, -3, 0, 0, -5, 16, 0, 0, 0, 0, 0, null]";
  std::string minutes_between =
      "[525600, -527040, 44640, -43200, 1440, -2880, 60, -60, 1, -1, 0, 0, 0, -1, "
      "-439200, 86338, null]";
  std::string minutes_between_date =
      "[525600, -527040, 44640, -43200, 1440, -2880, 0, 0, 0, 0, 0, 0, 0, -1440, "
      "-439200, 86400, null]";
  std::string minutes_between_time =
      "[0, 0, 151, -94, 1080, -7, -180, 20, 0, -300, 960, 0, 0, 0, 0, 0, null]";
  std::string seconds_between =
      "[31536000, -31622400, 2678400, -2592000, 86400, -172800, 3600, -3600, 60, -60, 1, "
      "-1, 0, -1, -26352000, 5180277, null]";
  std::string seconds_between_date =
      "[31536000, -31622400, 2678400, -2592000, 86400, -172800, 0, 0, 0, 0, 0, "
      "0, 0, -86400, -26352000, 5184000, null]";
  std::string seconds_between_time =
      "[0, 0, 9082, -5618, 64800, -420, -10800, 1200, 0, -18000, 57624, -33, 0, 0, 0, 0, "
      "null]";
  std::string milliseconds_between =
      "[31536000000, -31622400000, 2678400000, -2592000000, 86400000, -172800000, "
      "3600000, -3600000, 60000, -60000, 1000, -1000, 0, -1000, -26352000000, "
      "5180277000, null]";
  std::string milliseconds_between_date =
      "[31536000000, -31622400000, 2678400000, -2592000000, 86400000, -172800000, "
      "0, 0, 0, 0, 0, 0, 0, -86400000, -26352000000, 5184000000, null]";
  std::string milliseconds_between_time =
      "[-20, 0, 9082000, -5618000, 64800004, -420002, -10800000, 1200000, 0, "
      "-18000000, 57624000, -33000, 0, 0, 0, 0, null]";
  std::string milliseconds_between_time_s =
      "[0, 0, 9082000, -5618000, 64800000, -420000, -10800000, 1200000, 0, "
      "-18000000, 57624000, -33000, 0, 0, 0, 0, null]";
  std::string microseconds_between =
      "[31536000000000, -31622400000000, 2678400000000, -2592000000000, 86400000000, "
      "-172800000000, 3600000000, -3600000000, 60000000, -60000000, 1000000, -1000000, "
      "0, -1000000, -26352000000000, 5180277000000, null]";
  std::string microseconds_between_date =
      "[31536000000000, -31622400000000, 2678400000000, -2592000000000, 86400000000, "
      "-172800000000, 0, 0, 0, 0, 0, 0, 0, -86400000000, -26352000000000, 5184000000000, "
      "null]";
  std::string microseconds_between_time =
      "[-19980, 0, 9082000000, -5618000000, 64800004000, -420002000, -10800000000, "
      "1200000300, -300, -18000000000, 57624000000, -33000000, 0, 0, 0, 0, null]";
  std::string microseconds_between_time_s =
      "[0, 0, 9082000000, -5618000000, 64800000000, -420000000, -10800000000, "
      "1200000000, 0, -18000000000, 57624000000, -33000000, 0, 0, 0, 0, null]";
  std::string microseconds_between_time_ms =
      "[-20000, 0, 9082000000, -5618000000, 64800004000, -420002000, -10800000000, "
      "1200000000, 0, -18000000000, 57624000000, -33000000, 0, 0, 0, 0, null]";
  std::string nanoseconds_between =
      "[31536000000000000, -31622400000000000, 2678400000000000, -2592000000000000, "
      "86400000000000, -172800000000000, 3600000000000, -3600000000000, 60000000000, "
      "-60000000000, 1000000000, -1000000000, 0, -1000000000, -26352000000000000, "
      "5180277000000000, null]";
  std::string nanoseconds_between_date =
      "[31536000000000000, -31622400000000000, 2678400000000000, -2592000000000000, "
      "86400000000000, -172800000000000, 0, 0, 0, 0, 0, 0, 0, -86400000000000, "
      "-26352000000000000, 5184000000000000, null]";
  std::string nanoseconds_between_time =
      "[-19979990, -90, 9082000000000, -5618000000000, 64800004000000, -420002000000, "
      "-10800000000000, 1200000300000, -300000, -18000000000000, 57624000000000, "
      "-33000000000, 0, 0, 0, 0, null]";
  std::string nanoseconds_between_time_s =
      "[0, 0, 9082000000000, -5618000000000, 64800000000000, -420000000000, "
      "-10800000000000, 1200000000000, 0, -18000000000000, 57624000000000, "
      "-33000000000, 0, 0, 0, 0, null]";
  std::string nanoseconds_between_time_ms =
      "[-20000000, 0, 9082000000000, -5618000000000, 64800004000000, -420002000000, "
      "-10800000000000, 1200000000000, 0, -18000000000000, 57624000000000, "
      "-33000000000, 0, 0, 0, 0, null]";
  std::string nanoseconds_between_time_us =
      "[-19980000, 0, 9082000000000, -5618000000000, 64800004000000, -420002000000, "
      "-10800000000000, 1200000300000, -300000, -18000000000000, 57624000000000, "
      "-33000000000, 0, 0, 0, 0, null]";

  RoundTemporalOptions round_to_1_nanoseconds =
      RoundTemporalOptions(1, CalendarUnit::NANOSECOND);
  RoundTemporalOptions round_to_1_microseconds =
      RoundTemporalOptions(1, CalendarUnit::MICROSECOND);
  RoundTemporalOptions round_to_1_milliseconds =
      RoundTemporalOptions(1, CalendarUnit::MILLISECOND);
  RoundTemporalOptions round_to_1_seconds = RoundTemporalOptions(1, CalendarUnit::SECOND);
  RoundTemporalOptions round_to_1_minutes = RoundTemporalOptions(1, CalendarUnit::MINUTE);
  RoundTemporalOptions round_to_1_hours = RoundTemporalOptions(1, CalendarUnit::HOUR);
  RoundTemporalOptions round_to_1_days = RoundTemporalOptions(1, CalendarUnit::DAY);
  RoundTemporalOptions round_to_1_weeks =
      RoundTemporalOptions(1, CalendarUnit::WEEK, true);
  RoundTemporalOptions round_to_1_weeks_sunday =
      RoundTemporalOptions(1, CalendarUnit::WEEK, false);
  RoundTemporalOptions round_to_1_months = RoundTemporalOptions(1, CalendarUnit::MONTH);
  RoundTemporalOptions round_to_1_quarters =
      RoundTemporalOptions(1, CalendarUnit::QUARTER);
  RoundTemporalOptions round_to_1_years = RoundTemporalOptions(1, CalendarUnit::YEAR);
  RoundTemporalOptions round_to_15_nanoseconds =
      RoundTemporalOptions(15, CalendarUnit::NANOSECOND);
  RoundTemporalOptions round_to_15_microseconds =
      RoundTemporalOptions(15, CalendarUnit::MICROSECOND);
  RoundTemporalOptions round_to_15_milliseconds =
      RoundTemporalOptions(15, CalendarUnit::MILLISECOND);
  RoundTemporalOptions round_to_15_seconds =
      RoundTemporalOptions(15, CalendarUnit::SECOND);
  RoundTemporalOptions round_to_15_minutes =
      RoundTemporalOptions(15, CalendarUnit::MINUTE);
  RoundTemporalOptions round_to_15_hours = RoundTemporalOptions(15, CalendarUnit::HOUR);
  RoundTemporalOptions round_to_15_days = RoundTemporalOptions(15, CalendarUnit::DAY);
  RoundTemporalOptions round_to_15_weeks =
      RoundTemporalOptions(15, CalendarUnit::WEEK, true);
  RoundTemporalOptions round_to_15_weeks_sunday =
      RoundTemporalOptions(15, CalendarUnit::WEEK, false);
  RoundTemporalOptions round_to_15_months = RoundTemporalOptions(15, CalendarUnit::MONTH);
  RoundTemporalOptions round_to_15_quarters =
      RoundTemporalOptions(15, CalendarUnit::QUARTER);
  RoundTemporalOptions round_to_15_years = RoundTemporalOptions(15, CalendarUnit::YEAR);

 protected:
  void SetUp() override {
#ifdef _WIN32
    // Initialize timezone database on Windows
    ASSERT_OK(InitTestTimezoneDatabase());
#endif
  }
};

class ScalarTemporalTestStrictCeil : public ScalarTemporalTest {
 public:
  RoundTemporalOptions round_to_1_nanoseconds = RoundTemporalOptions(
      1, CalendarUnit::NANOSECOND, /*week_starts_monday=*/true,
      /*ceil_is_strictly_greater=*/true, /*calendar_based_origin=*/false);
  RoundTemporalOptions round_to_1_microseconds =
      RoundTemporalOptions(1, CalendarUnit::MICROSECOND, true, true, false);
  RoundTemporalOptions round_to_1_milliseconds =
      RoundTemporalOptions(1, CalendarUnit::MILLISECOND, true, true, false);
  RoundTemporalOptions round_to_1_seconds =
      RoundTemporalOptions(1, CalendarUnit::SECOND, true, true, false);
  RoundTemporalOptions round_to_1_minutes =
      RoundTemporalOptions(1, CalendarUnit::MINUTE, true, true, false);
  RoundTemporalOptions round_to_1_hours =
      RoundTemporalOptions(1, CalendarUnit::HOUR, true, true, false);
  RoundTemporalOptions round_to_1_days =
      RoundTemporalOptions(1, CalendarUnit::DAY, true, true, false);
  RoundTemporalOptions round_to_1_weeks =
      RoundTemporalOptions(1, CalendarUnit::WEEK, true, true, false);
  RoundTemporalOptions round_to_1_weeks_sunday =
      RoundTemporalOptions(1, CalendarUnit::WEEK, false, true, false);
  RoundTemporalOptions round_to_1_months =
      RoundTemporalOptions(1, CalendarUnit::MONTH, true, true, false);
  RoundTemporalOptions round_to_1_quarters =
      RoundTemporalOptions(1, CalendarUnit::QUARTER, true, true, false);
  RoundTemporalOptions round_to_1_years =
      RoundTemporalOptions(1, CalendarUnit::YEAR, true, true, false);

  RoundTemporalOptions round_to_15_nanoseconds =
      RoundTemporalOptions(15, CalendarUnit::NANOSECOND, true, true, false);
  RoundTemporalOptions round_to_15_microseconds =
      RoundTemporalOptions(15, CalendarUnit::MICROSECOND, true, true, false);
  RoundTemporalOptions round_to_15_milliseconds =
      RoundTemporalOptions(15, CalendarUnit::MILLISECOND, true, true, false);
  RoundTemporalOptions round_to_13_seconds =
      RoundTemporalOptions(13, CalendarUnit::SECOND, true, true, false);
  RoundTemporalOptions round_to_13_minutes =
      RoundTemporalOptions(13, CalendarUnit::MINUTE, true, true, false);
  RoundTemporalOptions round_to_15_hours =
      RoundTemporalOptions(15, CalendarUnit::HOUR, true, true, false);
  RoundTemporalOptions round_to_15_days =
      RoundTemporalOptions(15, CalendarUnit::DAY, true, true, false);
  RoundTemporalOptions round_to_3_weeks =
      RoundTemporalOptions(3, CalendarUnit::WEEK, true, true, false);
  RoundTemporalOptions round_to_3_weeks_sunday =
      RoundTemporalOptions(3, CalendarUnit::WEEK, false, true, false);
  RoundTemporalOptions round_to_5_months =
      RoundTemporalOptions(5, CalendarUnit::MONTH, true, true, false);
  RoundTemporalOptions round_to_15_months =
      RoundTemporalOptions(15, CalendarUnit::MONTH, true, true, false);
  RoundTemporalOptions round_to_3_quarters =
      RoundTemporalOptions(3, CalendarUnit::QUARTER, true, true, false);
  RoundTemporalOptions round_to_15_quarters =
      RoundTemporalOptions(15, CalendarUnit::QUARTER, true, true, false);
  RoundTemporalOptions round_to_15_years =
      RoundTemporalOptions(15, CalendarUnit::YEAR, true, true, false);
};

class ScalarTemporalTestMultipleSinceGreaterUnit : public ScalarTemporalTest {
 public:
  RoundTemporalOptions round_to_1_nanoseconds = RoundTemporalOptions(
      1, CalendarUnit::NANOSECOND, /*week_starts_monday=*/true,
      /*ceil_is_strictly_greater=*/true, /*calendar_based_origin=*/true);
  RoundTemporalOptions round_to_1_microseconds =
      RoundTemporalOptions(1, CalendarUnit::MICROSECOND, true, true, true);
  RoundTemporalOptions round_to_1_milliseconds =
      RoundTemporalOptions(1, CalendarUnit::MILLISECOND, true, true, true);
  RoundTemporalOptions round_to_1_seconds =
      RoundTemporalOptions(1, CalendarUnit::SECOND, true, true, true);
  RoundTemporalOptions round_to_1_minutes =
      RoundTemporalOptions(1, CalendarUnit::MINUTE, true, true, true);
  RoundTemporalOptions round_to_1_hours =
      RoundTemporalOptions(1, CalendarUnit::HOUR, true, true, true);
  RoundTemporalOptions round_to_1_days =
      RoundTemporalOptions(1, CalendarUnit::DAY, true, true, true);
  RoundTemporalOptions round_to_1_weeks =
      RoundTemporalOptions(1, CalendarUnit::WEEK, true, true, true);
  RoundTemporalOptions round_to_1_weeks_sunday =
      RoundTemporalOptions(1, CalendarUnit::WEEK, false, true, true);
  RoundTemporalOptions round_to_1_months =
      RoundTemporalOptions(1, CalendarUnit::MONTH, true, true, true);
  RoundTemporalOptions round_to_1_quarters =
      RoundTemporalOptions(1, CalendarUnit::QUARTER, true, true, true);
  RoundTemporalOptions round_to_1_years =
      RoundTemporalOptions(1, CalendarUnit::YEAR, true, true, true);

  RoundTemporalOptions round_to_15_nanoseconds =
      RoundTemporalOptions(15, CalendarUnit::NANOSECOND, true, true, true);
  RoundTemporalOptions round_to_15_microseconds =
      RoundTemporalOptions(15, CalendarUnit::MICROSECOND, true, true, true);
  RoundTemporalOptions round_to_15_milliseconds =
      RoundTemporalOptions(15, CalendarUnit::MILLISECOND, true, true, true);
  RoundTemporalOptions round_to_13_seconds =
      RoundTemporalOptions(13, CalendarUnit::SECOND, true, true, true);
  RoundTemporalOptions round_to_13_minutes =
      RoundTemporalOptions(13, CalendarUnit::MINUTE, true, true, true);
  RoundTemporalOptions round_to_15_hours =
      RoundTemporalOptions(15, CalendarUnit::HOUR, true, true, true);
  RoundTemporalOptions round_to_15_days =
      RoundTemporalOptions(15, CalendarUnit::DAY, true, true, true);
  RoundTemporalOptions round_to_3_weeks =
      RoundTemporalOptions(3, CalendarUnit::WEEK, true, true, true);
  RoundTemporalOptions round_to_3_weeks_sunday =
      RoundTemporalOptions(3, CalendarUnit::WEEK, false, true, true);
  RoundTemporalOptions round_to_5_months =
      RoundTemporalOptions(5, CalendarUnit::MONTH, true, true, true);
  RoundTemporalOptions round_to_15_months =
      RoundTemporalOptions(15, CalendarUnit::MONTH, true, true, true);
  RoundTemporalOptions round_to_3_quarters =
      RoundTemporalOptions(3, CalendarUnit::QUARTER, true, true, true);
  RoundTemporalOptions round_to_15_quarters =
      RoundTemporalOptions(15, CalendarUnit::QUARTER, true, true, true);
  RoundTemporalOptions round_to_15_years =
      RoundTemporalOptions(15, CalendarUnit::YEAR, true, true, true);
};

TEST_F(ScalarTemporalTest, TestTemporalComponentExtractionAllTemporalTypes) {
  std::vector<std::shared_ptr<DataType>> units = {date32(), date64(),
                                                  timestamp(TimeUnit::NANO)};
  std::vector<const char*> samples = {date32s, date64s, times};
  DCHECK_EQ(units.size(), samples.size());
  for (size_t i = 0; i < samples.size(); ++i) {
    auto unit = units[i];
    auto sample = samples[i];
    CheckScalarUnary("year", unit, sample, int64(), year);
    CheckScalarUnary("is_leap_year", unit, sample, boolean(), is_leap_year);
    CheckScalarUnary("month", unit, sample, int64(), month);
    CheckScalarUnary("day", unit, sample, int64(), day);
    CheckScalarUnary("year_month_day", ArrayFromJSON(unit, sample), year_month_day);
    CheckScalarUnary("day_of_week", unit, sample, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, sample, int64(), day_of_year);
    CheckScalarUnary("us_year", unit, sample, int64(), us_year);
    CheckScalarUnary("iso_year", unit, sample, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, sample, int64(), iso_week);
    CheckScalarUnary("us_week", unit, sample, int64(), us_week);
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

  CheckScalarUnary("hour", time32(TimeUnit::SECOND), times_s, int64(), hour);
  CheckScalarUnary("minute", time32(TimeUnit::SECOND), times_s, int64(), minute);
  CheckScalarUnary("second", time32(TimeUnit::SECOND), times_s, int64(), second);
  CheckScalarUnary("millisecond", time32(TimeUnit::SECOND), times_s, int64(), zeros);
  CheckScalarUnary("microsecond", time32(TimeUnit::SECOND), times_s, int64(), zeros);
  CheckScalarUnary("nanosecond", time32(TimeUnit::SECOND), times_s, int64(), zeros);
  CheckScalarUnary("subsecond", time32(TimeUnit::SECOND), times_s, float64(), zeros);

  CheckScalarUnary("hour", time32(TimeUnit::MILLI), times_ms, int64(), hour);
  CheckScalarUnary("minute", time32(TimeUnit::MILLI), times_ms, int64(), minute);
  CheckScalarUnary("second", time32(TimeUnit::MILLI), times_ms, int64(), second);
  CheckScalarUnary("millisecond", time32(TimeUnit::MILLI), times_ms, int64(),
                   millisecond);
  CheckScalarUnary("microsecond", time32(TimeUnit::MILLI), times_ms, int64(), zeros);
  CheckScalarUnary("nanosecond", time32(TimeUnit::MILLI), times_ms, int64(), zeros);
  CheckScalarUnary("subsecond", time32(TimeUnit::MILLI), times_ms, float64(),
                   subsecond_ms);

  CheckScalarUnary("hour", time64(TimeUnit::MICRO), times_us, int64(), hour);
  CheckScalarUnary("minute", time64(TimeUnit::MICRO), times_us, int64(), minute);
  CheckScalarUnary("second", time64(TimeUnit::MICRO), times_us, int64(), second);
  CheckScalarUnary("millisecond", time64(TimeUnit::MICRO), times_us, int64(),
                   millisecond);
  CheckScalarUnary("microsecond", time64(TimeUnit::MICRO), times_us, int64(),
                   microsecond);
  CheckScalarUnary("nanosecond", time64(TimeUnit::MICRO), times_us, int64(), zeros);
  CheckScalarUnary("subsecond", time64(TimeUnit::MICRO), times_us, float64(),
                   subsecond_us);

  CheckScalarUnary("hour", time64(TimeUnit::NANO), times_ns, int64(), hour);
  CheckScalarUnary("minute", time64(TimeUnit::NANO), times_ns, int64(), minute);
  CheckScalarUnary("second", time64(TimeUnit::NANO), times_ns, int64(), second);
  CheckScalarUnary("millisecond", time64(TimeUnit::NANO), times_ns, int64(), millisecond);
  CheckScalarUnary("microsecond", time64(TimeUnit::NANO), times_ns, int64(), microsecond);
  CheckScalarUnary("nanosecond", time64(TimeUnit::NANO), times_ns, int64(), nanosecond);
  CheckScalarUnary("subsecond", time64(TimeUnit::NANO), times_ns, float64(), subsecond);
}

TEST_F(ScalarTemporalTest, TestTemporalComponentExtractionWithDifferentUnits) {
  for (auto u : TimeUnit::values()) {
    auto unit = timestamp(u);
    CheckScalarUnary("year", unit, times_seconds_precision, int64(), year);
    CheckScalarUnary("is_leap_year", unit, times_seconds_precision, boolean(),
                     is_leap_year);
    CheckScalarUnary("month", unit, times_seconds_precision, int64(), month);
    CheckScalarUnary("day", unit, times_seconds_precision, int64(), day);
    CheckScalarUnary("year_month_day", ArrayFromJSON(unit, times_seconds_precision),
                     year_month_day);
    CheckScalarUnary("day_of_week", unit, times_seconds_precision, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, times_seconds_precision, int64(), day_of_year);
    ASSERT_RAISES(Invalid,
                  IsDaylightSavings(ArrayFromJSON(unit, times_seconds_precision)));
    CheckScalarUnary("us_year", unit, times_seconds_precision, int64(), us_year);
    CheckScalarUnary("iso_year", unit, times_seconds_precision, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, times_seconds_precision, int64(), iso_week);
    CheckScalarUnary("us_week", unit, times_seconds_precision, int64(), us_week);
    CheckScalarUnary("week", unit, times_seconds_precision, int64(), week);
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
  auto year = "[1677, 2262]";
  auto is_leap_year = "[false, false]";
  auto month = "[9, 4]";
  auto day = "[20, 13]";
  auto year_month_day = ArrayFromJSON(year_month_day_type,
                                      R"([{"year": 1677, "month": 9, "day": 20},
                          {"year": 2262, "month": 4, "day": 13}])");
  auto day_of_week = "[0, 6]";
  auto day_of_year = "[263, 103]";
  auto us_year = "[1677, 2262]";
  auto iso_year = "[1677, 2262]";
  auto iso_week = "[38, 15]";
  auto us_week = "[38, 16]";
  auto week = "[38, 15]";
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
  CheckScalarUnary("is_leap_year", unit, times, boolean(), is_leap_year);
  CheckScalarUnary("month", unit, times, int64(), month);
  CheckScalarUnary("day", unit, times, int64(), day);
  CheckScalarUnary("year_month_day", ArrayFromJSON(unit, times), year_month_day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("us_year", unit, times, int64(), us_year);
  CheckScalarUnary("iso_year", unit, times, int64(), iso_year);
  CheckScalarUnary("iso_week", unit, times, int64(), iso_week);
  CheckScalarUnary("us_week", unit, times, int64(), us_week);
  CheckScalarUnary("week", unit, times, int64(), week);
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

TEST_F(ScalarTemporalTest, TestIsLeapYear) {
  auto is_leap_year_marquesas =
      "[false, true, false, false, false, false, false, false, false, false, false, "
      "false, false, true, true, false, null]";

  auto is_leap_year_broken_hill =
      "[false, true, false, false, true, false, false, false, false, false, false, "
      "false, false, true, true, true, null]";

  auto is_leap_year_pago_pago =
      "[false, true, false, false, false, false, false, false, false, false, false, "
      "false, false, true, true, false, null]";

  CheckScalarUnary("is_leap_year", timestamp(TimeUnit::NANO, "Pacific/Marquesas"), times,
                   boolean(), is_leap_year_marquesas);
  CheckScalarUnary("is_leap_year", timestamp(TimeUnit::NANO, "Australia/Broken_Hill"),
                   times, boolean(), is_leap_year_broken_hill);
  CheckScalarUnary("is_leap_year", timestamp(TimeUnit::NANO, "Pacific/Pago_Pago"), times,
                   boolean(), is_leap_year_pago_pago);
}

TEST_F(ScalarTemporalTest, TestZoned1) {
  auto unit = timestamp(TimeUnit::NANO, "Pacific/Marquesas");
  auto year =
      "[1969, 2000, 1898, 2033, 2019, 2019, 2019, 2009, 2009, 2010, 2010, 2005, 2005, "
      "2008, 2008, 2011, null]";
  auto is_leap_year =
      "[false, true, false, false, false, false, false, false, false, false, false, "
      "false, false, true, true, false, null]";
  auto month = "[12, 2, 12, 5, 12, 12, 12, 12, 12, 1, 1, 12, 12, 12, 12, 12, null]";
  auto day = "[31, 29, 31, 17, 31, 30, 29, 30, 31, 2, 3, 31, 31, 27, 28, 31, null]";
  auto year_month_day = ArrayFromJSON(year_month_day_type,
                                      R"([{"year": 1969, "month": 12, "day": 31},
                        {"year": 2000, "month": 2, "day": 29},
                        {"year": 1898, "month": 12, "day": 31},
                        {"year": 2033, "month": 5, "day": 17},
                        {"year": 2019, "month": 12, "day": 31},
                        {"year": 2019, "month": 12, "day": 30},
                        {"year": 2019, "month": 12, "day": 29},
                        {"year": 2009, "month": 12, "day": 30},
                        {"year": 2009, "month": 12, "day": 31},
                        {"year": 2010, "month": 1, "day": 2},
                        {"year": 2010, "month": 1, "day": 3},
                        {"year": 2005, "month": 12, "day": 31},
                        {"year": 2005, "month": 12, "day": 31},
                        {"year": 2008, "month": 12, "day": 27},
                        {"year": 2008, "month": 12, "day": 28},
                        {"year": 2011, "month": 12, "day": 31}, null])");
  auto day_of_week = "[2, 1, 5, 1, 1, 0, 6, 2, 3, 5, 6, 5, 5, 5, 6, 5, null]";
  auto day_of_year =
      "[365, 60, 365, 137, 365, 364, 363, 364, 365, 2, 3, 365, 365, 362, 363, 365, null]";
  std::string is_dst =
      "[false, false, false, false, false, false, false, false, false, false, false, "
      "false, false, false, false, false, null]";
  auto us_year =
      "[1969, 2000, 1898, 2033, 2020, 2020, 2020, 2009, 2009, 2009, 2010, 2005, 2005, "
      "2008, 2008, 2011, null]";
  auto iso_year =
      "[1970, 2000, 1898, 2033, 2020, 2020, 2019, 2009, 2009, 2009, 2009, 2005, 2005, "
      "2008, 2008, 2011, null]";
  auto iso_week = "[1, 9, 52, 20, 1, 1, 52, 53, 53, 53, 53, 52, 52, 52, 52, 52, null]";
  auto us_week = "[53, 9, 52, 20, 1, 1, 1, 52, 52, 52, 1, 52, 52, 52, 53, 52, null]";
  auto week = "[1, 9, 52, 20, 1, 1, 52, 53, 53, 53, 53, 52, 52, 52, 52, 52, null]";
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
  CheckScalarUnary("year_month_day", ArrayFromJSON(unit, times), year_month_day);
  CheckScalarUnary("day_of_week", unit, times, int64(), day_of_week);
  CheckScalarUnary("day_of_year", unit, times, int64(), day_of_year);
  CheckScalarUnary("is_dst", unit, times, boolean(), is_dst);
  CheckScalarUnary("us_year", unit, times, int64(), us_year);
  CheckScalarUnary("iso_year", unit, times, int64(), iso_year);
  CheckScalarUnary("iso_week", unit, times, int64(), iso_week);
  CheckScalarUnary("is_leap_year", unit, times, boolean(), is_leap_year);
  CheckScalarUnary("us_week", unit, times, int64(), us_week);
  CheckScalarUnary("week", unit, times, int64(), week);
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
  for (auto u : TimeUnit::values()) {
    auto unit = timestamp(u, "Australia/Broken_Hill");
    auto month = "[1, 3, 1, 5, 1, 12, 12, 12, 1, 1, 1, 1, 12, 12, 12, 1, null]";
    auto day = "[1, 1, 1, 18, 1, 31, 30, 31, 1, 3, 4, 1, 31, 28, 29, 1, null]";
    auto year_month_day = ArrayFromJSON(year_month_day_type,
                                        R"([{"year": 1970, "month": 1, "day": 1},
                          {"year": 2000, "month": 3, "day": 1},
                          {"year": 1899, "month": 1, "day": 1},
                          {"year": 2033, "month": 5, "day": 18},
                          {"year": 2020, "month": 1, "day": 1},
                          {"year": 2019, "month": 12, "day": 31},
                          {"year": 2019, "month": 12, "day": 30},
                          {"year": 2009, "month": 12, "day": 31},
                          {"year": 2010, "month": 1, "day": 1},
                          {"year": 2010, "month": 1, "day": 3},
                          {"year": 2010, "month": 1, "day": 4},
                          {"year": 2006, "month": 1, "day": 1},
                          {"year": 2005, "month": 12, "day": 31},
                          {"year": 2008, "month": 12, "day": 28},
                          {"year": 2008, "month": 12, "day": 29},
                          {"year": 2012, "month": 1, "day": 1}, null])");
    auto day_of_week = "[3, 2, 6, 2, 2, 1, 0, 3, 4, 6, 0, 6, 5, 6, 0, 6, null]";
    auto day_of_year =
        "[1, 61, 1, 138, 1, 365, 364, 365, 1, 3, 4, 1, 365, 363, 364, 1, null]";
    std::string is_dst =
        "[false, true, false, false, true, true, true, true, true, true, true, true, "
        "true, true, true, true, null]";
    auto us_year =
        "[1969, 2000, 1899, 2033, 2020, 2020, 2020, 2009, 2009, 2010, 2010, 2006, 2005, "
        "2008, 2008, 2012, null]";
    auto iso_year =
        "[1970, 2000, 1898, 2033, 2020, 2020, 2020, 2009, 2009, 2009, 2010, 2005, 2005, "
        "2008, 2009, 2011, null]";
    auto iso_week = "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";
    auto us_week = "[53, 9, 1, 20, 1, 1, 1, 52, 52, 1, 1, 1, 52, 53, 53, 1, null]";
    auto week = "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";
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
    CheckScalarUnary("is_leap_year", unit, times_seconds_precision, boolean(),
                     is_leap_year);
    CheckScalarUnary("month", unit, times_seconds_precision, int64(), month);
    CheckScalarUnary("day", unit, times_seconds_precision, int64(), day);
    CheckScalarUnary("year_month_day", ArrayFromJSON(unit, times_seconds_precision),
                     year_month_day);
    CheckScalarUnary("day_of_week", unit, times_seconds_precision, int64(), day_of_week);
    CheckScalarUnary("day_of_year", unit, times_seconds_precision, int64(), day_of_year);
    CheckScalarUnary("is_dst", unit, times_seconds_precision, boolean(), is_dst);
    CheckScalarUnary("us_year", unit, times_seconds_precision, int64(), us_year);
    CheckScalarUnary("iso_year", unit, times_seconds_precision, int64(), iso_year);
    CheckScalarUnary("iso_week", unit, times_seconds_precision, int64(), iso_week);
    CheckScalarUnary("us_week", unit, times_seconds_precision, int64(), us_week);
    CheckScalarUnary("week", unit, times_seconds_precision, int64(), week);
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

  for (auto u : TimeUnit::values()) {
    auto ts_type = timestamp(u, "Mars/Mariner_Valley");
    auto timestamp_array = std::make_shared<NumericArray<TimestampType>>(
        ts_type, 2, data_buffer, null_buffer, 0);
    ASSERT_RAISES(Invalid, Year(timestamp_array));
    ASSERT_RAISES(Invalid, IsLeapYear(timestamp_array));
    ASSERT_RAISES(Invalid, Month(timestamp_array));
    ASSERT_RAISES(Invalid, Day(timestamp_array));
    ASSERT_RAISES(Invalid, YearMonthDay(timestamp_array));
    ASSERT_RAISES(Invalid, DayOfWeek(timestamp_array));
    ASSERT_RAISES(Invalid, DayOfYear(timestamp_array));
    ASSERT_RAISES(Invalid, IsDaylightSavings(timestamp_array));
    ASSERT_RAISES(Invalid, USYear(timestamp_array));
    ASSERT_RAISES(Invalid, ISOYear(timestamp_array));
    ASSERT_RAISES(Invalid, Week(timestamp_array));
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

TEST_F(ScalarTemporalTest, Week) {
  auto unit = timestamp(TimeUnit::NANO);
  std::string week_100 =
      "[1, 9, 52, 20, 1, 1, 1, 53, 53, 53, 1, 52, 52, 52, 1, 52, null]";
  std::string week_110 = "[1, 9, 0, 20, 1, 53, 53, 53, 0, 0, 1, 0, 52, 52, 53, 0, null]";
  std::string week_010 = "[0, 9, 1, 20, 1, 53, 53, 52, 0, 1, 1, 1, 52, 53, 53, 1, null]";
  std::string week_000 = "[53, 9, 1, 20, 1, 1, 1, 52, 52, 1, 1, 1, 52, 53, 53, 1, null]";
  std::string week_111 = "[0, 9, 0, 20, 0, 52, 52, 52, 0, 0, 1, 0, 52, 51, 52, 0, null]";
  std::string week_011 = "[0, 9, 1, 20, 0, 52, 52, 52, 0, 1, 1, 1, 52, 52, 52, 1, null]";
  std::string week_101 =
      "[52, 9, 52, 20, 52, 52, 52, 52, 52, 52, 1, 52, 52, 51, 52, 52, null]";
  std::string week_001 =
      "[52, 9, 1, 20, 52, 52, 52, 52, 52, 1, 1, 1, 52, 52, 52, 1, null]";

  auto options_100 = WeekOptions(/*week_starts_monday*/ true, /*count_from_zero=*/false,
                                 /*first_week_is_fully_in_year=*/false);
  auto options_110 = WeekOptions(/*week_starts_monday*/ true, /*count_from_zero=*/true,
                                 /*first_week_is_fully_in_year=*/false);
  auto options_010 = WeekOptions(/*week_starts_monday*/ false, /*count_from_zero=*/true,
                                 /*first_week_is_fully_in_year=*/false);
  auto options_000 = WeekOptions(/*week_starts_monday*/ false, /*count_from_zero=*/false,
                                 /*first_week_is_fully_in_year=*/false);
  auto options_111 = WeekOptions(/*week_starts_monday*/ true, /*count_from_zero=*/true,
                                 /*first_week_is_fully_in_year=*/true);
  auto options_011 = WeekOptions(/*week_starts_monday*/ false, /*count_from_zero=*/true,
                                 /*first_week_is_fully_in_year=*/true);
  auto options_101 = WeekOptions(/*week_starts_monday*/ true, /*count_from_zero=*/false,
                                 /*first_week_is_fully_in_year=*/true);
  auto options_001 = WeekOptions(/*week_starts_monday*/ false, /*count_from_zero=*/false,
                                 /*first_week_is_fully_in_year=*/true);

  CheckScalarUnary("iso_week", unit, times, int64(), week_100);
  CheckScalarUnary("us_week", unit, times, int64(), week_000);
  CheckScalarUnary("week", unit, times, int64(), week_100, &options_100);
  CheckScalarUnary("week", unit, times, int64(), week_110, &options_110);
  CheckScalarUnary("week", unit, times, int64(), week_010, &options_010);
  CheckScalarUnary("week", unit, times, int64(), week_000, &options_000);
  CheckScalarUnary("week", unit, times, int64(), week_111, &options_111);
  CheckScalarUnary("week", unit, times, int64(), week_011, &options_011);
  CheckScalarUnary("week", unit, times, int64(), week_101, &options_101);
  CheckScalarUnary("week", unit, times, int64(), week_001, &options_001);
}

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
                                /*count_from_zero=*/true, /*week_start=*/7)));
  ASSERT_TRUE(result_70.Equals(expected_70));

  auto expected_20 = ArrayFromJSON(int64(), day_of_week_week_start_2_zero_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_20,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*count_from_zero=*/true, /*week_start=*/2)));
  ASSERT_TRUE(result_20.Equals(expected_20));

  auto expected_71 = ArrayFromJSON(int64(), day_of_week_week_start_7_one_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_71,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*count_from_zero=*/false, /*week_start=*/7)));
  ASSERT_TRUE(result_71.Equals(expected_71));

  auto expected_21 = ArrayFromJSON(int64(), day_of_week_week_start_2_one_based);
  ASSERT_OK_AND_ASSIGN(
      Datum result_21,
      DayOfWeek(timestamps, DayOfWeekOptions(
                                /*count_from_zero=*/false, /*week_start=*/2)));
  ASSERT_TRUE(result_21.Equals(expected_21));

  ASSERT_RAISES(Invalid, DayOfWeek(timestamps, DayOfWeekOptions(/*count_from_zero=*/false,
                                                                /*week_start=*/0)));
  ASSERT_RAISES(Invalid, DayOfWeek(timestamps, DayOfWeekOptions(/*count_from_zero=*/true,
                                                                /*week_start=*/8)));
}

TEST_F(ScalarTemporalTest, TestTemporalDifference) {
  for (auto u : TimeUnit::values()) {
    auto unit = timestamp(u);
    auto arr1 = ArrayFromJSON(unit, times_seconds_precision);
    auto arr2 = ArrayFromJSON(unit, times_seconds_precision2);
    CheckScalarBinary("years_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("years_between", arr1, arr2, ArrayFromJSON(int64(), years_between));
    CheckScalarBinary("quarters_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("quarters_between", arr1, arr2,
                      ArrayFromJSON(int64(), quarters_between));
    CheckScalarBinary("month_interval_between", arr1, arr1,
                      ArrayFromJSON(month_interval(), zeros));
    CheckScalarBinary("month_interval_between", arr1, arr1,
                      ArrayFromJSON(month_interval(), zeros));
    CheckScalarBinary("month_interval_between", arr1, arr2,
                      ArrayFromJSON(month_interval(), months_between));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr1,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_zeros));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr2,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between));
    CheckScalarBinary(
        "day_time_interval_between", arr1, arr1,
        ArrayFromJSON(day_time_interval(), day_time_interval_between_zeros));
    CheckScalarBinary("day_time_interval_between", arr1, arr2,
                      ArrayFromJSON(day_time_interval(), day_time_interval_between));
    CheckScalarBinary("weeks_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("weeks_between", arr1, arr2, ArrayFromJSON(int64(), weeks_between));
    CheckScalarBinary("days_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("days_between", arr1, arr2, ArrayFromJSON(int64(), days_between));
    CheckScalarBinary("hours_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("hours_between", arr1, arr2, ArrayFromJSON(int64(), hours_between));
    CheckScalarBinary("minutes_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("minutes_between", arr1, arr2,
                      ArrayFromJSON(int64(), minutes_between));
    CheckScalarBinary("seconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("seconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), seconds_between));
    CheckScalarBinary("milliseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("milliseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), milliseconds_between));
    CheckScalarBinary("microseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("microseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), microseconds_between));
    CheckScalarBinary("nanoseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("nanoseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), nanoseconds_between));
  }

  for (auto date_case : {std::make_tuple(date32(), date32s, date32s2),
                         std::make_tuple(date64(), date64s, date64s2)}) {
    auto ty = std::get<0>(date_case);
    auto arr1 = ArrayFromJSON(ty, std::get<1>(date_case));
    auto arr2 = ArrayFromJSON(ty, std::get<2>(date_case));
    CheckScalarBinary("years_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("years_between", arr1, arr2, ArrayFromJSON(int64(), years_between));
    CheckScalarBinary("quarters_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("quarters_between", arr1, arr2,
                      ArrayFromJSON(int64(), quarters_between));
    CheckScalarBinary("month_interval_between", arr1, arr1,
                      ArrayFromJSON(month_interval(), zeros));
    CheckScalarBinary("month_interval_between", arr1, arr2,
                      ArrayFromJSON(month_interval(), months_between));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr1,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_zeros));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr2,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_date));
    CheckScalarBinary(
        "day_time_interval_between", arr1, arr1,
        ArrayFromJSON(day_time_interval(), day_time_interval_between_zeros));
    CheckScalarBinary("day_time_interval_between", arr1, arr2,
                      ArrayFromJSON(day_time_interval(), day_time_interval_between_date));
    CheckScalarBinary("weeks_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("weeks_between", arr1, arr2, ArrayFromJSON(int64(), weeks_between));
    CheckScalarBinary("days_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("days_between", arr1, arr2, ArrayFromJSON(int64(), days_between));
    CheckScalarBinary("hours_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("hours_between", arr1, arr2,
                      ArrayFromJSON(int64(), hours_between_date));
    CheckScalarBinary("minutes_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("minutes_between", arr1, arr2,
                      ArrayFromJSON(int64(), minutes_between_date));
    CheckScalarBinary("seconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("seconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), seconds_between_date));
    CheckScalarBinary("milliseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("milliseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), milliseconds_between_date));
    CheckScalarBinary("microseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("microseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), microseconds_between_date));
    CheckScalarBinary("nanoseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("nanoseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), nanoseconds_between_date));
  }

  struct TimeCase {
    std::shared_ptr<DataType> ty;
    std::string times1;
    std::string times2;
    std::string month_day_nano_interval_between;
    std::string day_time_interval_between;
    std::string milliseconds_between;
    std::string microseconds_between;
    std::string nanoseconds_between;
  };
  std::vector<TimeCase> cases = {
      {time32(TimeUnit::SECOND), times_s, times_s2,
       month_day_nano_interval_between_time_s, day_time_interval_between_time_s,
       milliseconds_between_time_s, microseconds_between_time_s,
       nanoseconds_between_time_s},
      {time32(TimeUnit::MILLI), times_ms, times_ms2,
       month_day_nano_interval_between_time_ms, day_time_interval_between_time,
       milliseconds_between_time, microseconds_between_time_ms,
       nanoseconds_between_time_ms},
      {time64(TimeUnit::MICRO), times_us, times_us2,
       month_day_nano_interval_between_time_us, day_time_interval_between_time,
       milliseconds_between_time, microseconds_between_time, nanoseconds_between_time_us},
      {time64(TimeUnit::NANO), times_ns, times_ns2, month_day_nano_interval_between_time,
       day_time_interval_between_time, milliseconds_between_time,
       microseconds_between_time, nanoseconds_between_time},
  };
  for (auto time_case : cases) {
    auto arr1 = ArrayFromJSON(time_case.ty, time_case.times1);
    auto arr2 = ArrayFromJSON(time_case.ty, time_case.times2);
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr1,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_zeros));
    CheckScalarBinary("month_day_nano_interval_between", arr1, arr2,
                      ArrayFromJSON(month_day_nano_interval(),
                                    time_case.month_day_nano_interval_between));
    CheckScalarBinary("hours_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("hours_between", arr1, arr2,
                      ArrayFromJSON(int64(), hours_between_time));
    CheckScalarBinary("minutes_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("minutes_between", arr1, arr2,
                      ArrayFromJSON(int64(), minutes_between_time));
    CheckScalarBinary("seconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("seconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), seconds_between_time));
    CheckScalarBinary(
        "day_time_interval_between", arr1, arr1,
        ArrayFromJSON(day_time_interval(), day_time_interval_between_zeros));
    CheckScalarBinary(
        "day_time_interval_between", arr1, arr2,
        ArrayFromJSON(day_time_interval(), time_case.day_time_interval_between));
    CheckScalarBinary("milliseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("milliseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), time_case.milliseconds_between));
    CheckScalarBinary("microseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("microseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), time_case.microseconds_between));
    CheckScalarBinary("nanoseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("nanoseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), time_case.nanoseconds_between));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalAddDateAndDuration) {
  for (auto op : {"add", "add_checked"}) {
    std::string milliseconds_between_date_and_time =
        "[59000, 84203000, 3560000, 12800000, 3905000, 7810000, 11715000, 15620000, "
        "19525000, 23430000, 27335000, 31240000, 35145000, 0, 0, 3723000, null]";
    std::string microseconds_between_date_and_time =
        "[59000000, 84203000000, 3560000000, 12800000000, 3905000000, 7810000000, "
        "11715000000, 15620000000, 19525000000, 23430000000, 27335000000, 31240000000, "
        "35145000000, 0, 0, 3723000000, null]";
    auto dates32 = ArrayFromJSON(date32(), date32s);
    auto dates64 = ArrayFromJSON(date64(), date64s);

    auto durations_ms =
        ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_date_and_time);
    auto timestamps_ms =
        ArrayFromJSON(timestamp(TimeUnit::MILLI), times_seconds_precision);
    CheckScalarBinaryCommutative(op, dates32, durations_ms, timestamps_ms);
    CheckScalarBinaryCommutative(op, dates64, durations_ms, timestamps_ms);

    auto durations_us =
        ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_date_and_time);
    auto timestamps_us =
        ArrayFromJSON(timestamp(TimeUnit::MICRO), times_seconds_precision);
    CheckScalarBinaryCommutative(op, dates32, durations_us, timestamps_us);
    CheckScalarBinaryCommutative(op, dates64, durations_us, timestamps_us);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalAddDuration) {
  for (auto op : {"add", "add_checked"}) {
    for (auto u : TimeUnit::values()) {
      auto unit = duration(u);
      CheckScalarBinary(op, ArrayFromJSON(unit, times_s),
                        ArrayFromJSON(unit, seconds_between_time),
                        ArrayFromJSON(unit, times_s2));
    }

    auto seconds_1 = ArrayFromJSON(duration(TimeUnit::SECOND), R"([1, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_3k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([3000, null])");
    CheckScalarBinary(op, seconds_1, milliseconds_2k, milliseconds_3k);
  }

  for (auto unit : TimeUnit::values()) {
    auto duration_ty = duration(unit);
    auto arr1 = ArrayFromJSON(duration_ty, R"([9223372036854775807, null])");
    auto arr2 = ArrayFromJSON(duration_ty, R"([1, null])");
    auto arr3 = ArrayFromJSON(duration_ty, R"([-9223372036854775808, null])");

    CheckScalarBinary("add", arr1, arr2, arr3);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("overflow"),
                                    CallFunction("add_checked", {arr1, arr2}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractDateAndDuration) {
  for (auto op : {"subtract", "subtract_checked"}) {
    std::string milliseconds_between_time_and_date =
        "[31535941000, -31706603000, 2674840000, -2604800000, 82495000,"
        "-180610000, -11715000, -15620000, -19525000, -23430000, -27335000,"
        "-31240000, -35145000, -86400000, -26352000000, 5180277000, null]";
    std::string microseconds_between_time_and_date =
        "[31535941000000, -31706603000000, 2674840000000, -2604800000000, 82495000000,"
        "-180610000000, -11715000000, -15620000000, -19525000000, -23430000000, "
        "-27335000000, -31240000000, -35145000000, -86400000000, -26352000000000, "
        "5180277000000, null]";
    auto dates32 = ArrayFromJSON(date32(), date32s2);
    auto dates64 = ArrayFromJSON(date64(), date64s2);

    auto durations_ms =
        ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_time_and_date);
    auto timestamps_ms =
        ArrayFromJSON(timestamp(TimeUnit::MILLI), times_seconds_precision);
    CheckScalarBinary(op, dates32, durations_ms, timestamps_ms);
    CheckScalarBinary(op, dates64, durations_ms, timestamps_ms);

    auto durations_us =
        ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_time_and_date);
    auto timestamps_us =
        ArrayFromJSON(timestamp(TimeUnit::MICRO), times_seconds_precision);
    CheckScalarBinary(op, dates32, durations_us, timestamps_us);
    CheckScalarBinary(op, dates64, durations_us, timestamps_us);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalAddTimestampAndDuration) {
  for (auto op : {"add", "add_checked"}) {
    for (auto tz : {"", "UTC", "Pacific/Marquesas"}) {
      auto timestamp_unit_s = timestamp(TimeUnit::SECOND, tz);
      auto duration_unit_s = duration(TimeUnit::SECOND);
      auto timestamp_unit_ms = timestamp(TimeUnit::MILLI, tz);
      auto duration_unit_ms = duration(TimeUnit::MILLI);
      auto timestamp_unit_us = timestamp(TimeUnit::MICRO, tz);
      auto duration_unit_us = duration(TimeUnit::MICRO);
      auto timestamp_unit_ns = timestamp(TimeUnit::NANO, tz);
      auto duration_unit_ns = duration(TimeUnit::NANO);

      CheckScalarBinaryCommutative(
          op, ArrayFromJSON(timestamp_unit_s, times_seconds_precision),
          ArrayFromJSON(duration_unit_s, seconds_between),
          ArrayFromJSON(timestamp_unit_s, times_seconds_precision2));
      CheckScalarBinaryCommutative(
          op, ArrayFromJSON(timestamp_unit_ms, times_seconds_precision),
          ArrayFromJSON(duration_unit_ms, milliseconds_between),
          ArrayFromJSON(timestamp_unit_ms, times_seconds_precision2));
      CheckScalarBinaryCommutative(
          op, ArrayFromJSON(timestamp_unit_us, times_seconds_precision),
          ArrayFromJSON(duration_unit_us, microseconds_between),
          ArrayFromJSON(timestamp_unit_us, times_seconds_precision2));
      CheckScalarBinaryCommutative(
          op, ArrayFromJSON(timestamp_unit_ns, times_seconds_precision),
          ArrayFromJSON(duration_unit_ns, nanoseconds_between),
          ArrayFromJSON(timestamp_unit_ns, times_seconds_precision2));
    }

    auto seconds_1 = ArrayFromJSON(timestamp(TimeUnit::SECOND), R"([1, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_3k = ArrayFromJSON(timestamp(TimeUnit::MILLI), R"([3000, null])");
    CheckScalarBinaryCommutative(op, seconds_1, milliseconds_2k, milliseconds_3k);

    auto seconds_1_tz = ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), R"([1, null])");
    auto milliseconds_3k_tz =
        ArrayFromJSON(timestamp(TimeUnit::MILLI, "UTC"), R"([3000, null])");
    CheckScalarBinaryCommutative(op, seconds_1_tz, milliseconds_2k, milliseconds_3k_tz);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractTimestampAndDuration) {
  for (auto op : {"subtract", "subtract_checked"}) {
    for (auto tz : {"", "UTC", "Pacific/Marquesas"}) {
      auto timestamp_unit_s = timestamp(TimeUnit::SECOND, tz);
      auto duration_unit_s = duration(TimeUnit::SECOND);
      auto timestamp_unit_ms = timestamp(TimeUnit::MILLI, tz);
      auto duration_unit_ms = duration(TimeUnit::MILLI);
      auto timestamp_unit_us = timestamp(TimeUnit::MICRO, tz);
      auto duration_unit_us = duration(TimeUnit::MICRO);
      auto timestamp_unit_ns = timestamp(TimeUnit::NANO, tz);
      auto duration_unit_ns = duration(TimeUnit::NANO);

      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_s, times_seconds_precision2),
                        ArrayFromJSON(duration_unit_s, seconds_between),
                        ArrayFromJSON(timestamp_unit_s, times_seconds_precision));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_ms, times_seconds_precision2),
                        ArrayFromJSON(duration_unit_ms, milliseconds_between),
                        ArrayFromJSON(timestamp_unit_ms, times_seconds_precision));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_us, times_seconds_precision2),
                        ArrayFromJSON(duration_unit_us, microseconds_between),
                        ArrayFromJSON(timestamp_unit_us, times_seconds_precision));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_ns, times_seconds_precision2),
                        ArrayFromJSON(duration_unit_ns, nanoseconds_between),
                        ArrayFromJSON(timestamp_unit_ns, times_seconds_precision));
    }

    auto seconds_3 = ArrayFromJSON(timestamp(TimeUnit::SECOND), R"([3, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_1k = ArrayFromJSON(timestamp(TimeUnit::MILLI), R"([1000, null])");
    CheckScalarBinary(op, seconds_3, milliseconds_2k, milliseconds_1k);

    auto seconds_3_tz = ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), R"([3, null])");
    auto milliseconds_1k_tz =
        ArrayFromJSON(timestamp(TimeUnit::MILLI, "UTC"), R"([1000, null])");
    CheckScalarBinary(op, seconds_3_tz, milliseconds_2k, milliseconds_1k_tz);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractDate) {
  for (auto op : {"subtract", "subtract_checked"}) {
    auto arr_date32s = ArrayFromJSON(date32(), date32s);
    auto arr_date32s2 = ArrayFromJSON(date32(), date32s2);
    auto arr_date64s = ArrayFromJSON(date64(), date64s);
    auto arr_date64s2 = ArrayFromJSON(date64(), date64s2);

    CheckScalarBinary(op, arr_date32s2, arr_date32s,
                      ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_date));
    CheckScalarBinary(
        op, arr_date64s2, arr_date64s,
        ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_date));
    CheckScalarBinary(
        op, arr_date64s2, arr_date32s,
        ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_date));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractTimestampAndDate) {
  std::string seconds_between_date_and_time =
      "[59, 84203, 3560, 12800, 3905, 7810, 11715, 15620, "
      "19525, 23430, 27335, 31240, 35145, 0, 0, 3723, null]";
  std::string milliseconds_between_date_and_time =
      "[59000, 84203000, 3560000, 12800000, 3905000, 7810000, 11715000, 15620000, "
      "19525000, 23430000, 27335000, 31240000, 35145000, 0, 0, 3723000, null]";
  std::string microseconds_between_date_and_time =
      "[59000000, 84203000000, 3560000000, 12800000000, 3905000000, 7810000000, "
      "11715000000, 15620000000, 19525000000, 23430000000, 27335000000, 31240000000, "
      "35145000000, 0, 0, 3723000000, null]";
  std::string nanoseconds_between_date_and_time =
      "[59000000000, 84203000000000, 3560000000000, 12800000000000, 3905000000000, "
      "7810000000000, 11715000000000, 15620000000000, 19525000000000, 23430000000000, "
      "27335000000000, 31240000000000, 35145000000000, 0, 0, 3723000000000, null]";
  std::string seconds_between_date_and_time2 =
      "[-59, -84203, -3560, -12800, -3905, -7810, -11715, -15620, "
      "-19525, -23430, -27335, -31240, -35145, 0, 0, -3723, null]";
  std::string milliseconds_between_date_and_time2 =
      "[-59000, -84203000, -3560000, -12800000, -3905000, -7810000, -11715000,"
      "-15620000, -19525000, -23430000, -27335000, -31240000, -35145000, 0, 0, "
      "-3723000, null]";
  std::string microseconds_between_date_and_time2 =
      "[-59000000, -84203000000, -3560000000, -12800000000, -3905000000, -7810000000, "
      "-11715000000, -15620000000, -19525000000, -23430000000, -27335000000,"
      "-31240000000, -35145000000, 0, 0, -3723000000, null]";
  std::string nanoseconds_between_date_and_time2 =
      "[-59000000000, -84203000000000, -3560000000000, -12800000000000, "
      "-3905000000000, -7810000000000, -11715000000000, -15620000000000, "
      "-19525000000000, -23430000000000, -27335000000000, -31240000000000, "
      "-35145000000000, 0, 0, -3723000000000, null]";

  auto arr_date32s = ArrayFromJSON(date32(), date32s);
  auto arr_date32s2 = ArrayFromJSON(date32(), date32s2);
  auto arr_date64s = ArrayFromJSON(date64(), date64s);
  auto arr_date64s2 = ArrayFromJSON(date64(), date64s2);
  auto timestamp_s = ArrayFromJSON(timestamp(TimeUnit::SECOND), times_seconds_precision);
  auto timestamp_ms = ArrayFromJSON(timestamp(TimeUnit::MILLI), times_seconds_precision);
  auto timestamp_us = ArrayFromJSON(timestamp(TimeUnit::MICRO), times_seconds_precision);
  auto timestamp_ns = ArrayFromJSON(timestamp(TimeUnit::NANO), times_seconds_precision);
  auto between_s =
      ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_date_and_time);
  auto between_ms =
      ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_date_and_time);
  auto between_us =
      ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_date_and_time);
  auto between_ns =
      ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between_date_and_time);
  auto between_s2 =
      ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_date_and_time2);
  auto between_ms2 =
      ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_date_and_time2);
  auto between_us2 =
      ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_date_and_time2);
  auto between_ns2 =
      ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between_date_and_time2);

  for (auto op : {"subtract", "subtract_checked"}) {
    CheckScalarBinary(op, timestamp_s, arr_date32s, between_s);
    CheckScalarBinary(op, timestamp_ms, arr_date32s, between_ms);
    CheckScalarBinary(op, timestamp_us, arr_date32s, between_us);
    CheckScalarBinary(op, timestamp_ns, arr_date32s, between_ns);
    CheckScalarBinary(op, timestamp_s, arr_date64s, between_ms);
    CheckScalarBinary(op, timestamp_ms, arr_date64s, between_ms);
    CheckScalarBinary(op, timestamp_us, arr_date64s, between_us);
    CheckScalarBinary(op, timestamp_ns, arr_date64s, between_ns);

    CheckScalarBinary(op, arr_date32s, timestamp_s, between_s2);
    CheckScalarBinary(op, arr_date32s, timestamp_ms, between_ms2);
    CheckScalarBinary(op, arr_date32s, timestamp_us, between_us2);
    CheckScalarBinary(op, arr_date32s, timestamp_ns, between_ns2);
    CheckScalarBinary(op, arr_date64s, timestamp_s, between_ms2);
    CheckScalarBinary(op, arr_date64s, timestamp_ms, between_ms2);
    CheckScalarBinary(op, arr_date64s, timestamp_us, between_us2);
    CheckScalarBinary(op, arr_date64s, timestamp_ns, between_ns2);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractTimestamp) {
  for (auto op : {"subtract", "subtract_checked"}) {
    for (auto tz : {"", "UTC", "Pacific/Marquesas"}) {
      auto timestamp_unit_s = timestamp(TimeUnit::SECOND, tz);
      auto duration_unit_s = duration(TimeUnit::SECOND);
      auto timestamp_unit_ms = timestamp(TimeUnit::MILLI, tz);
      auto duration_unit_ms = duration(TimeUnit::MILLI);
      auto timestamp_unit_us = timestamp(TimeUnit::MICRO, tz);
      auto duration_unit_us = duration(TimeUnit::MICRO);
      auto timestamp_unit_ns = timestamp(TimeUnit::NANO, tz);
      auto duration_unit_ns = duration(TimeUnit::NANO);

      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_s, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_s, times_seconds_precision),
                        ArrayFromJSON(duration_unit_s, seconds_between));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_ms, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_ms, times_seconds_precision),
                        ArrayFromJSON(duration_unit_ms, milliseconds_between));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_us, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_us, times_seconds_precision),
                        ArrayFromJSON(duration_unit_us, microseconds_between));
      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_ns, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_ns, times_seconds_precision),
                        ArrayFromJSON(duration_unit_ns, nanoseconds_between));

      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_s, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_ms, times_seconds_precision),
                        ArrayFromJSON(duration_unit_ms, milliseconds_between));

      CheckScalarBinary(op, ArrayFromJSON(timestamp_unit_ms, times_seconds_precision2),
                        ArrayFromJSON(timestamp_unit_s, times_seconds_precision),
                        ArrayFromJSON(duration_unit_ms, milliseconds_between));
    }

    CheckScalarBinary(
        op, ArrayFromJSON(timestamp(TimeUnit::NANO, "UTC"), times_seconds_precision2),
        ArrayFromJSON(timestamp(TimeUnit::SECOND, "Pacific/Marquesas"),
                      times_seconds_precision),
        ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between));

    CheckScalarBinary(
        op, ArrayFromJSON(timestamp(TimeUnit::SECOND), times_seconds_precision2),
        ArrayFromJSON(timestamp(TimeUnit::NANO), times_seconds_precision),
        ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between));

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("Subtraction of zoned and non-zoned times is ambiguous."),
        CallFunction(
            op,
            {ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), times_seconds_precision2),
             ArrayFromJSON(timestamp(TimeUnit::SECOND), times_seconds_precision)}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractTime) {
  for (auto op : {"subtract", "subtract_checked"}) {
    auto arr_s = ArrayFromJSON(time32(TimeUnit::SECOND), times_s);
    auto arr_s2 = ArrayFromJSON(time32(TimeUnit::SECOND), times_s2);
    auto arr_ms = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms);
    auto arr_ms2 = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms2);
    auto arr_us = ArrayFromJSON(time64(TimeUnit::MICRO), times_us);
    auto arr_us2 = ArrayFromJSON(time64(TimeUnit::MICRO), times_us2);
    auto arr_ns = ArrayFromJSON(time64(TimeUnit::NANO), times_ns);
    auto arr_ns2 = ArrayFromJSON(time64(TimeUnit::NANO), times_ns2);

    CheckScalarBinary(op, arr_s2, arr_s,
                      ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_time));
    CheckScalarBinary(
        op, arr_ms2, arr_ms,
        ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_time));
    CheckScalarBinary(
        op, arr_us2, arr_us,
        ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_time));
    CheckScalarBinary(op, arr_ns2, arr_ns,
                      ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between_time));

    CheckScalarBinary(op, arr_s, arr_s, ArrayFromJSON(duration(TimeUnit::SECOND), zeros));
    CheckScalarBinary(op, arr_ms, arr_ms,
                      ArrayFromJSON(duration(TimeUnit::MILLI), zeros));
    CheckScalarBinary(op, arr_us, arr_us,
                      ArrayFromJSON(duration(TimeUnit::MICRO), zeros));
    CheckScalarBinary(op, arr_ns, arr_ns, ArrayFromJSON(duration(TimeUnit::NANO), zeros));

    auto seconds_3 = ArrayFromJSON(time32(TimeUnit::SECOND), R"([3, null])");
    auto milliseconds_2k = ArrayFromJSON(time32(TimeUnit::MILLI), R"([1999, null])");
    auto milliseconds_1k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([1001, null])");
    auto microseconds_2M = ArrayFromJSON(time64(TimeUnit::MICRO), R"([1999999, null])");
    auto microseconds_1M = ArrayFromJSON(duration(TimeUnit::MICRO), R"([1000001, null])");
    CheckScalarBinary(op, seconds_3, milliseconds_2k, milliseconds_1k);
    CheckScalarBinary(op, seconds_3, microseconds_2M, microseconds_1M);
  }
}

TEST_F(ScalarTemporalTest, TestTemporalAddTimeAndDuration) {
  for (auto op : {"add", "add_checked"}) {
    auto arr_s = ArrayFromJSON(time32(TimeUnit::SECOND), times_s);
    auto arr_s2 = ArrayFromJSON(time32(TimeUnit::SECOND), times_s2);
    auto arr_ms = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms);
    auto arr_ms2 = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms2);
    auto arr_us = ArrayFromJSON(time64(TimeUnit::MICRO), times_us);
    auto arr_us2 = ArrayFromJSON(time64(TimeUnit::MICRO), times_us2);
    auto arr_ns = ArrayFromJSON(time64(TimeUnit::NANO), times_ns);
    auto arr_ns2 = ArrayFromJSON(time64(TimeUnit::NANO), times_ns2);

    CheckScalarBinaryCommutative(
        op, arr_s, ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_time),
        arr_s2);
    CheckScalarBinaryCommutative(
        op, arr_ms, ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_time),
        arr_ms2);
    CheckScalarBinaryCommutative(
        op, arr_us, ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_time),
        arr_us2);
    CheckScalarBinaryCommutative(
        op, arr_ns, ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between_time),
        arr_ns2);

    auto seconds_1 = ArrayFromJSON(time32(TimeUnit::SECOND), R"([1, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_3k = ArrayFromJSON(time32(TimeUnit::MILLI), R"([3000, null])");
    auto nanoseconds_1G = ArrayFromJSON(time64(TimeUnit::NANO), R"([1000000000, null])");
    auto microseconds_2M = ArrayFromJSON(duration(TimeUnit::MICRO), R"([2000000, null])");
    auto nanoseconds_3M = ArrayFromJSON(time64(TimeUnit::NANO), R"([3000000000, null])");
    auto microseconds_3M = ArrayFromJSON(time64(TimeUnit::MICRO), R"([3000000, null])");
    CheckScalarBinaryCommutative(op, seconds_1, milliseconds_2k, milliseconds_3k);
    CheckScalarBinaryCommutative(op, nanoseconds_1G, microseconds_2M, nanoseconds_3M);
    CheckScalarBinaryCommutative(op, seconds_1, microseconds_2M, microseconds_3M);

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("-1 is not within the acceptable range of [0, 86400)"),
        CallFunction(op, {ArrayFromJSON(time32(TimeUnit::SECOND), R"([0, null])"),
                          ArrayFromJSON(duration(TimeUnit::SECOND), R"([-1, null])")}));

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "86400000000001 is not within the acceptable range of [0, 86400000000000)"),
        CallFunction(op,
                     {ArrayFromJSON(time64(TimeUnit::MICRO), R"([86400000000, null])"),
                      ArrayFromJSON(duration(TimeUnit::NANO), R"([1, null])")}));

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "86400000001 is not within the acceptable range of [0, 86400000000)"),
        CallFunction(op,
                     {ArrayFromJSON(time64(TimeUnit::MICRO), R"([86400000000, null])"),
                      ArrayFromJSON(duration(TimeUnit::MICRO), R"([1, null])")}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractTimeAndDuration) {
  for (auto op : {"subtract", "subtract_checked"}) {
    auto arr_s = ArrayFromJSON(time32(TimeUnit::SECOND), times_s);
    auto arr_s2 = ArrayFromJSON(time32(TimeUnit::SECOND), times_s2);
    auto arr_ms = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms);
    auto arr_ms2 = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms2);
    auto arr_us = ArrayFromJSON(time64(TimeUnit::MICRO), times_us);
    auto arr_us2 = ArrayFromJSON(time64(TimeUnit::MICRO), times_us2);
    auto arr_ns = ArrayFromJSON(time64(TimeUnit::NANO), times_ns);
    auto arr_ns2 = ArrayFromJSON(time64(TimeUnit::NANO), times_ns2);

    CheckScalarBinary(op, arr_s2,
                      ArrayFromJSON(duration(TimeUnit::SECOND), seconds_between_time),
                      arr_s);
    CheckScalarBinary(op, arr_ms2,
                      ArrayFromJSON(duration(TimeUnit::MILLI), milliseconds_between_time),
                      arr_ms);
    CheckScalarBinary(op, arr_us2,
                      ArrayFromJSON(duration(TimeUnit::MICRO), microseconds_between_time),
                      arr_us);
    CheckScalarBinary(op, arr_ns2,
                      ArrayFromJSON(duration(TimeUnit::NANO), nanoseconds_between_time),
                      arr_ns);

    auto seconds_3 = ArrayFromJSON(time32(TimeUnit::SECOND), R"([3, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_1k = ArrayFromJSON(time32(TimeUnit::MILLI), R"([1000, null])");
    auto nanoseconds_3G = ArrayFromJSON(time64(TimeUnit::NANO), R"([3000000000, null])");
    auto microseconds_2M = ArrayFromJSON(duration(TimeUnit::MICRO), R"([2000000, null])");
    auto nanoseconds_1M = ArrayFromJSON(time64(TimeUnit::NANO), R"([1000000000, null])");
    auto microseconds_1M = ArrayFromJSON(time64(TimeUnit::MICRO), R"([1000000, null])");
    CheckScalarBinary(op, seconds_3, milliseconds_2k, milliseconds_1k);
    CheckScalarBinary(op, nanoseconds_3G, microseconds_2M, nanoseconds_1M);
    CheckScalarBinary(op, seconds_3, microseconds_2M, microseconds_1M);

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("-1 is not within the acceptable range of [0, 86400)"),
        CallFunction(op, {ArrayFromJSON(time32(TimeUnit::SECOND), R"([1, null])"),
                          ArrayFromJSON(duration(TimeUnit::SECOND), R"([2, null])")}));

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "-1000 is not within the acceptable range of [0, 86400000000000)"),
        CallFunction(op, {ArrayFromJSON(time64(TimeUnit::MICRO), R"([1, null])"),
                          ArrayFromJSON(duration(TimeUnit::NANO), R"([2000, null])")}));

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "86400000001000 is not within the acceptable range of [0, 86400000000000)"),
        CallFunction(op,
                     {ArrayFromJSON(time64(TimeUnit::MICRO), R"([86400000000, null])"),
                      ArrayFromJSON(duration(TimeUnit::NANO), R"([-1000, null])")}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalSubtractDuration) {
  for (auto op : {"subtract", "subtract_checked"}) {
    for (auto u : TimeUnit::values()) {
      auto unit = duration(u);
      CheckScalarBinary(op, ArrayFromJSON(unit, times_s2), ArrayFromJSON(unit, times_s),
                        ArrayFromJSON(unit, seconds_between_time));
    }

    auto seconds_3 = ArrayFromJSON(duration(TimeUnit::SECOND), R"([3, null])");
    auto milliseconds_2k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([2000, null])");
    auto milliseconds_1k = ArrayFromJSON(duration(TimeUnit::MILLI), R"([1000, null])");
    CheckScalarBinary(op, seconds_3, milliseconds_2k, milliseconds_1k);
  }

  for (auto unit : TimeUnit::values()) {
    auto duration_ty = duration(unit);
    auto arr1 = ArrayFromJSON(duration_ty, R"([-9223372036854775808, null])");
    auto arr2 = ArrayFromJSON(duration_ty, R"([1, null])");
    auto arr3 = ArrayFromJSON(duration_ty, R"([9223372036854775807, null])");

    CheckScalarBinary("subtract", arr1, arr2, arr3);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("overflow"),
                                    CallFunction("subtract_checked", {arr1, arr2}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalMultiplyDuration) {
  std::shared_ptr<Array> max_array;
  auto max = std::numeric_limits<int64_t>::max();
  ArrayFromVector<Int64Type, int64_t>({max, max, max, max, max}, &max_array);

  for (auto u : TimeUnit::values()) {
    auto unit = duration(u);
    auto durations = ArrayFromJSON(unit, R"([0, -1, 2, 6, null])");
    auto multipliers = ArrayFromJSON(int64(), R"([0, 3, 2, 7, null])");
    auto durations_multiplied = ArrayFromJSON(unit, R"([0, -3, 4, 42, null])");

    CheckScalarBinaryCommutative("multiply", durations, multipliers,
                                 durations_multiplied);
    CheckScalarBinaryCommutative("multiply_checked", durations, multipliers,
                                 durations_multiplied);

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Invalid: overflow"),
        CallFunction("multiply_checked", {durations, max_array}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Invalid: overflow"),
        CallFunction("multiply_checked", {max_array, durations}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalDivideDuration) {
  for (auto u : TimeUnit::values()) {
    auto unit = duration(u);
    auto divided_durations = ArrayFromJSON(unit, R"([0, -1, -2, 6, null])");
    auto divisors = ArrayFromJSON(int64(), R"([3, 3, -2, 7, null])");
    auto durations = ArrayFromJSON(unit, R"([1, -3, 4, 42, null])");
    auto zeros = ArrayFromJSON(int64(), R"([0, 0, 0, 0, null])");
    CheckScalarBinary("divide", durations, divisors, divided_durations);
    CheckScalarBinary("divide_checked", durations, divisors, divided_durations);

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("Invalid: divide by zero"),
                                    CallFunction("divide", {durations, zeros}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("Invalid: divide by zero"),
                                    CallFunction("divide_checked", {durations, zeros}));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalDifferenceWeeks) {
  auto raw_days = ArrayFromJSON(timestamp(TimeUnit::SECOND), R"([
    "2021-08-09", "2021-08-10", "2021-08-11", "2021-08-12", "2021-08-13", "2021-08-14", "2021-08-15",
    "2021-08-16", "2021-08-17", "2021-08-18", "2021-08-19", "2021-08-20", "2021-08-21", "2021-08-22",
    "2021-08-23", "2021-08-24", "2021-08-25", "2021-08-26", "2021-08-27", "2021-08-28", "2021-08-29"
  ])");
  std::vector<std::string> ts_scalars = {R"("2021-08-16")", R"("2021-08-17")",
                                         R"("2021-08-18")"};
  std::vector<std::string> date32_scalars = {"18855", "18856", "18857"};
  std::vector<std::string> date64_scalars = {"1629072000000", "1629158400000",
                                             "1629244800000"};

  for (const auto& test_case : {std::make_pair(timestamp(TimeUnit::SECOND), ts_scalars),
                                std::make_pair(date32(), date32_scalars),
                                std::make_pair(date64(), date64_scalars)}) {
    auto ty = test_case.first;
    std::shared_ptr<Array> days;
    if (ty->id() == Type::TIMESTAMP) {
      days = raw_days;
    } else {
      ASSERT_OK_AND_ASSIGN(auto temp, Cast(raw_days, ty));
      days = temp.make_array();
    }
    auto aug16 = ScalarFromJSON(ty, test_case.second[0]);
    auto aug17 = ScalarFromJSON(ty, test_case.second[1]);
    auto aug18 = ScalarFromJSON(ty, test_case.second[2]);

    DayOfWeekOptions options(/*one_based_numbering=*/false, /*week_start=Monday*/ 1);
    EXPECT_THAT(CallFunction("weeks_between", {aug16, days}, &options),
                ResultWith(Datum(ArrayFromJSON(int64(), R"([
-1, -1, -1, -1, -1, -1, -1,
0, 0, 0, 0, 0, 0, 0,
1, 1, 1, 1, 1, 1, 1
])"))));
    EXPECT_THAT(CallFunction("weeks_between", {aug17, days}, &options),
                ResultWith(Datum(ArrayFromJSON(int64(), R"([
-1, -1, -1, -1, -1, -1, -1,
0, 0, 0, 0, 0, 0, 0,
1, 1, 1, 1, 1, 1, 1
])"))));

    options.week_start = 3;  // Wednesday
    EXPECT_THAT(CallFunction("weeks_between", {aug16, days}, &options),
                ResultWith(Datum(ArrayFromJSON(int64(), R"([
-1, -1, 0, 0, 0, 0, 0,
0, 0, 1, 1, 1, 1, 1,
1, 1, 2, 2, 2, 2, 2
])"))));
    EXPECT_THAT(CallFunction("weeks_between", {aug17, days}, &options),
                ResultWith(Datum(ArrayFromJSON(int64(), R"([
-1, -1, 0, 0, 0, 0, 0,
0, 0, 1, 1, 1, 1, 1,
1, 1, 2, 2, 2, 2, 2
])"))));
    EXPECT_THAT(CallFunction("weeks_between", {aug18, days}, &options),
                ResultWith(Datum(ArrayFromJSON(int64(), R"([
-2, -2, -1, -1, -1, -1, -1,
-1, -1, 0, 0, 0, 0, 0,
0, 0, 1, 1, 1, 1, 1
])"))));
  }
}

TEST_F(ScalarTemporalTest, TestTemporalDifferenceErrors) {
  Datum arr1 = ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/New_York"),
                             R"(["1970-01-01T00:00:59"])");
  Datum arr2 = ArrayFromJSON(timestamp(TimeUnit::SECOND, "America/Phoenix"),
                             R"(["1970-01-01T00:00:59"])");
  Datum arr3 = ArrayFromJSON(timestamp(TimeUnit::SECOND), R"(["1970-01-01T00:00:59"])");
  Datum arr4 =
      ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), R"(["1970-01-01T00:00:59"])");
  for (auto fn :
       {"years_between", "month_interval_between", "month_day_nano_interval_between",
        "day_time_interval_between", "weeks_between", "days_between", "hours_between",
        "minutes_between", "seconds_between", "milliseconds_between",
        "microseconds_between", "nanoseconds_between"}) {
    SCOPED_TRACE(fn);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr("Got differing time zone 'America/Phoenix' for argument 2; "
                             "expected 'America/New_York'"),
        CallFunction(fn, {arr1, arr2}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr(
            "Got differing time zone 'America/Phoenix' for argument 2; expected ''"),
        CallFunction(fn, {arr3, arr2}));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        TypeError,
        ::testing::HasSubstr("Got differing time zone 'UTC' for argument 2; expected ''"),
        CallFunction(fn, {arr3, arr4}));
  }

  DayOfWeekOptions options;
  options.week_start = 20;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("week_start must follow ISO convention (Monday=1, Sunday=7). "
                           "Got week_start=20"),
      CallFunction("weeks_between", {arr1, arr1}, &options));
}

TEST_F(ScalarTemporalTest, TestLocalTimestamp) {
  const char* times_seconds_precision =
      R"(["1970-01-01T00:00:59", "2000-02-29T23:23:23", "2033-05-18T03:33:20",
          "2020-01-01T01:05:05", "2019-12-31T02:10:10", "2019-12-30T03:15:15",
          "2009-12-31T04:20:20", "2010-01-01T05:25:25", "2010-01-03T06:30:30",
          "2010-01-04T07:35:35", "2006-01-01T08:40:40", "2005-12-31T09:45:45",
          "2008-12-28", "2008-12-29", "2012-01-01 01:02:03", null])";

  const char* expected_local_kolkata =
      R"(["1970-01-01 05:30:59", "2000-03-01 04:53:23", "2033-05-18 09:03:20",
          "2020-01-01 06:35:05", "2019-12-31 07:40:10", "2019-12-30 08:45:15",
          "2009-12-31 09:50:20", "2010-01-01 10:55:25", "2010-01-03 12:00:30",
          "2010-01-04 13:05:35", "2006-01-01 14:10:40", "2005-12-31 15:15:45",
          "2008-12-28 05:30:00", "2008-12-29 05:30:00", "2012-01-01 06:32:03", null])";
  const char* expected_local_marquesas =
      R"(["1969-12-31 14:30:59", "2000-02-29 13:53:23", "2033-05-17 18:03:20",
          "2019-12-31 15:35:05", "2019-12-30 16:40:10", "2019-12-29 17:45:15",
          "2009-12-30 18:50:20", "2009-12-31 19:55:25", "2010-01-02 21:00:30",
          "2010-01-03 22:05:35", "2005-12-31 23:10:40", "2005-12-31 00:15:45",
          "2008-12-27 14:30:00", "2008-12-28 14:30:00", "2011-12-31 15:32:03", null])";

  for (auto u : TimeUnit::values()) {
    CheckScalarUnary("local_timestamp", timestamp(u), times_seconds_precision,
                     timestamp(u), times_seconds_precision);
    CheckScalarUnary("local_timestamp", timestamp(u, "UTC"), times_seconds_precision,
                     timestamp(u), times_seconds_precision);
    CheckScalarUnary("local_timestamp", timestamp(u, "Asia/Kolkata"),
                     times_seconds_precision, timestamp(u), expected_local_kolkata);
    CheckScalarUnary("local_timestamp", timestamp(u, "Pacific/Marquesas"),
                     times_seconds_precision, timestamp(u), expected_local_marquesas);
  }
}

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

  for (auto u : TimeUnit::values()) {
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

  for (auto u : TimeUnit::values()) {
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

  for (auto u : TimeUnit::values()) {
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

  auto options_hms = StrftimeOptions("%H:%M:%S");
  auto options_ymdhms = StrftimeOptions("%Y-%m-%dT%H:%M:%S");

  const char* times_s = R"([59, null])";
  const char* times_ms = R"([59123, null])";
  const char* times_us = R"([59123456, null])";
  const char* times_ns = R"([59123456789, null])";
  const char* hms_s = R"(["00:00:59", null])";
  const char* hms_ms = R"(["00:00:59.123", null])";
  const char* hms_us = R"(["00:00:59.123456", null])";
  const char* hms_ns = R"(["00:00:59.123456789", null])";
  const char* ymdhms_s = R"(["1970-01-01T00:00:59", null])";
  const char* ymdhms_ms = R"(["1970-01-01T00:00:59.123", null])";
  const char* ymdhms_us = R"(["1970-01-01T00:00:59.123456", null])";
  const char* ymdhms_ns = R"(["1970-01-01T00:00:59.123456789", null])";

  CheckScalarUnary("strftime", time32(TimeUnit::SECOND), times_s, utf8(), hms_s,
                   &options_hms);
  CheckScalarUnary("strftime", time32(TimeUnit::MILLI), times_ms, utf8(), hms_ms,
                   &options_hms);
  CheckScalarUnary("strftime", time64(TimeUnit::MICRO), times_us, utf8(), hms_us,
                   &options_hms);
  CheckScalarUnary("strftime", time64(TimeUnit::NANO), times_ns, utf8(), hms_ns,
                   &options_hms);

  CheckScalarUnary("strftime", time32(TimeUnit::SECOND), times_s, utf8(), ymdhms_s,
                   &options_ymdhms);
  CheckScalarUnary("strftime", time32(TimeUnit::MILLI), times_ms, utf8(), ymdhms_ms,
                   &options_ymdhms);
  CheckScalarUnary("strftime", time64(TimeUnit::MICRO), times_us, utf8(), ymdhms_us,
                   &options_ymdhms);
  CheckScalarUnary("strftime", time64(TimeUnit::NANO), times_ns, utf8(), ymdhms_ns,
                   &options_ymdhms);

  auto arr_s = ArrayFromJSON(time32(TimeUnit::SECOND), times_s);
  auto arr_ms = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms);
  auto arr_us = ArrayFromJSON(time64(TimeUnit::MICRO), times_us);
  auto arr_ns = ArrayFromJSON(time64(TimeUnit::NANO), times_ns);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr_s, StrftimeOptions("%Y-%m-%dT%H:%M:%S%z")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr_ms, StrftimeOptions("%Y-%m-%dT%H:%M:%S%Z")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr_us, StrftimeOptions("%Y-%m-%dT%H:%M:%S%z")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: Timezone not present, cannot convert to string"),
      Strftime(arr_ns, StrftimeOptions("%Y-%m-%dT%H:%M:%S%Z")));

  auto options_ymd = StrftimeOptions("%Y-%m-%d");

  const char* date32s = R"([0, 10957, 10958, null])";
  const char* date64s = R"([0, 946684800000, 946771200000, null])";
  const char* dates32_ymd = R"(["1970-01-01", "2000-01-01", "2000-01-02", null])";
  const char* dates64_ymd = R"(["1970-01-01", "2000-01-01", "2000-01-02", null])";
  const char* dates32_ymdhms =
      R"(["1970-01-01T00:00:00", "2000-01-01T00:00:00", "2000-01-02T00:00:00", null])";
  const char* dates64_ymdhms =
      R"(["1970-01-01T00:00:00.000", "2000-01-01T00:00:00.000",
          "2000-01-02T00:00:00.000", null])";

  CheckScalarUnary("strftime", date32(), date32s, utf8(), dates32_ymd, &options_ymd);
  CheckScalarUnary("strftime", date64(), date64s, utf8(), dates64_ymd, &options_ymd);
  CheckScalarUnary("strftime", date32(), date32s, utf8(), dates32_ymdhms,
                   &options_ymdhms);
  CheckScalarUnary("strftime", date64(), date64s, utf8(), dates64_ymdhms,
                   &options_ymdhms);
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
#ifdef _WIN32
  GTEST_SKIP() << "There is a known bug in strftime for locales on Windows (ARROW-15922)";
#else
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
#endif
}

TEST_F(ScalarTemporalTest, StrftimeInvalidLocale) {
  auto options = StrftimeOptions("%d %B %Y %H:%M:%S", "non-existent");
  const char* seconds = R"(["1970-01-01T00:00:59", null])";
  auto arr = ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC"), seconds);

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Cannot find locale 'non-existent'"),
                                  Strftime(arr, options));
}

TEST_F(ScalarTemporalTest, TestTemporalDifferenceZoned) {
  for (auto u : TimeUnit::values()) {
    auto unit = timestamp(u, "Pacific/Marquesas");
    auto arr1 = ArrayFromJSON(unit, times_seconds_precision);
    auto arr2 = ArrayFromJSON(unit, times_seconds_precision2);
    CheckScalarBinary("years_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("years_between", arr1, arr2,
                      ArrayFromJSON(int64(), years_between_tz));
    CheckScalarBinary("quarters_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("quarters_between", arr1, arr2,
                      ArrayFromJSON(int64(), quarters_between_tz));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr1,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_zeros));
    CheckScalarBinary(
        "month_day_nano_interval_between", arr1, arr2,
        ArrayFromJSON(month_day_nano_interval(), month_day_nano_interval_between_tz));
    CheckScalarBinary("month_interval_between", arr1, arr1,
                      ArrayFromJSON(month_interval(), zeros));
    CheckScalarBinary("month_interval_between", arr1, arr2,
                      ArrayFromJSON(month_interval(), months_between_tz));
    CheckScalarBinary(
        "day_time_interval_between", arr1, arr1,
        ArrayFromJSON(day_time_interval(), day_time_interval_between_zeros));
    CheckScalarBinary("day_time_interval_between", arr1, arr2,
                      ArrayFromJSON(day_time_interval(), day_time_interval_between_tz));
    CheckScalarBinary("weeks_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("weeks_between", arr1, arr2,
                      ArrayFromJSON(int64(), weeks_between_tz));
    CheckScalarBinary("days_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("days_between", arr1, arr2,
                      ArrayFromJSON(int64(), days_between_tz));
    CheckScalarBinary("hours_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("hours_between", arr1, arr2,
                      ArrayFromJSON(int64(), hours_between_tz));
    CheckScalarBinary("minutes_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("minutes_between", arr1, arr2,
                      ArrayFromJSON(int64(), minutes_between));
    CheckScalarBinary("seconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("seconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), seconds_between));
    CheckScalarBinary("milliseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("milliseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), milliseconds_between));
    CheckScalarBinary("microseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("microseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), microseconds_between));
    CheckScalarBinary("nanoseconds_between", arr1, arr1, ArrayFromJSON(int64(), zeros));
    CheckScalarBinary("nanoseconds_between", arr1, arr2,
                      ArrayFromJSON(int64(), nanoseconds_between));
  }
}

TEST_F(ScalarTemporalTest, TestCeilTemporal) {
  std::string op = "ceil_temporal";
  const char* ceil_1_nanosecond =
      R"(["1970-01-01 00:00:59.123456789", "2000-02-29 23:23:23.999999999",
          "1899-01-01 00:59:20.001001001", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* ceil_1_microsecond =
      R"(["1970-01-01 00:00:59.123457", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001002", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004132",
          "2010-01-01 05:25:25.005321", "2010-01-03 06:30:30.006163",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* ceil_1_millisecond =
      R"(["1970-01-01 00:00:59.124", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:20.002", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.001", "2019-12-31 02:10:10.002",
          "2019-12-30 03:15:15.003", "2009-12-31 04:20:20.005",
          "2010-01-01 05:25:25.006", "2010-01-03 06:30:30.007",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* ceil_1_second =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:23:24", "1899-01-01 00:59:21",
          "2033-05-18 03:33:20", "2020-01-01 01:05:06", "2019-12-31 02:10:11",
          "2019-12-30 03:15:16", "2009-12-31 04:20:21", "2010-01-01 05:25:26",
          "2010-01-03 06:30:31", "2010-01-04 07:35:35", "2006-01-01 08:40:40",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:03", null])";
  const char* ceil_1_minute =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:24:00", "1899-01-01 01:00:00",
          "2033-05-18 03:34:00", "2020-01-01 01:06:00", "2019-12-31 02:11:00",
          "2019-12-30 03:16:00", "2009-12-31 04:21:00", "2010-01-01 05:26:00",
          "2010-01-03 06:31:00", "2010-01-04 07:36:00", "2006-01-01 08:41:00",
          "2005-12-31 09:46:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:03:00", null])";
  const char* ceil_1_hour =
      R"(["1970-01-01 01:00:00", "2000-03-01 00:00:00", "1899-01-01 01:00:00",
          "2033-05-18 04:00:00", "2020-01-01 02:00:00", "2019-12-31 03:00:00",
          "2019-12-30 04:00:00", "2009-12-31 05:00:00", "2010-01-01 06:00:00",
          "2010-01-03 07:00:00", "2010-01-04 08:00:00", "2006-01-01 09:00:00",
          "2005-12-31 10:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 02:00:00", null])";
  const char* ceil_1_day =
      R"(["1970-01-02", "2000-03-01", "1899-01-02", "2033-05-19", "2020-01-02",
          "2020-01-01", "2019-12-31", "2010-01-01", "2010-01-02", "2010-01-04",
          "2010-01-05", "2006-01-02", "2006-01-01", "2008-12-28", "2008-12-29",
          "2012-01-02", null])";
  const char* ceil_1_weeks =
      R"(["1970-01-05", "2000-03-06", "1899-01-02", "2033-05-23", "2020-01-06",
          "2020-01-06", "2020-01-06", "2010-01-04", "2010-01-04", "2010-01-04",
          "2010-01-11", "2006-01-02", "2006-01-02", "2008-12-29", "2008-12-29",
          "2012-01-02",  null])";
  const char* ceil_1_weeks_sunday =
      R"(["1970-01-04", "2000-03-05", "1899-01-08", "2033-05-22", "2020-01-05",
          "2020-01-05", "2020-01-05", "2010-01-03", "2010-01-03", "2010-01-10",
          "2010-01-10", "2006-01-08", "2006-01-01", "2008-12-28", "2009-01-04",
          "2012-01-08",  null])";
  const char* ceil_1_months =
      R"(["1970-02-01", "2000-03-01", "1899-02-01", "2033-06-01", "2020-02-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-02-01", "2010-02-01",
          "2010-02-01", "2006-02-01", "2006-01-01", "2009-01-01", "2009-01-01",
          "2012-02-01", null])";
  const char* ceil_1_quarters =
      R"(["1970-04-01", "2000-04-01", "1899-04-01", "2033-07-01", "2020-04-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-04-01", "2010-04-01",
          "2010-04-01", "2006-04-01", "2006-01-01", "2009-01-01", "2009-01-01",
          "2012-04-01", null])";
  const char* ceil_1_years =
      R"(["1971-01-01", "2001-01-01", "1900-01-01", "2034-01-01", "2021-01-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2011-01-01", "2011-01-01",
          "2011-01-01", "2007-01-01", "2006-01-01", "2009-01-01", "2009-01-01",
          "2013-01-01", null])";
  const char* ceil_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456790", "2000-02-29 23:23:24.000000000",
          "1899-01-01 00:59:20.001001005", "2033-05-18 03:33:20.000000010",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163005",
          "2010-01-04 07:35:35.000000010", "2006-01-01 08:40:40.000000005",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* ceil_15_microsecond =
      R"(["1970-01-01 00:00:59.123460", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001015", "2033-05-18 03:33:20.000010",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005330", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000010", "2006-01-01 08:40:40.000005",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* ceil_15_millisecond =
      R"(["1970-01-01 00:00:59.130", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:20.010", "2033-05-18 03:33:20.010",
          "2020-01-01 01:05:05.010", "2019-12-31 02:10:10.005",
          "2019-12-30 03:15:15.015", "2009-12-31 04:20:20.010",
          "2010-01-01 05:25:25.020", "2010-01-03 06:30:30.015",
          "2010-01-04 07:35:35.010", "2006-01-01 08:40:40.005",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* ceil_15_second =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:23:30", "1899-01-01 00:59:30",
          "2033-05-18 03:33:30", "2020-01-01 01:05:15", "2019-12-31 02:10:15",
          "2019-12-30 03:15:30", "2009-12-31 04:20:30", "2010-01-01 05:25:30",
          "2010-01-03 06:30:45", "2010-01-04 07:35:45", "2006-01-01 08:40:45",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:15", null])";
  const char* ceil_15_minute =
      R"(["1970-01-01 00:15:00", "2000-02-29 23:30:00", "1899-01-01 01:00:00",
          "2033-05-18 03:45:00", "2020-01-01 01:15:00", "2019-12-31 02:15:00",
          "2019-12-30 03:30:00", "2009-12-31 04:30:00", "2010-01-01 05:30:00",
          "2010-01-03 06:45:00", "2010-01-04 07:45:00", "2006-01-01 08:45:00",
          "2005-12-31 10:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:15:00", null])";
  const char* ceil_15_hour =
      R"(["1970-01-01 15:00:00", "2000-03-01 12:00:00", "1899-01-01 03:00:00",
          "2033-05-18 18:00:00", "2020-01-01 12:00:00", "2019-12-31 06:00:00",
          "2019-12-30 15:00:00", "2009-12-31 09:00:00", "2010-01-01 15:00:00",
          "2010-01-03 12:00:00", "2010-01-04 18:00:00", "2006-01-01 09:00:00",
          "2005-12-31 18:00:00", "2008-12-28 06:00:00", "2008-12-29 12:00:00",
          "2012-01-01 15:00:00", null])";
  const char* ceil_15_day =
      R"(["1970-01-16", "2000-03-09", "1899-01-13", "2033-05-30", "2020-01-09",
          "2020-01-09", "2020-01-09", "2010-01-01", "2010-01-16", "2010-01-16",
          "2010-01-16", "2006-01-07", "2006-01-07", "2009-01-06", "2009-01-06",
          "2012-01-06", null])";
  const char* ceil_15_weeks =
      R"(["1970-04-13", "2000-03-06", "1899-04-10", "2033-07-11", "2020-01-06",
          "2020-01-06", "2020-01-06", "2010-03-29", "2010-03-29", "2010-03-29",
          "2010-03-29", "2006-03-20", "2006-03-20", "2009-02-02", "2009-02-02",
          "2012-04-02", null])";
  const char* ceil_15_weeks_sunday =
      R"(["1970-04-12", "2000-03-05", "1899-04-09", "2033-07-10", "2020-01-05",
          "2020-01-05", "2020-01-05", "2010-03-28", "2010-03-28", "2010-03-28",
          "2010-03-28", "2006-03-19", "2006-03-19", "2009-02-01", "2009-02-01",
          "2012-04-01", null])";
  const char* ceil_15_months =
      R"(["1971-04-01", "2001-04-01", "1900-01-01", "2033-10-01", "2021-04-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2011-04-01", "2011-04-01",
          "2011-04-01", "2006-04-01", "2006-04-01", "2010-01-01", "2010-01-01",
          "2012-07-01", null])";
  const char* ceil_15_quarters =
      R"(["1973-10-01", "2003-10-01", "1902-07-01", "2033-10-01", "2022-07-01",
          "2022-07-01", "2022-07-01", "2011-04-01", "2011-04-01", "2011-04-01",
          "2011-04-01", "2007-07-01", "2007-07-01", "2011-04-01", "2011-04-01",
          "2015-01-01", null])";
  const char* ceil_15_years =
      R"(["1980-01-01", "2010-01-01", "1905-01-01", "2040-01-01", "2025-01-01",
          "2025-01-01", "2025-01-01", "2010-01-01", "2025-01-01", "2025-01-01",
          "2025-01-01", "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2025-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, ceil_1_nanosecond, &round_to_1_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_microsecond, &round_to_1_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_millisecond, &round_to_1_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_second, &round_to_1_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_minute, &round_to_1_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_1_hour, &round_to_1_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_1_day, &round_to_1_days);
  CheckScalarUnary(op, unit, times, unit, ceil_1_weeks, &round_to_1_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_1_weeks_sunday, &round_to_1_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_1_months, &round_to_1_months);
  CheckScalarUnary(op, unit, times, unit, ceil_1_quarters, &round_to_1_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_1_years, &round_to_1_years);

  CheckScalarUnary(op, unit, times, unit, ceil_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_microsecond, &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_millisecond, &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_second, &round_to_15_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_minute, &round_to_15_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, ceil_15_weeks, &round_to_15_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_15_weeks_sunday,
                   &round_to_15_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, ceil_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestStrictCeil, TestCeilTemporalStrictCeil) {
  std::string op = "ceil_temporal";

  const char* ceil_1_nanosecond =
      R"(["1970-01-01 00:00:59.123456790", "2000-02-29 23:23:24.000000000",
          "1899-01-01 00:59:20.001001002", "2033-05-18 03:33:20.000000001",
          "2020-01-01 01:05:05.001000001", "2019-12-31 02:10:10.002000001",
          "2019-12-30 03:15:15.003000001", "2009-12-31 04:20:20.004132001",
          "2010-01-01 05:25:25.005321001", "2010-01-03 06:30:30.006163001",
          "2010-01-04 07:35:35.000000001", "2006-01-01 08:40:40.000000001",
          "2005-12-31 09:45:45.000000001", "2008-12-28 00:00:00.000000001",
          "2008-12-29 00:00:00.000000001", "2012-01-01 01:02:03.000000001", null])";
  const char* ceil_1_microsecond =
      R"(["1970-01-01 00:00:59.123457", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001002", "2033-05-18 03:33:20.000001",
          "2020-01-01 01:05:05.001001", "2019-12-31 02:10:10.002001",
          "2019-12-30 03:15:15.003001", "2009-12-31 04:20:20.004133",
          "2010-01-01 05:25:25.005322", "2010-01-03 06:30:30.006164",
          "2010-01-04 07:35:35.000001", "2006-01-01 08:40:40.000001",
          "2005-12-31 09:45:45.000001", "2008-12-28 00:00:00.000001",
          "2008-12-29 00:00:00.000001", "2012-01-01 01:02:03.000001", null])";
  const char* ceil_1_millisecond =
      R"(["1970-01-01 00:00:59.124", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:20.002", "2033-05-18 03:33:20.001",
          "2020-01-01 01:05:05.002", "2019-12-31 02:10:10.003",
          "2019-12-30 03:15:15.004", "2009-12-31 04:20:20.005",
          "2010-01-01 05:25:25.006", "2010-01-03 06:30:30.007",
          "2010-01-04 07:35:35.001", "2006-01-01 08:40:40.001",
          "2005-12-31 09:45:45.001", "2008-12-28 00:00:00.001",
          "2008-12-29 00:00:00.001", "2012-01-01 01:02:03.001", null])";
  const char* ceil_1_second =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:23:24", "1899-01-01 00:59:21",
          "2033-05-18 03:33:21", "2020-01-01 01:05:06", "2019-12-31 02:10:11",
          "2019-12-30 03:15:16", "2009-12-31 04:20:21", "2010-01-01 05:25:26",
          "2010-01-03 06:30:31", "2010-01-04 07:35:36", "2006-01-01 08:40:41",
          "2005-12-31 09:45:46", "2008-12-28 00:00:01", "2008-12-29 00:00:01",
          "2012-01-01 01:02:04", null])";
  const char* ceil_1_minute =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:24:00", "1899-01-01 01:00:00",
             "2033-05-18 03:34:00", "2020-01-01 01:06:00", "2019-12-31 02:11:00",
             "2019-12-30 03:16:00", "2009-12-31 04:21:00", "2010-01-01 05:26:00",
             "2010-01-03 06:31:00", "2010-01-04 07:36:00", "2006-01-01 08:41:00",
             "2005-12-31 09:46:00", "2008-12-28 00:01:00", "2008-12-29 00:01:00",
             "2012-01-01 01:03:00", null])";
  const char* ceil_1_hour =
      R"(["1970-01-01 01:00:00", "2000-03-01 00:00:00", "1899-01-01 01:00:00",
          "2033-05-18 04:00:00", "2020-01-01 02:00:00", "2019-12-31 03:00:00",
          "2019-12-30 04:00:00", "2009-12-31 05:00:00", "2010-01-01 06:00:00",
          "2010-01-03 07:00:00", "2010-01-04 08:00:00", "2006-01-01 09:00:00",
          "2005-12-31 10:00:00", "2008-12-28 01:00:00", "2008-12-29 01:00:00",
          "2012-01-01 02:00:00", null])";
  const char* ceil_1_day =
      R"(["1970-01-02", "2000-03-01", "1899-01-02", "2033-05-19",
          "2020-01-02", "2020-01-01", "2019-12-31", "2010-01-01",
          "2010-01-02", "2010-01-04", "2010-01-05", "2006-01-02",
          "2006-01-01", "2008-12-29", "2008-12-30", "2012-01-02", null])";
  const char* ceil_1_weeks =
      R"(["1970-01-05", "2000-03-06", "1899-01-02", "2033-05-23",
          "2020-01-06", "2020-01-06", "2020-01-06", "2010-01-04",
          "2010-01-04", "2010-01-04", "2010-01-11", "2006-01-02",
          "2006-01-02", "2008-12-29", "2009-01-05", "2012-01-02",  null])";
  const char* ceil_1_weeks_sunday =
      R"(["1970-01-04", "2000-03-05", "1899-01-08", "2033-05-22",
          "2020-01-05", "2020-01-05", "2020-01-05", "2010-01-03",
          "2010-01-03", "2010-01-10", "2010-01-10", "2006-01-08",
          "2006-01-01", "2009-01-04", "2009-01-04", "2012-01-08",  null])";
  const char* ceil_1_months =
      R"(["1970-02-01", "2000-03-01", "1899-02-01", "2033-06-01",
          "2020-02-01", "2020-01-01", "2020-01-01", "2010-01-01",
          "2010-02-01", "2010-02-01", "2010-02-01", "2006-02-01",
          "2006-01-01", "2009-01-01", "2009-01-01", "2012-02-01", null])";
  const char* ceil_1_quarters =
      R"(["1970-04-01", "2000-04-01", "1899-04-01", "2033-07-01",
          "2020-04-01", "2020-01-01", "2020-01-01", "2010-01-01",
          "2010-04-01", "2010-04-01", "2010-04-01", "2006-04-01",
          "2006-01-01", "2009-01-01", "2009-01-01", "2012-04-01", null])";
  const char* ceil_1_years =
      R"(["1971-01-01", "2001-01-01", "1900-01-01", "2034-01-01",
          "2021-01-01", "2020-01-01", "2020-01-01", "2010-01-01",
          "2011-01-01", "2011-01-01", "2011-01-01", "2007-01-01",
          "2006-01-01", "2009-01-01", "2009-01-01", "2013-01-01", null])";

  const char* ceil_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456790", "2000-02-29 23:23:24.000000000",
          "1899-01-01 00:59:20.001001005", "2033-05-18 03:33:20.000000010",
          "2020-01-01 01:05:05.001000015", "2019-12-31 02:10:10.002000015",
          "2019-12-30 03:15:15.003000015", "2009-12-31 04:20:20.004132015",
          "2010-01-01 05:25:25.005321015", "2010-01-03 06:30:30.006163005",
          "2010-01-04 07:35:35.000000010", "2006-01-01 08:40:40.000000005",
          "2005-12-31 09:45:45.000000015", "2008-12-28 00:00:00.000000015",
          "2008-12-29 00:00:00.000000015", "2012-01-01 01:02:03.000000015", null])";
  const char* ceil_15_microsecond =
      R"(["1970-01-01 00:00:59.123460", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001015", "2033-05-18 03:33:20.000010",
          "2020-01-01 01:05:05.001015", "2019-12-31 02:10:10.002015",
          "2019-12-30 03:15:15.003015", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005330", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000010", "2006-01-01 08:40:40.000005",
          "2005-12-31 09:45:45.000015", "2008-12-28 00:00:00.000015",
          "2008-12-29 00:00:00.000015", "2012-01-01 01:02:03.000015", null])";
  const char* ceil_15_millisecond =
      R"(["1970-01-01 00:00:59.130", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:20.010", "2033-05-18 03:33:20.010",
          "2020-01-01 01:05:05.010", "2019-12-31 02:10:10.005",
          "2019-12-30 03:15:15.015", "2009-12-31 04:20:20.010",
          "2010-01-01 05:25:25.020", "2010-01-03 06:30:30.015",
          "2010-01-04 07:35:35.010", "2006-01-01 08:40:40.005",
          "2005-12-31 09:45:45.015", "2008-12-28 00:00:00.015",
          "2008-12-29 00:00:00.015", "2012-01-01 01:02:03.015", null])";
  const char* ceil_13_second =
      R"(["1970-01-01 00:01:05", "2000-02-29 23:23:24", "1899-01-01 00:59:29",
          "2033-05-18 03:33:22", "2020-01-01 01:05:06", "2019-12-31 02:10:21",
          "2019-12-30 03:15:23", "2009-12-31 04:20:32", "2010-01-01 05:25:30",
          "2010-01-03 06:30:39", "2010-01-04 07:35:37", "2006-01-01 08:40:53",
          "2005-12-31 09:45:55", "2008-12-28 00:00:01", "2008-12-29 00:00:12",
          "2012-01-01 01:02:11", null])";
  const char* ceil_13_minute =
      R"(["1970-01-01 00:13:00", "2000-02-29 23:26:00", "1899-01-01 01:01:00",
          "2033-05-18 03:39:00", "2020-01-01 01:09:00", "2019-12-31 02:11:00",
          "2019-12-30 03:26:00", "2009-12-31 04:24:00", "2010-01-01 05:32:00",
          "2010-01-03 06:43:00", "2010-01-04 07:38:00", "2006-01-01 08:45:00",
          "2005-12-31 09:47:00", "2008-12-28 00:05:00", "2008-12-29 00:08:00",
          "2012-01-01 01:05:00", null])";
  const char* ceil_15_hour =
      R"(["1970-01-01 15:00:00", "2000-03-01 12:00:00", "1899-01-01 03:00:00",
          "2033-05-18 18:00:00", "2020-01-01 12:00:00", "2019-12-31 06:00:00",
          "2019-12-30 15:00:00", "2009-12-31 09:00:00", "2010-01-01 15:00:00",
          "2010-01-03 12:00:00", "2010-01-04 18:00:00", "2006-01-01 09:00:00",
          "2005-12-31 18:00:00", "2008-12-28 06:00:00", "2008-12-29 12:00:00",
          "2012-01-01 15:00:00", null])";
  const char* ceil_15_day =
      R"(["1970-01-16", "2000-03-09", "1899-01-13", "2033-05-30", "2020-01-09",
          "2020-01-09", "2020-01-09", "2010-01-01", "2010-01-16", "2010-01-16",
          "2010-01-16", "2006-01-07", "2006-01-07", "2009-01-06", "2009-01-06",
          "2012-01-06", null])";
  const char* ceil_3_weeks =
      R"(["1970-01-19", "2000-03-06", "1899-01-16", "2033-05-30", "2020-01-06",
          "2020-01-06", "2020-01-06", "2010-01-04", "2010-01-04", "2010-01-04",
          "2010-01-25", "2006-01-16", "2006-01-16", "2009-01-12", "2009-01-12",
          "2012-01-09", null])";
  const char* ceil_3_weeks_sunday =
      R"(["1970-01-18", "2000-03-05", "1899-01-15", "2033-05-29", "2020-01-05",
          "2020-01-05", "2020-01-05", "2010-01-03", "2010-01-03", "2010-01-24",
          "2010-01-24", "2006-01-15", "2006-01-15", "2009-01-11", "2009-01-11",
          "2012-01-08", null])";
  const char* ceil_15_months =
      R"(["1971-04-01", "2001-04-01", "1900-01-01", "2033-10-01", "2021-04-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2011-04-01", "2011-04-01",
          "2011-04-01", "2006-04-01", "2006-04-01", "2010-01-01", "2010-01-01",
          "2012-07-01", null])";
  const char* ceil_15_quarters =
      R"(["1973-10-01", "2003-10-01", "1902-07-01", "2033-10-01", "2022-07-01",
          "2022-07-01", "2022-07-01", "2011-04-01", "2011-04-01", "2011-04-01",
          "2011-04-01", "2007-07-01", "2007-07-01", "2011-04-01", "2011-04-01",
          "2015-01-01", null])";
  const char* ceil_15_years =
      R"(["1980-01-01", "2010-01-01", "1905-01-01", "2040-01-01", "2025-01-01",
          "2025-01-01", "2025-01-01", "2010-01-01", "2025-01-01", "2025-01-01",
          "2025-01-01", "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2025-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, ceil_1_nanosecond, &round_to_1_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_microsecond, &round_to_1_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_millisecond, &round_to_1_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_second, &round_to_1_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_1_minute, &round_to_1_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_1_hour, &round_to_1_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_1_day, &round_to_1_days);
  CheckScalarUnary(op, unit, times, unit, ceil_1_weeks, &round_to_1_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_1_weeks_sunday, &round_to_1_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_1_months, &round_to_1_months);
  CheckScalarUnary(op, unit, times, unit, ceil_1_quarters, &round_to_1_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_1_years, &round_to_1_years);

  CheckScalarUnary(op, unit, times, unit, ceil_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_microsecond, &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_millisecond, &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, ceil_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, CeilUTC) {
  std::string op = "ceil_temporal";

  // Data for tests below was generaed via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* ceil_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456795", "2000-02-29 23:23:24.000000005",
          "1899-01-01 00:59:20.001001015", "2033-05-18 03:33:20.000000015",
          "2020-01-01 01:05:05.001000015", "2019-12-31 02:10:10.002000015",
          "2019-12-30 03:15:15.003000015", "2009-12-31 04:20:20.004132015",
          "2010-01-01 05:25:25.005321015", "2010-01-03 06:30:30.006163015",
          "2010-01-04 07:35:35.000000015", "2006-01-01 08:40:40.000000015",
          "2005-12-31 09:45:45.000000015", "2008-12-28 00:00:00.000000015",
          "2008-12-29 00:00:00.000000015", "2012-01-01 01:02:03.000000015", null])";
  const char* ceil_15_microsecond =
      R"(["1970-01-01 00:00:59.123465", "2000-02-29 23:23:24.000005",
          "1899-01-01 00:59:20.001015", "2033-05-18 03:33:20.000015",
          "2020-01-01 01:05:05.001015", "2019-12-31 02:10:10.002015",
          "2019-12-30 03:15:15.003015", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005330", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000015", "2006-01-01 08:40:40.000015",
          "2005-12-31 09:45:45.000015", "2008-12-28 00:00:00.000015",
          "2008-12-29 00:00:00.000015", "2012-01-01 01:02:03.000015", null])";
  const char* ceil_15_millisecond =
      R"(["1970-01-01 00:00:59.135", "2000-02-29 23:23:24.005",
          "1899-01-01 00:59:20.015", "2033-05-18 03:33:20.015",
          "2020-01-01 01:05:05.015", "2019-12-31 02:10:10.015",
          "2019-12-30 03:15:15.015", "2009-12-31 04:20:20.015",
          "2010-01-01 05:25:25.015", "2010-01-03 06:30:30.015",
          "2010-01-04 07:35:35.015", "2006-01-01 08:40:40.015",
          "2005-12-31 09:45:45.015", "2008-12-28 00:00:00.015",
          "2008-12-29 00:00:00.015", "2012-01-01 01:02:03.015", null])";
  const char* ceil_13_second =
      R"(["1970-01-01 00:01:05", "2000-02-29 23:23:26", "1899-01-01 00:59:26",
          "2033-05-18 03:33:26", "2020-01-01 01:05:13", "2019-12-31 02:10:13",
          "2019-12-30 03:15:26", "2009-12-31 04:20:26", "2010-01-01 05:25:26",
          "2010-01-03 06:30:39", "2010-01-04 07:35:39", "2006-01-01 08:40:52",
          "2005-12-31 09:45:52", "2008-12-28 00:00:13", "2008-12-29 00:00:13",
          "2012-01-01 01:02:13", null])";
  const char* ceil_13_minute =
      R"(["1970-01-01 00:13:00", "2000-02-29 23:26:00", "1899-01-01 01:05:00",
          "2033-05-18 03:39:00", "2020-01-01 01:13:00", "2019-12-31 02:13:00",
          "2019-12-30 03:26:00", "2009-12-31 04:26:00", "2010-01-01 05:26:00",
          "2010-01-03 06:39:00", "2010-01-04 07:39:00", "2006-01-01 08:52:00",
          "2005-12-31 09:52:00", "2008-12-28 00:13:00", "2008-12-29 00:13:00",
          "2012-01-01 01:13:00", null])";
  const char* ceil_15_hour =
      R"(["1970-01-01 15:00:00", "2000-03-01 06:00:00", "1899-01-01 15:00:00",
          "2033-05-18 15:00:00", "2020-01-01 15:00:00", "2019-12-31 15:00:00",
          "2019-12-30 15:00:00", "2009-12-31 15:00:00", "2010-01-01 15:00:00",
          "2010-01-03 15:00:00", "2010-01-04 15:00:00", "2006-01-01 15:00:00",
          "2005-12-31 15:00:00", "2008-12-28 15:00:00", "2008-12-29 15:00:00",
          "2012-01-01 15:00:00", null])";
  const char* ceil_15_day =
      R"(["1970-01-16", "2000-03-02", "1899-01-16", "2033-05-31",
          "2020-01-16", "2020-01-15", "2019-12-31", "2010-01-15",
          "2010-01-16", "2010-01-16", "2010-01-16", "2006-01-16",
          "2006-01-15", "2008-12-31", "2008-12-31", "2012-01-16", null])";
  const char* ceil_3_weeks =
      R"(["1970-01-19", "2000-03-06", "1899-01-23", "2033-05-30", "2020-01-20",
          "2020-01-20", "2020-01-20", "2010-01-25", "2010-01-25", "2010-01-25",
          "2010-01-25", "2006-01-23", "2006-01-23", "2009-01-12", "2009-01-19",
          "2012-01-23", null])";
  const char* ceil_3_weeks_sunday =
      R"(["1970-01-25", "2000-03-05", "1899-01-22", "2033-05-29", "2020-01-19",
          "2020-01-19", "2020-01-19", "2010-01-24", "2010-01-24", "2010-01-24",
          "2010-01-24", "2006-01-22", "2006-01-22", "2009-01-25", "2009-01-25",
          "2012-01-22", null])";
  const char* ceil_15_months =
      R"(["1971-04-01", "2001-04-01", "1900-04-01", "2034-04-01",
          "2021-04-01", "2020-04-01", "2020-04-01", "2010-04-01",
          "2011-04-01", "2011-04-01", "2011-04-01", "2007-04-01",
          "2006-04-01", "2009-04-01", "2009-04-01", "2013-04-01", null])";
  const char* ceil_15_quarters =
      R"(["1973-10-01", "2003-10-01", "1902-10-01", "2036-10-01",
          "2023-10-01", "2022-10-01", "2022-10-01", "2012-10-01",
          "2013-10-01", "2013-10-01", "2013-10-01", "2009-10-01",
          "2008-10-01", "2011-10-01", "2011-10-01", "2015-10-01", null])";
  const char* ceil_15_years =
      R"(["1980-01-01", "2010-01-01", "1905-01-01", "2040-01-01",
          "2025-01-01", "2025-01-01", "2025-01-01", "2010-01-01",
          "2025-01-01", "2025-01-01", "2025-01-01", "2010-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2025-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, ceil_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_microsecond, &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_millisecond, &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, ceil_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, CeilZoned) {
  std::string op = "ceil_temporal";

  // Data for tests below was generated via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* ceil_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456795", "2000-02-29 23:23:24.000000005",
          "1899-01-01 00:59:20.001001015", "2033-05-18 03:33:20.000000015",
          "2020-01-01 01:05:05.001000015", "2019-12-31 02:10:10.002000015",
          "2019-12-30 03:15:15.003000015", "2009-12-31 04:20:20.004132015",
          "2010-01-01 05:25:25.005321015", "2010-01-03 06:30:30.006163015",
          "2010-01-04 07:35:35.000000015", "2006-01-01 08:40:40.000000015",
          "2005-12-31 09:45:45.000000015", "2008-12-28 00:00:00.000000015",
          "2008-12-29 00:00:00.000000015", "2012-01-01 01:02:03.000000015", null])";
  const char* ceil_15_microsecond =
      R"(["1970-01-01 00:00:59.123465", "2000-02-29 23:23:24.000005",
          "1899-01-01 00:59:20.001015", "2033-05-18 03:33:20.000015",
          "2020-01-01 01:05:05.001015", "2019-12-31 02:10:10.002015",
          "2019-12-30 03:15:15.003015", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005330", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000015", "2006-01-01 08:40:40.000015",
          "2005-12-31 09:45:45.000015", "2008-12-28 00:00:00.000015",
          "2008-12-29 00:00:00.000015", "2012-01-01 01:02:03.000015", null])";
  const char* ceil_15_millisecond =
      R"(["1970-01-01 00:00:59.135", "2000-02-29 23:23:24.005",
          "1899-01-01 00:59:20.015", "2033-05-18 03:33:20.015",
          "2020-01-01 01:05:05.015", "2019-12-31 02:10:10.015",
          "2019-12-30 03:15:15.015", "2009-12-31 04:20:20.015",
          "2010-01-01 05:25:25.015", "2010-01-03 06:30:30.015",
          "2010-01-04 07:35:35.015", "2006-01-01 08:40:40.015",
          "2005-12-31 09:45:45.015", "2008-12-28 00:00:00.015",
          "2008-12-29 00:00:00.015", "2012-01-01 01:02:03.015", null])";
  const char* ceil_13_second = R"([
    "1970-01-01 00:01:05", "2000-02-29 23:23:26", "1899-01-01 00:59:26", "2033-05-18 03:33:26",
    "2020-01-01 01:05:13", "2019-12-31 02:10:13", "2019-12-30 03:15:26", "2009-12-31 04:20:26",
    "2010-01-01 05:25:26", "2010-01-03 06:30:39", "2010-01-04 07:35:39", "2006-01-01 08:40:52",
    "2005-12-31 09:45:52", "2008-12-28 00:00:13", "2008-12-29 00:00:13", "2012-01-01 01:02:13", null])";
  const char* ceil_13_minute = R"([
    "1970-01-01 00:09:00", "2000-02-29 23:35:00", "1899-01-01 01:05:00", "2033-05-18 03:43:00",
    "2020-01-01 01:09:00", "2019-12-31 02:22:00", "2019-12-30 03:22:00", "2009-12-31 04:22:00",
    "2010-01-01 05:35:00", "2010-01-03 06:43:00", "2010-01-04 07:43:00", "2006-01-01 08:43:00",
    "2005-12-31 09:56:00", "2008-12-28 00:09:00", "2008-12-29 00:09:00", "2012-01-01 01:09:00", null])";
  const char* ceil_15_hour = R"([
    "1970-01-01 05:30:00", "2000-03-01 04:30:00", "1899-01-01 06:00:00", "2033-05-18 05:30:00",
    "2020-01-01 04:30:00", "2019-12-31 04:30:00", "2019-12-30 04:30:00", "2009-12-31 04:30:00",
    "2010-01-01 19:30:00", "2010-01-03 19:30:00", "2010-01-04 19:30:00", "2006-01-01 19:30:00",
    "2005-12-31 19:30:00", "2008-12-28 04:30:00", "2008-12-29 04:30:00", "2012-01-01 04:30:00", null])";
  const char* ceil_15_day = R"([
    "1970-01-15 14:30:00", "2000-03-15 13:30:00", "1899-01-15 15:00:00", "2033-05-30 14:30:00",
    "2020-01-15 13:30:00", "2020-01-14 13:30:00", "2019-12-30 13:30:00", "2010-01-14 13:30:00",
    "2010-01-15 13:30:00", "2010-01-15 13:30:00", "2010-01-15 13:30:00", "2006-01-15 13:30:00",
    "2006-01-14 13:30:00", "2008-12-30 13:30:00", "2008-12-30 13:30:00", "2012-01-15 13:30:00", null])";
  const char* ceil_3_weeks = R"([
    "1970-01-18 14:30:00", "2000-03-05 13:30:00", "1899-01-22 15:00:00", "2033-05-29 14:30:00",
    "2020-01-19 13:30:00", "2020-01-19 13:30:00", "2020-01-19 13:30:00", "2010-01-24 13:30:00",
    "2010-01-24 13:30:00", "2010-01-24 13:30:00", "2010-01-24 13:30:00", "2006-01-22 13:30:00",
    "2006-01-22 13:30:00", "2009-01-11 13:30:00", "2009-01-18 13:30:00", "2012-01-22 13:30:00", null])";
  const char* ceil_3_weeks_sunday = R"([
    "1970-01-24 14:30:00", "2000-03-25 13:30:00", "1899-01-21 15:00:00", "2033-05-28 14:30:00",
    "2020-01-18 13:30:00", "2020-01-18 13:30:00", "2020-01-18 13:30:00", "2010-01-23 13:30:00",
    "2010-01-23 13:30:00", "2010-01-23 13:30:00", "2010-01-23 13:30:00", "2006-01-21 13:30:00",
    "2006-01-21 13:30:00", "2009-01-24 13:30:00", "2009-01-24 13:30:00", "2012-01-21 13:30:00", null])";
  const char* ceil_5_months = R"([
    "1970-05-31 14:30:00", "2000-05-31 14:30:00", "1899-05-31 14:30:00", "2033-05-31 14:30:00",
    "2020-05-31 14:30:00", "2020-03-31 13:30:00", "2020-03-31 13:30:00", "2010-03-31 13:30:00",
    "2010-05-31 14:30:00", "2010-05-31 14:30:00", "2010-05-31 14:30:00", "2006-05-31 14:30:00",
    "2006-03-31 13:30:00", "2009-03-31 13:30:00", "2009-03-31 13:30:00", "2012-05-31 14:30:00", null])";
  const char* ceil_3_quarters = R"([
    "1970-09-30 14:30:00", "2000-09-30 14:30:00", "1899-09-30 14:30:00", "2033-09-30 14:30:00",
    "2020-09-30 14:30:00", "2020-06-30 14:30:00", "2020-06-30 14:30:00", "2010-06-30 14:30:00",
    "2010-09-30 14:30:00", "2010-09-30 14:30:00", "2010-09-30 14:30:00", "2006-09-30 14:30:00",
    "2006-06-30 14:30:00", "2009-06-30 14:30:00", "2009-06-30 14:30:00", "2012-09-30 14:30:00", null])";
  const char* ceil_15_years = R"([
    "1979-12-31 13:30:00", "2009-12-31 13:30:00", "1904-12-31 14:30:00", "2039-12-31 13:30:00",
    "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2009-12-31 13:30:00",
    "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2009-12-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2024-12-31 13:30:00", null])";

  // Australia/Broken_Hill timezone is defined as UTC+9:30 and UTC+10:30 during DST.
  // DST runs from first Sunday in October to first Sunday in April.
  auto unit = timestamp(TimeUnit::NANO, "Australia/Broken_Hill");
  CheckScalarUnary(op, unit, times, unit, ceil_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_microsecond, &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_15_millisecond, &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, ceil_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, ceil_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, ceil_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, ceil_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, ceil_5_months, &round_to_5_months);
  CheckScalarUnary(op, unit, times, unit, ceil_3_quarters, &round_to_3_quarters);
  CheckScalarUnary(op, unit, times, unit, ceil_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTest, TestFloorTemporal) {
  std::string op = "floor_temporal";

  const char* floor_1_nanosecond =
      R"(["1970-01-01 00:00:59.123456789", "2000-02-29 23:23:23.999999999",
          "1899-01-01 00:59:20.001001001", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* floor_1_microsecond =
      R"(["1970-01-01 00:00:59.123456", "2000-02-29 23:23:23.999999",
          "1899-01-01 00:59:20.001001", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004132",
          "2010-01-01 05:25:25.005321", "2010-01-03 06:30:30.006163",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* floor_1_millisecond =
      R"(["1970-01-01 00:00:59.123", "2000-02-29 23:23:23.999",
          "1899-01-01 00:59:20.001", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.001", "2019-12-31 02:10:10.002",
          "2019-12-30 03:15:15.003", "2009-12-31 04:20:20.004",
          "2010-01-01 05:25:25.005", "2010-01-03 06:30:30.006",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* floor_1_second =
      R"(["1970-01-01 00:00:59", "2000-02-29 23:23:23", "1899-01-01 00:59:20",
          "2033-05-18 03:33:20", "2020-01-01 01:05:05", "2019-12-31 02:10:10",
          "2019-12-30 03:15:15", "2009-12-31 04:20:20", "2010-01-01 05:25:25",
          "2010-01-03 06:30:30", "2010-01-04 07:35:35", "2006-01-01 08:40:40",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:03", null])";
  const char* floor_1_minute =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:23:00", "1899-01-01 00:59:00",
          "2033-05-18 03:33:00", "2020-01-01 01:05:00", "2019-12-31 02:10:00",
          "2019-12-30 03:15:00", "2009-12-31 04:20:00", "2010-01-01 05:25:00",
          "2010-01-03 06:30:00", "2010-01-04 07:35:00", "2006-01-01 08:40:00",
          "2005-12-31 09:45:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* floor_1_hour =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:00:00", "1899-01-01 00:00:00",
          "2033-05-18 03:00:00", "2020-01-01 01:00:00", "2019-12-31 02:00:00",
          "2019-12-30 03:00:00", "2009-12-31 04:00:00", "2010-01-01 05:00:00",
          "2010-01-03 06:00:00", "2010-01-04 07:00:00", "2006-01-01 08:00:00",
          "2005-12-31 09:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* floor_1_day =
      R"(["1970-01-01", "2000-02-29", "1899-01-01", "2033-05-18", "2020-01-01",
          "2019-12-31", "2019-12-30", "2009-12-31", "2010-01-01", "2010-01-03",
          "2010-01-04", "2006-01-01", "2005-12-31", "2008-12-28", "2008-12-29",
          "2012-01-01", null])";
  const char* floor_1_weeks =
      R"(["1969-12-29", "2000-02-28", "1898-12-26", "2033-05-16", "2019-12-30",
            "2019-12-30", "2019-12-30", "2009-12-28", "2009-12-28", "2009-12-28",
            "2010-01-04", "2005-12-26", "2005-12-26", "2008-12-22", "2008-12-29",
            "2011-12-26", null])";
  const char* floor_1_weeks_sunday =
      R"(["1969-12-28", "2000-02-27", "1899-01-01", "2033-05-15", "2019-12-29",
            "2019-12-29", "2019-12-29", "2009-12-27", "2009-12-27", "2010-01-03",
            "2010-01-03", "2006-01-01", "2005-12-25", "2008-12-28", "2008-12-28",
            "2012-01-01", null])";
  const char* floor_1_months =
      R"(["1970-01-01", "2000-02-01", "1899-01-01", "2033-05-01", "2020-01-01",
          "2019-12-01", "2019-12-01", "2009-12-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2005-12-01", "2008-12-01", "2008-12-01",
          "2012-01-01", null])";
  const char* floor_1_quarters =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-04-01", "2020-01-01",
          "2019-10-01", "2019-10-01", "2009-10-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2005-10-01", "2008-10-01", "2008-10-01",
          "2012-01-01", null])";
  const char* floor_1_years =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-01-01", "2020-01-01",
          "2019-01-01", "2019-01-01", "2009-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2005-01-01", "2008-01-01", "2008-01-01",
          "2012-01-01", null])";

  const char* floor_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456775", "2000-02-29 23:23:23.999999985",
          "1899-01-01 00:59:20.001000990", "2033-05-18 03:33:19.999999995",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006162990",
          "2010-01-04 07:35:34.999999995", "2006-01-01 08:40:39.999999990",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* floor_15_microsecond =
      R"(["1970-01-01 00:00:59.123445", "2000-02-29 23:23:23.999985",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:19.999995",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004120",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006150",
          "2010-01-04 07:35:34.999995", "2006-01-01 08:40:39.999990",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* floor_15_millisecond =
      R"(["1970-01-01 00:00:59.115", "2000-02-29 23:23:23.985",
          "1899-01-01 00:59:19.995", "2033-05-18 03:33:19.995",
          "2020-01-01 01:05:04.995", "2019-12-31 02:10:09.990",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:19.995",
          "2010-01-01 05:25:25.005", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:34.995", "2006-01-01 08:40:39.990",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* floor_15_second =
      R"(["1970-01-01 00:00:45", "2000-02-29 23:23:15", "1899-01-01 00:59:15",
          "2033-05-18 03:33:15", "2020-01-01 01:05:00", "2019-12-31 02:10:00",
          "2019-12-30 03:15:15", "2009-12-31 04:20:15", "2010-01-01 05:25:15",
          "2010-01-03 06:30:30", "2010-01-04 07:35:30", "2006-01-01 08:40:30",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* floor_15_minute =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:15:00", "1899-01-01 00:45:00",
          "2033-05-18 03:30:00", "2020-01-01 01:00:00", "2019-12-31 02:00:00",
          "2019-12-30 03:15:00", "2009-12-31 04:15:00", "2010-01-01 05:15:00",
          "2010-01-03 06:30:00", "2010-01-04 07:30:00", "2006-01-01 08:30:00",
          "2005-12-31 09:45:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* floor_15_hour =
      R"(["1970-01-01 00:00:00", "2000-02-29 21:00:00", "1898-12-31 12:00:00",
          "2033-05-18 03:00:00", "2019-12-31 21:00:00", "2019-12-30 15:00:00",
          "2019-12-30 00:00:00", "2009-12-30 18:00:00", "2010-01-01 00:00:00",
          "2010-01-02 21:00:00", "2010-01-04 03:00:00", "2005-12-31 18:00:00",
          "2005-12-31 03:00:00", "2008-12-27 15:00:00", "2008-12-28 21:00:00",
          "2012-01-01 00:00:00", null])";
  const char* floor_15_day =
      R"(["1970-01-01", "2000-02-23", "1898-12-29", "2033-05-15", "2019-12-25",
          "2019-12-25", "2019-12-25", "2009-12-17", "2010-01-01", "2010-01-01",
          "2010-01-01", "2005-12-23", "2005-12-23", "2008-12-22", "2008-12-22",
          "2011-12-22", null])";
  const char* floor_15_weeks =
      R"(["1969-12-29", "1999-11-22", "1898-12-26", "2033-03-28", "2019-09-23",
          "2019-09-23", "2019-09-23", "2009-12-14", "2009-12-14", "2009-12-14",
          "2009-12-14", "2005-12-05", "2005-12-05", "2008-10-20", "2008-10-20",
          "2011-12-19", null])";
  const char* floor_15_weeks_sunday =
      R"(["1969-12-28", "1999-11-21", "1898-12-25", "2033-03-27", "2019-09-22",
          "2019-09-22", "2019-09-22", "2009-12-13", "2009-12-13", "2009-12-13",
          "2009-12-13", "2005-12-04", "2005-12-04", "2008-10-19", "2008-10-19",
          "2011-12-18", null])";
  const char* floor_15_months =
      R"(["1970-01-01", "2000-01-01", "1898-10-01", "2032-07-01", "2020-01-01",
          "2018-10-01", "2018-10-01", "2008-10-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2005-01-01", "2005-01-01", "2008-10-01", "2008-10-01",
          "2011-04-01", null])";
  const char* floor_15_quarters =
      R"(["1970-01-01", "2000-01-01", "1898-10-01", "2030-01-01", "2018-10-01",
          "2018-10-01", "2018-10-01", "2007-07-01", "2007-07-01", "2007-07-01",
          "2007-07-01", "2003-10-01", "2003-10-01", "2007-07-01", "2007-07-01",
          "2011-04-01", null])";
  const char* floor_15_years =
      R"(["1965-01-01", "1995-01-01", "1890-01-01", "2025-01-01", "2010-01-01",
          "2010-01-01", "2010-01-01", "1995-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "1995-01-01", "1995-01-01", "1995-01-01", "1995-01-01",
          "2010-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, floor_1_nanosecond, &round_to_1_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, floor_1_microsecond, &round_to_1_microseconds);
  CheckScalarUnary(op, unit, times, unit, floor_1_millisecond, &round_to_1_milliseconds);
  CheckScalarUnary(op, unit, times, unit, floor_1_second, &round_to_1_seconds);
  CheckScalarUnary(op, unit, times, unit, floor_1_minute, &round_to_1_minutes);
  CheckScalarUnary(op, unit, times, unit, floor_1_hour, &round_to_1_hours);
  CheckScalarUnary(op, unit, times, unit, floor_1_day, &round_to_1_days);
  CheckScalarUnary(op, unit, times, unit, floor_1_weeks, &round_to_1_weeks);
  CheckScalarUnary(op, unit, times, unit, floor_1_weeks_sunday, &round_to_1_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, floor_1_months, &round_to_1_months);
  CheckScalarUnary(op, unit, times, unit, floor_1_quarters, &round_to_1_quarters);
  CheckScalarUnary(op, unit, times, unit, floor_1_years, &round_to_1_years);

  CheckScalarUnary(op, unit, times, unit, floor_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_microsecond,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_millisecond,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_second, &round_to_15_seconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_minute, &round_to_15_minutes);
  CheckScalarUnary(op, unit, times, unit, floor_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, floor_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, floor_15_weeks, &round_to_15_weeks);
  CheckScalarUnary(op, unit, times, unit, floor_15_weeks_sunday,
                   &round_to_15_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, floor_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, floor_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, floor_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, FloorUTC) {
  std::string op = "floor_temporal";

  // Data for tests below was generaed via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* floor_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456780", "2000-02-29 23:23:23.999999990",
          "1899-01-01 00:59:20.001001000", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* floor_15_microsecond =
      R"(["1970-01-01 00:00:59.123450", "2000-02-29 23:23:23.999990",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004120",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006150",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* floor_15_millisecond =
      R"(["1970-01-01 00:00:59.120", "2000-02-29 23:23:23.990",
          "1899-01-01 00:59:20.000", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.000", "2019-12-31 02:10:10.000",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:20.000",
          "2010-01-01 05:25:25.000", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* floor_13_second =
      R"(["1970-01-01 00:00:52", "2000-02-29 23:23:13", "1899-01-01 00:59:13",
          "2033-05-18 03:33:13", "2020-01-01 01:05:00", "2019-12-31 02:10:00",
          "2019-12-30 03:15:13", "2009-12-31 04:20:13", "2010-01-01 05:25:13",
          "2010-01-03 06:30:26", "2010-01-04 07:35:26", "2006-01-01 08:40:39",
          "2005-12-31 09:45:39", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* floor_13_minute =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:13:00", "1899-01-01 00:52:00",
          "2033-05-18 03:26:00", "2020-01-01 01:00:00", "2019-12-31 02:00:00",
          "2019-12-30 03:13:00", "2009-12-31 04:13:00", "2010-01-01 05:13:00",
          "2010-01-03 06:26:00", "2010-01-04 07:26:00", "2006-01-01 08:39:00",
          "2005-12-31 09:39:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* floor_15_hour =
      R"(["1970-01-01 00:00:00", "2000-02-29 15:00:00", "1899-01-01 00:00:00",
          "2033-05-18 00:00:00", "2020-01-01 00:00:00", "2019-12-31 00:00:00",
          "2019-12-30 00:00:00", "2009-12-31 00:00:00", "2010-01-01 00:00:00",
          "2010-01-03 00:00:00", "2010-01-04 00:00:00", "2006-01-01 00:00:00",
          "2005-12-31 00:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 00:00:00", null])";
  const char* floor_15_day =
      R"(["1970-01-01", "2000-02-16", "1899-01-01", "2033-05-16",
          "2020-01-01", "2019-12-31", "2019-12-16", "2009-12-31",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-12-31", "2008-12-16", "2008-12-16", "2012-01-01", null])";
  const char* floor_3_weeks =
      R"(["1969-12-29", "2000-02-14", "1899-01-02", "2033-05-09",
          "2019-12-30", "2019-12-30", "2019-12-30", "2010-01-04",
          "2010-01-04", "2010-01-04", "2010-01-04", "2006-01-02",
          "2006-01-02", "2008-12-22", "2008-12-29", "2012-01-02",  null])";
  const char* floor_3_weeks_sunday =
      R"(["1970-01-04", "2000-02-13", "1899-01-01", "2033-05-08",
          "2019-12-29", "2019-12-29", "2019-12-29", "2010-01-03",
          "2010-01-03", "2010-01-03", "2010-01-03", "2006-01-01",
          "2006-01-01", "2009-01-04", "2009-01-04", "2012-01-01", null])";
  const char* floor_15_months =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-01-01",
          "2020-01-01", "2019-01-01", "2019-01-01", "2009-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-01-01", "2008-01-01", "2008-01-01", "2012-01-01", null])";
  const char* floor_15_quarters =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-01-01",
          "2020-01-01", "2019-01-01", "2019-01-01", "2009-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-01-01", "2008-01-01", "2008-01-01", "2012-01-01", null])";
  const char* floor_15_years =
      R"(["1965-01-01", "1995-01-01", "1890-01-01", "2025-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "1995-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "1995-01-01",
          "1995-01-01", "1995-01-01", "1995-01-01", "2010-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, floor_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_microsecond,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_millisecond,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, floor_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, floor_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, floor_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, floor_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, floor_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, floor_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, floor_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, floor_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, floor_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, FloorZoned) {
  std::string op = "floor_temporal";

  // Data for tests below was generated via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* floor_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456780", "2000-02-29 23:23:23.999999990",
          "1899-01-01 00:59:20.001001000", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* floor_15_microsecond =
      R"(["1970-01-01 00:00:59.123450", "2000-02-29 23:23:23.999990",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004120",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006150",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* floor_15_millisecond =
      R"(["1970-01-01 00:00:59.120", "2000-02-29 23:23:23.990",
          "1899-01-01 00:59:20.000", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.000", "2019-12-31 02:10:10.000",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:20.000",
          "2010-01-01 05:25:25.000", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* floor_13_second = R"([
    "1970-01-01 00:00:52", "2000-02-29 23:23:13", "1899-01-01 00:59:13", "2033-05-18 03:33:13",
    "2020-01-01 01:05:00", "2019-12-31 02:10:00", "2019-12-30 03:15:13", "2009-12-31 04:20:13",
    "2010-01-01 05:25:13", "2010-01-03 06:30:26", "2010-01-04 07:35:26", "2006-01-01 08:40:39",
    "2005-12-31 09:45:39", "2008-12-28 00:00:00", "2008-12-29 00:00:00", "2012-01-01 01:02:00", null])";
  const char* floor_13_minute = R"([
    "1969-12-31 23:56:00", "2000-02-29 23:22:00", "1899-01-01 00:52:00", "2033-05-18 03:30:00",
    "2020-01-01 00:56:00", "2019-12-31 02:09:00", "2019-12-30 03:09:00", "2009-12-31 04:09:00",
    "2010-01-01 05:22:00", "2010-01-03 06:30:00", "2010-01-04 07:30:00", "2006-01-01 08:30:00",
    "2005-12-31 09:43:00", "2008-12-27 23:56:00", "2008-12-28 23:56:00", "2012-01-01 00:56:00", null])";
  const char* floor_15_hour = R"([
    "1969-12-31 14:30:00", "2000-02-29 13:30:00", "1898-12-31 15:00:00", "2033-05-17 14:30:00",
    "2019-12-31 13:30:00", "2019-12-30 13:30:00", "2019-12-29 13:30:00", "2009-12-30 13:30:00",
    "2010-01-01 04:30:00", "2010-01-03 04:30:00", "2010-01-04 04:30:00", "2006-01-01 04:30:00",
    "2005-12-31 04:30:00", "2008-12-27 13:30:00", "2008-12-28 13:30:00", "2011-12-31 13:30:00", null])";
  const char* floor_15_day = R"([
    "1969-12-31 14:30:00", "2000-02-29 13:30:00", "1898-12-31 15:00:00", "2033-05-15 14:30:00",
    "2019-12-31 13:30:00", "2019-12-30 13:30:00", "2019-12-15 13:30:00", "2009-12-30 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-12-30 13:30:00", "2008-12-15 13:30:00", "2008-12-15 13:30:00", "2011-12-31 13:30:00", null])";
  const char* floor_3_weeks = R"([
    "1969-12-28 14:30:00", "2000-02-13 13:30:00", "1899-01-01 15:00:00", "2033-05-08 14:30:00",
    "2019-12-29 13:30:00", "2019-12-29 13:30:00", "2019-12-29 13:30:00", "2010-01-03 13:30:00",
    "2010-01-03 13:30:00", "2010-01-03 13:30:00", "2010-01-03 13:30:00", "2006-01-01 13:30:00",
    "2006-01-01 13:30:00", "2008-12-21 13:30:00", "2008-12-28 13:30:00", "2012-01-01 13:30:00", null])";
  const char* floor_3_weeks_sunday = R"([
    "1970-01-03 14:30:00", "2000-03-04 13:30:00", "1898-12-31 15:00:00", "2033-05-07 14:30:00",
    "2019-12-28 13:30:00", "2019-12-28 13:30:00", "2019-12-28 13:30:00", "2010-01-02 13:30:00",
    "2010-01-02 13:30:00", "2010-01-02 13:30:00", "2010-01-02 13:30:00", "2005-12-31 13:30:00",
    "2005-12-31 13:30:00", "2009-01-03 13:30:00", "2009-01-03 13:30:00", "2011-12-31 13:30:00", null])";
  const char* floor_5_months = R"([
    "1969-12-31 14:30:00", "1999-12-31 13:30:00", "1898-12-31 15:00:00", "2032-12-31 13:30:00",
    "2019-12-31 13:30:00", "2019-10-31 13:30:00", "2019-10-31 13:30:00", "2009-10-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-10-31 13:30:00", "2008-10-31 13:30:00", "2008-10-31 13:30:00", "2011-12-31 13:30:00", null])";
  const char* floor_3_quarters = R"([
    "1969-12-31 14:30:00", "1999-12-31 13:30:00", "1898-12-31 15:00:00", "2032-12-31 13:30:00",
    "2019-12-31 13:30:00", "2019-09-30 14:30:00", "2019-09-30 14:30:00", "2009-09-30 14:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-09-30 14:30:00", "2008-09-30 14:30:00", "2008-09-30 14:30:00", "2011-12-31 13:30:00", null])";
  const char* floor_15_years = R"([
    "1964-12-31 14:30:00", "1994-12-31 13:30:00", "1889-12-31 14:34:12", "2024-12-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "1994-12-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "1994-12-31 13:30:00",
    "1994-12-31 13:30:00", "1994-12-31 13:30:00", "1994-12-31 13:30:00", "2009-12-31 13:30:00", null])";

  // Australia/Broken_Hill timezone is defined as UTC+9:30 and UTC+10:30 during DST.
  // DST runs from first Sunday in October to first Sunday in April.
  auto unit = timestamp(TimeUnit::NANO, "Australia/Broken_Hill");
  CheckScalarUnary(op, unit, times, unit, floor_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_microsecond,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, floor_15_millisecond,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, floor_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, floor_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, floor_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, floor_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, floor_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, floor_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, floor_5_months, &round_to_5_months);
  CheckScalarUnary(op, unit, times, unit, floor_3_quarters, &round_to_3_quarters);
  CheckScalarUnary(op, unit, times, unit, floor_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTest, TestRoundTemporal) {
  std::string op = "round_temporal";

  const char* round_1_nanoseconds =
      R"(["1970-01-01 00:00:59.123456789", "2000-02-29 23:23:23.999999999",
          "1899-01-01 00:59:20.001001001", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* round_1_microseconds =
      R"(["1970-01-01 00:00:59.123457", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001001", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004132",
          "2010-01-01 05:25:25.005321", "2010-01-03 06:30:30.006163",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* round_1_milliseconds =
      R"(["1970-01-01 00:00:59.123", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:20.001", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.001", "2019-12-31 02:10:10.002",
          "2019-12-30 03:15:15.003", "2009-12-31 04:20:20.004",
          "2010-01-01 05:25:25.005", "2010-01-03 06:30:30.006",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* round_1_seconds =
      R"(["1970-01-01 00:00:59", "2000-02-29 23:23:24", "1899-01-01 00:59:20",
          "2033-05-18 03:33:20", "2020-01-01 01:05:05", "2019-12-31 02:10:10",
          "2019-12-30 03:15:15", "2009-12-31 04:20:20", "2010-01-01 05:25:25",
          "2010-01-03 06:30:30", "2010-01-04 07:35:35", "2006-01-01 08:40:40",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:03", null])";
  const char* round_1_minutes =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:23:00", "1899-01-01 00:59:00",
          "2033-05-18 03:33:00", "2020-01-01 01:05:00", "2019-12-31 02:10:00",
          "2019-12-30 03:15:00", "2009-12-31 04:20:00", "2010-01-01 05:25:00",
          "2010-01-03 06:31:00", "2010-01-04 07:36:00", "2006-01-01 08:41:00",
          "2005-12-31 09:46:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* round_1_hours =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:00:00", "1899-01-01 01:00:00",
          "2033-05-18 04:00:00", "2020-01-01 01:00:00", "2019-12-31 02:00:00",
          "2019-12-30 03:00:00", "2009-12-31 04:00:00", "2010-01-01 05:00:00",
          "2010-01-03 07:00:00", "2010-01-04 08:00:00", "2006-01-01 09:00:00",
          "2005-12-31 10:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* round_1_days =
      R"(["1970-01-01", "2000-03-01", "1899-01-01", "2033-05-18", "2020-01-01",
          "2019-12-31", "2019-12-30", "2009-12-31", "2010-01-01", "2010-01-03",
          "2010-01-04", "2006-01-01", "2005-12-31", "2008-12-28", "2008-12-29",
          "2012-01-01", null])";
  const char* round_1_weeks =
      R"(["1969-12-29", "2000-02-28", "1899-01-02", "2033-05-16", "2019-12-30",
          "2019-12-30", "2019-12-30", "2009-12-28", "2010-01-04", "2010-01-04",
          "2010-01-04", "2006-01-02", "2006-01-02", "2008-12-29", "2008-12-29",
          "2012-01-02", null])";
  const char* round_1_weeks_sunday =
      R"(["1970-01-04", "2000-02-27", "1899-01-01", "2033-05-15", "2019-12-29",
          "2019-12-29", "2019-12-29", "2010-01-03", "2010-01-03", "2010-01-03",
          "2010-01-03", "2006-01-01", "2006-01-01", "2008-12-28", "2008-12-28",
          "2012-01-01", null])";
  const char* round_1_months =
      R"(["1970-01-01", "2000-03-01", "1899-01-01", "2033-06-01", "2020-01-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2006-01-01", "2009-01-01", "2009-01-01",
           "2012-01-01", null])";
  const char* round_1_quarters =
      R"(["1970-01-01", "2000-04-01", "1899-01-01", "2033-07-01", "2020-01-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2006-01-01", "2009-01-01", "2009-01-01",
          "2012-01-01", null])";
  const char* round_1_years =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-01-01", "2020-01-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-01", "2006-01-01", "2009-01-01", "2009-01-01",
          "2012-01-01", null])";

  const char* round_15_nanoseconds =
      R"(["1970-01-01 00:00:59.123456790", "2000-02-29 23:23:24.000000000",
          "1899-01-01 00:59:20.001001005", "2033-05-18 03:33:19.999999995",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163005",
          "2010-01-04 07:35:34.999999995", "2006-01-01 08:40:40.000000005",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* round_15_microseconds =
      R"(["1970-01-01 00:00:59.123460", "2000-02-29 23:23:24.000000",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:19.999995",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:34.999995", "2006-01-01 08:40:40.000005",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* round_15_milliseconds =
      R"(["1970-01-01 00:00:59.130", "2000-02-29 23:23:24.000",
          "1899-01-01 00:59:19.995", "2033-05-18 03:33:19.995",
          "2020-01-01 01:05:04.995", "2019-12-31 02:10:10.005",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:20.010",
          "2010-01-01 05:25:25.005", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:34.995", "2006-01-01 08:40:40.005",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* round_15_seconds =
      R"(["1970-01-01 00:01:00", "2000-02-29 23:23:30", "1899-01-01 00:59:15",
          "2033-05-18 03:33:15", "2020-01-01 01:05:00", "2019-12-31 02:10:15",
          "2019-12-30 03:15:15", "2009-12-31 04:20:15", "2010-01-01 05:25:30",
          "2010-01-03 06:30:30", "2010-01-04 07:35:30", "2006-01-01 08:40:45",
          "2005-12-31 09:45:45", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* round_15_minutes =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:30:00", "1899-01-01 01:00:00",
          "2033-05-18 03:30:00", "2020-01-01 01:00:00", "2019-12-31 02:15:00",
          "2019-12-30 03:15:00", "2009-12-31 04:15:00", "2010-01-01 05:30:00",
          "2010-01-03 06:30:00", "2010-01-04 07:30:00", "2006-01-01 08:45:00",
          "2005-12-31 09:45:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* round_15_hours =
      R"(["1970-01-01 00:00:00", "2000-02-29 21:00:00", "1899-01-01 03:00:00",
          "2033-05-18 03:00:00", "2019-12-31 21:00:00", "2019-12-31 06:00:00",
          "2019-12-30 00:00:00", "2009-12-31 09:00:00", "2010-01-01 00:00:00",
          "2010-01-03 12:00:00", "2010-01-04 03:00:00", "2006-01-01 09:00:00",
          "2005-12-31 03:00:00", "2008-12-28 06:00:00", "2008-12-28 21:00:00",
          "2012-01-01 00:00:00", null])";
  const char* round_15_days =
      R"(["1970-01-01", "2000-02-23", "1898-12-29", "2033-05-15", "2019-12-25",
          "2019-12-25", "2019-12-25", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-01-07", "2006-01-07", "2008-12-22", "2008-12-22",
          "2012-01-06", null])";
  const char* round_15_weeks =
      R"(["1969-12-29", "2000-03-06", "1898-12-26", "2033-03-28", "2020-01-06",
          "2020-01-06", "2020-01-06", "2009-12-14", "2009-12-14", "2009-12-14",
          "2009-12-14", "2005-12-05", "2005-12-05", "2009-02-02", "2009-02-02",
          "2011-12-19", null])";
  const char* round_15_weeks_sunday =
      R"(["1969-12-28", "2000-03-05", "1898-12-25", "2033-03-27", "2020-01-05",
          "2020-01-05", "2020-01-05", "2009-12-13", "2009-12-13", "2009-12-13",
          "2009-12-13", "2005-12-04", "2005-12-04", "2009-02-01", "2009-02-01",
          "2011-12-18", null])";
  const char* round_15_months =
      R"(["1970-01-01", "2000-01-01", "1898-10-01", "2033-10-01", "2020-01-01",
          "2020-01-01", "2020-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2006-04-01", "2006-04-01", "2008-10-01", "2008-10-01",
          "2012-07-01", null])";
  const char* round_15_quarters =
      R"(["1970-01-01", "2000-01-01", "1898-10-01", "2033-10-01", "2018-10-01",
          "2018-10-01", "2018-10-01", "2011-04-01", "2011-04-01", "2011-04-01",
          "2011-04-01", "2007-07-01", "2007-07-01", "2007-07-01", "2007-07-01",
          "2011-04-01", null])";
  const char* round_15_years =
      R"(["1965-01-01", "1995-01-01", "1905-01-01", "2040-01-01", "2025-01-01",
          "2025-01-01", "2025-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", null])";

  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, round_1_nanoseconds, &round_to_1_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, round_1_microseconds, &round_to_1_microseconds);
  CheckScalarUnary(op, unit, times, unit, round_1_milliseconds, &round_to_1_milliseconds);
  CheckScalarUnary(op, unit, times, unit, round_1_seconds, &round_to_1_seconds);
  CheckScalarUnary(op, unit, times, unit, round_1_minutes, &round_to_1_minutes);
  CheckScalarUnary(op, unit, times, unit, round_1_hours, &round_to_1_hours);
  CheckScalarUnary(op, unit, times, unit, round_1_days, &round_to_1_days);
  CheckScalarUnary(op, unit, times, unit, round_1_weeks, &round_to_1_weeks);
  CheckScalarUnary(op, unit, times, unit, round_1_weeks_sunday, &round_to_1_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, round_1_months, &round_to_1_months);
  CheckScalarUnary(op, unit, times, unit, round_1_quarters, &round_to_1_quarters);
  CheckScalarUnary(op, unit, times, unit, round_1_years, &round_to_1_years);

  CheckScalarUnary(op, unit, times, unit, round_15_nanoseconds, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_microseconds,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_milliseconds,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_seconds, &round_to_15_seconds);
  CheckScalarUnary(op, unit, times, unit, round_15_minutes, &round_to_15_minutes);
  CheckScalarUnary(op, unit, times, unit, round_15_hours, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, round_15_days, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, round_15_weeks, &round_to_15_weeks);
  CheckScalarUnary(op, unit, times, unit, round_15_weeks_sunday,
                   &round_to_15_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, round_15_months, &round_to_15_months);
  CheckScalarUnary(op, unit, times, unit, round_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, round_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTest, TestCeilFloorRoundTemporalBrussels) {
  RoundTemporalOptions round_to_1_hours = RoundTemporalOptions(1, CalendarUnit::HOUR);
  RoundTemporalOptions round_to_2_hours = RoundTemporalOptions(2, CalendarUnit::HOUR);
  auto unit = timestamp(TimeUnit::NANO, "Europe/Brussels");

  const char* times = R"(["2021-12-23 12:17:00", null])";
  const char* ceil_1_hours = R"(["2021-12-23 13:00", null])";
  const char* ceil_2_hours = R"(["2021-12-23 13:00", null])";
  const char* floor_1_hours = R"(["2021-12-23 12:00", null])";
  const char* floor_2_hours = R"(["2021-12-23 11:00", null])";
  const char* round_1_hours = R"(["2021-12-23 12:00", null])";
  const char* round_2_hours = R"(["2021-12-23 13:00", null])";

  CheckScalarUnary("ceil_temporal", unit, times, unit, ceil_1_hours, &round_to_1_hours);
  CheckScalarUnary("ceil_temporal", unit, times, unit, ceil_2_hours, &round_to_2_hours);
  CheckScalarUnary("floor_temporal", unit, times, unit, floor_1_hours, &round_to_1_hours);
  CheckScalarUnary("floor_temporal", unit, times, unit, floor_2_hours, &round_to_2_hours);
  CheckScalarUnary("round_temporal", unit, times, unit, round_1_hours, &round_to_1_hours);
  CheckScalarUnary("round_temporal", unit, times, unit, round_2_hours, &round_to_2_hours);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, RoundUTC) {
  std::string op = "round_temporal";

  // Data for tests below was generaed via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* round_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456795", "2000-02-29 23:23:24.000000005",
          "1899-01-01 00:59:20.001001000", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* round_15_microsecond =
      R"(["1970-01-01 00:00:59.123450", "2000-02-29 23:23:24.000005",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* round_15_millisecond =
      R"(["1970-01-01 00:00:59.120", "2000-02-29 23:23:24.005",
          "1899-01-01 00:59:20.000", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.000", "2019-12-31 02:10:10.000",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:20.000",
          "2010-01-01 05:25:25.000", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* round_13_second =
      R"(["1970-01-01 00:01:05", "2000-02-29 23:23:26", "1899-01-01 00:59:26",
          "2033-05-18 03:33:26", "2020-01-01 01:05:00", "2019-12-31 02:10:13",
          "2019-12-30 03:15:13", "2009-12-31 04:20:26", "2010-01-01 05:25:26",
          "2010-01-03 06:30:26", "2010-01-04 07:35:39", "2006-01-01 08:40:39",
          "2005-12-31 09:45:39", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:02:00", null])";
  const char* round_13_minute =
      R"(["1970-01-01 00:00:00", "2000-02-29 23:26:00", "1899-01-01 01:05:00",
          "2033-05-18 03:39:00", "2020-01-01 01:00:00", "2019-12-31 02:13:00",
          "2019-12-30 03:13:00", "2009-12-31 04:26:00", "2010-01-01 05:26:00",
          "2010-01-03 06:26:00", "2010-01-04 07:39:00", "2006-01-01 08:39:00",
          "2005-12-31 09:52:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 01:00:00", null])";
  const char* round_15_hour =
      R"(["1970-01-01 00:00:00", "2000-03-01 06:00:00", "1899-01-01 00:00:00",
          "2033-05-18 00:00:00", "2020-01-01 00:00:00", "2019-12-31 00:00:00",
          "2019-12-30 00:00:00", "2009-12-31 00:00:00", "2010-01-01 00:00:00",
          "2010-01-03 00:00:00", "2010-01-04 15:00:00", "2006-01-01 15:00:00",
          "2005-12-31 15:00:00", "2008-12-28 00:00:00", "2008-12-29 00:00:00",
          "2012-01-01 00:00:00", null])";
  const char* round_15_day =
      R"(["1970-01-01", "2000-03-02", "1899-01-01", "2033-05-16",
          "2020-01-01", "2019-12-31", "2019-12-31", "2009-12-31",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-12-31", "2008-12-31", "2008-12-31", "2012-01-01", null])";
  const char* round_3_weeks =
      R"(["1969-12-29", "2000-03-06", "1899-01-02", "2033-05-09",
          "2019-12-30", "2019-12-30", "2019-12-30", "2010-01-04",
          "2010-01-04", "2010-01-04", "2010-01-04", "2006-01-02",
          "2006-01-02", "2008-12-22", "2008-12-29", "2012-01-02", null])";
  const char* round_3_weeks_sunday =
      R"(["1970-01-04", "2000-03-05", "1899-01-01", "2033-05-08",
          "2019-12-29", "2019-12-29", "2019-12-29", "2010-01-03",
          "2010-01-03", "2010-01-03", "2010-01-03", "2006-01-01",
          "2006-01-01", "2009-01-04", "2009-01-04", "2012-01-01", null])";
  const char* round_5_months =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-06-01",
          "2020-01-01", "2019-11-01", "2019-11-01", "2009-11-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-11-01", "2008-11-01", "2008-11-01", "2012-01-01",  null])";
  const char* round_15_quarters =
      R"(["1970-01-01", "2000-01-01", "1899-01-01", "2033-01-01",
          "2020-01-01", "2019-01-01", "2019-01-01", "2009-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2006-01-01",
          "2005-01-01", "2008-01-01", "2008-01-01", "2012-01-01", null])";
  const char* round_15_years =
      R"(["1965-01-01", "1995-01-01", "1905-01-01", "2040-01-01",
          "2025-01-01", "2025-01-01", "2025-01-01", "2010-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01",
          "2010-01-01", "2010-01-01", "2010-01-01", "2010-01-01", null])";
  auto unit = timestamp(TimeUnit::NANO, "UTC");
  CheckScalarUnary(op, unit, times, unit, round_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_microsecond,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_millisecond,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, round_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, round_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, round_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, round_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, round_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, round_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, round_5_months, &round_to_5_months);
  CheckScalarUnary(op, unit, times, unit, round_15_quarters, &round_to_15_quarters);
  CheckScalarUnary(op, unit, times, unit, round_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTestMultipleSinceGreaterUnit, RoundZoned) {
  std::string op = "round_temporal";

  // Data for tests below was generated via lubridate with the exception
  // of week data because lubridate currently does not support rounding to
  // multiple of week.
  const char* round_15_nanosecond =
      R"(["1970-01-01 00:00:59.123456795", "2000-02-29 23:23:24.000000005",
          "1899-01-01 00:59:20.001001000", "2033-05-18 03:33:20.000000000",
          "2020-01-01 01:05:05.001000000", "2019-12-31 02:10:10.002000000",
          "2019-12-30 03:15:15.003000000", "2009-12-31 04:20:20.004132000",
          "2010-01-01 05:25:25.005321000", "2010-01-03 06:30:30.006163000",
          "2010-01-04 07:35:35.000000000", "2006-01-01 08:40:40.000000000",
          "2005-12-31 09:45:45.000000000", "2008-12-28 00:00:00.000000000",
          "2008-12-29 00:00:00.000000000", "2012-01-01 01:02:03.000000000", null])";
  const char* round_15_microsecond =
      R"(["1970-01-01 00:00:59.123450", "2000-02-29 23:23:24.000005",
          "1899-01-01 00:59:20.001000", "2033-05-18 03:33:20.000000",
          "2020-01-01 01:05:05.001000", "2019-12-31 02:10:10.002000",
          "2019-12-30 03:15:15.003000", "2009-12-31 04:20:20.004135",
          "2010-01-01 05:25:25.005315", "2010-01-03 06:30:30.006165",
          "2010-01-04 07:35:35.000000", "2006-01-01 08:40:40.000000",
          "2005-12-31 09:45:45.000000", "2008-12-28 00:00:00.000000",
          "2008-12-29 00:00:00.000000", "2012-01-01 01:02:03.000000", null])";
  const char* round_15_millisecond =
      R"(["1970-01-01 00:00:59.120", "2000-02-29 23:23:24.005",
          "1899-01-01 00:59:20.000", "2033-05-18 03:33:20.000",
          "2020-01-01 01:05:05.000", "2019-12-31 02:10:10.000",
          "2019-12-30 03:15:15.000", "2009-12-31 04:20:20.000",
          "2010-01-01 05:25:25.000", "2010-01-03 06:30:30.000",
          "2010-01-04 07:35:35.000", "2006-01-01 08:40:40.000",
          "2005-12-31 09:45:45.000", "2008-12-28 00:00:00.000",
          "2008-12-29 00:00:00.000", "2012-01-01 01:02:03.000", null])";
  const char* round_13_second = R"([
    "1970-01-01 00:01:05", "2000-02-29 23:23:26", "1899-01-01 00:59:26", "2033-05-18 03:33:26",
    "2020-01-01 01:05:00", "2019-12-31 02:10:13", "2019-12-30 03:15:13", "2009-12-31 04:20:26",
    "2010-01-01 05:25:26", "2010-01-03 06:30:26", "2010-01-04 07:35:39", "2006-01-01 08:40:39",
    "2005-12-31 09:45:39", "2008-12-28 00:00:00", "2008-12-29 00:00:00", "2012-01-01 01:02:00", null])";
  const char* round_13_minute = R"([
    "1969-12-31 23:56:00", "2000-02-29 23:22:00", "1899-01-01 01:05:00", "2033-05-18 03:30:00",
    "2020-01-01 01:09:00", "2019-12-31 02:09:00", "2019-12-30 03:09:00", "2009-12-31 04:22:00",
    "2010-01-01 05:22:00", "2010-01-03 06:30:00", "2010-01-04 07:30:00", "2006-01-01 08:43:00",
    "2005-12-31 09:43:00", "2008-12-27 23:56:00", "2008-12-28 23:56:00", "2012-01-01 00:56:00", null])";
  const char* round_15_hour = R"([
    "1970-01-01 05:30:00", "2000-03-01 04:30:00", "1899-01-01 06:00:00", "2033-05-18 05:30:00",
    "2020-01-01 04:30:00", "2019-12-31 04:30:00", "2019-12-30 04:30:00", "2009-12-31 04:30:00",
    "2010-01-01 04:30:00", "2010-01-03 04:30:00", "2010-01-04 04:30:00", "2006-01-01 04:30:00",
    "2005-12-31 04:30:00", "2008-12-28 04:30:00", "2008-12-29 04:30:00", "2012-01-01 04:30:00", null])";
  const char* round_15_day = R"([
    "1969-12-31 14:30:00", "2000-02-29 13:30:00", "1898-12-31 15:00:00", "2033-05-15 14:30:00",
    "2019-12-31 13:30:00", "2019-12-30 13:30:00", "2019-12-30 13:30:00", "2009-12-30 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-12-30 13:30:00", "2008-12-30 13:30:00", "2008-12-30 13:30:00", "2011-12-31 13:30:00", null])";
  const char* round_3_weeks = R"([
    "1969-12-28 14:30:00", "2000-03-05 13:30:00", "1899-01-01 15:00:00", "2033-05-08 14:30:00",
    "2019-12-29 13:30:00", "2019-12-29 13:30:00", "2019-12-29 13:30:00", "2010-01-03 13:30:00",
    "2010-01-03 13:30:00", "2010-01-03 13:30:00", "2010-01-03 13:30:00", "2006-01-01 13:30:00",
    "2006-01-01 13:30:00", "2008-12-21 13:30:00", "2008-12-28 13:30:00", "2012-01-01 13:30:00",null])";
  const char* round_3_weeks_sunday = R"([
    "1970-01-03 14:30:00", "2000-03-04 13:30:00", "1898-12-31 15:00:00", "2033-05-28 14:30:00",
    "2019-12-28 13:30:00", "2019-12-28 13:30:00", "2019-12-28 13:30:00", "2010-01-02 13:30:00",
    "2010-01-02 13:30:00", "2010-01-02 13:30:00", "2010-01-02 13:30:00", "2005-12-31 13:30:00",
    "2005-12-31 13:30:00", "2009-01-03 13:30:00", "2009-01-03 13:30:00", "2011-12-31 13:30:00", null])";
  const char* round_5_months = R"([
    "1969-12-31 14:30:00", "1999-12-31 13:30:00", "1898-12-31 15:00:00", "2033-05-31 14:30:00",
    "2019-12-31 13:30:00", "2019-10-31 13:30:00", "2019-10-31 13:30:00", "2009-10-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-10-31 13:30:00", "2008-10-31 13:30:00", "2008-10-31 13:30:00", "2011-12-31 13:30:00", null])";
  const char* round_3_quarters = R"([
    "1969-12-31 14:30:00", "1999-12-31 13:30:00", "1898-12-31 15:00:00", "2033-09-30 14:30:00",
    "2019-12-31 13:30:00", "2019-09-30 14:30:00", "2019-09-30 14:30:00", "2009-09-30 14:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2005-12-31 13:30:00",
    "2005-09-30 14:30:00", "2008-09-30 14:30:00", "2008-09-30 14:30:00", "2011-12-31 13:30:00", null])";
  const char* round_15_years = R"([
    "1964-12-31 14:30:00", "1994-12-31 13:30:00", "1904-12-31 14:30:00", "2039-12-31 13:30:00",
    "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2024-12-31 13:30:00", "2009-12-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00",
    "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", "2009-12-31 13:30:00", null])";

  // Australia/Broken_Hill timezone is defined as UTC+9:30 and UTC+10:30 during DST.
  // DST runs from first Sunday in October to first Sunday in April.
  auto unit = timestamp(TimeUnit::NANO, "Australia/Broken_Hill");
  CheckScalarUnary(op, unit, times, unit, round_15_nanosecond, &round_to_15_nanoseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_microsecond,
                   &round_to_15_microseconds);
  CheckScalarUnary(op, unit, times, unit, round_15_millisecond,
                   &round_to_15_milliseconds);
  CheckScalarUnary(op, unit, times, unit, round_13_second, &round_to_13_seconds);
  CheckScalarUnary(op, unit, times, unit, round_13_minute, &round_to_13_minutes);
  CheckScalarUnary(op, unit, times, unit, round_15_hour, &round_to_15_hours);
  CheckScalarUnary(op, unit, times, unit, round_15_day, &round_to_15_days);
  CheckScalarUnary(op, unit, times, unit, round_3_weeks, &round_to_3_weeks);
  CheckScalarUnary(op, unit, times, unit, round_3_weeks_sunday, &round_to_3_weeks_sunday);
  CheckScalarUnary(op, unit, times, unit, round_5_months, &round_to_5_months);
  CheckScalarUnary(op, unit, times, unit, round_3_quarters, &round_to_3_quarters);
  CheckScalarUnary(op, unit, times, unit, round_15_years, &round_to_15_years);
}

TEST_F(ScalarTemporalTest, TestCeilFloorRoundTemporalKolkata) {
  // Kolkata timezone was defined as UTC+5:21:10 from 1871 to 1906 when it changed to
  // IST (UTC+05:30) without DST. This test is to check rounding is done in historical
  // local time.
  RoundTemporalOptions round_to_1_hours = RoundTemporalOptions(1, CalendarUnit::HOUR);
  RoundTemporalOptions round_to_2_hours = RoundTemporalOptions(2, CalendarUnit::HOUR);
  auto unit = timestamp(TimeUnit::NANO, "Asia/Kolkata");

  const char* times =
      R"(["2021-12-23 12:17", "1899-04-18 01:57:09.190202880",
          "1899-09-12 07:03:30.080325120", "1904-06-21 20:55:36.493869056", null])";
  const char* ceil_1_hours =
      R"(["2021-12-23 12:30", "1899-04-18 02:38:50", "1899-09-12 07:38:50",
          "1904-06-21 21:38:50", null])";
  const char* ceil_2_hours =
      R"(["2021-12-23 12:30", "1899-04-18 02:38:50", "1899-09-12 08:38:50",
          "1904-06-21 22:38:50", null])";
  const char* floor_1_hours =
      R"(["2021-12-23 11:30", "1899-04-18 01:38:50", "1899-09-12 06:38:50",
          "1904-06-21 20:38:50", null])";
  const char* floor_2_hours =
      R"(["2021-12-23 10:30", "1899-04-18 00:38:50", "1899-09-12 06:38:50",
          "1904-06-21 20:38:50", null])";
  const char* round_1_hours =
      R"(["2021-12-23 12:30", "1899-04-18 01:38:50", "1899-09-12 06:38:50",
          "1904-06-21 20:38:50", null])";
  const char* round_2_hours =
      R"(["2021-12-23 12:30", "1899-04-18 02:38:50", "1899-09-12 06:38:50",
          "1904-06-21 20:38:50", null])";

  CheckScalarUnary("ceil_temporal", unit, times, unit, ceil_1_hours, &round_to_1_hours);
  CheckScalarUnary("ceil_temporal", unit, times, unit, ceil_2_hours, &round_to_2_hours);
  CheckScalarUnary("floor_temporal", unit, times, unit, floor_1_hours, &round_to_1_hours);
  CheckScalarUnary("floor_temporal", unit, times, unit, floor_2_hours, &round_to_2_hours);
  CheckScalarUnary("round_temporal", unit, times, unit, round_1_hours, &round_to_1_hours);
  CheckScalarUnary("round_temporal", unit, times, unit, round_2_hours, &round_to_2_hours);
}

TEST_F(ScalarTemporalTest, TestCeilFloorRoundTemporalDate) {
  RoundTemporalOptions round_to_2_hours = RoundTemporalOptions(2, CalendarUnit::HOUR);
  const char* date32s = R"([0, 11016, -25932, null])";
  const char* date64s = R"([0, 951782400000, -2240524800000, null])";
  auto dates32 = ArrayFromJSON(date32(), date32s);
  auto dates64 = ArrayFromJSON(date64(), date64s);

  CheckScalarUnary("ceil_temporal", dates32, dates32, &round_to_2_hours);
  CheckScalarUnary("floor_temporal", dates32, dates32, &round_to_2_hours);
  CheckScalarUnary("round_temporal", dates32, dates32, &round_to_2_hours);
  CheckScalarUnary("ceil_temporal", dates64, dates64, &round_to_2_hours);
  CheckScalarUnary("floor_temporal", dates64, dates64, &round_to_2_hours);
  CheckScalarUnary("round_temporal", dates64, dates64, &round_to_2_hours);

  const char* times_s = R"([0, 7200, null])";
  const char* times_ms = R"([0, 7200000, null])";
  const char* times_us = R"([0, 7200000000, null])";
  const char* times_ns = R"([0, 7200000000000, null])";
  auto arr_s = ArrayFromJSON(time32(TimeUnit::SECOND), times_s);
  auto arr_ms = ArrayFromJSON(time32(TimeUnit::MILLI), times_ms);
  auto arr_us = ArrayFromJSON(time64(TimeUnit::MICRO), times_us);
  auto arr_ns = ArrayFromJSON(time64(TimeUnit::NANO), times_ns);

  CheckScalarUnary("ceil_temporal", arr_s, arr_s, &round_to_2_hours);
  CheckScalarUnary("ceil_temporal", arr_ms, arr_ms, &round_to_2_hours);
  CheckScalarUnary("ceil_temporal", arr_us, arr_us, &round_to_2_hours);
  CheckScalarUnary("ceil_temporal", arr_ns, arr_ns, &round_to_2_hours);
}

}  // namespace compute
}  // namespace arrow

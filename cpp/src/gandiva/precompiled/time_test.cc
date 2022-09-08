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

#include "gandiva/execution_context.h"
#include "gandiva/precompiled/testing.h"
#include "gandiva/precompiled/time_constants.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestTime, TestCastDate) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castDATE_utf8(context_ptr, "1967-12-1", 9), -65836800000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "2067-12-1", 9), 3089923200000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "7-12-1", 6), 1196467200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "067-12-1", 8), 3089923200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "0067-12-1", 9), -60023980800000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "00067-12-1", 10), -60023980800000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "167-12-1", 8), -56868307200000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "1972-12-1", 9), 92016000000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "72-12-1", 7), 92016000000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "1972222222", 10), 0);
  EXPECT_EQ(context.get_error(), "Not a valid date value 1972222222");
  context.Reset();

  EXPECT_EQ(castDATE_utf8(context_ptr, "blahblah", 8), 0);
  EXPECT_EQ(castDATE_utf8(context_ptr, "1967-12-1bb", 11), -65836800000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "67-1-1", 6), 3061065600000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-1-1", 6), 31536000000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-45-1", 7), 0);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-12-XX", 8), 0);

  EXPECT_EQ(castDATE_date32(1), 86400000);
}

TEST(TestTime, TestCastTimestamp) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1967-12-1", 9), -65836800000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2067-12-1", 9), 3089923200000);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "7-12-1", 6), 1196467200000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "067-12-1", 8), 3089923200000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "0067-12-1", 9), -60023980800000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "00067-12-1", 10), -60023980800000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "167-12-1", 8), -56868307200000);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1972-12-1", 9), 92016000000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "72-12-1", 7), 92016000000);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1972-12-1", 9), 92016000000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "67-1-1", 6), 3061065600000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "71-1-1", 6), 31536000000);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30", 18), 969702330000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920", 22), 969702330920);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920 +08:00", 29),
            969673530920);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920 -11:45", 29),
            969744630920);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "65-03-04 00:20:40.920 +00:30", 28),
            3003349840920);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1932-05-18 11:30:00.920 +11:30", 30),
            -1187308799080);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1857-02-11 20:31:40.920 -05:30", 30),
            -3562264699080);
  EXPECT_EQ(castTIMESTAMP_date64(
                castDATE_utf8(context_ptr, "2000-09-23 9:45:30.920 +08:00", 29)),
            castTIMESTAMP_utf8(context_ptr, "2000-09-23 0:00:00.000 +00:00", 29));

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.1", 20),
            castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30", 18) + 100);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.10", 20),
            castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30", 18) + 100);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.100", 20),
            castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30", 18) + 100);

  // error cases
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-01-01 24:00:00", 19), 0);
  EXPECT_EQ(context.get_error(),
            "Not a valid time for timestamp value 2000-01-01 24:00:00");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-01-01 00:60:00", 19), 0);
  EXPECT_EQ(context.get_error(),
            "Not a valid time for timestamp value 2000-01-01 00:60:00");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-01-01 00:00:100", 20), 0);
  EXPECT_EQ(context.get_error(),
            "Not a valid time for timestamp value 2000-01-01 00:00:100");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-01-01 00:00:00.0001", 24), 0);
  EXPECT_EQ(context.get_error(),
            "Invalid millis for timestamp value 2000-01-01 00:00:00.0001");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-01-01 00:00:00.1000", 24), 0);
  EXPECT_EQ(context.get_error(),
            "Invalid millis for timestamp value 2000-01-01 00:00:00.1000");
  context.Reset();
}

TEST(TestTime, TestCastTimeUtf8) {
  ExecutionContext context;
  auto context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castTIME_utf8(context_ptr, "9:45:30", 7), 35130000);
  EXPECT_EQ(castTIME_utf8(context_ptr, "9:45:30.920", 11), 35130920);

  EXPECT_EQ(castTIME_utf8(context_ptr, "9:45:30.1", 9),
            castTIME_utf8(context_ptr, "9:45:30", 7) + 100);

  EXPECT_EQ(castTIME_utf8(context_ptr, "9:45:30.10", 10),
            castTIME_utf8(context_ptr, "9:45:30", 7) + 100);

  EXPECT_EQ(castTIME_utf8(context_ptr, "9:45:30.100", 11),
            castTIME_utf8(context_ptr, "9:45:30", 7) + 100);

  // error cases
  EXPECT_EQ(castTIME_utf8(context_ptr, "24H00H00", 8), 0);
  EXPECT_EQ(context.get_error(), "Invalid character in time 24H00H00");
  context.Reset();

  EXPECT_EQ(castTIME_utf8(context_ptr, "24:00:00", 8), 0);
  EXPECT_EQ(context.get_error(), "Not a valid time value 24:00:00");
  context.Reset();

  EXPECT_EQ(castTIME_utf8(context_ptr, "00:60:00", 8), 0);
  EXPECT_EQ(context.get_error(), "Not a valid time value 00:60:00");
  context.Reset();

  EXPECT_EQ(castTIME_utf8(context_ptr, "00:00:100", 9), 0);
  EXPECT_EQ(context.get_error(), "Not a valid time value 00:00:100");
  context.Reset();

  EXPECT_EQ(castTIME_utf8(context_ptr, "00:00:00.0001", 13), 0);
  EXPECT_EQ(context.get_error(), "Invalid millis for time value 00:00:00.0001");
  context.Reset();

  EXPECT_EQ(castTIME_utf8(context_ptr, "00:00:00.1000", 13), 0);
  EXPECT_EQ(context.get_error(), "Invalid millis for time value 00:00:00.1000");
  context.Reset();
}

#ifndef _WIN32

// TODO(wesm): ARROW-4495. Need to address TZ database issues on Windows

TEST(TestTime, TestCastTimestampWithTZ) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920 Canada/Pacific", 37),
            969727530920);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2012-02-28 23:30:59 Asia/Kolkata", 32),
            1330452059000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1923-10-07 03:03:03 America/New_York", 36),
            -1459094217000);
}

TEST(TestTime, TestCastTimestampErrors) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  // error cases
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "20000923", 8), 0);
  EXPECT_EQ(context.get_error(), "Not a valid day for timestamp value 20000923");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-2b", 10), 0);
  EXPECT_EQ(context.get_error(),
            "Invalid timestamp or unknown zone for timestamp value 2000-09-2b");
  context.Reset();

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920 Unknown/Zone", 35),
            0);
  EXPECT_EQ(context.get_error(),
            "Invalid timestamp or unknown zone for timestamp value 2000-09-23 "
            "9:45:30.920 Unknown/Zone");
  context.Reset();
}

#endif

TEST(TestTime, TestExtractTime) {
  // 10:20:33
  gdv_int32 time_as_millis_in_day = 37233000;

  EXPECT_EQ(extractHour_time32(time_as_millis_in_day), 10);
  EXPECT_EQ(extractMinute_time32(time_as_millis_in_day), 20);
  EXPECT_EQ(extractSecond_time32(time_as_millis_in_day), 33);
}

TEST(TestTime, TestDateDiff) {
  gdv_timestamp ts1 = StringToTimestamp("2019-06-30 00:00:00");
  gdv_timestamp ts2 = StringToTimestamp("2019-05-31 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), 30);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-02-28 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), 122);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-03-31 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), 91);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-06-30 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), 0);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-01 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), -1);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-31 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), -31);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-30 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), -30);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-29 00:00:00");
  EXPECT_EQ(datediff_timestamp_timestamp(ts1, ts2), -29);
}

TEST(TestTime, TestTimestampDiffMonth) {
  gdv_timestamp ts1 = StringToTimestamp("2019-06-30 00:00:00");
  gdv_timestamp ts2 = StringToTimestamp("2019-05-31 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), -1);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-02-28 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), -4);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-03-31 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), -3);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-06-30 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), 0);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-31 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), 1);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-30 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), 1);

  ts1 = StringToTimestamp("2019-06-30 00:00:00");
  ts2 = StringToTimestamp("2019-07-29 00:00:00");
  EXPECT_EQ(timestampdiffMonth_timestamp_timestamp(ts1, ts2), 0);
}

TEST(TestTime, TestExtractTimestamp) {
  gdv_timestamp ts = StringToTimestamp("1970-05-02 10:20:33");

  EXPECT_EQ(extractMillennium_timestamp(ts), 2);
  EXPECT_EQ(extractCentury_timestamp(ts), 20);
  EXPECT_EQ(extractDecade_timestamp(ts), 197);
  EXPECT_EQ(extractYear_timestamp(ts), 1970);
  EXPECT_EQ(extractDoy_timestamp(ts), 122);
  EXPECT_EQ(extractMonth_timestamp(ts), 5);
  EXPECT_EQ(extractDow_timestamp(ts), 7);
  EXPECT_EQ(extractDay_timestamp(ts), 2);
  EXPECT_EQ(extractHour_timestamp(ts), 10);
  EXPECT_EQ(extractMinute_timestamp(ts), 20);
  EXPECT_EQ(extractSecond_timestamp(ts), 33);
}

TEST(TestTime, TimeStampTrunc) {
  EXPECT_EQ(date_trunc_Second_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-05-05 10:20:34"));
  EXPECT_EQ(date_trunc_Minute_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-05-05 10:20:00"));
  EXPECT_EQ(date_trunc_Hour_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-05-05 10:00:00"));
  EXPECT_EQ(date_trunc_Day_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-05-05 00:00:00"));
  EXPECT_EQ(date_trunc_Month_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-05-01 00:00:00"));
  EXPECT_EQ(date_trunc_Quarter_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-04-01 00:00:00"));
  EXPECT_EQ(date_trunc_Year_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2015-01-01 00:00:00"));
  EXPECT_EQ(date_trunc_Decade_date64(StringToTimestamp("2015-05-05 10:20:34")),
            StringToTimestamp("2010-01-01 00:00:00"));
  EXPECT_EQ(date_trunc_Century_date64(StringToTimestamp("2115-05-05 10:20:34")),
            StringToTimestamp("2101-01-01 00:00:00"));
  EXPECT_EQ(date_trunc_Millennium_date64(StringToTimestamp("2115-05-05 10:20:34")),
            StringToTimestamp("2001-01-01 00:00:00"));

  // truncate week going to previous year
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-01 10:10:10")),
            StringToTimestamp("2010-12-27 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-02 10:10:10")),
            StringToTimestamp("2010-12-27 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-03 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-04 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-05 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-06 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-07 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-08 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2011-01-09 10:10:10")),
            StringToTimestamp("2011-01-03 00:00:00"));

  // truncate week for Feb in a leap year
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-02-28 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-02-29 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-01 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-02 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-03 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-04 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-05 10:10:10")),
            StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(date_trunc_Week_timestamp(StringToTimestamp("2000-03-06 10:10:10")),
            StringToTimestamp("2000-03-06 00:00:00"));
}

TEST(TestTime, TimeStampAdd) {
  EXPECT_EQ(
      timestampaddSecond_int32_timestamp(30, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("2000-05-01 10:21:04"));

  EXPECT_EQ(
      timestampaddSecond_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 30),
      StringToTimestamp("2000-05-01 10:21:04"));

  EXPECT_EQ(
      timestampaddMinute_int64_timestamp(-30, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("2000-05-01 09:50:34"));

  EXPECT_EQ(
      timestampaddMinute_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), -30),
      StringToTimestamp("2000-05-01 09:50:34"));

  EXPECT_EQ(
      timestampaddHour_int32_timestamp(20, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("2000-05-02 06:20:34"));

  EXPECT_EQ(
      timestampaddHour_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 20),
      StringToTimestamp("2000-05-02 06:20:34"));

  EXPECT_EQ(
      timestampaddDay_int64_timestamp(-35, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("2000-03-27 10:20:34"));

  EXPECT_EQ(
      timestampaddDay_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), -35),
      StringToTimestamp("2000-03-27 10:20:34"));

  EXPECT_EQ(timestampaddWeek_int32_timestamp(4, StringToTimestamp("2000-05-01 10:20:34")),
            StringToTimestamp("2000-05-29 10:20:34"));

  EXPECT_EQ(timestampaddWeek_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 4),
            StringToTimestamp("2000-05-29 10:20:34"));

  EXPECT_EQ(timestampaddWeek_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 4),
            StringToTimestamp("2000-05-29 10:20:34"));

  EXPECT_EQ(
      timestampaddMonth_int64_timestamp(10, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("2001-03-01 10:20:34"));

  EXPECT_EQ(
      timestampaddMonth_int64_timestamp(1, StringToTimestamp("2000-01-31 10:20:34")),
      StringToTimestamp("2000-2-29 10:20:34"));
  EXPECT_EQ(
      timestampaddMonth_int64_timestamp(13, StringToTimestamp("2001-01-31 10:20:34")),
      StringToTimestamp("2002-02-28 10:20:34"));

  EXPECT_EQ(
      timestampaddMonth_int64_timestamp(11, StringToTimestamp("2000-05-31 10:20:34")),
      StringToTimestamp("2001-04-30 10:20:34"));

  EXPECT_EQ(
      timestampaddMonth_timestamp_int64(StringToTimestamp("2000-05-31 10:20:34"), 11),
      StringToTimestamp("2001-04-30 10:20:34"));

  EXPECT_EQ(
      timestampaddQuarter_int32_timestamp(-2, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("1999-11-01 10:20:34"));

  EXPECT_EQ(timestampaddYear_int64_timestamp(2, StringToTimestamp("2000-05-01 10:20:34")),
            StringToTimestamp("2002-05-01 10:20:34"));

  EXPECT_EQ(
      timestampaddQuarter_int32_timestamp(-5, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("1999-02-01 10:20:34"));
  EXPECT_EQ(
      timestampaddQuarter_int32_timestamp(-6, StringToTimestamp("2000-05-01 10:20:34")),
      StringToTimestamp("1998-11-01 10:20:34"));

  // date_add
  EXPECT_EQ(date_add_int32_timestamp(7, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-08 00:00:00"));

  EXPECT_EQ(add_int32_timestamp(4, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-05 00:00:00"));

  EXPECT_EQ(add_int64_timestamp(7, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-08 00:00:00"));

  EXPECT_EQ(date_add_int64_timestamp(4, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-05 00:00:00"));

  EXPECT_EQ(date_add_int64_timestamp(4, StringToTimestamp("2000-02-27 00:00:00")),
            StringToTimestamp("2000-03-02 00:00:00"));

  EXPECT_EQ(add_date64_int64(StringToTimestamp("2000-02-27 00:00:00"), 4),
            StringToTimestamp("2000-03-02 00:00:00"));

  // date_sub
  EXPECT_EQ(date_sub_timestamp_int32(StringToTimestamp("2000-05-01 00:00:00"), 7),
            StringToTimestamp("2000-04-24 00:00:00"));

  EXPECT_EQ(subtract_timestamp_int32(StringToTimestamp("2000-05-01 00:00:00"), -7),
            StringToTimestamp("2000-05-08 00:00:00"));

  EXPECT_EQ(date_diff_timestamp_int64(StringToTimestamp("2000-05-01 00:00:00"), 365),
            StringToTimestamp("1999-05-02 00:00:00"));

  EXPECT_EQ(date_diff_timestamp_int64(StringToTimestamp("2000-03-01 00:00:00"), 1),
            StringToTimestamp("2000-02-29 00:00:00"));

  EXPECT_EQ(date_diff_timestamp_int64(StringToTimestamp("2000-02-29 00:00:00"), 365),
            StringToTimestamp("1999-03-01 00:00:00"));
}

// test cases from http://www.staff.science.uu.nl/~gent0113/calendar/isocalendar.htm
TEST(TestTime, TestExtractWeek) {
  std::vector<std::string> data;

  // A type
  // Jan 1, 2 and 3
  data.push_back("2006-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2006-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2006-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2006-04-24 10:10:10");
  data.push_back("17");
  data.push_back("2006-04-30 10:10:10");
  data.push_back("17");
  // Dec 29-31
  data.push_back("2006-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2006-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2006-12-31 10:10:10");
  data.push_back("52");
  // B(C) type
  // Jan 1, 2 and 3
  data.push_back("2011-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2011-01-02 10:10:10");
  data.push_back("52");
  data.push_back("2011-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2011-07-18 10:10:10");
  data.push_back("29");
  data.push_back("2011-07-24 10:10:10");
  data.push_back("29");
  // Dec 29-31
  data.push_back("2011-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2011-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2011-12-31 10:10:10");
  data.push_back("52");
  // B(DC) type
  // Jan 1, 2 and 3
  data.push_back("2005-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2005-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2005-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2005-11-07 10:10:10");
  data.push_back("45");
  data.push_back("2005-11-13 10:10:10");
  data.push_back("45");
  // Dec 29-31
  data.push_back("2005-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2005-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2005-12-31 10:10:10");
  data.push_back("52");
  // C type
  // Jan 1, 2 and 3
  data.push_back("2010-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2010-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2010-01-03 10:10:10");
  data.push_back("53");
  // middle, Monday and Sunday
  data.push_back("2010-09-13 10:10:10");
  data.push_back("37");
  data.push_back("2010-09-19 10:10:10");
  data.push_back("37");
  // Dec 29-31
  data.push_back("2010-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2010-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2010-12-31 10:10:10");
  data.push_back("52");
  // D type
  // Jan 1, 2 and 3
  data.push_back("2037-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2037-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2037-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2037-08-17 10:10:10");
  data.push_back("34");
  data.push_back("2037-08-23 10:10:10");
  data.push_back("34");
  // Dec 29-31
  data.push_back("2037-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2037-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2037-12-31 10:10:10");
  data.push_back("53");
  // E type
  // Jan 1, 2 and 3
  data.push_back("2014-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2014-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2014-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2014-01-13 10:10:10");
  data.push_back("3");
  data.push_back("2014-01-19 10:10:10");
  data.push_back("3");
  // Dec 29-31
  data.push_back("2014-12-29 10:10:10");
  data.push_back("1");
  data.push_back("2014-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2014-12-31 10:10:10");
  data.push_back("1");
  // F type
  // Jan 1, 2 and 3
  data.push_back("2019-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2019-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2019-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2019-02-11 10:10:10");
  data.push_back("7");
  data.push_back("2019-02-17 10:10:10");
  data.push_back("7");
  // Dec 29-31
  data.push_back("2019-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2019-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2019-12-31 10:10:10");
  data.push_back("1");
  // G type
  // Jan 1, 2 and 3
  data.push_back("2001-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2001-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2001-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2001-03-19 10:10:10");
  data.push_back("12");
  data.push_back("2001-03-25 10:10:10");
  data.push_back("12");
  // Dec 29-31
  data.push_back("2001-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2001-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2001-12-31 10:10:10");
  data.push_back("1");
  // AG type
  // Jan 1, 2 and 3
  data.push_back("2012-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2012-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2012-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2012-04-02 10:10:10");
  data.push_back("14");
  data.push_back("2012-04-08 10:10:10");
  data.push_back("14");
  // Dec 29-31
  data.push_back("2012-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2012-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2012-12-31 10:10:10");
  data.push_back("1");
  // BA type
  // Jan 1, 2 and 3
  data.push_back("2000-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2000-01-02 10:10:10");
  data.push_back("52");
  data.push_back("2000-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2000-05-22 10:10:10");
  data.push_back("21");
  data.push_back("2000-05-28 10:10:10");
  data.push_back("21");
  // Dec 29-31
  data.push_back("2000-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2000-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2000-12-31 10:10:10");
  data.push_back("52");
  // CB type
  // Jan 1, 2 and 3
  data.push_back("2016-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2016-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2016-01-03 10:10:10");
  data.push_back("53");
  // middle, Monday and Sunday
  data.push_back("2016-06-20 10:10:10");
  data.push_back("25");
  data.push_back("2016-06-26 10:10:10");
  data.push_back("25");
  // Dec 29-31
  data.push_back("2016-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2016-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2016-12-31 10:10:10");
  data.push_back("52");
  // DC type
  // Jan 1, 2 and 3
  data.push_back("2004-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2004-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2004-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2004-07-19 10:10:10");
  data.push_back("30");
  data.push_back("2004-07-25 10:10:10");
  data.push_back("30");
  // Dec 29-31
  data.push_back("2004-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2004-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2004-12-31 10:10:10");
  data.push_back("53");
  // ED type
  // Jan 1, 2 and 3
  data.push_back("2020-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2020-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2020-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2020-08-17 10:10:10");
  data.push_back("34");
  data.push_back("2020-08-23 10:10:10");
  data.push_back("34");
  // Dec 29-31
  data.push_back("2020-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2020-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2020-12-31 10:10:10");
  data.push_back("53");
  // FE type
  // Jan 1, 2 and 3
  data.push_back("2008-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2008-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2008-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2008-09-15 10:10:10");
  data.push_back("38");
  data.push_back("2008-09-21 10:10:10");
  data.push_back("38");
  // Dec 29-31
  data.push_back("2008-12-29 10:10:10");
  data.push_back("1");
  data.push_back("2008-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2008-12-31 10:10:10");
  data.push_back("1");
  // GF type
  // Jan 1, 2 and 3
  data.push_back("2024-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2024-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2024-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2024-10-07 10:10:10");
  data.push_back("41");
  data.push_back("2024-10-13 10:10:10");
  data.push_back("41");
  // Dec 29-31
  data.push_back("2024-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2024-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2024-12-31 10:10:10");
  data.push_back("1");

  for (uint32_t i = 0; i < data.size(); i += 2) {
    gdv_timestamp ts = StringToTimestamp(data.at(i).c_str());
    gdv_int64 exp = atol(data.at(i + 1).c_str());
    EXPECT_EQ(extractWeek_timestamp(ts), exp);
  }
}

TEST(TestTime, TestIsNullInterval) {
  EXPECT_EQ(isnull_day_time_interval(150, true), false);
  EXPECT_EQ(isnull_day_time_interval(150, false), true);
}

TEST(TestTime, TestMonthsBetween) {
  std::vector<std::string> testStrings = {
      "1995-03-02 00:00:00", "1995-02-02 00:00:00", "1.0",
      "1995-02-02 00:00:00", "1995-03-02 00:00:00", "-1.0",
      "1995-03-31 00:00:00", "1995-02-28 00:00:00", "1.0",
      "1996-03-31 00:00:00", "1996-02-28 00:00:00", "1.09677418",
      "1996-03-31 00:00:00", "1996-02-29 00:00:00", "1.0",
      "1996-05-31 00:00:00", "1996-04-30 00:00:00", "1.0",
      "1996-05-31 00:00:00", "1996-03-31 00:00:00", "2.0",
      "1996-05-31 00:00:00", "1996-03-30 00:00:00", "2.03225806",
      "1996-03-15 00:00:00", "1996-02-14 00:00:00", "1.03225806",
      "1995-02-02 00:00:00", "1995-01-01 00:00:00", "1.03225806",
      "1995-02-02 10:00:00", "1995-01-01 11:00:00", "1.03091397"};

  for (uint32_t i = 0; i < testStrings.size();) {
    gdv_timestamp endTs = StringToTimestamp(testStrings[i++].c_str());
    gdv_timestamp startTs = StringToTimestamp(testStrings[i++].c_str());

    double expectedResult = atof(testStrings[i++].c_str());
    double actualResult = months_between_timestamp_timestamp(endTs, startTs);

    double diff = actualResult - expectedResult;
    if (diff < 0) {
      diff = expectedResult - actualResult;
    }

    EXPECT_TRUE(diff < 0.001);
  }
}

TEST(TestTime, castVarcharTimestamp) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);
  gdv_int32 out_len;
  gdv_timestamp ts = StringToTimestamp("2000-05-01 10:20:34");
  const char* out = castVARCHAR_timestamp_int64(context_ptr, ts, 30L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "2000-05-01 10:20:34.000");

  out = castVARCHAR_timestamp_int64(context_ptr, ts, 19L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "2000-05-01 10:20:34");

  out = castVARCHAR_timestamp_int64(context_ptr, ts, 0L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");

  ts = StringToTimestamp("2-5-1 00:00:04");
  out = castVARCHAR_timestamp_int64(context_ptr, ts, 24L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "0002-05-01 00:00:04.000");
}

TEST(TestTime, TestCastTimestampToDate) {
  gdv_timestamp ts = StringToTimestamp("2000-05-01 10:20:34");
  auto out = castDATE_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2000-05-01 00:00:00"), out);
}

TEST(TestTime, TestNextDay) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  gdv_timestamp ts = StringToTimestamp("2021-11-08 10:20:34");
  auto out = next_day_from_timestamp(context_ptr, ts, "FR", 2);
  EXPECT_EQ(StringToTimestamp("2021-11-12 00:00:00"), out);

  out = next_day_from_timestamp(context_ptr, ts, "FRI", 3);
  EXPECT_EQ(StringToTimestamp("2021-11-12 00:00:00"), out);

  out = next_day_from_timestamp(context_ptr, ts, "FRIDAY", 6);
  EXPECT_EQ(StringToTimestamp("2021-11-12 00:00:00"), out);

  ts = StringToTimestamp("2015-08-06 11:12:30");
  out = next_day_from_timestamp(context_ptr, ts, "THU", 3);
  EXPECT_EQ(StringToTimestamp("2015-08-13 00:00:00"), out);

  ts = StringToTimestamp("2012-08-14 11:12:30");
  out = next_day_from_timestamp(context_ptr, ts, "TUE", 3);
  EXPECT_EQ(StringToTimestamp("2012-08-21 00:00:00"), out);

  ts = StringToTimestamp("2012-12-12 12:00:00");
  out = next_day_from_timestamp(context_ptr, ts, "TU", 2);
  EXPECT_EQ(StringToTimestamp("2012-12-18 00:00:00"), out);

  ts = StringToTimestamp("2000-01-01 20:15:00");
  out = next_day_from_timestamp(context_ptr, ts, "SATURDAY", 8);
  EXPECT_EQ(StringToTimestamp("2000-01-08 00:00:00"), out);

  ts = StringToTimestamp("2015-08-06 11:12:30");
  out = next_day_from_timestamp(context_ptr, ts, "AHSRK", 5);
  EXPECT_EQ(context.get_error(), "The weekday in this entry is invalid");
  context.Reset();
}

TEST(TestTime, TestCastTimestampToTime) {
  gdv_timestamp ts = StringToTimestamp("2000-05-01 10:20:34");
  auto expected_response =
      static_cast<int32_t>(ts - StringToTimestamp("2000-05-01 00:00:00"));
  auto out = castTIME_timestamp(ts);
  EXPECT_EQ(expected_response, out);

  // Test when the defined value is midnight, so the returned value must 0
  ts = StringToTimestamp("1998-12-01 00:00:00");
  expected_response = 0;
  out = castTIME_timestamp(ts);
  EXPECT_EQ(expected_response, out);

  ts = StringToTimestamp("2015-09-16 23:59:59");
  expected_response = static_cast<int32_t>(ts - StringToTimestamp("2015-09-16 00:00:00"));
  out = castTIME_timestamp(ts);
  EXPECT_EQ(expected_response, out);
}

TEST(TestTime, TestIntToTime) {
  int32_t val = 1000;
  int32_t expected_response = val;
  auto out = castTIME_int32(val);
  EXPECT_EQ(expected_response, out);

  val = MILLIS_IN_DAY - 1;
  expected_response = val;
  out = castTIME_int32(val);
  EXPECT_EQ(expected_response, out);

  val = MILLIS_IN_DAY + 1;
  expected_response = 1;
  out = castTIME_int32(val);
  EXPECT_EQ(expected_response, out);

  val = -1;
  expected_response = 0;
  out = castTIME_int32(val);
  EXPECT_EQ(expected_response, out);
}

TEST(TestTime, TestLastDay) {
  // leap year test
  gdv_timestamp ts = StringToTimestamp("2016-02-11 03:20:34");
  auto out = last_day_from_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2016-02-29 00:00:00"), out);

  ts = StringToTimestamp("2016-02-29 23:59:59");
  out = last_day_from_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2016-02-29 00:00:00"), out);

  ts = StringToTimestamp("2016-01-30 23:59:00");
  out = last_day_from_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2016-01-31 00:00:00"), out);

  // normal year
  ts = StringToTimestamp("2017-02-03 23:59:59");
  out = last_day_from_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2017-02-28 00:00:00"), out);

  // december
  ts = StringToTimestamp("2015-12-03 03:12:59");
  out = last_day_from_timestamp(ts);
  EXPECT_EQ(StringToTimestamp("2015-12-31 00:00:00"), out);
}

TEST(TestTime, TestToTimestamp) {
  auto ts = StringToTimestamp("1970-01-01 00:00:00");
  EXPECT_EQ(ts, to_timestamp_int32(0));
  EXPECT_EQ(ts, to_timestamp_int64(0));
  EXPECT_EQ(ts, to_timestamp_float32(0));
  EXPECT_EQ(ts, to_timestamp_float64(0));

  ts = StringToTimestamp("1970-01-01 00:00:01");
  EXPECT_EQ(ts, to_timestamp_int32(1));
  EXPECT_EQ(ts, to_timestamp_int64(1));
  EXPECT_EQ(ts, to_timestamp_float32(1));
  EXPECT_EQ(ts, to_timestamp_float64(1));

  ts = StringToTimestamp("2021-07-14 09:31:39");
  EXPECT_EQ(ts, to_timestamp_int32(1626255099));
  EXPECT_EQ(ts, to_timestamp_int64(1626255099));

  ts = StringToTimestamp("1970-01-01 00:01:00");
  EXPECT_EQ(ts, to_timestamp_int32(60));
  EXPECT_EQ(ts, to_timestamp_int64(60));
  EXPECT_EQ(ts, to_timestamp_float32(60));
  EXPECT_EQ(ts, to_timestamp_float64(60));

  ts = StringToTimestamp("1970-01-01 01:00:00");
  EXPECT_EQ(ts, to_timestamp_int32(3600));
  EXPECT_EQ(ts, to_timestamp_int64(3600));
  EXPECT_EQ(ts, to_timestamp_float32(3600));
  EXPECT_EQ(ts, to_timestamp_float64(3600));

  ts = StringToTimestamp("1970-01-02 00:00:00");
  EXPECT_EQ(ts, to_timestamp_int32(86400));
  EXPECT_EQ(ts, to_timestamp_int64(86400));
  EXPECT_EQ(ts, to_timestamp_float32(86400));
  EXPECT_EQ(ts, to_timestamp_float64(86400));

  // tests with fractional part
  ts = StringToTimestamp("1970-01-01 00:00:01") + 500;
  EXPECT_EQ(ts, to_timestamp_float32(1.500f));
  EXPECT_EQ(ts, to_timestamp_float64(1.500));

  ts = StringToTimestamp("1970-01-01 00:01:01") + 600;
  EXPECT_EQ(ts, to_timestamp_float32(61.600f));
  EXPECT_EQ(ts, to_timestamp_float64(61.600));

  ts = StringToTimestamp("1970-01-01 01:00:01") + 400;
  EXPECT_EQ(ts, to_timestamp_float32(3601.400f));
  EXPECT_EQ(ts, to_timestamp_float64(3601.400));
}

TEST(TestTime, TestToTimeNumeric) {
  // input timestamp in seconds: 1970-01-01 00:00:00
  int64_t expected_output = 0;  // 0 milliseconds
  EXPECT_EQ(expected_output, to_time_int32(0));
  EXPECT_EQ(expected_output, to_time_int64(0));
  EXPECT_EQ(expected_output, to_time_float32(0.000f));
  EXPECT_EQ(expected_output, to_time_float64(0.000));

  // input timestamp in seconds: 1970-01-01 00:00:01
  expected_output = 1000;  // 1 seconds
  EXPECT_EQ(expected_output, to_time_int32(1));
  EXPECT_EQ(expected_output, to_time_int64(1));
  EXPECT_EQ(expected_output, to_time_float32(1.000f));
  EXPECT_EQ(expected_output, to_time_float64(1.000));

  // input timestamp in seconds: 1970-01-01 01:00:00
  expected_output = 3600000;  // 3600 seconds
  EXPECT_EQ(expected_output, to_time_int32(3600));
  EXPECT_EQ(expected_output, to_time_int64(3600));
  EXPECT_EQ(expected_output, to_time_float32(3600.000f));
  EXPECT_EQ(expected_output, to_time_float64(3600.000));

  // input timestamp in seconds: 1970-01-01 23:59:59
  expected_output = 86399000;  // 86399 seconds
  EXPECT_EQ(expected_output, to_time_int32(86399));
  EXPECT_EQ(expected_output, to_time_int64(86399));
  EXPECT_EQ(expected_output, to_time_float32(86399.000f));
  EXPECT_EQ(expected_output, to_time_float64(86399.000));

  // input timestamp in seconds: 2020-01-01 00:00:01
  expected_output = 1000;  // 1 second
  EXPECT_EQ(expected_output, to_time_int64(1577836801));
  EXPECT_EQ(expected_output, to_time_float64(1577836801.000));

  // tests with fractional part
  // input timestamp in seconds: 1970-01-01 00:00:01.500
  expected_output = 1500;  // 1.5 seconds
  EXPECT_EQ(expected_output, to_time_float32(1.500f));
  EXPECT_EQ(expected_output, to_time_float64(1.500));

  // input timestamp in seconds: 1970-01-01 00:01:01.500
  expected_output = 61500;  // 61.5 seconds
  EXPECT_EQ(expected_output, to_time_float32(61.500f));
  EXPECT_EQ(expected_output, to_time_float64(61.500));

  // input timestamp in seconds: 1970-01-01 01:00:01.500
  expected_output = 3601500;  // 3601.5 seconds
  EXPECT_EQ(expected_output, to_time_float32(3601.500f));
  EXPECT_EQ(expected_output, to_time_float64(3601.500));
}

TEST(TestTime, TestCastIntDayInterval) {
  EXPECT_EQ(castBIGINT_daytimeinterval(10), 864000000);
  EXPECT_EQ(castBIGINT_daytimeinterval(-100), -8640000001);
  EXPECT_EQ(castBIGINT_daytimeinterval(-0), 0);
}

TEST(TestTime, TestCastIntYearInterval) {
  EXPECT_EQ(castINT_year_interval(24), 2);
  EXPECT_EQ(castINT_year_interval(-24), -2);
  EXPECT_EQ(castINT_year_interval(-23), -1);

  EXPECT_EQ(castBIGINT_year_interval(24), 2);
  EXPECT_EQ(castBIGINT_year_interval(-24), -2);
  EXPECT_EQ(castBIGINT_year_interval(-23), -1);
}

TEST(TestTime, TestCastNullableInterval) {
  ExecutionContext context;
  auto context_ptr = reinterpret_cast<int64_t>(&context);
  // Test castNULLABLEINTERVALDAY for int and bigint
  EXPECT_EQ(castNULLABLEINTERVALDAY_int32(1), 1);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int32(12), 12);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int32(-55), -55);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int32(-1201), -1201);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int64(1), 1);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int64(12), 12);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int64(-55), -55);
  EXPECT_EQ(castNULLABLEINTERVALDAY_int64(-1201), -1201);

  // Test castNULLABLEINTERVALYEAR for int and bigint
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int32(context_ptr, 1), 1);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int32(context_ptr, 12), 12);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int32(context_ptr, 55), 55);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int32(context_ptr, 1201), 1201);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int64(context_ptr, 1), 1);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int64(context_ptr, 12), 12);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int64(context_ptr, 55), 55);
  EXPECT_EQ(castNULLABLEINTERVALYEAR_int64(context_ptr, 1201), 1201);
  // validate overflow error when using bigint as input
  castNULLABLEINTERVALYEAR_int64(context_ptr, INT64_MAX);
  EXPECT_EQ(context.get_error(), "Integer overflow");
  context.Reset();
}

}  // namespace gandiva

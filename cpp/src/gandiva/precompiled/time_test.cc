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
#include <time.h>
#include "../execution_context.h"
#include "gandiva/precompiled/testing.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestTime, TestCastDate) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castDATE_utf8(context_ptr, "1967-12-1", 9), -65836800000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "1972-12-1", 9), 92016000000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "1972222222", 10), 0);
  EXPECT_EQ(context.get_error(), "Not a valid date value 1972222222");
  context.Reset();

  EXPECT_EQ(castDATE_utf8(context_ptr, "blahblah", 8), 0);
  EXPECT_EQ(castDATE_utf8(context_ptr, "1967-12-1bb", 11), -65836800000);

  EXPECT_EQ(castDATE_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "67-1-1", 7), 3061065600000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-1-1", 7), 31536000000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-45-1", 7), 0);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-12-XX", 8), 0);
}

TEST(TestTime, TestCastTimestamp) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1967-12-1", 9), -65836800000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1972-12-1", 9), 92016000000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "67-12-1", 7), 3089923200000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "67-1-1", 7), 3061065600000);
  EXPECT_EQ(castDATE_utf8(context_ptr, "71-1-1", 7), 31536000000);

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

  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2000-09-23 9:45:30.920 Canada/Pacific", 37),
            969727530920);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "2012-02-28 23:30:59 Asia/Kolkata", 32),
            1330452059000);
  EXPECT_EQ(castTIMESTAMP_utf8(context_ptr, "1923-10-07 03:03:03 America/New_York", 36),
            -1459094217000);

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

TEST(TestTime, TestExtractTime) {
  // 10:20:33
  int32 time_as_millis_in_day = 37233000;

  EXPECT_EQ(extractHour_time32(time_as_millis_in_day), 10);
  EXPECT_EQ(extractMinute_time32(time_as_millis_in_day), 20);
  EXPECT_EQ(extractSecond_time32(time_as_millis_in_day), 33);
}

TEST(TestTime, TestExtractTimestamp) {
  timestamp ts = StringToTimestamp("1970-05-02 10:20:33");

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
      timestampaddSecond_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 30),
      StringToTimestamp("2000-05-01 10:21:04"));

  EXPECT_EQ(
      timestampaddMinute_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), -30),
      StringToTimestamp("2000-05-01 09:50:34"));

  EXPECT_EQ(
      timestampaddHour_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 20),
      StringToTimestamp("2000-05-02 06:20:34"));

  EXPECT_EQ(
      timestampaddDay_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), -35),
      StringToTimestamp("2000-03-27 10:20:34"));

  EXPECT_EQ(timestampaddWeek_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), 4),
            StringToTimestamp("2000-05-29 10:20:34"));

  EXPECT_EQ(
      timestampaddMonth_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), 10),
      StringToTimestamp("2001-03-01 10:20:34"));

  EXPECT_EQ(
      timestampaddQuarter_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), -2),
      StringToTimestamp("1999-11-01 10:20:34"));

  EXPECT_EQ(timestampaddYear_timestamp_int64(StringToTimestamp("2000-05-01 10:20:34"), 2),
            StringToTimestamp("2002-05-01 10:20:34"));

  EXPECT_EQ(
      timestampaddQuarter_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), -5),
      StringToTimestamp("1999-02-01 10:20:34"));
  EXPECT_EQ(
      timestampaddQuarter_timestamp_int32(StringToTimestamp("2000-05-01 10:20:34"), -6),
      StringToTimestamp("1998-11-01 10:20:34"));

  // date_add
  EXPECT_EQ(date_add_timestamp_int32(StringToTimestamp("2000-05-01 00:00:00"), 7),
            StringToTimestamp("2000-05-08 00:00:00"));

  EXPECT_EQ(add_int32_timestamp(4, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-05 00:00:00"));

  EXPECT_EQ(add_timestamp_int64(StringToTimestamp("2000-05-01 00:00:00"), 7),
            StringToTimestamp("2000-05-08 00:00:00"));

  EXPECT_EQ(date_add_int64_timestamp(4, StringToTimestamp("2000-05-01 00:00:00")),
            StringToTimestamp("2000-05-05 00:00:00"));

  EXPECT_EQ(date_add_int64_timestamp(4, StringToTimestamp("2000-02-27 00:00:00")),
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
    timestamp ts = StringToTimestamp(data.at(i).c_str());
    int64 exp = atol(data.at(i + 1).c_str());
    EXPECT_EQ(extractWeek_timestamp(ts), exp);
  }
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
    timestamp endTs = StringToTimestamp(testStrings[i++].c_str());
    timestamp startTs = StringToTimestamp(testStrings[i++].c_str());

    double expectedResult = atof(testStrings[i++].c_str());
    double actualResult = months_between_timestamp_timestamp(endTs, startTs);

    double diff = actualResult - expectedResult;
    if (diff < 0) {
      diff = expectedResult - actualResult;
    }

    EXPECT_TRUE(diff < 0.001);
  }
}

}  // namespace gandiva

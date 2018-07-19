// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <time.h>

#include <gtest/gtest.h>
#include "precompiled/types.h"

namespace gandiva {

timestamp StringToTimestamp(const char *buf) {
  struct tm tm;
  strptime(buf, "%Y-%m-%d %H:%M:%S", &tm);
  return timegm(&tm) * 1000;  // to millis
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

}  // namespace gandiva

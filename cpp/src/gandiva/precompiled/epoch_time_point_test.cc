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

#include <ctime>

#include <gtest/gtest.h>
#include "./epoch_time_point.h"
#include "gandiva/precompiled/testing.h"
#include "gandiva/precompiled/types.h"

#include "gandiva/date_utils.h"

namespace gandiva {

TEST(TestEpochTimePoint, TestTm) {
  auto ts = StringToTimestamp("2015-05-07 10:20:34");
  EpochTimePoint tp(ts);

  struct tm* tm_ptr;
#if defined(_MSC_VER)
  __time64_t tsec = ts / 1000;
  tm_ptr = _gmtime64(&tsec);
#else
  struct tm tm;
  time_t tsec = ts / 1000;
  tm_ptr = gmtime_r(&tsec, &tm);
#endif

  EXPECT_EQ(tp.TmYear(), tm_ptr->tm_year);
  EXPECT_EQ(tp.TmMon(), tm_ptr->tm_mon);
  EXPECT_EQ(tp.TmYday(), tm_ptr->tm_yday);
  EXPECT_EQ(tp.TmMday(), tm_ptr->tm_mday);
  EXPECT_EQ(tp.TmWday(), tm_ptr->tm_wday);
  EXPECT_EQ(tp.TmHour(), tm_ptr->tm_hour);
  EXPECT_EQ(tp.TmMin(), tm_ptr->tm_min);
  EXPECT_EQ(tp.TmSec(), tm_ptr->tm_sec);
}

TEST(TestEpochTimePoint, TestAddYears) {
  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddYears(2),
            EpochTimePoint(StringToTimestamp("2017-05-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddYears(0),
            EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddYears(-1),
            EpochTimePoint(StringToTimestamp("2014-05-05 10:20:34")));
}

TEST(TestEpochTimePoint, TestAddMonths) {
  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddMonths(2),
            EpochTimePoint(StringToTimestamp("2015-07-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddMonths(11),
            EpochTimePoint(StringToTimestamp("2016-04-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddMonths(0),
            EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddMonths(-1),
            EpochTimePoint(StringToTimestamp("2015-04-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddMonths(-10),
            EpochTimePoint(StringToTimestamp("2014-07-05 10:20:34")));
}

TEST(TestEpochTimePoint, TestAddDays) {
  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddDays(2),
            EpochTimePoint(StringToTimestamp("2015-05-07 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddDays(11),
            EpochTimePoint(StringToTimestamp("2015-05-16 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddDays(0),
            EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddDays(-1),
            EpochTimePoint(StringToTimestamp("2015-05-04 10:20:34")));

  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).AddDays(-10),
            EpochTimePoint(StringToTimestamp("2015-04-25 10:20:34")));
}

TEST(TestEpochTimePoint, TestClearTimeOfDay) {
  EXPECT_EQ(EpochTimePoint(StringToTimestamp("2015-05-05 10:20:34")).ClearTimeOfDay(),
            EpochTimePoint(StringToTimestamp("2015-05-05 00:00:00")));
}

}  // namespace gandiva

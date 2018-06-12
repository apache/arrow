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
  return timegm(&tm) * 1000; // to millis
}

TEST(TestTime, TestExtractTimestamp) {
  timestamp ts = StringToTimestamp("1970-05-02 10:20:33");

  EXPECT_EQ(extractYear_timestamp(ts), 1970);
  EXPECT_EQ(extractMonth_timestamp(ts), 5);
  EXPECT_EQ(extractDay_timestamp(ts), 2);
  EXPECT_EQ(extractHour_timestamp(ts), 10);
  EXPECT_EQ(extractMinute_timestamp(ts), 20);
}

} // namespace gandiva

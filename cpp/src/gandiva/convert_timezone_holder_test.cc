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

#include "gandiva/convert_timezone_holder.h"

#include <gtest/gtest.h>
#include "gandiva/precompiled/testing.h"

#include <memory>
#include <vector>

namespace gandiva {
class TestConvertTimezone : public ::testing::Test {
 public:
  FunctionNode BuildConvert(std::string srcTz, std::string dstTz) {
    auto field = std::make_shared<FieldNode>(arrow::field("times", arrow::int64()));
    auto srcTz_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(srcTz), false);
    auto dstTz_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(dstTz), false);
    return FunctionNode("convert_timezone", {field, srcTz_node, dstTz_node},
                        arrow::int64());
  }
};

TEST_F(TestConvertTimezone, TestConvertTimezoneName) {
  std::shared_ptr<ConvertTimezoneHolder> convert_holder;

  auto status =
      ConvertTimezoneHolder::Make("Canada/Pacific", "Asia/Kolkata", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-01 08:29:00")),
            StringToTimestamp("2016-02-01 21:59:00"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 08:29:00")),
            StringToTimestamp("2016-10-01 20:59:00"));  // Checks if it considers Daylight
                                                        // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-28 23:59:59")),
            StringToTimestamp("2016-02-29 13:29:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-02-28 23:59:59")),
            StringToTimestamp("2015-03-01 13:29:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("1969-01-01 08:29:00")),
            StringToTimestamp("1969-01-01 21:59:00"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("1950-10-01 08:29:00")),
            StringToTimestamp("1950-10-01 21:59:00"));  // Checks if it considers Daylight
                                                        // saving time periods.
}

TEST_F(TestConvertTimezone, TestConvertTimezoneAbbreviations) {
  std::shared_ptr<ConvertTimezoneHolder> convert_holder;

  auto status = ConvertTimezoneHolder::Make("PST", "LKT", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-01 08:29:00")),
            StringToTimestamp("2016-02-01 21:59:00"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 08:29:00")),
            StringToTimestamp("2016-10-01 21:59:00"));  // Checks if it considers Daylight
  // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-28 23:59:59")),
            StringToTimestamp("2016-02-29 13:29:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-02-28 23:59:59")),
            StringToTimestamp("2015-03-01 13:29:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("1969-01-01 08:29:00")),
            StringToTimestamp("1969-01-01 21:59:00"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("1950-10-01 08:29:00")),
            StringToTimestamp("1950-10-01 21:59:00"));  // Checks if it considers Daylight
  // saving time periods.
}

TEST_F(TestConvertTimezone, TestUnknownTimezones) {
  std::shared_ptr<ConvertTimezoneHolder> convert_holder;

  auto status = ConvertTimezoneHolder::Make("PSF", "LKT", &convert_holder);

  EXPECT_FALSE(status.ok());
  EXPECT_STREQ(status.message().c_str(),
               "Couldn't find one of the timezones given or it's invalid.");

  status = ConvertTimezoneHolder::Make("-02:00", "-25:00", &convert_holder);

  EXPECT_FALSE(status.ok());
  EXPECT_STREQ(status.message().c_str(),
               "Couldn't find one of the timezones given or it's invalid.");

  status = ConvertTimezoneHolder::Make("UT-25", "UTC+2", &convert_holder);

  EXPECT_FALSE(status.ok());
  EXPECT_STREQ(status.message().c_str(),
               "Couldn't find one of the timezones given or it's invalid.");
}

TEST_F(TestConvertTimezone, TestConvertTimezoneOffset) {
  std::shared_ptr<ConvertTimezoneHolder> convert_holder;

  auto status = ConvertTimezoneHolder::Make("-01:00", "+02:00", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2020-06-03 21:33:20")),
            StringToTimestamp("2020-06-04 00:33:20"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 08:29:00")),
            StringToTimestamp("2016-10-01 11:29:00"));  // Checks if it considers Daylight
                                                        // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-28 23:59:59")),
            StringToTimestamp("2016-02-29 02:59:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-02-28 23:59:59")),
            StringToTimestamp("2015-03-01 02:59:59"));

  status = ConvertTimezoneHolder::Make("+02:00", "+01:00", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2020-06-03 22:33:20")),
            StringToTimestamp("2020-06-03 21:33:20"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 09:29:00")),
            StringToTimestamp("2016-10-01 08:29:00"));  // Checks if it considers Daylight
                                                        // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-29 00:59:59")),
            StringToTimestamp("2016-02-28 23:59:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-03-01 00:59:59")),
            StringToTimestamp("2015-02-28 23:59:59"));
}

TEST_F(TestConvertTimezone, TestConvertTimezoneShift) {
  std::shared_ptr<ConvertTimezoneHolder> convert_holder;

  auto status = ConvertTimezoneHolder::Make("UT-1", "UTC+2", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2020-06-03 21:33:20")),
            StringToTimestamp("2020-06-04 00:33:20"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 08:29:00")),
            StringToTimestamp("2016-10-01 11:29:00"));  // Checks if it considers Daylight
  // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-28 23:59:59")),
            StringToTimestamp("2016-02-29 02:59:59"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-02-28 23:59:59")),
            StringToTimestamp("2015-03-01 02:59:59"));

  status = ConvertTimezoneHolder::Make("GMT+08:45:15", "GMT-08:45:15", &convert_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2020-06-03 22:33:20")),
            StringToTimestamp("2020-06-03 05:02:50"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-10-01 09:29:00")),
            StringToTimestamp("2016-09-30 15:58:30"));  // Checks if it considers Daylight
  // saving time periods.
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2016-02-29 00:59:59")),
            StringToTimestamp("2016-02-28 07:29:29"));
  EXPECT_EQ(convert_holder->convert(StringToTimestamp("2015-03-01 00:59:59")),
            StringToTimestamp("2015-02-28 07:29:29"));
}
}  // namespace gandiva

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

#include "gandiva/from_date_functions_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "gandiva/execution_context.h"

namespace gandiva {

// Block of tests to test the holder for From Date functions
class TestFromDateHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestFromDateHolder, TestFromUnixtimeWithPattern) {
  std::shared_ptr<FromUnixtimeHolder> from_date_holder;
  ASSERT_OK(FromUnixtimeHolder::Make("YYYY-MM-DD HH24:MI:SS", 1, &from_date_holder));

  auto& from_unixtime = *from_date_holder;
  bool out_valid;

  int64_t unix_timestamp = 1275375502000;
  auto output_timestamp =
      from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-06-01 06:58:22");

  unix_timestamp = 585360187000;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1988-07-20 00:03:07");

  unix_timestamp = 2536258000;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 08:30:58");

  unix_timestamp = 0;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  unix_timestamp = -1;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");
}

TEST_F(TestFromDateHolder, TestFromUnixtimeWithoutPattern) {
  std::shared_ptr<FromUnixtimeHolder> from_date_holder;
  ASSERT_OK(FromUnixtimeHolder::Make("", 1, &from_date_holder));

  auto& from_unixtime = *from_date_holder;
  bool out_valid;

  int64_t unix_timestamp = 1275375502000;
  auto output_timestamp =
      from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-06-01 06:58:22");

  unix_timestamp = 585360187000;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1988-07-20 00:03:07");

  unix_timestamp = 2536258000;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 08:30:58");

  unix_timestamp = 0;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  unix_timestamp = -1;
  output_timestamp = from_unixtime(&execution_context_, unix_timestamp, true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");
}

TEST_F(TestFromDateHolder, TestFromUtcTimestampUtf8) {
  std::shared_ptr<FromUtcTimestampUtf8Holder> from_date_holder;
  ASSERT_OK(FromUtcTimestampUtf8Holder::Make(1, &from_date_holder));

  auto& from_utc_timestamp = *from_date_holder;
  bool out_valid;
  const char* date = "2010-06-01 06:58:22";
  auto output_timestamp =
      from_utc_timestamp(&execution_context_, date, 19, "PST8PDT", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-05-31 23:58:22");

  date = "1970-01-30 08:30:58";
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, 19, "Etc/GMT+8", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 00:30:58");
}

TEST_F(TestFromDateHolder, TestFromUtcTimestampInt32) {
  std::shared_ptr<FromUtcTimestampInt32Holder> from_date_holder;
  ASSERT_OK(FromUtcTimestampInt32Holder::Make(1, &from_date_holder));

  auto& from_utc_timestamp = *from_date_holder;
  bool out_valid;

  int32_t date = 1275375502;
  auto output_timestamp =
      from_utc_timestamp(&execution_context_, date, "PST8PDT", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-05-31 23:58:22");

  date = 2536258;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+8", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 00:30:58");

  date = 0;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+0", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  date = -1;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+1", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1969-12-31 22:59:59");
}

TEST_F(TestFromDateHolder, TestFromUtcTimestampInt64) {
  std::shared_ptr<FromUtcTimestampInt64Holder> from_date_holder;
  ASSERT_OK(FromUtcTimestampInt64Holder::Make(1, &from_date_holder));

  auto& from_utc_timestamp = *from_date_holder;
  bool out_valid;

  int64_t date = 1275375502000;
  auto output_timestamp =
      from_utc_timestamp(&execution_context_, date, "PST8PDT", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-05-31 23:58:22");

  date = 2536258000;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+8", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 00:30:58");

  date = 0;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+0", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  date = -1;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+1", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1969-12-31 22:59:59");
}

TEST_F(TestFromDateHolder, TestFromUtcTimestampFloat32) {
  std::shared_ptr<FromUtcTimestampFloat32Holder> from_date_holder;
  ASSERT_OK(FromUtcTimestampFloat32Holder::Make(1, &from_date_holder));

  auto& from_utc_timestamp = *from_date_holder;
  bool out_valid;

  float date = 1275375502.0f;
  auto output_timestamp =
      from_utc_timestamp(&execution_context_, date, "PST8PDT", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-05-31 23:58:08");

  date = 2536258.0f;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+8", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 00:30:58");

  date = 0.0f;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+0", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  date = -1.0f;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+1", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1969-12-31 22:59:59");
}

TEST_F(TestFromDateHolder, TestFromUtcTimestampFloat64) {
  std::shared_ptr<FromUtcTimestampFloat64Holder> from_date_holder;
  ASSERT_OK(FromUtcTimestampFloat64Holder::Make(1, &from_date_holder));

  auto& from_utc_timestamp = *from_date_holder;
  bool out_valid;

  double date = 1275375502.0;
  auto output_timestamp =
      from_utc_timestamp(&execution_context_, date, "PST8PDT", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "2010-05-31 23:58:22");

  date = 2536258.0;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+8", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-30 00:30:58");

  date = 0.0;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+0", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1970-01-01 00:00:00");

  date = -1.0;
  output_timestamp =
      from_utc_timestamp(&execution_context_, date, "Etc/GMT+1", true, &out_valid);
  EXPECT_EQ(std::string(output_timestamp, strlen(output_timestamp)),
            "1969-12-31 22:59:59");
}

}  // namespace gandiva

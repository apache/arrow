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

#include <memory>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "gandiva/execution_context.h"
#include "gandiva/to_date_functions_holder.h"

namespace gandiva {

class TestUnixTimestampHolder : public ::testing::Test {
 public:
  FunctionNode BuildUnixTimestampNode() {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    return FunctionNode("unix_timestamp_utf8", {field}, arrow::int64());
  }

 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestUnixTimestampHolder, TestSimpleDateTime) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS", 1, &unix_timestamp_holder));

  auto& to_date = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861);

  s = std::string(" ");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1986-12-01 01:01:01 +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861);

  s = std::string("1886-12-01 01:01:01");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621890739);

  s = std::string("1986-12-11 01:30:00");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 534648600);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeWithMillis) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS.FFF", 1, &unix_timestamp_holder));

  // Milliseconds in formatting are not supported yet by the library that do the
  // date formatting
  auto& to_date = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01.347");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDate) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make("YYYY-MM-DD", 1, &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200);

  s = std::string("1986-12-01");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200);

  s = std::string("1886-12-1");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621894400);

  s = std::string("2012-12-1");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 1354320000);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeError) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;

  auto status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
      << status.message();

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch =
      unix_timestamp(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

TEST_F(TestUnixTimestampHolder, TestWithTimezones) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  // Timezone information are yet not supported by the arrow formatters
  auto status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();

  status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzd", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeWithoutPattern) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make(BuildUnixTimestampNode(), &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  int64_t millis_since_epoch = unix_timestamp(
      &execution_context_, s.data(), static_cast<int>(s.length()), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861);

  s = std::string(" ");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1986-12-01 01:01:01 +0800");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861);

  s = std::string("1886-12-01 01:01:01");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621890739);

  s = std::string("1986-12-11 01:30:00");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 534648600);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeErrorWithoutPattern) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make(BuildUnixTimestampNode(), &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos);

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch =
      unix_timestamp(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

}  // namespace gandiva

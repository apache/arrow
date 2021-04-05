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

#include <memory>
#include <vector>

#include "arrow/testing/gtest_util.h"

#include "gandiva/execution_context.h"
#include "gandiva/to_date_functions_holder.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestToTimestampHolder : public ::testing::Test {
 public:
  FunctionNode BuildToTime(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    auto suppress_error_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(0), false);
    return FunctionNode("to_timestamp_utf8_utf8_int32",
                        {field, pattern_node, suppress_error_node}, arrow::int64());
  }

 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestToTimestampHolder, TestSimpleDateTime) {
  std::shared_ptr<ToTimestampHolder> to_timestamp_holder;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD HH:MI:SS", 1, &to_timestamp_holder));

  auto& to_date = *to_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861000);

  s = std::string(" ");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1986-12-01 01:01:01 +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533782861000);

  s = std::string("1886-12-01 01:01:01");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621890739000);

  s = std::string("1986-12-11 01:30:00");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 534648600000);
}

TEST_F(TestToTimestampHolder, TestSimpleDateTimeWithMillis) {
  std::shared_ptr<ToTimestampHolder> to_timestamp_holder;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD HH:MI:SS.FFF", 1, &to_timestamp_holder));

  // Milliseconds in formatting are not supported yet by the library that do the
  // date formatting
  auto& to_date = *to_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01.347");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestToTimestampHolder, TestSimpleDate) {
  std::shared_ptr<ToTimestampHolder> to_timestamp_holder;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD", 1, &to_timestamp_holder));

  auto& to_time = *to_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01");
  int64_t millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1986-12-01");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1886-12-1");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621894400000);

  s = std::string("2012-12-1");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 1354320000000);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestToTimestampHolder, TestSimpleDateTimeError) {
  std::shared_ptr<ToTimestampHolder> to_timestamp_holder;

  auto status = ToTimestampHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &to_timestamp_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& to_time = *to_timestamp_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
            << status.message();

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch = to_time(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

TEST_F(TestToTimestampHolder, TestWithTimezones) {
  std::shared_ptr<ToTimestampHolder> to_timestamp_holder;
  // Timezone information are yet not supported by the arrow formatters
  auto status = ToTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &to_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();

  status = ToTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzd", 0, &to_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

}  // namespace gandiva

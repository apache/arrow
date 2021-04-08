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

class TestIsDateHolder : public ::testing::Test {
 public:
  FunctionNode BuildIsDate() {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    return FunctionNode("is_date_utf8", {field}, arrow::boolean());
  }

 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestIsDateHolder, TestSimpleDateTime) {
  std::shared_ptr<IsDateHolder> is_date_holder;
  ASSERT_OK(IsDateHolder::Make("YYYY-MM-DD HH:MI:SS", 1, &is_date_holder));

  auto& is_date = *is_date_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  bool valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string(" ");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, false);

  s = std::string("1986-12-01 01:01:01 +0800");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1886-12-01 01:01:01");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1986-12-11 01:30:00");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);
}

TEST_F(TestIsDateHolder, TestSimpleDate) {
  std::shared_ptr<IsDateHolder> is_date_holder;
  ASSERT_OK(IsDateHolder::Make("YYYY-MM-DD", 1, &is_date_holder));

  auto& is_date = *is_date_holder;
  bool out_valid;
  std::string s("1986-12-01");
  bool valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1986-12-01");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1886-12-1");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("2012-12-1");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, true);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, false);
}

TEST_F(TestIsDateHolder, TestSimpleDateTimeError) {
  std::shared_ptr<IsDateHolder> is_date_holder;

  auto status = IsDateHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &is_date_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& is_date = *is_date_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  bool valid = is_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(valid, false);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
      << status.message();

  // not valid should not return error
  execution_context_.Reset();
  valid = is_date(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(valid, false);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

TEST_F(TestIsDateHolder, TestWithTimezones) {
  std::shared_ptr<IsDateHolder> is_date_holder;
  // Timezone information are yet not supported by the arrow formatters
  auto status = IsDateHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &is_date_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();

  status = IsDateHolder::Make("YYYY-MM-DD HH:MI:SS tzd", 0, &is_date_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

TEST_F(TestIsDateHolder, TestSimpleDateTimeWithoutPattern) {
  std::shared_ptr<IsDateHolder> is_date_holder;
  ASSERT_OK(IsDateHolder::Make(BuildIsDate(), &is_date_holder));

  auto& is_date = *is_date_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  bool valid = is_date(&execution_context_, s.data(), static_cast<int>(s.length()), true,
                       &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string(" ");
  valid = is_date(&execution_context_, s.data(), static_cast<int>(s.length()), true,
                  &out_valid);
  EXPECT_EQ(valid, false);

  s = std::string("1986-12-01 01:01:01 +0800");
  valid = is_date(&execution_context_, s.data(), static_cast<int>(s.length()), true,
                  &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1886-12-01 01:01:01");
  valid = is_date(&execution_context_, s.data(), static_cast<int>(s.length()), true,
                  &out_valid);
  EXPECT_EQ(valid, true);

  s = std::string("1986-12-11 01:30:00");
  valid = is_date(&execution_context_, s.data(), static_cast<int>(s.length()), true,
                  &out_valid);
  EXPECT_EQ(valid, true);
}

}  // namespace gandiva

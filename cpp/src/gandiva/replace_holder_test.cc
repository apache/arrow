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

#include "gandiva/replace_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace gandiva {

class TestReplaceHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestReplaceHolder, TestMultipleReplace) {
  std::shared_ptr<ReplaceHolder> replace_holder;

  auto status = ReplaceHolder::Make("ana", &replace_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "banana";
  std::string replace_string;
  int32_t out_length = 0;

  auto& replace = *replace_holder;
  const char* ret =
      replace(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), replace_string.c_str(),
              static_cast<int32_t>(replace_string.length()), &out_length);
  std::string ret_as_str(ret, out_length);
  EXPECT_EQ(out_length, 3);
  EXPECT_EQ(ret_as_str, "bna");

  input_string = "bananaana";

  ret = replace(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), replace_string.c_str(),
                static_cast<int32_t>(replace_string.length()), &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 3);
  EXPECT_EQ(ret_as_str, "bna");

  input_string = "bananana";

  ret = replace(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), replace_string.c_str(),
                static_cast<int32_t>(replace_string.length()), &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 2);
  EXPECT_EQ(ret_as_str, "bn");

  input_string = "anaana";

  ret = replace(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), replace_string.c_str(),
                static_cast<int32_t>(replace_string.length()), &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 0);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(ret_as_str, "");
}

TEST_F(TestReplaceHolder, TestNoMatchPattern) {
  std::shared_ptr<ReplaceHolder> replace_holder;

  auto status = ReplaceHolder::Make("ana", &replace_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "apple";
  std::string replace_string;
  int32_t out_length = 0;

  auto& replace = *replace_holder;
  const char* ret =
      replace(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), replace_string.c_str(),
              static_cast<int32_t>(replace_string.length()), &out_length);
  std::string ret_as_string(ret, out_length);
  EXPECT_EQ(out_length, 5);
  EXPECT_EQ(ret_as_string, "apple");
}

TEST_F(TestReplaceHolder, TestReplaceSameSize) {
  std::shared_ptr<ReplaceHolder> replace_holder;

  auto status = ReplaceHolder::Make("a", &replace_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "ananindeua";
  std::string replace_string = "b";
  int32_t out_length = 0;

  auto& replace = *replace_holder;
  const char* ret =
      replace(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), replace_string.c_str(),
              static_cast<int32_t>(replace_string.length()), &out_length);
  std::string ret_as_string(ret, out_length);
  EXPECT_EQ(out_length, 10);
  EXPECT_EQ(ret_as_string, "bnbnindeub");
}

TEST_F(TestReplaceHolder, TestReplaceInvalidPattern) {
  std::shared_ptr<ReplaceHolder> replace_holder;

  auto status = ReplaceHolder::Make("+", &replace_holder);
  EXPECT_EQ(status.ok(), false) << status.message();

  execution_context_.Reset();
}

}  // namespace gandiva

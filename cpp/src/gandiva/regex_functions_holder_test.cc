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

#include "gandiva/regex_functions_holder.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include "gandiva/regex_util.h"

namespace gandiva {

class TestLikeHolder : public ::testing::Test {
 public:
  RE2::Options regex_op;
  FunctionNode BuildLike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("like", {field, pattern_node}, arrow::boolean());
  }

  FunctionNode BuildLike(std::string pattern, char escape_char) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    auto escape_char_node = std::make_shared<LiteralNode>(
        arrow::utf8(), LiteralHolder(std::string(1, escape_char)), false);
    return FunctionNode("like", {field, pattern_node, escape_char_node},
                        arrow::boolean());
  }
};

TEST_F(TestLikeHolder, TestMatchAny) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab%", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestLikeHolder, TestMatchOne) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestPcreSpecial) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make(".*ab_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like(".*abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxabc"));
}

TEST_F(TestLikeHolder, TestRegexEscape) {
  std::string res;
  auto status = RegexUtil::SqlLikePatternToPcre("#%hello#_abc_def##", '#', res);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_EQ(res, "%hello_abc.def#");
}

TEST_F(TestLikeHolder, TestDot) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("abc.", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestLikeHolder, TestMatchSubString) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("%abc%", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_FALSE(like("xxabdc"));

  status = LikeHolder::Make("%ab-.^$*+?()[]{}|—/c\\%%", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like_reserved_char = *like_holder;
  EXPECT_TRUE(like_reserved_char("XXab-.^$*+?()[]{}|—/c%d"));
  EXPECT_FALSE(like_reserved_char("xxad-.^$*+?()[]{}|—/c"));
}

TEST_F(TestLikeHolder, TestOptimise) {
  // optimise for 'starts_with'
  auto fnode = LikeHolder::TryOptimize(BuildLike("xy 123z%"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) 'xy 123z')");

  // optimise for 'ends_with'
  fnode = LikeHolder::TryOptimize(BuildLike("%xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((string) in, (const string) 'xyz')");

  // optimise for 'is_substr'
  fnode = LikeHolder::TryOptimize(BuildLike("%abc%"));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) 'abc')");

  // optimise for 'is_substr with special characters'
  fnode = LikeHolder::TryOptimize(BuildLike("%ab-c%"));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) 'ab-c')");

  // optimise for 'ends_with with special characters'
  fnode = LikeHolder::TryOptimize(BuildLike("%ab-c"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(),
            "bool ends_with((string) in, (const string) "
            "'ab-c')");

  // optimise for 'starts_with with special characters'
  fnode = LikeHolder::TryOptimize(BuildLike("ab-c%"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(),
            "bool starts_with((string) in, (const string) "
            "'ab-c')");

  // no optimisation for others.
  fnode = LikeHolder::TryOptimize(BuildLike("xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("_xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("_xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("%xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("x_yz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  // no optimisation for escaped pattern.
  fnode = LikeHolder::TryOptimize(BuildLike("\\%xyz", '\\'));
  EXPECT_EQ(fnode.descriptor()->name(), "like");
  EXPECT_EQ(fnode.ToString(),
            "bool like((string) in, (const string) '\\%xyz', (const string) '\\')");

  // optimised for escape pattern that are pcre special chars.
  fnode = LikeHolder::TryOptimize(BuildLike("%ab^_cd^_de%", '^'));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) 'ab_cd_de')");

  fnode = LikeHolder::TryOptimize(BuildLike("%ab^^cd^^de%", '^'));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) 'ab^cd^de')");
}

TEST_F(TestLikeHolder, TestMatchOneEscape) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab\\_", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab_"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestMatchManyEscape) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab\\%", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab%"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestMatchEscape) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab\\\\", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\"));

  EXPECT_FALSE(like("abc"));
}

TEST_F(TestLikeHolder, TestEmptyEscapeChar) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab\\_", "", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\c"));
  EXPECT_TRUE(like("ab\\_"));

  EXPECT_FALSE(like("ab\\_d"));
  EXPECT_FALSE(like("ab__"));
}

TEST_F(TestLikeHolder, TestMultipleEscapeChar) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab\\_", "\\\\", &like_holder);
  EXPECT_EQ(status.ok(), false) << status.message();
}

class TestILikeHolder : public ::testing::Test {
 public:
  RE2::Options regex_op;
  FunctionNode BuildILike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("ilike", {field, pattern_node}, arrow::boolean());
  }
};

TEST_F(TestILikeHolder, TestMatchAny) {
  std::shared_ptr<LikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = LikeHolder::Make("ab%", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("aBc"));
  EXPECT_TRUE(like("ABCD"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestILikeHolder, TestMatchOne) {
  std::shared_ptr<LikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = LikeHolder::Make("Ab_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("aBd"));

  EXPECT_FALSE(like("A"));
  EXPECT_FALSE(like("Abcd"));
  EXPECT_FALSE(like("DaBc"));
}

TEST_F(TestILikeHolder, TestPcreSpecial) {
  std::shared_ptr<LikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = LikeHolder::Make(".*aB_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like(".*Abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxAbc"));
}

TEST_F(TestILikeHolder, TestDot) {
  std::shared_ptr<LikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = LikeHolder::Make("aBc.", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

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

// Tests related to the REGEXP_EXTRACT function
class TestExtractHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestExtractHolder, TestSimpleExtract) {
  std::shared_ptr<ExtractHolder> extract_holder;

  // Pattern to match of two group of letters
  auto status = ExtractHolder::Make(R"((\w+) (\w+))", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "John Doe";
  int32_t extract_index = 2;  // Retrieve the surname
  int32_t out_length = 0;

  auto& extract = *extract_holder;
  const char* ret =
      extract(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  std::string ret_as_str(ret, out_length);
  EXPECT_EQ(out_length, 3);
  EXPECT_EQ(ret_as_str, "Doe");

  input_string = "Ringo Beast";
  extract_index = 1;  // Retrieve the first name

  ret = extract(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 5);
  EXPECT_EQ(ret_as_str, "Ringo");

  input_string = "Paul Test";
  extract_index = 0;  // Retrieve all match

  ret = extract(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 9);
  EXPECT_EQ(ret_as_str, "Paul Test");

  status = ExtractHolder::Make(R"((\w+) (\w+) - (\d+))", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& extract2 = *extract_holder;

  input_string = "John Doe - 124";
  extract_index = 0;  // Retrieve all match

  ret = extract2(&execution_context_, input_string.c_str(),
                 static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 14);
  EXPECT_EQ(ret_as_str, "John Doe - 124");

  input_string = "John Doe - 124 MoreString";
  extract_index = 0;  // Retrieve all match

  ret = extract2(&execution_context_, input_string.c_str(),
                 static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 14);
  EXPECT_EQ(ret_as_str, "John Doe - 124");

  input_string = "MoreString John Doe - 124";
  extract_index = 0;  // Retrieve all match

  ret = extract2(&execution_context_, input_string.c_str(),
                 static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 14);
  EXPECT_EQ(ret_as_str, "John Doe - 124");

  // Pattern to match only numbers
  status = ExtractHolder::Make(R"(((\w+)))", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& extract_numbers = *extract_holder;

  input_string = "路%$大a";
  extract_index = 0;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 1);
  EXPECT_EQ(ret_as_str, "a");

  input_string = "b路%$大";
  extract_index = 0;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 1);
  EXPECT_EQ(ret_as_str, "b");

  input_string = "路%c$大";
  extract_index = 0;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 1);
  EXPECT_EQ(ret_as_str, "c");

  input_string = "路%c$大";
  extract_index = 1;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 1);
  EXPECT_EQ(ret_as_str, "c");

  input_string = "路%c$大";
  extract_index = 2;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 1);
  EXPECT_EQ(ret_as_str, "c");

  input_string = "路%c$大";
  extract_index = 3;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 0);
  EXPECT_TRUE(execution_context_.has_error());
}

TEST_F(TestExtractHolder, TestNoMatches) {
  std::shared_ptr<ExtractHolder> extract_holder;

  // Pattern to match of two group of letters
  auto status = ExtractHolder::Make(R"((\w+) (\w+))", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "John";
  int32_t extract_index = 2;  // The regex will not match with the input string
  int32_t out_length = 0;

  auto& extract = *extract_holder;
  const char* ret =
      extract(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  std::string ret_as_str(ret, out_length);
  EXPECT_EQ(out_length, 0);
  EXPECT_FALSE(execution_context_.has_error());

  // Pattern to match only numbers
  status = ExtractHolder::Make(R"(\d+)", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& extract_numbers = *extract_holder;

  input_string = "12345";
  extract_index = 0;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 5);
  EXPECT_EQ(ret_as_str, "12345");

  input_string = "12345A";
  extract_index = 0;  // Retrieve all matched string

  ret = extract_numbers(&execution_context_, input_string.c_str(),
                        static_cast<int32_t>(input_string.length()), extract_index,
                        &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 5);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(ret_as_str, "12345");
}

TEST_F(TestExtractHolder, TestInvalidRange) {
  std::shared_ptr<ExtractHolder> extract_holder;

  // Pattern to match of two group of letters
  auto status = ExtractHolder::Make(R"((\w+) (\w+))", &extract_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  std::string input_string = "John Doe";
  int32_t extract_index = -1;
  int32_t out_length = 0;

  auto& extract = *extract_holder;
  const char* ret =
      extract(&execution_context_, input_string.c_str(),
              static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  std::string ret_as_str(ret, out_length);
  EXPECT_EQ(out_length, 0);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // The test regex has two capturing groups, so the higher index
  // allowed for the test regex is 2
  extract_index = 3;

  ret = extract(&execution_context_, input_string.c_str(),
                static_cast<int32_t>(input_string.length()), extract_index, &out_length);
  ret_as_str = std::string(ret, out_length);
  EXPECT_EQ(out_length, 0);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();
}

TEST_F(TestExtractHolder, TestExtractInvalidPattern) {
  std::shared_ptr<ExtractHolder> extract_holder;

  auto status = ExtractHolder::Make("+", &extract_holder);
  EXPECT_EQ(status.ok(), false) << status.message();

  execution_context_.Reset();
}

TEST_F(TestExtractHolder, TestErrorWhileBuildingHolder) {
  std::shared_ptr<ExtractHolder> extract_holder;

  // Create function with incorrect number of params
  auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  auto pattern_node = std::make_shared<LiteralNode>(
      arrow::utf8(), LiteralHolder(R"((\w+) (\w+))"), false);
  auto function_node =
      FunctionNode("regexp_extract", {field, pattern_node}, arrow::utf8());

  auto status = ExtractHolder::Make(function_node, &extract_holder);
  EXPECT_EQ(status.ok(), false);
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr("'extract' function requires three parameters"));

  execution_context_.Reset();

  // Create function with non-utf8 literal parameter as pattern
  field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  pattern_node = std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(2), false);
  auto index_node = std::make_shared<FieldNode>(arrow::field("idx", arrow::int32()));
  function_node =
      FunctionNode("regexp_extract", {field, pattern_node, index_node}, arrow::utf8());

  status = ExtractHolder::Make(function_node, &extract_holder);
  EXPECT_EQ(status.ok(), false);
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr(
                  "'extract' function requires a literal as the second parameter"));

  execution_context_.Reset();

  // Create function not using a literal parameter as pattern
  field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  auto pattern_as_node =
      std::make_shared<FieldNode>(arrow::field("pattern", arrow::utf8()));
  index_node = std::make_shared<FieldNode>(arrow::field("idx", arrow::int32()));
  function_node =
      FunctionNode("regexp_extract", {field, pattern_as_node, index_node}, arrow::utf8());

  status = ExtractHolder::Make(function_node, &extract_holder);
  EXPECT_EQ(status.ok(), false);
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr(
                  "'extract' function requires a literal as the second parameter"));

  execution_context_.Reset();
}

}  // namespace gandiva

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

#include "arrow/testing/gtest_util.h"
#include "gandiva/regex_util.h"

namespace gandiva {

class TestLikeHolder : public ::testing::Test {
 public:
  RE2::Options regex_op;
  FunctionNode BuildLike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return {"like", {field, pattern_node}, arrow::boolean()};
  }

  FunctionNode BuildLike(std::string pattern, char escape_char) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    auto escape_char_node = std::make_shared<LiteralNode>(
        arrow::utf8(), LiteralHolder(std::string(1, escape_char)), false);
    return {"like", {field, pattern_node, escape_char_node}, arrow::boolean()};
  }
};

TEST_F(TestLikeHolder, TestMatchAny) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab%", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestLikeHolder, TestMatchOne) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab_", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestPcreSpecial) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make(".*ab_", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like(".*abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxabc"));
}

TEST_F(TestLikeHolder, TestRegexEscape) {
  std::string res;
  ARROW_EXPECT_OK(RegexUtil::SqlLikePatternToPcre("#%hello#_abc_def##", '#', res));

  EXPECT_EQ(res, "%hello_abc.def#");
}

TEST_F(TestLikeHolder, TestDot) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("abc.", regex_op));

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestLikeHolder, TestMatchSubString) {
  EXPECT_OK_AND_ASSIGN(auto like_holder, LikeHolder::Make("%abc%", "\\"));

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_FALSE(like("xxabdc"));

  EXPECT_OK_AND_ASSIGN(like_holder, LikeHolder::Make("%ab-.^$*+?()[]{}|—/c\\%%", "\\"));

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
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab\\_", "\\"));

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab_"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestMatchManyEscape) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab\\%", "\\"));

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab%"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestLikeHolder, TestMatchEscape) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab\\\\", "\\"));

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\"));

  EXPECT_FALSE(like("abc"));
}

TEST_F(TestLikeHolder, TestEmptyEscapeChar) {
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab\\_", ""));

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\c"));
  EXPECT_TRUE(like("ab\\_"));

  EXPECT_FALSE(like("ab\\_d"));
  EXPECT_FALSE(like("ab__"));
}

TEST_F(TestLikeHolder, TestMultipleEscapeChar) {
  ASSERT_RAISES(Invalid, LikeHolder::Make("ab\\_", "\\\\").status());
}

class TestILikeHolder : public ::testing::Test {
 public:
  RE2::Options regex_op;
  FunctionNode BuildILike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return {"ilike", {field, pattern_node}, arrow::boolean()};
  }
};

TEST_F(TestILikeHolder, TestMatchAny) {
  regex_op.set_case_sensitive(false);

  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("ab%", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("aBc"));
  EXPECT_TRUE(like("ABCD"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestILikeHolder, TestMatchOne) {
  regex_op.set_case_sensitive(false);
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("Ab_", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("aBd"));

  EXPECT_FALSE(like("A"));
  EXPECT_FALSE(like("Abcd"));
  EXPECT_FALSE(like("DaBc"));
}

TEST_F(TestILikeHolder, TestPcreSpecial) {
  regex_op.set_case_sensitive(false);
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make(".*aB_", regex_op));

  auto& like = *like_holder;
  EXPECT_TRUE(like(".*Abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxAbc"));
}

TEST_F(TestILikeHolder, TestDot) {
  regex_op.set_case_sensitive(false);
  EXPECT_OK_AND_ASSIGN(auto const like_holder, LikeHolder::Make("aBc.", regex_op));

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

class TestReplaceHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestReplaceHolder, TestMultipleReplace) {
  EXPECT_OK_AND_ASSIGN(auto const replace_holder, ReplaceHolder::Make("ana"));

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
  EXPECT_OK_AND_ASSIGN(auto const replace_holder, ReplaceHolder::Make("ana"));

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
  EXPECT_OK_AND_ASSIGN(auto const replace_holder, ReplaceHolder::Make("a"));

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
  ASSERT_RAISES(Invalid, ReplaceHolder::Make("+"));
  execution_context_.Reset();
}

// Tests related to the REGEXP_EXTRACT function
class TestExtractHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestExtractHolder, TestSimpleExtract) {
  // Pattern to match of two group of letters
  EXPECT_OK_AND_ASSIGN(auto extract_holder, ExtractHolder::Make(R"((\w+) (\w+))"));

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

  EXPECT_OK_AND_ASSIGN(extract_holder, ExtractHolder::Make(R"((\w+) (\w+) - (\d+))"));

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
  EXPECT_OK_AND_ASSIGN(extract_holder, ExtractHolder::Make(R"(((\w+)))"));

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
  // Pattern to match of two group of letters
  EXPECT_OK_AND_ASSIGN(auto extract_holder, ExtractHolder::Make(R"((\w+) (\w+))"));

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
  EXPECT_OK_AND_ASSIGN(extract_holder, ExtractHolder::Make(R"(\d+)"));

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
  // Pattern to match of two group of letters
  EXPECT_OK_AND_ASSIGN(auto const extract_holder, ExtractHolder::Make(R"((\w+) (\w+))"));

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
  ASSERT_RAISES(Invalid, ExtractHolder::Make("+"));
  execution_context_.Reset();
}

TEST_F(TestExtractHolder, TestErrorWhileBuildingHolder) {
  // Create function with incorrect number of params
  auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  auto pattern_node = std::make_shared<LiteralNode>(
      arrow::utf8(), LiteralHolder(R"((\w+) (\w+))"), false);
  auto function_node =
      FunctionNode("regexp_extract", {field, pattern_node}, arrow::utf8());

  auto extract_holder = ExtractHolder::Make(function_node);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("'extract' function requires three parameters"),
      extract_holder.status());

  execution_context_.Reset();

  // Create function with non-utf8 literal parameter as pattern
  field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  pattern_node = std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(2), false);
  auto index_node = std::make_shared<FieldNode>(arrow::field("idx", arrow::int32()));
  function_node =
      FunctionNode("regexp_extract", {field, pattern_node, index_node}, arrow::utf8());

  extract_holder = ExtractHolder::Make(function_node);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "'extract' function requires a literal as the second parameter"),
      extract_holder.status());

  execution_context_.Reset();

  // Create function not using a literal parameter as pattern
  field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
  auto pattern_as_node =
      std::make_shared<FieldNode>(arrow::field("pattern", arrow::utf8()));
  index_node = std::make_shared<FieldNode>(arrow::field("idx", arrow::int32()));
  function_node =
      FunctionNode("regexp_extract", {field, pattern_as_node, index_node}, arrow::utf8());

  extract_holder = ExtractHolder::Make(function_node);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "'extract' function requires a literal as the second parameter"),
      extract_holder.status());

  execution_context_.Reset();
}

}  // namespace gandiva

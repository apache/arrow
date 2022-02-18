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

#include "gandiva/like_holder.h"
#include "gandiva/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

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
        arrow::int8(), LiteralHolder((int8_t)escape_char), false);
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
            "bool like((string) in, (const string) '\\%xyz', (const int8) \\)");
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
}  // namespace gandiva

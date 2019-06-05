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
  FunctionNode BuildLike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("like", {field, pattern_node}, arrow::boolean());
  }
};

TEST_F(TestLikeHolder, TestMatchAny) {
  std::shared_ptr<LikeHolder> like_holder;

  auto status = LikeHolder::Make("ab%", &like_holder);
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

  auto status = LikeHolder::Make("ab_", &like_holder);
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

  auto status = LikeHolder::Make(".*ab_", &like_holder);
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

  auto status = LikeHolder::Make("abc.", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestLikeHolder, TestOptimise) {
  // optimise for 'starts_with'
  auto fnode = LikeHolder::TryOptimize(BuildLike("xy 123z%"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((utf8) in, (const string) xy 123z)");

  // optimise for 'ends_with'
  fnode = LikeHolder::TryOptimize(BuildLike("%xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((utf8) in, (const string) xyz)");

  // no optimisation for others.
  fnode = LikeHolder::TryOptimize(BuildLike("xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("_xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("%xyz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("_xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("%xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = LikeHolder::TryOptimize(BuildLike("x_yz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");
}

}  // namespace gandiva

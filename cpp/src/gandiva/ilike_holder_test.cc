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

#include "gandiva/ilike_holder.h"
#include "gandiva/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestILikeHolder : public ::testing::Test {
 public:
  FunctionNode BuildILike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("ilike", {field, pattern_node}, arrow::boolean());
  }
};

TEST_F(TestILikeHolder, TestMatchAny) {
  std::shared_ptr<IlikeHolder> ilike_holder;

  auto status = IlikeHolder::Make("ab%", &ilike_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *ilike_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("aBc"));
  EXPECT_TRUE(like("ABCD"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestILikeHolder, TestMatchOne) {
  std::shared_ptr<IlikeHolder> ilike_holder;

  auto status = IlikeHolder::Make("Ab_", &ilike_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *ilike_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("aBd"));

  EXPECT_FALSE(like("A"));
  EXPECT_FALSE(like("Abcd"));
  EXPECT_FALSE(like("DaBc"));
}

TEST_F(TestILikeHolder, TestPcreSpecial) {
  std::shared_ptr<IlikeHolder> ilike_holder;

  auto status = IlikeHolder::Make(".*aB_", &ilike_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *ilike_holder;
  EXPECT_TRUE(like(".*Abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxAbc"));
}

TEST_F(TestILikeHolder, TestDot) {
  std::shared_ptr<IlikeHolder> ilike_holder;

  auto status = IlikeHolder::Make("aBc.", &ilike_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *ilike_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestILikeHolder, TestOptimise) {
  // optimise for 'starts_with'
  auto fnode = IlikeHolder::TryOptimize(BuildILike("xy 123z%"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) xy 123z)");

  // optimise for 'ends_with'
  fnode = IlikeHolder::TryOptimize(BuildILike("%xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((string) in, (const string) xyz)");

  // optimise for 'is_substr'
  fnode = IlikeHolder::TryOptimize(BuildILike("%abc%"));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) abc)");

  // no optimisation for others.
  fnode = IlikeHolder::TryOptimize(BuildILike("xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");

  fnode = IlikeHolder::TryOptimize(BuildILike("_xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");

  fnode = IlikeHolder::TryOptimize(BuildILike("_xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");

  fnode = IlikeHolder::TryOptimize(BuildILike("%xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");

  fnode = IlikeHolder::TryOptimize(BuildILike("x_yz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");
}

}  // namespace gandiva

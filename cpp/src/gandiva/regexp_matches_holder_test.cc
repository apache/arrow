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

#include "gandiva/regexp_matches_holder.h"
#include "gandiva/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestRegexpMatchesHolder : public ::testing::Test {
 public:
  FunctionNode BuildRegexpMatches(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("regexp_matches", {field, pattern_node}, arrow::boolean());
  }
};

TEST_F(TestRegexpMatchesHolder, TestString) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("ab", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));
  EXPECT_TRUE(like("cab"));

  EXPECT_FALSE(like("a"));
}

TEST_F(TestRegexpMatchesHolder, TestDotStar) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("a.*b", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("adeb"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("cabc"));
  EXPECT_TRUE(like("caebf"));

  EXPECT_FALSE(like("ba"));
  EXPECT_FALSE(like("a"));
}

TEST_F(TestRegexpMatchesHolder, TestDot) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("ab.", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abd"));
  EXPECT_TRUE(like("abcd"));
  EXPECT_TRUE(like("dabc"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestAnchors) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("^ab.*c$", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("abdc"));
  EXPECT_TRUE(like("abc"));

  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestRegexpMatchesHolder, TestIgnoreCase) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("(?i)ab", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("daBc"));
  EXPECT_TRUE(like("CAB"));

  EXPECT_FALSE(like("ba"));
}

TEST_F(TestRegexpMatchesHolder, TestCharacterClass) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("[ab]c", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("acd"));
  EXPECT_TRUE(like("ebc"));
  EXPECT_TRUE(like("abc"));

  EXPECT_FALSE(like("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestEscapeCharacter) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("\\.\\*", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like(".*"));

  EXPECT_FALSE(like("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestNonAsciiMatches) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make(".*çåå†.*", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *regexp_matches_holder;
  EXPECT_TRUE(like("açåå†b"));

  EXPECT_FALSE(like("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestOptimise) {
  // optimise for 'starts_with'
  auto fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^abc"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) abc)");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^abc.*"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) abc)");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^ab cd"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) ab cd)");

  // optimise for 'ends_with'
  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("xyz$"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((string) in, (const string) xyz)");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches(".*xyz$"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((string) in, (const string) xyz)");

  // no optimisation for others.
  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^xyz$"));
  EXPECT_EQ(fnode.descriptor()->name(), "regexp_matches");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^xy.*z"));
  EXPECT_EQ(fnode.descriptor()->name(), "regexp_matches");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^.*"));
  EXPECT_EQ(fnode.descriptor()->name(), "regexp_matches");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("x.yz$"));
  EXPECT_EQ(fnode.descriptor()->name(), "regexp_matches");

  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("^[xyz]"));
  EXPECT_EQ(fnode.descriptor()->name(), "regexp_matches");
}
}  // namespace gandiva

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

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("ab"));
  EXPECT_TRUE(regexp_matches("abc"));
  EXPECT_TRUE(regexp_matches("abcd"));
  EXPECT_TRUE(regexp_matches("cab"));

  EXPECT_FALSE(regexp_matches("a"));
}

TEST_F(TestRegexpMatchesHolder, TestDotStar) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("a.*b", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("ab"));
  EXPECT_TRUE(regexp_matches("adeb"));
  EXPECT_TRUE(regexp_matches("abc"));
  EXPECT_TRUE(regexp_matches("cabc"));
  EXPECT_TRUE(regexp_matches("caebf"));

  EXPECT_FALSE(regexp_matches("ba"));
  EXPECT_FALSE(regexp_matches("a"));
}

TEST_F(TestRegexpMatchesHolder, TestDot) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("ab.", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("abc"));
  EXPECT_TRUE(regexp_matches("abd"));
  EXPECT_TRUE(regexp_matches("abcd"));
  EXPECT_TRUE(regexp_matches("dabc"));

  EXPECT_FALSE(regexp_matches("a"));
  EXPECT_FALSE(regexp_matches("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestAnchors) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("^ab.*c$", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("abdc"));
  EXPECT_TRUE(regexp_matches("abc"));

  EXPECT_FALSE(regexp_matches("abcd"));
  EXPECT_FALSE(regexp_matches("dabc"));
}

TEST_F(TestRegexpMatchesHolder, TestIgnoreCase) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("(?i)ab", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("abc"));
  EXPECT_TRUE(regexp_matches("daBc"));
  EXPECT_TRUE(regexp_matches("CAB"));

  EXPECT_FALSE(regexp_matches("ba"));
}

TEST_F(TestRegexpMatchesHolder, TestCharacterClass) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("[ab]c", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("acd"));
  EXPECT_TRUE(regexp_matches("ebc"));
  EXPECT_TRUE(regexp_matches("abc"));

  EXPECT_FALSE(regexp_matches("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestEscapeCharacter) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make("\\.\\*", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches(".*"));

  EXPECT_FALSE(regexp_matches("ab"));
}

TEST_F(TestRegexpMatchesHolder, TestNonAsciiMatches) {
  std::shared_ptr<RegexpMatchesHolder> regexp_matches_holder;

  auto status = RegexpMatchesHolder::Make(".*çåå†.*", &regexp_matches_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& regexp_matches = *regexp_matches_holder;
  EXPECT_TRUE(regexp_matches("açåå†b"));

  EXPECT_FALSE(regexp_matches("ab"));
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

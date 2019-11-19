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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "gandiva/regex_util.h"

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

  // optimise for 'is_substr'
  fnode = RegexpMatchesHolder::TryOptimize(BuildRegexpMatches("xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) xyz)");

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

class TestSQLLikeHolder : public ::testing::Test {
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

TEST_F(TestSQLLikeHolder, TestMatchAny) {
  std::shared_ptr<SQLLikeHolder> sql_like_holder;

  auto status = SQLLikeHolder::Make("ab%", &sql_like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *sql_like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestSQLLikeHolder, TestMatchOne) {
  std::shared_ptr<SQLLikeHolder> sql_like_holder;

  auto status = SQLLikeHolder::Make("ab_", &sql_like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *sql_like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abd"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestSQLLikeHolder, TestPcreSpecial) {
  std::shared_ptr<SQLLikeHolder> sql_like_holder;

  auto status = SQLLikeHolder::Make(".*ab_", &sql_like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *sql_like_holder;
  EXPECT_TRUE(like(".*abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxabc"));
}

TEST_F(TestSQLLikeHolder, TestRegexEscape) {
  std::string res;
  auto status = RegexUtil::SqlLikePatternToPcre("#%hello#_abc_def##", '#', res);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_EQ(res, "^%hello_abc.def#$");
}

TEST_F(TestSQLLikeHolder, Test) {
  std::vector<std::tuple<std::string, std::string, char>> cases = {
      {"test12", "^test12$", '\\'},        {"_test_test_", "^.test.test.$", '\\'},
      {"%test%test%", "test.*test", '\\'}, {"\\%test.%", "^%test\\.", '\\'},
      {"f%test.%", "^%test\\.", 'f'},      {"$25.00", "^\\$25\\.00$", '\\'},
      {"\\test", "^\\\\test$", '#'}};

  for (auto&& test_case : cases) {
    std::string pattern_like, pattern_pcre;
    char escape_char;
    std::tie(pattern_like, pattern_pcre, escape_char) = test_case;

    std::string res;
    auto status = RegexUtil::SqlLikePatternToPcre(pattern_like, escape_char, res);
    EXPECT_TRUE(status.ok()) << status.message();

    EXPECT_EQ(res, pattern_pcre);
  }
}

TEST_F(TestSQLLikeHolder, TestDot) {
  std::shared_ptr<SQLLikeHolder> sql_like_holder;

  auto status = SQLLikeHolder::Make("abc.", &sql_like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *sql_like_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestSQLLikeHolder, TestOptimise) {
  // optimise for 'starts_with'
  auto fnode = SQLLikeHolder::TryOptimize(BuildLike("xy 123z%"));
  EXPECT_EQ(fnode.descriptor()->name(), "starts_with");
  EXPECT_EQ(fnode.ToString(), "bool starts_with((string) in, (const string) xy 123z)");

  // optimise for 'ends_with'
  fnode = SQLLikeHolder::TryOptimize(BuildLike("%xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "ends_with");
  EXPECT_EQ(fnode.ToString(), "bool ends_with((string) in, (const string) xyz)");

  // optimise for 'is_substr'
  fnode = SQLLikeHolder::TryOptimize(BuildLike("%abc%"));
  EXPECT_EQ(fnode.descriptor()->name(), "is_substr");
  EXPECT_EQ(fnode.ToString(), "bool is_substr((string) in, (const string) abc)");

  // no optimisation for others.
  fnode = SQLLikeHolder::TryOptimize(BuildLike("xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = SQLLikeHolder::TryOptimize(BuildLike("_xyz"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = SQLLikeHolder::TryOptimize(BuildLike("_xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = SQLLikeHolder::TryOptimize(BuildLike("%xyz_"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  fnode = SQLLikeHolder::TryOptimize(BuildLike("x_yz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");

  // no optimisation for escaped pattern.
  fnode = SQLLikeHolder::TryOptimize(BuildLike("\\%xyz", '\\'));
  EXPECT_EQ(fnode.descriptor()->name(), "like");
  EXPECT_EQ(fnode.ToString(),
            "bool like((string) in, (const string) \\%xyz, (const int8) \\)");

  fnode = SQLLikeHolder::TryOptimize(BuildLike("x_yz%"));
  EXPECT_EQ(fnode.descriptor()->name(), "like");
}

TEST_F(TestSQLLikeHolder, TestMatchOneEscape) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  auto status = SQLLikeHolder::Make("ab\\_", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab_"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestSQLLikeHolder, TestMatchManyEscape) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  auto status = SQLLikeHolder::Make("ab\\%", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab%"));

  EXPECT_FALSE(like("abc"));
  EXPECT_FALSE(like("abd"));
  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("abcd"));
  EXPECT_FALSE(like("dabc"));
}

TEST_F(TestSQLLikeHolder, TestMatchEscape) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  auto status = SQLLikeHolder::Make("ab\\\\", "\\", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\"));

  EXPECT_FALSE(like("abc"));
}

TEST_F(TestSQLLikeHolder, TestEmptyEscapeChar) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  auto status = SQLLikeHolder::Make("ab\\_", "", &like_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;

  EXPECT_TRUE(like("ab\\c"));
  EXPECT_TRUE(like("ab\\_"));

  EXPECT_FALSE(like("ab\\_d"));
  EXPECT_FALSE(like("ab__"));
}

TEST_F(TestSQLLikeHolder, TestMultipleEscapeChar) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  auto status = SQLLikeHolder::Make("ab\\_", "\\\\", &like_holder);
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
  std::shared_ptr<SQLLikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = SQLLikeHolder::Make("ab%", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("aBc"));
  EXPECT_TRUE(like("ABCD"));

  EXPECT_FALSE(like("a"));
  EXPECT_FALSE(like("cab"));
}

TEST_F(TestILikeHolder, TestMatchOne) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = SQLLikeHolder::Make("Ab_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("aBd"));

  EXPECT_FALSE(like("A"));
  EXPECT_FALSE(like("Abcd"));
  EXPECT_FALSE(like("DaBc"));
}

TEST_F(TestILikeHolder, TestPcreSpecial) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = SQLLikeHolder::Make(".*aB_", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like(".*Abc"));  // . and * aren't special in sql regex
  EXPECT_FALSE(like("xxAbc"));
}

TEST_F(TestILikeHolder, TestDot) {
  std::shared_ptr<SQLLikeHolder> like_holder;

  regex_op.set_case_sensitive(false);
  auto status = SQLLikeHolder::Make("aBc.", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_FALSE(like("abcd"));
}

TEST_F(TestILikeHolder, TestOptimise) {
  // no optimise for ilike
  auto fnode = SQLLikeHolder::TryOptimize(BuildILike("%abc%"));
  EXPECT_EQ(fnode.descriptor()->name(), "ilike");
  EXPECT_EQ(fnode.ToString(), "bool ilike((string) in, (const string) %abc%)");
}

}  // namespace gandiva

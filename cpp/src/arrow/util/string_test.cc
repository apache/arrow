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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/string.h"

namespace arrow {
namespace internal {

TEST(Trim, Basics) {
  std::vector<std::pair<std::string, std::string>> test_cases = {
      {"", ""},         {" ", ""},     {"  ", ""},       {"\t ", ""},
      {" \ta\t ", "a"}, {" \ta", "a"}, {"ab   \t", "ab"}};
  for (auto case_ : test_cases) {
    EXPECT_EQ(case_.second, TrimString(case_.first));
  }
}

TEST(AsciiEqualsCaseInsensitive, Basics) {
  ASSERT_TRUE(AsciiEqualsCaseInsensitive("foo", "Foo"));
  ASSERT_TRUE(AsciiEqualsCaseInsensitive("foo!", "FOO!"));
  ASSERT_TRUE(AsciiEqualsCaseInsensitive("", ""));
  ASSERT_TRUE(AsciiEqualsCaseInsensitive("fooo", "fooO"));

  ASSERT_FALSE(AsciiEqualsCaseInsensitive("f", "G"));
  ASSERT_FALSE(AsciiEqualsCaseInsensitive("foo!", "FOO"));
}

TEST(AsciiToLower, Basics) {
  ASSERT_EQ("something", AsciiToLower("Something"));
  ASSERT_EQ("something", AsciiToLower("SOMETHING"));
  ASSERT_EQ("", AsciiToLower(""));
}

TEST(ParseHexValue, Valid) {
  uint8_t output;

  // evaluate valid letters
  std::string input = "AB";
  ASSERT_OK(ParseHexValue(input.c_str(), &output));
  EXPECT_EQ(171, output);

  // evaluate valid numbers
  input = "12";
  ASSERT_OK(ParseHexValue(input.c_str(), &output));
  EXPECT_EQ(18, output);

  // evaluate mixed hex numbers
  input = "B1";
  ASSERT_OK(ParseHexValue(input.c_str(), &output));
  EXPECT_EQ(177, output);
}

TEST(ParseHexValue, Invalid) {
  uint8_t output;

  // evaluate invalid letters
  std::string input = "XY";
  ASSERT_RAISES(Invalid, ParseHexValue(input.c_str(), &output));

  // evaluate invalid signs
  input = "@?";
  ASSERT_RAISES(Invalid, ParseHexValue(input.c_str(), &output));

  // evaluate lower-case letters
  input = "ab";
  ASSERT_RAISES(Invalid, ParseHexValue(input.c_str(), &output));
}

TEST(Replace, Basics) {
  auto s = Replace("dat_{i}.txt", "{i}", "23");
  EXPECT_TRUE(s);
  EXPECT_EQ(*s, "dat_23.txt");

  // only replace the first occurrence of token
  s = Replace("dat_{i}_{i}.txt", "{i}", "23");
  EXPECT_TRUE(s);
  EXPECT_EQ(*s, "dat_23_{i}.txt");

  s = Replace("dat_.txt", "{nope}", "23");
  EXPECT_FALSE(s);
}

TEST(SplitString, InnerDelimiter) {
  std::string input = "a:b:c";
  auto parts = SplitString(input, ':');
  ASSERT_EQ(parts.size(), 3);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[1], "b");
  EXPECT_EQ(parts[2], "c");
}

TEST(SplitString, OuterRightDelimiter) {
  std::string input = "a:b:c:";
  auto parts = SplitString(input, ':');
  ASSERT_EQ(parts.size(), 4);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[1], "b");
  EXPECT_EQ(parts[2], "c");
  EXPECT_EQ(parts[3], "");
}

TEST(SplitString, OuterLeftAndOuterRightDelimiter) {
  std::string input = ":a:b:c:";
  auto parts = SplitString(input, ':');
  ASSERT_EQ(parts.size(), 5);
  EXPECT_EQ(parts[0], "");
  EXPECT_EQ(parts[1], "a");
  EXPECT_EQ(parts[2], "b");
  EXPECT_EQ(parts[3], "c");
  EXPECT_EQ(parts[4], "");
}

TEST(SplitString, OnlyDemiliter) {
  std::string input = ":";
  auto parts = SplitString(input, ':');
  ASSERT_EQ(parts.size(), 2);
  EXPECT_EQ(parts[0], "");
  EXPECT_EQ(parts[1], "");
}

TEST(SplitString, Limit) {
  std::string input = "a:b:c";
  auto parts = SplitString(input, ':', 2);
  ASSERT_EQ(parts.size(), 2);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[1], "b:c");
}

TEST(SplitString, LimitOver) {
  std::string input = "a:b:c";
  auto parts = SplitString(input, ':', 4);
  ASSERT_EQ(parts.size(), 3);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[1], "b");
  EXPECT_EQ(parts[2], "c");
}

TEST(SplitString, LimitZero) {
  std::string input = "a:b:c";
  auto parts = SplitString(input, ':', 0);
  ASSERT_EQ(parts.size(), 3);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[1], "b");
  EXPECT_EQ(parts[2], "c");
}

}  // namespace internal
}  // namespace arrow

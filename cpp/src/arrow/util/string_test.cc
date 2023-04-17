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

#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/regex.h"
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

TEST(StartsWith, Basics) {
  std::string empty{};
  std::string abc{"abc"};
  std::string abcdef{"abcdef"};
  std::string def{"def"};
  ASSERT_TRUE(StartsWith(empty, empty));
  ASSERT_TRUE(StartsWith(abc, empty));
  ASSERT_TRUE(StartsWith(abc, abc));
  ASSERT_TRUE(StartsWith(abcdef, abc));
  ASSERT_FALSE(StartsWith(abc, abcdef));
  ASSERT_FALSE(StartsWith(def, abcdef));
  ASSERT_FALSE(StartsWith(abcdef, def));
}

TEST(EndsWith, Basics) {
  std::string empty{};
  std::string abc{"abc"};
  std::string abcdef{"abcdef"};
  std::string def{"def"};
  ASSERT_TRUE(EndsWith(empty, empty));
  ASSERT_TRUE(EndsWith(abc, empty));
  ASSERT_TRUE(EndsWith(abc, abc));
  ASSERT_TRUE(EndsWith(abcdef, def));
  ASSERT_FALSE(EndsWith(abcdef, abc));
  ASSERT_FALSE(EndsWith(def, abcdef));
  ASSERT_FALSE(EndsWith(abcdef, abc));
}

TEST(RegexMatch, Basics) {
  std::regex regex("a+(b*)(c+)d+");
  std::string_view b, c;

  ASSERT_FALSE(RegexMatch(regex, "", {&b, &c}));
  ASSERT_FALSE(RegexMatch(regex, "ad", {&b, &c}));
  ASSERT_FALSE(RegexMatch(regex, "bc", {&b, &c}));

  auto check_match = [&](std::string_view target, std::string_view expected_b,
                         std::string_view expected_c) {
    b = c = "!!!";  // dummy init value
    ASSERT_TRUE(RegexMatch(regex, target, {&b, &c}));
    ASSERT_EQ(b, expected_b);
    ASSERT_EQ(c, expected_c);
  };

  check_match("abcd", "b", "c");
  check_match("acd", "", "c");
  check_match("abbcccd", "bb", "ccc");
}

TEST(ToChars, Integers) {
  ASSERT_EQ(ToChars(static_cast<char>(0)), "0");
  ASSERT_EQ(ToChars(static_cast<unsigned char>(0)), "0");
  ASSERT_EQ(ToChars(static_cast<int8_t>(0)), "0");
  ASSERT_EQ(ToChars(static_cast<uint64_t>(0)), "0");
  ASSERT_EQ(ToChars(1234), "1234");
  ASSERT_EQ(ToChars(-5678), "-5678");

  if constexpr (have_to_chars<int>) {
    ASSERT_EQ(ToChars(1234, /*base=*/2), "10011010010");
  }

  // Beyond pre-allocated result size
  ASSERT_EQ(ToChars(9223372036854775807LL), "9223372036854775807");
  ASSERT_EQ(ToChars(-9223372036854775807LL - 1), "-9223372036854775808");
  ASSERT_EQ(ToChars(18446744073709551615ULL), "18446744073709551615");

  if constexpr (have_to_chars<unsigned long long>) {  // NOLINT: runtime/int
    // Will overflow any small string optimization
    ASSERT_EQ(ToChars(18446744073709551615ULL, /*base=*/2),
              "1111111111111111111111111111111111111111111111111111111111111111");
  }
}

TEST(ToChars, FloatingPoint) {
  if constexpr (have_to_chars<double>) {
    ASSERT_EQ(ToChars(0.0f), "0");
    ASSERT_EQ(ToChars(0.0), "0");
    ASSERT_EQ(ToChars(-0.0), "-0");
    ASSERT_EQ(ToChars(0.25), "0.25");
    ASSERT_EQ(ToChars(-0.25f), "-0.25");

    ASSERT_EQ(ToChars(0.1111111111111111), "0.1111111111111111");

    // XXX Can't test std::chars_format as it's not defined by all standard libraries
    // and even "if constexpr" wouldn't prevent the failing lookup.
  } else {
    // If std::to_chars isn't implemented for floating-point types, we fall back
    // to std::to_string which may make ad hoc formatting choices, so we cannot
    // really test much about the result.
    auto result = ToChars(0.0f);
    ASSERT_TRUE(StartsWith(result, "0")) << result;
    result = ToChars(0.25);
    ASSERT_TRUE(StartsWith(result, "0.25")) << result;
  }
}

#if !defined(_WIN32) || defined(NDEBUG)

TEST(ToChars, LocaleIndependent) {
  if constexpr (have_to_chars<double>) {
    // French locale uses the comma as decimal point
    LocaleGuard locale_guard("fr_FR.UTF-8");

    ASSERT_EQ(ToChars(0.25), "0.25");
    ASSERT_EQ(ToChars(-0.25f), "-0.25");
  }
}

#endif  // _WIN32

}  // namespace internal
}  // namespace arrow

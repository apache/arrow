// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
//
// Licensed under the MIT License (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://opensource.org/licenses/MIT
//
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

#include "unittest.h"
#include "rapidjson/internal/regex.h"

using namespace rapidjson::internal;

TEST(Regex, Single) {
    Regex re("a");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("b"));
}

TEST(Regex, Concatenation) {
    Regex re("abc");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("abcd"));
}

TEST(Regex, Alternation1) {
    Regex re("abab|abbb");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abab"));
    EXPECT_TRUE(rs.Match("abbb"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("ababa"));
    EXPECT_FALSE(rs.Match("abb"));
    EXPECT_FALSE(rs.Match("abbbb"));
}

TEST(Regex, Alternation2) {
    Regex re("a|b|c");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("c"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match("ab"));
}

TEST(Regex, Parenthesis1) {
    Regex re("(ab)c");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("abcd"));
}

TEST(Regex, Parenthesis2) {
    Regex re("a(bc)");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("abcd"));
}

TEST(Regex, Parenthesis3) {
    Regex re("(a|b)(c|d)");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ac"));
    EXPECT_TRUE(rs.Match("ad"));
    EXPECT_TRUE(rs.Match("bc"));
    EXPECT_TRUE(rs.Match("bd"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("cd"));
}

TEST(Regex, ZeroOrOne1) {
    Regex re("a?");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match(""));
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, ZeroOrOne2) {
    Regex re("a?b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match("bb"));
    EXPECT_FALSE(rs.Match("ba"));
}

TEST(Regex, ZeroOrOne3) {
    Regex re("ab?");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match("bb"));
    EXPECT_FALSE(rs.Match("ba"));
}

TEST(Regex, ZeroOrOne4) {
    Regex re("a?b?");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match(""));
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_FALSE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match("bb"));
    EXPECT_FALSE(rs.Match("ba"));
    EXPECT_FALSE(rs.Match("abc"));
}

TEST(Regex, ZeroOrOne5) {
    Regex re("a(ab)?b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aabb"));
    EXPECT_FALSE(rs.Match("aab"));
    EXPECT_FALSE(rs.Match("abb"));
}

TEST(Regex, ZeroOrMore1) {
    Regex re("a*");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match(""));
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ab"));
}

TEST(Regex, ZeroOrMore2) {
    Regex re("a*b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aab"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("bb"));
}

TEST(Regex, ZeroOrMore3) {
    Regex re("a*b*");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match(""));
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("aa"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("bb"));
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aabb"));
    EXPECT_FALSE(rs.Match("ba"));
}

TEST(Regex, ZeroOrMore4) {
    Regex re("a(ab)*b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aabb"));
    EXPECT_TRUE(rs.Match("aababb"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, OneOrMore1) {
    Regex re("a+");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("aa"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ab"));
}

TEST(Regex, OneOrMore2) {
    Regex re("a+b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aab"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("b"));
}

TEST(Regex, OneOrMore3) {
    Regex re("a+b+");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ab"));
    EXPECT_TRUE(rs.Match("aab"));
    EXPECT_TRUE(rs.Match("abb"));
    EXPECT_TRUE(rs.Match("aabb"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("ba"));
}

TEST(Regex, OneOrMore4) {
    Regex re("a(ab)+b");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("aabb"));
    EXPECT_TRUE(rs.Match("aababb"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("ab"));
}

TEST(Regex, QuantifierExact1) {
    Regex re("ab{3}c");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbc"));
    EXPECT_FALSE(rs.Match("ac"));
    EXPECT_FALSE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match("abbc"));
    EXPECT_FALSE(rs.Match("abbbbc"));
}

TEST(Regex, QuantifierExact2) {
    Regex re("a(bc){3}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abcbcbcd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abcd"));
    EXPECT_FALSE(rs.Match("abcbcd"));
    EXPECT_FALSE(rs.Match("abcbcbcbcd"));
}

TEST(Regex, QuantifierExact3) {
    Regex re("a(b|c){3}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbd"));
    EXPECT_TRUE(rs.Match("acccd"));
    EXPECT_TRUE(rs.Match("abcbd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abbd"));
    EXPECT_FALSE(rs.Match("accccd"));
    EXPECT_FALSE(rs.Match("abbbbd"));
}

TEST(Regex, QuantifierMin1) {
    Regex re("ab{3,}c");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbc"));
    EXPECT_TRUE(rs.Match("abbbbc"));
    EXPECT_TRUE(rs.Match("abbbbbc"));
    EXPECT_FALSE(rs.Match("ac"));
    EXPECT_FALSE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match("abbc"));
}

TEST(Regex, QuantifierMin2) {
    Regex re("a(bc){3,}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abcbcbcd"));
    EXPECT_TRUE(rs.Match("abcbcbcbcd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abcd"));
    EXPECT_FALSE(rs.Match("abcbcd"));
}

TEST(Regex, QuantifierMin3) {
    Regex re("a(b|c){3,}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbd"));
    EXPECT_TRUE(rs.Match("acccd"));
    EXPECT_TRUE(rs.Match("abcbd"));
    EXPECT_TRUE(rs.Match("accccd"));
    EXPECT_TRUE(rs.Match("abbbbd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abbd"));
}

TEST(Regex, QuantifierMinMax1) {
    Regex re("ab{3,5}c");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbc"));
    EXPECT_TRUE(rs.Match("abbbbc"));
    EXPECT_TRUE(rs.Match("abbbbbc"));
    EXPECT_FALSE(rs.Match("ac"));
    EXPECT_FALSE(rs.Match("abc"));
    EXPECT_FALSE(rs.Match("abbc"));
    EXPECT_FALSE(rs.Match("abbbbbbc"));
}

TEST(Regex, QuantifierMinMax2) {
    Regex re("a(bc){3,5}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abcbcbcd"));
    EXPECT_TRUE(rs.Match("abcbcbcbcd"));
    EXPECT_TRUE(rs.Match("abcbcbcbcbcd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abcd"));
    EXPECT_FALSE(rs.Match("abcbcd"));
    EXPECT_FALSE(rs.Match("abcbcbcbcbcbcd"));
}

TEST(Regex, QuantifierMinMax3) {
    Regex re("a(b|c){3,5}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("abbbd"));
    EXPECT_TRUE(rs.Match("acccd"));
    EXPECT_TRUE(rs.Match("abcbd"));
    EXPECT_TRUE(rs.Match("accccd"));
    EXPECT_TRUE(rs.Match("abbbbd"));
    EXPECT_TRUE(rs.Match("acccccd"));
    EXPECT_TRUE(rs.Match("abbbbbd"));
    EXPECT_FALSE(rs.Match("ad"));
    EXPECT_FALSE(rs.Match("abbd"));
    EXPECT_FALSE(rs.Match("accccccd"));
    EXPECT_FALSE(rs.Match("abbbbbbd"));
}

// Issue538
TEST(Regex, QuantifierMinMax4) {
    Regex re("a(b|c){0,3}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ad"));
    EXPECT_TRUE(rs.Match("abd"));
    EXPECT_TRUE(rs.Match("acd"));
    EXPECT_TRUE(rs.Match("abbd"));
    EXPECT_TRUE(rs.Match("accd"));
    EXPECT_TRUE(rs.Match("abcd"));
    EXPECT_TRUE(rs.Match("abbbd"));
    EXPECT_TRUE(rs.Match("acccd"));
    EXPECT_FALSE(rs.Match("abbbbd"));
    EXPECT_FALSE(rs.Match("add"));
    EXPECT_FALSE(rs.Match("accccd"));
    EXPECT_FALSE(rs.Match("abcbcd"));
}

// Issue538
TEST(Regex, QuantifierMinMax5) {
    Regex re("a(b|c){0,}d");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("ad"));
    EXPECT_TRUE(rs.Match("abd"));
    EXPECT_TRUE(rs.Match("acd"));
    EXPECT_TRUE(rs.Match("abbd"));
    EXPECT_TRUE(rs.Match("accd"));
    EXPECT_TRUE(rs.Match("abcd"));
    EXPECT_TRUE(rs.Match("abbbd"));
    EXPECT_TRUE(rs.Match("acccd"));
    EXPECT_TRUE(rs.Match("abbbbd"));
    EXPECT_TRUE(rs.Match("accccd"));
    EXPECT_TRUE(rs.Match("abcbcd"));
    EXPECT_FALSE(rs.Match("add"));
    EXPECT_FALSE(rs.Match("aad"));
}

#define EURO "\xE2\x82\xAC" // "\xE2\x82\xAC" is UTF-8 rsquence of Euro sign U+20AC

TEST(Regex, Unicode) {
    Regex re("a" EURO "+b"); 
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a" EURO "b"));
    EXPECT_TRUE(rs.Match("a" EURO EURO "b"));
    EXPECT_FALSE(rs.Match("a?b"));
    EXPECT_FALSE(rs.Match("a" EURO "\xAC" "b")); // unaware of UTF-8 will match
}

TEST(Regex, AnyCharacter) {
    Regex re(".");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match(EURO));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, CharacterRange1) {
    Regex re("[abc]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("c"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("`"));
    EXPECT_FALSE(rs.Match("d"));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, CharacterRange2) {
    Regex re("[^abc]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("`"));
    EXPECT_TRUE(rs.Match("d"));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("c"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, CharacterRange3) {
    Regex re("[a-c]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("b"));
    EXPECT_TRUE(rs.Match("c"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("`"));
    EXPECT_FALSE(rs.Match("d"));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, CharacterRange4) {
    Regex re("[^a-c]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("`"));
    EXPECT_TRUE(rs.Match("d"));
    EXPECT_FALSE(rs.Match("a"));
    EXPECT_FALSE(rs.Match("b"));
    EXPECT_FALSE(rs.Match("c"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("aa"));
}

TEST(Regex, CharacterRange5) {
    Regex re("[-]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("-"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("a"));
}

TEST(Regex, CharacterRange6) {
    Regex re("[a-]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("-"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("`"));
    EXPECT_FALSE(rs.Match("b"));
}

TEST(Regex, CharacterRange7) {
    Regex re("[-a]");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("a"));
    EXPECT_TRUE(rs.Match("-"));
    EXPECT_FALSE(rs.Match(""));
    EXPECT_FALSE(rs.Match("`"));
    EXPECT_FALSE(rs.Match("b"));
}

TEST(Regex, CharacterRange8) {
    Regex re("[a-zA-Z0-9]*");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("Milo"));
    EXPECT_TRUE(rs.Match("MT19937"));
    EXPECT_TRUE(rs.Match("43"));
    EXPECT_FALSE(rs.Match("a_b"));
    EXPECT_FALSE(rs.Match("!"));
}

TEST(Regex, Search) {
    Regex re("abc");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Search("abc"));
    EXPECT_TRUE(rs.Search("_abc"));
    EXPECT_TRUE(rs.Search("abc_"));
    EXPECT_TRUE(rs.Search("_abc_"));
    EXPECT_TRUE(rs.Search("__abc__"));
    EXPECT_TRUE(rs.Search("abcabc"));
    EXPECT_FALSE(rs.Search("a"));
    EXPECT_FALSE(rs.Search("ab"));
    EXPECT_FALSE(rs.Search("bc"));
    EXPECT_FALSE(rs.Search("cba"));
}

TEST(Regex, Search_BeginAnchor) {
    Regex re("^abc");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Search("abc"));
    EXPECT_TRUE(rs.Search("abc_"));
    EXPECT_TRUE(rs.Search("abcabc"));
    EXPECT_FALSE(rs.Search("_abc"));
    EXPECT_FALSE(rs.Search("_abc_"));
    EXPECT_FALSE(rs.Search("a"));
    EXPECT_FALSE(rs.Search("ab"));
    EXPECT_FALSE(rs.Search("bc"));
    EXPECT_FALSE(rs.Search("cba"));
}

TEST(Regex, Search_EndAnchor) {
    Regex re("abc$");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Search("abc"));
    EXPECT_TRUE(rs.Search("_abc"));
    EXPECT_TRUE(rs.Search("abcabc"));
    EXPECT_FALSE(rs.Search("abc_"));
    EXPECT_FALSE(rs.Search("_abc_"));
    EXPECT_FALSE(rs.Search("a"));
    EXPECT_FALSE(rs.Search("ab"));
    EXPECT_FALSE(rs.Search("bc"));
    EXPECT_FALSE(rs.Search("cba"));
}

TEST(Regex, Search_BothAnchor) {
    Regex re("^abc$");
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Search("abc"));
    EXPECT_FALSE(rs.Search(""));
    EXPECT_FALSE(rs.Search("a"));
    EXPECT_FALSE(rs.Search("b"));
    EXPECT_FALSE(rs.Search("ab"));
    EXPECT_FALSE(rs.Search("abcd"));
}

TEST(Regex, Escape) {
    const char* s = "\\^\\$\\|\\(\\)\\?\\*\\+\\.\\[\\]\\{\\}\\\\\\f\\n\\r\\t\\v[\\b][\\[][\\]]";
    Regex re(s);
    ASSERT_TRUE(re.IsValid());
    RegexSearch rs(re);
    EXPECT_TRUE(rs.Match("^$|()?*+.[]{}\\\x0C\n\r\t\x0B\b[]"));
    EXPECT_FALSE(rs.Match(s)); // Not escaping
}

TEST(Regex, Invalid) {
#define TEST_INVALID(s) \
    {\
        Regex re(s);\
        EXPECT_FALSE(re.IsValid());\
    }

    TEST_INVALID("");
    TEST_INVALID("a|");
    TEST_INVALID("()");
    TEST_INVALID("(");
    TEST_INVALID(")");
    TEST_INVALID("(a))");
    TEST_INVALID("(a|)");
    TEST_INVALID("(a||b)");
    TEST_INVALID("(|b)");
    TEST_INVALID("?");
    TEST_INVALID("*");
    TEST_INVALID("+");
    TEST_INVALID("{");
    TEST_INVALID("{}");
    TEST_INVALID("a{a}");
    TEST_INVALID("a{0}");
    TEST_INVALID("a{-1}");
    TEST_INVALID("a{}");
    // TEST_INVALID("a{0,}");   // Support now
    TEST_INVALID("a{,0}");
    TEST_INVALID("a{1,0}");
    TEST_INVALID("a{-1,0}");
    TEST_INVALID("a{-1,1}");
    TEST_INVALID("a{4294967296}"); // overflow of unsigned
    TEST_INVALID("a{1a}");
    TEST_INVALID("[");
    TEST_INVALID("[]");
    TEST_INVALID("[^]");
    TEST_INVALID("[\\a]");
    TEST_INVALID("\\a");

#undef TEST_INVALID
}

TEST(Regex, Issue538) {
    Regex re("^[0-9]+(\\\\.[0-9]+){0,2}");
    EXPECT_TRUE(re.IsValid());
}

TEST(Regex, Issue583) {
    Regex re("[0-9]{99999}");
    ASSERT_TRUE(re.IsValid());
}

#undef EURO

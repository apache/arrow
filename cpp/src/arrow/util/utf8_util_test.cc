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

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/string.h"
#include "arrow/util/utf8_internal.h"

namespace arrow {
namespace util {

class UTF8Test : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    InitializeUTF8();

    all_valid_sequences.clear();
    for (const auto& v :
         {valid_sequences_1, valid_sequences_2, valid_sequences_3, valid_sequences_4}) {
      all_valid_sequences.insert(all_valid_sequences.end(), v.begin(), v.end());
    }

    all_invalid_sequences.clear();
    for (const auto& v : {invalid_sequences_1, invalid_sequences_2, invalid_sequences_3,
                          invalid_sequences_4}) {
      all_invalid_sequences.insert(all_invalid_sequences.end(), v.begin(), v.end());
    }
  }

  static std::vector<std::string> valid_sequences_1;
  static std::vector<std::string> valid_sequences_2;
  static std::vector<std::string> valid_sequences_3;
  static std::vector<std::string> valid_sequences_4;

  static std::vector<std::string> all_valid_sequences;

  static std::vector<std::string> invalid_sequences_1;
  static std::vector<std::string> invalid_sequences_2;
  static std::vector<std::string> invalid_sequences_3;
  static std::vector<std::string> invalid_sequences_4;

  static std::vector<std::string> all_invalid_sequences;

  static std::vector<std::string> valid_sequences_ascii;
  static std::vector<std::string> invalid_sequences_ascii;
};

std::vector<std::string> UTF8Test::valid_sequences_1 = {"a", "\x7f",
                                                        std::string("\0", 1)};
std::vector<std::string> UTF8Test::valid_sequences_2 = {"\xc2\x80", "\xc3\xbf",
                                                        "\xdf\xbf"};
std::vector<std::string> UTF8Test::valid_sequences_3 = {"\xe0\xa0\x80", "\xe8\x9d\xa5",
                                                        "\xef\xbf\xbf"};
std::vector<std::string> UTF8Test::valid_sequences_4 = {
    "\xf0\x90\x80\x80", "\xf0\x9f\xbf\xbf", "\xf4\x80\x80\x80", "\xf4\x8f\xbf\xbf"};

std::vector<std::string> UTF8Test::all_valid_sequences;

std::vector<std::string> UTF8Test::invalid_sequences_1 = {"\x80", "\xa0", "\xbf", "\xc0",
                                                          "\xc1"};
std::vector<std::string> UTF8Test::invalid_sequences_2 = {
    "\x80\x80", "\x80\xbf", "\xbf\x80", "\xbf\xbf",
    "\xc1\x80", "\xc2\x7f", "\xc3\xff", "\xdf\xc0"};
std::vector<std::string> UTF8Test::invalid_sequences_3 = {
    "\xe0\x80\x80", "\xe0\x9f\x80", "\xef\xbf\xc0", "\xef\xc0\xbf", "\xef\xff\xff",
    // Surrogates
    "\xed\xa0\x80", "\xed\xbf\xbf"};
std::vector<std::string> UTF8Test::invalid_sequences_4 = {
    "\xf0\x80\x80\x80", "\xf0\x8f\x80\x80", "\xf4\x8f\xbf\xc0", "\xf4\x8f\xc0\xbf",
    "\xf4\x90\x80\x80"};

std::vector<std::string> UTF8Test::all_invalid_sequences;

std::vector<std::string> UTF8Test::valid_sequences_ascii = {"a", "\x7f", "B", "&"};
std::vector<std::string> UTF8Test::invalid_sequences_ascii = {
    "\x80", "\xa0\x1e", "\xbf\xef\x6a", "\xc1\x9f\xc3\xd9"};

class UTF8ValidationTest : public UTF8Test {};

class ASCIIValidationTest : public UTF8Test {};

::testing::AssertionResult IsValidUTF8(const std::string& s) {
  if (ValidateUTF8(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure()
           << "string '" << h << "' didn't validate as UTF8";
  }
}

::testing::AssertionResult IsInvalidUTF8(const std::string& s) {
  if (!ValidateUTF8(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure() << "string '" << h << "' validated as UTF8";
  }
}

::testing::AssertionResult IsValidASCII(const std::string& s) {
  if (ValidateAscii(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure()
           << "string '" << h << "' didn't validate as ASCII";
  }
}

::testing::AssertionResult IsInvalidASCII(const std::string& s) {
  if (!ValidateAscii(reinterpret_cast<const uint8_t*>(s.data()), s.size())) {
    return ::testing::AssertionSuccess();
  } else {
    std::string h = HexEncode(reinterpret_cast<const uint8_t*>(s.data()),
                              static_cast<int32_t>(s.size()));
    return ::testing::AssertionFailure() << "string '" << h << "' validated as ASCII";
  }
}

template <typename ValidationFunc>
void ValidateWithPrefixes(ValidationFunc&& validate, const std::string& s) {
  // Exercise SIMD optimizations
  for (int prefix_size = 1; prefix_size < 64; ++prefix_size) {
    std::string longer(prefix_size, 'x');
    longer.append(s);
    ASSERT_TRUE(validate(longer));
    longer.append(prefix_size, 'y');
    ASSERT_TRUE(validate(longer));
  }
}

void AssertValidUTF8(const std::string& s) { ASSERT_TRUE(IsValidUTF8(s)); }

void AssertInvalidUTF8(const std::string& s) { ASSERT_TRUE(IsInvalidUTF8(s)); }

void AssertValidASCII(const std::string& s) {
  ASSERT_TRUE(IsValidASCII(s));
  ValidateWithPrefixes(IsValidASCII, s);
}

void AssertInvalidASCII(const std::string& s) {
  ASSERT_TRUE(IsInvalidASCII(s));
  ValidateWithPrefixes(IsInvalidASCII, s);
}

TEST_F(ASCIIValidationTest, AsciiValid) {
  for (const auto& s : valid_sequences_ascii) {
    AssertValidASCII(s);
  }
}

TEST_F(ASCIIValidationTest, AsciiInvalid) {
  for (const auto& s : invalid_sequences_ascii) {
    AssertInvalidASCII(s);
  }
}

TEST_F(UTF8ValidationTest, EmptyString) { AssertValidUTF8(""); }

TEST_F(UTF8ValidationTest, OneCharacterValid) {
  for (const auto& s : all_valid_sequences) {
    AssertValidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterValid) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_valid_sequences) {
      AssertValidUTF8(s1 + s2);
    }
  }
}

TEST_F(UTF8ValidationTest, RandomValid) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    for (int j = 0; j < nchars; ++j) {
      s += all_valid_sequences[valid_dist(gen)];
    }
    AssertValidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, OneCharacterTruncated) {
  for (const auto& s : all_valid_sequences) {
    if (s.size() > 1) {
      AssertInvalidUTF8(s.substr(0, s.size() - 1));
    }
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterTruncated) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_valid_sequences) {
      if (s2.size() > 1) {
        AssertInvalidUTF8(s1 + s2.substr(0, s2.size() - 1));
        AssertInvalidUTF8(s2.substr(0, s2.size() - 1) + s1);
      }
    }
  }
}

TEST_F(UTF8ValidationTest, OneCharacterInvalid) {
  for (const auto& s : all_invalid_sequences) {
    AssertInvalidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, TwoCharacterInvalid) {
  for (const auto& s1 : all_valid_sequences) {
    for (const auto& s2 : all_invalid_sequences) {
      AssertInvalidUTF8(s1 + s2);
      AssertInvalidUTF8(s2 + s1);
    }
  }
  for (const auto& s1 : all_invalid_sequences) {
    for (const auto& s2 : all_invalid_sequences) {
      AssertInvalidUTF8(s1 + s2);
    }
  }
}

TEST_F(UTF8ValidationTest, RandomInvalid) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);
  std::uniform_int_distribution<int> invalid_pos_dist(0, nchars - 1);
  std::uniform_int_distribution<size_t> invalid_dist(0, all_invalid_sequences.size() - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    // Stuff a single invalid sequence somewhere in a valid UTF8 stream
    int invalid_pos = invalid_pos_dist(gen);
    for (int j = 0; j < nchars; ++j) {
      if (j == invalid_pos) {
        s += all_invalid_sequences[invalid_dist(gen)];
      } else {
        s += all_valid_sequences[valid_dist(gen)];
      }
    }
    AssertInvalidUTF8(s);
  }
}

TEST_F(UTF8ValidationTest, RandomTruncated) {
#ifdef ARROW_VALGRIND
  const int niters = 50;
#else
  const int niters = 1000;
#endif
  const int nchars = 100;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<size_t> valid_dist(0, all_valid_sequences.size() - 1);
  std::uniform_int_distribution<int> invalid_pos_dist(0, nchars - 1);

  for (int i = 0; i < niters; ++i) {
    std::string s;
    s.reserve(nchars * 4);
    // Truncate a single sequence somewhere in a valid UTF8 stream
    int invalid_pos = invalid_pos_dist(gen);
    for (int j = 0; j < nchars; ++j) {
      if (j == invalid_pos) {
        while (true) {
          // Ensure we truncate a 2-byte or more sequence
          const std::string& t = all_valid_sequences[valid_dist(gen)];
          if (t.size() > 1) {
            s += t.substr(0, t.size() - 1);
            break;
          }
        }
      } else {
        s += all_valid_sequences[valid_dist(gen)];
      }
    }
    AssertInvalidUTF8(s);
  }
}

TEST(SkipUTF8BOM, Basics) {
  auto CheckOk = [](const std::string& s, size_t expected_offset) -> void {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* res;
    ASSERT_OK_AND_ASSIGN(res, SkipUTF8BOM(data, static_cast<int64_t>(s.size())));
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res - data, expected_offset);
  };

  auto CheckTruncated = [](const std::string& s) -> void {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(s.data());
    ASSERT_RAISES(Invalid, SkipUTF8BOM(data, static_cast<int64_t>(s.size())));
  };

  CheckOk("", 0);
  CheckOk("a", 0);
  CheckOk("ab", 0);
  CheckOk("abc", 0);
  CheckOk("abcd", 0);
  CheckOk("\xc3\xa9", 0);
  CheckOk("\xee", 0);
  CheckOk("\xef\xbc", 0);
  CheckOk("\xef\xbb\xbe", 0);
  CheckOk("\xef\xbb\xbf", 3);
  CheckOk("\xef\xbb\xbfx", 3);

  CheckTruncated("\xef");
  CheckTruncated("\xef\xbb");
}

// Currently a known issue with Valgrind and wide string AVX2 instructions
// https://sourceware.org/bugzilla/show_bug.cgi?id=22954
#ifndef ARROW_VALGRIND
TEST(UTF8ToWideString, Basics) {
  auto CheckOk = [](const std::string& s, const std::wstring& expected) -> void {
    ASSERT_OK_AND_ASSIGN(std::wstring ws, UTF8ToWideString(s));
    ASSERT_EQ(ws, expected);
  };

  auto CheckInvalid = [](const std::string& s) -> void {
    ASSERT_RAISES(Invalid, UTF8ToWideString(s));
  };

  CheckOk("", L"");
  CheckOk("foo", L"foo");
  CheckOk("h\xc3\xa9h\xc3\xa9", L"h\u00e9h\u00e9");
  CheckOk("\xf0\x9f\x98\x80", L"\U0001F600");
  CheckOk("\xf4\x8f\xbf\xbf", L"\U0010FFFF");
  CheckOk({0, 'x'}, {0, L'x'});

  CheckInvalid("\xff");
  CheckInvalid("h\xc3");
}
#endif

TEST(WideStringToUTF8, Basics) {
  auto CheckOk = [](const std::wstring& ws, const std::string& expected) -> void {
    ASSERT_OK_AND_ASSIGN(std::string s, WideStringToUTF8(ws));
    ASSERT_EQ(s, expected);
  };

  auto CheckInvalid = [](const std::wstring& ws) -> void {
    ASSERT_RAISES(Invalid, WideStringToUTF8(ws));
  };

  CheckOk(L"", "");
  CheckOk(L"foo", "foo");
  CheckOk(L"h\u00e9h\u00e9", "h\xc3\xa9h\xc3\xa9");
  CheckOk(L"\U0001F600", "\xf0\x9f\x98\x80");
  CheckOk(L"\U0010FFFF", "\xf4\x8f\xbf\xbf");
  CheckOk({0, L'x'}, {0, 'x'});

  // Lone surrogate
  CheckInvalid({0xD800});
  CheckInvalid({0xDFFF});
  // Invalid code point
#if WCHAR_MAX > 0xFFFF
  CheckInvalid({0x110000});
#endif
}

TEST(UTF8DecodeReverse, Basics) {
  auto CheckOk = [](const std::string& s) -> void {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.c_str());
    const uint8_t* end = begin + s.length();
    const uint8_t* i = end - 1;
    uint32_t codepoint;
    EXPECT_TRUE(UTF8DecodeReverse(&i, &codepoint));
    EXPECT_EQ(i, begin - 1);
  };

  // 0x80 == 0b10000000
  // 0xC0 == 0b11000000
  // 0xE0 == 0b11100000
  // 0xF0 == 0b11110000
  CheckOk("a");
  CheckOk("\xC0\x80");
  CheckOk("\xE0\x80\x80");
  CheckOk("\xF0\x80\x80\x80");

  auto CheckInvalid = [](const std::string& s) -> void {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.c_str());
    const uint8_t* end = begin + s.length();
    const uint8_t* i = end - 1;
    uint32_t codepoint;
    EXPECT_FALSE(UTF8DecodeReverse(&i, &codepoint));
  };

  // too many continuation code units
  CheckInvalid("a\x80");
  CheckInvalid("\xC0\x80\x80");
  CheckInvalid("\xE0\x80\x80\x80");
  CheckInvalid("\xF0\x80\x80\x80\x80");
  // not enough continuation code units
  CheckInvalid("\xC0");
  CheckInvalid("\xE0\x80");
  CheckInvalid("\xF0\x80\x80");
}

TEST(UTF8FindIf, Basics) {
  auto CheckOk = [](const std::string& s, unsigned char test, int64_t offset_left,
                    int64_t offset_right) -> void {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.c_str());
    const uint8_t* end = begin + s.length();
    std::reverse_iterator<const uint8_t*> rbegin(end);
    std::reverse_iterator<const uint8_t*> rend(begin);
    const uint8_t* left = nullptr;
    const uint8_t* right = nullptr;
    auto predicate = [&](uint32_t c) { return c == test; };
    EXPECT_TRUE(UTF8FindIf(begin, end, predicate, &left));
    EXPECT_TRUE(UTF8FindIfReverse(begin, end, predicate, &right));
    EXPECT_EQ(offset_left, left - begin);
    EXPECT_EQ(offset_right, right - begin);
    EXPECT_EQ(std::find_if(begin, end, predicate) - begin, left - begin);
    EXPECT_EQ(std::find_if(rbegin, rend, predicate).base() - begin, right - begin);
  };
  auto CheckOkUTF8 = [](const std::string& s, uint32_t test, int64_t offset_left,
                        int64_t offset_right) -> void {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.c_str());
    const uint8_t* end = begin + s.length();
    std::reverse_iterator<const uint8_t*> rbegin(end);
    std::reverse_iterator<const uint8_t*> rend(begin);
    const uint8_t* left = nullptr;
    const uint8_t* right = nullptr;
    auto predicate = [&](uint32_t c) { return c == test; };
    EXPECT_TRUE(UTF8FindIf(begin, end, predicate, &left));
    EXPECT_TRUE(UTF8FindIfReverse(begin, end, predicate, &right));
    EXPECT_EQ(offset_left, left - begin);
    EXPECT_EQ(offset_right, right - begin);
    // we cannot check the unicode version with find_if semantics, because it's byte based
    // EXPECT_EQ(std::find_if(begin, end, predicate) - begin, left - begin);
    // EXPECT_EQ(std::find_if(rbegin, rend, predicate).base() - begin, right - begin);
  };

  CheckOk("aaaba", 'a', 0, 5);
  CheckOkUTF8("aaaβa", 'a', 0, 6);

  CheckOk("aaaba", 'b', 3, 4);
  CheckOkUTF8("aaaβa", U'β', 3, 5);

  CheckOk("aaababa", 'b', 3, 6);
  CheckOkUTF8("aaaβaβa", U'β', 3, 8);

  CheckOk("aaababa", 'c', 7, 0);
  CheckOk("aaaβaβa", 'c', 9, 0);
  CheckOkUTF8("aaaβaβa", U'ɑ', 9, 0);

  CheckOk("a", 'a', 0, 1);
  CheckOkUTF8("ɑ", U'ɑ', 0, 2);

  CheckOk("a", 'b', 1, 0);
  CheckOkUTF8("ɑ", 'b', 2, 0);

  CheckOk("", 'b', 0, 0);
  CheckOkUTF8("", U'β', 0, 0);
}

TEST(UTF8Length, Basics) {
  auto length = [](const std::string& s) {
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    return UTF8Length(p, p + s.length());
  };
  ASSERT_EQ(length("abcde"), 5);
  // accented a encoded as a single codepoint
  ASSERT_EQ(length("\xc3\x81"
                   "bcde"),
            5);
  // accented a encoded as two codepoints via combining character
  ASSERT_EQ(length("a\xcc\x81"
                   "bcde"),
            6);
  // hiragana a (3 bytes)
  ASSERT_EQ(length("\xe3\x81\x81"), 1);
  // raised hands emoji (4 bytes)
  ASSERT_EQ(length("\xf0\x9f\x99\x8c"), 1);
}

}  // namespace util
}  // namespace arrow

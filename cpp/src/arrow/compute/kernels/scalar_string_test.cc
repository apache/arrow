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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace compute {

// interesting utf8 characters for testing (lower case / upper case):
//  * ῦ / Υ͂ (3 to 4 code units) (Note, we don't support this yet, utf8proc does not use
//  SpecialCasing.txt)
//  * ɑ / Ɑ (2 to 3 code units)
//  * ı / I (2 to 1 code units)
//  * Ⱥ / ⱥ (2 to 3 code units)

template <typename TestType>
class BaseTestStringKernels : public ::testing::Test {
 protected:
  using OffsetType = typename TypeTraits<TestType>::OffsetType;
  using ScalarType = typename TypeTraits<TestType>::ScalarType;

  void CheckUnary(std::string func_name, std::string json_input,
                  std::shared_ptr<DataType> out_ty, std::string json_expected,
                  const FunctionOptions* options = nullptr) {
    CheckScalarUnary(func_name, type(), json_input, out_ty, json_expected, options);
  }

  void CheckUnary(std::string func_name, const std::shared_ptr<Array>& input,
                  std::shared_ptr<DataType> out_ty, std::string json_expected,
                  const FunctionOptions* options = nullptr) {
    CheckScalar(func_name, {Datum(input)}, Datum(ArrayFromJSON(out_ty, json_expected)),
                options);
  }

  void CheckUnary(std::string func_name, const std::shared_ptr<Array>& input,
                  const std::shared_ptr<Array>& expected,
                  const FunctionOptions* options = nullptr) {
    CheckScalar(func_name, {Datum(input)}, Datum(expected), options);
  }

  void CheckVarArgsScalar(std::string func_name, std::string json_input,
                          std::shared_ptr<DataType> out_ty, std::string json_expected,
                          const FunctionOptions* options = nullptr) {
    // CheckScalar (on arrays) checks scalar arguments individually,
    // but this lets us test the all-scalar case explicitly
    ScalarVector inputs;
    std::shared_ptr<Array> args = ArrayFromJSON(type(), json_input);
    for (int64_t i = 0; i < args->length(); i++) {
      ASSERT_OK_AND_ASSIGN(auto scalar, args->GetScalar(i));
      inputs.push_back(std::move(scalar));
    }
    CheckScalar(func_name, inputs, ScalarFromJSON(out_ty, json_expected), options);
  }

  void CheckVarArgs(std::string func_name, const DatumVector& inputs,
                    std::shared_ptr<DataType> out_ty, std::string json_expected,
                    const FunctionOptions* options = nullptr) {
    CheckScalar(func_name, inputs, ArrayFromJSON(out_ty, json_expected), options);
  }

  std::shared_ptr<DataType> type() { return TypeTraits<TestType>::type_singleton(); }

  template <typename CType>
  std::shared_ptr<ScalarType> scalar(CType value) {
    return std::make_shared<ScalarType>(value);
  }

  std::shared_ptr<DataType> offset_type() {
    return TypeTraits<OffsetType>::type_singleton();
  }

  template <typename CType = const char*>
  std::shared_ptr<Array> MakeArray(const std::vector<CType>& values,
                                   const std::vector<bool>& is_valid = {}) {
    return _MakeArray<TestType, CType>(type(), values, is_valid);
  }
};

template <typename TestType>
class TestBaseBinaryKernels : public BaseTestStringKernels<TestType> {};

TYPED_TEST_SUITE(TestBaseBinaryKernels, BaseBinaryArrowTypes);

template <typename TestType>
class TestBinaryKernels : public BaseTestStringKernels<TestType> {};

TYPED_TEST_SUITE(TestBinaryKernels, BinaryArrowTypes);

template <typename TestType>
class TestStringKernels : public BaseTestStringKernels<TestType> {};

TYPED_TEST_SUITE(TestStringKernels, StringArrowTypes);

TYPED_TEST(TestBaseBinaryKernels, BinaryLength) {
  this->CheckUnary("binary_length", R"(["aaa", null, "áéíóú", "", "b"])",
                   this->offset_type(), "[3, null, 10, 0, 1]");

  // Invalid UTF-8 inputs
  this->CheckUnary("binary_length", this->MakeArray({"\xf7\x0f\xab", "\xff\x9b\xc3\xbb"}),
                   this->offset_type(), "[3, 4]");

  // Invalid UTF-8 inputs with null bytes
  this->CheckUnary("binary_length",
                   this->template MakeArray<std::string>(
                       {{"\xf7\x00\xab", 3}, {"\x00\x9b\x00\xbb", 4}, {"\x00\x00", 2}}),
                   this->offset_type(), "[3, 4, 2]");
}

// The NonUtf8XXX tests use kernels that do not accept invalid UTF-8 when
// processing [Large]StringType data. These tests use invalid UTF-8 inputs.
TYPED_TEST(TestBinaryKernels, NonUtf8) {
#ifdef ARROW_WITH_RE2
  for (auto ignore_case : {true, false}) {
#else
  for (auto ignore_case : {false}) {
#endif
    MatchSubstringOptions options("\xfc\x40", ignore_case);
    this->CheckUnary(
        "find_substring",
        this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab", "\x01\xfc\x41"}),
        this->offset_type(), "[0, 2, -1]", &options);

    options.pattern = "\x40";
    this->CheckUnary(
        "find_substring",
        this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab", "\x01\xfc\x41"}),
        this->offset_type(), "[1, 3, -1]", &options);

    options.pattern = "\xfc\x40";
    this->CheckUnary("count_substring",
                     this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab",
                                      "\x01\xfc\x41", "\x01\xfc\x40\x40\xfc\x40\xab"}),
                     this->offset_type(), "[1, 1, 0, 2]", &options);

    options.pattern = "\xfc\x40";
    this->CheckUnary("match_substring",
                     this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab",
                                      "\x01\xfc\x41", "\x01\xfc\x40\x40\xfc\x40\xab"}),
                     boolean(), "[true, true, false, true]", &options);

    options.pattern = "\xfc\x40";
    this->CheckUnary("starts_with",
                     this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab",
                                      "\x01\xfc\x41", "\x01\xfc\x40\x40\xfc\x40\xab"}),
                     boolean(), "[true, false, false, false]", &options);

    options.pattern = "\xfc\x40";
    this->CheckUnary("ends_with",
                     this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab",
                                      "\x01\xfc\x41", "\x01\xfc\x40\x40\xfc\x40"}),
                     boolean(), "[false, false, false, true]", &options);
  }
  {
    // "foo<non-UTF8>bar" = \x66\x6f\x6f\xfc\x62\x61\x72
    SplitPatternOptions options("\xfc");
    this->CheckUnary("split_pattern",
                     this->MakeArray({"\x66\x6f\x6f\xfc\x62\x61\x72", "foo"}),
                     list(this->type()), R"([["foo", "bar"], ["foo"]])", &options);
  }
  {
    ReplaceSubstringOptions options("\xfc\x40", "bazz", 1);
    this->CheckUnary("replace_substring",
                     this->MakeArray({"\xfc\x40", "this \xfc\x40 that \xfc\x40"}),
                     this->MakeArray({"bazz", "this bazz that \xfc\x40"}), &options);
  }
  {
    ReplaceSliceOptions options(1, 2, "\xfc\x40");
    this->CheckUnary(
        "binary_replace_slice", this->MakeArray({"\xf7\x0f\xab", "\xff\x9b\xc3\xbb"}),
        this->MakeArray({"\xf7\xfc\x40\xab", "\xff\xfc\x40\xc3\xbb"}), &options);
  }
}

TYPED_TEST(TestBinaryKernels, NonUtf8WithNull) {
#ifdef ARROW_WITH_RE2
  for (auto ignore_case : {true, false}) {
#else
  for (auto ignore_case : {false}) {
#endif
    MatchSubstringOptions options{std::string("\x00\x40", 2), ignore_case};
    this->CheckUnary(
        "find_substring",
        this->template MakeArray<std::string>(
            {{"\x00\x40\xab", 3}, {"\x00\x9b\x00\x40\xab", 5}, {"\x40\x00\x41", 3}}),
        this->offset_type(), "[0, 2, -1]", &options);

    this->CheckUnary(
        "count_substring",
        this->template MakeArray<std::string>({{"\x00\x40\xab", 3},
                                               {"\x01\xfc\x41", 3},
                                               {"\x01\x00\x00\x40\x00\x40\xab", 7}}),
        this->offset_type(), "[1, 0, 2]", &options);

    this->CheckUnary(
        "match_substring",
        this->template MakeArray<std::string>({{"\x00\x40\xab", 3},
                                               {"\x00\xfc\x41", 3},
                                               {"\x01\xfc\x00\x40\x00\x40\xab", 7}}),
        boolean(), "[true, false, true]", &options);

    this->CheckUnary(
        "starts_with",
        this->template MakeArray<std::string>(
            {{"\x00\x40\xab", 3}, {"\x01\xfc\x41", 3}, {"\x00\x00\x00\x00\x00\x40", 6}}),
        boolean(), "[true, false, false]", &options);

    this->CheckUnary(
        "ends_with",
        this->template MakeArray<std::string>(
            {{"\x00\x40\xab", 3}, {"\x01\xfc\x41", 3}, {"\x00\x00\x00\x00\x00\x40", 6}}),
        boolean(), "[false, false, true]", &options);
  }
  {
    // "foo<non-UTF8>bar" = \x66\x6f\x6f\xfc\x62\x61\x72
    SplitPatternOptions options(std::string("\xfc\x00", 2));
    this->CheckUnary(
        "split_pattern",
        this->template MakeArray<std::string>({{"\x66\x6f\x6f\xfc\x00\x62\x61\x72", 8}}),
        list(this->type()), R"([["foo", "bar"]])", &options);
  }
  {
    ReplaceSubstringOptions options(std::string("\x00\x40", 2), "bazz", 1);
    this->CheckUnary("replace_substring",
                     this->template MakeArray<std::string>({{"\x00\x40", 2}}),
                     this->type(), R"(["bazz"])", &options);
  }
  {
    ReplaceSliceOptions options(1, 2, std::string("\x00\x40", 2));
    this->CheckUnary("binary_replace_slice",
                     this->template MakeArray<std::string>(
                         {{"\x00\x0f\xab", 3}, {"\x00\x9b\xc3\xbb", 4}}),
                     this->template MakeArray<std::string>(
                         {{"\x00\x00\x40\xab", 4}, {"\x00\x00\x40\xc3\xbb", 5}}),
                     &options);
  }
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBinaryKernels, NonUtf8Regex) {
  for (auto ignore_case : {true, false}) {
    MatchSubstringOptions options("\xfc\x40", ignore_case);
    options.pattern = "\x40+";
    this->CheckUnary(
        "find_substring_regex",
        this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab", "\x01\xfc\x41"}),
        this->offset_type(), "[1, 3, -1]", &options);

    options.pattern = "\x40*\x41";
    this->CheckUnary("count_substring_regex",
                     this->MakeArray({"\xfc\x42\xab", "\xff\x9b\x40\x41\xab",
                                      "\x01\x41\x41", "\x01\x40\x41\x40\x40\x41\xab"}),
                     this->offset_type(), "[0, 1, 2, 2]", &options);

    options.pattern = "\xfc*\xab";
    this->CheckUnary("match_substring_regex",
                     this->MakeArray({"\xfc\x42\xab", "\xff\x9b\x40\x41\xab",
                                      "\x01\x41\x41", "\x01\x40\x41\x40\x40\x41\xab"}),
                     boolean(), "[true, true, false, true]", &options);

    options.pattern = "%\xfc\x40";
    this->CheckUnary("match_like",
                     this->MakeArray({"\xfc\x40\xab", "\xff\x9b\xfc\x40\xab",
                                      "\x01\xfc\x41", "\x01\xfc\x40\x40\xfc\x40"}),
                     boolean(), "[false, false, false, true]", &options);
  }
  {
    // "foo<non-UTF8>bar" = \x66\x6f\x6f\xfc\x62\x61\x72
    SplitPatternOptions options("\xfc");
    this->CheckUnary("split_pattern_regex",
                     this->MakeArray({"\x66\x6f\x6f\xfc\x62\x61\x72", "foo"}),
                     list(this->type()), R"([["foo", "bar"], ["foo"]])", &options);

    options.pattern = "\xfc+|\x10";
    this->CheckUnary("split_pattern_regex",
                     this->MakeArray({"\x66\xfc\xfc\x6f\xfc\x62\x10\x72", "bar"}),
                     list(this->type()), R"([["f", "o", "b", "r"], ["bar"]])", &options);
  }
  {
    ReplaceSubstringOptions options("\xfc\x40", "bazz", 1);
    this->CheckUnary("replace_substring_regex",
                     this->MakeArray({"\xfc\x40", "this \xfc\x40 that \xfc\x40"}),
                     this->MakeArray({"bazz", "this bazz that \xfc\x40"}), &options);
  }
  {
    ExtractRegexOptions options("(?P<letter>[\\xfc])(?P<digit>\\d)");
    auto null_bitmap = std::make_shared<Buffer>("0");
    auto output = StructArray::Make(
        {this->MakeArray({"\xfc", "1"}), this->MakeArray({"\xfc", "2"})},
        {field("letter", this->type()), field("digit", this->type())}, null_bitmap);
    this->CheckUnary("extract_regex", this->MakeArray({"foo\xfc 1bar", "\x02\xfc\x40"}),
                     std::static_pointer_cast<Array>(*output), &options);
  }
}

TYPED_TEST(TestBinaryKernels, NonUtf8WithNullRegex) {
  for (auto ignore_case : {true, false}) {
    MatchSubstringOptions options{std::string("\x00\x40", 2), ignore_case};
    this->CheckUnary(
        "find_substring_regex",
        this->template MakeArray<std::string>(
            {{"\x00\x40\xab", 3}, {"\x00\x9b\x00\x40\xab", 5}, {"\x40\x00\x41", 3}}),
        this->offset_type(), "[0, 2, -1]", &options);

    this->CheckUnary(
        "count_substring_regex",
        this->template MakeArray<std::string>({{"\x00\x40\xab", 3},
                                               {"\x01\xfc\x41", 3},
                                               {"\x01\x00\x00\x40\x00\x40\xab", 7}}),
        this->offset_type(), "[1, 0, 2]", &options);

    this->CheckUnary(
        "match_substring_regex",
        this->template MakeArray<std::string>({{"\x00\x40\xab", 3},
                                               {"\x00\xfc\x41", 3},
                                               {"\x01\xfc\x00\x40\x00\x40\xab", 7}}),
        boolean(), "[true, false, true]", &options);

    options.pattern = std::string("%\x00\x40", 3);
    this->CheckUnary(
        "match_like",
        this->template MakeArray<std::string>({{"\x00\x40\xab", 3},
                                               {"\xff\x9b\x00\x40\xab", 5},
                                               {"\xff\xfc\x40\x40\x00\x40", 6}}),
        boolean(), "[false, false, true]", &options);
  }
  {
    // "foo<non-UTF8>bar" = \x66\x6f\x6f\xfc\x62\x61\x72
    SplitPatternOptions options(std::string("\xfc\x00", 2));
    this->CheckUnary(
        "split_pattern_regex",
        this->template MakeArray<std::string>({{"\x66\x6f\x6f\xfc\x00\x62\x61\x72", 8}}),
        list(this->type()), R"([["foo", "bar"]])", &options);
  }
  {
    ReplaceSubstringOptions options(std::string("\x00\x40", 2), "bazz", 1);
    this->CheckUnary("replace_substring_regex",
                     this->template MakeArray<std::string>({{"\x00\x40", 2}}),
                     this->type(), R"(["bazz"])", &options);
  }
  {
    ExtractRegexOptions options("(?P<null>[\\x00])(?P<digit>\\d)");
    auto null_bitmap = std::make_shared<Buffer>("0");
    auto output = StructArray::Make(
        {this->template MakeArray<std::string>({{"\x00", 1}, {"1", 1}}),
         this->template MakeArray<std::string>({{"\x00", 1}, {"2", 1}})},
        {field("null", this->type()), field("digit", this->type())}, null_bitmap);
    this->CheckUnary(
        "extract_regex",
        this->template MakeArray<std::string>({{"foo\x00 1bar", 9}, {"\x02\x00\x40", 3}}),
        std::static_pointer_cast<Array>(*output), &options);
  }
  {
    ReplaceSliceOptions options(1, 2, std::string("\x00\x40", 2));
    this->CheckUnary("binary_replace_slice",
                     this->template MakeArray<std::string>(
                         {{"\x00\x0f\xab", 3}, {"\x00\x9b\xc3\xbb", 4}}),
                     this->template MakeArray<std::string>(
                         {{"\x00\x00\x40\xab", 4}, {"\x00\x00\x40\xc3\xbb", 5}}),
                     &options);
  }
}
#endif

TYPED_TEST(TestBinaryKernels, BinaryReverse) {
  this->CheckUnary(
      "binary_reverse",
      this->template MakeArray<std::string>(
          {{"abc123", 6}, {"\x00\x00\x42\xfe\xff", 5}, {"\xf0", 1}, {"", 0}}),
      this->template MakeArray<std::string>(
          {{"321cba", 6}, {"\xff\xfe\x42\x00\x00", 5}, {"\xf0", 1}, {"", 0}}));
}

TYPED_TEST(TestBaseBinaryKernels, BinaryReplaceSlice) {
  ReplaceSliceOptions options{0, 1, "XX"};
  this->CheckUnary("binary_replace_slice", "[]", this->type(), "[]", &options);
  this->CheckUnary("binary_replace_slice", R"([null, "", "a", "ab", "abc"])",
                   this->type(), R"([null, "XX", "XX", "XXb", "XXbc"])", &options);

  ReplaceSliceOptions options_whole{0, 5, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcde", "abcdef"])", this->type(),
                   R"([null, "XX", "XX", "XX", "XX", "XX", "XXf"])", &options_whole);

  ReplaceSliceOptions options_middle{2, 4, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "aXX", "abXX", "abXX", "abXX", "abXXe"])",
                   &options_middle);

  ReplaceSliceOptions options_neg_start{-3, -2, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "XXa", "XXab", "XXbc", "aXXcd", "abXXde"])",
                   &options_neg_start);

  ReplaceSliceOptions options_neg_end{2, -2, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "aXX", "abXX", "abXXc", "abXXcd", "abXXde"])",
                   &options_neg_end);

  ReplaceSliceOptions options_neg_pos{-1, 2, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "XX", "aXX", "abXXc", "abcXXd", "abcdXXe"])",
                   &options_neg_pos);

  // Effectively the same as [2, 2)
  ReplaceSliceOptions options_flip{2, 0, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "aXX", "abXX", "abXXc", "abXXcd", "abXXcde"])",
                   &options_flip);

  // Effectively the same as [-3, -3)
  ReplaceSliceOptions options_neg_flip{-3, -5, "XX"};
  this->CheckUnary("binary_replace_slice",
                   R"([null, "", "a", "ab", "abc", "abcd", "abcde"])", this->type(),
                   R"([null, "XX", "XXa", "XXab", "XXabc", "aXXbcd", "abXXcde"])",
                   &options_neg_flip);
}

TYPED_TEST(TestBaseBinaryKernels, FindSubstring) {
  MatchSubstringOptions options{"ab"};
  this->CheckUnary("find_substring", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary("find_substring", R"(["abc", "acb", "cab", null, "bac"])",
                   this->offset_type(), "[0, -1, 1, null, -1]", &options);

  MatchSubstringOptions options_repeated{"abab"};
  this->CheckUnary("find_substring", R"(["abab", "ab", "cababc", null, "bac"])",
                   this->offset_type(), "[0, -1, 1, null, -1]", &options_repeated);

  MatchSubstringOptions options_double_char{"aab"};
  this->CheckUnary("find_substring", R"(["aacb", "aab", "ab", "aaab"])",
                   this->offset_type(), "[-1, 0, -1, 1]", &options_double_char);

  MatchSubstringOptions options_double_char_2{"bbcaa"};
  this->CheckUnary("find_substring", R"(["abcbaabbbcaabccabaab"])", this->offset_type(),
                   "[7]", &options_double_char_2);

  MatchSubstringOptions options_empty{""};
  this->CheckUnary("find_substring", R"(["", "a", null])", this->offset_type(),
                   "[0, 0, null]", &options_empty);
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBaseBinaryKernels, FindSubstringIgnoreCase) {
  MatchSubstringOptions options{"?AB)", /*ignore_case=*/true};
  this->CheckUnary("find_substring", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary("find_substring",
                   R"-(["?aB)c", "acb", "c?Ab)", null, "?aBc", "AB)"])-",
                   this->offset_type(), "[0, -1, 1, null, -1, -1]", &options);
}

TYPED_TEST(TestBaseBinaryKernels, FindSubstringRegex) {
  MatchSubstringOptions options{"a+", /*ignore_case=*/false};
  this->CheckUnary("find_substring_regex", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary("find_substring_regex", R"(["a", "A", "baaa", null, "", "AaaA"])",
                   this->offset_type(), "[0, -1, 1, null, -1, 1]", &options);

  options.ignore_case = true;
  this->CheckUnary("find_substring_regex", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary("find_substring_regex", R"(["a", "A", "baaa", null, "", "AaaA"])",
                   this->offset_type(), "[0, 0, 1, null, -1, 0]", &options);
}

TYPED_TEST(TestBaseBinaryKernels, FindSubstringRegexWrongPattern) {
  MatchSubstringOptions options{"(a", /*ignore_case=*/false};

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Invalid regular expression"),
      CallFunction("find_substring_regex",
                   {Datum(R"(["a", "A", "baaa", null, "", "AaaA"])")}, &options));
}

#else
TYPED_TEST(TestBaseBinaryKernels, FindSubstringIgnoreCase) {
  MatchSubstringOptions options{"a+", /*ignore_case=*/true};
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("find_substring", {input}, &options));
}
#endif

TYPED_TEST(TestBaseBinaryKernels, CountSubstring) {
  MatchSubstringOptions options{"aba"};
  this->CheckUnary("count_substring", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary(
      "count_substring",
      R"(["", null, "ab", "aba", "baba", "ababa", "abaaba", "babacaba", "ABA"])",
      this->offset_type(), "[0, null, 0, 1, 1, 1, 2, 2, 0]", &options);

  MatchSubstringOptions options_empty{""};
  this->CheckUnary("count_substring", R"(["", null, "abc"])", this->offset_type(),
                   "[1, null, 4]", &options_empty);

  MatchSubstringOptions options_repeated{"aaa"};
  this->CheckUnary("count_substring", R"(["", "aaaa", "aaaaa", "aaaaaa", "aaá"])",
                   this->offset_type(), "[0, 1, 1, 2, 0]", &options_repeated);
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBaseBinaryKernels, CountSubstringRegex) {
  MatchSubstringOptions options{"aba"};
  this->CheckUnary("count_substring_regex", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary(
      "count_substring",
      R"(["", null, "ab", "aba", "baba", "ababa", "abaaba", "babacaba", "ABA"])",
      this->offset_type(), "[0, null, 0, 1, 1, 1, 2, 2, 0]", &options);

  MatchSubstringOptions options_empty{""};
  this->CheckUnary("count_substring_regex", R"(["", null, "abc"])", this->offset_type(),
                   "[1, null, 4]", &options_empty);

  MatchSubstringOptions options_as{"a+"};
  this->CheckUnary("count_substring_regex", R"(["", "bacaaadaaaa", "c", "AAA"])",
                   this->offset_type(), "[0, 3, 0, 0]", &options_as);

  MatchSubstringOptions options_empty_match{"a*"};
  this->CheckUnary("count_substring_regex", R"(["", "bacaaadaaaa", "c", "AAA"])",
                   // 7 is because it matches at |b|a|c|aaa|d|aaaa|
                   this->offset_type(), "[1, 7, 2, 4]", &options_empty_match);

  MatchSubstringOptions options_repeated{"aaa"};
  this->CheckUnary("count_substring", R"(["", "aaaa", "aaaaa", "aaaaaa", "aaá"])",
                   this->offset_type(), "[0, 1, 1, 2, 0]", &options_repeated);
}

TYPED_TEST(TestBaseBinaryKernels, CountSubstringIgnoreCase) {
  MatchSubstringOptions options{"aba", /*ignore_case=*/true};
  this->CheckUnary("count_substring", "[]", this->offset_type(), "[]", &options);
  this->CheckUnary(
      "count_substring",
      R"(["", null, "ab", "aBa", "bAbA", "aBaBa", "abaAbA", "babacaba", "ABA"])",
      this->offset_type(), "[0, null, 0, 1, 1, 1, 2, 2, 1]", &options);

  MatchSubstringOptions options_empty{"", /*ignore_case=*/true};
  this->CheckUnary("count_substring", R"(["", null, "abc"])", this->offset_type(),
                   "[1, null, 4]", &options_empty);
}

TYPED_TEST(TestBaseBinaryKernels, CountSubstringRegexIgnoreCase) {
  MatchSubstringOptions options_as{"a+", /*ignore_case=*/true};
  this->CheckUnary("count_substring_regex", R"(["", "bacAaAdaAaA", "c", "AAA"])",
                   this->offset_type(), "[0, 3, 0, 1]", &options_as);

  MatchSubstringOptions options_empty_match{"a*", /*ignore_case=*/true};
  this->CheckUnary("count_substring_regex", R"(["", "bacAaAdaAaA", "c", "AAA"])",
                   this->offset_type(), "[1, 7, 2, 2]", &options_empty_match);
}
#else
TYPED_TEST(TestBaseBinaryKernels, CountSubstringIgnoreCase) {
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");
  MatchSubstringOptions options{"a", /*ignore_case=*/true};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("count_substring", {input}, &options));
}
#endif

TYPED_TEST(TestBaseBinaryKernels, BinaryJoinElementWise) {
  const auto ty = this->type();
  JoinOptions options;
  JoinOptions options_skip(JoinOptions::SKIP);
  JoinOptions options_replace(JoinOptions::REPLACE, "X");
  // Scalar args, Scalar separator
  this->CheckVarArgsScalar("binary_join_element_wise", R"([null])", ty, R"(null)",
                           &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["-"])", ty, R"("")", &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", "-"])", ty, R"("a")",
                           &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", "b", "-"])", ty,
                           R"("a-b")", &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", "b", null])", ty,
                           R"(null)", &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", null, "-"])", ty,
                           R"(null)", &options);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["foo", "bar", "baz", "++"])",
                           ty, R"("foo++bar++baz")", &options);

  // Scalar args, Array separator
  const auto sep = ArrayFromJSON(ty, R"([null, "-", "--"])");
  const auto scalar1 = ScalarFromJSON(ty, R"("foo")");
  const auto scalar2 = ScalarFromJSON(ty, R"("bar")");
  const auto scalar3 = ScalarFromJSON(ty, R"("")");
  const auto scalar_null = ScalarFromJSON(ty, R"(null)");
  this->CheckVarArgs("binary_join_element_wise", {sep}, ty, R"([null, "", ""])",
                     &options);
  this->CheckVarArgs("binary_join_element_wise", {scalar1, sep}, ty,
                     R"([null, "foo", "foo"])", &options);
  this->CheckVarArgs("binary_join_element_wise", {scalar1, scalar2, sep}, ty,
                     R"([null, "foo-bar", "foo--bar"])", &options);
  this->CheckVarArgs("binary_join_element_wise", {scalar1, scalar_null, sep}, ty,
                     R"([null, null, null])", &options);
  this->CheckVarArgs("binary_join_element_wise", {scalar1, scalar2, scalar3, sep}, ty,
                     R"([null, "foo-bar-", "foo--bar--"])", &options);

  // Array args, Scalar separator
  const auto sep1 = ScalarFromJSON(ty, R"("-")");
  const auto sep2 = ScalarFromJSON(ty, R"("--")");
  const auto arr1 = ArrayFromJSON(ty, R"([null, "a", "bb", "ccc"])");
  const auto arr2 = ArrayFromJSON(ty, R"(["d", null, "e", ""])");
  const auto arr3 = ArrayFromJSON(ty, R"(["gg", null, "h", "iii"])");
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, arr3, scalar_null}, ty,
                     R"([null, null, null, null])", &options);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, arr3, sep1}, ty,
                     R"([null, null, "bb-e-h", "ccc--iii"])", &options);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, arr3, sep2}, ty,
                     R"([null, null, "bb--e--h", "ccc----iii"])", &options);

  // Array args, Array separator
  const auto sep3 = ArrayFromJSON(ty, R"(["-", "--", null, "---"])");
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, arr3, sep3}, ty,
                     R"([null, null, null, "ccc------iii"])", &options);

  // Mixed
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep3}, ty,
                     R"([null, null, null, "ccc------bar"])", &options);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, sep3}, ty,
                     R"([null, null, null, null])", &options);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep1}, ty,
                     R"([null, null, "bb-e-bar", "ccc--bar"])", &options);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, scalar_null},
                     ty, R"([null, null, null, null])", &options);

  // Skip
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", null, "b", "-"])", ty,
                           R"("a-b")", &options_skip);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", null, "b", null])", ty,
                           R"(null)", &options_skip);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep3}, ty,
                     R"(["d-bar", "a--bar", null, "ccc------bar"])", &options_skip);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, sep3}, ty,
                     R"(["d", "a", null, "ccc---"])", &options_skip);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep1}, ty,
                     R"(["d-bar", "a-bar", "bb-e-bar", "ccc--bar"])", &options_skip);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, scalar_null},
                     ty, R"([null, null, null, null])", &options_skip);

  // Replace
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", null, "b", "-"])", ty,
                           R"("a-X-b")", &options_replace);
  this->CheckVarArgsScalar("binary_join_element_wise", R"(["a", null, "b", null])", ty,
                           R"(null)", &options_replace);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep3}, ty,
                     R"(["X-d-bar", "a--X--bar", null, "ccc------bar"])",
                     &options_replace);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, sep3}, ty,
                     R"(["X-d-X", "a--X--X", null, "ccc------X"])", &options_replace);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar2, sep1}, ty,
                     R"(["X-d-bar", "a-X-bar", "bb-e-bar", "ccc--bar"])",
                     &options_replace);
  this->CheckVarArgs("binary_join_element_wise", {arr1, arr2, scalar_null, scalar_null},
                     ty, R"([null, null, null, null])", &options_replace);

  // Error cases
  ASSERT_RAISES(Invalid,
                CallFunction("binary_join_element_wise", ExecBatch({}, 0), &options));
}

class TestFixedSizeBinaryKernels : public ::testing::Test {
 protected:
  void CheckUnary(std::string func_name, std::string json_input,
                  std::shared_ptr<DataType> out_ty, std::string json_expected,
                  const FunctionOptions* options = nullptr) {
    CheckScalarUnary(func_name, type(), json_input, out_ty, json_expected, options);
    // Ensure the equivalent binary kernel does the same thing
    CheckScalarUnary(func_name, binary(), json_input,
                     out_ty->id() == Type::FIXED_SIZE_BINARY ? binary() : out_ty,
                     json_expected, options);
  }

  std::shared_ptr<DataType> type() const { return fixed_size_binary(6); }
  std::shared_ptr<DataType> offset_type() const { return int32(); }
};

TEST_F(TestFixedSizeBinaryKernels, BinaryLength) {
  CheckUnary("binary_length", R"(["aaaaaa", null, "áéí"])", offset_type(),
             "[6, null, 6]");
}

TEST_F(TestFixedSizeBinaryKernels, BinaryReplaceSlice) {
  ReplaceSliceOptions options{0, 1, "XX"};
  CheckUnary("binary_replace_slice", "[]", fixed_size_binary(7), "[]", &options);
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(7),
             R"([null, "XXbcdef"])", &options);

  ReplaceSliceOptions options_shrink{0, 2, ""};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(4),
             R"([null, "cdef"])", &options_shrink);

  ReplaceSliceOptions options_whole{0, 6, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(2),
             R"([null, "XX"])", &options_whole);

  ReplaceSliceOptions options_middle{2, 4, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(6),
             R"([null, "abXXef"])", &options_middle);

  ReplaceSliceOptions options_neg_start{-3, -2, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(7),
             R"([null, "abcXXef"])", &options_neg_start);

  ReplaceSliceOptions options_neg_end{2, -2, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(6),
             R"([null, "abXXef"])", &options_neg_end);

  ReplaceSliceOptions options_neg_pos{-1, 2, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(8),
             R"([null, "abcdeXXf"])", &options_neg_pos);

  // Effectively the same as [2, 2)
  ReplaceSliceOptions options_flip{2, 0, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(8),
             R"([null, "abXXcdef"])", &options_flip);

  // Effectively the same as [-3, -3)
  ReplaceSliceOptions options_neg_flip{-3, -5, "XX"};
  CheckUnary("binary_replace_slice", R"([null, "abcdef"])", fixed_size_binary(8),
             R"([null, "abcXXdef"])", &options_neg_flip);
}

TEST_F(TestFixedSizeBinaryKernels, CountSubstring) {
  MatchSubstringOptions options{"aba"};
  CheckUnary("count_substring", "[]", offset_type(), "[]", &options);
  CheckUnary(
      "count_substring",
      R"(["      ", null, "  ab  ", " aba  ", "baba  ", "ababa ", "abaaba", "ABAABA"])",
      offset_type(), "[0, null, 0, 1, 1, 1, 2, 0]", &options);

  MatchSubstringOptions options_empty{""};
  CheckUnary("count_substring", R"(["      ", null, "abc   "])", offset_type(),
             "[7, null, 7]", &options_empty);

  MatchSubstringOptions options_repeated{"aaa"};
  CheckUnary("count_substring", R"(["      ", "aaaa  ", "aaaaa ", "aaaaaa", "aaáaa"])",
             offset_type(), "[0, 1, 1, 2, 0]", &options_repeated);
}

#ifdef ARROW_WITH_RE2
TEST_F(TestFixedSizeBinaryKernels, CountSubstringRegex) {
  MatchSubstringOptions options{"aba"};
  CheckUnary("count_substring_regex", "[]", offset_type(), "[]", &options);
  CheckUnary(
      "count_substring_regex",
      R"(["      ", null, "  ab  ", " aba  ", "baba  ", "ababa ", "abaaba", "ABAABA"])",
      offset_type(), "[0, null, 0, 1, 1, 1, 2, 0]", &options);

  MatchSubstringOptions options_empty{""};
  CheckUnary("count_substring_regex", R"(["      ", null, "abc   "])", offset_type(),
             "[7, null, 7]", &options_empty);

  MatchSubstringOptions options_repeated{"aaa"};
  CheckUnary("count_substring_regex",
             R"(["      ", "aaaa  ", "aaaaa ", "aaaaaa", "aaáaa"])", offset_type(),
             "[0, 1, 1, 2, 0]", &options_repeated);

  MatchSubstringOptions options_as{"a+"};
  CheckUnary("count_substring_regex", R"(["      ", "bacaaa", "c     ", "AAAAAA"])",
             offset_type(), "[0, 2, 0, 0]", &options_as);

  MatchSubstringOptions options_empty_match{"a*"};
  CheckUnary("count_substring_regex", R"(["      ", "bacaaa", "c     ", "AAAAAA"])",
             // 5 is because it matches at |b|a|c|aaa|
             offset_type(), "[7, 5, 7, 7]", &options_empty_match);
}

TEST_F(TestFixedSizeBinaryKernels, CountSubstringIgnoreCase) {
  MatchSubstringOptions options{"aba", /*ignore_case=*/true};
  CheckUnary("count_substring", "[]", offset_type(), "[]", &options);
  CheckUnary(
      "count_substring",
      R"(["      ", null, "ab    ", "aBa   ", " bAbA ", " aBaBa", "abaAbA", "abaaba", "ABAabc"])",
      offset_type(), "[0, null, 0, 1, 1, 1, 2, 2, 1]", &options);

  MatchSubstringOptions options_empty{"", /*ignore_case=*/true};
  CheckUnary("count_substring", R"(["      ", null, "abcABc"])", offset_type(),
             "[7, null, 7]", &options_empty);
}

TEST_F(TestFixedSizeBinaryKernels, CountSubstringRegexIgnoreCase) {
  MatchSubstringOptions options_as{"a+", /*ignore_case=*/true};
  CheckUnary("count_substring_regex", R"(["      ", "aAadaA", "c     ", "AAAbbb"])",
             offset_type(), "[0, 2, 0, 1]", &options_as);

  MatchSubstringOptions options_empty_match{"a*", /*ignore_case=*/true};
  CheckUnary("count_substring_regex", R"(["      ", "aAadaA", "c     ", "AAAbbb"])",
             offset_type(), "[7, 4, 7, 5]", &options_empty_match);
}
#else
TEST_F(TestFixedSizeBinaryKernels, CountSubstringIgnoreCase) {
  Datum input = ArrayFromJSON(type(), R"(["    a "])");
  MatchSubstringOptions options{"a", /*ignore_case=*/true};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("count_substring", {input}, &options));
}
#endif

TEST_F(TestFixedSizeBinaryKernels, FindSubstring) {
  MatchSubstringOptions options{"ab"};
  CheckUnary("find_substring", "[]", offset_type(), "[]", &options);
  CheckUnary("find_substring", R"(["abc   ", "   acb", " cab  ", null, "  bac "])",
             offset_type(), "[0, -1, 2, null, -1]", &options);

  MatchSubstringOptions options_repeated{"abab"};
  CheckUnary("find_substring", R"([" abab ", "  ab  ", "cababc", null, "  bac "])",
             offset_type(), "[1, -1, 1, null, -1]", &options_repeated);

  MatchSubstringOptions options_double_char{"aab"};
  CheckUnary("find_substring", R"(["  aacb", "aab   ", "  ab  ", "  aaab"])",
             offset_type(), "[-1, 0, -1, 3]", &options_double_char);

  MatchSubstringOptions options_double_char_2{"bbcaa"};
  CheckUnary("find_substring", R"(["bbbcaa"])", offset_type(), "[1]",
             &options_double_char_2);

  MatchSubstringOptions options_empty{""};
  CheckUnary("find_substring", R"(["      ", "aaaaaa", null])", offset_type(),
             "[0, 0, null]", &options_empty);
}

#ifdef ARROW_WITH_RE2
TEST_F(TestFixedSizeBinaryKernels, FindSubstringIgnoreCase) {
  MatchSubstringOptions options{"?AB)", /*ignore_case=*/true};
  CheckUnary("find_substring", "[]", offset_type(), "[]", &options);
  CheckUnary("find_substring",
             R"-(["?aB)c ", " acb  ", " c?Ab)", null, " ?aBc ", " AB)  "])-",
             offset_type(), "[0, -1, 2, null, -1, -1]", &options);
}

TEST_F(TestFixedSizeBinaryKernels, FindSubstringRegex) {
  MatchSubstringOptions options{"a+", /*ignore_case=*/false};
  CheckUnary("find_substring_regex", "[]", offset_type(), "[]", &options);
  CheckUnary("find_substring_regex",
             R"(["a     ", "  A   ", "  baaa", null, "      ", " AaaA "])", offset_type(),
             "[0, -1, 3, null, -1, 2]", &options);

  options.ignore_case = true;
  CheckUnary("find_substring_regex", "[]", offset_type(), "[]", &options);
  CheckUnary("find_substring_regex",
             R"(["a     ", "  A   ", "  baaa", null, "      ", " AaaA "])", offset_type(),
             "[0, 2, 3, null, -1, 1]", &options);
}
#else
TEST_F(TestFixedSizeBinaryKernels, FindSubstringIgnoreCase) {
  MatchSubstringOptions options{"a+", /*ignore_case=*/true};
  Datum input = ArrayFromJSON(type(), R"(["aaaaaa"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("find_substring", {input}, &options));
}
#endif

TYPED_TEST(TestStringKernels, AsciiUpper) {
  this->CheckUnary("ascii_upper", "[]", this->type(), "[]");
  this->CheckUnary("ascii_upper", "[\"aAazZæÆ&\", null, \"\", \"bbb\"]", this->type(),
                   "[\"AAAZZæÆ&\", null, \"\", \"BBB\"]");
}

TYPED_TEST(TestStringKernels, AsciiLower) {
  this->CheckUnary("ascii_lower", "[]", this->type(), "[]");
  this->CheckUnary("ascii_lower", "[\"aAazZæÆ&\", null, \"\", \"BBB\"]", this->type(),
                   "[\"aaazzæÆ&\", null, \"\", \"bbb\"]");
}

TYPED_TEST(TestStringKernels, AsciiSwapCase) {
  this->CheckUnary("ascii_swapcase", "[]", this->type(), "[]");
  this->CheckUnary("ascii_swapcase", "[\"aAazZæÆ&\", null, \"\", \"BbB\"]", this->type(),
                   "[\"AaAZzæÆ&\", null, \"\", \"bBb\"]");
  this->CheckUnary("ascii_swapcase", "[\"hEllO, WoRld!\", \"$. A35?\"]", this->type(),
                   "[\"HeLLo, wOrLD!\", \"$. a35?\"]");
}

TYPED_TEST(TestStringKernels, AsciiCapitalize) {
  this->CheckUnary("ascii_capitalize", "[]", this->type(), "[]");
  this->CheckUnary("ascii_capitalize",
                   "[\"aAazZæÆ&\", null, \"\", \"bBB\", \"hEllO, WoRld!\", \"$. A3\", "
                   "\"!hELlo, wORLd!\"]",
                   this->type(),
                   "[\"AaazzæÆ&\", null, \"\", \"Bbb\", \"Hello, world!\", \"$. a3\", "
                   "\"!hello, world!\"]");
}

TYPED_TEST(TestStringKernels, AsciiTitle) {
  this->CheckUnary(
      "ascii_title",
      R"([null, "", "b", "aAaz;ZeA&", "arRoW", "iI", "a.a.a..A", "hEllO, WoRld!", "foo   baR;heHe0zOP", "!%$^.,;"])",
      this->type(),
      R"([null, "", "B", "Aaaz;Zea&", "Arrow", "Ii", "A.A.A..A", "Hello, World!", "Foo   Bar;Hehe0Zop", "!%$^.,;"])");
}

TYPED_TEST(TestStringKernels, AsciiReverse) {
  this->CheckUnary("ascii_reverse", "[]", this->type(), "[]");
  this->CheckUnary("ascii_reverse", R"(["abcd", null, "", "bbb"])", this->type(),
                   R"(["dcba", null, "", "bbb"])");

  auto invalid_input = ArrayFromJSON(this->type(), R"(["aAazZæÆ&", null, "", "bcd"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Non-ASCII sequence in input"),
                                  CallFunction("ascii_reverse", {invalid_input}));
  auto masked_input = TweakValidityBit(invalid_input, 0, false);
  CheckScalarUnary("ascii_reverse", masked_input,
                   ArrayFromJSON(this->type(), R"([null, null, "", "dcb"])"));
}

TYPED_TEST(TestStringKernels, Utf8Reverse) {
  this->CheckUnary("utf8_reverse", "[]", this->type(), "[]");
  this->CheckUnary("utf8_reverse", R"(["abcd", null, "", "bbb"])", this->type(),
                   R"(["dcba", null, "", "bbb"])");
  this->CheckUnary("utf8_reverse", R"(["aAazZæÆ&", null, "", "bbb", "ɑɽⱤæÆ"])",
                   this->type(), R"(["&ÆæZzaAa", null, "", "bbb", "ÆæⱤɽɑ"])");

  // inputs with malformed utf8 chars would produce garbage output, but the end result
  // would produce arrays with same lengths. Hence checking offset buffer equality
  auto malformed_input = ArrayFromJSON(this->type(), "[\"ɑ\xFFɑa\", \"ɽ\xe1\xbdɽa\"]");
  const Result<Datum>& res = CallFunction("utf8_reverse", {malformed_input});
  ASSERT_TRUE(res->array()->buffers[1]->Equals(*malformed_input->data()->buffers[1]));
}

#ifdef ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, Utf8Normalize) {
  Utf8NormalizeOptions nfc_options{Utf8NormalizeOptions::NFC};
  Utf8NormalizeOptions nfkc_options{Utf8NormalizeOptions::NFKC};
  Utf8NormalizeOptions nfd_options{Utf8NormalizeOptions::NFD};
  Utf8NormalizeOptions nfkd_options{Utf8NormalizeOptions::NFKD};

  std::vector<Utf8NormalizeOptions> all_options{nfc_options, nfkc_options, nfd_options,
                                                nfkd_options};
  std::vector<Utf8NormalizeOptions> compose_options{nfc_options, nfkc_options};
  std::vector<Utf8NormalizeOptions> decompose_options{nfd_options, nfkd_options};
  std::vector<Utf8NormalizeOptions> canonical_options{nfc_options, nfd_options};
  std::vector<Utf8NormalizeOptions> compatibility_options{nfkc_options, nfkd_options};

  for (const auto& options : all_options) {
    this->CheckUnary("utf8_normalize", "[]", this->type(), "[]", &options);
    const char* json_data = R"([null, "", "abc"])";
    this->CheckUnary("utf8_normalize", json_data, this->type(), json_data, &options);
  }

  // decomposed: U+0061(LATIN SMALL LETTER A) + U+0301(COMBINING ACUTE ACCENT)
  // composed: U+00E1(LATIN SMALL LETTER A WITH ACUTE)
  const char* json_composed = "[\"foo\", \"á\"]";
  const char* json_decomposed = "[\"foo\", \"a\xcc\x81\"]";
  for (const auto& options : compose_options) {
    this->CheckUnary("utf8_normalize", json_decomposed, this->type(), json_composed,
                     &options);
    this->CheckUnary("utf8_normalize", json_composed, this->type(), json_composed,
                     &options);
  }
  for (const auto& options : decompose_options) {
    this->CheckUnary("utf8_normalize", json_composed, this->type(), json_decomposed,
                     &options);
    this->CheckUnary("utf8_normalize", json_decomposed, this->type(), json_decomposed,
                     &options);
  }

  // canonical: U+00B2(Superscript Two)
  // compatibility: "2"
  const char* json_canonical = "[\"01\xc2\xb2!\"]";
  const char* json_compatibility = "[\"012!\"]";
  for (const auto& options : canonical_options) {
    this->CheckUnary("utf8_normalize", json_canonical, this->type(), json_canonical,
                     &options);
    this->CheckUnary("utf8_normalize", json_compatibility, this->type(),
                     json_compatibility, &options);
  }
  for (const auto& options : compatibility_options) {
    this->CheckUnary("utf8_normalize", json_canonical, this->type(), json_compatibility,
                     &options);
    this->CheckUnary("utf8_normalize", json_compatibility, this->type(),
                     json_compatibility, &options);
  }

  // canonical: U+FDFA(Arabic Ligature Sallallahou Alayhe Wasallam)
  // compatibility: <18 codepoints>
  json_canonical = "[\"\xef\xb7\xba\"]";
  json_compatibility = "[\"صلى الله عليه وسلم\"]";
  for (const auto& options : canonical_options) {
    this->CheckUnary("utf8_normalize", json_canonical, this->type(), json_canonical,
                     &options);
    this->CheckUnary("utf8_normalize", json_compatibility, this->type(),
                     json_compatibility, &options);
  }
  for (const auto& options : compatibility_options) {
    this->CheckUnary("utf8_normalize", json_canonical, this->type(), json_compatibility,
                     &options);
    this->CheckUnary("utf8_normalize", json_compatibility, this->type(),
                     json_compatibility, &options);
  }
}

#endif

TEST(TestStringKernels, LARGE_MEMORY_TEST(Utf8Upper32bitGrowth)) {
  // 0x7fff * 0xffff is the max a 32 bit string array can hold
  // since the utf8_upper kernel can grow it by 3/2, the max we should accept is is
  // 0x7fff * 0xffff * 2/3 = 0x5555 * 0xffff, so this should give us a CapacityError
  std::string str(0x5556 * 0xffff, 'a');
  arrow::StringBuilder builder;
  ASSERT_OK(builder.Append(str));
  std::shared_ptr<arrow::Array> array;
  arrow::Status st = builder.Finish(&array);
  const FunctionOptions* options = nullptr;
  EXPECT_RAISES_WITH_MESSAGE_THAT(CapacityError,
                                  testing::HasSubstr("Result might not fit"),
                                  CallFunction("utf8_upper", {array}, options));
  ASSERT_OK_AND_ASSIGN(auto scalar, array->GetScalar(0));
  EXPECT_RAISES_WITH_MESSAGE_THAT(CapacityError,
                                  testing::HasSubstr("Result might not fit"),
                                  CallFunction("utf8_upper", {scalar}, options));
}

TYPED_TEST(TestStringKernels, Utf8Length) {
  this->CheckUnary("utf8_length",
                   R"(["aaa", null, "áéíóú", "ɑɽⱤoW😀", "áéí 0😀", "", "b"])",
                   this->offset_type(), "[3, null, 5, 6, 6, 0, 1]");
}

#ifdef ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, Utf8Upper) {
  this->CheckUnary("utf8_upper", "[\"aAazZæÆ&\", null, \"\", \"b\"]", this->type(),
                   "[\"AAAZZÆÆ&\", null, \"\", \"B\"]");

  // test varying encoding lengths and thus changing indices/offsets
  this->CheckUnary("utf8_upper", "[\"ɑɽⱤoW\", null, \"ıI\", \"b\"]", this->type(),
                   "[\"ⱭⱤⱤOW\", null, \"II\", \"B\"]");

  // ῦ to Υ͂ not supported
  // this->CheckUnary("utf8_upper", "[\"ῦɐɜʞȿ\"]", this->type(),
  // "[\"Υ͂ⱯꞫꞰⱾ\"]");

  // test maximum buffer growth
  this->CheckUnary("utf8_upper", "[\"ɑɑɑɑ\"]", this->type(), "[\"ⱭⱭⱭⱭ\"]");

  // Test invalid data
  auto invalid_input = ArrayFromJSON(this->type(), "[\"ɑa\xFFɑ\", \"ɽ\xe1\xbdɽaa\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8 sequence"),
                                  CallFunction("utf8_upper", {invalid_input}));
}

TYPED_TEST(TestStringKernels, Utf8Lower) {
  this->CheckUnary("utf8_lower", "[\"aAazZæÆ&\", null, \"\", \"b\"]", this->type(),
                   "[\"aaazzææ&\", null, \"\", \"b\"]");

  // test varying encoding lengths and thus changing indices/offsets
  this->CheckUnary("utf8_lower", "[\"ⱭɽⱤoW\", null, \"ıI\", \"B\"]", this->type(),
                   "[\"ɑɽɽow\", null, \"ıi\", \"b\"]");

  // ῦ to Υ͂ is not supported, but in principle the reverse is, but it would need
  // normalization
  // this->CheckUnary("utf8_lower", "[\"Υ͂ⱯꞫꞰⱾ\"]", this->type(),
  // "[\"ῦɐɜʞȿ\"]");

  // test maximum buffer growth
  this->CheckUnary("utf8_lower", "[\"ȺȺȺȺ\"]", this->type(), "[\"ⱥⱥⱥⱥ\"]");

  // Test invalid data
  auto invalid_input = ArrayFromJSON(this->type(), "[\"Ⱥa\xFFⱭ\", \"Ɽ\xe1\xbdⱤaA\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8 sequence"),
                                  CallFunction("utf8_lower", {invalid_input}));
}

TYPED_TEST(TestStringKernels, Utf8SwapCase) {
  this->CheckUnary("utf8_swapcase", "[\"aAazZæÆ&\", null, \"\", \"b\"]", this->type(),
                   "[\"AaAZzÆæ&\", null, \"\", \"B\"]");

  // test varying encoding lengths and thus changing indices/offsets
  this->CheckUnary("utf8_swapcase", "[\"ⱭɽⱤoW\", null, \"ıI\", \"B\"]", this->type(),
                   "[\"ɑⱤɽOw\", null, \"Ii\", \"b\"]");

  // test maximum buffer growth
  this->CheckUnary("utf8_swapcase", "[\"ȺȺȺȺ\"]", this->type(), "[\"ⱥⱥⱥⱥ\"]");

  this->CheckUnary("utf8_swapcase", "[\"hEllO, WoRld!\", \"$. A35?\"]", this->type(),
                   "[\"HeLLo, wOrLD!\", \"$. a35?\"]");

  // Test invalid data
  auto invalid_input = ArrayFromJSON(this->type(), "[\"Ⱥa\xFFⱭ\", \"Ɽ\xe1\xbdⱤaA\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8 sequence"),
                                  CallFunction("utf8_swapcase", {invalid_input}));
}

TYPED_TEST(TestStringKernels, Utf8Capitalize) {
  this->CheckUnary("utf8_capitalize", "[]", this->type(), "[]");
  this->CheckUnary("utf8_capitalize",
                   "[\"aAazZæÆ&\", null, \"\", \"b\", \"ɑɽⱤoW\", \"ıI\", \"ⱥⱥⱥȺ\", "
                   "\"hEllO, WoRld!\", \"$. A3\", \"!ɑⱤⱤow\"]",
                   this->type(),
                   "[\"Aaazzææ&\", null, \"\", \"B\", \"Ɑɽɽow\", \"Ii\", \"Ⱥⱥⱥⱥ\", "
                   "\"Hello, world!\", \"$. a3\", \"!ɑɽɽow\"]");
}

TYPED_TEST(TestStringKernels, Utf8Title) {
  this->CheckUnary(
      "utf8_title",
      R"([null, "", "b", "aAaz;ZæÆ&", "ɑɽⱤoW", "ıI", "ⱥ.ⱥ.ⱥ..Ⱥ", "hEllO, WoRld!", "foo   baR;héHé0zOP", "!%$^.,;"])",
      this->type(),
      R"([null, "", "B", "Aaaz;Zææ&", "Ɑɽɽow", "Ii", "Ⱥ.Ⱥ.Ⱥ..Ⱥ", "Hello, World!", "Foo   Bar;Héhé0Zop", "!%$^.,;"])");
}

TYPED_TEST(TestStringKernels, BinaryRepeatWithScalarRepeat) {
  auto values = ArrayFromJSON(this->type(),
                              R"(["aAazZæÆ&", null, "", "b", "ɑɽⱤoW", "ıI",
                                  "ⱥⱥⱥȺ", "hEllO, WoRld!", "$. A3", "!ɑⱤⱤow"])");
  std::vector<std::pair<int, std::string>> nrepeats_and_expected{{
      {0, R"(["", null, "", "", "", "", "", "", "", ""])"},
      {1, R"(["aAazZæÆ&", null, "", "b", "ɑɽⱤoW", "ıI", "ⱥⱥⱥȺ", "hEllO, WoRld!",
              "$. A3", "!ɑⱤⱤow"])"},
      {4, R"(["aAazZæÆ&aAazZæÆ&aAazZæÆ&aAazZæÆ&", null, "", "bbbb",
              "ɑɽⱤoWɑɽⱤoWɑɽⱤoWɑɽⱤoW", "ıIıIıIıI", "ⱥⱥⱥȺⱥⱥⱥȺⱥⱥⱥȺⱥⱥⱥȺ",
              "hEllO, WoRld!hEllO, WoRld!hEllO, WoRld!hEllO, WoRld!",
              "$. A3$. A3$. A3$. A3", "!ɑⱤⱤow!ɑⱤⱤow!ɑⱤⱤow!ɑⱤⱤow"])"},
  }};

  for (const auto& pair : nrepeats_and_expected) {
    auto num_repeat = pair.first;
    auto expected = pair.second;
    for (const auto& ty : IntTypes()) {
      this->CheckVarArgs("binary_repeat",
                         {values, Datum(*arrow::MakeScalar(ty, num_repeat))},
                         this->type(), expected);
    }
  }

  // Negative repeat count
  for (auto num_repeat_ : {-1, -2, -5}) {
    auto num_repeat = *arrow::MakeScalar(int64(), num_repeat_);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Repeat count must be a non-negative integer"),
        CallFunction("binary_repeat", {values, num_repeat}));
  }

  // Floating-point repeat count
  for (auto num_repeat_ : {0.0, 1.2, -1.3}) {
    auto num_repeat = *arrow::MakeScalar(float64(), num_repeat_);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("has no kernel matching input types"),
        CallFunction("binary_repeat", {values, num_repeat}));
  }
}

TYPED_TEST(TestStringKernels, BinaryRepeatWithArrayRepeat) {
  auto values = ArrayFromJSON(this->type(),
                              R"([null, "aAazZæÆ&", "", "b", "ɑɽⱤoW", "ıI",
                                  "ⱥⱥⱥȺ", "hEllO, WoRld!", "$. A3", "!ɑⱤⱤow"])");
  for (const auto& ty : IntTypes()) {
    auto num_repeats = ArrayFromJSON(ty, R"([100, 1, 2, 5, 2, 0, 1, 3, null, 3])");
    std::string expected =
        R"([null, "aAazZæÆ&", "", "bbbbb", "ɑɽⱤoWɑɽⱤoW", "", "ⱥⱥⱥȺ",
            "hEllO, WoRld!hEllO, WoRld!hEllO, WoRld!", null,
            "!ɑⱤⱤow!ɑⱤⱤow!ɑⱤⱤow"])";
    this->CheckVarArgs("binary_repeat", {values, num_repeats}, this->type(), expected);
  }

  // Negative repeat count
  auto num_repeats = ArrayFromJSON(int64(), R"([100, -1, 2, -5, 2, -1, 3, -2, 3, -100])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Repeat count must be a non-negative integer"),
      CallFunction("binary_repeat", {values, num_repeats}));

  // Floating-point repeat count
  num_repeats = ArrayFromJSON(float64(), R"([0.0, 1.2, -1.3])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, ::testing::HasSubstr("has no kernel matching input types"),
      CallFunction("binary_repeat", {values, num_repeats}));
}

TYPED_TEST(TestStringKernels, IsAlphaNumericUnicode) {
  // U+08BE (utf8: 	\xE0\xA2\xBE) is undefined, but utf8proc things it is
  // UTF8PROC_CATEGORY_LO
  this->CheckUnary("utf8_is_alnum", "[\"ⱭɽⱤoW123\", null, \"Ɑ2\", \"!\", \"\"]",
                   boolean(), "[true, null, true, false, false]");
}

TYPED_TEST(TestStringKernels, IsAlphaUnicode) {
  // U+08BE (utf8: 	\xE0\xA2\xBE) is undefined, but utf8proc things it is
  // UTF8PROC_CATEGORY_LO
  this->CheckUnary("utf8_is_alpha", "[\"ⱭɽⱤoW\", null, \"Ɑ2\", \"!\", \"\"]", boolean(),
                   "[true, null, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsAscii) {
  this->CheckUnary("string_is_ascii", "[\"azAZ~\", null, \"Ɑ\", \"\"]", boolean(),
                   "[true, null, false, true]");
}

TYPED_TEST(TestStringKernels, IsDecimalUnicode) {
  // ٣ is arabic 3 (decimal), Ⅳ roman (non-decimal)
  this->CheckUnary("utf8_is_decimal", "[\"12\", null, \"٣\", \"Ⅳ\", \"1a\", \"\"]",
                   boolean(), "[true, null, true, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsDigitUnicode) {
  // These are digits according to Python, but we don't have the information in
  // utf8proc for this
  // this->CheckUnary("utf8_is_digit", "[\"²\", \"①\"]", boolean(), "[true,
  // true]");
}

TYPED_TEST(TestStringKernels, IsNumericUnicode) {
  // ٣ is arabic 3 (decimal), Ⅳ roman (non-decimal)
  this->CheckUnary("utf8_is_numeric", "[\"12\", null, \"٣\", \"Ⅳ\", \"1a\", \"\"]",
                   boolean(), "[true, null, true, true, false, false]");
  // These are numerical according to Python, but we don't have the information in
  // utf8proc for this
  // this->CheckUnary("utf8_is_numeric", "[\"㐅\", \"卌\"]", boolean(),
  //                  "[true, null, true, true, false, false]");
}

TYPED_TEST(TestStringKernels, IsLowerUnicode) {
  // ٣ is arabic 3 (decimal), Φ capital
  this->CheckUnary("utf8_is_lower",
                   "[\"12\", null, \"٣a\", \"٣A\", \"1a\", \"Φ\", \"\", \"with space\", "
                   "\"With space\"]",
                   boolean(),
                   "[false, null, true, false, true, false, false, true, false]");
  // lower case character utf8proc does not know about
  // this->CheckUnary("utf8_is_lower", "[\"ª\", \"ₕ\"]", boolean(), "[true,
  // true]");
}

TYPED_TEST(TestStringKernels, IsPrintableUnicode) {
  // U+2008 (utf8: \xe2\x80\x88) is punctuation space, it is NOT printable
  // U+0378 (utf8: \xCD\xB8) is an undefined char, it has no category
  this->CheckUnary(
      "utf8_is_printable",
      "[\" 123azAZ!~\", null, \"\xe2\x80\x88\", \"\", \"\\r\", \"\xCD\xB8\"]", boolean(),
      "[true, null, false, true, false, false]");
}

TYPED_TEST(TestStringKernels, IsSpaceUnicode) {
  // U+2008 (utf8: \xe2\x80\x88) is punctuation space
  this->CheckUnary("utf8_is_space", "[\" \", null, \"  \", \"\\t\\r\"]", boolean(),
                   "[true, null, true, true]");
  this->CheckUnary("utf8_is_space", "[\" a\", null, \"a \", \"~\", \"\xe2\x80\x88\"]",
                   boolean(), "[false, null, false, false, true]");
}

TYPED_TEST(TestStringKernels, IsTitleUnicode) {
  // ٣ is arabic 3 (decimal), Φ capital
  this->CheckUnary("utf8_is_title",
                   "[\"Is\", null, \"Is Title\", \"Is٣Title\", \"Is_Ǆ\", \"Φ\", \"Ǆ\"]",
                   boolean(), "[true, null, true, true, true, true, true]");
  this->CheckUnary(
      "utf8_is_title",
      "[\"IsN\", null, \"IsNoTitle\", \"Is No T٣tle\", \"IsǄ\", \"ΦΦ\", \"ǆ\", \"_\"]",
      boolean(), "[false, null, false, false, false, false, false, false]");
}

// Older versions of utf8proc fail
#if !(UTF8PROC_VERSION_MAJOR <= 2 && UTF8PROC_VERSION_MINOR < 5)

TYPED_TEST(TestStringKernels, IsUpperUnicode) {
  // ٣ is arabic 3 (decimal), Φ capital
  this->CheckUnary("utf8_is_upper",
                   "[\"12\", null, \"٣a\", \"٣A\", \"1A\", \"Φ\", \"\", \"Ⅰ\", \"Ⅿ\"]",
                   boolean(),
                   "[false, null, false, true, true, true, false, true, true]");
  // * Ⅰ to Ⅿ is a special case (roman capital), as well as Ⓐ to Ⓩ
  // * ϒ - \xCF\x92 - Greek Upsilon with Hook Symbol - upper case, but has no direct lower
  // case
  // * U+1F88 - ᾈ - \E1\xBE\x88 - Greek Capital Letter Alpha with Psili and Prosgegrammeni
  // - title case
  // U+10400 - 𐐀 - \xF0x90x90x80 - Deseret Capital Letter Long - upper case
  // * U+A7BA - Ꞻ - \xEA\x9E\xBA - Latin Capital Letter Glottal A -  new in unicode 13
  // (not tested since it depends on the version of libutf8proc)
  // * U+A7BB - ꞻ - \xEA\x9E\xBB - Latin Small Letter Glottal A - new in unicode 13
  this->CheckUnary("utf8_is_upper",
                   "[\"Ⓐ\", \"Ⓩ\", \"ϒ\", \"ᾈ\", \"\xEA\x9E\xBA\", \"xF0x90x90x80\"]",
                   boolean(), "[true, true, true, false, true, false]");
}

#endif  // UTF8PROC_VERSION_MINOR >= 5

#endif  // ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, IsAlphaNumericAscii) {
  this->CheckUnary("ascii_is_alnum",
                   "[\"ⱭɽⱤoW123\", null, \"Ɑ2\", \"!\", \"\", \"a space\", \"1 space\"]",
                   boolean(), "[false, null, false, false, false, false, false]");
  this->CheckUnary("ascii_is_alnum", "[\"aRoW123\", null, \"a2\", \"a\", \"2\", \"\"]",
                   boolean(), "[true, null, true, true, true, false]");
}

TYPED_TEST(TestStringKernels, IsAlphaAscii) {
  this->CheckUnary("ascii_is_alpha", "[\"ⱭɽⱤoW\", \"arrow\", null, \"a2\", \"!\", \"\"]",
                   boolean(), "[false, true, null, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsDecimalAscii) {
  // ٣ is arabic 3
  this->CheckUnary("ascii_is_decimal", "[\"12\", null, \"٣\", \"Ⅳ\", \"1a\", \"\"]",
                   boolean(), "[true, null, false, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsLowerAscii) {
  // ٣ is arabic 3 (decimal), φ lower greek
  this->CheckUnary("ascii_is_lower",
                   "[\"12\", null, \"٣a\", \"٣A\", \"1a\", \"φ\", \"\"]", boolean(),
                   "[false, null, true, false, true, false, false]");
}
TYPED_TEST(TestStringKernels, IsPrintableAscii) {
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("ascii_is_printable",
                   "[\" 123azAZ!~\", null, \"\xe2\x80\x88\", \"\", \"\\r\"]", boolean(),
                   "[true, null, false, true, false]");
}

TYPED_TEST(TestStringKernels, IsSpaceAscii) {
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("ascii_is_space", "[\" \", null, \"  \", \"\\t\\r\"]", boolean(),
                   "[true, null, true, true]");
  this->CheckUnary("ascii_is_space", "[\" a\", null, \"a \", \"~\", \"\xe2\x80\x88\"]",
                   boolean(), "[false, null, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsTitleAscii) {
  // ٣ is Arabic 3 (decimal), Φ capital
  this->CheckUnary("ascii_is_title",
                   "[\"Is\", null, \"Is Title\", \"Is٣Title\", \"Is_Ǆ\", \"Φ\", \"Ǆ\"]",
                   boolean(), "[true, null, true, true, true, false, false]");
  this->CheckUnary(
      "ascii_is_title",
      "[\"IsN\", null, \"IsNoTitle\", \"Is No T٣tle\", \"IsǄ\", \"ΦΦ\", \"ǆ\", \"_\"]",
      boolean(), "[false, null, false, false, true, false, false, false]");
}

TYPED_TEST(TestStringKernels, IsUpperAscii) {
  // ٣ is arabic 3 (decimal), Φ capital greek
  this->CheckUnary("ascii_is_upper",
                   "[\"12\", null, \"٣a\", \"٣A\", \"1A\", \"Φ\", \"\"]", boolean(),
                   "[false, null, false, true, true, false, false]");
}

TYPED_TEST(TestBaseBinaryKernels, MatchSubstring) {
  MatchSubstringOptions options{"ab"};
  this->CheckUnary("match_substring", "[]", boolean(), "[]", &options);
  this->CheckUnary("match_substring", R"(["abc", "acb", "cab", null, "bac", "AB"])",
                   boolean(), "[true, false, true, null, false, false]", &options);

  MatchSubstringOptions options_repeated{"abab"};
  this->CheckUnary("match_substring", R"(["abab", "ab", "cababc", null, "bac"])",
                   boolean(), "[true, false, true, null, false]", &options_repeated);

  // ARROW-9460
  MatchSubstringOptions options_double_char{"aab"};
  this->CheckUnary("match_substring", R"(["aacb", "aab", "ab", "aaab"])", boolean(),
                   "[false, true, false, true]", &options_double_char);
  MatchSubstringOptions options_double_char_2{"bbcaa"};
  this->CheckUnary("match_substring", R"(["abcbaabbbcaabccabaab"])", boolean(), "[true]",
                   &options_double_char_2);

  MatchSubstringOptions options_empty{""};
  this->CheckUnary("match_substring", "[]", boolean(), "[]", &options);
  this->CheckUnary("match_substring", R"(["abc", "acb", "cab", null, "bac", "AB", ""])",
                   boolean(), "[true, true, true, null, true, true, true]",
                   &options_empty);
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestStringKernels, MatchSubstringIgnoreCase) {
  MatchSubstringOptions options_insensitive{"aé(", /*ignore_case=*/true};
  this->CheckUnary("match_substring", R"(["abc", "aEb", "baÉ(", "aé(", "ae(", "Aé("])",
                   boolean(), "[false, false, true, true, false, true]",
                   &options_insensitive);
}
#else
TYPED_TEST(TestBaseBinaryKernels, MatchSubstringIgnoreCase) {
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");
  MatchSubstringOptions options{"a", /*ignore_case=*/true};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("match_substring", {input}, &options));
}
#endif

TYPED_TEST(TestBaseBinaryKernels, MatchStartsWith) {
  MatchSubstringOptions options{"abab"};
  this->CheckUnary("starts_with", "[]", boolean(), "[]", &options);
  this->CheckUnary("starts_with", R"([null, "", "ab", "abab", "$abab", "abab$"])",
                   boolean(), "[null, false, false, true, false, true]", &options);
  this->CheckUnary("starts_with", R"(["ABAB", "BABAB", "ABABC", "bAbAb", "aBaBc"])",
                   boolean(), "[false, false, false, false, false]", &options);
}

TYPED_TEST(TestBaseBinaryKernels, MatchEndsWith) {
  MatchSubstringOptions options{"abab"};
  this->CheckUnary("ends_with", "[]", boolean(), "[]", &options);
  this->CheckUnary("ends_with", R"([null, "", "ab", "abab", "$abab", "abab$"])",
                   boolean(), "[null, false, false, true, true, false]", &options);
  this->CheckUnary("ends_with", R"(["ABAB", "BABAB", "ABABC", "bAbAb", "aBaBc"])",
                   boolean(), "[false, false, false, false, false]", &options);
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBaseBinaryKernels, MatchStartsWithIgnoreCase) {
  MatchSubstringOptions options{"aBAb", /*ignore_case=*/true};
  this->CheckUnary("starts_with", "[]", boolean(), "[]", &options);
  this->CheckUnary("starts_with", R"([null, "", "ab", "abab", "$abab", "abab$"])",
                   boolean(), "[null, false, false, true, false, true]", &options);
  this->CheckUnary("starts_with", R"(["ABAB", "$ABAB", "ABAB$", "$AbAb", "aBaB$"])",
                   boolean(), "[true, false, true, false, true]", &options);
}

TYPED_TEST(TestBaseBinaryKernels, MatchEndsWithIgnoreCase) {
  MatchSubstringOptions options{"aBAb", /*ignore_case=*/true};
  this->CheckUnary("ends_with", "[]", boolean(), "[]", &options);
  this->CheckUnary("ends_with", R"([null, "", "ab", "abab", "$abab", "abab$"])",
                   boolean(), "[null, false, false, true, true, false]", &options);
  this->CheckUnary("ends_with", R"(["ABAB", "$ABAB", "ABAB$", "$AbAb", "aBaB$"])",
                   boolean(), "[true, true, false, true, false]", &options);
}
#else
TYPED_TEST(TestBaseBinaryKernels, MatchStartsWithIgnoreCase) {
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");
  MatchSubstringOptions options{"a", /*ignore_case=*/true};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("starts_with", {input}, &options));
}

TYPED_TEST(TestBaseBinaryKernels, MatchEndsWithIgnoreCase) {
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");
  MatchSubstringOptions options{"a", /*ignore_case=*/true};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("ignore_case requires RE2"),
                                  CallFunction("ends_with", {input}, &options));
}
#endif

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestStringKernels, MatchSubstringRegex) {
  MatchSubstringOptions options{"ab"};
  this->CheckUnary("match_substring_regex", "[]", boolean(), "[]", &options);
  this->CheckUnary("match_substring_regex", R"(["abc", "acb", "cab", null, "bac", "AB"])",
                   boolean(), "[true, false, true, null, false, false]", &options);
  MatchSubstringOptions options_repeated{"(ab){2}"};
  this->CheckUnary("match_substring_regex", R"(["abab", "ab", "cababc", null, "bac"])",
                   boolean(), "[true, false, true, null, false]", &options_repeated);
  MatchSubstringOptions options_digit{"\\d"};
  this->CheckUnary("match_substring_regex", R"(["aacb", "a2ab", "", "24"])", boolean(),
                   "[false, true, false, true]", &options_digit);
  MatchSubstringOptions options_star{"a*b"};
  this->CheckUnary("match_substring_regex", R"(["aacb", "aab", "dab", "caaab", "b", ""])",
                   boolean(), "[true, true, true, true, true, false]", &options_star);
  MatchSubstringOptions options_plus{"a+b"};
  this->CheckUnary("match_substring_regex", R"(["aacb", "aab", "dab", "caaab", "b", ""])",
                   boolean(), "[false, true, true, true, false, false]", &options_plus);
  MatchSubstringOptions options_insensitive{"ab|é", /*ignore_case=*/true};
  this->CheckUnary("match_substring_regex", R"(["abc", "acb", "É", null, "bac", "AB"])",
                   boolean(), "[true, false, true, null, false, true]",
                   &options_insensitive);

  // Unicode character semantics
  // "\pL" means: unicode category "letter"
  // (re2 interprets "\w" as ASCII-only: https://github.com/google/re2/wiki/Syntax)
  MatchSubstringOptions options_unicode{"^\\pL+$"};
  this->CheckUnary("match_substring_regex", R"(["été", "ß", "€", ""])", boolean(),
                   "[true, true, false, false]", &options_unicode);
}

TYPED_TEST(TestBaseBinaryKernels, MatchSubstringRegexNoOptions) {
  Datum input = ArrayFromJSON(this->type(), "[]");
  ASSERT_RAISES(Invalid, CallFunction("match_substring_regex", {input}));
}

TYPED_TEST(TestBaseBinaryKernels, MatchSubstringRegexInvalid) {
  Datum input = ArrayFromJSON(this->type(), "[null]");
  MatchSubstringOptions options{"invalid["};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Invalid regular expression: missing ]"),
      CallFunction("match_substring_regex", {input}, &options));
}

TYPED_TEST(TestStringKernels, MatchLike) {
  auto inputs = R"(["foo", "bar", "foobar", "barfoo", "o", "\nfoo", "foo\n", null])";

  MatchSubstringOptions prefix_match{"foo%"};
  this->CheckUnary("match_like", "[]", boolean(), "[]", &prefix_match);
  this->CheckUnary("match_like", inputs, boolean(),
                   "[true, false, true, false, false, false, true, null]", &prefix_match);

  MatchSubstringOptions suffix_match{"%foo"};
  this->CheckUnary("match_like", inputs, boolean(),
                   "[true, false, false, true, false, true, false, null]", &suffix_match);

  MatchSubstringOptions substring_match{"%foo%"};
  this->CheckUnary("match_like", inputs, boolean(),
                   "[true, false, true, true, false, true, true, null]",
                   &substring_match);

  MatchSubstringOptions trivial_match{"%%"};
  this->CheckUnary("match_like", inputs, boolean(),
                   "[true, true, true, true, true, true, true, null]", &trivial_match);

  MatchSubstringOptions regex_match{"foo%bar"};
  this->CheckUnary("match_like", inputs, boolean(),
                   "[false, false, true, false, false, false, false, null]",
                   &regex_match);

  // ignore_case means this still gets mapped to a regex search
  MatchSubstringOptions insensitive_substring{"%é%", /*ignore_case=*/true};
  this->CheckUnary("match_like", R"(["é", "fooÉbar", "e"])", boolean(),
                   "[true, true, false]", &insensitive_substring);

  MatchSubstringOptions insensitive_regex{"_é%", /*ignore_case=*/true};
  this->CheckUnary("match_like", R"(["éfoo", "aÉfoo", "e"])", boolean(),
                   "[false, true, false]", &insensitive_regex);
}

TYPED_TEST(TestBaseBinaryKernels, MatchLikeEscaping) {
  auto inputs = R"(["%%foo", "_bar", "({", "\\baz"])";

  // N.B. I believe Impala mistakenly optimizes these into substring searches
  MatchSubstringOptions escape_percent{"\\%%"};
  this->CheckUnary("match_like", inputs, boolean(), "[true, false, false, false]",
                   &escape_percent);

  MatchSubstringOptions not_substring{"%\\%%"};
  this->CheckUnary("match_like", inputs, boolean(), "[true, false, false, false]",
                   &not_substring);

  MatchSubstringOptions escape_underscore{"\\____"};
  this->CheckUnary("match_like", inputs, boolean(), "[false, true, false, false]",
                   &escape_underscore);

  MatchSubstringOptions escape_regex{"(%"};
  this->CheckUnary("match_like", inputs, boolean(), "[false, false, true, false]",
                   &escape_regex);

  MatchSubstringOptions escape_escape{"\\\\%"};
  this->CheckUnary("match_like", inputs, boolean(), "[false, false, false, true]",
                   &escape_escape);

  MatchSubstringOptions special_chars{"!@#$^&*()[]{}.?"};
  this->CheckUnary("match_like", R"(["!@#$^&*()[]{}.?"])", boolean(), "[true]",
                   &special_chars);

  MatchSubstringOptions escape_sequences{"\n\t%"};
  this->CheckUnary("match_like", R"(["\n\tfoo\t", "\n\t", "\n"])", boolean(),
                   "[true, true, false]", &escape_sequences);
}
#endif

TYPED_TEST(TestBaseBinaryKernels, SplitBasics) {
  SplitPatternOptions options{" "};
  // basics
  this->CheckUnary("split_pattern", R"(["foo bar", "foo"])", list(this->type()),
                   R"([["foo", "bar"], ["foo"]])", &options);
  this->CheckUnary("split_pattern", R"(["foo bar", "foo", null])", list(this->type()),
                   R"([["foo", "bar"], ["foo"], null])", &options);
  // edgy cases
  this->CheckUnary("split_pattern", R"(["f  o o "])", list(this->type()),
                   R"([["f", "", "o", "o", ""]])", &options);
  this->CheckUnary("split_pattern", "[]", list(this->type()), "[]", &options);
  // longer patterns
  SplitPatternOptions options_long{"---"};
  this->CheckUnary("split_pattern", R"(["-foo---bar--", "---foo---b"])",
                   list(this->type()), R"([["-foo", "bar--"], ["", "foo", "b"]])",
                   &options_long);
  SplitPatternOptions options_long_reverse{"---", -1, /*reverse=*/true};
  this->CheckUnary("split_pattern", R"(["-foo---bar--", "---foo---b"])",
                   list(this->type()), R"([["-foo", "bar--"], ["", "foo", "b"]])",
                   &options_long_reverse);
}

TYPED_TEST(TestBaseBinaryKernels, SplitMax) {
  SplitPatternOptions options{"---", 2};
  SplitPatternOptions options_reverse{"---", 2, /*reverse=*/true};
  this->CheckUnary("split_pattern", R"(["foo---bar", "foo", "foo---bar------ar"])",
                   list(this->type()),
                   R"([["foo", "bar"], ["foo"], ["foo", "bar", "---ar"]])", &options);
  this->CheckUnary(
      "split_pattern", R"(["foo---bar", "foo", "foo---bar------ar"])", list(this->type()),
      R"([["foo", "bar"], ["foo"], ["foo---bar", "", "ar"]])", &options_reverse);
}

TYPED_TEST(TestStringKernels, SplitWhitespaceAscii) {
  SplitOptions options;
  SplitOptions options_max{1};
  // basics
  this->CheckUnary("ascii_split_whitespace", R"(["foo bar", "foo  bar \tba"])",
                   list(this->type()), R"([["foo", "bar"], ["foo", "bar", "ba"]])",
                   &options);
  this->CheckUnary("ascii_split_whitespace", R"(["foo bar", "foo  bar \tba"])",
                   list(this->type()), R"([["foo", "bar"], ["foo", "bar \tba"]])",
                   &options_max);
}

TYPED_TEST(TestStringKernels, SplitWhitespaceAsciiReverse) {
  SplitOptions options{-1, /*reverse=*/true};
  SplitOptions options_max{1, /*reverse=*/true};
  // basics
  this->CheckUnary("ascii_split_whitespace", R"(["foo bar", "foo  bar \tba"])",
                   list(this->type()), R"([["foo", "bar"], ["foo", "bar", "ba"]])",
                   &options);
  this->CheckUnary("ascii_split_whitespace", R"(["foo bar", "foo  bar \tba"])",
                   list(this->type()), R"([["foo", "bar"], ["foo  bar", "ba"]])",
                   &options_max);
}

#ifdef ARROW_WITH_UTF8PROC
TYPED_TEST(TestStringKernels, SplitWhitespaceUTF8) {
  SplitOptions options;
  SplitOptions options_max{1};
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("utf8_split_whitespace",
                   "[\"foo bar\", \"foo\xe2\x80\x88  bar \\tba\"]", list(this->type()),
                   R"([["foo", "bar"], ["foo", "bar", "ba"]])", &options);
  this->CheckUnary("utf8_split_whitespace",
                   "[\"foo bar\", \"foo\xe2\x80\x88  bar \\tba\"]", list(this->type()),
                   R"([["foo", "bar"], ["foo", "bar \tba"]])", &options_max);
}

TYPED_TEST(TestStringKernels, SplitWhitespaceUTF8Reverse) {
  SplitOptions options{-1, /*reverse=*/true};
  SplitOptions options_max{1, /*reverse=*/true};
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("utf8_split_whitespace",
                   "[\"foo bar\", \"foo\xe2\x80\x88  bar \\tba\"]", list(this->type()),
                   R"([["foo", "bar"], ["foo", "bar", "ba"]])", &options);
  this->CheckUnary("utf8_split_whitespace",
                   "[\"foo bar\", \"foo\xe2\x80\x88  bar \\tba\"]", list(this->type()),
                   "[[\"foo\", \"bar\"], [\"foo\xe2\x80\x88  bar\", \"ba\"]]",
                   &options_max);
}
#endif

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBaseBinaryKernels, SplitRegex) {
  SplitPatternOptions options{"a+|b"};

  this->CheckUnary(
      "split_pattern_regex", R"(["aaaab", "foob", "foo bar", "foo", "AaaaBaaaC", null])",
      list(this->type()),
      R"([["", "", ""], ["foo", ""], ["foo ", "", "r"], ["foo"], ["A", "B", "C"], null])",
      &options);

  options.max_splits = 1;
  this->CheckUnary(
      "split_pattern_regex", R"(["aaaab", "foob", "foo bar", "foo", "AaaaBaaaC", null])",
      list(this->type()),
      R"([["", "b"], ["foo", ""], ["foo ", "ar"], ["foo"], ["A", "BaaaC"], null])",
      &options);
}

TYPED_TEST(TestBaseBinaryKernels, SplitRegexReverse) {
  SplitPatternOptions options{"a+|b", /*max_splits=*/1, /*reverse=*/true};
  Datum input = ArrayFromJSON(this->type(), R"(["a"])");

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, ::testing::HasSubstr("Cannot split in reverse with regex"),
      CallFunction("split_pattern_regex", {input}, &options));
}
#endif

TYPED_TEST(TestStringKernels, Utf8ReplaceSlice) {
  ReplaceSliceOptions options{0, 1, "χχ"};
  this->CheckUnary("utf8_replace_slice", "[]", this->type(), "[]", &options);
  this->CheckUnary("utf8_replace_slice", R"([null, "", "π", "πb", "πbθ"])", this->type(),
                   R"([null, "χχ", "χχ", "χχb", "χχbθ"])", &options);

  ReplaceSliceOptions options_whole{0, 5, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθde", "πbθdef"])", this->type(),
                   R"([null, "χχ", "χχ", "χχ", "χχ", "χχ", "χχf"])", &options_whole);

  ReplaceSliceOptions options_middle{2, 4, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "πχχ", "πbχχ", "πbχχ", "πbχχ", "πbχχe"])",
                   &options_middle);

  ReplaceSliceOptions options_neg_start{-3, -2, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "χχπ", "χχπb", "χχbθ", "πχχθd", "πbχχde"])",
                   &options_neg_start);

  ReplaceSliceOptions options_neg_end{2, -2, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "πχχ", "πbχχ", "πbχχθ", "πbχχθd", "πbχχde"])",
                   &options_neg_end);

  ReplaceSliceOptions options_neg_pos{-1, 2, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "χχ", "πχχ", "πbχχθ", "πbθχχd", "πbθdχχe"])",
                   &options_neg_pos);

  // Effectively the same as [2, 2)
  ReplaceSliceOptions options_flip{2, 0, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "πχχ", "πbχχ", "πbχχθ", "πbχχθd", "πbχχθde"])",
                   &options_flip);

  // Effectively the same as [-3, -3)
  ReplaceSliceOptions options_neg_flip{-3, -5, "χχ"};
  this->CheckUnary("utf8_replace_slice",
                   R"([null, "", "π", "πb", "πbθ", "πbθd", "πbθde"])", this->type(),
                   R"([null, "χχ", "χχπ", "χχπb", "χχπbθ", "πχχbθd", "πbχχθde"])",
                   &options_neg_flip);
}

TYPED_TEST(TestBaseBinaryKernels, ReplaceSubstring) {
  ReplaceSubstringOptions options{"foo", "bazz"};
  this->CheckUnary("replace_substring", R"(["foo", "this foo that foo", null])",
                   this->type(), R"(["bazz", "this bazz that bazz", null])", &options);

  options = ReplaceSubstringOptions{"foo", "bazz", 1};
  this->CheckUnary("replace_substring", R"(["foo", "this foo that foo", null])",
                   this->type(), R"(["bazz", "this bazz that foo", null])", &options);

  Datum input = ArrayFromJSON(this->type(), "[]");
  ASSERT_RAISES(Invalid, CallFunction("replace_substring", {input}));
}

#ifdef ARROW_WITH_RE2
TYPED_TEST(TestBaseBinaryKernels, ReplaceSubstringRegex) {
  ReplaceSubstringOptions options{"(fo+)\\s*", "\\1-bazz"};
  this->CheckUnary("replace_substring_regex", R"(["foo ", "this foo   that foo", null])",
                   this->type(), R"(["foo-bazz", "this foo-bazzthat foo-bazz", null])",
                   &options);
  // make sure we match non-overlapping
  options = ReplaceSubstringOptions{"(a.a)", "aba\\1"};
  this->CheckUnary("replace_substring_regex", R"(["aaaaaa"])", this->type(),
                   R"(["abaaaaabaaaa"])", &options);

  // ARROW-18202: Allow matching against empty string again
  options = ReplaceSubstringOptions{"^$", "x"};
  this->CheckUnary("replace_substring_regex", R"([""])", this->type(), R"(["x"])",
                   &options);

  // ARROW-12774
  options = ReplaceSubstringOptions{"X", "Y"};
  this->CheckUnary("replace_substring_regex",
                   R"(["A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A"])",
                   this->type(),
                   R"(["A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A"])",
                   &options);

  // With a finite number of replacements
  options = ReplaceSubstringOptions{"foo", "bazz", 1};
  this->CheckUnary("replace_substring", R"(["foo", "this foo that foo", null])",
                   this->type(), R"(["bazz", "this bazz that foo", null])", &options);

  options = ReplaceSubstringOptions{"(fo+)\\s*", "\\1-bazz", 1};
  this->CheckUnary("replace_substring_regex", R"(["foo ", "this foo   that foo", null])",
                   this->type(), R"(["foo-bazz", "this foo-bazzthat foo", null])",
                   &options);
}

TYPED_TEST(TestBaseBinaryKernels, ReplaceSubstringRegexInvalid) {
  {
    Datum input = ArrayFromJSON(this->type(), "[]");
    ASSERT_RAISES(Invalid, CallFunction("replace_substring_regex", {input}));
  }
  {
    Datum input = ArrayFromJSON(this->type(), R"(["foo"])");
    ReplaceSubstringOptions options{"invalid[", ""};
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Invalid regular expression: missing ]"),
        CallFunction("replace_substring_regex", {input}, &options));

    // Capture group number out of range
    options = ReplaceSubstringOptions{"(.)", "\\9"};
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Invalid replacement string"),
        CallFunction("replace_substring_regex", {input}, &options));
  }
}

TYPED_TEST(TestBaseBinaryKernels, ExtractRegex) {
  ExtractRegexOptions options{"(?P<letter>[ab])(?P<digit>\\d)"};
  auto type = struct_({field("letter", this->type()), field("digit", this->type())});
  this->CheckUnary("extract_regex", R"([])", type, R"([])", &options);
  this->CheckUnary(
      "extract_regex", R"(["a1", "b2", "c3", null])", type,
      R"([{"letter": "a", "digit": "1"}, {"letter": "b", "digit": "2"}, null, null])",
      &options);
  this->CheckUnary(
      "extract_regex", R"(["a1", "c3", null, "b2"])", type,
      R"([{"letter": "a", "digit": "1"}, null, null, {"letter": "b", "digit": "2"}])",
      &options);
  this->CheckUnary("extract_regex", R"(["a1", "b2"])", type,
                   R"([{"letter": "a", "digit": "1"}, {"letter": "b", "digit": "2"}])",
                   &options);
  this->CheckUnary("extract_regex", R"(["a1", "zb3z"])", type,
                   R"([{"letter": "a", "digit": "1"}, {"letter": "b", "digit": "3"}])",
                   &options);
}

TYPED_TEST(TestBaseBinaryKernels, ExtractRegexNoCapture) {
  // XXX Should we accept this or is it a user error?
  ExtractRegexOptions options{"foo"};
  auto type = struct_({});
  this->CheckUnary("extract_regex", R"(["oofoo", "bar", null])", type,
                   R"([{}, null, null])", &options);
}

TYPED_TEST(TestBaseBinaryKernels, ExtractRegexNoOptions) {
  Datum input = ArrayFromJSON(this->type(), "[]");
  ASSERT_RAISES(Invalid, CallFunction("extract_regex", {input}));
}

TYPED_TEST(TestBaseBinaryKernels, ExtractRegexInvalid) {
  Datum input = ArrayFromJSON(this->type(), "[]");
  ExtractRegexOptions options{"invalid["};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Invalid regular expression: missing ]"),
      CallFunction("extract_regex", {input}, &options));

  options = ExtractRegexOptions{"(.)"};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Regular expression contains unnamed groups"),
      CallFunction("extract_regex", {input}, &options));
}

#endif

TYPED_TEST(TestStringKernels, Strptime) {
  std::string input1 = R"(["5/1/2020", null, null, "12/13/1900", null])";
  std::string input2 = R"(["5-1-2020", "12/13/1900"])";
  std::string input3 = R"(["5/1/2020", "AA/BB/CCCC"])";
  std::string input4 = R"(["5/1/2020", "AA/BB/CCCC", "AA/BB/CCCC", "AA/BB/CCCC", null])";
  std::string input5 = R"(["5/1/2020 %z", null, null, "12/13/1900 %z", null])";
  std::string output1 = R"(["2020-05-01", null, null, "1900-12-13", null])";
  std::string output2 = R"([null, "1900-12-13"])";
  std::string output3 = R"(["2020-05-01", null])";
  std::string output4 = R"(["2020-01-05", null, null, null, null])";

  StrptimeOptions options("%m/%d/%Y", TimeUnit::MICRO, /*error_is_null=*/true);
  auto unit = timestamp(TimeUnit::MICRO);
  this->CheckUnary("strptime", input1, unit, output1, &options);
  this->CheckUnary("strptime", input2, unit, output2, &options);
  this->CheckUnary("strptime", input3, unit, output3, &options);

  options.format = "%d/%m/%Y";
  this->CheckUnary("strptime", input4, unit, output4, &options);

  options.format = "%m/%d/%Y %%z";
  this->CheckUnary("strptime", input5, unit, output1, &options);

  options.error_is_null = false;
  this->CheckUnary("strptime", input5, unit, output1, &options);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid: Failed to parse string: '5/1/2020'"),
      Strptime(ArrayFromJSON(this->type(), input1), options));
}

TYPED_TEST(TestStringKernels, StrptimeZoneOffset) {
  if (!arrow::internal::kStrptimeSupportsZone) {
    GTEST_SKIP() << "strptime does not support %z on this platform";
  }
  // N.B. BSD strptime only supports (+/-)HHMM and not the wider range
  // of values GNU strptime supports.
  std::string input1 = R"(["5/1/2020 +0100", null, "12/11/1900 -0130"])";
  std::string output1 =
      R"(["2020-04-30T23:00:00.000000", null, "1900-12-11T01:30:00.000000"])";
  StrptimeOptions options("%m/%d/%Y %z", TimeUnit::MICRO, /*error_is_null=*/true);
  this->CheckUnary("strptime", input1, timestamp(TimeUnit::MICRO, "UTC"), output1,
                   &options);
}

TYPED_TEST(TestStringKernels, StrptimeDoesNotProvideDefaultOptions) {
  auto input = ArrayFromJSON(this->type(), R"(["2020-05-01", null, "1900-12-11"])");
  ASSERT_RAISES(Invalid, CallFunction("strptime", {input}));
}

TYPED_TEST(TestStringKernels, BinaryJoin) {
  // Scalar separator
  auto separator = this->scalar("--");
  std::string list_json =
      R"([["a", "bb", "ccc"], [], null, ["dd"], ["eee", null], ["ff", ""]])";
  auto expected =
      ArrayFromJSON(this->type(), R"(["a--bb--ccc", "", null, "dd", null, "ff--"])");
  CheckScalarBinary("binary_join", ArrayFromJSON(list(this->type()), list_json),
                    Datum(separator), expected);
  CheckScalarBinary("binary_join", ArrayFromJSON(large_list(this->type()), list_json),
                    Datum(separator), expected);

  auto separator_null = MakeNullScalar(this->type());
  expected = ArrayFromJSON(this->type(), R"([null, null, null, null, null, null])");
  CheckScalarBinary("binary_join", ArrayFromJSON(list(this->type()), list_json),
                    separator_null, expected);
  CheckScalarBinary("binary_join", ArrayFromJSON(large_list(this->type()), list_json),
                    separator_null, expected);

  // Array list, Array separator
  auto separators =
      ArrayFromJSON(this->type(), R"(["1", "2", "3", "4", "5", "6", null])");
  list_json =
      R"([["a", "bb", "ccc"], [], null, ["dd"], ["eee", null], ["ff", ""], ["hh", "ii"]])";
  expected =
      ArrayFromJSON(this->type(), R"(["a1bb1ccc", "", null, "dd", null, "ff6", null])");
  CheckScalarBinary("binary_join", ArrayFromJSON(list(this->type()), list_json),
                    separators, expected);
  CheckScalarBinary("binary_join", ArrayFromJSON(large_list(this->type()), list_json),
                    separators, expected);

  // Scalar list, Array separator
  separators = ArrayFromJSON(this->type(), R"(["1", "", null])");
  list_json = R"(["a", "bb", "ccc"])";
  expected = ArrayFromJSON(this->type(), R"(["a1bb1ccc", "abbccc", null])");
  CheckScalarBinary("binary_join", ScalarFromJSON(list(this->type()), list_json),
                    separators, expected);
  CheckScalarBinary("binary_join", ScalarFromJSON(large_list(this->type()), list_json),
                    separators, expected);
  list_json = R"(["a", "bb", null])";
  expected = ArrayFromJSON(this->type(), R"([null, null, null])");
  CheckScalarBinary("binary_join", ScalarFromJSON(list(this->type()), list_json),
                    separators, expected);
  CheckScalarBinary("binary_join", ScalarFromJSON(large_list(this->type()), list_json),
                    separators, expected);
}

TYPED_TEST(TestStringKernels, PadUTF8) {
  // \xe2\x80\x88 = \u2008 is punctuation space, \xc3\xa1 = \u00E1 = á
  PadOptions options{/*width=*/5, "\xe2\x80\x88"};
  this->CheckUnary(
      "utf8_center", R"([null, "a", "bb", "b\u00E1r", "foobar"])", this->type(),
      R"([null, "\u2008\u2008a\u2008\u2008", "\u2008bb\u2008\u2008", "\u2008b\u00E1r\u2008", "foobar"])",
      &options);
  this->CheckUnary(
      "utf8_lpad", R"([null, "a", "bb", "b\u00E1r", "foobar"])", this->type(),
      R"([null, "\u2008\u2008\u2008\u2008a", "\u2008\u2008\u2008bb", "\u2008\u2008b\u00E1r", "foobar"])",
      &options);
  this->CheckUnary(
      "utf8_rpad", R"([null, "a", "bb", "b\u00E1r", "foobar"])", this->type(),
      R"([null, "a\u2008\u2008\u2008\u2008", "bb\u2008\u2008\u2008", "b\u00E1r\u2008\u2008", "foobar"])",
      &options);

  PadOptions options_bad{/*width=*/3, /*padding=*/"spam"};
  auto input = ArrayFromJSON(this->type(), R"(["foo"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("Padding must be one codepoint"),
                                  CallFunction("utf8_lpad", {input}, &options_bad));
  options_bad.padding = "";
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("Padding must be one codepoint"),
                                  CallFunction("utf8_lpad", {input}, &options_bad));
}

#ifdef ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, TrimWhitespaceUTF8) {
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("utf8_trim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(), "[\"foo\", null, \"bar\", \"foo bar\"]");
  this->CheckUnary("utf8_rtrim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(),
                   "[\" \\tfoo\", null, \"bar\", \" \xe2\x80\x88 foo bar\"]");
  this->CheckUnary("utf8_ltrim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(), "[\"foo\", null, \"bar  \", \"foo bar \"]");
}

TYPED_TEST(TestStringKernels, TrimUTF8) {
  auto options = TrimOptions{"ab"};
  this->CheckUnary("utf8_trim", "[\"azȺz矢ba\", null, \"bab\", \"zȺz\"]", this->type(),
                   "[\"zȺz矢\", null, \"\", \"zȺz\"]", &options);
  this->CheckUnary("utf8_ltrim", "[\"azȺz矢ba\", null, \"bab\", \"zȺz\"]", this->type(),
                   "[\"zȺz矢ba\", null, \"\", \"zȺz\"]", &options);
  this->CheckUnary("utf8_rtrim", "[\"azȺz矢ba\", null, \"bab\", \"zȺz\"]", this->type(),
                   "[\"azȺz矢\", null, \"\", \"zȺz\"]", &options);

  options = TrimOptions{"ȺA"};
  this->CheckUnary("utf8_trim", "[\"ȺȺfoo矢ȺAȺ\", null, \"barȺAȺ\", \"ȺAȺfooȺAȺ矢barA\"]",
                   this->type(), "[\"foo矢\", null, \"bar\", \"fooȺAȺ矢bar\"]", &options);
  this->CheckUnary(
      "utf8_ltrim", "[\"ȺȺfoo矢ȺAȺ\", null, \"barȺAȺ\", \"ȺAȺfooȺAȺ矢barA\"]",
      this->type(), "[\"foo矢ȺAȺ\", null, \"barȺAȺ\", \"fooȺAȺ矢barA\"]", &options);
  this->CheckUnary(
      "utf8_rtrim", "[\"ȺȺfoo矢ȺAȺ\", null, \"barȺAȺ\", \"ȺAȺfooȺAȺ矢barA\"]",
      this->type(), "[\"ȺȺfoo矢\", null, \"bar\", \"ȺAȺfooȺAȺ矢bar\"]", &options);

  TrimOptions options_invalid{"ɑa\xFFɑ"};
  auto input = ArrayFromJSON(this->type(), "[\"foo\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8"),
                                  CallFunction("utf8_trim", {input}, &options_invalid));
}
#endif

// produce test data with e.g.:
// repr([k[-3:1] for k in ["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"]]).replace("'", '"')

#ifdef ARROW_WITH_UTF8PROC
TYPED_TEST(TestStringKernels, SliceCodeunitsBasic) {
  SliceOptions options{2, 4};
  this->CheckUnary("utf8_slice_codeunits", R"(["foo", "fo", null, "foo bar"])",
                   this->type(), R"(["o", "", null, "o "])", &options);
  SliceOptions options_2{2, 3};
  // ensure we slice in codeunits, not graphemes
  // a\u0308 is ä, which is 1 grapheme (character), but two codepoints
  // \u0308 in utf8 encoding is \xcc\x88
  this->CheckUnary("utf8_slice_codeunits", R"(["ää", "bä"])", this->type(),
                   "[\"a\", \"\xcc\x88\"]", &options_2);
  SliceOptions options_empty_pos{6, 6};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓öõ"])", this->type(), R"(["",
  ""])",
                   &options_empty_pos);
  SliceOptions options_empty_neg{-6, -6};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓öõ"])", this->type(), R"(["",
  ""])",
                   &options_empty_neg);
  SliceOptions options_empty_neg_to_zero{-6, 0};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓öõ"])", this->type(), R"(["", ""])",
                   &options_empty_neg_to_zero);

  // end is beyond 0, but before start (hence empty)
  SliceOptions options_edgecase_1{-3, 1};
  this->CheckUnary("utf8_slice_codeunits", R"(["𝑓öõḍš"])", this->type(), R"([""])",
                   &options_edgecase_1);

  // this is a safeguard agains an optimization path possible, but actually a tricky case
  SliceOptions options_edgecase_2{-6, -2};
  this->CheckUnary("utf8_slice_codeunits", R"(["𝑓öõḍš"])", this->type(), R"(["𝑓öõ"])",
                   &options_edgecase_2);

  auto input = ArrayFromJSON(this->type(), R"(["𝑓öõḍš"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr(
          "Function 'utf8_slice_codeunits' cannot be called without options"),
      CallFunction("utf8_slice_codeunits", {input}));

  SliceOptions options_invalid{2, 4, 0};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Slice step cannot be zero"),
      CallFunction("utf8_slice_codeunits", {input}, &options_invalid));
}

TYPED_TEST(TestStringKernels, SliceCodeunitsPosPos) {
  SliceOptions options{2, 4};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "", "õ", "õḍ", "õḍ"])", &options);
  SliceOptions options_step{1, 5, 2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "ö", "ö", "öḍ", "öḍ"])", &options_step);
  SliceOptions options_step_neg{5, 1, -2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "", "õ", "ḍ", "šõ"])", &options_step_neg);
  options_step_neg.stop = 0;
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ","𝑓öõḍš"])",
                   this->type(), R"(["", "", "ö", "õ", "ḍö", "šõ"])", &options_step_neg);
}

TYPED_TEST(TestStringKernels, SliceCodeunitsPosNeg) {
  SliceOptions options{2, -1};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "", "", "õ", "õḍ"])", &options);
  SliceOptions options_step{1, -1, 2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "f", "fö", "föo", "föod","foodš"])",
                   this->type(), R"(["", "", "", "ö", "ö", "od"])", &options_step);
  SliceOptions options_step_neg{3, -4, -2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ","𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "ö", "õ𝑓", "ḍö", "ḍ"])", &options_step_neg);
  options_step_neg.stop = -5;
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ","𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "ö", "õ𝑓", "ḍö", "ḍö"])",
                   &options_step_neg);
}

TYPED_TEST(TestStringKernels, SliceCodeunitsNegNeg) {
  SliceOptions options{-2, -1};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "𝑓", "ö", "õ", "ḍ"])", &options);
  SliceOptions options_step{-4, -1, 2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "𝑓", "𝑓", "𝑓õ", "öḍ"])", &options_step);
  SliceOptions options_step_neg{-1, -3, -2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "ö", "õ", "ḍ", "š"])", &options_step_neg);
  options_step_neg.stop = -4;
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "ö", "õ𝑓", "ḍö", "šõ"])",
                   &options_step_neg);
}

TYPED_TEST(TestStringKernels, SliceCodeunitsNegPos) {
  SliceOptions options{-2, 4};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "𝑓ö", "öõ", "õḍ", "ḍ"])", &options);
  SliceOptions options_step{-4, 4, 2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "𝑓", "𝑓", "𝑓õ", "𝑓õ", "öḍ"])", &options_step);
  SliceOptions options_step_neg{-1, 1, -2};
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "", "õ", "ḍ", "šõ"])", &options_step_neg);
  options_step_neg.stop = 0;
  this->CheckUnary("utf8_slice_codeunits", R"(["", "𝑓", "𝑓ö", "𝑓öõ", "𝑓öõḍ", "𝑓öõḍš"])",
                   this->type(), R"(["", "", "ö", "õ", "ḍö", "šõ"])", &options_step_neg);
}

#endif  // ARROW_WITH_UTF8PROC

TYPED_TEST(TestBinaryKernels, SliceBytesBasic) {
  SliceOptions options{2, 4};
  this->CheckUnary("binary_slice", "[\"fo\xc2\xa2\", \"fo\", null, \"fob \"]",
                   this->type(), "[\"\xc2\xa2\", \"\", null, \"b \"]", &options);

  // end is beyond 0, but before start (hence empty)
  SliceOptions options_edgecase_1{-3, 1};
  this->CheckUnary("binary_slice",
                   "[\"f\xc2\xa2"
                   "ds\"]",
                   this->type(), R"([""])", &options_edgecase_1);

  // this is a safeguard agains an optimization path possible, but actually a tricky case
  SliceOptions options_edgecase_2{-6, -2};
  this->CheckUnary("binary_slice",
                   "[\"f\xc2\xa2"
                   "ds\"]",
                   this->type(), "[\"f\xc2\xa2\"]", &options_edgecase_2);

  auto input = ArrayFromJSON(this->type(), R"(["foods"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Function 'binary_slice' cannot be called without options"),
      CallFunction("binary_slice", {input}));

  SliceOptions options_invalid{2, 4, 0};
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Slice step cannot be zero"),
      CallFunction("binary_slice", {input}, &options_invalid));
}

TYPED_TEST(TestBinaryKernels, SliceBytesPosPos) {
  SliceOptions options{2, 4};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"ab\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"\", \"\xa2\", \"\xc2\xa2\", \"\xc2\xff\"]", &options);
  SliceOptions options_step{1, 5, 2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"ab\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"b\", \"\xc2\", \"b\xa2\", \"b\xff\"]", &options_step);
  SliceOptions options_step_neg{5, 1, -2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"ab\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"\", \"\xa2\", \"\xa2\", \"Z\xc2\"]",
      &options_step_neg);
  options_step_neg.stop = 0;
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"aZ\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"b\", \"\xa2\", \"\xa2Z\", \"Z\xc2\"]",
      &options_step_neg);
}

TYPED_TEST(TestBinaryKernels, SliceBytesPosNeg) {
  SliceOptions options{2, -1};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"aZ\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"\", \"\", \"\xc2\", \"\xc2\xff\"]", &options);
  SliceOptions options_step{1, -1, 2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"a\xc2\xa2\", \"aZ\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"\", \"\xc2\", \"Z\", \"b\xff\"]", &options_step);
  SliceOptions options_step_neg{3, -4, -2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"b\", \"\xa2Z\", \"\xa2Z\", \"\xff\"]",
      &options_step_neg);
  options_step_neg.stop = -5;
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"b\", \"\xa2Z\", \"\xa2Z\", \"\xffP\"]",
      &options_step_neg);
}

TYPED_TEST(TestBinaryKernels, SliceBytesNegNeg) {
  SliceOptions options{-2, -1};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"ab\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"a\", \"\xc2\", \"\xc2\", \"\xff\"]", &options);
  SliceOptions options_step{-4, -1, 2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"a\", \"Z\", \"a\xc2\", \"P\xff\"]", &options_step);
  SliceOptions options_step_neg{-1, -3, -2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"b\", \"\xa2\", \"\xa2\", \"Z\"]", &options_step_neg);
  options_step_neg.stop = -4;
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"b\", \"\xa2Z\", \"\xa2Z\", \"Z\xc2\"]",
      &options_step_neg);
}

TYPED_TEST(TestBinaryKernels, SliceBytesNegPos) {
  SliceOptions options{-2, 4};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"ab\", \"\xc2\xa2\", \"\xc2\xa2\", \"\xff\"]",
      &options);
  SliceOptions options_step{-4, 4, 2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"a\", \"a\", \"Z\xa2\", \"a\xc2\", \"P\xff\"]",
      &options_step);
  SliceOptions options_step_neg{-1, 1, -2};
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"\", \"\xa2\", \"\xa2\", \"Z\xc2\"]",
      &options_step_neg);
  options_step_neg.stop = 0;
  this->CheckUnary(
      "binary_slice",
      "[\"\", \"a\", \"ab\", \"Z\xc2\xa2\", \"aZ\xc2\xa2\", \"aP\xc2\xffZ\"]",
      this->type(), "[\"\", \"\", \"b\", \"\xa2\", \"\xa2Z\", \"Z\xc2\"]",
      &options_step_neg);
}

TYPED_TEST(TestStringKernels, PadAscii) {
  PadOptions options{/*width=*/5, " "};
  this->CheckUnary("ascii_center", R"([null, "a", "bb", "bar", "foobar"])", this->type(),
                   R"([null, "  a  ", " bb  ", " bar ", "foobar"])", &options);
  this->CheckUnary("ascii_lpad", R"([null, "a", "bb", "bar", "foobar"])", this->type(),
                   R"([null, "    a", "   bb", "  bar", "foobar"])", &options);
  this->CheckUnary("ascii_rpad", R"([null, "a", "bb", "bar", "foobar"])", this->type(),
                   R"([null, "a    ", "bb   ", "bar  ", "foobar"])", &options);

  PadOptions options_bad{/*width=*/3, /*padding=*/"spam"};
  auto input = ArrayFromJSON(this->type(), R"(["foo"])");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("Padding must be one byte"),
                                  CallFunction("ascii_lpad", {input}, &options_bad));
  options_bad.padding = "";
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("Padding must be one byte"),
                                  CallFunction("ascii_lpad", {input}, &options_bad));
}

TYPED_TEST(TestStringKernels, TrimWhitespaceAscii) {
  // \xe2\x80\x88 is punctuation space
  this->CheckUnary("ascii_trim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(), "[\"foo\", null, \"bar\", \"\xe2\x80\x88 foo bar\"]");
  this->CheckUnary("ascii_rtrim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(),
                   "[\" \\tfoo\", null, \"bar\", \" \xe2\x80\x88 foo bar\"]");
  this->CheckUnary("ascii_ltrim_whitespace",
                   "[\" \\tfoo\", null, \"bar  \", \" \xe2\x80\x88 foo bar \"]",
                   this->type(), "[\"foo\", null, \"bar  \", \"\xe2\x80\x88 foo bar \"]");
}

TYPED_TEST(TestStringKernels, TrimAscii) {
  TrimOptions options{"BA"};
  this->CheckUnary("ascii_trim", "[\"BBfooBAB\", null, \"barBAB\", \"BABfooBABbarA\"]",
                   this->type(), "[\"foo\", null, \"bar\", \"fooBABbar\"]", &options);
  this->CheckUnary("ascii_ltrim", "[\"BBfooBAB\", null, \"barBAB\", \"BABfooBABbarA\"]",
                   this->type(), "[\"fooBAB\", null, \"barBAB\", \"fooBABbarA\"]",
                   &options);
  this->CheckUnary("ascii_rtrim", "[\"BBfooBAB\", null, \"barBAB\", \"BABfooBABbarA\"]",
                   this->type(), "[\"BBfoo\", null, \"bar\", \"BABfooBABbar\"]",
                   &options);
}

#ifdef ARROW_WITH_UTF8PROC
TEST(TestStringKernels, UnicodeLibraryAssumptions) {
  uint8_t output[4];
  for (utf8proc_int32_t codepoint = 0x100; codepoint < 0x110000; codepoint++) {
    utf8proc_ssize_t encoded_nbytes = utf8proc_encode_char(codepoint, output);
    utf8proc_int32_t codepoint_upper = utf8proc_toupper(codepoint);
    utf8proc_ssize_t encoded_nbytes_upper = utf8proc_encode_char(codepoint_upper, output);
    // validate that upper casing will only lead to a byte length growth of max 3/2
    if (encoded_nbytes == 2) {
      EXPECT_LE(encoded_nbytes_upper, 3)
          << "Expected the upper case codepoint for a 2 byte encoded codepoint to be "
             "encoded in maximum 3 bytes, not "
          << encoded_nbytes_upper;
    }
    utf8proc_int32_t codepoint_lower = utf8proc_tolower(codepoint);
    utf8proc_ssize_t encoded_nbytes_lower = utf8proc_encode_char(codepoint_lower, output);
    // validate that lower casing will only lead to a byte length growth of max 3/2
    if (encoded_nbytes == 2) {
      EXPECT_LE(encoded_nbytes_lower, 3)
          << "Expected the lower case codepoint for a 2 byte encoded codepoint to be "
             "encoded in maximum 3 bytes, not "
          << encoded_nbytes_lower;
    }
  }
}
#endif

}  // namespace compute
}  // namespace arrow

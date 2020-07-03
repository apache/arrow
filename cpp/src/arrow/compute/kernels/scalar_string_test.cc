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

#include <gtest/gtest.h>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

using StringTypes = ::testing::Types<StringType, LargeStringType>;

// interesting utf8 characters for testing (lower case / upper case):
//  * ῦ / Υ͂ (3 to 4 code units) (Note, we don't support this yet, utf8proc does not use
//  SpecialCasing.txt)
//  * ɑ /  Ɑ (2 to 3 code units)
//  * ı / I (2 to 1 code units)
//  * Ⱥ / ⱥ  (2 to 3 code units)

template <typename TestType>
class TestStringKernels : public ::testing::Test {
 protected:
  using OffsetType = typename TypeTraits<TestType>::OffsetType;

  void CheckUnary(std::string func_name, std::string json_input,
                  std::shared_ptr<DataType> out_ty, std::string json_expected,
                  const FunctionOptions* options = nullptr) {
    CheckScalarUnary(func_name, string_type(), json_input, out_ty, json_expected,
                     options);
  }

  std::shared_ptr<DataType> string_type() {
    return TypeTraits<TestType>::type_singleton();
  }

  std::shared_ptr<DataType> offset_type() {
    return TypeTraits<OffsetType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestStringKernels, StringTypes);

TYPED_TEST(TestStringKernels, AsciiLength) {
  this->CheckUnary("ascii_length", R"(["aaa", null, "", "b"])", this->offset_type(),
                   "[3, null, 0, 1]");
}

TYPED_TEST(TestStringKernels, AsciiUpper) {
  this->CheckUnary("ascii_upper", "[]", this->string_type(), "[]");
  this->CheckUnary("ascii_upper", "[\"aAazZæÆ&\", null, \"\", \"bbb\"]",
                   this->string_type(), "[\"AAAZZæÆ&\", null, \"\", \"BBB\"]");
}

TYPED_TEST(TestStringKernels, AsciiLower) {
  this->CheckUnary("ascii_lower", "[]", this->string_type(), "[]");
  this->CheckUnary("ascii_lower", "[\"aAazZæÆ&\", null, \"\", \"BBB\"]",
                   this->string_type(), "[\"aaazzæÆ&\", null, \"\", \"bbb\"]");
}

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

#ifdef ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, Utf8Upper) {
  this->CheckUnary("utf8_upper", "[\"aAazZæÆ&\", null, \"\", \"b\"]", this->string_type(),
                   "[\"AAAZZÆÆ&\", null, \"\", \"B\"]");

  // test varying encoding lenghts and thus changing indices/offsets
  this->CheckUnary("utf8_upper", "[\"ɑɽⱤoW\", null, \"ıI\", \"b\"]", this->string_type(),
                   "[\"ⱭⱤⱤOW\", null, \"II\", \"B\"]");

  // ῦ to Υ͂ not supported
  // this->CheckUnary("utf8_upper", "[\"ῦɐɜʞȿ\"]", this->string_type(),
  // "[\"Υ͂ⱯꞫꞰⱾ\"]");

  // test maximum buffer growth
  this->CheckUnary("utf8_upper", "[\"ɑɑɑɑ\"]", this->string_type(), "[\"ⱭⱭⱭⱭ\"]");

  // Test invalid data
  auto invalid_input =
      ArrayFromJSON(this->string_type(), "[\"ɑa\xFFɑ\", \"ɽ\xe1\xbdɽaa\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8 sequence"),
                                  CallFunction("utf8_upper", {invalid_input}));
}

TYPED_TEST(TestStringKernels, Utf8Lower) {
  this->CheckUnary("utf8_lower", "[\"aAazZæÆ&\", null, \"\", \"b\"]", this->string_type(),
                   "[\"aaazzææ&\", null, \"\", \"b\"]");

  // test varying encoding lenghts and thus changing indices/offsets
  this->CheckUnary("utf8_lower", "[\"ⱭɽⱤoW\", null, \"ıI\", \"B\"]", this->string_type(),
                   "[\"ɑɽɽow\", null, \"ıi\", \"b\"]");

  // ῦ to Υ͂ is not supported, but in principle the reverse is, but it would need
  // normalization
  // this->CheckUnary("utf8_lower", "[\"Υ͂ⱯꞫꞰⱾ\"]", this->string_type(),
  // "[\"ῦɐɜʞȿ\"]");

  // test maximum buffer growth
  this->CheckUnary("utf8_lower", "[\"ȺȺȺȺ\"]", this->string_type(), "[\"ⱥⱥⱥⱥ\"]");

  // Test invalid data
  auto invalid_input =
      ArrayFromJSON(this->string_type(), "[\"Ⱥa\xFFⱭ\", \"Ɽ\xe1\xbdⱤaA\"]");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("Invalid UTF8 sequence"),
                                  CallFunction("utf8_lower", {invalid_input}));
}

#endif  // ARROW_WITH_UTF8PROC

TYPED_TEST(TestStringKernels, BinaryContainsExact) {
  BinaryContainsExactOptions options{"ab"};
  this->CheckUnary("binary_contains_exact", "[]", boolean(), "[]", &options);
  this->CheckUnary("binary_contains_exact", R"(["abc", "acb", "cab", null, "bac"])",
                   boolean(), "[true, false, true, null, false]", &options);

  BinaryContainsExactOptions options_repeated{"abab"};
  this->CheckUnary("binary_contains_exact", R"(["abab", "ab", "cababc", null, "bac"])",
                   boolean(), "[true, false, true, null, false]", &options_repeated);
}

TYPED_TEST(TestStringKernels, Strptime) {
  std::string input1 = R"(["5/1/2020", null, "12/11/1900"])";
  std::string output1 = R"(["2020-05-01", null, "1900-12-11"])";
  StrptimeOptions options("%m/%d/%Y", TimeUnit::MICRO);
  this->CheckUnary("strptime", input1, timestamp(TimeUnit::MICRO), output1, &options);
}

TYPED_TEST(TestStringKernels, StrptimeDoesNotProvideDefaultOptions) {
  auto input =
      ArrayFromJSON(this->string_type(), R"(["2020-05-01", null, "1900-12-11"])");
  ASSERT_RAISES(Invalid, CallFunction("strptime", {input}));
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

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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestStringOps, TestCompare) {
  const char* left = "abcd789";
  const char* right = "abcd123";

  // 0 for equal
  EXPECT_EQ(mem_compare(left, 4, right, 4), 0);

  // compare lengths if the prefixes match
  EXPECT_GT(mem_compare(left, 5, right, 4), 0);
  EXPECT_LT(mem_compare(left, 4, right, 5), 0);

  // compare bytes if the prefixes don't match
  EXPECT_GT(mem_compare(left, 5, right, 5), 0);
  EXPECT_GT(mem_compare(left, 5, right, 7), 0);
  EXPECT_GT(mem_compare(left, 7, right, 5), 0);
}

TEST(TestStringOps, TestBeginsEnds) {
  // starts_with
  EXPECT_TRUE(starts_with_utf8_utf8("hello sir", 9, "hello", 5));
  EXPECT_TRUE(starts_with_utf8_utf8("hellos", 6, "hello", 5));
  EXPECT_TRUE(starts_with_utf8_utf8("hello", 5, "hello", 5));
  EXPECT_FALSE(starts_with_utf8_utf8("hell", 4, "hello", 5));
  EXPECT_FALSE(starts_with_utf8_utf8("world hello", 11, "hello", 5));

  // ends_with
  EXPECT_TRUE(ends_with_utf8_utf8("hello sir", 9, "sir", 3));
  EXPECT_TRUE(ends_with_utf8_utf8("ssir", 4, "sir", 3));
  EXPECT_TRUE(ends_with_utf8_utf8("sir", 3, "sir", 3));
  EXPECT_FALSE(ends_with_utf8_utf8("ir", 2, "sir", 3));
  EXPECT_FALSE(ends_with_utf8_utf8("hello", 5, "sir", 3));
}

TEST(TestStringOps, TestIsSubstr) {
  EXPECT_TRUE(is_substr_utf8_utf8("hello world", 11, "world", 5));
  EXPECT_TRUE(is_substr_utf8_utf8("hello world", 11, "lo wo", 5));
  EXPECT_FALSE(is_substr_utf8_utf8("hello world", 11, "adsed", 5));
  EXPECT_FALSE(is_substr_utf8_utf8("hel", 3, "hello", 5));
  EXPECT_TRUE(is_substr_utf8_utf8("hello", 5, "hello", 5));
  EXPECT_TRUE(is_substr_utf8_utf8("hello world", 11, "", 0));
}

TEST(TestStringOps, TestCharLength) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  EXPECT_EQ(utf8_length(ctx_ptr, "hello sir", 9), 9);

  std::string a("âpple");
  EXPECT_EQ(utf8_length(ctx_ptr, a.data(), static_cast<int>(a.length())), 5);

  std::string b("मदन");
  EXPECT_EQ(utf8_length(ctx_ptr, b.data(), static_cast<int>(b.length())), 3);

  // invalid utf8
  std::string c("\xf8\x28");
  EXPECT_EQ(utf8_length(ctx_ptr, c.data(), static_cast<int>(c.length())), 0);
  EXPECT_TRUE(ctx.get_error().find(
                  "unexpected byte \\f8 encountered while decoding utf8 string") !=
              std::string::npos)
      << ctx.get_error();
  ctx.Reset();

  std::string d("aa\xc3");
  EXPECT_EQ(utf8_length(ctx_ptr, d.data(), static_cast<int>(d.length())), 0);
  EXPECT_TRUE(ctx.get_error().find(
                  "unexpected byte \\c3 encountered while decoding utf8 string") !=
              std::string::npos)
      << ctx.get_error();
  ctx.Reset();

  std::string e(
      "a\xc3"
      "a");
  EXPECT_EQ(utf8_length(ctx_ptr, e.data(), static_cast<int>(e.length())), 0);
  EXPECT_TRUE(ctx.get_error().find(
                  "unexpected byte \\61 encountered while decoding utf8 string") !=
              std::string::npos)
      << ctx.get_error();
  ctx.Reset();

  std::string f(
      "a\xc3\xe3"
      "a");
  EXPECT_EQ(utf8_length(ctx_ptr, f.data(), static_cast<int>(f.length())), 0);
  EXPECT_TRUE(ctx.get_error().find(
                  "unexpected byte \\e3 encountered while decoding utf8 string") !=
              std::string::npos)
      << ctx.get_error();
  ctx.Reset();
}

TEST(TestStringOps, TestCastVarhcar) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "a");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asd");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  // do not truncate if output length is 0
  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "", 0, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†", 9, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†", 9, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†", 9, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†", 9, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†", 9, 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "abc", 3, -1, &out_len);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();

  std::string d("aa\xc3");
  out_str = castVARCHAR_utf8_int64(ctx_ptr, d.data(), static_cast<int>(d.length()), 2,
                                   &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "aa");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567812341234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234123");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 12, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "12345678");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812341234", 16, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "1234567812çåå†123456", 25, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567812çåå†12");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "123456781234çåå†1234", 25, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "12çåå†34567812123456", 25, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "12çåå†3456781212");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†1234567812123456", 25, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "çåå†1234567812123456", 25, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_utf8_int64(ctx_ptr, "123456781234çåå†", 21, 40, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234çåå†");
  EXPECT_FALSE(ctx.has_error());

  std::string f("123456781234çåå\xc3");
  out_str = castVARCHAR_utf8_int64(ctx_ptr, f.data(), static_cast<int32_t>(f.length()),
                                   16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestStringOps, TestSubstring) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, 1, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, 1, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "as");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, 1, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, 0, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, -2, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "df");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "asdf", 4, -5, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "अपाचे एरो", 25, 1, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "अपाचे");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "अपाचे एरो", 25, 7, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "एरो");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "çåå†", 9, 4, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "†");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "çåå†", 9, 2, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "åå");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "çåå†", 9, 0, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çå");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "afg", 4, 0, -5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64_int64(ctx_ptr, "", 0, 5, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64(ctx_ptr, "abcd", 4, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "bcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64(ctx_ptr, "abcd", 4, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = substr_utf8_int64(ctx_ptr, "çåå†", 9, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "åå†");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestSubstringInvalidInputs) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  char bytes[] = {'\xA7', 'a'};
  const char* out_str = substr_utf8_int64_int64(ctx_ptr, bytes, 2, 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  char midbytes[] = {'c', '\xA7', 'a'};
  out_str = substr_utf8_int64_int64(ctx_ptr, midbytes, 3, 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  char midbytes2[] = {'\xC3', 'a', 'a'};
  out_str = substr_utf8_int64_int64(ctx_ptr, midbytes2, 3, 1, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  char endbytes[] = {'a', 'a', '\xA7'};
  out_str = substr_utf8_int64_int64(ctx_ptr, endbytes, 3, 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  char endbytes2[] = {'a', 'a', '\xC3'};
  out_str = substr_utf8_int64_int64(ctx_ptr, endbytes2, 3, 1, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  out_str = substr_utf8_int64_int64(ctx_ptr, "çåå†", 9, 2147483656, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestConcat) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = concatOperator_utf8_utf8(ctx_ptr, "asdf", 4, "jkl", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdfjkl");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8(ctx_ptr, "asdf", 4, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8(ctx_ptr, "", 0, "jkl", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "jkl");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8(ctx_ptr, "", 0, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8(ctx_ptr, "abcd\n", 5, "a", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcd\na");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestLower) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = lower_utf8(ctx_ptr, "AsDfJ", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdfj");
  EXPECT_FALSE(ctx.has_error());

  out_str = lower_utf8(ctx_ptr, "asdfj", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdfj");
  EXPECT_FALSE(ctx.has_error());

  out_str = lower_utf8(ctx_ptr, "Ç††AbD", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Ç††abd");
  EXPECT_FALSE(ctx.has_error());

  out_str = lower_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestReverse) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str;
  out_str = reverse_utf8(ctx_ptr, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "gnirtStseT");
  EXPECT_FALSE(ctx.has_error());

  out_str = reverse_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = reverse_utf8(ctx_ptr, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "†ååç");
  EXPECT_FALSE(ctx.has_error());

  std::string d("aa\xc3");
  out_str = reverse_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestStringOps, TestTrim) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = trim_utf8(ctx_ptr, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = trim_utf8(ctx_ptr, "      TestString  ", 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = trim_utf8(ctx_ptr, " Test  çåå†bD   ", 21, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test  çåå†bD");
  EXPECT_FALSE(ctx.has_error());

  out_str = trim_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = trim_utf8(ctx_ptr, "      ", 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestLocate) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  int pos;

  pos = locate_utf8_utf8(ctx_ptr, "String", 6, "TestString", 10);
  EXPECT_EQ(pos, 5);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "String", 6, "TestString", 10, 1);
  EXPECT_EQ(pos, 5);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "abc", 3, "abcabc", 6, 2);
  EXPECT_EQ(pos, 4);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8(ctx_ptr, "çåå", 6, "s†å†emçåå†d", 21);
  EXPECT_EQ(pos, 7);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "bar", 3, "†barbar", 9, 3);
  EXPECT_EQ(pos, 5);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "sub", 3, "", 0, 1);
  EXPECT_EQ(pos, 0);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "", 0, "str", 3, 1);
  EXPECT_EQ(pos, 0);
  EXPECT_FALSE(ctx.has_error());

  pos = locate_utf8_utf8_int32(ctx_ptr, "bar", 3, "barbar", 6, 0);
  EXPECT_EQ(pos, 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Start position must be greater than 0"));
  ctx.Reset();

  pos = locate_utf8_utf8_int32(ctx_ptr, "bar", 3, "barbar", 6, 7);
  EXPECT_EQ(pos, 0);
  EXPECT_FALSE(ctx.has_error());

  std::string d(
      "a\xff"
      "c");
  pos =
      locate_utf8_utf8_int32(ctx_ptr, "c", 1, d.data(), static_cast<int>(d.length()), 3);
  EXPECT_EQ(pos, 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\ff encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestStringOps, TestReplace) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str;
  out_str = replace_utf8_utf8_utf8(ctx_ptr, "TestString1String2", 18, "String", 6,
                                   "Replace", 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestReplace1Replace2");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      replace_utf8_utf8_utf8(ctx_ptr, "TestString1", 11, "String", 6, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test1");
  EXPECT_FALSE(ctx.has_error());

  out_str = replace_utf8_utf8_utf8(ctx_ptr, "", 0, "test", 4, "rep", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = replace_utf8_utf8_utf8(ctx_ptr, "Ç††çåå†", 17, "†", 3, "t", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Çttçååt");
  EXPECT_FALSE(ctx.has_error());

  out_str = replace_utf8_utf8_utf8(ctx_ptr, "TestString", 10, "", 0, "rep", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      replace_utf8_utf8_utf8(ctx_ptr, "Test", 4, "TestString", 10, "rep", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");
  EXPECT_FALSE(ctx.has_error());

  out_str = replace_utf8_utf8_utf8(ctx_ptr, "Test", 4, "Test", 4, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      replace_utf8_utf8_utf8(ctx_ptr, "TestString", 10, "abc", 3, "xyz", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = replace_with_max_len_utf8_utf8_utf8(ctx_ptr, "Hell", 4, "ell", 3, "ollow", 5,
                                                5, &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer overflow for output string"));
  ctx.Reset();

  out_str = replace_with_max_len_utf8_utf8_utf8(ctx_ptr, "eeee", 4, "e", 1, "aaaa", 4, 14,
                                                &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer overflow for output string"));
  ctx.Reset();
}

TEST(TestArithmeticOps, TestCastINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castINT_utf8(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(castINT_utf8(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(castINT_utf8(ctx_ptr, "2147483647", 10), 2147483647);
  EXPECT_EQ(castINT_utf8(ctx_ptr, "02147483647", 11), 2147483647);
  EXPECT_EQ(castINT_utf8(ctx_ptr, "-2147483648", 11), -2147483648LL);
  EXPECT_EQ(castINT_utf8(ctx_ptr, "-02147483648", 12), -2147483648LL);
  EXPECT_EQ(castINT_utf8(ctx_ptr, " 12 ", 4), 12);

  castINT_utf8(ctx_ptr, "2147483648", 10);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 2147483648 to int32"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "-2147483649", 11);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string -2147483649 to int32"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int32"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int32"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int32"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int32"));
  ctx.Reset();
}

TEST(TestArithmeticOps, TestCastBIGINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "9223372036854775807", 19), 9223372036854775807LL);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "09223372036854775807", 20), 9223372036854775807LL);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "-9223372036854775808", 20),
            -9223372036854775807LL - 1);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, "-009223372036854775808", 22),
            -9223372036854775807LL - 1);
  EXPECT_EQ(castBIGINT_utf8(ctx_ptr, " 12 ", 4), 12);

  castBIGINT_utf8(ctx_ptr, "9223372036854775808", 19);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string 9223372036854775808 to int64"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "-9223372036854775809", 20);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string -9223372036854775809 to int64"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int64"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int64"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int64"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int64"));
  ctx.Reset();
}

TEST(TestArithmeticOps, TestCastFloat4) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "-45.34", 6), -45.34f);
  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "0", 1), 0.0f);
  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "5", 1), 5.0f);
  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, " 3.4 ", 5), 3.4f);

  castFLOAT4_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to float32"));
  ctx.Reset();

  castFLOAT4_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to float32"));
  ctx.Reset();
}

TEST(TestParseStringHolder, TestCastFloat8) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "-45.34", 6), -45.34);
  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "0", 1), 0.0);
  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "5", 1), 5.0);
  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, " 3.4 ", 5), 3.4);

  castFLOAT8_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to float64"));
  ctx.Reset();

  castFLOAT8_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to float64"));
  ctx.Reset();
}

}  // namespace gandiva

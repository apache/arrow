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

TEST(TestStringOps, TestConvertReplaceInvalidUtf8Char) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  // invalid utf8 (xf8 is invalid but x28 is not - x28 = '(')
  std::string a(
      "ok-\xf8\x28"
      "-a");
  auto a_in_out_len = static_cast<int>(a.length());
  const char* a_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, a.data(), a_in_out_len, "a", 1, &a_in_out_len);
  EXPECT_EQ(std::string(a_str, a_in_out_len), "ok-a(-a");
  EXPECT_FALSE(ctx.has_error());

  // invalid utf8 (xa0 and xa1 are invalid)
  std::string b("ok-\xa0\xa1-valid");
  auto b_in_out_len = static_cast<int>(b.length());
  const char* b_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, b.data(), b_in_out_len, "b", 1, &b_in_out_len);
  EXPECT_EQ(std::string(b_str, b_in_out_len), "ok-bb-valid");
  EXPECT_FALSE(ctx.has_error());

  // full valid utf8
  std::string c("all-valid");
  auto c_in_out_len = static_cast<int>(c.length());
  const char* c_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, c.data(), c_in_out_len, "c", 1, &c_in_out_len);
  EXPECT_EQ(std::string(c_str, c_in_out_len), "all-valid");
  EXPECT_FALSE(ctx.has_error());

  // valid utf8 (महसुस is 4-char string, each char of which is likely a multibyte char)
  std::string d("ok-महसुस-valid-new");
  auto d_in_out_len = static_cast<int>(d.length());
  const char* d_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, d.data(), d_in_out_len, "d", 1, &d_in_out_len);
  EXPECT_EQ(std::string(d_str, d_in_out_len), "ok-महसुस-valid-new");
  EXPECT_FALSE(ctx.has_error());

  // full valid utf8, but invalid replacement char length
  std::string e("all-valid");
  auto e_in_out_len = static_cast<int>(e.length());
  const char* e_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, e.data(), e_in_out_len, "ee", 2, &e_in_out_len);
  EXPECT_EQ(std::string(e_str, e_in_out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  // full valid utf8, but invalid replacement char length
  std::string f("ok-\xa0\xa1-valid");
  auto f_in_out_len = static_cast<int>(f.length());
  const char* f_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, f.data(), f_in_out_len, "", 0, &f_in_out_len);
  EXPECT_EQ(std::string(f_str, f_in_out_len), "ok-\xa0\xa1-valid");
  EXPECT_FALSE(ctx.has_error());

  ctx.Reset();
}

TEST(TestStringOps, TestCastBoolToVarchar) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = castVARCHAR_bool_int64(ctx_ptr, true, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "tr");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_bool_int64(ctx_ptr, true, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "true");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_bool_int64(ctx_ptr, false, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "fals");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_bool_int64(ctx_ptr, false, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "false");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_bool_int64(ctx_ptr, true, -3, &out_len);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();
}

TEST(TestStringOps, TestCastVarchar) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  // BINARY TESTS
  const char* out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "a");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asd");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  // do not truncate if output length is 0
  out_str = castVARCHAR_binary_int64(ctx_ptr, "asdf", 4, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdf");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "", 0, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†", 9, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†", 9, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†", 9, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†", 9, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†", 9, 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "abc", 3, -1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();

  std::string z("aa\xc3");
  out_str = castVARCHAR_binary_int64(ctx_ptr, z.data(), static_cast<int>(z.length()), 2,
                                     &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "aa");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567812341234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234123");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 12, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "12345678");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812341234", 16, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "1234567812çåå†123456", 25, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1234567812çåå†12");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "123456781234çåå†1234", 25, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "12çåå†34567812123456", 25, 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "12çåå†3456781212");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†1234567812123456", 25, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå†");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "çåå†1234567812123456", 25, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "çåå");
  EXPECT_FALSE(ctx.has_error());

  out_str = castVARCHAR_binary_int64(ctx_ptr, "123456781234çåå†", 21, 40, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123456781234çåå†");
  EXPECT_FALSE(ctx.has_error());

  std::string f("123456781234çåå\xc3");
  out_str = castVARCHAR_binary_int64(ctx_ptr, f.data(), static_cast<int32_t>(f.length()),
                                     16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();

  // UTF8 TESTS
  out_str = castVARCHAR_utf8_int64(ctx_ptr, "asdf", 4, 1, &out_len);
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
  EXPECT_EQ(std::string(out_str, out_len), "");
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

  std::string y("123456781234çåå\xc3");
  out_str = castVARCHAR_utf8_int64(ctx_ptr, y.data(), static_cast<int32_t>(y.length()),
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

  const char* out_str =
      concat_utf8_utf8(ctx_ptr, "abcd", 4, true, "\npq", 3, false, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8(ctx_ptr, "asdf", 4, "jkl", 3, &out_len);
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

  out_str = concat_utf8_utf8_utf8(ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3,
                                  true, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqard");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      concatOperator_utf8_utf8_utf8(ctx_ptr, "abcd\n", 5, "a", 1, "bcd", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcd\nabcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8(ctx_ptr, "abcd", 4, "a", 1, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcda");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8(ctx_ptr, "", 0, "a", 1, "pqrs", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "apqrs");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8(ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard",
                                       3, true, "uvw", 3, false, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqard");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8(ctx_ptr, "pqrs", 4, "", 0, "\nabc", 4, "y",
                                               1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrs\nabcy");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8(ctx_ptr, "abcd", 4, false, "\npq", 3, true,
                                            "ard", 3, true, "uvw", 3, false, "abc\n", 4,
                                            true, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\n");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8(ctx_ptr, "pqrs", 4, "", 0, "\nabc", 4,
                                                    "y", 1, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrs\nabcy");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3, true, "uvw", 3, false,
      "abc\n", 4, true, "sdfgs", 5, true, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\nsdfgs");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "pqrs", 4, "", 0, "\nabc", 4, "y", 1, "", 0, "\nbcd", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrs\nabcy\nbcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3, true, "uvw", 3, false,
      "abc\n", 4, true, "sdfgs", 5, true, "wfw", 3, false, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\nsdfgs");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "", 0, "pqrs", 4, "abc\n", 4, "y", 1, "", 0, "asdf", 4, "jkl", 3,
      &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrsabc\nyasdfjkl");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3, true, "uvw", 3, false,
      "abc\n", 4, true, "sdfgs", 5, true, "wfw", 3, false, "", 0, true, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\nsdfgs");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "", 0, "pqrs", 4, "abc\n", 4, "y", 1, "", 0, "asdf", 4, "jkl", 3, "", 0,
      &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrsabc\nyasdfjkl");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3, true, "uvw", 3, false,
      "abc\n", 4, true, "sdfgs", 5, true, "wfw", 3, false, "", 0, true, "qwert|n", 7,
      true, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\nsdfgsqwert|n");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "", 0, "pqrs", 4, "abc\n", 4, "y", 1, "", 0, "asdf", 4, "jkl", 3, "", 0,
      "sfl\n", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrsabc\nyasdfjklsfl\n");
  EXPECT_FALSE(ctx.has_error());

  out_str = concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "abcd", 4, false, "\npq", 3, true, "ard", 3, true, "uvw", 3, false,
      "abc\n", 4, true, "sdfgs", 5, true, "wfw", 3, false, "", 0, true, "qwert|n", 7,
      true, "ewfwe", 5, false, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\npqardabc\nsdfgsqwert|n");
  EXPECT_FALSE(ctx.has_error());

  out_str = concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8(
      ctx_ptr, "", 0, "pqrs", 4, "abc\n", 4, "y", 1, "", 0, "asdf", 4, "", 0, "jkl", 3,
      "sfl\n", 4, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "pqrsabc\nyasdfjklsfl\n");
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

TEST(TestStringOps, TestLtrim) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = ltrim_utf8(ctx_ptr, "TestString  ", 12, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString  ");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8(ctx_ptr, "      TestString  ", 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString  ");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8(ctx_ptr, " Test  çåå†bD", 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test  çåå†bD");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8(ctx_ptr, "      ", 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "", 0, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "TestString", 10, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "abcbbaccabbcdef", 15, "abc", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "abcbbaccabbcdef", 15, "ababbac", 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "ååçåå†eç†Dd", 21, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "eç†Dd");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "ç†ååçåå†", 18, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  std::string d(
      "aa\xc3"
      "bcd");
  out_str =
      ltrim_utf8_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), "a", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "\xc3"
            "bcd");
  EXPECT_FALSE(ctx.has_error());

  std::string e(
      "åå\xe0\xa0"
      "bcd");
  out_str =
      ltrim_utf8_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), "å", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "\xE0\xa0"
            "bcd");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "TestString", 10, "abcd", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = ltrim_utf8_utf8(ctx_ptr, "acbabbcabb", 10, "abcbd", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestRtrim) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = rtrim_utf8(ctx_ptr, "  TestString", 12, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "  TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8(ctx_ptr, "  TestString      ", 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "  TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8(ctx_ptr, "Test  çåå†bD   ", 20, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test  çåå†bD");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8(ctx_ptr, "      ", 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "", 0, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "TestString", 10, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "TestString", 10, "ring", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestSt");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "defabcbbaccabbc", 15, "abc", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "defabcbbaccabbc", 15, "ababbac", 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "eDdç†ååçåå†", 21, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "eDd");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "ç†ååçåå†", 18, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  std::string d(
      "\xc3"
      "aaa");
  out_str =
      rtrim_utf8_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), "a", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  std::string e(
      "\xe0\xa0"
      "åå");
  out_str =
      rtrim_utf8_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), "å", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  out_str = rtrim_utf8_utf8(ctx_ptr, "åeçå", 7, "çå", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "åe");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "TestString", 10, "abcd", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = rtrim_utf8_utf8(ctx_ptr, "acbabbcabb", 10, "abcbd", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestBtrim) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = btrim_utf8(ctx_ptr, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8(ctx_ptr, "      TestString  ", 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8(ctx_ptr, " Test  çåå†bD   ", 21, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test  çåå†bD");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8(ctx_ptr, "      ", 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "", 0, "TestString", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "TestString", 10, "Test", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "String");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "TestString", 10, "String", 6, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Tes");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "TestString", 10, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "abcbbadefccabbc", 15, "abc", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "abcbbadefccabbc", 15, "ababbac", 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "ååçåå†Ddeç†", 21, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Dde");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "ç†ååçåå†", 18, "çåå†", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());
  ctx.Reset();

  std::string d(
      "acd\xc3"
      "aaa");
  out_str =
      btrim_utf8_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), "a", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  std::string e(
      "åbc\xe0\xa0"
      "åå");
  out_str =
      btrim_utf8_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), "å", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_TRUE(ctx.has_error());
  ctx.Reset();

  std::string f(
      "aa\xc3"
      "bcd");
  out_str =
      btrim_utf8_utf8(ctx_ptr, f.data(), static_cast<int>(f.length()), "a", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "\xc3"
            "bcd");
  EXPECT_FALSE(ctx.has_error());

  std::string g(
      "åå\xe0\xa0"
      "bcå");
  out_str =
      btrim_utf8_utf8(ctx_ptr, g.data(), static_cast<int>(g.length()), "å", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "\xe0\xa0"
            "bc");

  out_str = btrim_utf8_utf8(ctx_ptr, "åe†çå", 10, "çå", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "e†");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "TestString", 10, "abcd", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = btrim_utf8_utf8(ctx_ptr, "acbabbcabb", 10, "abcbd", 5, &out_len);
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

TEST(TestStringOps, TestBinaryString) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = binary_string(ctx_ptr, "TestString", 10, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_EQ(output, "TestString");

  out_str = binary_string(ctx_ptr, "", 0, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = binary_string(ctx_ptr, "T", 1, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "T");

  out_str = binary_string(ctx_ptr, "\\x41\\x42\\x43", 12, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "ABC");

  out_str = binary_string(ctx_ptr, "\\x41", 4, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "A");

  out_str = binary_string(ctx_ptr, "\\x6d\\x6D", 8, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "mm");

  out_str = binary_string(ctx_ptr, "\\x6f\\x6d", 8, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "om");

  out_str = binary_string(ctx_ptr, "\\x4f\\x4D", 8, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "OM");
}

TEST(TestStringOps, TestSplitPart) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = split_part(ctx_ptr, "A,B,C", 5, ",", 1, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Index in split_part must be positive, value provided was 0"));

  out_str = split_part(ctx_ptr, "A,B,C", 5, ",", 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "A");

  out_str = split_part(ctx_ptr, "A,B,C", 5, ",", 1, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "B");

  out_str = split_part(ctx_ptr, "A,B,C", 5, ",", 1, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "C");

  out_str = split_part(ctx_ptr, "abc~@~def~@~ghi", 15, "~@~", 3, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abc");

  out_str = split_part(ctx_ptr, "abc~@~def~@~ghi", 15, "~@~", 3, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "def");

  out_str = split_part(ctx_ptr, "abc~@~def~@~ghi", 15, "~@~", 3, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ghi");

  // Result must be empty when the index is > no of elements
  out_str = split_part(ctx_ptr, "123|456|789", 11, "|", 1, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = split_part(ctx_ptr, "123|", 4, "|", 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123");

  out_str = split_part(ctx_ptr, "|123", 4, "|", 1, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = split_part(ctx_ptr, "ç†ååçåå†", 18, "å", 2, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ç†");

  out_str = split_part(ctx_ptr, "ç†ååçåå†", 18, "†åå", 6, 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ç");

  out_str = split_part(ctx_ptr, "ç†ååçåå†", 18, "†", 3, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ååçåå");
}

}  // namespace gandiva

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

#include <limits>

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

TEST(TestStringOps, TestAscii) {
  // ASCII
  EXPECT_EQ(ascii_utf8("ABC", 3), 65);
  EXPECT_EQ(ascii_utf8("abc", 3), 97);
  EXPECT_EQ(ascii_utf8("Hello World!", 12), 72);
  EXPECT_EQ(ascii_utf8("This is us", 10), 84);
  EXPECT_EQ(ascii_utf8("", 0), 0);
  EXPECT_EQ(ascii_utf8("123", 3), 49);
  EXPECT_EQ(ascii_utf8("999", 3), 57);
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

TEST(TestStringOps, TestSpace) {
  // Space - returns a string with 'n' spaces
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  int32_t out_len = 0;

  auto out = space_int32(ctx_ptr, 1, &out_len);
  EXPECT_EQ(std::string(out, out_len), " ");
  out = space_int32(ctx_ptr, 10, &out_len);
  EXPECT_EQ(std::string(out, out_len), "          ");
  out = space_int32(ctx_ptr, 5, &out_len);
  EXPECT_EQ(std::string(out, out_len), "     ");
  out = space_int32(ctx_ptr, -5, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
  out = space_int32(ctx_ptr, 65537, &out_len);
  EXPECT_EQ(std::string(out, out_len), std::string(65536, ' '));
  out = space_int32(ctx_ptr, 2147483647, &out_len);
  EXPECT_EQ(std::string(out, out_len), std::string(65536, ' '));

  out = space_int64(ctx_ptr, 2, &out_len);
  EXPECT_EQ(std::string(out, out_len), "  ");
  out = space_int64(ctx_ptr, 9, &out_len);
  EXPECT_EQ(std::string(out, out_len), "         ");
  out = space_int64(ctx_ptr, 4, &out_len);
  EXPECT_EQ(std::string(out, out_len), "    ");
  out = space_int64(ctx_ptr, -5, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
  out = space_int64(ctx_ptr, 65536, &out_len);
  EXPECT_EQ(std::string(out, out_len), std::string(65536, ' '));
  out = space_int64(ctx_ptr, 9223372036854775807, &out_len);
  EXPECT_EQ(std::string(out, out_len), std::string(65536, ' '));
  out = space_int64(ctx_ptr, -2639077559LL, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
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

  // invalid utf8 (xa0 and xa1 are invalid) with empty replacement char length
  std::string f("ok-\xa0\xa1-valid");
  auto f_in_out_len = static_cast<int>(f.length());
  const char* f_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, f.data(), f_in_out_len, "", 0, &f_in_out_len);
  EXPECT_EQ(std::string(f_str, f_in_out_len), "ok--valid");
  EXPECT_FALSE(ctx.has_error());
  ctx.Reset();

  // invalid utf8 (xa0 and xa1 are invalid) with empty replacement char length
  std::string g("\xa0\xa1-ok-\xa0\xa1-valid-\xa0\xa1");
  auto g_in_out_len = static_cast<int>(g.length());
  const char* g_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, g.data(), g_in_out_len, "", 0, &g_in_out_len);
  EXPECT_EQ(std::string(g_str, g_in_out_len), "-ok--valid-");
  EXPECT_FALSE(ctx.has_error());
  ctx.Reset();

  std::string h("\xa0\xa1-valid");
  auto h_in_out_len = static_cast<int>(h.length());
  const char* h_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, h.data(), h_in_out_len, "", 0, &h_in_out_len);
  EXPECT_EQ(std::string(h_str, h_in_out_len), "-valid");
  EXPECT_FALSE(ctx.has_error());
  ctx.Reset();

  std::string i("\xa0\xa1-valid-\xa0\xa1-valid-\xa0\xa1");
  auto i_in_out_len = static_cast<int>(i.length());
  const char* i_str = convert_replace_invalid_fromUTF8_binary(
      ctx_ptr, i.data(), i_in_out_len, "", 0, &i_in_out_len);
  EXPECT_EQ(std::string(i_str, i_in_out_len), "-valid--valid-");
  EXPECT_FALSE(ctx.has_error());
  ctx.Reset();
}

TEST(TestStringOps, TestRepeat) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = repeat_utf8_int32(ctx_ptr, "abc", 3, 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcabc");
  EXPECT_FALSE(ctx.has_error());

  out_str = repeat_utf8_int32(ctx_ptr, "a", 1, 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "aaaaa");
  EXPECT_FALSE(ctx.has_error());

  out_str = repeat_utf8_int32(ctx_ptr, "", 0, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = repeat_utf8_int32(ctx_ptr, "", -20, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = repeat_utf8_int32(ctx_ptr, "a", 1, -10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Repeat number can't be negative"));
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

  castVARCHAR_bool_int64(ctx_ptr, true, -3, &out_len);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();
}

TEST(TestStringOps, TestCastVarcharToBool) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "true", 4), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "     true     ", 14), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "true     ", 9), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "     true", 9), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "TRUE", 4), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "TrUe", 4), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "1", 1), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "  1", 3), true);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "false", 5), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "false     ", 10), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "     false", 10), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "0", 1), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "0   ", 4), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "FALSE", 5), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "FaLsE", 5), false);
  EXPECT_FALSE(ctx.has_error());

  EXPECT_EQ(castBIT_utf8(ctx_ptr, "test", 4), false);
  EXPECT_TRUE(ctx.has_error());
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Invalid value for boolean"));
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

TEST(TestGdvFnStubs, TestCastVarbinaryUtf8) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;
  const char* input = "abc";
  const char* out;

  out = castVARBINARY_utf8_int64(ctx_ptr, input, 3, 0, &out_len);
  EXPECT_EQ(std::string(out, out_len), input);

  out = castVARBINARY_utf8_int64(ctx_ptr, input, 3, 1, &out_len);
  EXPECT_EQ(std::string(out, out_len), "a");

  out = castVARBINARY_utf8_int64(ctx_ptr, input, 3, 500, &out_len);
  EXPECT_EQ(std::string(out, out_len), input);

  out = castVARBINARY_utf8_int64(ctx_ptr, input, 3, -10, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVarbinaryBinary) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;
  const char* input = "\\x41\\x42\\x43";
  const char* out;

  out = castVARBINARY_binary_int64(ctx_ptr, input, 12, 0, &out_len);
  EXPECT_EQ(std::string(out, out_len), input);

  out = castVARBINARY_binary_int64(ctx_ptr, input, 8, 8, &out_len);
  EXPECT_EQ(std::string(out, out_len), "\\x41\\x42");

  out = castVARBINARY_binary_int64(ctx_ptr, input, 12, 500, &out_len);
  EXPECT_EQ(std::string(out, out_len), input);

  out = castVARBINARY_binary_int64(ctx_ptr, input, 12, -10, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Output buffer length can't be negative"));
  ctx.Reset();
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

TEST(TestStringOps, TestQuote) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = quote_utf8(ctx_ptr, "dont", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\'dont\'");
  EXPECT_FALSE(ctx.has_error());

  out_str = quote_utf8(ctx_ptr, "abc", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\'abc\'");
  EXPECT_FALSE(ctx.has_error());

  out_str = quote_utf8(ctx_ptr, "don't", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "\'don\\'t\'");
  EXPECT_FALSE(ctx.has_error());

  out_str = quote_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = quote_utf8(ctx_ptr, "'", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "'\\''");
  EXPECT_FALSE(ctx.has_error());

  out_str = quote_utf8(ctx_ptr, "'''''''''", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "'\\'\\'\\'\\'\\'\\'\\'\\'\\''");
  EXPECT_FALSE(ctx.has_error());
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

TEST(TestStringOps, TestLpadString) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  // LPAD function tests - with defined fill pad text
  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 4, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 10, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 0, 10, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 0, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, -500, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 500, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 18, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "FillFillTestString");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 15, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "FillFTestString");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 20, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "FillFillFiTestString");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "абвгд", 10, 7, "д", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ддабвгд");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "абвгд", 10, 20, "абвгд", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "абвгдабвгдабвгдабвгд");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "hello", 5, 6, "д", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "дhello");

  out_str = lpad_utf8_int32_utf8(ctx_ptr, "大学路", 9, 65536, "哈", 3, &out_len);
  EXPECT_EQ(out_len, 65536 * 3);

  // LPAD function tests - with NO pad text
  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 0, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, -500, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "        TestString");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "     TestString");

  out_str = lpad_utf8_int32(ctx_ptr, "абвгд", 10, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "  абвгд");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, 65537, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), std::string(65526, ' ') + "TestString");

  out_str = lpad_utf8_int32(ctx_ptr, "TestString", 10, -1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
}

TEST(TestStringOps, TestRpadString) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  // RPAD function tests - with defined fill pad text
  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 4, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 10, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 0, 10, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 0, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, -500, "fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 500, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 18, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestStringFillFill");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 15, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestStringFillF");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "TestString", 10, 20, "Fill", 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestStringFillFillFi");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "абвгд", 10, 7, "д", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "абвгддд");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "абвгд", 10, 20, "абвгд", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "абвгдабвгдабвгдабвгд");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "hello", 5, 6, "д", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "helloд");

  out_str = rpad_utf8_int32_utf8(ctx_ptr, "大学路", 9, 655360, "哈雷路", 3, &out_len);
  EXPECT_EQ(out_len, 65536 * 3);

  // RPAD function tests - with NO pad text
  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 0, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, -500, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 18, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString        ");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString     ");

  out_str = rpad_utf8_int32(ctx_ptr, "абвгд", 10, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "абвгд  ");

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, 65537, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString" + std::string(65526, ' '));

  out_str = rpad_utf8_int32(ctx_ptr, "TestString", 10, -1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
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

TEST(TestStringOps, TestByteSubstr) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str;
  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 5, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "String");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, -6, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "String");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 0, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 0, -500, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 1, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 1, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 1, 1000, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 5, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Str");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, 5, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "String");
  EXPECT_FALSE(ctx.has_error());

  out_str = byte_substr_binary_int32_int32(ctx_ptr, "TestString", 10, -100, 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "TestString");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestStringOps, TestStrPos) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);

  int pos;

  pos = strpos_utf8_utf8(ctx_ptr, "TestString", 10, "String", 6);
  EXPECT_EQ(pos, 5);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "TestString", 10, "String", 6);
  EXPECT_EQ(pos, 5);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "abcabc", 6, "abc", 3);
  EXPECT_EQ(pos, 1);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "s†å†emçåå†d", 21, "çåå", 6);
  EXPECT_EQ(pos, 7);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "†barbar", 9, "bar", 3);
  EXPECT_EQ(pos, 2);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "", 0, "sub", 3);
  EXPECT_EQ(pos, 0);
  EXPECT_FALSE(ctx.has_error());

  pos = strpos_utf8_utf8(ctx_ptr, "str", 3, "", 0);
  EXPECT_EQ(pos, 0);
  EXPECT_FALSE(ctx.has_error());

  std::string d(
      "a\xff"
      "c");
  pos = strpos_utf8_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), "c", 1);
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

  replace_with_max_len_utf8_utf8_utf8(ctx_ptr, "Hell", 4, "ell", 3, "ollow", 5, 5,
                                      &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer overflow for output string"));
  ctx.Reset();

  replace_with_max_len_utf8_utf8_utf8(ctx_ptr, "eeee", 4, "e", 1, "aaaa", 4, 14,
                                      &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer overflow for output string"));
  ctx.Reset();
}

TEST(TestStringOps, TestLeftString) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = left_utf8_int32(ctx_ptr, "TestString", 10, 10, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_EQ(output, "TestString");

  out_str = left_utf8_int32(ctx_ptr, "", 0, 0, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = left_utf8_int32(ctx_ptr, "", 0, 500, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = left_utf8_int32(ctx_ptr, "TestString", 10, 3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "Tes");

  out_str = left_utf8_int32(ctx_ptr, "TestString", 10, -3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "TestStr");

  // the text length for this string is 10 (each utf8 char is represented by two bytes)
  out_str = left_utf8_int32(ctx_ptr, "абвгд", 10, 3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "абв");
}

TEST(TestStringOps, TestRightString) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = right_utf8_int32(ctx_ptr, "TestString", 10, 10, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_EQ(output, "TestString");

  out_str = right_utf8_int32(ctx_ptr, "", 0, 0, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = right_utf8_int32(ctx_ptr, "", 0, 500, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = right_utf8_int32(ctx_ptr, "TestString", 10, 3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "ing");

  out_str = right_utf8_int32(ctx_ptr, "TestString", 10, -3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "tString");

  // the text length for this string is 10 (each utf8 char is represented by two bytes)
  out_str = right_utf8_int32(ctx_ptr, "абвгд", 10, 3, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "вгд");
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

TEST(TestStringOps, TestConvertTo) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  const int32_t ALL_BYTES_MATCH = 0;

  int32_t integer_value = std::numeric_limits<int32_t>::max();
  out_str = convert_toINT(ctx_ptr, integer_value, &out_len);
  EXPECT_EQ(out_len, sizeof(integer_value));
  EXPECT_EQ(ALL_BYTES_MATCH, memcmp(out_str, &integer_value, out_len));

  int64_t big_integer_value = std::numeric_limits<int64_t>::max();
  out_str = convert_toBIGINT(ctx_ptr, big_integer_value, &out_len);
  EXPECT_EQ(out_len, sizeof(big_integer_value));
  EXPECT_EQ(ALL_BYTES_MATCH, memcmp(out_str, &big_integer_value, out_len));

  float float_value = std::numeric_limits<float>::max();
  out_str = convert_toFLOAT(ctx_ptr, float_value, &out_len);
  EXPECT_EQ(out_len, sizeof(float_value));
  EXPECT_EQ(ALL_BYTES_MATCH, memcmp(out_str, &float_value, out_len));

  double double_value = std::numeric_limits<double>::max();
  out_str = convert_toDOUBLE(ctx_ptr, double_value, &out_len);
  EXPECT_EQ(out_len, sizeof(double_value));
  EXPECT_EQ(ALL_BYTES_MATCH, memcmp(out_str, &double_value, out_len));

  const char* test_string = "test string";
  int32_t str_len = 11;
  out_str = convert_toUTF8(ctx_ptr, test_string, str_len, &out_len);
  EXPECT_EQ(out_len, str_len);
  EXPECT_EQ(ALL_BYTES_MATCH, memcmp(out_str, test_string, out_len));
}

TEST(TestStringOps, TestConvertToBigEndian) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  gdv_int32 out_len_big_endian = 0;
  const char* out_str;
  const char* out_str_big_endian;

  int64_t big_integer_value = std::numeric_limits<int64_t>::max();
  out_str = convert_toBIGINT(ctx_ptr, big_integer_value, &out_len);
  out_str_big_endian =
      convert_toBIGINT_be(ctx_ptr, big_integer_value, &out_len_big_endian);
  EXPECT_EQ(out_len_big_endian, sizeof(big_integer_value));
  EXPECT_EQ(out_len_big_endian, out_len);

#if ARROW_LITTLE_ENDIAN
  // Checks that bytes are in reverse order
  for (auto i = 0; i < out_len; i++) {
    EXPECT_EQ(out_str[i], out_str_big_endian[out_len - (i + 1)]);
  }
#else
  for (auto i = 0; i < out_len; i++) {
    EXPECT_EQ(out_str[i], out_str_big_endian[i]);
  }
#endif

  double double_value = std::numeric_limits<double>::max();
  out_str = convert_toDOUBLE(ctx_ptr, double_value, &out_len);
  out_str_big_endian = convert_toDOUBLE_be(ctx_ptr, double_value, &out_len_big_endian);
  EXPECT_EQ(out_len_big_endian, sizeof(double_value));
  EXPECT_EQ(out_len_big_endian, out_len);

#if ARROW_LITTLE_ENDIAN
  // Checks that bytes are in reverse order
  for (auto i = 0; i < out_len; i++) {
    EXPECT_EQ(out_str[i], out_str_big_endian[out_len - (i + 1)]);
  }
#else
  for (auto i = 0; i < out_len; i++) {
    EXPECT_EQ(out_str[i], out_str_big_endian[i]);
  }
#endif
}

TEST(TestStringOps, TestConcatWs) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  const char* separator = "-";
  auto sep_len = static_cast<int32_t>(strlen(separator));
  int32_t out_len;
  const char* word1 = "hey";
  int32_t word1_len = static_cast<int32_t>(strlen(word1));
  const char* word2 = "hello";
  int32_t word2_len = static_cast<int32_t>(strlen(word2));

  const char* out = concat_ws_utf8_utf8(ctx_ptr, separator, sep_len, word1, word1_len,
                                        word2, word2_len, &out_len);
  EXPECT_EQ(std::string(out, out_len), "hey-hello");

  separator = "#";
  sep_len = static_cast<int32_t>(strlen(separator));
  const char* word3 = "wow";
  int32_t word3_len = static_cast<int32_t>(strlen(word3));

  out = concat_ws_utf8_utf8_utf8(ctx_ptr, separator, sep_len, word1, word1_len, word2,
                                 word2_len, word3, word3_len, &out_len);
  EXPECT_EQ(std::string(out, out_len), "hey#hello#wow");

  separator = "=";
  sep_len = static_cast<int32_t>(strlen(separator));
  const char* word4 = "awesome";
  int32_t word4_len = static_cast<int32_t>(strlen(word4));

  out = concat_ws_utf8_utf8_utf8_utf8(ctx_ptr, separator, sep_len, word1, word1_len,
                                      word2, word2_len, word3, word3_len, word4,
                                      word4_len, &out_len);
  EXPECT_EQ(std::string(out, out_len), "hey=hello=wow=awesome");

  separator = "&&";
  sep_len = static_cast<int32_t>(strlen(separator));
  const char* word5 = "super";
  int32_t word5_len = static_cast<int32_t>(strlen(word5));

  out = concat_ws_utf8_utf8_utf8_utf8_utf8(ctx_ptr, separator, sep_len, word1, word1_len,
                                           word2, word2_len, word3, word3_len, word4,
                                           word4_len, word5, word5_len, &out_len);
  EXPECT_EQ(std::string(out, out_len), "hey&&hello&&wow&&awesome&&super");

  out = concat_ws_utf8_utf8(ctx_ptr, "", 0, "", 0, "", 0, &out_len);
  EXPECT_EQ(std::string(out, out_len), "");
}

TEST(TestStringOps, TestEltFunction) {
  //  gandiva::ExecutionContext ctx;
  //  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  gdv_int32 out_len = 0;
  bool out_vality = false;

  const char* word1 = "john";
  auto word1_len = static_cast<int32_t>(strlen(word1));
  const char* word2 = "";
  auto word2_len = static_cast<int32_t>(strlen(word2));
  auto out_string = elt_int32_utf8_utf8(1, true, word1, word1_len, true, word2, word2_len,
                                        true, &out_vality, &out_len);
  EXPECT_EQ("john", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, true);

  word1 = "hello";
  word1_len = static_cast<int32_t>(strlen(word1));
  word2 = "world";
  word2_len = static_cast<int32_t>(strlen(word2));
  out_string = elt_int32_utf8_utf8(2, true, word1, word1_len, true, word2, word2_len,
                                   true, &out_vality, &out_len);
  EXPECT_EQ("world", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, true);

  word1 = "goodbye";
  word1_len = static_cast<int32_t>(strlen(word1));
  word2 = "world";
  word2_len = static_cast<int32_t>(strlen(word2));
  out_string = elt_int32_utf8_utf8(4, true, word1, word1_len, true, word2, word2_len,
                                   true, &out_vality, &out_len);
  EXPECT_EQ("", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, false);

  word1 = "hi";
  word1_len = static_cast<int32_t>(strlen(word1));
  word2 = "yeah";
  word2_len = static_cast<int32_t>(strlen(word2));
  out_string = elt_int32_utf8_utf8(0, true, word1, word1_len, true, word2, word2_len,
                                   true, &out_vality, &out_len);
  EXPECT_EQ("", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, false);

  const char* word3 = "wow";
  auto word3_len = static_cast<int32_t>(strlen(word3));
  out_string =
      elt_int32_utf8_utf8_utf8(3, true, word1, word1_len, true, word2, word2_len, true,
                               word3, word3_len, true, &out_vality, &out_len);
  EXPECT_EQ("wow", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, true);

  const char* word4 = "awesome";
  auto word4_len = static_cast<int32_t>(strlen(word4));
  out_string = elt_int32_utf8_utf8_utf8_utf8(
      4, true, word1, word1_len, true, word2, word2_len, true, word3, word3_len, true,
      word4, word4_len, true, &out_vality, &out_len);
  EXPECT_EQ("awesome", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, true);

  const char* word5 = "not-empty";
  auto word5_len = static_cast<int32_t>(strlen(word5));
  out_string = elt_int32_utf8_utf8_utf8_utf8_utf8(
      5, true, word1, word1_len, true, word2, word2_len, true, word3, word3_len, true,
      word4, word4_len, true, word5, word5_len, true, &out_vality, &out_len);
  EXPECT_EQ("not-empty", std::string(out_string, out_len));
  EXPECT_EQ(out_vality, true);
}

TEST(TestStringOps, TestToHex) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;
  int32_t in_len = 0;
  const char* out_str;

  in_len = 10;
  char in_str[] = {0x54, 0x65, 0x73, 0x74, 0x53, 0x74, 0x72, 0x69, 0x6E, 0x67};
  out_str = to_hex_binary(ctx_ptr, in_str, in_len, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "54657374537472696E67");

  in_len = 0;
  out_str = to_hex_binary(ctx_ptr, "", in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 0);
  EXPECT_EQ(output, "");

  in_len = 1;
  char in_str_one_char[] = {0x54};
  out_str = to_hex_binary(ctx_ptr, in_str_one_char, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "54");

  in_len = 16;
  char in_str_spaces[] = {0x54, 0x65, 0x73, 0x74, 0x20, 0x77, 0x69, 0x74,
                          0x68, 0x20, 0x73, 0x70, 0x61, 0x63, 0x65, 0x73};
  out_str = to_hex_binary(ctx_ptr, in_str_spaces, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "54657374207769746820737061636573");

  in_len = 20;
  char in_str_break_line[] = {0x54, 0x65, 0x78, 0x74, 0x20, 0x77, 0x69, 0x74, 0x68, 0x0A,
                              0x62, 0x72, 0x65, 0x61, 0x6B, 0x20, 0x6C, 0x69, 0x6E, 0x65};
  out_str = to_hex_binary(ctx_ptr, in_str_break_line, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "5465787420776974680A627265616B206C696E65");

  in_len = 27;
  char in_str_with_num[] = {0x54, 0x65, 0x73, 0x74, 0x20, 0x77, 0x69, 0x74, 0x68,
                            0x20, 0x6E, 0x75, 0x6D, 0x62, 0x65, 0x72, 0x73, 0x20,
                            0x31, 0x20, 0x2B, 0x20, 0x31, 0x20, 0x3D, 0x20, 0x32};
  out_str = to_hex_binary(ctx_ptr, in_str_with_num, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "546573742077697468206E756D626572732031202B2031203D2032");

  in_len = 22;
  char in_str_with_tabs[] = {0x09, 0x0A, 0x09, 0x0A, 0x09, 0x0A, 0x09, 0x0A,
                             0x0A, 0x0A, 0x09, 0x20, 0x61, 0x20, 0x6C, 0x65,
                             0x74, 0x74, 0x40, 0x5D, 0x65, 0x72};
  out_str = to_hex_binary(ctx_ptr, in_str_with_tabs, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "090A090A090A090A0A0A092061206C657474405D6572");

  in_len = 22;
  const char* binary_string =
      "\x09\x0A\x09\x0A\x09\x0A\x09\x0A\x0A\x0A\x09\x20\x61\x20\x6C\x65\x74\x74\x40\x5D"
      "\x65\x72";
  out_str = to_hex_binary(ctx_ptr, binary_string, in_len, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(out_len, 2 * in_len);
  EXPECT_EQ(output, "090A090A090A090A0A0A092061206C657474405D6572");
}

TEST(TestStringOps, TestToHexInt64) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;
  const char* out_str;

  int64_t max_data = INT64_MAX;
  out_str = to_hex_int64(ctx_ptr, max_data, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 16);
  EXPECT_EQ(output, "7FFFFFFFFFFFFFFF");
  ctx.Reset();

  int64_t min_data = INT64_MIN;
  out_str = to_hex_int64(ctx_ptr, min_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 16);
  EXPECT_EQ(output, "8000000000000000");
  ctx.Reset();

  int64_t zero_data = 0;
  out_str = to_hex_int64(ctx_ptr, zero_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "0");
  ctx.Reset();

  int64_t minus_zero_data = -0;
  out_str = to_hex_int64(ctx_ptr, minus_zero_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "0");
  ctx.Reset();

  int64_t minus_one_data = -1;
  out_str = to_hex_int64(ctx_ptr, minus_one_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 16);
  EXPECT_EQ(output, "FFFFFFFFFFFFFFFF");
  ctx.Reset();

  int64_t one_data = 1;
  out_str = to_hex_int64(ctx_ptr, one_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "1");
  ctx.Reset();
}

TEST(TestStringOps, TestToHexInt32) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;
  const char* out_str;

  int32_t max_data = INT32_MAX;
  out_str = to_hex_int32(ctx_ptr, max_data, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 8);
  EXPECT_EQ(output, "7FFFFFFF");
  ctx.Reset();

  int32_t min_data = INT32_MIN;
  out_str = to_hex_int32(ctx_ptr, min_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 8);
  EXPECT_EQ(output, "80000000");
  ctx.Reset();

  int32_t zero_data = 0;
  out_str = to_hex_int32(ctx_ptr, zero_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "0");
  ctx.Reset();

  int32_t minus_zero_data = -0;
  out_str = to_hex_int32(ctx_ptr, minus_zero_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "0");
  ctx.Reset();

  int32_t minus_one_data = -1;
  out_str = to_hex_int32(ctx_ptr, minus_one_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 8);
  EXPECT_EQ(output, "FFFFFFFF");
  ctx.Reset();

  int32_t one_data = 1;
  out_str = to_hex_int32(ctx_ptr, one_data, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_FALSE(ctx.has_error());
  EXPECT_EQ(out_len, 1);
  EXPECT_EQ(output, "1");
  ctx.Reset();
}

TEST(TestStringOps, TestFromHex) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;
  const char* out_str;

  out_str = from_hex_utf8(ctx_ptr, "414243", 6, &out_len);
  std::string output = std::string(out_str, out_len);
  EXPECT_EQ(output, "ABC");

  out_str = from_hex_utf8(ctx_ptr, "", 0, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");

  out_str = from_hex_utf8(ctx_ptr, "41", 2, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "A");

  out_str = from_hex_utf8(ctx_ptr, "6d6D", 4, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "mm");

  out_str = from_hex_utf8(ctx_ptr, "6f6d", 4, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "om");

  out_str = from_hex_utf8(ctx_ptr, "4f4D", 4, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "OM");

  out_str = from_hex_utf8(ctx_ptr, "T", 1, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Error parsing hex string, length was not a multiple of"));
  ctx.Reset();

  out_str = from_hex_utf8(ctx_ptr, "\\x41\\x42\\x43", 12, &out_len);
  output = std::string(out_str, out_len);
  EXPECT_EQ(output, "");
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Error parsing hex string, one or more bytes are not valid."));
  ctx.Reset();
}
}  // namespace gandiva

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

#include "gandiva/gdv_function_stubs.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "gandiva/execution_context.h"

namespace gandiva {

TEST(TestGdvFnStubs, TestCastVarbinaryNumeric) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  // tests for integer values as input
  const char* out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, -46, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-46");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, 2147483647, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2147483647");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, -2147483647 - 1, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-2147483648");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, 0, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, 34567, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "345");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int32_int64(ctx_ptr, 347, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  gdv_fn_castVARBINARY_int32_int64(ctx_ptr, 347, -1, &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer length can not be negative"));
  ctx.Reset();

  // tests for big integer values as input
  out_str =
      gdv_fn_castVARBINARY_int64_int64(ctx_ptr, 9223372036854775807LL, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "9223372036854775807");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int64_int64(ctx_ptr, -9223372036854775807LL - 1, 100,
                                             &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-9223372036854775808");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARBINARY_int64_int64(ctx_ptr, 0, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARBINARY_int64_int64(ctx_ptr, 12345, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestGdvFnStubs, TestBase64Encode) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  auto value = gdv_fn_base64_encode_binary(ctx_ptr, "hello", 5, &out_len);
  std::string out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "aGVsbG8=");

  value = gdv_fn_base64_encode_binary(ctx_ptr, "test", 4, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "dGVzdA==");

  value = gdv_fn_base64_encode_binary(ctx_ptr, "hive", 4, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "aGl2ZQ==");

  value = gdv_fn_base64_encode_binary(ctx_ptr, "", 0, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "");

  value = gdv_fn_base64_encode_binary(ctx_ptr, "test", -5, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "");
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer length can not be negative"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestBase64Decode) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  auto value = gdv_fn_base64_decode_utf8(ctx_ptr, "aGVsbG8=", 8, &out_len);
  std::string out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "hello");

  value = gdv_fn_base64_decode_utf8(ctx_ptr, "dGVzdA==", 8, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "test");

  value = gdv_fn_base64_decode_utf8(ctx_ptr, "aGl2ZQ==", 8, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "hive");

  value = gdv_fn_base64_decode_utf8(ctx_ptr, "", 0, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "");

  value = gdv_fn_base64_decode_utf8(ctx_ptr, "test", -5, &out_len);
  out_value = std::string(value, out_len);
  EXPECT_EQ(out_value, "");
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer length can not be negative"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "2147483647", 10), 2147483647);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "02147483647", 11), 2147483647);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "-2147483648", 11), -2147483648LL);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "-02147483648", 12), -2147483648LL);
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, " 12 ", 4), 12);

  gdv_fn_castINT_utf8(ctx_ptr, "2147483648", 10);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 2147483648 to int32"));
  ctx.Reset();

  gdv_fn_castINT_utf8(ctx_ptr, "-2147483649", 11);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string -2147483649 to int32"));
  ctx.Reset();

  gdv_fn_castINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int32"));
  ctx.Reset();

  gdv_fn_castINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int32"));
  ctx.Reset();

  gdv_fn_castINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int32"));
  ctx.Reset();

  gdv_fn_castINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int32"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastBIGINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "9223372036854775807", 19),
            9223372036854775807LL);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "09223372036854775807", 20),
            9223372036854775807LL);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "-9223372036854775808", 20),
            -9223372036854775807LL - 1);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, "-009223372036854775808", 22),
            -9223372036854775807LL - 1);
  EXPECT_EQ(gdv_fn_castBIGINT_utf8(ctx_ptr, " 12 ", 4), 12);

  gdv_fn_castBIGINT_utf8(ctx_ptr, "9223372036854775808", 19);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string 9223372036854775808 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_utf8(ctx_ptr, "-9223372036854775809", 20);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string -9223372036854775809 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int64"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastFloat4) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castFLOAT4_utf8(ctx_ptr, "-45.34", 6), -45.34f);
  EXPECT_EQ(gdv_fn_castFLOAT4_utf8(ctx_ptr, "0", 1), 0.0f);
  EXPECT_EQ(gdv_fn_castFLOAT4_utf8(ctx_ptr, "5", 1), 5.0f);
  EXPECT_EQ(gdv_fn_castFLOAT4_utf8(ctx_ptr, " 3.4 ", 5), 3.4f);

  gdv_fn_castFLOAT4_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to float"));
  ctx.Reset();

  gdv_fn_castFLOAT4_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to float"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastFloat8) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castFLOAT8_utf8(ctx_ptr, "-45.34", 6), -45.34);
  EXPECT_EQ(gdv_fn_castFLOAT8_utf8(ctx_ptr, "0", 1), 0.0);
  EXPECT_EQ(gdv_fn_castFLOAT8_utf8(ctx_ptr, "5", 1), 5.0);
  EXPECT_EQ(gdv_fn_castFLOAT8_utf8(ctx_ptr, " 3.4 ", 5), 3.4);

  gdv_fn_castFLOAT8_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to double"));
  ctx.Reset();

  gdv_fn_castFLOAT8_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to double"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVARCHARFromInt32) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  const char* out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, -46, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-46");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, 2147483647, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2147483647");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, -2147483647 - 1, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-2147483648");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, 0, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, 34567, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "345");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, 347, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int32_int64(ctx_ptr, 347, -1, &out_len);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Buffer length can not be negative"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVARCHARFromInt64) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  const char* out_str =
      gdv_fn_castVARCHAR_int64_int64(ctx_ptr, 9223372036854775807LL, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "9223372036854775807");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      gdv_fn_castVARCHAR_int64_int64(ctx_ptr, -9223372036854775807LL - 1, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-9223372036854775808");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_int64_int64(ctx_ptr, 0, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARCHAR_int64_int64(ctx_ptr, 12345, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "123");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestGdvFnStubs, TestCastVARCHARFromFloat) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  const char* out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 4.567f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "4.567");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, -3.4567f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-3.4567");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 0.00001f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1.0E-5");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 0.00099999f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "9.9999E-4");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 0.0f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0.0");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 10.00000f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "10.0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 1.2345f, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1.2");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestGdvFnStubs, TestCastVARCHARFromDouble) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  const char* out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, 4.567, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "4.567");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, -3.4567, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "-3.4567");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, 0.00001, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1.0E-5");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float32_int64(ctx_ptr, 0.00099999f, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "9.9999E-4");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, 0.0, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0.0");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, 10.0000000000, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "10.0");
  EXPECT_FALSE(ctx.has_error());

  // test with required length less than actual buffer length
  out_str = gdv_fn_castVARCHAR_float64_int64(ctx_ptr, 1.2345, 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1.2");
  EXPECT_FALSE(ctx.has_error());
}

TEST(TestGdvFnStubs, TestUpper) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = gdv_fn_upper_utf8(ctx_ptr, "AbcDEfGh", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ABCDEFGH");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "asdfj", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ASDFJ");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "s;dcGS,jO!l", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "S;DCGS,JO!L");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "münchen", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "MÜNCHEN");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "CITROËN", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "CITROËN");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "âBćDëFGH", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ÂBĆDËFGH");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "øhpqRšvñ", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ØHPQRŠVÑ");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "Möbelträgerfüße", 19, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "MÖBELTRÄGERFÜẞE");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "{õhp,PQŚv}ń+", 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "{ÕHP,PQŚV}Ń+");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_upper_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  std::string d("AbOJjÜoß\xc3");
  out_str = gdv_fn_upper_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();

  std::string e(
      "åbÑg\xe0\xa0"
      "åBUå");
  out_str = gdv_fn_upper_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\e0 encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestLower) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = gdv_fn_lower_utf8(ctx_ptr, "AbcDEfGh", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "abcdefgh");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "asdfj", 5, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "asdfj");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "S;DCgs,Jo!L", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "s;dcgs,jo!l");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "MÜNCHEN", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "münchen");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "citroën", 8, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "citroën");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "ÂbĆDËFgh", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "âbćdëfgh");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "ØHPQrŠvÑ", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "øhpqršvñ");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "MÖBELTRÄGERFÜẞE", 20, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "möbelträgerfüße");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "{ÕHP,pqśv}Ń+", 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "{õhp,pqśv}ń+");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_lower_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  std::string d("AbOJjÜoß\xc3");
  out_str = gdv_fn_lower_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();

  std::string e(
      "åbÑg\xe0\xa0"
      "åBUå");
  out_str = gdv_fn_lower_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\e0 encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestInitCap) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = gdv_fn_initcap_utf8(ctx_ptr, "test string", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Test String");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "asdfj\nhlqf", 10, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Asdfj\nHlqf");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "s;DCgs,Jo!l", 11, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "S;Dcgs,Jo!L");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, " mÜNCHEN", 9, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), " München");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "citroën CaR", 12, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Citroën Car");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "ÂbĆDËFgh\néll", 16, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Âbćdëfgh\nÉll");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "  øhpqršvñ  \n\n", 17, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "  Øhpqršvñ  \n\n");
  EXPECT_FALSE(ctx.has_error());

  out_str =
      gdv_fn_initcap_utf8(ctx_ptr, "möbelträgerfüße   \nmöbelträgerfüße", 42, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Möbelträgerfüße   \nMöbelträgerfüße");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "{ÕHP,pqśv}Ń+", 15, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "{Õhp,Pqśv}Ń+");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "sɦasasdsɦsd\"sdsdɦ", 19, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Sɦasasdsɦsd\"Sdsdɦ");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "mysuperscipt@number²isfine", 27, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Mysuperscipt@Number²Isfine");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "Ő<tŵas̓老ƕɱ¢vIYwށ", 25, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Ő<Tŵas̓老Ƕɱ¢Viywށ");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "ↆcheckↆnumberisspace", 24, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ↆcheckↆnumberisspace");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "testing ᾌTitleᾌcase", 23, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "Testing ᾌtitleᾄcase");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "ʳTesting mʳodified", 20, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "ʳTesting MʳOdified");
  EXPECT_FALSE(ctx.has_error());

  out_str = gdv_fn_initcap_utf8(ctx_ptr, "", 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_FALSE(ctx.has_error());

  std::string d("AbOJjÜoß\xc3");
  out_str =
      gdv_fn_initcap_utf8(ctx_ptr, d.data(), static_cast<int>(d.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\c3 encountered while decoding utf8 string"));
  ctx.Reset();

  std::string e(
      "åbÑg\xe0\xa0"
      "åBUå");
  out_str =
      gdv_fn_initcap_utf8(ctx_ptr, e.data(), static_cast<int>(e.length()), &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "");
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr(
                  "unexpected byte \\e0 encountered while decoding utf8 string"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestFromUnixtimeWithoutPattern) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);
  gdv_int32 out_len;
  // "2010-06-01 06:58:22" in seconds from epoch are:
  gdv_timestamp ts = 1275375502;
  const char* out = gdv_fn_from_unixtime_int64(context_ptr, ts, &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-06-01 06:58:22");

  out = gdv_fn_from_unixtime_int64(context_ptr, 0L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "1970-01-01 00:00:00");

  out = gdv_fn_from_unixtime_int64(context_ptr, -1L, &out_len);
  EXPECT_EQ(std::string(out, out_len), "1969-12-31 23:59:59");
}

TEST(TestGdvFnStubs, TestFromUnixtimeWithPattern) {
  ExecutionContext context;
  int64_t context_ptr = reinterpret_cast<int64_t>(&context);
  gdv_int32 out_len = 0;

  // "2010-06-01 06:58:22" in seconds from epoch are:
  gdv_timestamp ts_from_string =  1275375502;
  const char* pattern1 = "yyyy-MM-dd hh:mm:ss";
  int32_t pattern1_len = strlen(pattern1);
  const char* out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern1,
                                             pattern1_len, &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-06-01 06:58:22");

  const char* pattern2 = "yyyy-MM-dd hh:mm";
  int32_t pattern2_len = strlen(pattern2);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern2, pattern2_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-06-01 06:58");

  const char* pattern3 = "yyyy-MM-dd hh";
  int32_t pattern3_len = strlen(pattern3);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern3, pattern3_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-06-01 06");

  const char* pattern4 = "YYYY-MM-dd";
  int32_t pattern4_len = strlen(pattern4);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern4, pattern4_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-06-01");

  const char* pattern5 = "YYYY-dd-MM";
  int32_t pattern5_len = strlen(pattern5);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern5, pattern5_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "2010-01-06");

  const char* pattern6 = "dd-M-yy hh:mm";
  int32_t pattern6_len = strlen(pattern6);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern6, pattern6_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "01-Jun-10 06:58");

  const char* pattern7 = "Mm-dd-YY hh:mm:ss";
  int32_t pattern7_len = strlen(pattern7);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern7, pattern7_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "June-01-10 06:58:22");

  const char* pattern8 = "hh:mm:ss dd-M-yyyy";
  int32_t pattern8_len = strlen(pattern8);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, pattern8, pattern8_len,
                                 &out_len);
  EXPECT_EQ(std::string(out, out_len), "06:58:22 01-Jun-2010");

  const char* invalid_pattern1 = "yyyy-MM-dd hh:mm:ss.ss";
  int32_t invalid_pattern1_len = strlen(invalid_pattern1);
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, invalid_pattern1,
                                 invalid_pattern1_len, &out_len);
  std::string out_str = std::string(out, out_len);
  EXPECT_EQ(out_str, "");
  EXPECT_EQ(context.get_error(), "Pattern has invalid delimiter '.'");
  context.Reset();

  const char* invalid_pattern3 = "";
  int32_t invalid_pattern3_len = -1;
  out = gdv_fn_from_unixtime_int64_utf8(context_ptr, ts_from_string, invalid_pattern3,
                                 invalid_pattern3_len, &out_len);
  out_str = std::string(out, out_len);
  EXPECT_EQ(out_str, "");
  EXPECT_EQ(context.get_error(), "Invalid pattern, it must have at least 1 char");
  context.Reset();
}
}  // namespace gandiva

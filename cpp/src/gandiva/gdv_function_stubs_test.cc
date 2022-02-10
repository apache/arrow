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

#include <gandiva/precompiled/testing.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <openssl/crypto.h>

#include "arrow/util/logging.h"
#include "gandiva/execution_context.h"

namespace gandiva {

TEST(TestGdvFnStubs, TestCrc32) {
  gandiva::ExecutionContext ctx;
  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "ABC", 3), 2743272264);
  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "Hello", 5), 4157704578);
  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "hive", 4), 3698179064);
  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "372189372123", 12), 2607335846);
  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "", 0), 0);

  EXPECT_EQ(gdv_fn_crc_32_utf8(ctx_ptr, "-5", -5), 0);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Input length can't be negative"));
  ctx.Reset();
}

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
  EXPECT_EQ(gdv_fn_castINT_utf8(ctx_ptr, "12", 2), 12);

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

TEST(TestGdvFnStubs, TestCastVARCHARFromMilliseconds) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  gdv_date64 ts = StringToTimestamp("2021-04-23 10:20:33");
  const char* out_str = gdv_fn_castVARCHAR_date64_int64(ctx_ptr, ts, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2021-04-23");
  EXPECT_FALSE(ctx.has_error());

  ts = StringToTimestamp("2008-08-20 10:20:33");
  out_str = gdv_fn_castVARCHAR_date64_int64(ctx_ptr, ts, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2008-08-20");
  EXPECT_FALSE(ctx.has_error());

  ts = StringToTimestamp("2011-09-28 10:20:33");
  out_str = gdv_fn_castVARCHAR_date64_int64(ctx_ptr, ts, 100, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2011-09-28");
  EXPECT_FALSE(ctx.has_error());

  ts = StringToTimestamp("2021-04-21 10:20:33");
  out_str = gdv_fn_castVARCHAR_date64_int64(ctx_ptr, ts, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2021-04");
  EXPECT_FALSE(ctx.has_error());

  ts = StringToTimestamp("2008-04-21 10:20:33");
  out_str = gdv_fn_castVARCHAR_date64_int64(ctx_ptr, ts, 4, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "2008");
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

TEST(TestGdvFnStubs, TestCastVarbinaryINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "2147483647", 10), 2147483647);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "\x32\x33", 2), 23);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "02147483647", 11), 2147483647);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "-2147483648", 11), -2147483648LL);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, "-02147483648", 12), -2147483648LL);
  EXPECT_EQ(gdv_fn_castINT_varbinary(ctx_ptr, " 12 ", 4), 12);

  gdv_fn_castINT_varbinary(ctx_ptr, "2147483648", 10);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 2147483648 to int32"));
  ctx.Reset();

  gdv_fn_castINT_varbinary(ctx_ptr, "-2147483649", 11);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string -2147483649 to int32"));
  ctx.Reset();

  gdv_fn_castINT_varbinary(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int32"));
  ctx.Reset();

  gdv_fn_castINT_varbinary(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int32"));
  ctx.Reset();

  gdv_fn_castINT_varbinary(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int32"));
  ctx.Reset();

  gdv_fn_castINT_varbinary(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int32"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVarbinaryBIGINT) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "-45", 3), -45);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "0", 1), 0);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "9223372036854775807", 19),
            9223372036854775807LL);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "09223372036854775807", 20),
            9223372036854775807LL);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "-9223372036854775808", 20),
            -9223372036854775807LL - 1);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, "-009223372036854775808", 22),
            -9223372036854775807LL - 1);
  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr, " 12 ", 4), 12);

  EXPECT_EQ(gdv_fn_castBIGINT_varbinary(ctx_ptr,
                                        "\x39\x39\x39\x39\x39\x39\x39\x39\x39\x39", 10),
            9999999999LL);

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "9223372036854775808", 19);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string 9223372036854775808 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "-9223372036854775809", 20);
  EXPECT_THAT(
      ctx.get_error(),
      ::testing::HasSubstr("Failed to cast the string -9223372036854775809 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string 12.34 to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string abc to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to int64"));
  ctx.Reset();

  gdv_fn_castBIGINT_varbinary(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string - to int64"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVarbinaryFloat4) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castFLOAT4_varbinary(ctx_ptr, "-45.34", 6), -45.34f);
  EXPECT_EQ(gdv_fn_castFLOAT4_varbinary(ctx_ptr, "0", 1), 0.0f);
  EXPECT_EQ(gdv_fn_castFLOAT4_varbinary(ctx_ptr, "5", 1), 5.0f);
  EXPECT_EQ(gdv_fn_castFLOAT4_varbinary(ctx_ptr, " 3.4 ", 5), 3.4f);
  EXPECT_EQ(gdv_fn_castFLOAT4_varbinary(ctx_ptr, " \x33\x2E\x34 ", 5), 3.4f);

  gdv_fn_castFLOAT4_varbinary(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to float"));
  ctx.Reset();

  gdv_fn_castFLOAT4_varbinary(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to float"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestCastVarbinaryFloat8) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(gdv_fn_castFLOAT8_varbinary(ctx_ptr, "-45.34", 6), -45.34);
  EXPECT_EQ(gdv_fn_castFLOAT8_varbinary(ctx_ptr, "0", 1), 0.0);
  EXPECT_EQ(gdv_fn_castFLOAT8_varbinary(ctx_ptr, "5", 1), 5.0);
  EXPECT_EQ(gdv_fn_castFLOAT8_varbinary(ctx_ptr, " \x33\x2E\x34 ", 5), 3.4);

  gdv_fn_castFLOAT8_varbinary(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string  to double"));
  ctx.Reset();

  gdv_fn_castFLOAT8_varbinary(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed to cast the string e to double"));
  ctx.Reset();
}

TEST(TestGdvFnStubs, TestMaskFirstN) {
  gandiva::ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  std::string data = "a〜Çç&";
  auto data_len = static_cast<int32_t>(data.length());
  std::string expected = "x〜Xx&";
  const char* result =
      gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 4, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "世界您";
  data_len = static_cast<int32_t>(data.length());
  expected = "世界您";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 4, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "a6Ççé";
  data_len = static_cast<int32_t>(data.length());
  expected = "xnXxé";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 4, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "0123456789";
  data_len = static_cast<int32_t>(data.length());
  expected = "nnnnnnnnnn";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 10, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  data_len = static_cast<int32_t>(data.length());
  expected = "XXXXXXXXXXXXXXXXXXXXXXXXXX";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 26, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "abcdefghijklmnopqrstuvwxyz";
  expected = "xxxxxxxxxxxxxxxxxxxxxxxxxx";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 26, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "aB-6";
  data_len = static_cast<int32_t>(data.length());
  expected = "xX-6";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 3, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  expected = "xX-n";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 5, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  expected = "aB-6";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, -3, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 0, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "ABcd-123456";
  data_len = static_cast<int32_t>(data.length());
  expected = "XXxx-n23456";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 6, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "";
  data_len = 0;
  expected = "";
  result = gdv_mask_first_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 6, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));
}

TEST(TestGdvFnStubs, TestMaskLastN) {
  gandiva::ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  std::string data = "a〜Çç&";
  int32_t data_len = static_cast<int32_t>(data.length());
  std::string expected = "a〜Xx&";
  const char* result =
      gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 4, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "abÇçé";
  data_len = static_cast<int32_t>(data.length());
  expected = "axXxx";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 4, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "0123456789";
  data_len = static_cast<int32_t>(data.length());
  expected = "nnnnnnnnnn";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 10, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  data_len = static_cast<int32_t>(data.length());
  expected = "XXXXXXXXXXXXXXXXXXXXXXXXXX";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 26, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "abcdefghijklmnopqrstuvwxyz";
  expected = "xxxxxxxxxxxxxxxxxxxxxxxxxx";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 26, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "aB-6";
  data_len = static_cast<int32_t>(data.length());
  expected = "aX-n";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 3, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  expected = "xX-n";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 5, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  expected = "aB-6";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, -3, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 0, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "ABcd-123456";
  data_len = static_cast<int32_t>(data.length());
  expected = "ABcd-nnnnnn";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 6, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));

  data = "";
  data_len = 0;
  expected = "";
  result = gdv_mask_last_n_utf8_int32(ctx_ptr, data.c_str(), data_len, 6, &out_len);
  EXPECT_EQ(expected, std::string(result, out_len));
}

TEST(TestGdvFnStubs, TestMaskHash) {
  gandiva::ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);
  int32_t out_len = 0;

  if (FIPS_mode())
  {
    std::string data = "a〜Çç&";
    int32_t data_len = static_cast<int32_t>(data.length());
    std::string expected = "32d13016ddb8522a07eb24062775a3a6b8aea80bd3c54c1747f646a010a2d1f0bad021a24d881b55a7d795185b7e630f4d38c38fbeaf1897e725f884ca206c38";
    const char* result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    data_len = static_cast<int32_t>(data.length());
    expected = "f9292a765b5826c3e5786d9cf361e677f58ec5e3b5cecfd7a8bf122f5407b157196753f062d109ac7c16b29b0f471f81da9787c8d314e873413edca956027799";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    data_len = static_cast<int32_t>(data.length());
    expected = "f9292a765b5826c3e5786d9cf361e677f58ec5e3b5cecfd7a8bf122f5407b157196753f062d109ac7c16b29b0f471f81da9787c8d314e873413edca956027799";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "TestString-123";
    data_len = static_cast<int32_t>(data.length());
    expected = "f3a58111be6ecec11449ac44654e72376b7759883ea11723b6e51354d50436de645bd061cb5c2b07b68e15b7a7c342cac41f69b9c4efe19e810bbd7abf639a1c";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abÇçé";
    data_len = static_cast<int32_t>(data.length());
    expected = "0a84490bf1466dcae1c7ef234c903ca848ae9cf0fdd6498895c945bbea4af4ddd596c1e007bd7fdc7a0aa142da5e4276ec3070b946247e9c8c4f376e04f2847f";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len,  &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "0123456789";
    data_len = static_cast<int32_t>(data.length());
    expected = "bb96c2fc40d2d54617d6f276febe571f623a8dadf0b734855299b0e107fda32cf6b69f2da32b36445d73690b93cbd0f7bfc20e0f7f28553d2a4428f23b716e90";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len,  &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abcdefghijklmnopqrstuvwxyz";
    data_len = static_cast<int32_t>(data.length());
    expected = "4dbff86cc2ca1bae1e16468a05cb9881c97f1753bce3619034898faa1aabe429955a1bf8ec483d7421fe3c1646613a59ed5441fb0f321389f77f48a879c7b1f1";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "aB-6";
    data_len = static_cast<int32_t>(data.length());
    expected = "7f678df40c7c2cb16afddddf71eee6a7b85d1e24375f8f67730ea941aabf0b0d3b60d0c92efdac15b1f23dc2715ed7639e2080e9eb699ec6be32c8f8fa71b5af";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "b大路学路b";
    data_len = static_cast<int32_t>(data.length());
    expected = "388afb7f8b9d4ef3782e2ebc4e6b09b9b60b6c177484a1c6ccc0a5125e4a88fbe5b58fcb88bcacbc9017be8651b373c57d24554052c53de76d6bf7e05253d296";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "                          ";
    data_len = static_cast<int32_t>(data.length());
    expected = "5b5e139bae6046cac40738f75ecdcb2358e33e0cb488caf9b381a4b4656c23ad32c2b222d6ed0f146213aa33bed4cda080feee9c23402cbdff963121f5e244e1";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "";
    data_len = static_cast<int32_t>(data.length());
    expected = "";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    data_len = static_cast<int32_t>(data.length());
    expected = "204a8fc6dda82f0a0ced7beb8e08a41657c16ef468b228a8279be331a703c33596fd15c13b1b07f9aa1d3bea57789ca031ad85c7a71dd70354ec631238ca3445";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));
  }
  else {
    std::string data = "a〜Çç&";
    int32_t data_len = static_cast<int32_t>(data.length());
    std::string expected = "76d98bfb4c6b9b178324b7159bab0effbcc0665a503a193328255b741c26bca0";
    const char* result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    data_len = static_cast<int32_t>(data.length());
    expected = "d6ec6898de87ddac6e5b3611708a7aa1c2d298293349cc1a6c299a1db7149d38";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "TestString-123";
    data_len = static_cast<int32_t>(data.length());
    expected = "8b44d559dc5d60e4453c9b4edf2a455fbce054bb8504cd3eb9b5f391bd239c90";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abÇçé";
    data_len = static_cast<int32_t>(data.length());
    expected = "03af6bec2d006863245b5dcd9a9a03d0297cd0c80db0e9ea310305fc53e87e46";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len,  &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "0123456789";
    data_len = static_cast<int32_t>(data.length());
    expected = "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len,  &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abcdefghijklmnopqrstuvwxyz";
    data_len = static_cast<int32_t>(data.length());
    expected = "71c480df93d6ae2f1efad1447c66c9525e316218cf51fc8d9ed832f2daf18b73";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "aB-6";
    data_len = static_cast<int32_t>(data.length());
    expected = "9d7cd121662f9647db394950cee3bb7be7e703550a7433037bf302adcd0ad8bd";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "b大路学路b";
    data_len = static_cast<int32_t>(data.length());
    expected = "0874ca19c86a7de7b411457927e3a36ef478da8dd9d321197a42b3f701c08152";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "                          ";
    data_len = static_cast<int32_t>(data.length());
    expected = "99871ea61492b0fb650c49fe1dd2be7d81c8074542514671d2c585f4a3f247f1";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "";
    data_len = static_cast<int32_t>(data.length());
    expected = "";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));

    data = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    data_len = static_cast<int32_t>(data.length());
    expected = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
    result = gdv_mask_hash_utf8_utf8(ctx_ptr, data.c_str(), data_len, &out_len);
    EXPECT_EQ(expected, std::string(result, out_len));
  }

}

}  // namespace gandiva

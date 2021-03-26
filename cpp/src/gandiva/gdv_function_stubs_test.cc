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

}  // namespace gandiva

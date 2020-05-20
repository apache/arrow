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
#include "../execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestArithmeticOps, TestIsDistinctFrom) {
  EXPECT_EQ(is_distinct_from_timestamp_timestamp(1000, true, 1000, false), true);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp(1000, false, 1000, true), true);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp(1000, false, 1000, false), false);
  EXPECT_EQ(is_distinct_from_timestamp_timestamp(1000, true, 1000, true), false);

  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, true, 1000, false), false);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, false, 1000, true), false);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, false, 1000, false), true);
  EXPECT_EQ(is_not_distinct_from_int32_int32(1000, true, 1000, true), true);
}

TEST(TestArithmeticOps, TestMod) {
  gandiva::ExecutionContext context;
  EXPECT_EQ(mod_int64_int32(10, 0), 10);

  const double acceptable_abs_error = 0.00000000001;  // 1e-10

  EXPECT_DOUBLE_EQ(mod_float64_float64(reinterpret_cast<gdv_int64>(&context), 2.5, 0.0),
                   0.0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "divide by zero error");

  context.Reset();
  EXPECT_NEAR(mod_float64_float64(reinterpret_cast<gdv_int64>(&context), 2.5, 1.2), 0.1,
              acceptable_abs_error);
  EXPECT_FALSE(context.has_error());

  context.Reset();
  EXPECT_DOUBLE_EQ(mod_float64_float64(reinterpret_cast<gdv_int64>(&context), 2.5, 2.5),
                   0.0);
  EXPECT_FALSE(context.has_error());

  context.Reset();
  EXPECT_NEAR(mod_float64_float64(reinterpret_cast<gdv_int64>(&context), 9.2, 3.7), 1.8,
              acceptable_abs_error);
  EXPECT_FALSE(context.has_error());
}

TEST(TestArithmeticOps, TestDivide) {
  gandiva::ExecutionContext context;
  EXPECT_EQ(divide_int64_int64(reinterpret_cast<gdv_int64>(&context), 10, 0), 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");

  context.Reset();
  EXPECT_EQ(divide_int64_int64(reinterpret_cast<gdv_int64>(&context), 10, 2), 5);
  EXPECT_EQ(context.has_error(), false);
}

TEST(TestArithmeticOps, TestDiv) {
  gandiva::ExecutionContext context;
  EXPECT_EQ(div_int64_int64(reinterpret_cast<gdv_int64>(&context), 101, 0), 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");
  context.Reset();

  EXPECT_EQ(div_int64_int64(reinterpret_cast<gdv_int64>(&context), 101, 111), 0);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();

  EXPECT_EQ(div_float64_float64(reinterpret_cast<gdv_int64>(&context), 1010.1010, 2.1),
            481.0);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();

  EXPECT_EQ(
      div_float64_float64(reinterpret_cast<gdv_int64>(&context), 1010.1010, 0.00000),
      0.0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");
  context.Reset();

  EXPECT_EQ(div_float32_float32(reinterpret_cast<gdv_int64>(&context), 1010.1010f, 2.1f),
            481.0f);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();
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

  castINT_utf8(ctx_ptr, "2147483648", 10);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "-2147483649", 11);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
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

  castBIGINT_utf8(ctx_ptr, "9223372036854775808", 19);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "-9223372036854775809", 20);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "12.34", 5);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "abc", 3);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castBIGINT_utf8(ctx_ptr, "-", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();
}

TEST(TestArithmeticOps, TestCastFloat4) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "-45.34", 6), -45.34f);
  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "0", 1), 0.0f);
  EXPECT_EQ(castFLOAT4_utf8(ctx_ptr, "5", 1), 5.0f);

  castFLOAT4_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castFLOAT4_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();
}

TEST(TestParseStringHolder, TestCastFloat8) {
  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "-45.34", 6), -45.34);
  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "0", 1), 0.0);
  EXPECT_EQ(castFLOAT8_utf8(ctx_ptr, "5", 1), 5.0);

  castFLOAT8_utf8(ctx_ptr, "", 0);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();

  castFLOAT8_utf8(ctx_ptr, "e", 1);
  EXPECT_THAT(ctx.get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx.Reset();
}

}  // namespace gandiva

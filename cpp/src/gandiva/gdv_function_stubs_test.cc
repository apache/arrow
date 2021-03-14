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

}  // namespace gandiva

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

TEST(TestArithmeticOps, TestPmod) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<gdv_int64>(&context);
  EXPECT_EQ(pmod_int64_int64(ctx, 3, 4), 3);
  EXPECT_EQ(pmod_int64_int64(ctx, 4, 3), 1);
  EXPECT_EQ(pmod_int64_int64(ctx, -3, 4), 1);
  EXPECT_EQ(pmod_int64_int64(ctx, -4, 3), 2);
  EXPECT_EQ(pmod_int64_int64(ctx, 3, -4), -1);
  EXPECT_EQ(pmod_int64_int64(ctx, 4, -3), -2);

  EXPECT_EQ(pmod_int64_int64(ctx, 3, 0), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "divide by zero error");
  context.Reset();
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

TEST(TestArithmeticOps, TestGreatestLeast) {
  // Comparable functions - Greatest and Least
  EXPECT_EQ(greatest_int32_int32(1, 2), 2);
  EXPECT_EQ(greatest_int32_int32(1, INT32_MAX), INT32_MAX);
  EXPECT_EQ(greatest_int32_int32(2, 1), 2);
  EXPECT_EQ(greatest_int64_int64(1, 2), 2);
  EXPECT_EQ(greatest_int64_int64(1, INT64_MAX), INT64_MAX);
  EXPECT_EQ(greatest_int64_int64(2, 1), 2);
  EXPECT_EQ(greatest_float32_float32(1.0f, 2.0f), 2.0f);
  EXPECT_EQ(greatest_float32_float32(1.0f, INFINITY), INFINITY);
  EXPECT_EQ(greatest_float32_float32(1.0f, FLT_MAX), FLT_MAX);
  EXPECT_EQ(greatest_float32_float32(2.0f, 1.0f), 2.0f);
  EXPECT_EQ(greatest_float64_float64(1.0, 2.0), 2.0);
  EXPECT_EQ(greatest_float64_float64(1.0, DBL_MAX), DBL_MAX);
  EXPECT_EQ(greatest_float64_float64(2.0, 1.0), 2.0);
  EXPECT_EQ(least_int32_int32(1, 2), 1);
  EXPECT_EQ(least_int32_int32(INT32_MIN, 2), INT32_MIN);
  EXPECT_EQ(least_int32_int32(2, 1), 1);
  EXPECT_EQ(least_int64_int64(1, 2), 1);
  EXPECT_EQ(least_int64_int64(INT64_MIN, 2), INT64_MIN);
  EXPECT_EQ(least_int64_int64(2, 1), 1);
  EXPECT_EQ(least_float32_float32(1.0f, 2.0f), 1.0f);
  EXPECT_EQ(least_float32_float32(FLT_MIN, 2.0f), FLT_MIN);
  EXPECT_EQ(least_float32_float32(2.0f, 1.0f), 1.0f);
  EXPECT_EQ(least_float64_float64(1.0, 2.0), 1.0);
  EXPECT_EQ(least_float64_float64(DBL_MIN, 2.0), DBL_MIN);
  EXPECT_EQ(least_float64_float64(2.0, 1.0), 1.0);

  EXPECT_EQ(greatest_int32_int32_int32(1, 2, 3), 3);
  EXPECT_EQ(greatest_int32_int32_int32(3, 2, 1), 3);
  EXPECT_EQ(greatest_int64_int64_int64(1, 2, 3), 3);
  EXPECT_EQ(greatest_int64_int64_int64(3, 2, 1), 3);
  EXPECT_EQ(greatest_float32_float32_float32(1.0f, 2.0f, 3.0f), 3.0f);
  EXPECT_EQ(greatest_float32_float32_float32(3.0f, 2.0f, 1.0f), 3.0f);
  EXPECT_EQ(greatest_float64_float64_float64(1.0, 2.0, 3.0), 3.0);
  EXPECT_EQ(greatest_float64_float64_float64(3.0, 2.0, 1.0), 3.0);
  EXPECT_EQ(least_int32_int32_int32(1, 2, 3), 1);
  EXPECT_EQ(least_int32_int32_int32(2, 1, 3), 1);
  EXPECT_EQ(least_int64_int64_int64(1, 2, 3), 1);
  EXPECT_EQ(least_int64_int64_int64(2, 1, 3), 1);
  EXPECT_EQ(least_float32_float32_float32(1.0f, 2.0f, 3.0f), 1.0f);
  EXPECT_EQ(least_float32_float32_float32(3.0f, 2.0f, 1.0f), 1.0f);
  EXPECT_EQ(least_float64_float64_float64(1.0, 2.0, 2.0), 1.0);
  EXPECT_EQ(least_float64_float64_float64(3.0, 2.0, 1.0), 1.0);

  EXPECT_EQ(greatest_int32_int32_int32_int32(1, 2, 3, 4), 4);
  EXPECT_EQ(greatest_int32_int32_int32_int32(2, 4, 3, 1), 4);
  EXPECT_EQ(greatest_int64_int64_int64_int64(1, 2, 3, 4), 4);
  EXPECT_EQ(greatest_int64_int64_int64_int64(2, 4, 3, 1), 4);
  EXPECT_EQ(greatest_float32_float32_float32_float32(1.0f, 2.0f, 3.0f, 4.0f), 4.0f);
  EXPECT_EQ(greatest_float32_float32_float32_float32(2.0f, 4.0f, 3, 1.0f), 4.0f);
  EXPECT_EQ(greatest_float64_float64_float64_float64(1.0, 2.0, 3.0, 4.0), 4.0);
  EXPECT_EQ(greatest_float64_float64_float64_float64(2.0, 4.0, 3.0, 1.0), 4.0);
  EXPECT_EQ(least_int32_int32_int32_int32(1, 2, 3, 4), 1);
  EXPECT_EQ(least_int32_int32_int32_int32(2, 4, 3, 1), 1);
  EXPECT_EQ(least_int64_int64_int64_int64(1, 2, 3, 4), 1);
  EXPECT_EQ(least_int64_int64_int64_int64(2, 4, 3, 1), 1);
  EXPECT_EQ(least_float32_float32_float32_float32(1.0f, 2.0f, 3.0f, 4.0f), 1.0f);
  EXPECT_EQ(least_float32_float32_float32_float32(2.0f, 4.0f, 3, 1.0f), 1.0f);
  EXPECT_EQ(least_float64_float64_float64_float64(1.0, 2.0, 3.0, 4.0), 1.0);
  EXPECT_EQ(least_float64_float64_float64_float64(2.0, 4.0, 3.0, 1.0), 1.0);

  EXPECT_EQ(greatest_int32_int32_int32_int32_int32(1, 2, 3, 5, 4), 5);
  EXPECT_EQ(greatest_int32_int32_int32_int32_int32(2, 4, 5, 3, 1), 5);
  EXPECT_EQ(greatest_int64_int64_int64_int64_int64(1, 2, 3, 5, 4), 5);
  EXPECT_EQ(greatest_int64_int64_int64_int64_int64(2, 4, 5, 3, 1), 5);
  EXPECT_EQ(
      greatest_float32_float32_float32_float32_float32(1.0f, 2.0f, 3.0f, 5.0f, 4.0f),
      5.0f);
  EXPECT_EQ(
      greatest_float32_float32_float32_float32_float32(2.0f, 4.0f, 5.0f, 3.0f, 1.0f),
      5.0f);
  EXPECT_EQ(greatest_float64_float64_float64_float64_float64(1.0, 2.0, 3.0, 5.0, 4.0),
            5.0);
  EXPECT_EQ(greatest_float64_float64_float64_float64_float64(2.0, 4.0, 5.0, 3.0, 1.0),
            5.0);
  EXPECT_EQ(least_int32_int32_int32_int32_int32(1, 2, 3, 4, -10), -10);
  EXPECT_EQ(least_int32_int32_int32_int32_int32(-10, 4, 2, 1, 3), -10);
  EXPECT_EQ(least_int64_int64_int64_int64_int64(1, 2, 3, 4, -10), -10);
  EXPECT_EQ(least_int64_int64_int64_int64_int64(-10, 4, 2, 1, 3), -10);
  EXPECT_EQ(least_float32_float32_float32_float32_float32(1.0f, 2.0f, 3.0f, -10.0f, 4.0f),
            -10.0f);
  EXPECT_EQ(least_float32_float32_float32_float32_float32(2.0f, 4.0f, -10.0f, 3.0f, 1.0f),
            -10.0f);
  EXPECT_EQ(least_float64_float64_float64_float64_float64(1.0, 2.0, 3.0, -10.0, 4.0),
            -10.0);
  EXPECT_EQ(least_float64_float64_float64_float64_float64(-10.0, 4.0, 5.0, 3.0, 1.0),
            -10.0);

  EXPECT_EQ(greatest_int32_int32_int32_int32_int32_int32(7, 1, 2, 3, 5, 4), 7);
  EXPECT_EQ(greatest_int32_int32_int32_int32_int32_int32(2, 4, 7, 5, 3, 1), 7);
  EXPECT_EQ(greatest_int64_int64_int64_int64_int64_int64(7, 1, 2, 3, 5, 4), 7);
  EXPECT_EQ(greatest_int64_int64_int64_int64_int64_int64(2, 4, 7, 5, 3, 1), 7);
  EXPECT_EQ(greatest_float32_float32_float32_float32_float32_float32(7.0f, 1.0f, 2.0f,
                                                                     3.0f, 5.0f, 4.0f),
            7.0f);
  EXPECT_EQ(greatest_float32_float32_float32_float32_float32_float32(2.0f, 4.0f, 7.0f,
                                                                     5.0f, 3.0f, 1.0f),
            7.0f);
  EXPECT_EQ(greatest_float64_float64_float64_float64_float64_float64(7.0, 1.0, 2.0, 3.0,
                                                                     5.0, 4.0),
            7.0);
  EXPECT_EQ(greatest_float64_float64_float64_float64_float64_float64(2.0, 4.0, 7.0, 5.0,
                                                                     3.0, 1.0),
            7.0);

  EXPECT_EQ(least_int32_int32_int32_int32_int32_int32(1, 2, 3, -99, 4, -10), -99);
  EXPECT_EQ(least_int32_int32_int32_int32_int32_int32(-10, 4, 2, 1, -99, 3), -99);
  EXPECT_EQ(least_int64_int64_int64_int64_int64_int64(1, 2, 3, -99, 4, -10), -99);
  EXPECT_EQ(least_int64_int64_int64_int64_int64_int64(-10, 4, 2, 1, -99, 3), -99);
  EXPECT_EQ(least_float32_float32_float32_float32_float32_float32(1.0f, 2.0f, 3.0f,
                                                                  -99.0f, 4.0f, -10.0f),
            -99.0f);
  EXPECT_EQ(least_float32_float32_float32_float32_float32_float32(-10.0f, 4.0f, 2.0f,
                                                                  1.0f, -99.0f, 3.0f),
            -99.0f);
  EXPECT_EQ(least_float64_float64_float64_float64_float64_float64(1.0, 2.0, 3.0, -99.0,
                                                                  4.0, -10.0),
            -99.0);
  EXPECT_EQ(least_float64_float64_float64_float64_float64_float64(-10.0, 4.0, 2.0, 1.0,
                                                                  -99.0, 3.0),
            -99.0);
}

TEST(TestArithmeticOps, TestBitwiseOps) {
  // bitwise AND
  EXPECT_EQ(bitwise_and_int32_int32(0x0147D, 0x17159), 0x01059);
  EXPECT_EQ(bitwise_and_int32_int32(0xFFFFFFCC, 0x00000297), 0x00000284);
  EXPECT_EQ(bitwise_and_int32_int32(0x000, 0x285), 0x000);
  EXPECT_EQ(bitwise_and_int64_int64(0x563672F83, 0x0D9FCF85B), 0x041642803);
  EXPECT_EQ(bitwise_and_int64_int64(0xFFFFFFFFFFDA8F6A, 0xFFFFFFFFFFFF791C),
            0xFFFFFFFFFFDA0908);
  EXPECT_EQ(bitwise_and_int64_int64(0x6A5B1, 0x00000), 0x00000);

  // bitwise OR
  EXPECT_EQ(bitwise_or_int32_int32(0x0147D, 0x17159), 0x1757D);
  EXPECT_EQ(bitwise_or_int32_int32(0xFFFFFFCC, 0x00000297), 0xFFFFFFDF);
  EXPECT_EQ(bitwise_or_int32_int32(0x000, 0x285), 0x285);
  EXPECT_EQ(bitwise_or_int64_int64(0x563672F83, 0x0D9FCF85B), 0x5FBFFFFDB);
  EXPECT_EQ(bitwise_or_int64_int64(0xFFFFFFFFFFDA8F6A, 0xFFFFFFFFFFFF791C),
            0xFFFFFFFFFFFFFF7E);
  EXPECT_EQ(bitwise_or_int64_int64(0x6A5B1, 0x00000), 0x6A5B1);

  // bitwise XOR
  EXPECT_EQ(bitwise_xor_int32_int32(0x0147D, 0x17159), 0x16524);
  EXPECT_EQ(bitwise_xor_int32_int32(0xFFFFFFCC, 0x00000297), 0XFFFFFD5B);
  EXPECT_EQ(bitwise_xor_int32_int32(0x000, 0x285), 0x285);
  EXPECT_EQ(bitwise_xor_int64_int64(0x563672F83, 0x0D9FCF85B), 0x5BA9BD7D8);
  EXPECT_EQ(bitwise_xor_int64_int64(0xFFFFFFFFFFDA8F6A, 0xFFFFFFFFFFFF791C), 0X25F676);
  EXPECT_EQ(bitwise_xor_int64_int64(0x6A5B1, 0x00000), 0x6A5B1);
  EXPECT_EQ(bitwise_xor_int64_int64(0x6A5B1, 0x6A5B1), 0x00000);

  // bitwise NOT
  EXPECT_EQ(bitwise_not_int32(0x00017159), 0xFFFE8EA6);
  EXPECT_EQ(bitwise_not_int32(0xFFFFF226), 0x00000DD9);
  EXPECT_EQ(bitwise_not_int64(0x000000008BCAE9B4), 0xFFFFFFFF7435164B);
  EXPECT_EQ(bitwise_not_int64(0xFFFFFF966C8D7997), 0x0000006993728668);
  EXPECT_EQ(bitwise_not_int64(0x0000000000000000), 0xFFFFFFFFFFFFFFFF);
}

TEST(TestArithmeticOps, TestIntCastFloatDouble) {
  // castINT from floats
  EXPECT_EQ(castINT_float32(6.6f), 7);
  EXPECT_EQ(castINT_float32(-6.6f), -7);
  EXPECT_EQ(castINT_float32(-6.3f), -6);
  EXPECT_EQ(castINT_float32(0.0f), 0);
  EXPECT_EQ(castINT_float32(-0), 0);

  // castINT from doubles
  EXPECT_EQ(castINT_float64(6.6), 7);
  EXPECT_EQ(castINT_float64(-6.6), -7);
  EXPECT_EQ(castINT_float64(-6.3), -6);
  EXPECT_EQ(castINT_float64(0.0), 0);
  EXPECT_EQ(castINT_float64(-0), 0);
  EXPECT_EQ(castINT_float64(999999.99999999999999999999999), 1000000);
  EXPECT_EQ(castINT_float64(-999999.99999999999999999999999), -1000000);
  EXPECT_EQ(castINT_float64(INT32_MAX), 2147483647);
  EXPECT_EQ(castINT_float64(-2147483647), -2147483647);
}

TEST(TestArithmeticOps, TestBigIntCastFloatDouble) {
  // castINT from floats
  EXPECT_EQ(castBIGINT_float32(6.6f), 7);
  EXPECT_EQ(castBIGINT_float32(-6.6f), -7);
  EXPECT_EQ(castBIGINT_float32(-6.3f), -6);
  EXPECT_EQ(castBIGINT_float32(0.0f), 0);
  EXPECT_EQ(castBIGINT_float32(-0), 0);

  // castINT from doubles
  EXPECT_EQ(castBIGINT_float64(6.6), 7);
  EXPECT_EQ(castBIGINT_float64(-6.6), -7);
  EXPECT_EQ(castBIGINT_float64(-6.3), -6);
  EXPECT_EQ(castBIGINT_float64(0.0), 0);
  EXPECT_EQ(castBIGINT_float64(-0), 0);
  EXPECT_EQ(castBIGINT_float64(999999.99999999999999999999999), 1000000);
  EXPECT_EQ(castBIGINT_float64(-999999.99999999999999999999999), -1000000);
  EXPECT_EQ(castBIGINT_float64(INT32_MAX), 2147483647);
  EXPECT_EQ(castBIGINT_float64(-2147483647), -2147483647);
}

}  // namespace gandiva

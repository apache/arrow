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

#include <cstdint>

#include "../execution_context.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/decimal.h"
#include "gandiva/decimal_scalar.h"
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

  context.Reset();
  EXPECT_EQ(mod_uint32_uint32(10, 3), 1);
  EXPECT_FALSE(context.has_error());

  context.Reset();
  EXPECT_EQ(mod_uint64_uint64(10, 3), 1);
  EXPECT_FALSE(context.has_error());
}

TEST(TestArithmeticOps, TestNegativeDecimal) {
  gandiva::ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  int64_t out_high_bits = 0;
  uint64_t out_low_bits = 0;

  arrow::Decimal128 input_decimal("-10.5");
  negative_decimal(ctx_ptr, input_decimal.high_bits(), input_decimal.low_bits(), 3, 1, 3,
                   1, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal("10.5");
  EXPECT_EQ(output_decimal.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal2("10.5");
  negative_decimal(ctx_ptr, input_decimal2.high_bits(), input_decimal2.low_bits(), 3, 1,
                   3, 1, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal2("-10.5");
  EXPECT_EQ(output_decimal2.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal2.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal3("-23049223942343.532412");
  negative_decimal(ctx_ptr, input_decimal3.high_bits(), input_decimal3.low_bits(), 20, 6,
                   20, 6, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal3("23049223942343.532412");
  EXPECT_EQ(output_decimal3.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal3.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal4("15.001");
  negative_decimal(ctx_ptr, input_decimal4.high_bits(), input_decimal4.low_bits(), 2, 3,
                   2, 3, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal4("-15.001");
  EXPECT_EQ(output_decimal4.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal4.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal5("17");
  negative_decimal(ctx_ptr, input_decimal5.high_bits(), input_decimal5.low_bits(), 2, 0,
                   2, 0, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal5("-17");
  EXPECT_EQ(output_decimal5.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal5.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal6("-99917");
  negative_decimal(ctx_ptr, input_decimal6.high_bits(), input_decimal6.low_bits(), 5, 0,
                   5, 0, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal6("99917");
  EXPECT_EQ(output_decimal6.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal6.low_bits(), out_low_bits);

  arrow::Decimal128 input_decimal7("0.99917");
  negative_decimal(ctx_ptr, input_decimal7.high_bits(), input_decimal7.low_bits(), 0, 5,
                   0, 5, &out_high_bits, &out_low_bits);
  arrow::Decimal128 output_decimal7("-0.99917");
  EXPECT_EQ(output_decimal7.high_bits(), out_high_bits);
  EXPECT_EQ(output_decimal7.low_bits(), out_low_bits);
}

TEST(TestArithmeticOps, TestPositiveNegative) {
  EXPECT_EQ(positive_int32(10), 10);
  EXPECT_EQ(positive_int64(1000), 1000);
  EXPECT_EQ(positive_float32(1.500f), 1.500f);
  EXPECT_EQ(positive_float64(10.500f), 10.500f);

  EXPECT_EQ(negative_float32(1.500f), -1.500f);
  EXPECT_EQ(negative_float64(10.500f), -10.500f);

  EXPECT_EQ(positive_int32(-10), -10);
  EXPECT_EQ(positive_int64(-1000), -1000);
  EXPECT_EQ(positive_float32(-1.500f), -1.500f);
  EXPECT_EQ(positive_float64(-10.500f), -10.500f);

  EXPECT_EQ(negative_float32(-1.500f), 1.500f);
  EXPECT_EQ(negative_float64(-10.500f), 10.500f);

  gandiva::ExecutionContext ctx;

  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EXPECT_EQ(negative_int32(ctx_ptr, 100), -100);
  EXPECT_EQ(negative_int32(ctx_ptr, -100), 100);

  EXPECT_EQ(negative_int64(ctx_ptr, 100L), -100L);
  EXPECT_EQ(negative_int64(ctx_ptr, -100L), 100L);

  EXPECT_EQ(negative_int32(ctx_ptr, (INT32_MIN + 1)), (INT32_MAX));
  EXPECT_EQ(negative_int64(ctx_ptr, (INT64_MIN + 1)), (INT64_MAX));

  negative_int32(ctx_ptr, INT32_MIN);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Overflow in negative execution"));
  ctx.Reset();

  negative_int64(ctx_ptr, INT64_MIN);
  EXPECT_THAT(ctx.get_error(), ::testing::HasSubstr("Overflow in negative execution"));
  ctx.Reset();
}

TEST(TestArithmeticOps, TestNegativeIntervalTypes) {
  gandiva::ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  // Input: 8589934594;         Mean:  2 time &  2 days
  // Response: -4294967298;     Mean: -2 time & -2 days
  gdv_int64 result = negative_daytimeinterval(ctx_ptr, 8589934594);
  EXPECT_EQ(result, -4294967298);

  // Input: -4294967298;      Mean: -2 time & -2 days
  // Response: 8589934594;    Mean:  2 time &  2 days
  result = negative_daytimeinterval(ctx_ptr, -4294967298);
  EXPECT_EQ(result, 8589934594);

  // Input: -12884903388;      Mean: -4 time & -1500 days
  // Response: 17179870684;    Mean:  4 time &  1500 days
  result = negative_daytimeinterval(ctx_ptr, -12884903388);
  EXPECT_EQ(result, 17179870684);

  // Input: 44023418384000;      Mean:  10250 time &  3600000 days
  // Response: -44019123416704;  Mean: -10250 time & -3600000 days
  result = negative_daytimeinterval(ctx_ptr, 44023418384000);
  EXPECT_EQ(result, -44019123416704);

  // Input: 9223372034707292159;      Mean:  2147483647 time &  2147483647 days
  // Response: -9223372030412324863;  Mean: -2147483647 time & -2147483647 days
  const int64_t INT_MAX_TO_NEGATIVE_INTERVAL_DAY_TIME = 9223372034707292159;
  result = negative_daytimeinterval(ctx_ptr, INT_MAX_TO_NEGATIVE_INTERVAL_DAY_TIME);
  EXPECT_EQ(result, -9223372030412324863);

  result = negative_daytimeinterval(ctx_ptr, INT64_MAX);
  EXPECT_EQ(ctx.has_error(), true);
  EXPECT_EQ(ctx.get_error(),
            "Interval day time is out of boundaries for the negative function");
  ctx.Reset();

  const int64_t INT_MIN_TO_NEGATIVE_INTERVAL_DAY_TIME = -9223372030412324863;
  result = negative_daytimeinterval(ctx_ptr, INT_MIN_TO_NEGATIVE_INTERVAL_DAY_TIME);
  EXPECT_EQ(result, INT_MAX_TO_NEGATIVE_INTERVAL_DAY_TIME);

  result = negative_daytimeinterval(ctx_ptr, INT64_MIN);
  EXPECT_EQ(ctx.has_error(), true);
  EXPECT_EQ(ctx.get_error(),
            "Interval day time is out of boundaries for the negative function");
  ctx.Reset();

  // Month interval
  gdv_month_interval result2 = negative_month_interval(ctx_ptr, 2);
  EXPECT_EQ(result2, -2);

  result2 = negative_month_interval(ctx_ptr, -2);
  EXPECT_EQ(result2, 2);

  result2 = negative_month_interval(ctx_ptr, 510);
  EXPECT_EQ(result2, -510);

  result2 = negative_month_interval(ctx_ptr, INT32_MAX);
  EXPECT_EQ(result2, -INT32_MAX);
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

  EXPECT_EQ(div_uint32_uint32(reinterpret_cast<gdv_int64>(&context), 101, 111), 0);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();

  EXPECT_EQ(div_uint64_uint64(reinterpret_cast<gdv_int64>(&context), 101, 111), 0);
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

TEST(TestArithmeticOps, TestIsTrueFalse) {
  EXPECT_EQ(istrue_boolean(true, true), true);
  EXPECT_EQ(istrue_boolean(false, true), false);
  EXPECT_EQ(isfalse_boolean(true, true), false);
  EXPECT_EQ(isfalse_boolean(false, true), true);

  EXPECT_EQ(istrue_boolean(true, false), false);
  EXPECT_EQ(istrue_boolean(false, false), false);
  EXPECT_EQ(isfalse_boolean(true, false), false);
  EXPECT_EQ(isfalse_boolean(false, false), false);

  EXPECT_EQ(isnottrue_boolean(true, true), false);
  EXPECT_EQ(isnottrue_boolean(false, true), true);
  EXPECT_EQ(isnotfalse_boolean(true, true), true);
  EXPECT_EQ(isnotfalse_boolean(false, true), false);

  EXPECT_EQ(isnottrue_boolean(true, false), true);
  EXPECT_EQ(isnottrue_boolean(false, false), true);
  EXPECT_EQ(isnotfalse_boolean(true, false), true);
  EXPECT_EQ(isnotfalse_boolean(false, false), true);

  EXPECT_EQ(istrue_int32(10, true), true);
  EXPECT_EQ(isnottrue_int32(10, true), false);
  EXPECT_EQ(isnottrue_int32(10, false), true);

  EXPECT_EQ(istrue_int32(0, true), false);
  EXPECT_EQ(isnottrue_int32(0, true), true);

  EXPECT_EQ(isfalse_int32(10, true), false);
  EXPECT_EQ(isnotfalse_int32(10, true), true);

  EXPECT_EQ(isfalse_int32(0, true), true);
  EXPECT_EQ(isnotfalse_int32(0, true), false);
  EXPECT_EQ(isnotfalse_int32(0, false), true);

  EXPECT_EQ(istrue_int64(10, true), true);
  EXPECT_EQ(isnottrue_int64(10, true), false);
  EXPECT_EQ(isnottrue_int64(10, false), true);

  EXPECT_EQ(istrue_int64(0, true), false);
  EXPECT_EQ(isnottrue_int64(0, true), true);

  EXPECT_EQ(isfalse_int64(10, true), false);
  EXPECT_EQ(isnotfalse_int64(10, true), true);

  EXPECT_EQ(isfalse_int64(0, true), true);
  EXPECT_EQ(isnotfalse_int64(0, true), false);
  EXPECT_EQ(isnotfalse_int64(0, false), true);

  EXPECT_EQ(istrue_uint32(10, true), true);
  EXPECT_EQ(isnottrue_uint32(10, true), false);
  EXPECT_EQ(isnottrue_uint32(10, false), true);

  EXPECT_EQ(istrue_uint32(0, true), false);
  EXPECT_EQ(isnottrue_uint32(0, true), true);

  EXPECT_EQ(isfalse_uint32(10, true), false);
  EXPECT_EQ(isnotfalse_uint32(10, true), true);

  EXPECT_EQ(isfalse_uint32(0, true), true);
  EXPECT_EQ(isnotfalse_uint32(0, true), false);
  EXPECT_EQ(isnotfalse_uint32(0, false), true);

  EXPECT_EQ(istrue_uint64(10, true), true);
  EXPECT_EQ(isnottrue_uint64(10, true), false);
  EXPECT_EQ(isnottrue_uint64(10, false), true);

  EXPECT_EQ(istrue_uint64(0, true), false);
  EXPECT_EQ(isnottrue_uint64(0, true), true);

  EXPECT_EQ(isfalse_uint64(10, true), false);
  EXPECT_EQ(isnotfalse_uint64(10, true), true);

  EXPECT_EQ(isfalse_uint64(0, true), true);
  EXPECT_EQ(isnotfalse_uint64(0, true), false);
  EXPECT_EQ(isnotfalse_uint64(0, false), true);

  EXPECT_EQ(isfalse_uint64(0, false), false);
  EXPECT_EQ(isnotfalse_uint64(0, false), true);

  EXPECT_EQ(isfalse_uint64(10, false), false);
  EXPECT_EQ(isnotfalse_uint64(10, false), true);
}

TEST(TestArithmeticOps, TestNvl) {
  EXPECT_EQ(nvl_int32_int32(10, false, 20, true), 20);
  EXPECT_EQ(nvl_int64_int64(10, false, 20, true), 20);
  EXPECT_EQ(nvl_float32_float32(10.0, false, 20.0, true), 20);
  EXPECT_EQ(nvl_float64_float64(10.0, false, 20.0, true), 20);
  EXPECT_EQ(nvl_boolean_boolean(true, false, false, true), false);
  EXPECT_EQ(nvl_int32_int32(10, true, 20, true), 10);
  EXPECT_EQ(nvl_int64_int64(10, true, 20, true), 10);
  EXPECT_EQ(nvl_float32_float32(10.0, true, 20.0, true), 10);
  EXPECT_EQ(nvl_float64_float64(10.0, true, 20.0, true), 10);
  EXPECT_EQ(nvl_boolean_boolean(true, true, false, true), true);
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

TEST(TestArithmeticOps, TestSignIntFloatDouble) {
  // sign from int32
  EXPECT_EQ(sign_int32(43), 1);
  EXPECT_EQ(sign_int32(-54), -1);
  EXPECT_EQ(sign_int32(63), 1);
  EXPECT_EQ(sign_int32(INT32_MAX), 1);
  EXPECT_EQ(sign_int32(INT32_MIN), -1);

  // sign from int64
  EXPECT_EQ(sign_int64(90), 1);
  EXPECT_EQ(sign_int64(-7), -1);
  EXPECT_EQ(sign_int64(INT64_MAX), 1);
  EXPECT_EQ(sign_int64(INT64_MIN), -1);

  // sign from floats
  EXPECT_EQ(sign_float32(6.6f), 1.0f);
  EXPECT_EQ(sign_float32(-6.6f), -1.0f);
  EXPECT_EQ(sign_float32(-6.3f), -1.0f);
  EXPECT_EQ(sign_float32(0.0f), 0.0f);
  EXPECT_EQ(sign_float32(-0.0f), 0.0f);
  EXPECT_EQ(sign_float32(INFINITY), 1.0f);
  EXPECT_EQ(sign_float32(-INFINITY), -1.0f);

  // sign from doubles
  EXPECT_EQ(sign_float64(6.6), 1.0);
  EXPECT_EQ(sign_float64(-6.6), -1.0);
  EXPECT_EQ(sign_float64(-6.3), -1.0);
  EXPECT_EQ(sign_float64(0.0), 0);
  EXPECT_EQ(sign_float64(-0), 0);
  EXPECT_EQ(sign_float64(999999.99999999999999999999999), 1.0);
  EXPECT_EQ(sign_float64(-999999.99999999999999999999999), -1.0);
  EXPECT_EQ(sign_float64(INFINITY), 1.0);
  EXPECT_EQ(sign_float64(-INFINITY), -1.0);
  EXPECT_TRUE(std::isnan(sign_float64(std::numeric_limits<double>::quiet_NaN())));
  EXPECT_EQ(sign_float64(-2147483647), -1.0);
}

TEST(TestArithmeticOps, TestAbsIntFloatDouble) {
  // abs from int32
  EXPECT_EQ(abs_int32(43), 43);
  EXPECT_EQ(abs_int32(-54), 54);
  EXPECT_EQ(abs_int32(INT32_MAX), INT32_MAX);

  // abs from int64
  EXPECT_EQ(abs_int64(90), 90);
  EXPECT_EQ(abs_int64(-7), 7);
  EXPECT_EQ(abs_int64(INT64_MAX), INT64_MAX);

  // abs from floats
  EXPECT_EQ(abs_float32(6.6f), 6.6f);
  EXPECT_EQ(abs_float32(-6.6f), 6.6f);
  EXPECT_EQ(abs_float32(0.0f), 0.0f);
  EXPECT_EQ(abs_float32(-0.0f), 0.0f);
  EXPECT_EQ(abs_float32(INFINITY), INFINITY);
  EXPECT_EQ(abs_float32(-INFINITY), INFINITY);

  // abs from doubles
  EXPECT_EQ(abs_float64(6.6), 6.6);
  EXPECT_EQ(abs_float64(-6.6), 6.6);
  EXPECT_EQ(abs_float64(0.0), 0.);
  EXPECT_EQ(abs_float64(-0), 0.);
  EXPECT_EQ(abs_float64(999999.99999999999999999999999), 999999.99999999999999999999999);
  EXPECT_EQ(abs_float64(-999999.99999999999999999999999), 999999.99999999999999999999999);
  EXPECT_EQ(abs_float64(INFINITY), INFINITY);
  EXPECT_EQ(abs_float64(-INFINITY), INFINITY);
  EXPECT_TRUE(std::isnan(abs_float64(std::numeric_limits<double>::quiet_NaN())));
}

TEST(TestArithmeticOps, TestCeilingFloatDouble) {
  // ceiling from floats
  EXPECT_EQ(ceiling_float32(6.6f), 7.0f);
  EXPECT_EQ(ceiling_float32(-6.6f), -6.0f);
  EXPECT_EQ(ceiling_float32(-6.3f), -6.0f);
  EXPECT_EQ(ceiling_float32(0.0f), 0.0f);
  EXPECT_EQ(ceiling_float32(-0), 0.0);

  // ceiling from doubles
  EXPECT_EQ(ceiling_float64(6.6), 7.0);
  EXPECT_EQ(ceiling_float64(-6.6), -6.0);
  EXPECT_EQ(ceiling_float64(-6.3), -6.0);
  EXPECT_EQ(ceiling_float64(0.0), 0.0);
  EXPECT_EQ(ceiling_float64(-0), 0.0);
  EXPECT_EQ(ceiling_float64(999999.99999999999999999999999), 1000000.0);
  EXPECT_EQ(ceiling_float64(-999999.99999999999999999999999), -1000000.0);
  EXPECT_EQ(ceiling_float64(2147483647.7), 2147483648.0);
  EXPECT_EQ(ceiling_float64(-2147483647), -2147483647.0);
}

TEST(TestArithmeticOps, TestFloorFloatDouble) {
  // ceiling from floats
  EXPECT_EQ(floor_float32(6.6f), 6.0f);
  EXPECT_EQ(floor_float32(-6.6f), -7.0f);
  EXPECT_EQ(floor_float32(-6.3f), -7.0f);
  EXPECT_EQ(floor_float32(0.0f), 0.0f);
  EXPECT_EQ(floor_float32(-0), 0.0);

  // ceiling from doubles
  EXPECT_EQ(floor_float64(6.6), 6.0);
  EXPECT_EQ(floor_float64(-6.6), -7.0);
  EXPECT_EQ(floor_float64(-6.3), -7.0);
  EXPECT_EQ(floor_float64(0.0), 0.0);
  EXPECT_EQ(floor_float64(-0), 0.0);
  EXPECT_EQ(floor_float64(999999.99999999999999999999999), 1000000.0);
  EXPECT_EQ(floor_float64(-999999.99999999999999999999999), -1000000.0);
  EXPECT_EQ(floor_float64(2147483647.7), 2147483647.0);
  EXPECT_EQ(floor_float64(-2147483647), -2147483647.0);
}

TEST(TestArithmeticOps, TestSqrtIntFloatDouble) {
  // sqrt from int32
  EXPECT_EQ(sqrt_int32(36), 6.0);
  EXPECT_EQ(sqrt_int32(49), 7.0);
  EXPECT_EQ(sqrt_int32(64), 8.0);
  EXPECT_EQ(sqrt_int32(81), 9.0);

  // sqrt from int64
  EXPECT_EQ(sqrt_int64(4), 2.0);
  EXPECT_EQ(sqrt_int64(9), 3.0);
  EXPECT_EQ(sqrt_int64(64), 8.0);
  EXPECT_EQ(sqrt_int64(81), 9.0);

  // sqrt from floats
  EXPECT_EQ(sqrt_float32(16.0f), 4.0);
  EXPECT_EQ(sqrt_float32(49.0f), 7.0);
  EXPECT_EQ(sqrt_float32(36.0f), 6.0);
  EXPECT_EQ(sqrt_float32(0.0f), 0.0);

  // sqrt from doubles
  EXPECT_EQ(sqrt_float64(16.0), 4.0);
  EXPECT_EQ(sqrt_float64(11.0889), 3.33);
  EXPECT_EQ(sqrt_float64(1.522756), 1.234);
  EXPECT_EQ(sqrt_float64(49.0), 7.0);
  EXPECT_EQ(sqrt_float64(36.0), 6.0);
  EXPECT_EQ(sqrt_float64(0.0), 0.0);
  EXPECT_TRUE(std::isnan(sqrt_float64(-1.0)));
}

}  // namespace gandiva

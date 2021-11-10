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

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#include <gtest/gtest.h>
#include <cmath>
#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

static const double MAX_ERROR = 0.00005;

void VerifyFuzzyEquals(double actual, double expected, double max_error = MAX_ERROR) {
  EXPECT_TRUE(fabs(actual - expected) < max_error) << actual << " != " << expected;
}

TEST(TestExtendedMathOps, TestCbrt) {
  VerifyFuzzyEquals(cbrt_int32(27), 3);
  VerifyFuzzyEquals(cbrt_int64(27), 3);
  VerifyFuzzyEquals(cbrt_float32(27), 3);
  VerifyFuzzyEquals(cbrt_float64(27), 3);
  VerifyFuzzyEquals(cbrt_float64(-27), -3);

  VerifyFuzzyEquals(cbrt_float32(15.625), 2.5);
  VerifyFuzzyEquals(cbrt_float64(15.625), 2.5);
}

TEST(TestExtendedMathOps, TestExp) {
  double val = 20.085536923187668;

  VerifyFuzzyEquals(exp_int32(3), val);
  VerifyFuzzyEquals(exp_int64(3), val);
  VerifyFuzzyEquals(exp_float32(3), val);
  VerifyFuzzyEquals(exp_float64(3), val);
}

TEST(TestExtendedMathOps, TestLog) {
  double val = 4.1588830833596715;

  VerifyFuzzyEquals(log_int32(64), val);
  VerifyFuzzyEquals(log_int64(64), val);
  VerifyFuzzyEquals(log_float32(64), val);
  VerifyFuzzyEquals(log_float64(64), val);

  EXPECT_EQ(log_int32(0), -std::numeric_limits<double>::infinity());
}

TEST(TestExtendedMathOps, TestLog10) {
  VerifyFuzzyEquals(log10_int32(100), 2);
  VerifyFuzzyEquals(log10_int64(100), 2);
  VerifyFuzzyEquals(log10_float32(100), 2);
  VerifyFuzzyEquals(log10_float64(100), 2);
}

TEST(TestExtendedMathOps, TestPower) {
  VerifyFuzzyEquals(power_float64_float64(2, 5.4), 42.22425314473263);
  VerifyFuzzyEquals(power_float64_float64(5.4, 2), 29.160000000000004);
}

TEST(TestExtendedMathOps, TestLogWithBase) {
  gandiva::ExecutionContext context;
  gdv_float64 out =
      log_int32_int32(reinterpret_cast<gdv_int64>(&context), 1 /*base*/, 10 /*value*/);
  VerifyFuzzyEquals(out, 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_TRUE(context.get_error().find("divide by zero error") != std::string::npos)
      << context.get_error();

  gandiva::ExecutionContext context1;
  out = log_int32_int32(reinterpret_cast<gdv_int64>(&context), 2 /*base*/, 64 /*value*/);
  VerifyFuzzyEquals(out, 6);
  EXPECT_EQ(context1.has_error(), false);
}

TEST(TestExtendedMathOps, TestRoundDecimal) {
  EXPECT_FLOAT_EQ(round_float32(1234.245f), 1234);
  EXPECT_FLOAT_EQ(round_float32(-11.7892f), -12);
  EXPECT_FLOAT_EQ(round_float32(1.4999999f), 1);
  EXPECT_EQ(std::signbit(round_float32(0)), 0);
  EXPECT_FLOAT_EQ(round_float32_int32(1234.789f, 2), 1234.79f);
  EXPECT_FLOAT_EQ(round_float32_int32(1234.12345f, -3), 1000);
  EXPECT_FLOAT_EQ(round_float32_int32(-1234.4567f, 3), -1234.457f);
  EXPECT_FLOAT_EQ(round_float32_int32(-1234.4567f, -3), -1000);
  EXPECT_FLOAT_EQ(round_float32_int32(1234.4567f, 0), 1234);
  EXPECT_FLOAT_EQ(round_float32_int32(1.5499999523162842f, 1), 1.5f);
  EXPECT_EQ(std::signbit(round_float32_int32(0, 5)), 0);
  EXPECT_FLOAT_EQ(round_float32_int32(static_cast<float>(1.55), 1), 1.5f);
  EXPECT_FLOAT_EQ(round_float32_int32(static_cast<float>(9.134123), 2), 9.13f);
  EXPECT_FLOAT_EQ(round_float32_int32(static_cast<float>(-1.923), 1), -1.9f);

  VerifyFuzzyEquals(round_float64(1234.245), 1234);
  VerifyFuzzyEquals(round_float64(-11.7892), -12);
  VerifyFuzzyEquals(round_float64(1.4999999), 1);
  EXPECT_EQ(std::signbit(round_float64(0)), 0);
  VerifyFuzzyEquals(round_float64_int32(1234.789, 2), 1234.79);
  VerifyFuzzyEquals(round_float64_int32(1234.12345, -3), 1000);
  VerifyFuzzyEquals(round_float64_int32(-1234.4567, 3), -1234.457);
  VerifyFuzzyEquals(round_float64_int32(-1234.4567, -3), -1000);
  VerifyFuzzyEquals(round_float64_int32(1234.4567, 0), 1234);
  EXPECT_EQ(std::signbit(round_float64_int32(0, -2)), 0);
  VerifyFuzzyEquals(round_float64_int32((double)INT_MAX + 1, 0), (double)INT_MAX + 1);
  VerifyFuzzyEquals(round_float64_int32((double)INT_MIN - 1, 0), (double)INT_MIN - 1);
}

TEST(TestExtendedMathOps, TestBRoundDecimal) {
  EXPECT_DOUBLE_EQ(bround_float64(0.0), 0);
  EXPECT_DOUBLE_EQ(bround_float64(2.5), 2);
  EXPECT_DOUBLE_EQ(bround_float64(3.5), 4);
  EXPECT_DOUBLE_EQ(bround_float64(-2.5), -2);
  EXPECT_DOUBLE_EQ(bround_float64(-3.5), -4);
  EXPECT_DOUBLE_EQ(bround_float64(1.4999999), 1);
  EXPECT_DOUBLE_EQ(bround_float64(1.50001), 2);
  EXPECT_EQ(std::signbit(bround_float64(0)), 0);

  VerifyFuzzyEquals(bround_float64(2.5), 2);
  VerifyFuzzyEquals(bround_float64(3.5), 4);
  VerifyFuzzyEquals(bround_float64(-2.5), -2);
  VerifyFuzzyEquals(bround_float64(-3.5), -4);
  VerifyFuzzyEquals(bround_float64(1.4999999), 1);
  VerifyFuzzyEquals(bround_float64(1.50001), 2);
}

TEST(TestExtendedMathOps, TestRound) {
  EXPECT_EQ(round_int32(21134), 21134);
  EXPECT_EQ(round_int32(-132422), -132422);
  EXPECT_EQ(round_int32_int32(7589, -1), 7590);
  EXPECT_EQ(round_int32_int32(8532, -2), 8500);
  EXPECT_EQ(round_int32_int32(-8579, -1), -8580);
  EXPECT_EQ(round_int32_int32(-8612, -2), -8600);
  EXPECT_EQ(round_int32_int32(758, 2), 758);
  EXPECT_EQ(round_int32_int32(8612, -5), 0);

  EXPECT_EQ(round_int64(3453562312), 3453562312);
  EXPECT_EQ(round_int64(-23453462343), -23453462343);
  EXPECT_EQ(round_int64_int32(3453562312, -2), 3453562300);
  EXPECT_EQ(round_int64_int32(3453562343, -5), 3453600000);
  EXPECT_EQ(round_int64_int32(345353425343, 12), 345353425343);
  EXPECT_EQ(round_int64_int32(-23453462343, -4), -23453460000);
  EXPECT_EQ(round_int64_int32(-23453462343, -5), -23453500000);
  EXPECT_EQ(round_int64_int32(345353425343, -12), 0);
}

TEST(TestExtendedMathOps, TestTruncate) {
  EXPECT_EQ(truncate_int64_int32(1234, 4), 1234);
  EXPECT_EQ(truncate_int64_int32(-1234, 4), -1234);
  EXPECT_EQ(truncate_int64_int32(1234, -4), 0);
  EXPECT_EQ(truncate_int64_int32(-1234, -2), -1200);
  EXPECT_EQ(truncate_int64_int32(8124674407369523212, 0), 8124674407369523212);
  EXPECT_EQ(truncate_int64_int32(8124674407369523212, -2), 8124674407369523200);
}

TEST(TestExtendedMathOps, TestTrigonometricFunctions) {
  auto pi_float = static_cast<float>(M_PI);
  // Sin functions
  VerifyFuzzyEquals(sin_float32(0), sin(0));
  VerifyFuzzyEquals(sin_float32(0), sin(0));
  VerifyFuzzyEquals(sin_float32(pi_float / 2), sin(M_PI / 2));
  VerifyFuzzyEquals(sin_float32(pi_float), sin(M_PI));
  VerifyFuzzyEquals(sin_float32(-pi_float / 2), sin(-M_PI / 2));
  VerifyFuzzyEquals(sin_float64(0), sin(0));
  VerifyFuzzyEquals(sin_float64(M_PI / 2), sin(M_PI / 2));
  VerifyFuzzyEquals(sin_float64(M_PI), sin(M_PI));
  VerifyFuzzyEquals(sin_float64(-M_PI / 2), sin(-M_PI / 2));
  VerifyFuzzyEquals(sin_int32(0), sin(0));
  VerifyFuzzyEquals(sin_int64(0), sin(0));

  // Cos functions
  VerifyFuzzyEquals(cos_float32(0), cos(0));
  VerifyFuzzyEquals(cos_float32(pi_float / 2), cos(M_PI / 2));
  VerifyFuzzyEquals(cos_float32(pi_float), cos(M_PI));
  VerifyFuzzyEquals(cos_float32(-pi_float / 2), cos(-M_PI / 2));
  VerifyFuzzyEquals(cos_float64(0), cos(0));
  VerifyFuzzyEquals(cos_float64(M_PI / 2), cos(M_PI / 2));
  VerifyFuzzyEquals(cos_float64(M_PI), cos(M_PI));
  VerifyFuzzyEquals(cos_float64(-M_PI / 2), cos(-M_PI / 2));
  VerifyFuzzyEquals(cos_int32(0), cos(0));
  VerifyFuzzyEquals(cos_int64(0), cos(0));

  // Asin functions
  VerifyFuzzyEquals(asin_float32(-1.0), asin(-1.0));
  VerifyFuzzyEquals(asin_float32(1.0), asin(1.0));
  VerifyFuzzyEquals(asin_float64(-1.0), asin(-1.0));
  VerifyFuzzyEquals(asin_float64(1.0), asin(1.0));
  VerifyFuzzyEquals(asin_int32(0), asin(0));
  VerifyFuzzyEquals(asin_int64(0), asin(0));

  // Acos functions
  VerifyFuzzyEquals(acos_float32(-1.0), acos(-1.0));
  VerifyFuzzyEquals(acos_float32(1.0), acos(1.0));
  VerifyFuzzyEquals(acos_float64(-1.0), acos(-1.0));
  VerifyFuzzyEquals(acos_float64(1.0), acos(1.0));
  VerifyFuzzyEquals(acos_int32(0), acos(0));
  VerifyFuzzyEquals(acos_int64(0), acos(0));

  // Tan
  VerifyFuzzyEquals(tan_float32(pi_float), tan(M_PI));
  VerifyFuzzyEquals(tan_float32(-pi_float), tan(-M_PI));
  VerifyFuzzyEquals(tan_float64(M_PI), tan(M_PI));
  VerifyFuzzyEquals(tan_float64(-M_PI), tan(-M_PI));
  VerifyFuzzyEquals(tan_int32(0), tan(0));
  VerifyFuzzyEquals(tan_int64(0), tan(0));

  // Atan
  VerifyFuzzyEquals(atan_float32(pi_float), atan(M_PI));
  VerifyFuzzyEquals(atan_float32(-pi_float), atan(-M_PI));
  VerifyFuzzyEquals(atan_float64(M_PI), atan(M_PI));
  VerifyFuzzyEquals(atan_float64(-M_PI), atan(-M_PI));
  VerifyFuzzyEquals(atan_int32(0), atan(0));
  VerifyFuzzyEquals(atan_int64(0), atan(0));

  // Sinh functions
  VerifyFuzzyEquals(sinh_float32(0), sinh(0));
  VerifyFuzzyEquals(sinh_float32(pi_float / 2), sinh(M_PI / 2));
  VerifyFuzzyEquals(sinh_float32(pi_float), sinh(M_PI));
  VerifyFuzzyEquals(sinh_float32(-pi_float / 2), sinh(-M_PI / 2));
  VerifyFuzzyEquals(sinh_float64(0), sinh(0));
  VerifyFuzzyEquals(sinh_float64(M_PI / 2), sinh(M_PI / 2));
  VerifyFuzzyEquals(sinh_float64(M_PI), sinh(M_PI));
  VerifyFuzzyEquals(sinh_float64(-M_PI / 2), sinh(-M_PI / 2));
  VerifyFuzzyEquals(sinh_int32(0), sinh(0));
  VerifyFuzzyEquals(sinh_int64(0), sinh(0));

  // Cosh functions
  VerifyFuzzyEquals(cosh_float32(0), cosh(0));
  VerifyFuzzyEquals(cosh_float32(pi_float / 2), cosh(M_PI / 2));
  VerifyFuzzyEquals(cosh_float32(pi_float), cosh(M_PI));
  VerifyFuzzyEquals(cosh_float32(-pi_float / 2), cosh(-M_PI / 2));
  VerifyFuzzyEquals(cosh_float64(0), cosh(0));
  VerifyFuzzyEquals(cosh_float64(M_PI / 2), cosh(M_PI / 2));
  VerifyFuzzyEquals(cosh_float64(M_PI), cosh(M_PI));
  VerifyFuzzyEquals(cosh_float64(-M_PI / 2), cosh(-M_PI / 2));
  VerifyFuzzyEquals(cosh_int32(0), cosh(0));
  VerifyFuzzyEquals(cosh_int64(0), cosh(0));

  // Tanh
  VerifyFuzzyEquals(tanh_float32(pi_float), tanh(M_PI));
  VerifyFuzzyEquals(tanh_float32(-pi_float), tanh(-M_PI));
  VerifyFuzzyEquals(tanh_float64(M_PI), tanh(M_PI));
  VerifyFuzzyEquals(tanh_float64(-M_PI), tanh(-M_PI));
  VerifyFuzzyEquals(tanh_int32(0), tanh(0));
  VerifyFuzzyEquals(tanh_int64(0), tanh(0));

  // Atan2
  VerifyFuzzyEquals(atan2_float32_float32(1, 0), atan2(1, 0));
  VerifyFuzzyEquals(atan2_float32_float32(-1.0, 0), atan2(-1, 0));
  VerifyFuzzyEquals(atan2_float64_float64(1.0, 0.0), atan2(1, 0));
  VerifyFuzzyEquals(atan2_float64_float64(-1, 0), atan2(-1, 0));
  VerifyFuzzyEquals(atan2_int32_int32(1, 0), atan2(1, 0));
  VerifyFuzzyEquals(atan2_int64_int64(-1, 0), atan2(-1, 0));

  // Radians
  VerifyFuzzyEquals(radians_float32(0), 0);
  VerifyFuzzyEquals(radians_float32(180.0), M_PI);
  VerifyFuzzyEquals(radians_float32(90.0), M_PI / 2);
  VerifyFuzzyEquals(radians_float64(0), 0);
  VerifyFuzzyEquals(radians_float64(180.0), M_PI);
  VerifyFuzzyEquals(radians_float64(90.0), M_PI / 2);
  VerifyFuzzyEquals(radians_int32(180), M_PI);
  VerifyFuzzyEquals(radians_int64(90), M_PI / 2);

  // Degrees
  VerifyFuzzyEquals(degrees_float32(0), 0.0);
  VerifyFuzzyEquals(degrees_float32(pi_float), 180.0);
  VerifyFuzzyEquals(degrees_float32(pi_float / 2), 90.0);
  VerifyFuzzyEquals(degrees_float64(0), 0.0);
  VerifyFuzzyEquals(degrees_float64(M_PI), 180.0);
  VerifyFuzzyEquals(degrees_float64(M_PI / 2), 90.0);
  VerifyFuzzyEquals(degrees_int32(1), 57.2958);
  VerifyFuzzyEquals(degrees_int64(1), 57.2958);

  // Cot
  VerifyFuzzyEquals(cot_float32(pi_float / 2), tan(M_PI / 2 - M_PI / 2));
  VerifyFuzzyEquals(cot_float64(M_PI / 2), tan(M_PI / 2 - M_PI / 2));
}

TEST(TestExtendedMathOps, TestBinRepresentation) {
  gandiva::ExecutionContext ctx;
  uint64_t ctx_ptr = reinterpret_cast<gdv_int64>(&ctx);
  gdv_int32 out_len = 0;

  const char* out_str = bin_int32(ctx_ptr, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "111");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, 28550, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "110111110000110");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, -28550, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "11111111111111111001000001111010");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, 58117, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1110001100000101");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, -58117, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "11111111111111110001110011111011");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, INT32_MAX, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1111111111111111111111111111111");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int32(ctx_ptr, INT32_MIN, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "10000000000000000000000000000000");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, 7, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "111");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, 0, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "0");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, 28550, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "110111110000110");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, -28550, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "1111111111111111111111111111111111111111111111111001000001111010");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, 58117, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "1110001100000101");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, -58117, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "1111111111111111111111111111111111111111111111110001110011111011");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, INT64_MAX, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "111111111111111111111111111111111111111111111111111111111111111");
  EXPECT_FALSE(ctx.has_error());

  out_str = bin_int64(ctx_ptr, INT64_MIN, &out_len);
  EXPECT_EQ(std::string(out_str, out_len),
            "1000000000000000000000000000000000000000000000000000000000000000");
  EXPECT_FALSE(ctx.has_error());
}
}  // namespace gandiva

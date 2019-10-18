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

#include <math.h>

#include <gtest/gtest.h>
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
  float64 out =
      log_int32_int32(reinterpret_cast<int64>(&context), 1 /*base*/, 10 /*value*/);
  VerifyFuzzyEquals(out, 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_TRUE(context.get_error().find("divide by zero error") != std::string::npos)
      << context.get_error();

  gandiva::ExecutionContext context1;
  out = log_int32_int32(reinterpret_cast<int64>(&context), 2 /*base*/, 64 /*value*/);
  VerifyFuzzyEquals(out, 6);
  EXPECT_EQ(context1.has_error(), false);
}

TEST(TestExtendedMathOps, TestTruncate) {
  EXPECT_EQ(truncate_int64_int32(1234, 4), 1234);
  EXPECT_EQ(truncate_int64_int32(-1234, 4), -1234);
  EXPECT_EQ(truncate_int64_int32(1234, -4), 0);
  EXPECT_EQ(truncate_int64_int32(-1234, -2), -1200);
  EXPECT_EQ(truncate_int64_int32(8124674407369523212, 0), 8124674407369523212);
  EXPECT_EQ(truncate_int64_int32(8124674407369523212, -2), 8124674407369523200);
}

}  // namespace gandiva

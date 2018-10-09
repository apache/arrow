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

#include <gtest/gtest.h>
#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

TEST(TestExtendedMathOps, TestCbrt) {
  EXPECT_EQ(cbrt_int32(27), 3);
  EXPECT_EQ(cbrt_int64(27), 3);
  EXPECT_EQ(cbrt_float32(27), 3);
  EXPECT_EQ(cbrt_float64(27), 3);
  EXPECT_EQ(cbrt_float64(-27), -3);

  EXPECT_EQ(cbrt_float32(15.625), 2.5);
  EXPECT_EQ(cbrt_float64(15.625), 2.5);
}

TEST(TestExtendedMathOps, TestExp) {
  double val = 20.085536923187668;

  EXPECT_EQ(exp_int32(3), val);
  EXPECT_EQ(exp_int64(3), val);
  EXPECT_EQ(exp_float32(3), val);
  EXPECT_EQ(exp_float64(3), val);
}

TEST(TestExtendedMathOps, TestLog) {
  double val = 4.1588830833596715;

  EXPECT_EQ(log_int32(64), val);
  EXPECT_EQ(log_int64(64), val);
  EXPECT_EQ(log_float32(64), val);
  EXPECT_EQ(log_float64(64), val);

  EXPECT_EQ(log_int32(0), -std::numeric_limits<double>::infinity());
}

TEST(TestExtendedMathOps, TestLog10) {
  EXPECT_EQ(log10_int32(100), 2);
  EXPECT_EQ(log10_int64(100), 2);
  EXPECT_EQ(log10_float32(100), 2);
  EXPECT_EQ(log10_float64(100), 2);
}

TEST(TestExtendedMathOps, TestPower) {
  EXPECT_EQ(power_float64_float64(2, 5.4), 42.22425314473263);
  EXPECT_EQ(power_float64_float64(5.4, 2), 29.160000000000004);
}

TEST(TestArithmeticOps, TestLogWithBase) {
  boolean is_valid;
  gandiva::helpers::ExecutionContext error_holder;
  float64 out = log_int32_int32(1, true, 10, true, reinterpret_cast<int64>(&error_holder),
                                &is_valid);
  EXPECT_EQ(out, 0);
  EXPECT_EQ(is_valid, false);
  EXPECT_EQ(error_holder.has_error(), true);
  EXPECT_TRUE(error_holder.get_error().find("divide by zero error") != std::string::npos)
      << error_holder.get_error();

  gandiva::helpers::ExecutionContext error_holder1;
  out = log_int32_int32(2, true, 64, true, reinterpret_cast<int64>(&error_holder),
                        &is_valid);
  EXPECT_EQ(out, 6);
  EXPECT_EQ(is_valid, true);
  EXPECT_EQ(error_holder1.has_error(), false);
}

}  // namespace gandiva

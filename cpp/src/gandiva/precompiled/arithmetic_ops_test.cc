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

  EXPECT_DOUBLE_EQ(mod_float64_float64(reinterpret_cast<int64>(&context), 2.5, 0.0), 0.0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "divide by zero error");

  context.Reset();
  EXPECT_NEAR(mod_float64_float64(reinterpret_cast<int64>(&context), 2.5, 1.2), 0.1,
              acceptable_abs_error);
  EXPECT_FALSE(context.has_error());

  context.Reset();
  EXPECT_DOUBLE_EQ(mod_float64_float64(reinterpret_cast<int64>(&context), 2.5, 2.5), 0.0);
  EXPECT_FALSE(context.has_error());

  context.Reset();
  EXPECT_NEAR(mod_float64_float64(reinterpret_cast<int64>(&context), 9.2, 3.7), 1.8,
              acceptable_abs_error);
  EXPECT_FALSE(context.has_error());
}

TEST(TestArithmeticOps, TestDivide) {
  gandiva::ExecutionContext context;
  EXPECT_EQ(divide_int64_int64(reinterpret_cast<int64>(&context), 10, 0), 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");

  context.Reset();
  EXPECT_EQ(divide_int64_int64(reinterpret_cast<int64>(&context), 10, 2), 5);
  EXPECT_EQ(context.has_error(), false);
}

TEST(TestArithmeticOps, TestDiv) {
  gandiva::ExecutionContext context;
  EXPECT_EQ(div_int64_int64(reinterpret_cast<int64>(&context), 101, 0), 0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");
  context.Reset();

  EXPECT_EQ(div_int64_int64(reinterpret_cast<int64>(&context), 101, 111), 0);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();

  EXPECT_EQ(div_float64_float64(reinterpret_cast<int64>(&context), 1010.1010, 2.1),
            481.0);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();

  EXPECT_EQ(div_float64_float64(reinterpret_cast<int64>(&context), 1010.1010, 0.00000),
            0.0);
  EXPECT_EQ(context.has_error(), true);
  EXPECT_EQ(context.get_error(), "divide by zero error");
  context.Reset();

  EXPECT_EQ(div_float32_float32(reinterpret_cast<int64>(&context), 1010.1010f, 2.1f),
            481.0f);
  EXPECT_EQ(context.has_error(), false);
  context.Reset();
}

}  // namespace gandiva

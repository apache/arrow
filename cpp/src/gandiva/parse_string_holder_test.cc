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

#include "gandiva/parse_string_holder.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace gandiva {

class TestParseStringHolder : public ::testing::Test {};

template <typename Holder, typename C_TYPE>
void AssertConversion(Holder& h, ExecutionContext* ctx, const std::string& s,
                      C_TYPE expected) {
  EXPECT_EQ(h(ctx, s.data(), static_cast<int>(s.length())), expected);
  EXPECT_FALSE(ctx->has_error());
}

template <typename Holder>
void AssertConversionFails(Holder& h, ExecutionContext* ctx, const std::string& s) {
  h(ctx, s.data(), static_cast<int>(s.length()));
  EXPECT_THAT(ctx->get_error(),
              ::testing::HasSubstr("Failed parsing the string to required format"));
  ctx->Reset();
}

TEST_F(TestParseStringHolder, TestCastINT) {
  gandiva::ExecutionContext ctx;
  std::shared_ptr<ParseStringHolder<arrow::Int32Type>> parse_string_holder;

  auto status = ParseStringHolder<arrow::Int32Type>::Make(&parse_string_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& parse_string = *parse_string_holder;

  AssertConversion(parse_string, &ctx, "-45", -45);
  AssertConversion(parse_string, &ctx, "0", 0);
  AssertConversion(parse_string, &ctx, "2147483647", 2147483647);
  AssertConversion(parse_string, &ctx, "02147483647", 2147483647);
  AssertConversion(parse_string, &ctx, "-2147483648", -2147483648LL);
  AssertConversion(parse_string, &ctx, "-02147483648", -2147483648LL);

  AssertConversionFails(parse_string, &ctx, "2147483648");
  AssertConversionFails(parse_string, &ctx, "-2147483649");

  AssertConversionFails(parse_string, &ctx, "12.34");
  AssertConversionFails(parse_string, &ctx, "abc");
  AssertConversionFails(parse_string, &ctx, "");
  AssertConversionFails(parse_string, &ctx, "-");
}

TEST_F(TestParseStringHolder, TestCastBIGINT) {
  gandiva::ExecutionContext ctx;
  std::shared_ptr<ParseStringHolder<arrow::Int64Type>> parse_string_holder;

  auto status = ParseStringHolder<arrow::Int64Type>::Make(&parse_string_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& parse_string = *parse_string_holder;

  AssertConversion(parse_string, &ctx, "-45", -45);
  AssertConversion(parse_string, &ctx, "0", 0);
  AssertConversion(parse_string, &ctx, "9223372036854775807", 9223372036854775807LL);
  AssertConversion(parse_string, &ctx, "09223372036854775807", 9223372036854775807LL);
  AssertConversion(parse_string, &ctx, "-9223372036854775808",
                   -9223372036854775807LL - 1);
  AssertConversion(parse_string, &ctx, "-009223372036854775808",
                   -9223372036854775807LL - 1);

  AssertConversionFails(parse_string, &ctx, "9223372036854775808");
  AssertConversionFails(parse_string, &ctx, "-9223372036854775809");

  AssertConversionFails(parse_string, &ctx, "12.34");
  AssertConversionFails(parse_string, &ctx, "abc");
  AssertConversionFails(parse_string, &ctx, "");
  AssertConversionFails(parse_string, &ctx, "-");
}

TEST_F(TestParseStringHolder, TestCastFloat4) {
  gandiva::ExecutionContext ctx;
  std::shared_ptr<ParseStringHolder<arrow::FloatType>> parse_string_holder;

  auto status = ParseStringHolder<arrow::FloatType>::Make(&parse_string_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& parse_string = *parse_string_holder;

  AssertConversion(parse_string, &ctx, "-45.34", -45.34f);
  AssertConversion(parse_string, &ctx, "0", 0.0f);
  AssertConversion(parse_string, &ctx, "5", 5.0f);

  AssertConversionFails(parse_string, &ctx, "");
  AssertConversionFails(parse_string, &ctx, "e");
}

TEST_F(TestParseStringHolder, TestCastFloat8) {
  gandiva::ExecutionContext ctx;
  std::shared_ptr<ParseStringHolder<arrow::DoubleType>> parse_string_holder;

  auto status = ParseStringHolder<arrow::DoubleType>::Make(&parse_string_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& parse_string = *parse_string_holder;

  AssertConversion(parse_string, &ctx, "-45.34", -45.34);
  AssertConversion(parse_string, &ctx, "0", 0.0);
  AssertConversion(parse_string, &ctx, "5", 5.0);

  AssertConversionFails(parse_string, &ctx, "");
  AssertConversionFails(parse_string, &ctx, "e");
}
}  // namespace gandiva

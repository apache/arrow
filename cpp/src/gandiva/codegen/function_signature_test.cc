/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <memory>
#include "codegen/function_signature.h"

namespace gandiva {

class TestFunctionSignature : public ::testing::Test {
 protected:
  virtual void SetUp() {
    local_i32_type_ = std::make_shared<arrow::Int32Type>();
    local_i64_type_ = std::make_shared<arrow::Int64Type>();
    local_date32_type_ = std::make_shared<arrow::Date32Type>();
  }

  virtual void TearDown() {
    local_i32_type_.reset();
    local_i64_type_.reset();
    local_date32_type_.reset();
  }

  // virtual void TearDown() {}
  DataTypePtr local_i32_type_;
  DataTypePtr local_i64_type_;
  DataTypePtr local_date32_type_;
};

TEST_F(TestFunctionSignature, TestToString) {
  EXPECT_EQ(FunctionSignature("myfunc",
                              {arrow::int32(), arrow::float32()},
                              arrow::float64()).ToString(),
    "double myfunc(int32, float)");
}

TEST_F(TestFunctionSignature, TestEqualsName) {
  EXPECT_EQ(FunctionSignature("add", {arrow::int32()}, arrow::int32()),
            FunctionSignature("add", {arrow::int32()}, arrow::int32()));

  EXPECT_EQ(FunctionSignature("add", {arrow::int32()}, arrow::int64()),
            FunctionSignature("add", {local_i32_type_}, local_i64_type_));

  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
               FunctionSignature("sub", {arrow::int32()}, arrow::int32()));
}


TEST_F(TestFunctionSignature, TestEqualsParamCount) {
  EXPECT_FALSE(FunctionSignature("add", {arrow::int32(), arrow::int32()},
                                 arrow::int32()) ==
               FunctionSignature("add", {arrow::int32()}, arrow::int32()));
}

TEST_F(TestFunctionSignature, TestEqualsParamValue) {
  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
               FunctionSignature("add", {arrow::int64()}, arrow::int32()));

  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int32()) ==
               FunctionSignature("add", {arrow::float32(), arrow::float32()},
                                 arrow::int32()));

  EXPECT_FALSE(FunctionSignature("add", {arrow::int32(), arrow::int64()},
                                 arrow::int32()) ==
               FunctionSignature("add", {arrow::int64(), arrow::int32()},
                                 arrow::int32()));

  EXPECT_EQ(FunctionSignature("extract_month", {arrow::date32()}, arrow::int64()),
            FunctionSignature("extract_month", {local_date32_type_}, local_i64_type_));

  EXPECT_FALSE(FunctionSignature("extract_month", {arrow::date32()}, arrow::int64()) ==
               FunctionSignature("extract_month", {arrow::date64()}, arrow::date32()));
}

TEST_F(TestFunctionSignature, TestEqualsReturn) {
  EXPECT_FALSE(FunctionSignature("add", {arrow::int32()}, arrow::int64()) ==
               FunctionSignature("add", {arrow::int32()}, arrow::int32()));
}

TEST_F(TestFunctionSignature, TestHash) {
  FunctionSignature f1("add", {arrow::int32(), arrow::int32()}, arrow::int64());
  FunctionSignature f2("add", {local_i32_type_, local_i32_type_}, local_i64_type_);
  EXPECT_EQ(f1.Hash(), f2.Hash());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva

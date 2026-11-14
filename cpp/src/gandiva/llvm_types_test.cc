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

#include "gandiva/llvm_types.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestLLVMTypes : public ::testing::Test {
 protected:
  virtual void SetUp() { types_ = new LLVMTypes(context_); }
  virtual void TearDown() { delete types_; }

  llvm::LLVMContext context_;
  LLVMTypes* types_;
};

TEST_F(TestLLVMTypes, TestFound) {
  EXPECT_EQ(types_->IRType(arrow::Type::BOOL), types_->i1_type());
  EXPECT_EQ(types_->IRType(arrow::Type::INT32), types_->i32_type());
  EXPECT_EQ(types_->IRType(arrow::Type::INT64), types_->i64_type());
  EXPECT_EQ(types_->IRType(arrow::Type::FLOAT), types_->float_type());
  EXPECT_EQ(types_->IRType(arrow::Type::DOUBLE), types_->double_type());
  EXPECT_EQ(types_->IRType(arrow::Type::DATE64), types_->i64_type());
  EXPECT_EQ(types_->IRType(arrow::Type::TIME64), types_->i64_type());
  EXPECT_EQ(types_->IRType(arrow::Type::TIMESTAMP), types_->i64_type());

  EXPECT_EQ(types_->DataVecType(arrow::boolean()), types_->i1_type());
  EXPECT_EQ(types_->DataVecType(arrow::int32()), types_->i32_type());
  EXPECT_EQ(types_->DataVecType(arrow::int64()), types_->i64_type());
  EXPECT_EQ(types_->DataVecType(arrow::float32()), types_->float_type());
  EXPECT_EQ(types_->DataVecType(arrow::float64()), types_->double_type());
  EXPECT_EQ(types_->DataVecType(arrow::date64()), types_->i64_type());
  EXPECT_EQ(types_->DataVecType(arrow::time64(arrow::TimeUnit::MICRO)),
            types_->i64_type());
  EXPECT_EQ(types_->DataVecType(arrow::timestamp(arrow::TimeUnit::MILLI)),
            types_->i64_type());
}

TEST_F(TestLLVMTypes, TestNotFound) {
  EXPECT_EQ(types_->IRType(arrow::Type::SPARSE_UNION), nullptr);
  EXPECT_EQ(types_->IRType(arrow::Type::DENSE_UNION), nullptr);
  EXPECT_EQ(types_->DataVecType(arrow::null()), nullptr);
}

TEST_F(TestLLVMTypes, SignedIntegerConstants) {
  auto value = [](llvm::Constant* constant) -> const llvm::APInt& {
    return llvm::cast<llvm::ConstantInt>(constant)->getValue();
  };
  EXPECT_EQ(255U, value(types_->i8_constant(-1)).getZExtValue());
  EXPECT_EQ(32768U, value(types_->i16_constant(-32768)).getZExtValue());
  EXPECT_EQ(static_cast<uint32_t>(-447906508),
            value(types_->i32_constant(-447906508)).getZExtValue());
  EXPECT_EQ(int64_t{-447906508}, value(types_->i32_constant(-447906508)).getSExtValue());
  EXPECT_TRUE(value(types_->i64_constant(-1)).isAllOnes());
  EXPECT_TRUE(value(types_->i128_constant(-1)).isAllOnes());
  EXPECT_EQ(int64_t{-447906508}, value(types_->i128_constant(-447906508)).getSExtValue());
  EXPECT_EQ(0U, value(types_->i128_constant(1)).getHiBits(64).getZExtValue());
  EXPECT_TRUE(value(types_->true_constant()).isOne());
  EXPECT_TRUE(value(types_->false_constant()).isZero());
}

}  // namespace gandiva

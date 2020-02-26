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
//

#include "arrow/engine/expression.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/type.h"

namespace arrow {
namespace engine {

class ExprTest : public testing::Test {};

TEST_F(ExprTest, ExprType) {
  auto i32 = int32();
  auto s = schema({field("i32", i32)});

  auto scalar = ExprType::Scalar(i32);
  EXPECT_EQ(scalar.shape(), ExprType::Shape::SCALAR);
  EXPECT_TRUE(scalar.data_type()->Equals(i32));
  EXPECT_EQ(scalar.schema(), nullptr);

  auto array = ExprType::Array(i32);
  EXPECT_EQ(array.shape(), ExprType::Shape::ARRAY);
  EXPECT_TRUE(array.data_type()->Equals(i32));
  EXPECT_EQ(array.schema(), nullptr);

  auto table = ExprType::Table(s);
  EXPECT_EQ(table.shape(), ExprType::Shape::TABLE);
  EXPECT_EQ(table.data_type(), nullptr);
  EXPECT_TRUE(table.schema()->Equals(s));
}

TEST_F(ExprTest, ScalarExpr) {
  ASSERT_RAISES(Invalid, ScalarExpr::Make(nullptr));

  auto i32 = int32();
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(i32, 10));
  ASSERT_OK_AND_ASSIGN(auto expr, ScalarExpr::Make(value));
  EXPECT_EQ(expr->type(), ExprType::Scalar(i32));
  EXPECT_EQ(*expr->scalar(), *value);
}

TEST_F(ExprTest, FieldRefExpr) {
  ASSERT_RAISES(Invalid, FieldRefExpr::Make(nullptr));

  auto i32 = int32();
  auto f = field("i32", i32);

  ASSERT_OK_AND_ASSIGN(auto expr, FieldRefExpr::Make(f));
  EXPECT_EQ(expr->type(), ExprType::Scalar(i32));
  EXPECT_TRUE(expr->field()->Equals(f));
}

}  // namespace engine
}  // namespace arrow

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

#include "arrow/engine/expression.h"
#include "arrow/scalar.h"
#include "arrow/testing/gmock.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/type.h"

using testing::HasSubstr;
using testing::Not;
using testing::Pointee;

namespace arrow {
namespace engine {

class ExprTypeTest : public testing::Test {};

TEST_F(ExprTypeTest, Basic) {
  auto i32 = int32();
  auto s = schema({field("i32", i32)});

  auto scalar = ExprType::Scalar(i32);
  EXPECT_EQ(scalar.shape(), ExprType::Shape::SCALAR);
  EXPECT_TRUE(scalar.data_type()->Equals(i32));
  EXPECT_EQ(scalar.schema(), nullptr);
  EXPECT_TRUE(scalar.IsScalar());
  EXPECT_FALSE(scalar.IsArray());
  EXPECT_FALSE(scalar.IsTable());

  auto array = ExprType::Array(i32);
  EXPECT_EQ(array.shape(), ExprType::Shape::ARRAY);
  EXPECT_TRUE(array.data_type()->Equals(i32));
  EXPECT_EQ(array.schema(), nullptr);
  EXPECT_FALSE(array.IsScalar());
  EXPECT_TRUE(array.IsArray());
  EXPECT_FALSE(array.IsTable());

  auto table = ExprType::Table(s);
  EXPECT_EQ(table.shape(), ExprType::Shape::TABLE);
  EXPECT_EQ(table.data_type(), nullptr);
  EXPECT_TRUE(table.schema()->Equals(s));
  EXPECT_FALSE(table.IsScalar());
  EXPECT_FALSE(table.IsArray());
  EXPECT_TRUE(table.IsTable());
}

TEST_F(ExprTypeTest, IsPredicate) {
  auto bool_scalar = ExprType::Scalar(boolean());
  EXPECT_TRUE(bool_scalar.IsPredicate());

  auto bool_array = ExprType::Array(boolean());
  EXPECT_FALSE(bool_array.IsPredicate());

  auto bool_table = ExprType::Table(schema({field("b", boolean())}));
  EXPECT_FALSE(bool_table.IsPredicate());

  auto i32_scalar = ExprType::Scalar(int32());
  EXPECT_FALSE(i32_scalar.IsPredicate());
}

class ExprTest : public testing::Test {};

TEST_F(ExprTest, ScalarExpr) {
  ASSERT_RAISES(Invalid, ScalarExpr::Make(nullptr));

  auto i32 = int32();
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(i32, 10));
  ASSERT_OK_AND_ASSIGN(auto expr, ScalarExpr::Make(value));
  EXPECT_EQ(expr->kind(), Expr::SCALAR_LITERAL);
  EXPECT_EQ(expr->type(), ExprType::Scalar(i32));
  EXPECT_EQ(*expr->scalar(), *value);
}

TEST_F(ExprTest, FieldRefExpr) {
  ASSERT_RAISES(Invalid, FieldRefExpr::Make(nullptr));

  auto i32 = int32();
  auto f_i32 = field("i32", i32);

  ASSERT_OK_AND_ASSIGN(auto expr, FieldRefExpr::Make(f_i32));
  EXPECT_EQ(expr->kind(), Expr::FIELD_REFERENCE);
  EXPECT_EQ(expr->type(), ExprType::Scalar(i32));
  EXPECT_THAT(expr->field(), IsPtrEqual(f_i32));
}

template <typename CmpClass>
class CmpExprTest : public ExprTest {
 public:
  Expr::Kind kind() { return expr_traits<CmpClass>::kind_id; }

  Result<std::shared_ptr<CmpClass>> Make(std::shared_ptr<Expr> left,
                                         std::shared_ptr<Expr> right) {
    return CmpClass::Make(std::move(left), std::move(right));
  }
};

using CompareExprs = ::testing::Types<EqualCmpExpr, NotEqualCmpExpr>;

TYPED_TEST_CASE(CmpExprTest, CompareExprs);
TYPED_TEST(CmpExprTest, BasicCompareExpr) {
  auto i32 = int32();

  auto f_i32 = field("i32", i32);
  ASSERT_OK_AND_ASSIGN(auto f_expr, FieldRefExpr::Make(f_i32));

  ASSERT_OK_AND_ASSIGN(auto s_i32, MakeScalar(i32, 42));
  ASSERT_OK_AND_ASSIGN(auto s_expr, ScalarExpr::Make(s_i32));

  // Required fields
  ASSERT_RAISES(Invalid, this->Make(nullptr, nullptr));
  ASSERT_RAISES(Invalid, this->Make(s_expr, nullptr));
  ASSERT_RAISES(Invalid, this->Make(nullptr, f_expr));

  // Not type compatible
  ASSERT_OK_AND_ASSIGN(auto s_i64, MakeScalar(int64(), 42L));
  ASSERT_OK_AND_ASSIGN(auto s_expr_i64, ScalarExpr::Make(s_i64));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("operands must be of same type"),
                                  this->Make(s_expr_i64, f_expr));

  ASSERT_OK_AND_ASSIGN(auto expr, this->Make(f_expr, s_expr));
  EXPECT_EQ(expr->kind(), this->kind());
  EXPECT_EQ(expr->type(), ExprType::Scalar(boolean()));
  EXPECT_TRUE(expr->type().IsPredicate());
  EXPECT_THAT(expr, IsPtrEqual(expr));
  EXPECT_THAT(expr->left_operand(), IsPtrEqual(f_expr));
  EXPECT_THAT(expr->right_operand(), IsPtrEqual(s_expr));

  ASSERT_OK_AND_ASSIGN(auto other, this->Make(f_expr, s_expr));
  EXPECT_THAT(expr, IsPtrEqual(other));
  // Compare operators supports commutativity
  // TODO(fsaintjacques): what about floating point types?
  ASSERT_OK_AND_ASSIGN(auto swapped, this->Make(s_expr, f_expr));
  EXPECT_THAT(expr, IsPtrEqual(swapped));
}

class RelExprTest : public ExprTest {
 protected:
  void SetUp() override {
    CatalogBuilder builder;
    ASSERT_OK(builder.Add(table_1, MockTable(schema_1)));
    ASSERT_OK_AND_ASSIGN(catalog, builder.Finish());
  }

  std::string table_1 = "table_1";
  std::shared_ptr<Schema> schema_1 = schema({field("i32", int32())});

  std::shared_ptr<Catalog> catalog;
};

TEST_F(RelExprTest, EmptyRelExpr) {
  ASSERT_RAISES(Invalid, EmptyRelExpr::Make(nullptr));

  ASSERT_OK_AND_ASSIGN(auto empty, EmptyRelExpr::Make(schema_1));
  EXPECT_THAT(empty->type(), ExprType::Table(schema_1));
  EXPECT_THAT(empty->schema(), IsPtrEqual(schema_1));
  EXPECT_THAT(empty, IsPtrEqual(empty));

  ASSERT_OK_AND_ASSIGN(auto other, EmptyRelExpr::Make(schema_1));
  EXPECT_THAT(other, IsPtrEqual(empty));
}

TEST_F(RelExprTest, ScanRelExpr) {
  ASSERT_OK_AND_ASSIGN(auto table, catalog->Get(table_1));

  ASSERT_OK_AND_ASSIGN(auto scan, ScanRelExpr::Make(table));
  EXPECT_THAT(scan, IsPtrEqual(scan));
  EXPECT_THAT(scan->type(), ExprType::Table(schema_1));
  EXPECT_THAT(scan->schema(), IsPtrEqual(schema_1));

  ASSERT_OK_AND_ASSIGN(auto other, ScanRelExpr::Make(table));
  EXPECT_THAT(other, IsPtrEqual(scan));
}

TEST_F(RelExprTest, FilterRelExpr) {
  ASSERT_OK_AND_ASSIGN(auto empty, EmptyRelExpr::Make(schema_1));
  ASSERT_OK_AND_ASSIGN(auto pred, ScalarExpr::Make(MakeScalar(true)));

  ASSERT_RAISES(Invalid, FilterRelExpr::Make(nullptr, nullptr));
  ASSERT_RAISES(Invalid, FilterRelExpr::Make(empty, nullptr));
  ASSERT_RAISES(Invalid, FilterRelExpr::Make(nullptr, pred));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("input must be a table"),
                                  FilterRelExpr::Make(pred, pred));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("predicate must be a predicate"),
                                  FilterRelExpr::Make(empty, empty));

  ASSERT_OK_AND_ASSIGN(auto filter, FilterRelExpr::Make(empty, pred));
  EXPECT_THAT(filter, IsPtrEqual(filter));
  EXPECT_THAT(filter->type(), ExprType::Table(schema_1));
  EXPECT_THAT(filter->schema(), IsPtrEqual(schema_1));
  EXPECT_THAT(filter->operand(), IsPtrEqual(empty));
  EXPECT_THAT(filter->predicate(), IsPtrEqual(pred));
}

}  // namespace engine
}  // namespace arrow

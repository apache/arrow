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

#include "arrow/engine/catalog.h"
#include "arrow/engine/expression.h"
#include "arrow/engine/logical_plan.h"
#include "arrow/testing/gtest_common.h"

using testing::HasSubstr;

namespace arrow {
namespace engine {

using ResultExpr = LogicalPlanBuilder::ResultExpr;

class LogicalPlanBuilderTest : public testing::Test {
 protected:
  void SetUp() override {
    CatalogBuilder catalog_builder;
    ASSERT_OK(catalog_builder.Add(table_1, MockTable(schema_1)));
    ASSERT_OK_AND_ASSIGN(options.catalog, catalog_builder.Finish());
    builder = LogicalPlanBuilder{options};
  }

  ResultExpr scalar_expr() {
    auto forthy_two = MakeScalar(42);
    return builder.Scalar(forthy_two);
  }

  ResultExpr scan_expr() { return builder.Scan(table_1); }

  template <typename T>
  ResultExpr field_expr(T key, std::shared_ptr<Expr> input = nullptr) {
    if (input == nullptr) {
      ARROW_ASSIGN_OR_RAISE(input, scan_expr());
    }
    return builder.Field(input, key);
  }

  ResultExpr predicate_expr() { return nullptr; }

  std::string table_1 = "table_1";
  std::shared_ptr<Schema> schema_1 = schema({
      field("bool", boolean()),
      field("i32", int32()),
      field("u64", uint64()),
      field("f32", float32()),
      field("utf8", utf8()),
  });
  LogicalPlanBuilderOptions options{};
  LogicalPlanBuilder builder{};
};

TEST_F(LogicalPlanBuilderTest, Scalar) {
  auto forthy_two = MakeScalar(42);
  EXPECT_OK_AND_ASSIGN(auto scalar, builder.Scalar(forthy_two));
  ASSERT_TRUE(IsA<ScalarExpr>(scalar));
}

TEST_F(LogicalPlanBuilderTest, FieldReferences) {
  ASSERT_RAISES(Invalid, builder.Field(nullptr, "i32"));
  ASSERT_RAISES(Invalid, builder.Field(nullptr, 0));

  // Can't lookup a scalar
  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());
  ASSERT_RAISES(Invalid, builder.Field(scalar, "i32"));

  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());
  ASSERT_RAISES(KeyError, builder.Field(table, ""));
  ASSERT_RAISES(KeyError, builder.Field(table, -1));
  ASSERT_RAISES(KeyError, builder.Field(table, 9000));

  EXPECT_OK_AND_ASSIGN(auto field_name_ref, builder.Field(table, "i32"));
  ASSERT_TRUE(IsA<FieldRefExpr>(field_name_ref));

  EXPECT_OK_AND_ASSIGN(auto field_idx_ref, builder.Field(table, 0));
  ASSERT_TRUE(IsA<FieldRefExpr>(field_idx_ref));
}

TEST_F(LogicalPlanBuilderTest, BasicScan) {
  ASSERT_RAISES(KeyError, builder.Scan(""));
  ASSERT_RAISES(KeyError, builder.Scan("not_found"));

  EXPECT_OK_AND_ASSIGN(auto scan, builder.Scan(table_1));
  ASSERT_TRUE(IsA<ScanRelExpr>(scan));
}

TEST_F(LogicalPlanBuilderTest, Comparisons) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());
  EXPECT_OK_AND_ASSIGN(auto field, field_expr("i32", table));
  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());

  EXPECT_OK_AND_ASSIGN(auto eq, builder.Equal(field, scalar));
  ASSERT_TRUE(IsA<EqualExpr>(eq));

  EXPECT_OK_AND_ASSIGN(auto ne, builder.NotEqual(field, scalar));
  ASSERT_TRUE(IsA<NotEqualExpr>(ne));

  EXPECT_OK_AND_ASSIGN(auto gt, builder.GreaterThan(field, scalar));
  ASSERT_TRUE(IsA<GreaterThanExpr>(gt));

  EXPECT_OK_AND_ASSIGN(auto ge, builder.GreaterThanEqual(field, scalar));
  ASSERT_TRUE(IsA<GreaterThanEqualExpr>(ge));

  EXPECT_OK_AND_ASSIGN(auto lt, builder.LessThan(field, scalar));
  ASSERT_TRUE(IsA<LessThanExpr>(lt));

  EXPECT_OK_AND_ASSIGN(auto le, builder.LessThanEqual(field, scalar));
  ASSERT_TRUE(IsA<LessThanEqualExpr>(le));
}

TEST_F(LogicalPlanBuilderTest, Count) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());
  EXPECT_OK_AND_ASSIGN(auto field, field_expr("i32", table));

  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());
  EXPECT_OK_AND_ASSIGN(auto s_count, builder.Count(scalar));
  ASSERT_TRUE(IsA<CountExpr>(s_count));

  EXPECT_OK_AND_ASSIGN(auto f_count, builder.Count(field));
  ASSERT_TRUE(IsA<CountExpr>(f_count));

  EXPECT_OK_AND_ASSIGN(auto t_count, builder.Count(table));
  ASSERT_TRUE(IsA<CountExpr>(t_count));
}

TEST_F(LogicalPlanBuilderTest, Sum) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());

  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());
  EXPECT_OK_AND_ASSIGN(auto s_sum, builder.Sum(scalar));
  ASSERT_TRUE(IsA<SumExpr>(s_sum));

  EXPECT_OK_AND_ASSIGN(auto i32_field, field_expr("i32", table));
  EXPECT_OK_AND_ASSIGN(auto f_sum, builder.Sum(i32_field));
  ASSERT_TRUE(IsA<SumExpr>(s_sum));

  EXPECT_OK_AND_ASSIGN(auto str_field, field_expr("utf8", table));
  ASSERT_RAISES(Invalid, builder.Sum(str_field));
  ASSERT_RAISES(Invalid, builder.Sum(table));
}

TEST_F(LogicalPlanBuilderTest, Filter) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());

  EXPECT_OK_AND_ASSIGN(auto field, field_expr("i32", table));
  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());
  EXPECT_OK_AND_ASSIGN(auto predicate, EqualExpr::Make(field, scalar));

  EXPECT_OK_AND_ASSIGN(auto filter, builder.Filter(table, predicate));
  ASSERT_TRUE(IsA<FilterRelExpr>(filter));
}

TEST_F(LogicalPlanBuilderTest, ProjectionByNamesAndIndices) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());

  std::vector<std::string> no_names{};
  ASSERT_RAISES(Invalid, builder.Project(table, no_names));
  std::vector<std::string> invalid_names{"u64", "nope"};
  ASSERT_RAISES(KeyError, builder.Project(table, invalid_names));
  std::vector<int> invalid_idx{42, 0};
  ASSERT_RAISES(KeyError, builder.Project(table, invalid_idx));

  std::vector<std::string> valid_names{"u64", "f32"};
  ASSERT_OK(builder.Project(table, valid_names));
  std::vector<int> valid_idx{3, 1, 1};
  EXPECT_OK_AND_ASSIGN(auto project, builder.Project(table, valid_idx));
  ASSERT_TRUE(IsA<ProjectionRelExpr>(project));
}

}  // namespace engine
}  // namespace arrow

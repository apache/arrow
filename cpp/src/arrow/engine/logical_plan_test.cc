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
      field("f32", uint32()),
  });
  LogicalPlanBuilderOptions options{};
  LogicalPlanBuilder builder{};
};

TEST_F(LogicalPlanBuilderTest, Scalar) {
  auto forthy_two = MakeScalar(42);
  EXPECT_OK_AND_ASSIGN(auto scalar, builder.Scalar(forthy_two));
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
  EXPECT_OK_AND_ASSIGN(auto field_idx_ref, builder.Field(table, 0));
}

TEST_F(LogicalPlanBuilderTest, BasicScan) {
  LogicalPlanBuilder builder{options};
  ASSERT_RAISES(KeyError, builder.Scan(""));
  ASSERT_RAISES(KeyError, builder.Scan("not_found"));
  ASSERT_OK(builder.Scan(table_1));
}

TEST_F(LogicalPlanBuilderTest, Filter) {
  EXPECT_OK_AND_ASSIGN(auto table, scan_expr());

  EXPECT_OK_AND_ASSIGN(auto field, field_expr("i32", table));
  EXPECT_OK_AND_ASSIGN(auto scalar, scalar_expr());
  EXPECT_OK_AND_ASSIGN(auto predicate, EqualCmpExpr::Make(field, scalar));

  EXPECT_OK_AND_ASSIGN(auto filter, builder.Filter(table, predicate));
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
  ASSERT_OK(builder.Project(table, valid_idx));
}

}  // namespace engine
}  // namespace arrow

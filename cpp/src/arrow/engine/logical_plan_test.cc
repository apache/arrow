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
#include "arrow/engine/logical_plan.h"
#include "arrow/testing/gtest_common.h"

using testing::HasSubstr;

namespace arrow {
namespace engine {

class LogicalPlanBuilderTest : public testing::Test {
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

TEST_F(LogicalPlanBuilderTest, Scalar) {
  LogicalPlanBuilder builder{{catalog}};
  auto forthy_two = MakeScalar(42);
  EXPECT_OK_AND_ASSIGN(auto scalar, builder.Scalar(forthy_two));
}

TEST_F(LogicalPlanBuilderTest, BasicScan) {
  LogicalPlanBuilder builder{{catalog}};
  ASSERT_OK(builder.Scan(table_1));
}

TEST_F(LogicalPlanBuilderTest, FieldReferenceByName) {
  LogicalPlanBuilder builder{{catalog}};

  // Input must be non-null.
  ASSERT_RAISES(Invalid, builder.Field(nullptr, "i32"));

  // The input must have a Table shape.
  auto forthy_two = MakeScalar(42);
  EXPECT_OK_AND_ASSIGN(auto scalar, builder.Scalar(forthy_two));
  ASSERT_RAISES(Invalid, builder.Field(scalar, "not_found"));

  EXPECT_OK_AND_ASSIGN(auto table_scan, builder.Scan(table_1));
  EXPECT_OK_AND_ASSIGN(auto field_ref, builder.Field(table_scan, "i32"));
}

}  // namespace engine
}  // namespace arrow

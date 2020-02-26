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

TEST_F(LogicalPlanBuilderTest, BasicScan) {
  LogicalPlanBuilder builder(catalog);
  ASSERT_OK(builder.Scan(table_1));
  ASSERT_OK_AND_ASSIGN(auto plan, builder.Finish());
}

using testing::HasSubstr;

TEST_F(LogicalPlanBuilderTest, ErrorEmptyFinish) {
  LogicalPlanBuilder builder(catalog);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("LogicalPlan is empty"),
                                  builder.Finish());
}

TEST_F(LogicalPlanBuilderTest, ErrorOperatorsLeftOnStack) {
  LogicalPlanBuilder builder(catalog);
  ASSERT_OK(builder.Scan(table_1));
  ASSERT_OK(builder.Scan(table_1));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("LogicalPlan is ignoring operators"),
                                  builder.Finish());
}

}  // namespace engine
}  // namespace arrow

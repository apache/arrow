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
#include <math.h>
#include <time.h>
#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float64;
using arrow::int32;
using arrow::int64;

class TestToString : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

#define CHECK_EXPR_TO_STRING(e, str) EXPECT_STREQ(e->ToString().c_str(), str)

TEST_F(TestToString, TestAll) {
  auto literal_node = TreeExprBuilder::MakeLiteral((uint64_t)100);
  auto literal_expr =
      TreeExprBuilder::MakeExpression(literal_node, arrow::field("r", int64()));
  CHECK_EXPR_TO_STRING(literal_expr, "(uint64) 100");

  auto f0 = arrow::field("f0", float64());
  auto f0_node = TreeExprBuilder::MakeField(f0);
  auto f0_expr = TreeExprBuilder::MakeExpression(f0_node, f0);
  CHECK_EXPR_TO_STRING(f0_expr, "double");

  auto f1 = arrow::field("f1", int64());
  auto f2 = arrow::field("f2", int64());
  auto f1_node = TreeExprBuilder::MakeField(f1);
  auto f2_node = TreeExprBuilder::MakeField(f2);
  auto add_node = TreeExprBuilder::MakeFunction("add", {f1_node, f2_node}, int64());
  auto add_expr = TreeExprBuilder::MakeExpression(add_node, f1);
  CHECK_EXPR_TO_STRING(add_expr, "int64 add(int64, int64)");

  auto cond_node = TreeExprBuilder::MakeFunction(
      "lesser_than", {f0_node, TreeExprBuilder::MakeLiteral(static_cast<float>(0))},
      boolean());
  auto then_node = TreeExprBuilder::MakeField(f1);
  auto else_node = TreeExprBuilder::MakeField(f2);

  auto if_node = TreeExprBuilder::MakeIf(cond_node, then_node, else_node, int64());
  auto if_expr = TreeExprBuilder::MakeExpression(if_node, f1);
  CHECK_EXPR_TO_STRING(
      if_expr,
      "if (bool lesser_than(double, (float) 0 raw(0))) { int64 } else { int64 }");

  auto f1_gt_100 =
      TreeExprBuilder::MakeFunction("greater_than", {f1_node, literal_node}, boolean());
  auto f2_equals_100 =
      TreeExprBuilder::MakeFunction("equals", {f2_node, literal_node}, boolean());
  auto and_node = TreeExprBuilder::MakeAnd({f1_gt_100, f2_equals_100});
  auto and_expr =
      TreeExprBuilder::MakeExpression(and_node, arrow::field("f0", boolean()));
  CHECK_EXPR_TO_STRING(
      and_expr,
      "bool greater_than(int64, (uint64) 100) && bool equals(int64, (uint64) 100)");
}

}  // namespace gandiva

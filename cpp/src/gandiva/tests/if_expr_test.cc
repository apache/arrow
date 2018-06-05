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
#include "arrow/memory_pool.h"
#include "integ/test_util.h"
#include "gandiva/projector.h"
#include "gandiva/status.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::float32;
using arrow::boolean;

class TestIfExpr : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestIfExpr, TestSimple) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a > b)
  //   a
  // else
  //   b
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto condition = TreeExprBuilder::MakeFunction("greater_than",
                                                 {node_a, node_b},
                                                 boolean());
  auto if_node = TreeExprBuilder::MakeIf(condition, node_a, node_b, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({10, 12, -20, 5}, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({5, 15, 15, 17}, { true, true, true, true });

  // expected output
  auto exp = MakeArrowArrayInt32({10, 15, 15, 17 }, { true, true, true, true });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestIfExpr, TestSimpleArithmetic) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a > b)
  //   a + b
  // else
  //   a - b
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto condition = TreeExprBuilder::MakeFunction("greater_than",
                                                 {node_a, node_b},
                                                 boolean());
  auto sum = TreeExprBuilder::MakeFunction("add", {node_a, node_b}, int32());
  auto sub = TreeExprBuilder::MakeFunction("subtract", {node_a, node_b}, int32());
  auto if_node = TreeExprBuilder::MakeIf(condition, sum, sub, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({10, 12, -20, 5}, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({5, 15, 15, 17}, { true, true, true, true });

  // expected output
  auto exp = MakeArrowArrayInt32({15, -3, -35, 0 }, { true, true, true, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestIfExpr, TestNestedIf) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a > b)
  //   a + b
  // else if (a < b)
  //   a - b
  // else
  //   a * b
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto condition_gt = TreeExprBuilder::MakeFunction("greater_than",
                                                    {node_a, node_b},
                                                    boolean());
  auto condition_lt = TreeExprBuilder::MakeFunction("less_than",
                                                    {node_a, node_b},
                                                    boolean());
  auto sum = TreeExprBuilder::MakeFunction("add", {node_a, node_b}, int32());
  auto sub = TreeExprBuilder::MakeFunction("subtract", {node_a, node_b}, int32());
  auto mult = TreeExprBuilder::MakeFunction("multiply", {node_a, node_b}, int32());
  auto else_node = TreeExprBuilder::MakeIf(condition_lt, sub, mult, int32());
  auto if_node = TreeExprBuilder::MakeIf(condition_gt, sum, else_node, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({10, 12, 15, 5}, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({5, 15, 15, 17}, { true, true, true, true });

  // expected output
  auto exp = MakeArrowArrayInt32({15, -3, 225, 0 }, { true, true, true, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestIfExpr, TestBigNestedIf) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto schema = arrow::schema({fielda});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a < 10)
  //   10
  // else if (a < 20)
  //   20
  // ..
  // ..
  // else if (a < 190)
  //   190
  // else
  //   200
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto top_node = TreeExprBuilder::MakeLiteral(200);
  for (int thresh = 190; thresh > 0; thresh -= 10) {
    auto literal = TreeExprBuilder::MakeLiteral(thresh);
    auto condition = TreeExprBuilder::MakeFunction("less_than",
                                                   {node_a, literal},
                                                   boolean());
    auto if_node = TreeExprBuilder::MakeIf(condition, literal, top_node, int32());
    top_node = if_node;
  }
  auto expr = TreeExprBuilder::MakeExpression(top_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({10, 102, 158, 302}, { true, true, true, true });

  // expected output
  auto exp = MakeArrowArrayInt32({20, 110, 160, 200}, { true, true, true, true });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

} // namespace gandiva

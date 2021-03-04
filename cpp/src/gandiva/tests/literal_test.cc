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
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
using arrow::int32;
using arrow::int64;

class TestLiteral : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestLiteral, TestSimpleArithmetic) {
  // schema for input fields
  auto field_a = field("a", boolean());
  auto field_b = field("b", int32());
  auto field_c = field("c", int64());
  auto field_d = field("d", float32());
  auto field_e = field("e", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d, field_e});

  // output fields
  auto res_a = field("a+1", boolean());
  auto res_b = field("b+1", int32());
  auto res_c = field("c+1", int64());
  auto res_d = field("d+1", float32());
  auto res_e = field("e+1", float64());

  // build expressions.
  // a == true
  // b + 1
  // c + 1
  // d + 1
  // e + 1
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_a = TreeExprBuilder::MakeLiteral(true);
  auto func_a = TreeExprBuilder::MakeFunction("equal", {node_a, literal_a}, boolean());
  auto expr_a = TreeExprBuilder::MakeExpression(func_a, res_a);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_b = TreeExprBuilder::MakeLiteral((int32_t)1);
  auto func_b = TreeExprBuilder::MakeFunction("add", {node_b, literal_b}, int32());
  auto expr_b = TreeExprBuilder::MakeExpression(func_b, res_b);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto literal_c = TreeExprBuilder::MakeLiteral((int64_t)1);
  auto func_c = TreeExprBuilder::MakeFunction("add", {node_c, literal_c}, int64());
  auto expr_c = TreeExprBuilder::MakeExpression(func_c, res_c);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto literal_d = TreeExprBuilder::MakeLiteral(static_cast<float>(1));
  auto func_d = TreeExprBuilder::MakeFunction("add", {node_d, literal_d}, float32());
  auto expr_d = TreeExprBuilder::MakeExpression(func_d, res_d);

  auto node_e = TreeExprBuilder::MakeField(field_e);
  auto literal_e = TreeExprBuilder::MakeLiteral(static_cast<double>(1));
  auto func_e = TreeExprBuilder::MakeFunction("add", {node_e, literal_e}, float64());
  auto expr_e = TreeExprBuilder::MakeExpression(func_e, res_e);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_a, expr_b, expr_c, expr_d, expr_e},
                                TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayBool({true, true, false, true}, {true, true, true, false});
  auto array_b = MakeArrowArrayInt32({5, 15, -15, 17}, {true, true, true, false});
  auto array_c = MakeArrowArrayInt64({5, 15, -15, 17}, {true, true, true, false});
  auto array_d = MakeArrowArrayFloat32({5.2f, 15, -15.6f, 17}, {true, true, true, false});
  auto array_e = MakeArrowArrayFloat64({5.6f, 15, -15.9f, 17}, {true, true, true, false});

  // expected output
  auto exp_a = MakeArrowArrayBool({true, true, false, false}, {true, true, true, false});
  auto exp_b = MakeArrowArrayInt32({6, 16, -14, 0}, {true, true, true, false});
  auto exp_c = MakeArrowArrayInt64({6, 16, -14, 0}, {true, true, true, false});
  auto exp_d = MakeArrowArrayFloat32({6.2f, 16, -14.6f, 0}, {true, true, true, false});
  auto exp_e = MakeArrowArrayFloat64({6.6f, 16, -14.9f, 0}, {true, true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_a, array_b, array_c, array_d, array_e});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_a, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_b, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_c, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_d, outputs.at(3));
  EXPECT_ARROW_ARRAY_EQUALS(exp_e, outputs.at(4));
}

TEST_F(TestLiteral, TestLiteralHash) {
  auto schema = arrow::schema({});
  // output fields
  auto res = field("a", int32());
  auto int_literal = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto expr = TreeExprBuilder::MakeExpression(int_literal, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  auto res1 = field("a", int64());
  auto int_literal1 = TreeExprBuilder::MakeLiteral((int64_t)2);
  auto expr1 = TreeExprBuilder::MakeExpression(int_literal1, res1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();
  EXPECT_TRUE(projector.get() != projector1.get());
}

TEST_F(TestLiteral, TestNullLiteral) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int32());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res = field("a+b+null", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_c = TreeExprBuilder::MakeNull(arrow::int32());
  auto add_a_b = TreeExprBuilder::MakeFunction("add", {node_a, node_b}, int32());
  auto add_a_b_c = TreeExprBuilder::MakeFunction("add", {add_a_b, literal_c}, int32());
  auto expr = TreeExprBuilder::MakeExpression(add_a_b_c, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayInt32({5, 15, -15, 17}, {true, true, true, false});
  auto array_b = MakeArrowArrayInt32({5, 15, -15, 17}, {true, true, true, false});

  // expected output
  auto exp = MakeArrowArrayInt32({0, 0, 0, 0}, {false, false, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestLiteral, TestNullLiteralInIf) {
  // schema for input fields
  auto field_a = field("a", float64());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", float64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_5 = TreeExprBuilder::MakeLiteral(5.0);
  auto a_gt_5 = TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_5},
                                              arrow::boolean());
  auto literal_null = TreeExprBuilder::MakeNull(arrow::float64());
  auto if_node =
      TreeExprBuilder::MakeIf(a_gt_5, literal_5, literal_null, arrow::float64());
  auto expr = TreeExprBuilder::MakeExpression(if_node, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayFloat64({6, 15, -15, 17}, {true, true, true, false});

  // expected output
  auto exp = MakeArrowArrayFloat64({5, 5, 0, 0}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva

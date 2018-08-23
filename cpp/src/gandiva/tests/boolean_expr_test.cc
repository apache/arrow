// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/status.h"
#include "gandiva/tree_expr_builder.h"
#include "integ/test_util.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;

class TestBooleanExpr : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestBooleanExpr, SimpleAnd) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // (a > 0) && (b > 0)
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto a_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_0}, boolean());
  auto b_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_0}, boolean());

  auto node_and = TreeExprBuilder::MakeAnd({a_gt_0, b_gt_0});
  auto expr = TreeExprBuilder::MakeExpression(node_and, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  // FALSE_VALID && ?  => FALSE_VALID
  int num_records = 4;
  auto arraya = MakeArrowArrayInt32({-2, -2, -2, -2}, {true, true, true, true});
  auto arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  auto exp = MakeArrowArrayBool({false, false, false, false}, {true, true, true, true});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // FALSE_INVALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({-2, -2, -2, -2}, {false, false, false, false});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, false, false}, {true, false, false, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // TRUE_VALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({2, 2, 2, 2}, {true, true, true, true});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, true, false}, {true, false, true, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // TRUE_INVALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({2, 2, 2, 2}, {false, false, false, false});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, false, false}, {true, false, false, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestBooleanExpr, SimpleOr) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // (a > 0) || (b > 0)
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto a_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_0}, boolean());
  auto b_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_0}, boolean());

  auto node_or = TreeExprBuilder::MakeOr({a_gt_0, b_gt_0});
  auto expr = TreeExprBuilder::MakeExpression(node_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  // TRUE_VALID && ?  => TRUE_VALID
  int num_records = 4;
  auto arraya = MakeArrowArrayInt32({2, 2, 2, 2}, {true, true, true, true});
  auto arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  auto exp = MakeArrowArrayBool({true, true, true, true}, {true, true, true, true});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // TRUE_INVALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({2, 2, 2, 2}, {false, false, false, false});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, true, false}, {false, false, true, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // FALSE_VALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({-2, -2, -2, -2}, {true, true, true, true});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, true, false}, {true, false, true, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));

  // FALSE_INVALID && ?
  num_records = 4;
  arraya = MakeArrowArrayInt32({-2, -2, -2, -2}, {false, false, false, false});
  arrayb = MakeArrowArrayInt32({-2, -2, 2, 2}, {true, false, true, false});
  exp = MakeArrowArrayBool({false, false, true, false}, {false, false, true, false});
  in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});
  outputs.clear();
  projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestBooleanExpr, AndThree) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", int32());
  auto schema = arrow::schema({fielda, fieldb, fieldc});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // (a > 0) && (b > 0) && (c > 0)
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto a_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_0}, boolean());
  auto b_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_0}, boolean());
  auto c_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_c, literal_0}, boolean());

  auto node_and = TreeExprBuilder::MakeAnd({a_gt_0, b_gt_0, c_gt_0});
  auto expr = TreeExprBuilder::MakeExpression(node_and, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  int num_records = 8;
  std::vector<bool> validity({true, true, true, true, true, true, true, true});
  auto arraya = MakeArrowArrayInt32({2, 2, 2, 0, 2, 0, 0, 0}, validity);
  auto arrayb = MakeArrowArrayInt32({2, 2, 0, 2, 0, 2, 0, 0}, validity);
  auto arrayc = MakeArrowArrayInt32({2, 0, 2, 2, 0, 0, 2, 0}, validity);
  auto exp = MakeArrowArrayBool({true, false, false, false, false, false, false, false},
                                validity);

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb, arrayc});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestBooleanExpr, OrThree) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", int32());
  auto schema = arrow::schema({fielda, fieldb, fieldc});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // (a > 0) || (b > 0) || (c > 0)
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto a_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_0}, boolean());
  auto b_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_0}, boolean());
  auto c_gt_0 =
      TreeExprBuilder::MakeFunction("greater_than", {node_c, literal_0}, boolean());

  auto node_or = TreeExprBuilder::MakeOr({a_gt_0, b_gt_0, c_gt_0});
  auto expr = TreeExprBuilder::MakeExpression(node_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  int num_records = 8;
  std::vector<bool> validity({true, true, true, true, true, true, true, true});
  auto arraya = MakeArrowArrayInt32({2, 2, 2, 0, 2, 0, 0, 0}, validity);
  auto arrayb = MakeArrowArrayInt32({2, 2, 0, 2, 0, 2, 0, 0}, validity);
  auto arrayc = MakeArrowArrayInt32({2, 0, 2, 2, 0, 0, 2, 0}, validity);
  auto exp =
      MakeArrowArrayBool({true, true, true, true, true, true, true, false}, validity);

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb, arrayc});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestBooleanExpr, BooleanAndInsideIf) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // if (a > 2 && b > 2)
  //   a > 3 && b > 3
  // else
  //   a > 1 && b > 1
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto literal_1 = TreeExprBuilder::MakeLiteral((int32_t)1);
  auto literal_2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto literal_3 = TreeExprBuilder::MakeLiteral((int32_t)3);
  auto a_gt_1 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_1}, boolean());
  auto a_gt_2 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_2}, boolean());
  auto a_gt_3 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_3}, boolean());
  auto b_gt_1 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_1}, boolean());
  auto b_gt_2 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_2}, boolean());
  auto b_gt_3 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_3}, boolean());

  auto and_1 = TreeExprBuilder::MakeAnd({a_gt_1, b_gt_1});
  auto and_2 = TreeExprBuilder::MakeAnd({a_gt_2, b_gt_2});
  auto and_3 = TreeExprBuilder::MakeAnd({a_gt_3, b_gt_3});

  auto node_if = TreeExprBuilder::MakeIf(and_2, and_3, and_1, arrow::boolean());
  auto expr = TreeExprBuilder::MakeExpression(node_if, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  int num_records = 4;
  std::vector<bool> validity({true, true, true, true});
  auto arraya = MakeArrowArrayInt32({4, 4, 2, 1}, validity);
  auto arrayb = MakeArrowArrayInt32({5, 3, 3, 1}, validity);
  auto exp = MakeArrowArrayBool({true, false, true, false}, validity);

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestBooleanExpr, IfInsideBooleanAnd) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // (if (a > b) a > 3 else b > 3) && (if (a > b) a > 2 else b > 2)

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto literal_2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto literal_3 = TreeExprBuilder::MakeLiteral((int32_t)3);
  auto a_gt_b =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, node_b}, boolean());
  auto a_gt_2 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_2}, boolean());
  auto a_gt_3 =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, literal_3}, boolean());
  auto b_gt_2 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_2}, boolean());
  auto b_gt_3 =
      TreeExprBuilder::MakeFunction("greater_than", {node_b, literal_3}, boolean());

  auto if_3 = TreeExprBuilder::MakeIf(a_gt_b, a_gt_3, b_gt_3, arrow::boolean());
  auto if_2 = TreeExprBuilder::MakeIf(a_gt_b, a_gt_2, b_gt_2, arrow::boolean());
  auto node_and = TreeExprBuilder::MakeAnd({if_3, if_2});
  auto expr = TreeExprBuilder::MakeExpression(node_and, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  int num_records = 4;
  std::vector<bool> validity({true, true, true, true});
  auto arraya = MakeArrowArrayInt32({4, 3, 3, 2}, validity);
  auto arrayb = MakeArrowArrayInt32({3, 4, 2, 3}, validity);
  auto exp = MakeArrowArrayBool({true, true, false, false}, validity);

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arraya, arrayb});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva

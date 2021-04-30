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
using arrow::null;

class TestNull : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestNull, TestSimple) {
  // schema for input fields
  auto field_null = field("field_null", null());
  auto schema = arrow::schema({field_null});

  auto literal_null = TreeExprBuilder::MakeNull(arrow::null());
  auto node_field_null = TreeExprBuilder::MakeField(field_null);

  // output fields
  auto res_1 = field("res1", null());
  auto res_2 = field("res2", null());
  auto expr_1 = TreeExprBuilder::MakeExpression(literal_null, res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(node_field_null, res_2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_1, expr_2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  arrow::ArrayVector outputs;
  auto null_array = std::make_shared<arrow::NullArray>(4);
  auto in_batch = arrow::RecordBatch::Make(schema, 4, {null_array});
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(null_array, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(null_array, outputs.at(1));
}

TEST_F(TestNull, TestOps) {
  // schema for input fields
  auto field_null = field("field_null", null());
  auto schema = arrow::schema({field_null});

  // output fields
  auto res_1 = field("res1", null());
  auto res_2 = field("res2", null());
  auto res_3 = field("res3", null());
  auto res_4 = field("res4", null());
  auto res_5 = field("res5", null());
  auto res_6 = field("res6", null());
  auto res_7 = field("res7", boolean());
  auto res_8 = field("res8", boolean());
  auto expr_1 = TreeExprBuilder::MakeExpression("equal", {field_null, field_null}, res_1);
  auto expr_2 =
      TreeExprBuilder::MakeExpression("not_equal", {field_null, field_null}, res_2);
  auto expr_3 =
      TreeExprBuilder::MakeExpression("less_than", {field_null, field_null}, res_3);
  auto expr_4 = TreeExprBuilder::MakeExpression("less_than_or_equal_to",
                                                {field_null, field_null}, res_4);
  auto expr_5 =
      TreeExprBuilder::MakeExpression("greater_than", {field_null, field_null}, res_5);
  auto expr_6 = TreeExprBuilder::MakeExpression("greater_than_or_equal_to",
                                                {field_null, field_null}, res_6);
  auto expr_7 = TreeExprBuilder::MakeExpression("isnull", {field_null}, res_7);
  auto expr_8 = TreeExprBuilder::MakeExpression("isnotnull", {field_null}, res_8);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      schema, {expr_1, expr_2, expr_3, expr_4, expr_5, expr_6, expr_7, expr_8},
      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  arrow::ArrayVector outputs;
  auto null_array = std::make_shared<arrow::NullArray>(4);
  auto in_batch = arrow::RecordBatch::Make(schema, 4, {null_array});
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto exp_true = MakeArrowArrayBool({true, true, true, true}, {true, true, true, true});
  auto exp_false =
      MakeArrowArrayBool({false, false, false, false}, {true, true, true, true});
  for (int i = 0; i < 6; i++) {
    EXPECT_EQ(outputs.at(i)->null_count(), 4);
  }
  EXPECT_ARROW_ARRAY_EQUALS(exp_true, outputs.at(6));
  EXPECT_ARROW_ARRAY_EQUALS(exp_false, outputs.at(7));
}

TEST_F(TestNull, TestMakeIf) {
  // schema for input fields
  auto field_null = field("field_null", null());
  auto schema = arrow::schema({field_null});

  // output fields
  auto res_1 = field("res1", null());
  auto res_2 = field("res2", null());

  auto null_node = TreeExprBuilder::MakeNull(null());
  auto expr_1 = TreeExprBuilder::MakeExpression(
      TreeExprBuilder::MakeIf(TreeExprBuilder::MakeLiteral(true), null_node, null_node,
                              null()),
      res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(
      TreeExprBuilder::MakeIf(TreeExprBuilder::MakeLiteral(false), null_node, null_node,
                              null()),
      res_2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_1, expr_2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  arrow::ArrayVector outputs;
  auto null_array = std::make_shared<arrow::NullArray>(4);
  auto in_batch = arrow::RecordBatch::Make(schema, 4, {null_array});
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  for (auto& output : outputs) {
    EXPECT_EQ(output->null_count(), 4);
  }
}

}  // namespace gandiva

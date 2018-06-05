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
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::float32;
using arrow::boolean;

class TestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestProjector, TestIntSumSub) {
  /* schema for input fields */
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  /* output fields */
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  /*
   * Build expression
   */
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr = TreeExprBuilder::MakeExpression("subtract", {field0, field1},
                                                  field_sub);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {sum_expr, sub_expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  /* Create a row-batch with some sample data */
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });
  /* expected output */
  auto exp_sum = MakeArrowArrayInt32({ 12, 15, 0, 0 }, { true, true, false, false });
  auto exp_sub = MakeArrowArrayInt32({ -10, -11, 0, 0 }, { true, true, false, false });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  /*
   * Evaluate expression
   */
  auto outputs = projector->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_sub, outputs.at(1));
}

TEST_F(TestProjector, TestFloatLessThan) {
  /* schema for input fields */
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field1});

  /* output fields */
  auto field_result = field("res", boolean());

  /*
   * Build expression
   */
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1},
                                                 field_result);

  /*
   * Build a projector for the expressions.
   */
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {lt_expr}, pool_, &projector);
  EXPECT_TRUE(status.ok());


  /* Create a row-batch with some sample data */
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  auto array1 = MakeArrowArrayFloat32({ 4.0, 3.4, 6.8 }, { true, true, true });
  /* expected output */
  auto exp = MakeArrowArrayBool({ true, false, false }, { true, true, false });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  /*
   * Evaluate expression
   */
  auto outputs = projector->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestIsNotNull) {
  /* schema for input fields */
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  /* output fields */
  auto field_result = field("res", boolean());

  /*
   * Build expression
   */
  auto myexpr = TreeExprBuilder::MakeExpression("isnotnull", {field0}, field_result);

  /*
   * Build a projector for the expressions.
   */
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {myexpr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  /* Create a row-batch with some sample data */
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  /* expected output */
  auto exp = MakeArrowArrayBool({ true, true, false }, { true, true, true });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  /*
   * Evaluate expression
   */
  auto outputs = projector->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestNullInternal) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  auto myexpr = TreeExprBuilder::MakeExpression("half_or_null", {field0}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {myexpr}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 =
      MakeArrowArrayInt32({ 10, 10, -20, 5, -7 }, { true, false, true, true, true });

  // expected output
  auto exp =
      MakeArrowArrayInt32({ 5, 0, -10, 0, 0 }, { true, false, true, false, false });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestProjector, TestNestedFunctions) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_res1 = field("res1", int32());
  auto field_res2 = field("res2", boolean());

  // build expression.
  // expr1 : half_or_null(f0) * f1
  // expr2 : isnull(half_or_null(f0) * f1)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto half = TreeExprBuilder::MakeFunction("half_or_null", {node_f0}, int32());
  auto mult = TreeExprBuilder::MakeFunction("multiply", {half, node_f1}, int32());
  auto expr1 = TreeExprBuilder::MakeExpression(mult, field_res1);

  auto isnull = TreeExprBuilder::MakeFunction("isnull", {mult}, boolean());
  auto expr2 = TreeExprBuilder::MakeExpression(isnull, field_res2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr1, expr2}, pool_, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 10, 10, -20, 5}, { true, false, true, true });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });

  // expected output
  auto exp1 = MakeArrowArrayInt32({ 55, 65, -150, 0 }, { true, false, false, false });
  auto exp2 = MakeArrowArrayBool({ false, true, true, true }, { true, true, true, true });

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  auto outputs = projector->Evaluate(*in_batch);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp2, outputs.at(1));
}

} // namespace gandiva

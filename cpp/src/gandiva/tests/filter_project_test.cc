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
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/selection_vector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestFilterProject : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestFilterProject, TestSimple16) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimple32) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt32(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimple64) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt64(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimpleIf) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", int32());
  auto schema = arrow::schema({fielda, fieldb, fieldc});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);

  auto greater_than_function =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, node_b}, boolean());
  auto filter_condition = TreeExprBuilder::MakeCondition(greater_than_function);

  auto project_condition =
      TreeExprBuilder::MakeFunction("less_than", {node_b, node_c}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(project_condition, node_b, node_c, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);
  auto configuration = TestConfiguration();

  // Build a filter for the expressions.
  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, filter_condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 6;
  auto array0 =
      MakeArrowArrayInt32({10, 12, -20, 5, 21, 29}, {true, true, true, true, true, true});
  auto array1 =
      MakeArrowArrayInt32({5, 15, 15, 17, 12, 3}, {true, true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 25, 11, 30, -21, 30},
                                    {true, true, true, true, true, false});

  // Create a selection vector
  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt32(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // expected output
  auto exp = MakeArrowArrayInt32({1, -21, 0}, {true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  // Evaluate filter
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate project
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}
}  // namespace gandiva

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
#include <cmath>

#include "arrow/memory_pool.h"
#include "gandiva/filter.h"
#include "gandiva/function_registry_common.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
using arrow::int32;

class TestPreEvalIn : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestPreEvalIn, TestInSimple) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());

  auto literal_11_int32 = TreeExprBuilder::MakeLiteral(11);
  auto literal_6_int32 = TreeExprBuilder::MakeLiteral(6);

  auto pre_eval_expr = TreeExprBuilder::MakePreEvalInExpression(
      sum_func, {literal_6_int32, literal_11_int32});
  auto condition = TreeExprBuilder::MakeCondition(pre_eval_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 5}, {true, true, false, true, false});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 1});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestIntWithEval) {
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto field3 = field("f3", int32());
  auto schema = arrow::schema({field0, field1, field2, field3});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());

  auto sum_func_condition = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field2), TreeExprBuilder::MakeField(field3)},
      int32());
  auto literal_17_int32 = TreeExprBuilder::MakeLiteral(17);
  auto pre_eval_expr = TreeExprBuilder::MakePreEvalInExpression(
      sum_func, {sum_func_condition, literal_17_int32});

  auto condition = TreeExprBuilder::MakeCondition(pre_eval_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 5}, {true, true, false, true, false});
  auto array2 = MakeArrowArrayInt32({3, 8, 7, 7, 2}, {true, true, true, true, true});
  auto array3 = MakeArrowArrayInt32({3, 3, 3, 3, 9}, {true, true, true, true, false});

  auto exp = MakeArrowArrayUint16({0, 1});

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2, array3});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestFloat) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);

  auto literal_1 = TreeExprBuilder::MakeLiteral(6.5f);
  auto literal_2 = TreeExprBuilder::MakeLiteral(12.0f);
  auto literal_3 = TreeExprBuilder::MakeLiteral(11.5f);

  auto pre_eval_in_expr = TreeExprBuilder::MakePreEvalInExpression(
      node_f0, {literal_1, literal_2, literal_3});
  auto condition = TreeExprBuilder::MakeCondition(pre_eval_in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 =
      MakeArrowArrayFloat32({6.5f, 11.5f, 4, 3.15f, 6}, {true, true, false, true, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 1});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestDoube) {
  // schema for input fields
  auto field0 = field("double0", float64());
  auto field1 = field("double1", float64());
  auto schema = arrow::schema({field0, field1});

  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::float64());
  auto literal_1 = TreeExprBuilder::MakeLiteral(3.14159265359);
  auto literal_2 = TreeExprBuilder::MakeLiteral(15.5555555);
  auto literal_3 = TreeExprBuilder::MakeLiteral(15.5555556);
  auto pre_eval_expr =
      TreeExprBuilder::MakePreEvalInExpression(sum_func, {literal_1, literal_2});
  auto condition = TreeExprBuilder::MakeCondition(pre_eval_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayFloat64({1, 2, 3, 4, 11}, {true, true, true, false, false});
  auto array1 = MakeArrowArrayFloat64({5, 9, 0.14159265359, 17, 4.5555555},
                                      {true, true, true, true, true});

  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({2});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestInDecimal) {
  int32_t precision = 38;
  int32_t scale = 5;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  // schema for input fields
  auto field0 = field("f0", arrow::decimal(precision, scale));
  auto schema = arrow::schema({field0});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);

  gandiva::DecimalScalar128 d0("6", precision, scale);
  gandiva::DecimalScalar128 d1("12", precision, scale);
  gandiva::DecimalScalar128 d2("11", precision, scale);

  auto literal_1 = TreeExprBuilder::MakeDecimalLiteral(d0);
  auto literal_2 = TreeExprBuilder::MakeDecimalLiteral(d1);
  auto literal_3 = TreeExprBuilder::MakeDecimalLiteral(d2);

  auto pre_eval_in_expr = TreeExprBuilder::MakePreEvalInExpression(
      node_f0, {literal_1, literal_2, literal_3});

  auto condition = TreeExprBuilder::MakeCondition(pre_eval_in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto values0 = MakeDecimalVector({"1", "2", "0", "-6", "6"});
  auto array0 =
      MakeArrowArrayDecimal(decimal_type, values0, {true, true, true, false, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({4});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestInString) {
  // schema for input fields
  auto field0 = field("f0", arrow::utf8());
  auto schema = arrow::schema({field0});

  // Build f0 in ("test" ,"me")
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto literal_1 = TreeExprBuilder::MakeStringLiteral("test");
  auto literal_2 = TreeExprBuilder::MakeStringLiteral("me");
  auto in_expr =
      TreeExprBuilder::MakePreEvalInExpression(node_f0, {literal_1, literal_2});

  auto condition = TreeExprBuilder::MakeCondition(in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUtf8({"test", "lol", "me", "arrow", "test"},
                                    {true, true, true, true, false});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 2});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

template <typename Type>
void TestDateOrTimePreEvalInExpression(arrow::MemoryPool* pool, const DataTypePtr& type) {
  using c_type = typename Type::c_type;

  std::vector<c_type> in_constants({717629471, 717630471});

  // schema for input fields
  auto field0 = field("f0", type);
  auto schema = arrow::schema({field0});

  auto node_f0 = TreeExprBuilder::MakeField(field0);
  NodeVector literal_nodes;
  literal_nodes.reserve(in_constants.size());
  for (auto num : in_constants) {
    auto literal_node = std::make_shared<LiteralNode>(type, LiteralHolder(num), false);
    literal_nodes.emplace_back(literal_node);
  }

  auto pre_eval_in_expr =
      TreeExprBuilder::MakePreEvalInExpression(node_f0, literal_nodes);
  auto condition = TreeExprBuilder::MakeCondition(pre_eval_in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  ASSERT_TRUE(status.ok()) << status.message();

  int num_records = 5;
  auto array0 = MakeArrowArray<Type, c_type>(
      type, {717629471, 717630471, 717729471, 717629471, 717629571},
      {true, true, true, false, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 1});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool, &selection_vector);
  ASSERT_TRUE(status.ok()) << status.message();

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  ASSERT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestPreEvalIn, TestDa32) {
  TestDateOrTimePreEvalInExpression<arrow::Date32Type>(pool_, arrow::date32());
}

TEST_F(TestPreEvalIn, TestDate64) {
  TestDateOrTimePreEvalInExpression<arrow::Date64Type>(pool_, arrow::date64());
}

TEST_F(TestPreEvalIn, TestInTimeStamp) {
  TestDateOrTimePreEvalInExpression<arrow::TimestampType>(pool_, timestamp());
}

TEST_F(TestPreEvalIn, TestInTime32) {
  TestDateOrTimePreEvalInExpression<arrow::Time32Type>(pool_, time32());
}

TEST_F(TestPreEvalIn, TestTime64) {
  TestDateOrTimePreEvalInExpression<arrow::Time64Type>(pool_, time64());
}

}  // namespace gandiva

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

class TestIn : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};
std::vector<Decimal128> MakeDecimalVector(std::vector<std::string> values) {
  std::vector<arrow::Decimal128> ret;
  for (auto str : values) {
    Decimal128 decimal_value;
    int32_t decimal_precision;
    int32_t decimal_scale;

    DCHECK_OK(
        Decimal128::FromString(str, &decimal_value, &decimal_precision, &decimal_scale));

    ret.push_back(decimal_value);
  }
  return ret;
}

TEST_F(TestIn, TestInSimple) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  std::unordered_set<int32_t> in_constants({6, 11});
  auto in_expr = TreeExprBuilder::MakeInExpressionInt32(sum_func, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

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

TEST_F(TestIn, TestInFloat) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);

  std::unordered_set<float> in_constants({6.5f, 12.0f, 11.5f});
  auto in_expr = TreeExprBuilder::MakeInExpressionFloat(node_f0, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

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

TEST_F(TestIn, TestInDouble) {
  // schema for input fields
  auto field0 = field("double0", float64());
  auto field1 = field("double1", float64());
  auto schema = arrow::schema({field0, field1});

  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::float64());
  std::unordered_set<double> in_constants({3.14159265359, 15.5555555});
  auto in_expr = TreeExprBuilder::MakeInExpressionDouble(sum_func, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

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

TEST_F(TestIn, TestInDecimal) {
  int32_t precision = 38;
  int32_t scale = 5;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  // schema for input fields
  auto field0 = field("f0", arrow::decimal128(precision, scale));
  auto schema = arrow::schema({field0});

  // Build In f0 + f1 in (6, 11)
  auto node_f0 = TreeExprBuilder::MakeField(field0);

  gandiva::DecimalScalar128 d0("6", precision, scale);
  gandiva::DecimalScalar128 d1("12", precision, scale);
  gandiva::DecimalScalar128 d2("11", precision, scale);
  std::unordered_set<gandiva::DecimalScalar128> in_constants({d0, d1, d2});
  auto in_expr = TreeExprBuilder::MakeInExpressionDecimal(node_f0, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

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

TEST_F(TestIn, TestInString) {
  // schema for input fields
  auto field0 = field("f0", arrow::utf8());
  auto schema = arrow::schema({field0});

  // Build f0 in ("test" ,"me")
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  std::unordered_set<std::string> in_constants({"test", "me"});
  auto in_expr = TreeExprBuilder::MakeInExpressionString(node_f0, in_constants);

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

TEST_F(TestIn, TestInStringValidationError) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto schema = arrow::schema({field0});

  // Build f0 in ("test" ,"me")
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  std::unordered_set<std::string> in_constants({"test", "me"});
  auto in_expr = TreeExprBuilder::MakeInExpressionString(node_f0, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);

  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error = "Evaluation expression for IN clause returns ";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

// Test that timestamp types work as inputs to in expressions
template <typename Type>
void TestDateOrTimeInExpression(
    arrow::MemoryPool* pool, const DataTypePtr& type,
    std::function<NodePtr(NodePtr, std::unordered_set<typename Type::c_type>)>
        expression_factory) {
  using c_type = typename Type::c_type;

  // schema for input fields
  auto field0 = field("f0", type);
  auto schema = arrow::schema({field0});

  // Build f0 in (717629471, 717630471)
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  std::unordered_set<c_type> in_constants({717629471, 717630471});
  auto in_expr = expression_factory(node_f0, in_constants);
  auto condition = TreeExprBuilder::MakeCondition(in_expr);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  ASSERT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
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

TEST_F(TestIn, TestInDate32) {
  TestDateOrTimeInExpression<arrow::Date32Type>(
      pool_, date32(), [](auto node, auto in_constants) {
        return TreeExprBuilder::MakeInExpressionDate32(node, in_constants);
      });
}

TEST_F(TestIn, TestInDate64) {
  TestDateOrTimeInExpression<arrow::Date64Type>(
      pool_, date64(), [](auto node, auto in_constants) {
        return TreeExprBuilder::MakeInExpressionDate64(node, in_constants);
      });
}

TEST_F(TestIn, TestInTimeStamp) {
  TestDateOrTimeInExpression<arrow::TimestampType>(
      pool_, timestamp(), [](auto node, auto in_constants) {
        return TreeExprBuilder::MakeInExpressionTimeStamp(node, in_constants);
      });
}

TEST_F(TestIn, TestInTime32) {
  TestDateOrTimeInExpression<arrow::Time32Type>(
      pool_, time32(), [](auto node, auto in_constants) {
        return TreeExprBuilder::MakeInExpressionTime32(node, in_constants);
      });
}

TEST_F(TestIn, TestInTime64) {
  TestDateOrTimeInExpression<arrow::Time64Type>(
      pool_, time64(), [](auto node, auto in_constants) {
        return TreeExprBuilder::MakeInExpressionTime64(node, in_constants);
      });
}

}  // namespace gandiva

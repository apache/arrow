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

#include <sstream>

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"

#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

using arrow::Decimal128;

namespace gandiva {

class TestDecimal : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

  std::vector<Decimal128> MakeDecimalVector(std::vector<std::string> values,
                                            int32_t scale);

 protected:
  arrow::MemoryPool* pool_;
};

std::vector<Decimal128> TestDecimal::MakeDecimalVector(std::vector<std::string> values,
                                                       int32_t scale) {
  std::vector<arrow::Decimal128> ret;
  for (auto str : values) {
    Decimal128 str_value;
    int32_t str_precision;
    int32_t str_scale;

    auto status = Decimal128::FromString(str, &str_value, &str_precision, &str_scale);
    DCHECK_OK(status);

    Decimal128 scaled_value;
    status = str_value.Rescale(str_scale, scale, &scaled_value);
    ret.push_back(scaled_value);
  }
  return ret;
}

TEST_F(TestDecimal, TestSimple) {
  // schema for input fields
  constexpr int32_t precision = 36;
  constexpr int32_t scale = 18;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = field("a", decimal_type);
  auto field_b = field("b", decimal_type);
  auto field_c = field("c", decimal_type);
  auto schema = arrow::schema({field_a, field_b, field_c});

  Decimal128TypePtr add2_type;
  auto status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                               {decimal_type, decimal_type}, &add2_type);

  Decimal128TypePtr output_type;
  status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                          {add2_type, decimal_type}, &output_type);

  // output fields
  auto res = field("res0", output_type);

  // build expression : a + b + c
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto add2 = TreeExprBuilder::MakeFunction("add", {node_a, node_b}, add2_type);
  auto add3 = TreeExprBuilder::MakeFunction("add", {add2, node_c}, output_type);
  auto expr = TreeExprBuilder::MakeExpression(add3, res);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"1", "2", "3", "4"}, scale),
                            {false, true, true, true});
  auto array_b =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"2", "3", "4", "5"}, scale),
                            {false, true, true, true});
  auto array_c =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"3", "4", "5", "6"}, scale),
                            {true, true, true, true});

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array_a, array_b, array_c});

  auto expected =
      MakeArrowArrayDecimal(output_type, MakeDecimalVector({"6", "9", "12", "15"}, scale),
                            {false, true, true, true});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(expected, outputs[0]);
}

TEST_F(TestDecimal, TestLiteral) {
  // schema for input fields
  constexpr int32_t precision = 36;
  constexpr int32_t scale = 18;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = field("a", decimal_type);
  auto schema = arrow::schema({
      field_a,
  });

  Decimal128TypePtr add2_type;
  auto status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                               {decimal_type, decimal_type}, &add2_type);

  // output fields
  auto res = field("res0", add2_type);

  // build expression : a + b + c
  auto node_a = TreeExprBuilder::MakeField(field_a);
  static std::string decimal_point_six = "6";
  DecimalScalar128 literal(decimal_point_six, 2, 1);
  auto node_b = TreeExprBuilder::MakeDecimalLiteral(literal);
  auto add2 = TreeExprBuilder::MakeFunction("add", {node_a, node_b}, add2_type);
  auto expr = TreeExprBuilder::MakeExpression(add2, res);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"1", "2", "3", "4"}, scale),
                            {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  auto expected = MakeArrowArrayDecimal(
      add2_type, MakeDecimalVector({"1.6", "2.6", "3.6", "4.6"}, scale),
      {false, true, true, true});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(expected, outputs[0]);
}

TEST_F(TestDecimal, TestIfElse) {
  // schema for input fields
  constexpr int32_t precision = 36;
  constexpr int32_t scale = 18;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = field("a", decimal_type);
  auto field_b = field("b", decimal_type);
  auto field_c = field("c", arrow::boolean());
  auto schema = arrow::schema({field_a, field_b, field_c});

  // output fields
  auto field_result = field("res", decimal_type);

  // build expression.
  // if (c)
  //   a
  // else
  //   b
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto if_node = TreeExprBuilder::MakeIf(node_c, node_a, node_b, decimal_type);

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"1", "2", "3", "4"}, scale),
                            {false, true, true, true});
  auto array_b =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"2", "3", "4", "5"}, scale),
                            {true, true, true, true});

  auto array_c = MakeArrowArrayBool({true, false, true, false}, {true, true, true, true});

  // expected output
  auto exp =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"0", "3", "3", "5"}, scale),
                            {false, true, true, true});

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array_a, array_b, array_c});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva

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

using arrow::boolean;
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
    if (str_scale == scale) {
      scaled_value = str_value;
    } else {
      status = str_value.Rescale(str_scale, scale, &scaled_value);
      DCHECK_OK(status);
    }
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

TEST_F(TestDecimal, TestCompare) {
  // schema for input fields
  constexpr int32_t precision = 36;
  constexpr int32_t scale = 18;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = field("a", decimal_type);
  auto field_b = field("b", decimal_type);
  auto schema = arrow::schema({field_a, field_b});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("equal", {field_a, field_b},
                                      field("res_eq", boolean())),
      TreeExprBuilder::MakeExpression("not_equal", {field_a, field_b},
                                      field("res_ne", boolean())),
      TreeExprBuilder::MakeExpression("less_than", {field_a, field_b},
                                      field("res_lt", boolean())),
      TreeExprBuilder::MakeExpression("less_than_or_equal_to", {field_a, field_b},
                                      field("res_le", boolean())),
      TreeExprBuilder::MakeExpression("greater_than", {field_a, field_b},
                                      field("res_gt", boolean())),
      TreeExprBuilder::MakeExpression("greater_than_or_equal_to", {field_a, field_b},
                                      field("res_ge", boolean())),
  };

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"1", "2", "3", "-4"}, scale),
                            {true, true, true, true});
  auto array_b =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"1", "3", "2", "-3"}, scale),
                            {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, false, false, false}),
                            outputs[0]);  // equal
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({false, true, true, true}),
                            outputs[1]);  // not_equal
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({false, true, false, true}),
                            outputs[2]);  // less_than
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, true, false, true}),
                            outputs[3]);  // less_than_or_equal_to
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({false, false, true, false}),
                            outputs[4]);  // greater_than
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, false, true, false}),
                            outputs[5]);  // greater_than_or_equal_to
}

TEST_F(TestDecimal, TestRoundFunctions) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_a = field("a", decimal_type);
  auto schema = arrow::schema({field_a});

  auto scale_1 = TreeExprBuilder::MakeLiteral(1);

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("abs", {field_a}, field("res_abs", decimal_type)),
      TreeExprBuilder::MakeExpression("ceil", {field_a},
                                      field("res_ceil", arrow::decimal(precision, 0))),
      TreeExprBuilder::MakeExpression("floor", {field_a},
                                      field("res_floor", arrow::decimal(precision, 0))),
      TreeExprBuilder::MakeExpression("round", {field_a},
                                      field("res_round", arrow::decimal(precision, 0))),
      TreeExprBuilder::MakeExpression(
          "truncate", {field_a}, field("res_truncate", arrow::decimal(precision, 0))),

      TreeExprBuilder::MakeExpression(
          TreeExprBuilder::MakeFunction("round",
                                        {TreeExprBuilder::MakeField(field_a), scale_1},
                                        arrow::decimal(precision, 1)),
          field("res_round_3", arrow::decimal(precision, 1))),

      TreeExprBuilder::MakeExpression(
          TreeExprBuilder::MakeFunction("truncate",
                                        {TreeExprBuilder::MakeField(field_a), scale_1},
                                        arrow::decimal(precision, 1)),
          field("res_truncate_3", arrow::decimal(precision, 1))),
  };

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto validity = {true, true, true, true};
  auto array_a = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.23", "1.58", "-1.23", "-1.58"}, scale),
      validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results

  // abs(x)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(decimal_type,
                            MakeDecimalVector({"1.23", "1.58", "1.23", "1.58"}, scale),
                            validity),
      outputs[0]);

  // ceil(x)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 0),
                            MakeDecimalVector({"2", "2", "-1", "-1"}, 0), validity),
      outputs[1]);

  // floor(x)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 0),
                            MakeDecimalVector({"1", "1", "-2", "-2"}, 0), validity),
      outputs[2]);

  // round(x)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 0),
                            MakeDecimalVector({"1", "2", "-1", "-2"}, 0), validity),
      outputs[3]);

  // truncate(x)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 0),
                            MakeDecimalVector({"1", "1", "-1", "-1"}, 0), validity),
      outputs[4]);

  // round(x, 1)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 1),
                            MakeDecimalVector({"1.2", "1.6", "-1.2", "-1.6"}, 1),
                            validity),
      outputs[5]);

  // truncate(x, 1)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 1),
                            MakeDecimalVector({"1.2", "1.5", "-1.2", "-1.5"}, 1),
                            validity),
      outputs[6]);
}

TEST_F(TestDecimal, TestCastFunctions) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto decimal_type_scale_1 = std::make_shared<arrow::Decimal128Type>(precision, 1);
  auto field_int64 = field("intt64", arrow::int64());
  auto field_float64 = field("float64", arrow::float64());
  auto field_dec = field("dec", decimal_type);
  auto schema = arrow::schema({field_int64, field_float64, field_dec});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_int64},
                                      field("int64_to_dec", decimal_type)),
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64},
                                      field("float64_to_dec", decimal_type)),
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_dec},
                                      field("dec_to_dec", decimal_type_scale_1)),
      TreeExprBuilder::MakeExpression("castBIGINT", {field_dec},
                                      field("dec_to_int64", arrow::int64())),
      TreeExprBuilder::MakeExpression("castFLOAT8", {field_dec},
                                      field("dec_to_float64", arrow::float64())),
  };

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto validity = {true, true, true, true};

  auto array_int64 = MakeArrowArrayInt64({123, 158, -123, -158});
  auto array_float64 = MakeArrowArrayFloat64({1.23, 1.58, -1.23, -1.58});
  auto array_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.23", "1.58", "-1.23", "-1.58"}, scale),
      validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_int64, array_float64, array_dec});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results

  // castDECIMAL(int64)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(decimal_type,
                            MakeDecimalVector({"123", "158", "-123", "-158"}, scale),
                            validity),
      outputs[0]);

  // castDECIMAL(float64)
  EXPECT_ARROW_ARRAY_EQUALS(array_dec, outputs[1]);

  // castDECIMAL(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 1),
                            MakeDecimalVector({"1.2", "1.6", "-1.2", "-1.6"}, 1),
                            validity),
      outputs[2]);

  // castBIGINT(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayInt64({1, 1, -1, -1}), outputs[3]);

  // castDOUBLE(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(array_float64, outputs[4]);
}

}  // namespace gandiva

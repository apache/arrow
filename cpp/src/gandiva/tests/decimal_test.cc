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
using arrow::utf8;

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
  auto field_int32 = field("int32", arrow::int32());
  auto field_int64 = field("int64", arrow::int64());
  auto field_float32 = field("float32", arrow::float32());
  auto field_float64 = field("float64", arrow::float64());
  auto field_dec = field("dec", decimal_type);
  auto schema =
      arrow::schema({field_int32, field_int64, field_float32, field_float64, field_dec});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_int32},
                                      field("int32_to_dec", decimal_type)),
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_int64},
                                      field("int64_to_dec", decimal_type)),
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_float32},
                                      field("float32_to_dec", decimal_type)),
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

  auto array_int32 = MakeArrowArrayInt32({123, 158, -123, -158});
  auto array_int64 = MakeArrowArrayInt64({123, 158, -123, -158});
  auto array_float32 = MakeArrowArrayFloat32({1.23f, 1.58f, -1.23f, -1.58f});
  auto array_float64 = MakeArrowArrayFloat64({1.23, 1.58, -1.23, -1.58});
  auto array_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.23", "1.58", "-1.23", "-1.58"}, scale),
      validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records,
      {array_int32, array_int64, array_float32, array_float64, array_dec});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto expected_int_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"123", "158", "-123", "-158"}, scale), validity);

  // castDECIMAL(int32)
  EXPECT_ARROW_ARRAY_EQUALS(expected_int_dec, outputs[0]);

  // castDECIMAL(int64)
  EXPECT_ARROW_ARRAY_EQUALS(expected_int_dec, outputs[1]);

  // castDECIMAL(float32)
  EXPECT_ARROW_ARRAY_EQUALS(array_dec, outputs[2]);

  // castDECIMAL(float64)
  EXPECT_ARROW_ARRAY_EQUALS(array_dec, outputs[3]);

  // castDECIMAL(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(
      MakeArrowArrayDecimal(arrow::decimal(precision, 1),
                            MakeDecimalVector({"1.2", "1.6", "-1.2", "-1.6"}, 1),
                            validity),
      outputs[4]);

  // castBIGINT(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayInt64({1, 2, -1, -2}), outputs[5]);

  // castDOUBLE(decimal)
  EXPECT_ARROW_ARRAY_EQUALS(array_float64, outputs[6]);
}

// isnull, isnumeric
TEST_F(TestDecimal, TestIsNullNumericFunctions) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_dec = field("dec", decimal_type);
  auto schema = arrow::schema({field_dec});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("isnull", {field_dec},
                                      field("isnull", arrow::boolean())),

      TreeExprBuilder::MakeExpression("isnotnull", {field_dec},
                                      field("isnotnull", arrow::boolean())),
      TreeExprBuilder::MakeExpression("isnumeric", {field_dec},
                                      field("isnumeric", arrow::boolean()))};

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 5;
  auto validity = {false, true, true, true, false};

  auto array_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_dec});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto is_null = outputs.at(0);
  auto is_not_null = outputs.at(1);
  auto is_numeric = outputs.at(2);

  // isnull
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, false, false, false, true}),
                            outputs[0]);

  // isnotnull
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool(validity), outputs[1]);

  // isnumeric
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool(validity), outputs[2]);
}

TEST_F(TestDecimal, TestIsDistinct) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale_1 = 2;
  auto decimal_type_1 = std::make_shared<arrow::Decimal128Type>(precision, scale_1);
  auto field_dec_1 = field("dec_1", decimal_type_1);
  constexpr int32_t scale_2 = 1;
  auto decimal_type_2 = std::make_shared<arrow::Decimal128Type>(precision, scale_2);
  auto field_dec_2 = field("dec_2", decimal_type_2);

  auto schema = arrow::schema({field_dec_1, field_dec_2});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("is_distinct_from", {field_dec_1, field_dec_2},
                                      field("isdistinct", arrow::boolean())),

      TreeExprBuilder::MakeExpression("is_not_distinct_from", {field_dec_1, field_dec_2},
                                      field("isnotdistinct", arrow::boolean()))};

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;

  auto validity_1 = {true, false, true, true};
  auto array_dec_1 = MakeArrowArrayDecimal(
      decimal_type_1, MakeDecimalVector({"1.51", "1.23", "1.20", "-1.20"}, scale_1),
      validity_1);

  auto validity_2 = {true, false, false, true};
  auto array_dec_2 = MakeArrowArrayDecimal(
      decimal_type_2, MakeDecimalVector({"1.5", "1.2", "1.2", "-1.2"}, scale_2),
      validity_2);

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array_dec_1, array_dec_2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto is_distinct = std::dynamic_pointer_cast<arrow::BooleanArray>(outputs.at(0));
  auto is_not_distinct = std::dynamic_pointer_cast<arrow::BooleanArray>(outputs.at(1));

  // isdistinct
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, false, true, false}), outputs[0]);

  // isnotdistinct
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({false, true, false, true}), outputs[1]);
}

// decimal hashes without seed
TEST_F(TestDecimal, TestHashFunctions) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_dec = field("dec", decimal_type);
  auto literal_seed32 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_seed64 = TreeExprBuilder::MakeLiteral((int64_t)10);
  auto schema = arrow::schema({field_dec});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("hash", {field_dec},
                                      field("hash_of_dec", arrow::int32())),

      TreeExprBuilder::MakeExpression("hash64", {field_dec},
                                      field("hash64_of_dec", arrow::int64())),

      TreeExprBuilder::MakeExpression("hash32AsDouble", {field_dec},
                                      field("hash32_as_double", arrow::int32())),

      TreeExprBuilder::MakeExpression("hash64AsDouble", {field_dec},
                                      field("hash64_as_double", arrow::int64()))};

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 5;
  auto validity = {false, true, true, true, true};

  auto array_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_dec});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  EXPECT_EQ(int32_arr->Value(1), int32_arr->Value(2));
  EXPECT_NE(int32_arr->Value(2), int32_arr->Value(3));
  EXPECT_NE(int32_arr->Value(3), int32_arr->Value(4));

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 0);
  EXPECT_EQ(int64_arr->Value(1), int64_arr->Value(2));
  EXPECT_NE(int64_arr->Value(2), int64_arr->Value(3));
  EXPECT_NE(int64_arr->Value(3), int64_arr->Value(4));
}

TEST_F(TestDecimal, TestHash32WithSeed) {
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_dec_1 = field("dec1", decimal_type);
  auto field_dec_2 = field("dec2", decimal_type);
  auto schema = arrow::schema({field_dec_1, field_dec_2});

  auto res = field("hash32_with_seed", arrow::int32());

  auto field_1_nodePtr = TreeExprBuilder::MakeField(field_dec_1);
  auto field_2_nodePtr = TreeExprBuilder::MakeField(field_dec_2);

  auto hash32 =
      TreeExprBuilder::MakeFunction("hash32", {field_2_nodePtr}, arrow::int32());
  auto hash32_with_seed =
      TreeExprBuilder::MakeFunction("hash32", {field_1_nodePtr, hash32}, arrow::int32());
  auto expr = TreeExprBuilder::MakeExpression(hash32, field("hash32", arrow::int32()));
  auto exprWS = TreeExprBuilder::MakeExpression(hash32_with_seed, res);

  auto exprs = std::vector<ExpressionPtr>{expr, exprWS};

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 5;
  auto validity_1 = {false, false, true, true, true};

  auto array_dec_1 = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity_1);

  auto validity_2 = {false, true, false, true, true};

  auto array_dec_2 = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity_2);

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array_dec_1, array_dec_2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  auto int32_arr_WS = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(1));
  EXPECT_EQ(int32_arr->null_count(), 0);
  // seed 0, null decimal
  EXPECT_EQ(int32_arr_WS->Value(0), 0);
  // null decimal => hash = seed
  EXPECT_EQ(int32_arr_WS->Value(1), int32_arr->Value(1));
  // seed = 0 => hash = hash without seed
  EXPECT_EQ(int32_arr_WS->Value(2), int32_arr->Value(1));
  // different inputs => different outputs
  EXPECT_NE(int32_arr_WS->Value(3), int32_arr_WS->Value(4));
  // hash with, without seed are not equal
  EXPECT_NE(int32_arr_WS->Value(4), int32_arr->Value(4));
}

TEST_F(TestDecimal, TestHash64WithSeed) {
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_dec_1 = field("dec1", decimal_type);
  auto field_dec_2 = field("dec2", decimal_type);
  auto schema = arrow::schema({field_dec_1, field_dec_2});

  auto res = field("hash64_with_seed", arrow::int64());

  auto field_1_nodePtr = TreeExprBuilder::MakeField(field_dec_1);
  auto field_2_nodePtr = TreeExprBuilder::MakeField(field_dec_2);

  auto hash64 =
      TreeExprBuilder::MakeFunction("hash64", {field_2_nodePtr}, arrow::int64());
  auto hash64_with_seed =
      TreeExprBuilder::MakeFunction("hash64", {field_1_nodePtr, hash64}, arrow::int64());
  auto expr = TreeExprBuilder::MakeExpression(hash64, field("hash64", arrow::int64()));
  auto exprWS = TreeExprBuilder::MakeExpression(hash64_with_seed, res);

  auto exprs = std::vector<ExpressionPtr>{expr, exprWS};

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 5;
  auto validity_1 = {false, false, true, true, true};

  auto array_dec_1 = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity_1);

  auto validity_2 = {false, true, false, true, true};

  auto array_dec_2 = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"1.51", "1.23", "1.23", "-1.23", "-1.24"}, scale),
      validity_2);

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array_dec_1, array_dec_2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(0));
  auto int64_arr_WS = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  // seed 0, null decimal
  EXPECT_EQ(int64_arr_WS->Value(0), 0);
  // null decimal => hash = seed
  EXPECT_EQ(int64_arr_WS->Value(1), int64_arr->Value(1));
  // seed = 0 => hash = hash without seed
  EXPECT_EQ(int64_arr_WS->Value(2), int64_arr->Value(1));
  // different inputs => different outputs
  EXPECT_NE(int64_arr_WS->Value(3), int64_arr_WS->Value(4));
  // hash with, without seed are not equal
  EXPECT_NE(int64_arr_WS->Value(4), int64_arr->Value(4));
}

TEST_F(TestDecimal, TestNullDecimalConstant) {
  // schema for input fields
  constexpr int32_t precision = 36;
  constexpr int32_t scale = 18;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field_b = field("b", decimal_type);
  auto field_c = field("c", arrow::boolean());
  auto schema = arrow::schema({field_b, field_c});

  // output fields
  auto field_result = field("res", decimal_type);

  // build expression.
  // if (c)
  //   null
  // else
  //   b
  auto node_a = TreeExprBuilder::MakeNull(decimal_type);
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

  auto array_b =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"2", "3", "4", "5"}, scale),
                            {true, true, true, true});

  auto array_c = MakeArrowArrayBool({true, false, true, false}, {true, true, true, true});

  // expected output
  auto exp =
      MakeArrowArrayDecimal(decimal_type, MakeDecimalVector({"0", "3", "3", "5"}, scale),
                            {false, true, false, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_b, array_c});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  DCHECK_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestDecimal, TestCastVarCharDecimal) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  auto field_dec = field("dec", decimal_type);
  auto field_res_str = field("res_str", utf8());
  auto field_res_str_1 = field("res_str_1", utf8());
  auto schema = arrow::schema({field_dec, field_res_str, field_res_str_1});

  // output fields
  auto res_str = field("res_str", utf8());
  auto equals_res_bool = field("equals_res", boolean());

  // build expressions.
  auto node_dec = TreeExprBuilder::MakeField(field_dec);
  auto node_res_str = TreeExprBuilder::MakeField(field_res_str);
  auto node_res_str_1 = TreeExprBuilder::MakeField(field_res_str_1);
  // limits decimal string to input length
  auto str_len_limit = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(5));
  auto str_len_limit_1 = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(1));
  auto cast_varchar =
      TreeExprBuilder::MakeFunction("castVARCHAR", {node_dec, str_len_limit}, utf8());
  auto cast_varchar_1 =
      TreeExprBuilder::MakeFunction("castVARCHAR", {node_dec, str_len_limit_1}, utf8());
  auto equals =
      TreeExprBuilder::MakeFunction("equal", {cast_varchar, node_res_str}, boolean());
  auto equals_1 =
      TreeExprBuilder::MakeFunction("equal", {cast_varchar_1, node_res_str_1}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(equals, equals_res_bool);
  auto expr_1 = TreeExprBuilder::MakeExpression(equals_1, equals_res_bool);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;

  auto status = Projector::Make(schema, {expr, expr_1}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_dec = MakeArrowArrayDecimal(
      decimal_type,
      MakeDecimalVector({"10.51", "1.23", "100.23", "-1000.23", "-0000.10"}, scale),
      {true, false, true, true, true});
  auto array_str_res = MakeArrowArrayUtf8({"10.51", "-null-", "100.2", "-1000", "-0.10"},
                                          {true, false, true, true, true});
  auto array_str_res_1 =
      MakeArrowArrayUtf8({"1", "-null-", "1", "-", "-"}, {true, false, true, true, true});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_dec, array_str_res, array_str_res_1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  auto exp = MakeArrowArrayBool({true, false, true, true, true},
                                {true, false, true, true, true});
  auto exp_1 = MakeArrowArrayBool({true, false, true, true, true},
                                  {true, false, true, true, true});
  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs[0]);
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs[1]);
}

TEST_F(TestDecimal, TestCastDecimalVarChar) {
  // schema for input fields
  constexpr int32_t precision = 4;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  auto field_str = field("in_str", utf8());
  auto schema = arrow::schema({field_str});

  // output fields
  auto res_dec = field("res_dec", decimal_type);

  // build expressions.
  auto node_str = TreeExprBuilder::MakeField(field_str);
  auto cast_decimal =
      TreeExprBuilder::MakeFunction("castDECIMAL", {node_str}, decimal_type);
  auto expr = TreeExprBuilder::MakeExpression(cast_decimal, res_dec);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;

  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;

  auto array_str = MakeArrowArrayUtf8({"10.5134", "-0.0", "-0.1", "10.516", "-1000"},
                                      {true, false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_str});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  auto array_dec = MakeArrowArrayDecimal(
      decimal_type, MakeDecimalVector({"10.51", "1.23", "-0.10", "10.52", "0.00"}, scale),
      {true, false, true, true, true});
  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(array_dec, outputs[0]);
}

TEST_F(TestDecimal, TestCastDecimalVarCharInvalidInput) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 0;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  auto field_str = field("in_str", utf8());
  auto schema = arrow::schema({field_str});

  // output fields
  auto res_dec = field("res_dec", decimal_type);

  // build expressions.
  auto node_str = TreeExprBuilder::MakeField(field_str);
  auto cast_decimal =
      TreeExprBuilder::MakeFunction("castDECIMAL", {node_str}, decimal_type);
  auto expr = TreeExprBuilder::MakeExpression(cast_decimal, res_dec);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;

  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;

  // imvalid input
  auto invalid_in = MakeArrowArrayUtf8({"a10.5134", "-0.0", "-0.1", "10.516", "-1000"},
                                       {true, false, true, true, true});

  // prepare input record batch
  auto in_batch_1 = arrow::RecordBatch::Make(schema, num_records, {invalid_in});

  // Evaluate expression
  arrow::ArrayVector outputs_1;
  status = projector->Evaluate(*in_batch_1, pool_, &outputs_1);
  EXPECT_FALSE(status.ok()) << status.message();
  EXPECT_TRUE(status.message().find("not a valid decimal number") != std::string::npos);
}

TEST_F(TestDecimal, TestVarCharDecimalNestedCast) {
  // schema for input fields
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 2;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);

  auto field_dec = field("dec", decimal_type);
  auto schema = arrow::schema({field_dec});

  // output fields
  auto field_dec_res = field("dec_res", decimal_type);

  // build expressions.
  auto node_dec = TreeExprBuilder::MakeField(field_dec);

  // limits decimal string to input length
  auto str_len_limit = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(5));
  auto cast_varchar =
      TreeExprBuilder::MakeFunction("castVARCHAR", {node_dec, str_len_limit}, utf8());
  auto cast_decimal =
      TreeExprBuilder::MakeFunction("castDECIMAL", {cast_varchar}, decimal_type);

  auto expr = TreeExprBuilder::MakeExpression(cast_decimal, field_dec_res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;

  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_dec = MakeArrowArrayDecimal(
      decimal_type,
      MakeDecimalVector({"10.51", "1.23", "100.23", "-1000.23", "-0000.10"}, scale),
      {true, false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_dec});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  auto array_dec_res = MakeArrowArrayDecimal(
      decimal_type,
      MakeDecimalVector({"10.51", "1.23", "100.20", "-1000.00", "-0.10"}, scale),
      {true, false, true, true, true});
  EXPECT_ARROW_ARRAY_EQUALS(array_dec_res, outputs[0]);
}

}  // namespace gandiva

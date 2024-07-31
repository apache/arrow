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
using arrow::date64;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

class TestUtf8 : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestUtf8, TestSimple) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_1 = field("res1", int32());
  auto res_2 = field("res2", boolean());
  auto res_3 = field("res3", int32());

  // build expressions.
  // octet_length(a)
  // octet_length(a) == bit_length(a) / 8
  // length(a)
  auto expr_a = TreeExprBuilder::MakeExpression("octet_length", {field_a}, res_1);

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto octet_length = TreeExprBuilder::MakeFunction("octet_length", {node_a}, int32());
  auto literal_8 = TreeExprBuilder::MakeLiteral((int32_t)8);
  auto bit_length = TreeExprBuilder::MakeFunction("bit_length", {node_a}, int32());
  auto div_8 = TreeExprBuilder::MakeFunction("divide", {bit_length, literal_8}, int32());
  auto is_equal =
      TreeExprBuilder::MakeFunction("equal", {octet_length, div_8}, boolean());
  auto expr_b = TreeExprBuilder::MakeExpression(is_equal, res_2);
  auto expr_c = TreeExprBuilder::MakeExpression("length", {field_a}, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_a, expr_b, expr_c}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUtf8({"foo", "hello", "bye", "hi", "मदन"},
                                    {true, true, false, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt32({3, 5, 0, 2, 9}, {true, true, false, true, true});
  auto exp_2 = MakeArrowArrayBool({true, true, false, true, true},
                                  {true, true, false, true, true});
  auto exp_3 = MakeArrowArrayInt32({3, 5, 0, 2, 3}, {true, true, false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_3, outputs.at(2));
}

TEST_F(TestUtf8, TestLiteral) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // a == literal(s)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_s = TreeExprBuilder::MakeStringLiteral("hello");
  auto is_equal = TreeExprBuilder::MakeFunction("equal", {node_a, literal_s}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_equal, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {true, true, true, false});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, false, false}, {true, true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestNullLiteral) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // a == literal(null)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_null = TreeExprBuilder::MakeNull(arrow::utf8());
  auto is_equal =
      TreeExprBuilder::MakeFunction("equal", {node_a, literal_null}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_equal, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {true, true, true, false});

  // expected output
  auto exp =
      MakeArrowArrayBool({false, false, false, false}, {false, false, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestLike) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // like(literal(s), a)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_s = TreeExprBuilder::MakeStringLiteral("%spark%");
  auto is_like = TreeExprBuilder::MakeFunction("like", {node_a, literal_s}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_like, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"park", "sparkle", "bright spark and fire", "spark"},
                                    {true, true, true, true});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, true, true}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestIlike) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // ilike(literal(s), a)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_s = TreeExprBuilder::MakeStringLiteral("%sparK%");
  auto is_like = TreeExprBuilder::MakeFunction("ilike", {node_a, literal_s}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_like, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"park", "sParKle", "bright spark and fire", "spark"},
                                    {true, true, true, true});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, true, true}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestLikeWithEscape) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // like(literal(s), a, '\')

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_s = TreeExprBuilder::MakeStringLiteral("%pa\\%rk%");
  auto escape_char = TreeExprBuilder::MakeStringLiteral("\\");
  auto is_like =
      TreeExprBuilder::MakeFunction("like", {node_a, literal_s, escape_char}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_like, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8(
      {"park", "spa%rkle", "bright spa%rk and fire", "spark"}, {true, true, true, true});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, true, false}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestBeginsEnds) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res1 = field("res1", boolean());
  auto res2 = field("res2", boolean());

  // build expressions.
  // like(literal("spark%"), a)
  // like(literal("%spark"), a)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_begin = TreeExprBuilder::MakeStringLiteral("spark%");
  auto is_like1 =
      TreeExprBuilder::MakeFunction("like", {node_a, literal_begin}, boolean());
  auto expr1 = TreeExprBuilder::MakeExpression(is_like1, res1);

  auto literal_end = TreeExprBuilder::MakeStringLiteral("%spark");
  auto is_like2 = TreeExprBuilder::MakeFunction("like", {node_a, literal_end}, boolean());
  auto expr2 = TreeExprBuilder::MakeExpression(is_like2, res2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr1, expr2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"park", "sparkle", "bright spark and fire", "fiery spark"},
                         {true, true, true, true});

  // expected output
  auto exp1 = MakeArrowArrayBool({false, true, false, false}, {true, true, true, true});
  auto exp2 = MakeArrowArrayBool({false, false, false, true}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp2, outputs.at(1));
}

TEST_F(TestUtf8, TestInternalAllocs) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // like(upper(a), literal("%SPARK%"))

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper_a = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto literal_spark = TreeExprBuilder::MakeStringLiteral("%SPARK%");
  auto is_like =
      TreeExprBuilder::MakeFunction("like", {upper_a, literal_spark}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_like, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUtf8(
      {"park", "Sparkle", "bright spark and fire", "fiery SPARK", "मदन"},
      {true, true, false, true, true});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, false, true, false},
                                {true, true, false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestCastDate) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_1 = field("res1", int64());

  // build expressions.
  // extractYear(castDATE(a))
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto cast_function = TreeExprBuilder::MakeFunction("castDATE", {node_a}, date64());
  auto extract_year =
      TreeExprBuilder::MakeFunction("extractYear", {cast_function}, int64());
  auto expr = TreeExprBuilder::MakeExpression(extract_year, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"1967-12-1", "67-12-01", "incorrect", "67-45-11"},
                                    {true, true, false, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt64({1967, 2067, 0, 0}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_EQ(status.code(), StatusCode::ExecutionError);
  std::string expected_error = "Not a valid date value ";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);

  auto array_a_2 = MakeArrowArrayUtf8({"1967-12-1", "67-12-01", "67-1-1", "91-1-1"},
                                      {true, true, true, true});
  auto exp_2 = MakeArrowArrayInt64({1967, 2067, 2067, 1991}, {true, true, true, true});
  auto in_batch_2 = arrow::RecordBatch::Make(schema, num_records, {array_a_2});
  arrow::ArrayVector outputs2;
  status = projector->Evaluate(*in_batch_2, pool_, &outputs2);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs2.at(0));
}

TEST_F(TestUtf8, TestToDateNoError) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_1 = field("res1", int64());

  // build expressions.
  // extractYear(castDATE(a))
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeStringLiteral("YYYY-MM-DD");
  auto node_c = TreeExprBuilder::MakeLiteral(1);

  auto cast_function =
      TreeExprBuilder::MakeFunction("to_date", {node_a, node_b, node_c}, date64());
  auto extract_year =
      TreeExprBuilder::MakeFunction("extractYear", {cast_function}, int64());
  auto expr = TreeExprBuilder::MakeExpression(extract_year, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"1967-12-1", "67-12-01", "incorrect", "67-45-11"},
                                    {true, true, false, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt64({1967, 67, 0, 0}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));

  // Create a row-batch with some sample data
  auto array_a_2 = MakeArrowArrayUtf8(
      {"1967-12-1", "1967-12-01", "1967-11-11", "1991-11-11"}, {true, true, true, true});
  auto exp_2 = MakeArrowArrayInt64({1967, 1967, 1967, 1991}, {true, true, true, true});
  auto in_batch_2 = arrow::RecordBatch::Make(schema, num_records, {array_a_2});
  arrow::ArrayVector outputs2;
  status = projector->Evaluate(*in_batch_2, pool_, &outputs2);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs2.at(0));
}

TEST_F(TestUtf8, TestToDateError) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_1 = field("res1", int64());

  // build expressions.
  // extractYear(castDATE(a))
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeStringLiteral("YYYY-MM-DD");
  auto node_c = TreeExprBuilder::MakeLiteral(0);

  auto cast_function =
      TreeExprBuilder::MakeFunction("to_date", {node_a, node_b, node_c}, date64());
  auto extract_year =
      TreeExprBuilder::MakeFunction("extractYear", {cast_function}, int64());
  auto expr = TreeExprBuilder::MakeExpression(extract_year, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"1967-12-1", "67-12-01", "incorrect", "67-45-11"},
                                    {true, true, false, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt64({1967, 67, 0, 0}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_EQ(status.code(), StatusCode::ExecutionError);
  std::string expected_error = "Error parsing value 67-45-11 for given format";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos)
      << status.message();
}

TEST_F(TestUtf8, TestIsNull) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // build expressions
  auto exprs = std::vector<ExpressionPtr>{
      TreeExprBuilder::MakeExpression("isnull", {field_a}, field("is_null", boolean())),
      TreeExprBuilder::MakeExpression("isnotnull", {field_a},
                                      field("is_not_null", boolean())),
  };

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  DCHECK_OK(status);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"hello", "world", "incorrect", "universe"},
                                    {true, true, false, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);

  // validate results
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({false, false, true, false}),
                            outputs[0]);  // isnull
  EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool({true, true, false, true}),
                            outputs[1]);  // isnotnull
}

TEST_F(TestUtf8, TestVarlenOutput) {
  // schema for input fields
  auto field_a = field("a", boolean());
  auto schema = arrow::schema({field_a});

  // build expressions.
  // if (a) literal_hi else literal_bye
  auto if_node = TreeExprBuilder::MakeIf(
      TreeExprBuilder::MakeField(field_a), TreeExprBuilder::MakeStringLiteral("hi"),
      TreeExprBuilder::MakeStringLiteral("bye"), utf8());
  auto expr = TreeExprBuilder::MakeExpression(if_node, field("res", utf8()));

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;

  // assert that it fails gracefully.
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_in =
      MakeArrowArrayBool({true, false, false, false}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_in});

  // Evaluate expression
  arrow::ArrayVector outputs;
  ASSERT_OK(projector->Evaluate(*in_batch, pool_, &outputs));

  // expected output
  auto exp = MakeArrowArrayUtf8({"hi", "bye", "bye", "bye"}, {true, true, true, true});

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestUtf8, TestConvertUtf8) {
  // schema for input fields
  auto field_a = field("a", arrow::binary());
  auto field_c = field("c", utf8());
  auto schema = arrow::schema({field_a, field_c});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_c = TreeExprBuilder::MakeField(field_c);

  // define char to replace
  auto node_b = TreeExprBuilder::MakeStringLiteral("z");

  auto convert_replace_utf8 =
      TreeExprBuilder::MakeFunction("convert_replaceUTF8", {node_a, node_b}, utf8());
  auto equals =
      TreeExprBuilder::MakeFunction("equal", {convert_replace_utf8, node_c}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(equals, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;
  auto array_a = MakeArrowArrayUtf8({"ok-\xf8\x28"
                                     "-a",
                                     "all-valid", "ok-\xa0\xa1-valid"},
                                    {true, true, true});

  auto array_b =
      MakeArrowArrayUtf8({"ok-z(-a", "all-valid", "ok-zz-valid"}, {true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  auto exp = MakeArrowArrayBool({true, true, true}, {true, true, true});
  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs[0]);
}

TEST_F(TestUtf8, TestCastVarChar) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_c = field("c", utf8());
  auto schema = arrow::schema({field_a, field_c});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_c = TreeExprBuilder::MakeField(field_c);
  // truncates the string to input length
  auto node_b = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(10));
  auto cast_varchar =
      TreeExprBuilder::MakeFunction("castVARCHAR", {node_a, node_b}, utf8());
  auto equals = TreeExprBuilder::MakeFunction("equal", {cast_varchar, node_c}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(equals, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUtf8(
      {"park", "Sparkle", "bright spark and fire", "fiery SPARK", "मदन"},
      {true, true, false, true, true});

  auto array_b =
      MakeArrowArrayUtf8({"park", "Sparkle", "bright spar", "fiery SPAR", "मदन"},
                         {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  auto exp = MakeArrowArrayBool({true, true, false, true, true},
                                {true, true, false, true, true});
  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs[0]);
}

TEST_F(TestUtf8, TestAscii) {
  // schema for input fields
  auto field0 = field("f0", arrow::utf8());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_asc = field("ascii", arrow::int32());

  // Build expression
  auto asc_expr = TreeExprBuilder::MakeExpression("ascii", {field0}, field_asc);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {asc_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 6;
  auto array0 = MakeArrowArrayUtf8({"ABC", "", "abc", "Hello World", "123", "999"},
                                   {true, true, true, true, true, true});
  // expected output
  auto exp_asc =
      MakeArrowArrayInt32({65, 0, 97, 72, 49, 57}, {true, true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_asc, outputs.at(0));
}

TEST_F(TestUtf8, TestSpace) {
  // schema for input fields
  auto field0 = field("f0", arrow::int64());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_space = field("space", arrow::utf8());

  // Build expression
  auto space_expr = TreeExprBuilder::MakeExpression("space", {field0}, field_space);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {space_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt64({1, 0, -5, 2}, {true, true, true, true});
  // expected output
  auto exp_space = MakeArrowArrayUtf8({" ", "", "", "  "}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_space, outputs.at(0));
}

}  // namespace gandiva

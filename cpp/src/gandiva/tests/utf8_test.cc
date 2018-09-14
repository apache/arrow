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
#include "gandiva/projector.h"
#include "gandiva/status.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
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

  // build expressions.
  // octet_length(a)
  // octet_length(a) == bit_length(a) / 8
  auto expr_a = TreeExprBuilder::MakeExpression("octet_length", {field_a}, res_1);

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto octet_length = TreeExprBuilder::MakeFunction("octet_length", {node_a}, int32());
  auto literal_8 = TreeExprBuilder::MakeLiteral((int32_t)8);
  auto bit_length = TreeExprBuilder::MakeFunction("bit_length", {node_a}, int32());
  auto div_8 = TreeExprBuilder::MakeFunction("divide", {bit_length, literal_8}, int32());
  auto is_equal =
      TreeExprBuilder::MakeFunction("equal", {octet_length, div_8}, boolean());
  auto expr_b = TreeExprBuilder::MakeExpression(is_equal, res_2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr_a, expr_b}, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {true, true, false, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt32({3, 5, 0, 2}, {true, true, false, true});
  auto exp_2 = MakeArrowArrayBool({true, true, false, true}, {true, true, false, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs.at(1));
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
  Status status = Projector::Make(schema, {expr}, &projector);
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
  Status status = Projector::Make(schema, {expr}, &projector);
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
  Status status = Projector::Make(schema, {expr}, &projector);
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

}  // namespace gandiva

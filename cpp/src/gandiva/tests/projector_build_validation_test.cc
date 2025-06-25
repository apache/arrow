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
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class ValidationTestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(ValidationTestProjector, TestNonexistentFunction) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto lt_expr = TreeExprBuilder::MakeExpression("nonexistent_function", {field0, field1},
                                                 field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error =
      "Function bool nonexistent_function(float, float) not supported yet.";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

TEST_F(ValidationTestProjector, TestNotMatchingDataType) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto lt_expr = TreeExprBuilder::MakeExpression(node_f0, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error =
      "Return type of root node float does not match that of expression bool";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

TEST_F(ValidationTestProjector, TestNotSupportedDataType) {
  // schema for input fields
  auto field0 = field("f0", list(int32()));
  auto schema = arrow::schema({field0});

  // output fields
  auto field_result = field("res", list(int32()));

  // Build expression
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto lt_expr = TreeExprBuilder::MakeExpression(node_f0, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error = "Field f0 has unsupported data type list";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

TEST_F(ValidationTestProjector, TestIncorrectSchemaMissingField) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field0});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto lt_expr =
      TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error = "Field f2 not in schema";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

TEST_F(ValidationTestProjector, TestIncorrectSchemaTypeNotMatching) {
  // schema for input fields
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto field2 = field("f2", int32());
  auto schema = arrow::schema({field0, field2});

  // output fields
  auto field_result = field("res", boolean());

  // Build expression
  auto lt_expr =
      TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {lt_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  std::string expected_error =
      "Field definition in schema f2: int32 different from field in expression f2: float";
  EXPECT_TRUE(status.message().find(expected_error) != std::string::npos);
}

TEST_F(ValidationTestProjector, TestIfNotSupportedFunction) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a > b)
  //   a
  // else
  //   b
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto condition =
      TreeExprBuilder::MakeFunction("nonexistent_function", {node_a, node_b}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(condition, node_a, node_b, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
}

TEST_F(ValidationTestProjector, TestIfNotMatchingReturnType) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto condition =
      TreeExprBuilder::MakeFunction("less_than", {node_a, node_b}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(condition, node_a, node_b, boolean());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
}

TEST_F(ValidationTestProjector, TestElseNotMatchingReturnType) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", boolean());
  auto schema = arrow::schema({fielda, fieldb, fieldc});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);
  auto condition =
      TreeExprBuilder::MakeFunction("less_than", {node_a, node_b}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(condition, node_a, node_c, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
}

TEST_F(ValidationTestProjector, TestElseNotSupportedType) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", list(int32()));
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);
  auto condition =
      TreeExprBuilder::MakeFunction("less_than", {node_a, node_b}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(condition, node_a, node_c, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
  EXPECT_EQ(status.code(), StatusCode::ExpressionValidationError);
}

TEST_F(ValidationTestProjector, TestAndMinChildren) {
  // schema for input fields
  auto fielda = field("a", boolean());
  auto schema = arrow::schema({fielda});

  // output fields
  auto field_result = field("res", boolean());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto and_node = TreeExprBuilder::MakeAnd({node_a});

  auto expr = TreeExprBuilder::MakeExpression(and_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
}

TEST_F(ValidationTestProjector, TestAndBooleanArgType) {
  // schema for input fields
  auto fielda = field("a", boolean());
  auto fieldb = field("b", int32());
  auto schema = arrow::schema({fielda, fieldb});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto and_node = TreeExprBuilder::MakeAnd({node_a, node_b});

  auto expr = TreeExprBuilder::MakeExpression(and_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.IsExpressionValidationError());
}

}  // namespace gandiva

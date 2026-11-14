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

TEST_F(ValidationTestProjector, TestCachedOutputTypeValidation) {
  auto schema = arrow::schema({field("cached_output_type", int32())});
  auto configuration = TestConfiguration();
  auto node = TreeExprBuilder::MakeLiteral(int32_t{1});

  // Same expression tree, compiled once with its own output type.
  std::shared_ptr<Projector> projector;
  auto valid = TreeExprBuilder::MakeExpression(node, field("res", int32()));
  ASSERT_OK(Projector::Make(schema, {valid}, configuration, &projector));

  // A different output type must not reuse the cached code and skip validation.
  auto invalid = TreeExprBuilder::MakeExpression(node, field("res", arrow::int64()));
  auto status = Projector::Make(schema, {invalid}, configuration, &projector);
  EXPECT_TRUE(status.IsExpressionValidationError()) << status.ToString();
}

// Validation compares a field node with the schema field, including nullability. The
// key has to keep that difference, or a cache hit skips the validation error.
TEST_F(ValidationTestProjector, TestCachedFieldNullabilityValidation) {
  auto configuration = TestConfiguration();
  // A unique field name, so that no other test seeds the cache for these expressions.
  auto make_expression = [](const FieldPtr& node_field) {
    auto node = TreeExprBuilder::MakeFunction(
        "add", {TreeExprBuilder::MakeField(node_field), TreeExprBuilder::MakeLiteral(1)},
        int32());
    return TreeExprBuilder::MakeExpression(node, field("res", int32()));
  };

  // Cold: nothing is cached yet, so validation runs and rejects the mismatch.
  auto cold_schema = arrow::schema({field("cold_nullability_field", int32(), true)});
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      cold_schema, {make_expression(field("cold_nullability_field", int32(), false))},
      configuration, &projector);
  EXPECT_TRUE(status.IsExpressionValidationError()) << status.ToString();

  // Warm: the matching expression is compiled and cached first.
  auto nullable_field = field("warm_nullability_field", int32(), true);
  auto required_field = field("warm_nullability_field", int32(), false);
  auto schema = arrow::schema({nullable_field});
  ASSERT_OK(Projector::Make(schema, {make_expression(nullable_field)}, configuration,
                            &projector));

  // The same schema with a field node of the other nullability must still be rejected.
  // Ask twice, so a hit on a cached entry is possible on both calls.
  for (int i = 0; i < 2; ++i) {
    status = Projector::Make(schema, {make_expression(required_field)}, configuration,
                             &projector);
    EXPECT_TRUE(status.IsExpressionValidationError())
        << "call " << i << ": " << status.ToString();
  }

  // The matching expression still builds, and the other direction is rejected too.
  ASSERT_OK(Projector::Make(schema, {make_expression(nullable_field)}, configuration,
                            &projector));
  auto required_schema = arrow::schema({required_field});
  ASSERT_OK(Projector::Make(required_schema, {make_expression(required_field)},
                            configuration, &projector));
  status = Projector::Make(required_schema, {make_expression(nullable_field)},
                           configuration, &projector);
  EXPECT_TRUE(status.IsExpressionValidationError()) << status.ToString();
}

}  // namespace gandiva

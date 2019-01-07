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
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestIn : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

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
}  // namespace gandiva

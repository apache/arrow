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
#include "arrow.h"
#include "projector.h"

namespace gandiva{
class TestToNumber : public ::testing::Test {
  public:
      void SetUp() { pool_ = arrow::default_memory_pool(); }

  protected:
      arrow::MemoryPool* pool_;
};

TEST_F(TestToNumber, TestToNumber){
  auto field0 = field("f0", arrow::utf8());
  auto schema = arrow::schema({field0});
  auto node = TreeExprBuilder::MakeField(field0);

  auto res = field("r0", arrow::float64());
  auto pattern_literal = TreeExprBuilder::MakeStringLiteral("##,###,###.###");
  auto to_number_node1 = TreeExprBuilder::MakeFunction("to_number",{node, pattern_literal},arrow::float64());
  auto expr = TreeExprBuilder::MakeExpression(to_number_node1,res);

  // Build filter for the expression
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data

  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"500.6667", "-600,000.3333", "0", ""}, {true, true, true, true});
  auto exp = MakeArrowArrayFloat64({500.6667, -600000.3333, 0, 0});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto double_arr = std::dynamic_pointer_cast<arrow::DoubleArray>(outputs.at(0));
  EXPECT_EQ(double_arr->null_count(), 0);
  EXPECT_ARROW_ARRAY_EQUALS(exp, double_arr);
}
}  // namespace gandiva

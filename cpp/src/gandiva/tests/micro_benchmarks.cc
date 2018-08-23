// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/status.h"
#include "gandiva/tree_expr_builder.h"
#include "integ/test_util.h"
#include "integ/timed_evaluate.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;

class TestBenchmarks : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestBenchmarks, TimedTestAdd3) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});

  // output field
  auto field_sum = field("add", int64());

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      int64());
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, int64());

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {sum_expr}, &projector);
  EXPECT_TRUE(status.ok());

  int64_t elapsed_millis;
  Int64DataGenerator data_generator;
  status = TimedEvaluate<arrow::Int64Type, int64_t>(schema, projector, data_generator,
                                                    pool_, 100 * MILLION, 16 * THOUSAND,
                                                    elapsed_millis);
  ASSERT_TRUE(status.ok());
  std::cout << "Time taken for Add3 " << elapsed_millis << " ms\n";
}

TEST_F(TestBenchmarks, TimedTestBigNested) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto schema = arrow::schema({fielda});

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a < 10)
  //   10
  // else if (a < 20)
  //   20
  // ..
  // ..
  // else if (a < 190)
  //   190
  // else
  //   200
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto top_node = TreeExprBuilder::MakeLiteral(200);
  for (int thresh = 190; thresh > 0; thresh -= 10) {
    auto literal = TreeExprBuilder::MakeLiteral(thresh);
    auto condition =
        TreeExprBuilder::MakeFunction("less_than", {node_a, literal}, boolean());
    auto if_node = TreeExprBuilder::MakeIf(condition, literal, top_node, int32());
    top_node = if_node;
  }
  auto expr = TreeExprBuilder::MakeExpression(top_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok());

  int64_t elapsed_millis;
  BoundedInt32DataGenerator data_generator(250);
  status = TimedEvaluate<arrow::Int32Type, int32_t>(schema, projector, data_generator,
                                                    pool_, 100 * MILLION, 16 * THOUSAND,
                                                    elapsed_millis);
  ASSERT_TRUE(status.ok());
  std::cout << "Time taken for BigNestedIf " << elapsed_millis << " ms\n";
}

}  // namespace gandiva

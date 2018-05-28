/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>
#include <utility>
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"
#include "expr/evaluator.h"
#include "expr/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::float32;
using arrow::boolean;
using arrow::Status;

class TestEvaluator : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

/*
 * Helper function to create an arrow-array of type ARROWTYPE
 * from primitive vectors of data & validity.
 *
 * arrow/test-util.h has good utility classes for this purpose.
 * Using those
 */
template<typename TYPE, typename C_TYPE>
static ArrayPtr MakeArrowArray(std::vector<C_TYPE> values,
                                     std::vector<bool> validity) {
  ArrayPtr out;
  arrow::ArrayFromVector<TYPE, C_TYPE>(validity, values, &out);
  return out;
}
#define MakeArrowArrayInt32 MakeArrowArray<arrow::Int32Type, int32_t>
#define MakeArrowArrayFloat32 MakeArrowArray<arrow::FloatType, float_t>
#define MakeArrowArrayBool MakeArrowArray<arrow::BooleanType, bool>

TEST_F(TestEvaluator, TestIntSumSub) {
  /* schema for input fields */
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});

  /* output fields */
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  /*
   * Build expression
   */
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr = TreeExprBuilder::MakeExpression("subtract", {field0, field1},
                                                  field_sub);

  /*
   * Build an evaluator for the expressions.
   */
  auto evaluator = Evaluator::Make(schema, {sum_expr, sub_expr}, pool_);

  /* Create a row-batch with some sample data */
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({ 1, 2, 3, 4 }, { true, true, true, false });
  auto array1 = MakeArrowArrayInt32({ 11, 13, 15, 17 }, { true, true, false, true });
  /* expected output */
  auto exp_sum = MakeArrowArrayInt32({ 12, 15, 0, 0 }, { true, true, false, false });
  auto exp_sub = MakeArrowArrayInt32({ -10, -11, 0, 0 }, { true, true, false, false });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  /*
   * Evaluate expression
   */
  auto outputs = evaluator->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_TRUE(exp_sum->Equals(outputs.at(0)));
  EXPECT_TRUE(exp_sub->Equals(outputs.at(1)));
}

TEST_F(TestEvaluator, TestFloatLessThan) {
  /* schema for input fields */
  auto field0 = field("f0", float32());
  auto field1 = field("f2", float32());
  auto schema = arrow::schema({field0, field1});

  /* output fields */
  auto field_result = field("res", boolean());

  /*
   * Build expression
   */
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1},
                                                 field_result);

  /*
   * Build an evaluator for the expressions.
   */
  auto evaluator = Evaluator::Make(schema, {lt_expr}, pool_);

  /* Create a row-batch with some sample data */
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  auto array1 = MakeArrowArrayFloat32({ 4.0, 3.4, 6.8 }, { true, true, true });
  /* expected output */
  auto exp = MakeArrowArrayBool({ true, false, false }, { true, true, false });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  /*
   * Evaluate expression
   */
  auto outputs = evaluator->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_TRUE(exp->Equals(outputs.at(0)));
}

TEST_F(TestEvaluator, TestIsNotNull) {
  /* schema for input fields */
  auto field0 = field("f0", float32());
  auto schema = arrow::schema({field0});

  /* output fields */
  auto field_result = field("res", boolean());

  /*
   * Build expression
   */
  auto myexpr = TreeExprBuilder::MakeExpression("isnotnull", {field0}, field_result);

  /*
   * Build an evaluator for the expressions.
   */
  auto evaluator = Evaluator::Make(schema, {myexpr}, pool_);

  /* Create a row-batch with some sample data */
  int num_records = 3;
  auto array0 = MakeArrowArrayFloat32({ 1.0, 8.9, 3.0 }, { true, true, false });
  /* expected output */
  auto exp = MakeArrowArrayBool({ true, true, false }, { true, true, true });

  /* prepare input record batch */
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  /*
   * Evaluate expression
   */
  auto outputs = evaluator->Evaluate(*in_batch);

  /*
   * Validate results
   */
  EXPECT_TRUE(exp->Equals(outputs.at(0)));
}

} // namespace gandiva

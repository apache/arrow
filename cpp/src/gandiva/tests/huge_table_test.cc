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
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class DISABLED_TestHugeProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

class DISABLED_TestHugeFilter : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(DISABLED_TestHugeProjector, SimpleTestSumHuge) {
  auto atype = arrow::TypeTraits<arrow::Int32Type>::type_singleton();

  // schema for input fields
  auto field0 = field("f0", atype);
  auto field1 = field("f1", atype);
  auto schema = arrow::schema({field0, field1});

  // output fields
  auto field_sum = field("add", atype);

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  // Cause an overflow in int32_t
  int64_t num_records = static_cast<int64_t>(INT32_MAX) + 3;
  std::vector<int32_t> input0 = {2, 29, 5, 37, 11, 59, 17, 19};
  std::vector<int32_t> input1 = {23, 3, 31, 7, 41, 47, 13};
  std::vector<bool> validity;

  std::vector<int32_t> arr1;
  std::vector<int32_t> arr2;
  // expected output
  std::vector<int32_t> sum1;

  for (int64_t i = 0; i < num_records; i++) {
    arr1.push_back(input0[i % 8]);
    arr2.push_back(input1[i % 7]);
    sum1.push_back(input0[i % 8] + input1[i % 7]);
    validity.push_back(true);
  }

  auto exp_sum = MakeArrowArray<arrow::Int32Type, int32_t>(sum1, validity);
  auto array0 = MakeArrowArray<arrow::Int32Type, int32_t>(arr1, validity);
  auto array1 = MakeArrowArray<arrow::Int32Type, int32_t>(arr2, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_sum, outputs.at(0));
}

TEST_F(DISABLED_TestHugeFilter, TestSimpleHugeFilter) {
  // Create a row-batch with some sample data
  // Cause an overflow in int32_t
  int64_t num_records = static_cast<int64_t>(INT32_MAX) + 3;
  std::vector<int32_t> input0 = {2, 29, 5, 37, 11, 59, 17, 19};
  std::vector<int32_t> input1 = {23, 3, 31, 7, 41, 47, 13};
  std::vector<bool> validity;

  std::vector<int32_t> arr1;
  std::vector<int32_t> arr2;
  // expected output
  std::vector<uint64_t> sel;

  for (int64_t i = 0; i < num_records; i++) {
    arr1.push_back(input0[i % 8]);
    arr2.push_back(input1[i % 7]);
    if (input0[i % 8] + input1[i % 7] > 50) {
      sel.push_back(i);
    }
    validity.push_back(true);
  }

  auto exp = MakeArrowArrayUint64(sel);

  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 50
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_50 = TreeExprBuilder::MakeLiteral((int32_t)50);
  auto less_than_50 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_50},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_50);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {arr1, arr2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt64(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

}  // namespace gandiva

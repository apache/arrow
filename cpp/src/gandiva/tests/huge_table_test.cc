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

#include "gandiva/projector.h"
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
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

TEST_F(DISABLED_TestHugeProjector, SimpleTestSum) {
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
  Status status = Projector::Make(schema, {sum_expr}, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int64_t num_records = INT32_MAX + 3; // Cause an overflow in int32_t
  std::vector<int32_t> input0 = {2, 29, 5, 37, 11, 59, 17, 19};
  std::vector<int32_t> input1 = {23, 3, 31, 7, 41, 47, 13};
  std::vector<bool> validity;

  std::vector<int32_t> arr1;
  std::vector<int32_t> arr2;
  // expected output
  std::vector<int32_t> sum1;
  std::vector<int32_t> sum2;

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

}  // namespace gandiva

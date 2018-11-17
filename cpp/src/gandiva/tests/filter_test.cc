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

#include "gandiva/filter.h"
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestFilter : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestFilter, TestFilterCache) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // same schema and condition, should return the same filter as above.
  std::shared_ptr<Filter> cached_filter;
  status = Filter::Make(schema, condition, &cached_filter);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(cached_filter.get() == filter.get());

  // schema is different should return a new filter.
  auto field2 = field("f2", int32());
  auto different_schema = arrow::schema({field0, field1, field2});
  std::shared_ptr<Filter> should_be_new_filter;
  status = Filter::Make(different_schema, condition, &should_be_new_filter);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(cached_filter.get() != should_be_new_filter.get());

  // condition is different, should return a new filter.
  auto greater_than_10 = TreeExprBuilder::MakeFunction(
      "greater_than", {sum_func, literal_10}, arrow::boolean());
  auto new_condition = TreeExprBuilder::MakeCondition(greater_than_10);
  std::shared_ptr<Filter> should_be_new_filter1;
  status = Filter::Make(schema, new_condition, &should_be_new_filter1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(cached_filter.get() != should_be_new_filter1.get());
}

TEST_F(TestFilter, TestSimple) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 4});

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

TEST_F(TestFilter, TestSimpleCustomConfig) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 != f1
  auto condition = TreeExprBuilder::MakeCondition("not_equal", {field0, field1});

  ConfigurationBuilder config_builder;
  std::shared_ptr<Configuration> config = config_builder.build();

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto array1 = MakeArrowArrayInt32({11, 2, 3, 17}, {true, true, false, true});
  // expected output
  auto exp = MakeArrowArrayUint16({0});

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

TEST_F(TestFilter, TestZeroCopy) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // Build condition
  auto condition = TreeExprBuilder::MakeCondition("isnotnull", {field0});

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayUint16({0, 1, 2});

  // allocate selection buffers
  int64_t data_sz = sizeof(int16_t) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, data_buf, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

TEST_F(TestFilter, TestZeroCopyNegative) {
  ArrayPtr output;

  // schema for input fields
  auto field0 = field("f0", int32());
  auto schema = arrow::schema({field0});

  // Build expression
  auto condition = TreeExprBuilder::MakeCondition("isnotnull", {field0});

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4}, {true, true, true, false});
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // expected output
  auto exp = MakeArrowArrayInt16({0, 1, 2});

  // allocate output buffers
  int64_t data_sz = sizeof(int16_t) * num_records;
  std::unique_ptr<uint8_t[]> data(new uint8_t[data_sz]);
  std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(data.get(), data_sz);

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, data_buf, &selection_vector);
  EXPECT_TRUE(status.ok());

  // the batch can't be empty.
  auto bad_batch = arrow::RecordBatch::Make(schema, 0 /*num_records*/, {array0});
  status = filter->Evaluate(*bad_batch, selection_vector);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the selection_vector can't be null.
  std::shared_ptr<SelectionVector> null_selection;
  status = filter->Evaluate(*in_batch, null_selection);
  EXPECT_EQ(status.code(), StatusCode::Invalid);

  // the selection vector must be suitably sized.
  std::shared_ptr<SelectionVector> bad_selection;
  status = SelectionVector::MakeInt16(num_records - 1, data_buf, &bad_selection);
  EXPECT_TRUE(status.ok());

  status = filter->Evaluate(*in_batch, bad_selection);
  EXPECT_EQ(status.code(), StatusCode::Invalid);
}

TEST_F(TestFilter, TestSimpleSVInt32) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);

  std::shared_ptr<Filter> filter;
  Status status = Filter::Make(schema, condition, &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint32({0, 4});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt32(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

}  // namespace gandiva

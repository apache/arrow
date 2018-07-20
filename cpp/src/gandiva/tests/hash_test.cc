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

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

class TestHash : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestHash, TestSimple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());

  // build expression.
  // hash32(a, 10)
  // hash64(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a, literal_10}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a}, int64());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr_0, expr_1}, pool_, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }
}

TEST_F(TestHash, TestBuf) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());

  // build expressions.
  // hash32(a)
  // hash64(a, 10)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_10 = TreeExprBuilder::MakeLiteral((int64_t)10);
  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a, literal_10}, int64());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {expr_0, expr_1}, pool_, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }
}

}  // namespace gandiva

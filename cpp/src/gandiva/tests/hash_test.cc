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

#include <sstream>

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
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
  auto status =
      Projector::Make(schema, {expr_0, expr_1}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 10);
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
  auto literal_10 = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(10));
  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a, literal_10}, int64());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1}, TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 10);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }
}

TEST_F(TestHash, TestSha256Simple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int64());
  auto field_c = field("c", float32());
  auto field_d = field("d", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_1 = field("res1", utf8());
  auto res_2 = field("res2", utf8());
  auto res_3 = field("res3", utf8());

  // build expressions.
  // hashSHA256(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha256_1 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha256_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha256_2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha256_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha256_3 = TreeExprBuilder::MakeFunction("hashSHA256", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha256_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashSha256_4 = TreeExprBuilder::MakeFunction("hashSHA256", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashSha256_4, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3},
                                TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 2;
  auto validity_array = {false, true};

  auto array_int32 = MakeArrowArrayInt32({1, 0}, validity_array);

  auto array_int64 = MakeArrowArrayInt64({1, 0}, validity_array);

  auto array_float32 = MakeArrowArrayFloat32({1.0, 0.0}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_int32, array_int64, array_float32, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response_int32 = outputs.at(0);
  auto response_int64 = outputs.at(1);
  auto response_float32 = outputs.at(2);
  auto response_float64 = outputs.at(3);

  // Checks if the null and zero representation for numeric values
  // are consistent between the types
  EXPECT_ARROW_ARRAY_EQUALS(response_int32, response_int64);
  EXPECT_ARROW_ARRAY_EQUALS(response_int64, response_float32);
  EXPECT_ARROW_ARRAY_EQUALS(response_float32, response_float64);

  const int sha256_hash_size = 64;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha256_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha256Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashSHA256(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha256 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha256, res_0);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY "
      "[ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY "
      "[ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_a =
      MakeArrowArrayUtf8({"foo", first_string, second_string}, {false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response = outputs.at(0);
  const int sha256_hash_size = 64;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), sha256_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha256_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha1Simple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int64());
  auto field_c = field("c", float32());
  auto field_d = field("d", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_1 = field("res1", utf8());
  auto res_2 = field("res2", utf8());
  auto res_3 = field("res3", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1_1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha1_2 = TreeExprBuilder::MakeFunction("hashSHA1", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha1_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha1_3 = TreeExprBuilder::MakeFunction("hashSHA1", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha1_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashSha1_4 = TreeExprBuilder::MakeFunction("hashSHA1", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashSha1_4, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3},
                                TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 2;
  auto validity_array = {false, true};

  auto array_int32 = MakeArrowArrayInt32({1, 0}, validity_array);

  auto array_int64 = MakeArrowArrayInt64({1, 0}, validity_array);

  auto array_float32 = MakeArrowArrayFloat32({1.0, 0.0}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_int32, array_int64, array_float32, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response_int32 = outputs.at(0);
  auto response_int64 = outputs.at(1);
  auto response_float32 = outputs.at(2);
  auto response_float64 = outputs.at(3);

  // Checks if the null and zero representation for numeric values
  // are consistent between the types
  EXPECT_ARROW_ARRAY_EQUALS(response_int32, response_int64);
  EXPECT_ARROW_ARRAY_EQUALS(response_int64, response_float32);
  EXPECT_ARROW_ARRAY_EQUALS(response_float32, response_float64);

  const int sha1_hash_size = 40;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha1_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha1Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1, res_0);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_a =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response = outputs.at(0);
  const int sha1_hash_size = 40;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), sha1_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha1_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}
}  // namespace gandiva

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

#include <sstream>

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

TEST_F(TestHash, TestSha1FunctionsAlias) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("c", int64());
  auto field_c = field("e", float64());
  auto schema = arrow::schema({field_a, field_b, field_c});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_0_sha1 = field("res0sha1", utf8());
  auto res_0_sha = field("res0sha", utf8());

  auto res_1 = field("res1", utf8());
  auto res_1_sha1 = field("res1sha1", utf8());
  auto res_1_sha = field("res1sha", utf8());

  auto res_2 = field("res2", utf8());
  auto res_2_sha1 = field("res2_sha1", utf8());
  auto res_2_sha = field("res2_sha", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1, res_0);
  auto sha1 = TreeExprBuilder::MakeFunction("sha1", {node_a}, utf8());
  auto expr_0_sha1 = TreeExprBuilder::MakeExpression(sha1, res_0_sha1);
  auto sha = TreeExprBuilder::MakeFunction("sha", {node_a}, utf8());
  auto expr_0_sha = TreeExprBuilder::MakeExpression(sha, res_0_sha);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha1_1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha1_1, res_1);
  auto sha1_1 = TreeExprBuilder::MakeFunction("sha1", {node_b}, utf8());
  auto expr_1_sha1 = TreeExprBuilder::MakeExpression(sha1_1, res_1_sha1);
  auto sha_1 = TreeExprBuilder::MakeFunction("sha", {node_b}, utf8());
  auto expr_1_sha = TreeExprBuilder::MakeExpression(sha_1, res_1_sha);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha1_2 = TreeExprBuilder::MakeFunction("hashSHA1", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha1_2, res_2);
  auto sha1_2 = TreeExprBuilder::MakeFunction("sha1", {node_c}, utf8());
  auto expr_2_sha1 = TreeExprBuilder::MakeExpression(sha1_2, res_2_sha1);
  auto sha_2 = TreeExprBuilder::MakeFunction("sha", {node_c}, utf8());
  auto expr_2_sha = TreeExprBuilder::MakeExpression(sha_2, res_2_sha);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema,
                                {expr_0, expr_0_sha, expr_0_sha1, expr_1, expr_1_sha,
                                 expr_1_sha1, expr_2, expr_2_sha, expr_2_sha1},
                                TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int32_t num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_utf8 =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  auto validity_array = {false, true, true};

  auto array_int64 = MakeArrowArrayInt64({1, 0, 32423}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0, 324893.3849}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_utf8, array_int64, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the first
  // field of utf8 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(0), outputs.at(1));  // hashSha1 and sha
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(1), outputs.at(2));  // sha and sha1

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the second
  // field of int64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(3), outputs.at(4));  // hashSha1 and sha
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(4), outputs.at(5));  // sha and sha1

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the first
  // field of float64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(6), outputs.at(7));  // hashSha1 and sha responses
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(7), outputs.at(8));  // sha and sha1 responses
}

TEST_F(TestHash, TestSha256FunctionsAlias) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("c", int64());
  auto field_c = field("e", float64());
  auto schema = arrow::schema({field_a, field_b, field_c});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_0_sha256 = field("res0sha256", utf8());

  auto res_1 = field("res1", utf8());
  auto res_1_sha256 = field("res1sha256", utf8());

  auto res_2 = field("res2", utf8());
  auto res_2_sha256 = field("res2_sha256", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha2, res_0);
  auto sha256 = TreeExprBuilder::MakeFunction("sha256", {node_a}, utf8());
  auto expr_0_sha256 = TreeExprBuilder::MakeExpression(sha256, res_0_sha256);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha2_1 = TreeExprBuilder::MakeFunction("hashSHA256", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha2_1, res_1);
  auto sha256_1 = TreeExprBuilder::MakeFunction("sha256", {node_b}, utf8());
  auto expr_1_sha256 = TreeExprBuilder::MakeExpression(sha256_1, res_1_sha256);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha2_2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha2_2, res_2);
  auto sha256_2 = TreeExprBuilder::MakeFunction("sha256", {node_c}, utf8());
  auto expr_2_sha256 = TreeExprBuilder::MakeExpression(sha256_2, res_2_sha256);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      schema, {expr_0, expr_0_sha256, expr_1, expr_1_sha256, expr_2, expr_2_sha256},
      TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int32_t num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_utf8 =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  auto validity_array = {false, true, true};

  auto array_int64 = MakeArrowArrayInt64({1, 0, 32423}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0, 324893.3849}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_utf8, array_int64, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the first
  // field of utf8 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(0), outputs.at(1));  // hashSha2 and sha256

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the second
  // field of int64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(2), outputs.at(3));  // hashSha2 and sha256

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the first
  // field of float64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(4),
                            outputs.at(5));  // hashSha2 and sha256 responses
}

TEST_F(TestHash, TestMD5Simple) {
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
  // hashMD5(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashMD5_1 = TreeExprBuilder::MakeFunction("hashMD5", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashMD5_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashMD5_2 = TreeExprBuilder::MakeFunction("hashMD5", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashMD5_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashMD5_3 = TreeExprBuilder::MakeFunction("hashMD5", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashMD5_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashMD5_4 = TreeExprBuilder::MakeFunction("hashMD5", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashMD5_4, res_3);

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

  const int MD5_hash_size = 32;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), MD5_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestMD5Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashMD5(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashMD5 = TreeExprBuilder::MakeFunction("hashMD5", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashMD5, res_0);

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
  const int MD5_hash_size = 32;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), MD5_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), MD5_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}
}  // namespace gandiva

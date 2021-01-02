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

#include <vector>

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
using std::string;
using std::vector;

class TestList : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

template <class ValueType, class ArrayType>
void _build_list_array(const vector<ValueType>& values, const vector<int64_t>& length,
                       const vector<bool>& validity, arrow::MemoryPool* pool,
                       ArrayPtr* array) {
  size_t sum = 0;
  for (auto& len : length) {
    sum += len;
  }
  EXPECT_TRUE(values.size() == sum);
  EXPECT_TRUE(length.size() == validity.size());

  auto value_builder = std::make_shared<ArrayType>(pool);
  auto builder = std::make_shared<arrow::ListBuilder>(pool, value_builder);
  int i = 0;
  for (size_t l = 0; l < length.size(); l++) {
    if (validity[l]) {
      auto status = builder->Append();
      for (int j = 0; j < length[l]; j++) {
        ASSERT_OK(value_builder->Append(values[i]));
        i++;
      }
    } else {
      ASSERT_OK(builder->AppendNull());
      for (int j = 0; j < length[l]; j++) {
        i++;
      }
    }
  }
  ASSERT_OK(builder->Finish(array));
}

/*
 * expression:
 *      input: a
 *      output: res
 * typeof(a) can be list<binary_like> / list<int> / list<float>
 */
void _test_list_type_field_alias(DataTypePtr type, ArrayPtr array,
                                 arrow::MemoryPool* pool) {
  auto field_a = field("a", type);
  auto schema = arrow::schema({field_a});
  auto result = field("res", type);

  auto num_records = 5;
  assert(array->length() == num_records);

  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array});

  // Make expression
  auto field_a_node = TreeExprBuilder::MakeField(field_a);
  auto expr = TreeExprBuilder::MakeExpression(field_a_node, result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_ARROW_ARRAY_EQUALS(array, outputs[0]);
  // EXPECT_ARROW_ARRAY_EQUALS will not check the length of child data, but
  // ArrayData::Slice method will check length. ArrayData::ToString method will call
  // ArrayData::Slice method
  EXPECT_TRUE(array->ToString() == outputs[0]->ToString());
  EXPECT_TRUE(array->null_count() == outputs[0]->null_count());
}

TEST_F(TestList, TestListUtf8) {
  ArrayPtr array;
  _build_list_array<string, arrow::StringBuilder>(
      {"a", "b", "bb", "c", "cc", "ccc", "d", "dd", "ddd", "dddd", "e", "ee", "eee",
       "eeee", "eeeee"},
      {1, 4, 3, 2, 5}, {true, true, false, true, true}, pool_, &array);
  _test_list_type_field_alias(list(utf8()), array, pool_);
}

TEST_F(TestList, TestListUtf8WithInvalidData) {
  ArrayPtr array;
  _build_list_array<string, arrow::StringBuilder>(
      {"a", "b", "bb", "c", "cc", "ccc", "d", "dd", "ddd", "dddd", "e", "ee", "eee",
       "eeee", "eeeee"},
      {1, 2, 3, 4, 5}, {true, false, true, true, false}, pool_, &array);
  _test_list_type_field_alias(list(utf8()), array, pool_);
}

TEST_F(TestList, TestListInt64) {
  ArrayPtr array;
  _build_list_array<int64_t, arrow::Int64Builder>(
      {1, 10, 20, 100, 200, 300, 1000, 2000, 3000, 4000, 10000, 20000, 30000, 40000,
       50000},
      {1, 2, 5, 4, 3}, {true, true, true, true, false}, pool_, &array);
  _test_list_type_field_alias(list(int64()), array, pool_);
}

TEST_F(TestList, TestListInt32) {
  ArrayPtr array;
  _build_list_array<int32_t, arrow::Int32Builder>(
      {1, 10, 20, 100, 200, 300, 1000, 2000, 3000, 4000, 10000, 20000, 30000, 40000,
       50000},
      {5, 2, 3, 4, 1}, {true, false, true, true, true}, pool_, &array);
  _test_list_type_field_alias(list(int32()), array, pool_);
}

TEST_F(TestList, TestListFloat32) {
  ArrayPtr array;
  _build_list_array<float, arrow::FloatBuilder>(
      {1.1f, 11.1f, 22.2f, 111.1f, 222.2f, 333.3f, 1111.1f, 2222.2f, 3333.3f, 4444.4f,
       11111.1f, 22222.2f, 33333.3f, 44444.4f, 55555.5f},
      {1, 2, 3, 4, 5}, {true, true, true, true, true}, pool_, &array);
  _test_list_type_field_alias(list(float32()), array, pool_);
}

TEST_F(TestList, TestListFloat64) {
  ArrayPtr array;
  _build_list_array<double, arrow::DoubleBuilder>(
      {1.1, 1.11, 2.22, 1.111, 2.222, 3.333, 1.1111, 2.2222, 3.3333, 4.4444, 1.11111,
       2.22222, 3.33333, 4.44444, 5.55555},
      {1, 2, 4, 3, 5}, {true, false, true, true, true}, pool_, &array);
  _test_list_type_field_alias(list(float64()), array, pool_);
}

/*
 * array_length(a)
 */
TEST_F(TestList, TestListUtf8Length) {
  // schema for input fields
  auto field_a = field("a", list(utf8()));
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", int64());

  // Create a row-batch with some sample data
  int num_records = 5;
  ArrayPtr array_a;
  _build_list_array<string, arrow::StringBuilder>(
      {"a", "b", "bb", "c", "cc", "ccc", "d", "dd", "ddd", "dddd", "e", "ee", "eee",
       "eeee", "eeeee"},
      {1, 2, 3, 4, 5}, {true, true, true, true, true}, pool_, &array_a);

  // expected output
  auto exp = MakeArrowArrayInt64({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // build expressions.
  // array_length(a)
  auto expr = TreeExprBuilder::MakeExpression("array_length", {field_a}, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

TEST_F(TestList, TestListUtf8LengthWithInvalidData) {
  // schema for input fields
  auto field_a = field("a", list(utf8()));
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", int64());

  // Create a row-batch with some sample data
  int num_records = 5;
  ArrayPtr array_a;
  _build_list_array<string, arrow::StringBuilder>(
      {"a", "b", "bb", "cc", "cc", "ccc", "d", "dd", "ddd"}, {1, 2, 2, 3, 1},
      {true, false, true, false, true}, pool_, &array_a);

  // expected output
  auto exp = MakeArrowArrayInt64({1, 2, 2, 3, 1}, {true, false, true, false, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // build expressions.
  // array_length(a)
  auto expr = TreeExprBuilder::MakeExpression("array_length", {field_a}, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

/*
 * array_contains(a, "element")
 */
TEST_F(TestList, TestListUtf8Contains) {
  // schema for input fields
  auto field_a = field("a", list(utf8()));
  auto field_b = field("b", utf8());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res = field("res", boolean());

  // Create a row-batch with some sample data
  int num_records = 5;
  ArrayPtr array_a;
  _build_list_array<string, arrow::StringBuilder>(
      {"rectangle", "circle", "rectangle", "circle", "triangle", "triangle", "circle",
       "rectangle"},
      {2, 3, 1, 1, 1}, {true, true, true, true, true}, pool_, &array_a);
  auto array_b =
      MakeArrowArrayUtf8({"rectangle", "circle", "circle", "circle", "rectangll"});

  // expected output
  auto exp = MakeArrowArrayBool({true, true, false, true, false},
                                {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // build expressions.
  // array_contains(a, b)
  auto expr = TreeExprBuilder::MakeExpression("array_contains", {field_a, field_b}, res);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva

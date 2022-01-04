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

#include "arrow/record_batch.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestRecordBatch : public ::testing::Test {};

TEST_F(TestRecordBatch, Equals) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  auto metadata = key_value_metadata({"foo"}, {"bar"});

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});
  auto schema2 = ::arrow::schema({f0, f1});
  auto schema3 = ::arrow::schema({f0, f1, f2}, metadata);

  random::RandomArrayGenerator gen(42);

  auto a0 = gen.ArrayOf(int32(), length);
  auto a1 = gen.ArrayOf(uint8(), length);
  auto a2 = gen.ArrayOf(int16(), length);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});
  auto b2 = RecordBatch::Make(schema3, length, {a0, a1, a2});
  auto b3 = RecordBatch::Make(schema2, length, {a0, a1});
  auto b4 = RecordBatch::Make(schema, length, {a0, a1, a1});

  ASSERT_TRUE(b1->Equals(*b1));
  ASSERT_FALSE(b1->Equals(*b3));
  ASSERT_FALSE(b1->Equals(*b4));

  // Different metadata
  ASSERT_TRUE(b1->Equals(*b2));
  ASSERT_FALSE(b1->Equals(*b2, /*check_metadata=*/true));
}

TEST_F(TestRecordBatch, Validate) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  auto schema = ::arrow::schema({f0, f1, f2});

  random::RandomArrayGenerator gen(42);

  auto a0 = gen.ArrayOf(int32(), length);
  auto a1 = gen.ArrayOf(uint8(), length);
  auto a2 = gen.ArrayOf(int16(), length);
  auto a3 = gen.ArrayOf(int16(), 5);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK(b1->ValidateFull());

  // Length mismatch
  auto b2 = RecordBatch::Make(schema, length, {a0, a1, a3});
  ASSERT_RAISES(Invalid, b2->ValidateFull());

  // Type mismatch
  auto b3 = RecordBatch::Make(schema, length, {a0, a1, a0});
  ASSERT_RAISES(Invalid, b3->ValidateFull());
}

TEST_F(TestRecordBatch, Slice) {
  const int length = 7;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int8());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema(fields);

  random::RandomArrayGenerator gen(42);

  auto a0 = gen.ArrayOf(int32(), length);
  auto a1 = gen.ArrayOf(uint8(), length);
  auto a2 = ArrayFromJSON(int8(), "[0, 1, 2, 3, 4, 5, 6]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1, a2});

  auto batch_slice = batch->Slice(2);
  auto batch_slice2 = batch->Slice(1, 5);

  ASSERT_EQ(batch_slice->num_rows(), batch->num_rows() - 2);

  for (int i = 0; i < batch->num_columns(); ++i) {
    ASSERT_EQ(2, batch_slice->column(i)->offset());
    ASSERT_EQ(length - 2, batch_slice->column(i)->length());

    ASSERT_EQ(1, batch_slice2->column(i)->offset());
    ASSERT_EQ(5, batch_slice2->column(i)->length());
  }

  // ARROW-9143: RecordBatch::Slice was incorrectly setting a2's
  // ArrayData::null_count to kUnknownNullCount
  ASSERT_EQ(batch_slice->column(2)->data()->null_count, 0);
  ASSERT_EQ(batch_slice2->column(2)->data()->null_count, 0);
}

TEST_F(TestRecordBatch, AddColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field2});

  random::RandomArrayGenerator gen(42);

  auto array1 = gen.ArrayOf(int32(), length);
  auto array2 = gen.ArrayOf(uint8(), length);
  auto array3 = gen.ArrayOf(int16(), length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array2});

  const RecordBatch& batch = *batch3;

  // Negative tests with invalid index
  ASSERT_RAISES(Invalid, batch.AddColumn(5, field1, array1));
  ASSERT_RAISES(Invalid, batch.AddColumn(2, field1, array1));
  ASSERT_RAISES(Invalid, batch.AddColumn(-1, field1, array1));

  // Negative test with wrong length
  auto longer_col = gen.ArrayOf(int32(), length + 1);
  ASSERT_RAISES(Invalid, batch.AddColumn(0, field1, longer_col));

  // Negative test with mismatch type
  ASSERT_RAISES(TypeError, batch.AddColumn(0, field1, array2));

  ASSERT_OK_AND_ASSIGN(auto new_batch, batch.AddColumn(0, field1, array1));
  AssertBatchesEqual(*new_batch, *batch1);

  ASSERT_OK_AND_ASSIGN(new_batch, batch.AddColumn(1, field3, array3));
  AssertBatchesEqual(*new_batch, *batch2);

  ASSERT_OK_AND_ASSIGN(auto new_batch2, batch.AddColumn(1, "f3", array3));
  AssertBatchesEqual(*new_batch2, *new_batch);

  ASSERT_TRUE(new_batch2->schema()->field(1)->nullable());
}

TEST_F(TestRecordBatch, SetColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2});
  auto schema2 = ::arrow::schema({field1, field3});
  auto schema3 = ::arrow::schema({field3, field2});

  random::RandomArrayGenerator gen(42);

  auto array1 = gen.ArrayOf(int32(), length);
  auto array2 = gen.ArrayOf(uint8(), length);
  auto array3 = gen.ArrayOf(int16(), length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2});
  auto batch2 = RecordBatch::Make(schema2, length, {array1, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array3, array2});

  const RecordBatch& batch = *batch1;

  // Negative tests with invalid index
  ASSERT_RAISES(Invalid, batch.SetColumn(5, field1, array1));
  ASSERT_RAISES(Invalid, batch.SetColumn(-1, field1, array1));

  // Negative test with wrong length
  auto longer_col = gen.ArrayOf(int32(), length + 1);
  ASSERT_RAISES(Invalid, batch.SetColumn(0, field1, longer_col));

  // Negative test with mismatch type
  ASSERT_RAISES(TypeError, batch.SetColumn(0, field1, array2));

  ASSERT_OK_AND_ASSIGN(auto new_batch, batch.SetColumn(1, field3, array3));
  AssertBatchesEqual(*new_batch, *batch2);

  ASSERT_OK_AND_ASSIGN(new_batch, batch.SetColumn(0, field3, array3));
  AssertBatchesEqual(*new_batch, *batch3);
}

TEST_F(TestRecordBatch, RemoveColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2, field3});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field1, field3});
  auto schema4 = ::arrow::schema({field1, field2});

  random::RandomArrayGenerator gen(42);

  auto array1 = gen.ArrayOf(int32(), length);
  auto array2 = gen.ArrayOf(uint8(), length);
  auto array3 = gen.ArrayOf(int16(), length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2, array3});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array1, array3});
  auto batch4 = RecordBatch::Make(schema4, length, {array1, array2});

  const RecordBatch& batch = *batch1;
  std::shared_ptr<RecordBatch> result;

  // Negative tests with invalid index
  ASSERT_RAISES(Invalid, batch.RemoveColumn(3));
  ASSERT_RAISES(Invalid, batch.RemoveColumn(-1));

  ASSERT_OK_AND_ASSIGN(auto new_batch, batch.RemoveColumn(0));
  AssertBatchesEqual(*new_batch, *batch2);

  ASSERT_OK_AND_ASSIGN(new_batch, batch.RemoveColumn(1));
  AssertBatchesEqual(*new_batch, *batch3);

  ASSERT_OK_AND_ASSIGN(new_batch, batch.RemoveColumn(2));
  AssertBatchesEqual(*new_batch, *batch4);
}

TEST_F(TestRecordBatch, SelectColumns) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2, field3});

  random::RandomArrayGenerator gen(42);

  auto array1 = gen.ArrayOf(int32(), length);
  auto array2 = gen.ArrayOf(uint8(), length);
  auto array3 = gen.ArrayOf(int16(), length);

  auto batch = RecordBatch::Make(schema1, length, {array1, array2, array3});

  ASSERT_OK_AND_ASSIGN(auto subset, batch->SelectColumns({0, 2}));
  ASSERT_OK(subset->ValidateFull());

  auto expected_schema = ::arrow::schema({schema1->field(0), schema1->field(2)});
  auto expected =
      RecordBatch::Make(expected_schema, length, {batch->column(0), batch->column(2)});
  ASSERT_TRUE(subset->Equals(*expected));

  // Out of bounds indices
  ASSERT_RAISES(Invalid, batch->SelectColumns({0, 3}));
  ASSERT_RAISES(Invalid, batch->SelectColumns({-1}));
}

TEST_F(TestRecordBatch, RemoveColumnEmpty) {
  const int length = 10;

  random::RandomArrayGenerator gen(42);

  auto field1 = field("f1", int32());
  auto schema1 = ::arrow::schema({field1});
  auto array1 = gen.ArrayOf(int32(), length);
  auto batch1 = RecordBatch::Make(schema1, length, {array1});

  ASSERT_OK_AND_ASSIGN(auto empty, batch1->RemoveColumn(0));
  ASSERT_EQ(batch1->num_rows(), empty->num_rows());

  ASSERT_OK_AND_ASSIGN(auto added, empty->AddColumn(0, field1, array1));
  AssertBatchesEqual(*added, *batch1);
}

TEST_F(TestRecordBatch, ToFromEmptyStructArray) {
  auto batch1 =
      RecordBatch::Make(::arrow::schema({}), 10, std::vector<std::shared_ptr<Array>>{});
  ASSERT_OK_AND_ASSIGN(auto struct_array, batch1->ToStructArray());
  ASSERT_EQ(10, struct_array->length());
  ASSERT_OK_AND_ASSIGN(auto batch2, RecordBatch::FromStructArray(struct_array));
  ASSERT_TRUE(batch1->Equals(*batch2));
}

TEST_F(TestRecordBatch, FromStructArrayInvalidType) {
  random::RandomArrayGenerator gen(42);
  ASSERT_RAISES(TypeError, RecordBatch::FromStructArray(gen.ArrayOf(int32(), 6)));
}

TEST_F(TestRecordBatch, FromStructArrayInvalidNullCount) {
  auto struct_array =
      ArrayFromJSON(struct_({field("f1", int32())}), R"([{"f1": 1}, null])");
  ASSERT_RAISES(Invalid, RecordBatch::FromStructArray(struct_array));
}

TEST_F(TestRecordBatch, MakeEmpty) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> empty,
                       RecordBatch::MakeEmpty(schema));
  AssertSchemaEqual(*schema, *empty->schema());
  ASSERT_OK(empty->ValidateFull());
  ASSERT_EQ(empty->num_rows(), 0);
}

}  // namespace arrow

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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/data.h"
#include "arrow/array/statistics_option.h"
#include "arrow/array/statistics_test_util.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/chunked_array.h"
#include "arrow/config.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/float16.h"
#include "arrow/util/iterator.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

using util::Float16;

class TestRecordBatch : public ::testing::Test {};

TEST_F(TestRecordBatch, Equals) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());
  auto f2b = field("f2b", int16());

  auto metadata = key_value_metadata({"foo"}, {"bar"});

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});
  auto schema2 = ::arrow::schema({f0, f1});
  auto schema3 = ::arrow::schema({f0, f1, f2}, metadata);
  auto schema4 = ::arrow::schema({f0, f1, f2b});

  random::RandomArrayGenerator gen(42);

  auto a0 = gen.ArrayOf(int32(), length);
  auto a1 = gen.ArrayOf(uint8(), length);
  auto a2 = gen.ArrayOf(int16(), length);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});
  auto b2 = RecordBatch::Make(schema3, length, {a0, a1, a2});
  auto b3 = RecordBatch::Make(schema2, length, {a0, a1});
  auto b4 = RecordBatch::Make(schema, length, {a0, a1, a1});
  auto b5 = RecordBatch::Make(schema4, length, {a0, a1, a2});

  ASSERT_TRUE(b1->Equals(*b1));
  ASSERT_FALSE(b1->Equals(*b3));
  ASSERT_FALSE(b1->Equals(*b4));

  // Same values and types, but different field names
  ASSERT_FALSE(b1->Equals(*b5));

  // Different metadata
  ASSERT_TRUE(b1->Equals(*b2));
  ASSERT_FALSE(b1->Equals(*b2, /*check_metadata=*/true));
}

TEST_F(TestRecordBatch, EqualOptions) {
  int length = 2;
  auto f = field("f", float64());

  std::vector<std::shared_ptr<Field>> fields = {f};
  auto schema = ::arrow::schema(fields);

  std::shared_ptr<Array> array1, array2;
  ArrayFromVector<DoubleType>(float64(), {true, true}, {0.5, NAN}, &array1);
  ArrayFromVector<DoubleType>(float64(), {true, true}, {0.5, NAN}, &array2);
  auto b1 = RecordBatch::Make(schema, length, {array1});
  auto b2 = RecordBatch::Make(schema, length, {array2});

  EXPECT_FALSE(b1->Equals(*b2, /*check_metadata=*/false,
                          EqualOptions::Defaults().nans_equal(false)));
  EXPECT_TRUE(b1->Equals(*b2, /*check_metadata=*/false,
                         EqualOptions::Defaults().nans_equal(true)));
}

TEST_F(TestRecordBatch, ApproxEqualOptions) {
  int length = 2;
  auto f = field("f", float64());

  std::vector<std::shared_ptr<Field>> fields = {f};
  auto schema = ::arrow::schema(fields);
  std::shared_ptr<Array> array1, array2;
  ArrayFromVector<DoubleType>(float64(), {true, true}, {0.5, NAN}, &array1);
  ArrayFromVector<DoubleType>(float64(), {true, true}, {0.501, NAN}, &array2);

  auto b1 = RecordBatch::Make(schema, length, {array1});
  auto b2 = RecordBatch::Make(schema, length, {array2});

  EXPECT_FALSE(b1->ApproxEquals(*b2, EqualOptions::Defaults().nans_equal(false)));
  EXPECT_FALSE(b1->ApproxEquals(*b2, EqualOptions::Defaults().nans_equal(true)));

  EXPECT_TRUE(b1->ApproxEquals(*b2, EqualOptions::Defaults().nans_equal(true).atol(0.1)));
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

TEST_F(TestRecordBatch, RenameColumns) {
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
  EXPECT_THAT(batch->ColumnNames(), testing::ElementsAre("f1", "f2", "f3"));

  ASSERT_OK_AND_ASSIGN(auto renamed, batch->RenameColumns({"zero", "one", "two"}));
  EXPECT_THAT(renamed->ColumnNames(), testing::ElementsAre("zero", "one", "two"));
  EXPECT_THAT(renamed->columns(), testing::ElementsAre(array1, array2, array3));
  ASSERT_OK(renamed->ValidateFull());

  ASSERT_RAISES(Invalid, batch->RenameColumns({"hello", "world"}));
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

TEST_F(TestRecordBatch, ColumnsThreadSafety) {
#ifndef ARROW_ENABLE_THREADING
  GTEST_SKIP() << "Test requires threading support";
#endif
  constexpr int kNumThreads = 10;
  constexpr int kLength = 10;

  random::RandomArrayGenerator gen(42);
  std::shared_ptr<ArrayData> array_data = gen.ArrayOf(utf8(), kLength)->data();
  auto schema = ::arrow::schema({field("f1", utf8())});
  auto record_batch = RecordBatch::Make(schema, kLength, {array_data});
  std::mutex mutex;
  std::vector<std::thread> threads;
  std::vector<Array*> all_columns;
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&]() {
      auto columns = record_batch->columns();
      mutex.lock();
      all_columns.push_back(columns[0].get());
      mutex.unlock();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // assert that all calls to columns() return the same arrays
  for (const auto& col : all_columns) {
    ASSERT_EQ(col, all_columns[0]);
  }
}

TEST_F(TestRecordBatch, ToFromEmptyStructArray) {
  auto batch1 =
      RecordBatch::Make(::arrow::schema({}), 10, std::vector<std::shared_ptr<Array>>{});
  ASSERT_OK_AND_ASSIGN(auto struct_array, batch1->ToStructArray());
  ASSERT_EQ(10, struct_array->length());
  ASSERT_OK_AND_ASSIGN(auto batch2, RecordBatch::FromStructArray(struct_array));
  ASSERT_TRUE(batch1->Equals(*batch2));
}

TEST_F(TestRecordBatch, FromSlicedStructArray) {
  static constexpr int64_t kLength = 10;
  std::shared_ptr<Array> x_arr = ArrayFromJSON(int64(), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
  StructArray struct_array(struct_({field("x", int64())}), kLength, {x_arr});
  std::shared_ptr<Array> sliced = struct_array.Slice(5, 3);
  ASSERT_OK_AND_ASSIGN(auto batch, RecordBatch::FromStructArray(sliced));

  std::shared_ptr<Array> expected_arr = ArrayFromJSON(int64(), "[5, 6, 7]");
  std::shared_ptr<RecordBatch> expected =
      RecordBatch::Make(schema({field("x", int64())}), 3, {expected_arr});
  AssertBatchesEqual(*expected, *batch);
}

TEST_F(TestRecordBatch, FromStructArrayInvalidType) {
  random::RandomArrayGenerator gen(42);
  ASSERT_RAISES(TypeError, RecordBatch::FromStructArray(gen.ArrayOf(int32(), 6)));
}

TEST_F(TestRecordBatch, FromStructArrayInvalidNullCount) {
  auto struct_array =
      ArrayFromJSON(struct_({field("f1", int32())}), R"([{"f1": 1}, null])");
  ASSERT_OK_AND_ASSIGN(auto batch, RecordBatch::FromStructArray(struct_array));
  std::shared_ptr<Array> expected_arr = ArrayFromJSON(int32(), "[1, null]");
  std::shared_ptr<RecordBatch> expected =
      RecordBatch::Make(schema({field("f1", int32())}), 2, {expected_arr});
  AssertBatchesEqual(*expected, *batch);
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

// See: https://github.com/apache/arrow/issues/35450
TEST_F(TestRecordBatch, ToStructArrayMismatchedColumnLengths) {
  constexpr int kNumRows = 5;
  FieldVector fields = {field("x", int64()), field("y", int64())};
  ArrayVector columns = {
      ArrayFromJSON(int64(), "[0, 1, 2, 3, 4]"),
      ArrayFromJSON(int64(), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"),
  };

  // Sanity check
  auto batch = RecordBatch::Make(schema({fields[0]}), kNumRows, {columns[0]});
  ASSERT_OK_AND_ASSIGN(auto array, batch->ToStructArray());
  ASSERT_EQ(array->length(), kNumRows);

  // One column with a mismatched length
  batch = RecordBatch::Make(schema({fields[1]}), kNumRows, {columns[1]});
  ASSERT_RAISES(Invalid, batch->ToStructArray());
  // Mix of columns with matching and non-matching lengths
  batch = RecordBatch::Make(schema(fields), kNumRows, columns);
  ASSERT_RAISES(Invalid, batch->ToStructArray());
  std::swap(columns[0], columns[1]);
  batch = RecordBatch::Make(schema(fields), kNumRows, columns);
  ASSERT_RAISES(Invalid, batch->ToStructArray());
}

class TestRecordBatchReader : public ::testing::Test {
 public:
  void SetUp() override { MakeBatchesAndReader(100); }

 protected:
  void MakeBatchesAndReader(int length) {
    auto field1 = field("f1", int32());
    auto field2 = field("f2", uint8());
    auto field3 = field("f3", int16());

    auto schema = ::arrow::schema({field1, field2, field3});

    random::RandomArrayGenerator gen(42);

    auto array1_1 = gen.ArrayOf(int32(), length);
    auto array1_2 = gen.ArrayOf(int32(), length);
    auto array1_3 = gen.ArrayOf(int32(), length);

    auto array2_1 = gen.ArrayOf(uint8(), length);
    auto array2_2 = gen.ArrayOf(uint8(), length);
    auto array2_3 = gen.ArrayOf(uint8(), length);

    auto array3_1 = gen.ArrayOf(int16(), length);
    auto array3_2 = gen.ArrayOf(int16(), length);
    auto array3_3 = gen.ArrayOf(int16(), length);

    auto batch1 = RecordBatch::Make(schema, length, {array1_1, array2_1, array3_1});
    auto batch2 = RecordBatch::Make(schema, length, {array1_2, array2_2, array3_2});
    auto batch3 = RecordBatch::Make(schema, length, {array1_3, array2_3, array3_3});

    batches_ = {batch1, batch2, batch3};

    ASSERT_OK_AND_ASSIGN(reader_, RecordBatchReader::Make(batches_));
  }
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  std::shared_ptr<RecordBatchReader> reader_;
};

TEST_F(TestRecordBatchReader, RangeForLoop) {
  int64_t i = 0;

  for (auto maybe_batch : *reader_) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    ASSERT_LT(i, static_cast<int64_t>(batches_.size()));
    AssertBatchesEqual(*batch, *batches_[i++]);
  }
  ASSERT_EQ(i, static_cast<int64_t>(batches_.size()));
}

TEST_F(TestRecordBatchReader, BeginEndForLoop) {
  int64_t i = 0;

  for (auto it = reader_->begin(); it != reader_->end(); ++it) {
    ASSERT_OK_AND_ASSIGN(auto batch, *it);
    ASSERT_LT(i, static_cast<int64_t>(batches_.size()));
    AssertBatchesEqual(*batch, *batches_[i++]);
  }
  ASSERT_EQ(i, static_cast<int64_t>(batches_.size()));
}

TEST_F(TestRecordBatchReader, ToRecordBatches) {
  ASSERT_OK_AND_ASSIGN(auto batches, reader_->ToRecordBatches());
  ASSERT_EQ(batches.size(), batches_.size());
  for (size_t index = 0; index < batches.size(); index++) {
    AssertBatchesEqual(*batches[index], *batches_[index]);
  }

  ASSERT_OK_AND_ASSIGN(batches, reader_->ToRecordBatches());
  ASSERT_EQ(batches.size(), 0);
}

TEST_F(TestRecordBatchReader, ToTable) {
  ASSERT_OK_AND_ASSIGN(auto table, reader_->ToTable());
  const auto& chunks = table->column(0)->chunks();
  ASSERT_EQ(chunks.size(), batches_.size());
  for (size_t index = 0; index < batches_.size(); index++) {
    AssertArraysEqual(*chunks[index], *batches_[index]->column(0));
  }

  ASSERT_OK_AND_ASSIGN(table, reader_->ToTable());
  ASSERT_EQ(table->column(0)->chunks().size(), 0);
}

TEST_F(TestRecordBatch, ReplaceSchema) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());
  auto f3 = field("f3", int8());

  auto schema = ::arrow::schema({f0, f1, f2});

  random::RandomArrayGenerator gen(42);

  auto a0 = gen.ArrayOf(int32(), length);
  auto a1 = gen.ArrayOf(uint8(), length);
  auto a2 = gen.ArrayOf(int16(), length);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});

  f0 = field("fd0", int32());
  f1 = field("fd1", uint8());
  f2 = field("fd2", int16());

  schema = ::arrow::schema({f0, f1, f2});
  ASSERT_OK_AND_ASSIGN(auto mutated, b1->ReplaceSchema(schema));
  auto expected = RecordBatch::Make(schema, length, b1->columns());
  ASSERT_TRUE(mutated->Equals(*expected));

  schema = ::arrow::schema({f0, f1, f3});
  ASSERT_RAISES(Invalid, b1->ReplaceSchema(schema));

  schema = ::arrow::schema({f0, f1});
  ASSERT_RAISES(Invalid, b1->ReplaceSchema(schema));
}

TEST_F(TestRecordBatch, ConcatenateRecordBatches) {
  int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());

  auto schema = ::arrow::schema({f0, f1});

  random::RandomArrayGenerator gen(42);

  auto b1 = gen.BatchOf(schema->fields(), length);

  length = 5;

  auto b2 = gen.BatchOf(schema->fields(), length);

  ASSERT_OK_AND_ASSIGN(auto batch, ConcatenateRecordBatches({b1, b2}));
  ASSERT_EQ(batch->num_rows(), b1->num_rows() + b2->num_rows());
  ASSERT_BATCHES_EQUAL(*batch->Slice(0, b1->num_rows()), *b1);
  ASSERT_BATCHES_EQUAL(*batch->Slice(b1->num_rows()), *b2);

  f0 = field("fd0", int32());
  f1 = field("fd1", uint8());

  schema = ::arrow::schema({f0, f1});

  auto b3 = gen.BatchOf(schema->fields(), length);

  ASSERT_RAISES(Invalid, ConcatenateRecordBatches({b1, b3}));

  auto null_batch = RecordBatch::Make(::arrow::schema({}), length,
                                      std::vector<std::shared_ptr<ArrayData>>{});
  ASSERT_OK_AND_ASSIGN(batch, ConcatenateRecordBatches({null_batch}));
  ASSERT_EQ(batch->num_rows(), null_batch->num_rows());
  ASSERT_BATCHES_EQUAL(*batch, *null_batch);
}

TEST_F(TestRecordBatch, ToTensorUnsupportedType) {
  const int length = 9;

  auto f0 = field("f0", int32());
  // Unsupported data type
  auto f1 = field("f1", utf8());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(utf8(), R"(["a", "b", "c", "a", "b", "c", "a", "b", "c"])");

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: DataType is not supported: " + a1->type()->ToString(),
      batch->ToTensor());

  // Unsupported boolean data type
  auto f2 = field("f2", boolean());

  std::vector<std::shared_ptr<Field>> fields2 = {f0, f2};
  auto schema2 = ::arrow::schema(fields2);
  auto a2 = ArrayFromJSON(boolean(),
                          "[true, false, true, true, false, true, false, true, true]");
  auto batch2 = RecordBatch::Make(schema2, length, {a0, a2});

  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: DataType is not supported: " + a2->type()->ToString(),
      batch2->ToTensor());
}

TEST_F(TestRecordBatch, ToTensorUnsupportedMissing) {
  const int length = 9;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", int32());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(int32(), "[10, 20, 30, 40, null, 60, 70, 80, 90]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only convert a RecordBatch with no nulls. "
                             "Set null_to_nan to true to convert nulls to NaN",
                             batch->ToTensor());
}

TEST_F(TestRecordBatch, ToTensorEmptyBatch) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int32());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<RecordBatch> empty,
                       RecordBatch::MakeEmpty(schema));

  ASSERT_OK_AND_ASSIGN(auto tensor_column,
                       empty->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor_column->Validate());

  ASSERT_OK_AND_ASSIGN(auto tensor_row, empty->ToTensor());
  ASSERT_OK(tensor_row->Validate());

  const std::vector<int64_t> strides = {4, 4};
  const std::vector<int64_t> shape = {0, 2};

  EXPECT_EQ(strides, tensor_column->strides());
  EXPECT_EQ(shape, tensor_column->shape());
  EXPECT_EQ(strides, tensor_row->strides());
  EXPECT_EQ(shape, tensor_row->shape());

  auto batch_no_columns =
      RecordBatch::Make(::arrow::schema({}), 10, std::vector<std::shared_ptr<Array>>{});

  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Conversion to Tensor for RecordBatches without "
                             "columns/schema is not supported.",
                             batch_no_columns->ToTensor());
}

template <typename DataType>
void CheckTensor(const std::shared_ptr<Tensor>& tensor, const int size,
                 const std::vector<int64_t> shape, const std::vector<int64_t> f_strides) {
  EXPECT_EQ(size, tensor->size());
  EXPECT_EQ(TypeTraits<DataType>::type_singleton(), tensor->type());
  EXPECT_EQ(shape, tensor->shape());
  EXPECT_EQ(f_strides, tensor->strides());
  EXPECT_FALSE(tensor->is_row_major());
  EXPECT_TRUE(tensor->is_column_major());
  EXPECT_TRUE(tensor->is_contiguous());
}

template <typename DataType>
void CheckTensorRowMajor(const std::shared_ptr<Tensor>& tensor, const int size,
                         const std::vector<int64_t> shape,
                         const std::vector<int64_t> strides) {
  EXPECT_EQ(size, tensor->size());
  EXPECT_EQ(TypeTraits<DataType>::type_singleton(), tensor->type());
  EXPECT_EQ(shape, tensor->shape());
  EXPECT_EQ(strides, tensor->strides());
  EXPECT_TRUE(tensor->is_row_major());
  EXPECT_FALSE(tensor->is_column_major());
  EXPECT_TRUE(tensor->is_contiguous());
}

TEST_F(TestRecordBatch, ToTensorSupportedNaN) {
  const int length = 9;

  auto f0 = field("f0", float32());
  auto f1 = field("f1", float32());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(float32(), "[NaN, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(float32(), "[10, 20, 30, 40, NaN, 60, 70, 80, 90]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       batch->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor->Validate());

  std::vector<int64_t> shape = {9, 2};
  const int64_t f32_size = sizeof(float);
  std::vector<int64_t> f_strides = {f32_size, f32_size * shape[0]};
  std::shared_ptr<Tensor> tensor_expected = TensorFromJSON(
      float32(), "[NaN, 2,  3,  4,  5, 6, 7, 8, 9, 10, 20, 30, 40, NaN, 60, 70, 80, 90]",
      shape, f_strides);

  EXPECT_FALSE(tensor_expected->Equals(*tensor));
  EXPECT_TRUE(tensor_expected->Equals(*tensor, EqualOptions().nans_equal(true)));
  CheckTensor<FloatType>(tensor, 18, shape, f_strides);
}

TEST_F(TestRecordBatch, ToTensorSupportedNullToNan) {
  const int length = 9;

  // int32 + float32 = float64
  auto f0 = field("f0", int32());
  auto f1 = field("f1", float32());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(int32(), "[null, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(float32(), "[10, 20, 30, 40, null, 60, 70, 80, 90]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       batch->ToTensor(/*null_to_nan=*/true, /*row_major=*/false));
  ASSERT_OK(tensor->Validate());

  std::vector<int64_t> shape = {9, 2};
  const int64_t f64_size = sizeof(double);
  std::vector<int64_t> f_strides = {f64_size, f64_size * shape[0]};
  std::shared_ptr<Tensor> tensor_expected = TensorFromJSON(
      float64(), "[NaN, 2,  3,  4,  5, 6, 7, 8, 9, 10, 20, 30, 40, NaN, 60, 70, 80, 90]",
      shape, f_strides);

  EXPECT_FALSE(tensor_expected->Equals(*tensor));
  EXPECT_TRUE(tensor_expected->Equals(*tensor, EqualOptions().nans_equal(true)));

  CheckTensor<DoubleType>(tensor, 18, shape, f_strides);

  ASSERT_OK_AND_ASSIGN(auto tensor_row, batch->ToTensor(/*null_to_nan=*/true));
  ASSERT_OK(tensor_row->Validate());

  std::vector<int64_t> strides = {f64_size * shape[1], f64_size};
  std::shared_ptr<Tensor> tensor_expected_row = TensorFromJSON(
      float64(), "[NaN, 10, 2,  20, 3, 30,  4, 40, 5, NaN, 6, 60, 7, 70, 8, 80, 9, 90]",
      shape, strides);

  EXPECT_FALSE(tensor_expected_row->Equals(*tensor_row));
  EXPECT_TRUE(tensor_expected_row->Equals(*tensor_row, EqualOptions().nans_equal(true)));

  CheckTensorRowMajor<DoubleType>(tensor_row, 18, shape, strides);

  // int32 -> float64
  auto f2 = field("f2", int32());

  std::vector<std::shared_ptr<Field>> fields1 = {f0, f2};
  auto schema1 = ::arrow::schema(fields1);

  auto a2 = ArrayFromJSON(int32(), "[10, 20, 30, 40, null, 60, 70, 80, 90]");
  auto batch1 = RecordBatch::Make(schema1, length, {a0, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor1,
                       batch1->ToTensor(/*null_to_nan=*/true, /*row_major=*/false));
  ASSERT_OK(tensor1->Validate());

  EXPECT_FALSE(tensor_expected->Equals(*tensor1));
  EXPECT_TRUE(tensor_expected->Equals(*tensor1, EqualOptions().nans_equal(true)));

  CheckTensor<DoubleType>(tensor1, 18, shape, f_strides);

  ASSERT_OK_AND_ASSIGN(auto tensor1_row, batch1->ToTensor(/*null_to_nan=*/true));
  ASSERT_OK(tensor1_row->Validate());

  EXPECT_FALSE(tensor_expected_row->Equals(*tensor1_row));
  EXPECT_TRUE(tensor_expected_row->Equals(*tensor1_row, EqualOptions().nans_equal(true)));

  CheckTensorRowMajor<DoubleType>(tensor1_row, 18, shape, strides);

  // int8 -> float32
  auto f3 = field("f3", int8());
  auto f4 = field("f4", int8());

  std::vector<std::shared_ptr<Field>> fields2 = {f3, f4};
  auto schema2 = ::arrow::schema(fields2);

  auto a3 = ArrayFromJSON(int8(), "[null, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a4 = ArrayFromJSON(int8(), "[10, 20, 30, 40, null, 60, 70, 80, 90]");
  auto batch2 = RecordBatch::Make(schema2, length, {a3, a4});

  ASSERT_OK_AND_ASSIGN(auto tensor2,
                       batch2->ToTensor(/*null_to_nan=*/true, /*row_major=*/false));
  ASSERT_OK(tensor2->Validate());

  const int64_t f32_size = sizeof(float);
  std::vector<int64_t> f_strides_2 = {f32_size, f32_size * shape[0]};
  std::shared_ptr<Tensor> tensor_expected_2 = TensorFromJSON(
      float32(), "[NaN, 2,  3,  4,  5, 6, 7, 8, 9, 10, 20, 30, 40, NaN, 60, 70, 80, 90]",
      shape, f_strides_2);

  EXPECT_FALSE(tensor_expected_2->Equals(*tensor2));
  EXPECT_TRUE(tensor_expected_2->Equals(*tensor2, EqualOptions().nans_equal(true)));

  CheckTensor<FloatType>(tensor2, 18, shape, f_strides_2);

  ASSERT_OK_AND_ASSIGN(auto tensor2_row, batch2->ToTensor(/*null_to_nan=*/true));
  ASSERT_OK(tensor2_row->Validate());

  std::vector<int64_t> strides_2 = {f32_size * shape[1], f32_size};
  std::shared_ptr<Tensor> tensor2_expected_row = TensorFromJSON(
      float32(), "[NaN, 10, 2,  20, 3, 30,  4, 40, 5, NaN, 6, 60, 7, 70, 8, 80, 9, 90]",
      shape, strides_2);

  EXPECT_FALSE(tensor2_expected_row->Equals(*tensor2_row));
  EXPECT_TRUE(
      tensor2_expected_row->Equals(*tensor2_row, EqualOptions().nans_equal(true)));

  CheckTensorRowMajor<FloatType>(tensor2_row, 18, shape, strides_2);
}

TEST_F(TestRecordBatch, ToTensorSupportedTypesMixed) {
  const int length = 9;

  auto f0 = field("f0", uint16());
  auto f1 = field("f1", int16());
  auto f2 = field("f2", float32());

  auto a0 = ArrayFromJSON(uint16(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(int16(), "[10, 20, 30, 40, 50, 60, 70, 80, 90]");
  auto a2 = ArrayFromJSON(float32(), "[100, 200, 300, NaN, 500, 600, 700, 800, 900]");

  // Single column
  std::vector<std::shared_ptr<Field>> fields = {f0};
  auto schema = ::arrow::schema(fields);
  auto batch = RecordBatch::Make(schema, length, {a0});

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       batch->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor->Validate());

  std::vector<int64_t> shape = {9, 1};
  const int64_t uint16_size = sizeof(uint16_t);
  std::vector<int64_t> f_strides = {uint16_size, uint16_size * shape[0]};
  std::shared_ptr<Tensor> tensor_expected =
      TensorFromJSON(uint16(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]", shape, f_strides);

  EXPECT_TRUE(tensor_expected->Equals(*tensor));
  CheckTensor<UInt16Type>(tensor, 9, shape, f_strides);

  // uint16 + int16 = int32
  std::vector<std::shared_ptr<Field>> fields1 = {f0, f1};
  auto schema1 = ::arrow::schema(fields1);
  auto batch1 = RecordBatch::Make(schema1, length, {a0, a1});

  ASSERT_OK_AND_ASSIGN(auto tensor1,
                       batch1->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor1->Validate());

  std::vector<int64_t> shape1 = {9, 2};
  const int64_t int32_size = sizeof(int32_t);
  std::vector<int64_t> f_strides_1 = {int32_size, int32_size * shape1[0]};
  std::shared_ptr<Tensor> tensor_expected_1 = TensorFromJSON(
      int32(), "[1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 20, 30, 40, 50, 60, 70, 80, 90]",
      shape1, f_strides_1);

  EXPECT_TRUE(tensor_expected_1->Equals(*tensor1));

  CheckTensor<Int32Type>(tensor1, 18, shape1, f_strides_1);

  ASSERT_EQ(tensor1->type()->bit_width(), tensor_expected_1->type()->bit_width());

  ASSERT_EQ(1, tensor_expected_1->Value<Int32Type>({0, 0}));
  ASSERT_EQ(2, tensor_expected_1->Value<Int32Type>({1, 0}));
  ASSERT_EQ(10, tensor_expected_1->Value<Int32Type>({0, 1}));

  // uint16 + int16 + float32 = float64
  std::vector<std::shared_ptr<Field>> fields2 = {f0, f1, f2};
  auto schema2 = ::arrow::schema(fields2);
  auto batch2 = RecordBatch::Make(schema2, length, {a0, a1, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor2,
                       batch2->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor2->Validate());

  std::vector<int64_t> shape2 = {9, 3};
  const int64_t f64_size = sizeof(double);
  std::vector<int64_t> f_strides_2 = {f64_size, f64_size * shape2[0]};
  std::shared_ptr<Tensor> tensor_expected_2 =
      TensorFromJSON(float64(),
                     "[1,   2,   3,   4,   5,  6,  7,  8,   9,   10,  20, 30,  40,  50,  "
                     "60,  70, 80, 90, 100, 200, 300, NaN, 500, 600, 700, 800, 900]",
                     shape2, f_strides_2);

  EXPECT_FALSE(tensor_expected_2->Equals(*tensor2));
  EXPECT_TRUE(tensor_expected_2->Equals(*tensor2, EqualOptions().nans_equal(true)));

  CheckTensor<DoubleType>(tensor2, 27, shape2, f_strides_2);
}

TEST_F(TestRecordBatch, ToTensorUnsupportedMixedFloat16) {
  const int length = 9;

  auto f0 = field("f0", float16());
  auto f1 = field("f1", float64());

  auto a0 = ArrayFromJSON(float16(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(float64(), "[10, 20, 30, 40, 50, 60, 70, 80, 90]");

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);
  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  ASSERT_RAISES_WITH_MESSAGE(
      NotImplemented, "NotImplemented: Casting from or to halffloat is not supported.",
      batch->ToTensor());

  std::vector<std::shared_ptr<Field>> fields1 = {f1, f0};
  auto schema1 = ::arrow::schema(fields1);
  auto batch1 = RecordBatch::Make(schema1, length, {a1, a0});

  ASSERT_RAISES_WITH_MESSAGE(
      NotImplemented, "NotImplemented: Casting from or to halffloat is not supported.",
      batch1->ToTensor());
}
namespace test {

TEST_F(TestRecordBatch, MakeStatisticsArrayNesting) {
  auto schema = ::arrow::schema({field("f0", int32())});
  ASSERT_OK_AND_ASSIGN(auto batch, RecordBatch::MakeEmpty(schema));
  auto options = StatisticsArrayTOptions::Default();
  options.max_recursion_depth = 1;
  ASSERT_OK(CheckDepth(batch, options));
  options.max_recursion_depth = 0;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             CheckDepth(batch, options));

  schema = ::arrow::schema({field("f0", int64()), field("f1", int64())});
  batch = RecordBatchFromJSON(
      schema, R"([{"f0": 1, "f1": 4},{"f0": 2, "f1": 5},{"f0": 3, "f1": 6}])");
  options = StatisticsArrayTOptions::Default();
  options.max_recursion_depth = 1;
  ASSERT_OK(CheckDepth(batch, options));
  options.max_recursion_depth = 0;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             CheckDepth(batch, options));

  options.max_recursion_depth = 2;
  ASSERT_OK_AND_ASSIGN(auto multi_children_struct,
                       MakeNestedStruct(options.max_recursion_depth));
  schema =
      ::arrow::schema({field("multi_children_struct", multi_children_struct->type())});
  batch =
      RecordBatch::Make(schema, multi_children_struct->length(), {multi_children_struct});

  options.max_recursion_depth = 1;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             CheckDepth(batch, options));

  options.max_recursion_depth = 2;
  ASSERT_OK(CheckDepth(batch, options));

  auto array_a = ArrayFromJSON(int64(), R"([1,2,3])");
  schema = ::arrow::schema({field("a", int64()), field("multi_children_struct",
                                                       multi_children_struct->type())});
  batch = RecordBatch::Make(schema, 3, {array_a, multi_children_struct});

  options.max_recursion_depth = 1;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             CheckDepth(batch, options));

  options.max_recursion_depth = 2;
  ASSERT_OK(CheckDepth(batch, options));
}

TEST_F(TestRecordBatch, MakeStatisticsArrayNestingStressTest) {
  ASSERT_OK_AND_ASSIGN(auto deep_children_struct, MakeNestedStruct(100));
  auto schema = ::arrow::schema({field("struct", deep_children_struct->type())});
  auto batch =
      RecordBatch::Make(schema, deep_children_struct->length(), {deep_children_struct});
  StatisticsArrayTOptions options;
  options.max_recursion_depth = 100;
  ASSERT_OK(CheckDepth(batch, options));
  options.max_recursion_depth = 99;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             batch->MakeStatisticsArray(options));

#if !defined(ARROW_VALGRIND)
  ASSERT_OK_AND_ASSIGN(deep_children_struct, MakeNestedStruct(500));
  schema = ::arrow::schema({field("struct", deep_children_struct->type())});
  batch =
      RecordBatch::Make(schema, deep_children_struct->length(), {deep_children_struct});
  options.max_recursion_depth = 500;
  ASSERT_OK(CheckDepth(batch, options));
  options.max_recursion_depth = 499;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                             batch->MakeStatisticsArray(options));
#endif
}

TEST_F(TestRecordBatch, MakeStatisticsArrayRowCount) {
  auto schema = ::arrow::schema({field("int32", int32())});
  auto int32_array = ArrayFromJSON(int32(), "[1, null, -1]");
  auto batch = RecordBatch::Make(schema, int32_array->length(), {int32_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(auto expected_statistics_array,
                       MakeMockStatisticsArray("[null]",
                                               {{
                                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                                               }},
                                               {{
                                                   ArrayStatistics::ValueType{int64_t{3}},
                                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);

  // I'm not sure whether the following states are valid
  batch = RecordBatch::Make(schema, 1, {int32_array});
  ASSERT_OK_AND_ASSIGN(statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(expected_statistics_array,
                       MakeMockStatisticsArray("[null]",
                                               {{
                                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                                               }},
                                               {{
                                                   ArrayStatistics::ValueType{int64_t{1}},
                                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);

  batch = RecordBatch::Make(schema, 10, {int32_array});
  ASSERT_OK_AND_ASSIGN(statistics_array, batch->MakeStatisticsArray());
  ASSERT_OK_AND_ASSIGN(
      expected_statistics_array,
      MakeMockStatisticsArray("[null]",
                              {{
                                  ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                              }},
                              {{
                                  ArrayStatistics::ValueType{int64_t{10}},
                              }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayNullCount) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("int32", int32())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto int32_array_data = ArrayFromJSON(int32(), "[1, null, -1]")->data()->Copy();
  int32_array_data->statistics = std::make_shared<ArrayStatistics>();
  int32_array_data->statistics->null_count = 1;
  auto int32_array = MakeArray(std::move(int32_array_data));
  auto batch = RecordBatch::Make(schema, int32_array->length(),
                                 {no_statistics_array, int32_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_NULL_COUNT_EXACT,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{int64_t{1}},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayDistinctCount) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("int32", int32())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto int32_array_data = ArrayFromJSON(int32(), "[1, null, -1]")->data()->Copy();
  int32_array_data->statistics = std::make_shared<ArrayStatistics>();
  int32_array_data->statistics->null_count = 1;
  int32_array_data->statistics->distinct_count = 2;
  auto int32_array = MakeArray(std::move(int32_array_data));
  auto batch = RecordBatch::Make(schema, int32_array->length(),
                                 {no_statistics_array, int32_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_NULL_COUNT_EXACT,
                                   ARROW_STATISTICS_KEY_DISTINCT_COUNT_EXACT,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{int64_t{1}},
                                   ArrayStatistics::ValueType{int64_t{2}},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayMinExact) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("uint32", uint32())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto uint32_array_data = ArrayFromJSON(uint32(), "[100, null, 1]")->data()->Copy();
  uint32_array_data->statistics = std::make_shared<ArrayStatistics>();
  uint32_array_data->statistics->is_min_exact = true;
  uint32_array_data->statistics->min = uint64_t{1};
  auto uint32_array = MakeArray(std::move(uint32_array_data));
  auto batch = RecordBatch::Make(schema, uint32_array->length(),
                                 {no_statistics_array, uint32_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_MIN_VALUE_EXACT,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{uint64_t{1}},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayMinApproximate) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("int32", int32())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto int32_array_data = ArrayFromJSON(int32(), "[1, null, -1]")->data()->Copy();
  int32_array_data->statistics = std::make_shared<ArrayStatistics>();
  int32_array_data->statistics->min = -1.0;
  auto int32_array = MakeArray(std::move(int32_array_data));
  auto batch = RecordBatch::Make(schema, int32_array->length(),
                                 {no_statistics_array, int32_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_MIN_VALUE_APPROXIMATE,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{-1.0},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayMaxExact) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("boolean", boolean())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto boolean_array_data =
      ArrayFromJSON(boolean(), "[true, null, false]")->data()->Copy();
  boolean_array_data->statistics = std::make_shared<ArrayStatistics>();
  boolean_array_data->statistics->is_max_exact = true;
  boolean_array_data->statistics->max = true;
  auto boolean_array = MakeArray(std::move(boolean_array_data));
  auto batch = RecordBatch::Make(schema, boolean_array->length(),
                                 {no_statistics_array, boolean_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_MAX_VALUE_EXACT,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{true},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayMaxApproximate) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("float64", float64())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto float64_array_data = ArrayFromJSON(float64(), "[1.0, null, -1.0]")->data()->Copy();
  float64_array_data->statistics = std::make_shared<ArrayStatistics>();
  float64_array_data->statistics->max = 1.0;
  auto float64_array = MakeArray(std::move(float64_array_data));
  auto batch = RecordBatch::Make(schema, float64_array->length(),
                                 {no_statistics_array, float64_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{1.0},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}

TEST_F(TestRecordBatch, MakeStatisticsArrayString) {
  auto schema =
      ::arrow::schema({field("no-statistics", boolean()), field("string", utf8())});
  auto no_statistics_array = ArrayFromJSON(boolean(), "[true, false, true]");
  auto string_array_data = ArrayFromJSON(utf8(), "[\"a\", null, \"c\"]")->data()->Copy();
  string_array_data->statistics = std::make_shared<ArrayStatistics>();
  string_array_data->statistics->is_max_exact = true;
  string_array_data->statistics->max = "c";
  auto string_array = MakeArray(std::move(string_array_data));
  auto batch = RecordBatch::Make(schema, string_array->length(),
                                 {no_statistics_array, string_array});

  ASSERT_OK_AND_ASSIGN(auto statistics_array, batch->MakeStatisticsArray());

  ASSERT_OK_AND_ASSIGN(
      auto expected_statistics_array,
      MakeMockStatisticsArray("[null, 1]",
                              {{
                                   ARROW_STATISTICS_KEY_ROW_COUNT_EXACT,
                               },
                               {
                                   ARROW_STATISTICS_KEY_MAX_VALUE_EXACT,
                               }},
                              {{
                                   ArrayStatistics::ValueType{int64_t{3}},
                               },
                               {
                                   ArrayStatistics::ValueType{"c"},
                               }}));
  AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
}
// Schema:
//   struct_a_b: struct {a: int64, b: int64}
//   c: int64
//   d: int64
//   struct_e_f: struct {e: int32, f: int32}
//
// Data:
// [
//   {struct_a_b: {a: 1, b: 6 }, c: 11, d: 16, struct_e_f: {a: 21, b: 26}},
//   {struct_a_b: {a: 2, b: 7 }, c: 12, d: 17, struct_e_f: {a: 22, b: 27}},
//   {struct_a_b: {a: 3, b: 8 }, c: 13, d: 18, struct_e_f: {a: 23, b: 28}},
//   {struct_a_b: {a: 4, b: 9 }, c: 14, d: 19, struct_e_f: {a: 24, b: 29}},
//   {struct_a_b: {a: 5, b: 10}, c: 15, d: 20, struct_e_f: {a: 25, b: 30}}
// ]
//
// Statistics:
//   batch:
//     row_count_exact: 5
//   struct_a_b:
//     null_count_exact: 0
//     max_approximate: {a: 5, b: 10}
//   struct_a_b.a:
//     min_approximate: 1
//   array_c:
//     max_approximate: 15
//   struct_e_f.e:
//     max_exact: 30

TEST_F(TestRecordBatch, MakeStatisticsArrayNestedType) {
  const auto struct_a_b_type = struct_({field("a", int64()), field("b", int64())});
  const auto struct_a_b = ArrayFromJSON(
      struct_a_b_type,
      R"([{"a": 1, "b": 6}, {"a": 2, "b": 7}, {"a": 3,"b": 8}, {"a": 4,"b": 9}, {"a": 5, "b": 10}])");
  const auto struct_a_b_statistics = std::make_shared<ArrayStatistics>();
  ASSERT_OK_AND_ASSIGN(struct_a_b_statistics->max, struct_a_b->GetScalar(4));
  struct_a_b_statistics->null_count = 0;
  struct_a_b->data()->statistics = struct_a_b_statistics;
  const auto struct_a_b_child_a_statistics = std::make_shared<ArrayStatistics>();
  struct_a_b_child_a_statistics->min = int64_t{1};
  struct_a_b->data()->child_data[0]->statistics = struct_a_b_child_a_statistics;
  const auto array_c = ArrayFromJSON(int64(), R"([11, 12, 13, 14, 15])");
  array_c->data()->statistics = std::make_shared<ArrayStatistics>();
  array_c->data()->statistics->max = int64_t{15};
  const auto array_d = ArrayFromJSON(int64(), R"([16, 17, 18, 19, 20])");
  const auto struct_e_f_type = struct_({field("e", int32()), field("f", int32())});
  const auto struct_e_f = ArrayFromJSON(
      struct_e_f_type,
      R"([{"e": 21, "f": 26}, {"e": 22,"f": 27}, {"e": 23, "f": 28}, {"e": 24, "f": 29}, {"e": 25,"f": 30}])");
  const auto struct_e_f_child_e_statistics = struct_e_f->data()->child_data[0];
  struct_e_f_child_e_statistics->statistics = std::make_shared<ArrayStatistics>();
  struct_e_f_child_e_statistics->statistics->max = int64_t{30};
  struct_e_f_child_e_statistics->statistics->is_max_exact = true;

  const auto expected_scalar = ScalarFromJSON(struct_a_b_type, R"([5 ,10])");

  ASSERT_OK_AND_ASSIGN(
      const auto expected_array_statistics,
      MakeMockStatisticsArray("[null , 0, 1, 3, 6]",
                              {{ARROW_STATISTICS_KEY_ROW_COUNT_EXACT},
                               {ARROW_STATISTICS_KEY_NULL_COUNT_EXACT,
                                ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE},
                               {ARROW_STATISTICS_KEY_MIN_VALUE_APPROXIMATE},
                               {ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE},
                               {ARROW_STATISTICS_KEY_MAX_VALUE_EXACT}},
                              {{ArrayStatistics::ValueType{int64_t{5}}},
                               {ArrayStatistics::ValueType{int64_t{0}},
                                ArrayStatistics::ValueType{expected_scalar}},
                               {ArrayStatistics::ValueType{int64_t{1}}},
                               {ArrayStatistics::ValueType{int64_t{15}}},
                               {ArrayStatistics::ValueType{int64_t{30}}}}));

  const auto schema =
      ::arrow::schema({field("struct_a_b", struct_a_b_type), field("c", int64()),
                       field("d", int64()), field("struct_e_f", struct_e_f->type())});
  const auto batch =
      RecordBatch::Make(schema, 5, {struct_a_b, array_c, array_d, struct_e_f});
  ASSERT_OK_AND_ASSIGN(auto batch_statistics, batch->MakeStatisticsArray());

  AssertArraysEqual(*expected_array_statistics, *batch_statistics, true);
}

// Schema
//   struct_parent: {struct_nested_0: {a: int32, b: int32},
//                   struct_nested_1: {c: int32, d: int32},
//                   struct_nested_2: {a: int32, b: int32}}
//
// Data:
// [
//   struct_parent: {struct_nested_0: {a: 1, b: 6 }, struct_nested_1: {c: 1, d: 6 },
//                   struct_nested_2: {a: 1, b: 6 }},
//   struct_parent: {struct_nested_0: {a: 2, b: 7 }, struct_nested_1: {c: 2, d: 7 },
//                   struct_nested_2: {a: 2, b: 7 }},
//   struct_parent: {struct_nested_0: {a: 3, b: 8 }, struct_nested_1: {c: 3, d: 8 },
//                   struct_nested_2: {a: 3, b: 8 }},
//   struct_parent: {struct_nested_0: {a: 4, b: 9 }, struct_nested_1: {c: 4, d: 9 },
//                   struct_nested_2: {a: 4, b: 9 }},
//   struct_parent: {struct_nested_0: {a: 5, b: 10}, struct_nested_1: {c: 5, d: 10}}
//                   struct_nested_2: {a: 5, b: 10}}
// ]
//
// Statistics:
//   batch:
//     row_count_exact: 5
//   struct_parent:
//     min_approximate: {struct_nested_0: {a: 1, b: 6 }, struct_nested_1: {c: 1, d: 6 },
//                       struct_nested_2: {a: 1, b: 6 }},
//   struct_nested_0:
//     max_approximate: {a:5, b: 10}
//   struct_nested_1:
//     max_exact: {c:5, d: 10}
//   struct_nested_2:
//     min_exact: {a:5, b: 4}
//
TEST_F(TestRecordBatch, MakeStatisticsArrayNestedNestedType) {
  const auto struct_nested_0_type = struct_({field("a", int32()), field("b", int32())});
  const auto struct_nested_0 = ArrayFromJSON(
      struct_nested_0_type,
      R"([{"a": 1,"b": 6}, {"a": 2, "b": 7}, {"a": 3, "b": 8}, {"a": 4, "b": 9}, {"a": 5,"b": 10}])");
  struct_nested_0->data()->statistics = std::make_shared<ArrayStatistics>();
  ASSERT_OK_AND_ASSIGN(struct_nested_0->data()->statistics->max,
                       struct_nested_0->GetScalar(4));
  const auto struct_nested_1_type = struct_({field("c", int32()), field("d", int32())});
  const auto struct_nested_1 = ArrayFromJSON(
      struct_nested_1_type,
      R"([{"c": 1, "d": 6}, {"c": 2, "d": 7}, {"c": 3, "d": 8}, {"c": 4, "d": 9}, {"c": 5,"d": 10}])");
  struct_nested_1->data()->statistics = std::make_shared<ArrayStatistics>();
  struct_nested_1->data()->statistics->is_max_exact = true;
  ASSERT_OK_AND_ASSIGN(struct_nested_1->data()->statistics->max,
                       struct_nested_1->GetScalar(4));

  auto struct_nested_2_type = struct_({field("a", int32()), field("b", int32())});
  const auto struct_nested_2 = ArrayFromJSON(
      struct_nested_0_type,
      R"([{"a": 1,"b": 6}, {"a": 2, "b": 7}, {"a": 3, "b": 8}, {"a": 4, "b": 9}, {"a": 5,"b": 10}])");
  struct_nested_2->data()->statistics = std::make_shared<ArrayStatistics>();
  struct_nested_2->data()->statistics->is_min_exact = true;
  ASSERT_OK_AND_ASSIGN(struct_nested_2->data()->statistics->min,
                       struct_nested_2->GetScalar(0));

  ASSERT_OK_AND_ASSIGN(
      const auto struct_parant,
      StructArray::Make({struct_nested_0, struct_nested_1, struct_nested_2},
                        {field("struct_nested_0", struct_nested_0->type()),
                         field("struct_nested_1", struct_nested_1->type()),
                         field("struct_nested_2", struct_nested_2->type())}));
  struct_parant->data()->statistics = std::make_shared<ArrayStatistics>();
  ASSERT_OK_AND_ASSIGN(struct_parant->data()->statistics->min,
                       struct_parant->GetScalar(0));

  const auto expected_scalar_parent =
      ScalarFromJSON(struct_parant->type(), R"([[1 ,6], [1, 6], [1, 6]])");

  const auto expected_scalar_nested_0 =
      ScalarFromJSON(struct_nested_0_type, R"([5, 10])");

  const auto expected_scalar_nested_1 =
      ScalarFromJSON(struct_nested_1_type, R"([5, 10])");

  const auto expected_scalar_nested_2 = ScalarFromJSON(struct_nested_2_type, R"([1, 6])");

  ASSERT_OK_AND_ASSIGN(
      const auto expected_array_statistics,
      MakeMockStatisticsArray(R"([null, 0, 1, 4, 7])",
                              {{ARROW_STATISTICS_KEY_ROW_COUNT_EXACT},
                               {ARROW_STATISTICS_KEY_MIN_VALUE_APPROXIMATE},
                               {ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE},
                               {ARROW_STATISTICS_KEY_MAX_VALUE_EXACT},
                               {ARROW_STATISTICS_KEY_MIN_VALUE_EXACT}},
                              {{ArrayStatistics::ValueType{int64_t{5}}},
                               {ArrayStatistics::ValueType{expected_scalar_parent}},
                               {ArrayStatistics::ValueType{expected_scalar_nested_0}},
                               {ArrayStatistics::ValueType{expected_scalar_nested_1}},
                               {expected_scalar_nested_2}}));

  const auto schema = ::arrow::schema({field("struct", struct_parant->type())});
  const auto batch = RecordBatch::Make(schema, 5, {struct_parant});
  ASSERT_OK_AND_ASSIGN(const auto batch_statistics, batch->MakeStatisticsArray());

  AssertArraysEqual(*expected_array_statistics, *batch_statistics, true);
}

}  // namespace test

template <typename DataType>
class TestBatchToTensorColumnMajor : public ::testing::Test {};

TYPED_TEST_SUITE_P(TestBatchToTensorColumnMajor);

TYPED_TEST_P(TestBatchToTensorColumnMajor, SupportedTypes) {
  using DataType = TypeParam;
  using c_data_type = typename DataType::c_type;
  const int unit_size = sizeof(c_data_type);

  const int length = 9;

  auto f0 = field("f0", TypeTraits<DataType>::type_singleton());
  auto f1 = field("f1", TypeTraits<DataType>::type_singleton());
  auto f2 = field("f2", TypeTraits<DataType>::type_singleton());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[10, 20, 30, 40, 50, 60, 70, 80, 90]");
  auto a2 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[100, 100, 100, 100, 100, 100, 100, 100, 100]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor,
                       batch->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor->Validate());

  std::vector<int64_t> shape = {9, 3};
  std::vector<int64_t> f_strides = {unit_size, unit_size * shape[0]};
  std::shared_ptr<Tensor> tensor_expected = TensorFromJSON(
      TypeTraits<DataType>::type_singleton(),
      "[1,   2,   3,   4,   5,   6,   7,   8,   9, 10,  20,  30,  40,  50,  60,  70,  "
      "80,  90, 100, 100, 100, 100, 100, 100, 100, 100, 100]",
      shape, f_strides);

  EXPECT_TRUE(tensor_expected->Equals(*tensor));
  CheckTensor<DataType>(tensor, 27, shape, f_strides);

  // Test offsets
  auto batch_slice = batch->Slice(1);

  ASSERT_OK_AND_ASSIGN(auto tensor_sliced,
                       batch_slice->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor_sliced->Validate());

  std::vector<int64_t> shape_sliced = {8, 3};
  std::vector<int64_t> f_strides_sliced = {unit_size, unit_size * shape_sliced[0]};
  std::shared_ptr<Tensor> tensor_expected_sliced =
      TensorFromJSON(TypeTraits<DataType>::type_singleton(),
                     "[2,   3,   4,   5,   6,   7,   8,   9, 20,  30,  40,  50,  60,  "
                     "70,  80,  90, 100, 100, 100, 100, 100, 100, 100, 100]",
                     shape_sliced, f_strides_sliced);

  EXPECT_TRUE(tensor_expected_sliced->Equals(*tensor_sliced));
  CheckTensor<DataType>(tensor_expected_sliced, 24, shape_sliced, f_strides_sliced);

  auto batch_slice_1 = batch->Slice(1, 5);

  ASSERT_OK_AND_ASSIGN(
      auto tensor_sliced_1,
      batch_slice_1->ToTensor(/*null_to_nan=*/false, /*row_major=*/false));
  ASSERT_OK(tensor_sliced_1->Validate());

  std::vector<int64_t> shape_sliced_1 = {5, 3};
  std::vector<int64_t> f_strides_sliced_1 = {unit_size, unit_size * shape_sliced_1[0]};
  std::shared_ptr<Tensor> tensor_expected_sliced_1 =
      TensorFromJSON(TypeTraits<DataType>::type_singleton(),
                     "[2, 3, 4, 5, 6, 20, 30, 40, 50, 60, 100, 100, 100, 100, 100]",
                     shape_sliced_1, f_strides_sliced_1);

  EXPECT_TRUE(tensor_expected_sliced_1->Equals(*tensor_sliced_1));
  CheckTensor<DataType>(tensor_expected_sliced_1, 15, shape_sliced_1, f_strides_sliced_1);
}

REGISTER_TYPED_TEST_SUITE_P(TestBatchToTensorColumnMajor, SupportedTypes);

INSTANTIATE_TYPED_TEST_SUITE_P(UInt8, TestBatchToTensorColumnMajor, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt16, TestBatchToTensorColumnMajor, UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt32, TestBatchToTensorColumnMajor, UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt64, TestBatchToTensorColumnMajor, UInt64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int8, TestBatchToTensorColumnMajor, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int16, TestBatchToTensorColumnMajor, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int32, TestBatchToTensorColumnMajor, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int64, TestBatchToTensorColumnMajor, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Float16, TestBatchToTensorColumnMajor, HalfFloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(Float32, TestBatchToTensorColumnMajor, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(Float64, TestBatchToTensorColumnMajor, DoubleType);

template <typename DataType>
class TestBatchToTensorRowMajor : public ::testing::Test {};

TYPED_TEST_SUITE_P(TestBatchToTensorRowMajor);

TYPED_TEST_P(TestBatchToTensorRowMajor, SupportedTypes) {
  using DataType = TypeParam;
  using c_data_type = typename DataType::c_type;
  const int unit_size = sizeof(c_data_type);

  const int length = 9;

  auto f0 = field("f0", TypeTraits<DataType>::type_singleton());
  auto f1 = field("f1", TypeTraits<DataType>::type_singleton());
  auto f2 = field("f2", TypeTraits<DataType>::type_singleton());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[10, 20, 30, 40, 50, 60, 70, 80, 90]");
  auto a2 = ArrayFromJSON(TypeTraits<DataType>::type_singleton(),
                          "[100, 100, 100, 100, 100, 100, 100, 100, 100]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());
  ASSERT_OK(tensor->Validate());

  std::vector<int64_t> shape = {9, 3};
  std::vector<int64_t> strides = {unit_size * shape[1], unit_size};
  std::shared_ptr<Tensor> tensor_expected =
      TensorFromJSON(TypeTraits<DataType>::type_singleton(),
                     "[1,   10, 100, 2, 20, 100, 3, 30, 100, 4, 40, 100, 5, 50, 100, 6, "
                     "60, 100, 7, 70, 100, 8, 80, 100, 9, 90, 100]",
                     shape, strides);

  EXPECT_TRUE(tensor_expected->Equals(*tensor));
  CheckTensorRowMajor<DataType>(tensor, 27, shape, strides);

  // Test offsets
  auto batch_slice = batch->Slice(1);

  ASSERT_OK_AND_ASSIGN(auto tensor_sliced, batch_slice->ToTensor());
  ASSERT_OK(tensor_sliced->Validate());

  std::vector<int64_t> shape_sliced = {8, 3};
  std::vector<int64_t> strides_sliced = {unit_size * shape[1], unit_size};
  std::shared_ptr<Tensor> tensor_expected_sliced =
      TensorFromJSON(TypeTraits<DataType>::type_singleton(),
                     "[2, 20, 100, 3, 30, 100, 4, 40, 100, 5, 50, 100, 6, "
                     "60, 100, 7, 70, 100, 8, 80, 100, 9, 90, 100]",
                     shape_sliced, strides_sliced);

  EXPECT_TRUE(tensor_expected_sliced->Equals(*tensor_sliced));
  CheckTensorRowMajor<DataType>(tensor_sliced, 24, shape_sliced, strides_sliced);

  auto batch_slice_1 = batch->Slice(1, 5);

  ASSERT_OK_AND_ASSIGN(auto tensor_sliced_1, batch_slice_1->ToTensor());
  ASSERT_OK(tensor_sliced_1->Validate());

  std::vector<int64_t> shape_sliced_1 = {5, 3};
  std::vector<int64_t> strides_sliced_1 = {unit_size * shape_sliced_1[1], unit_size};
  std::shared_ptr<Tensor> tensor_expected_sliced_1 =
      TensorFromJSON(TypeTraits<DataType>::type_singleton(),
                     "[2, 20, 100, 3, 30, 100, 4, 40, 100, 5, 50, 100, 6, 60, 100]",
                     shape_sliced_1, strides_sliced_1);

  EXPECT_TRUE(tensor_expected_sliced_1->Equals(*tensor_sliced_1));
  CheckTensorRowMajor<DataType>(tensor_sliced_1, 15, shape_sliced_1, strides_sliced_1);
}

REGISTER_TYPED_TEST_SUITE_P(TestBatchToTensorRowMajor, SupportedTypes);

INSTANTIATE_TYPED_TEST_SUITE_P(UInt8, TestBatchToTensorRowMajor, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt16, TestBatchToTensorRowMajor, UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt32, TestBatchToTensorRowMajor, UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(UInt64, TestBatchToTensorRowMajor, UInt64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int8, TestBatchToTensorRowMajor, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int16, TestBatchToTensorRowMajor, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int32, TestBatchToTensorRowMajor, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Int64, TestBatchToTensorRowMajor, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(Float16, TestBatchToTensorRowMajor, HalfFloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(Float32, TestBatchToTensorRowMajor, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(Float64, TestBatchToTensorRowMajor, DoubleType);

}  // namespace arrow

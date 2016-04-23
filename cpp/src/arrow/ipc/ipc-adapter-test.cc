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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/ipc/adapter.h"
#include "arrow/ipc/memory.h"
#include "arrow/ipc/test-common.h"

#include "arrow/test-util.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

// TODO(emkornfield) convert to google style kInt32, etc?
const auto INT32 = std::make_shared<Int32Type>();
const auto LIST_INT32 = std::make_shared<ListType>(INT32);
const auto LIST_LIST_INT32 = std::make_shared<ListType>(LIST_INT32);

typedef Status MakeRowBatch(std::shared_ptr<RowBatch>* out);

class TestWriteRowBatch : public ::testing::TestWithParam<MakeRowBatch*>,
                          public MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { MemoryMapFixture::TearDown(); }

  Status RoundTripHelper(const RowBatch& batch, int memory_map_size,
      std::shared_ptr<RowBatch>* batch_result) {
    std::string path = "test-write-row-batch";
    MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);
    int64_t header_location;
    RETURN_NOT_OK(WriteRowBatch(mmap_.get(), &batch, 0, &header_location));

    std::shared_ptr<RowBatchReader> reader;
    RETURN_NOT_OK(RowBatchReader::Open(mmap_.get(), header_location, &reader));

    RETURN_NOT_OK(reader->GetRowBatch(batch.schema(), batch_result));
    return Status::OK();
  }

 protected:
  std::shared_ptr<MemoryMappedSource> mmap_;
  MemoryPool* pool_;
};

TEST_P(TestWriteRowBatch, RoundTrip) {
  std::shared_ptr<RowBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue
  std::shared_ptr<RowBatch> batch_result;
  ASSERT_OK(RoundTripHelper(*batch, 1 << 16, &batch_result));

  // do checks
  ASSERT_TRUE(batch->schema()->Equals(batch_result->schema()));
  ASSERT_EQ(batch->num_columns(), batch_result->num_columns())
      << batch->schema()->ToString() << " result: " << batch_result->schema()->ToString();
  EXPECT_EQ(batch->num_rows(), batch_result->num_rows());
  for (int i = 0; i < batch->num_columns(); ++i) {
    EXPECT_TRUE(batch->column(i)->Equals(batch_result->column(i)))
        << "Idx: " << i << " Name: " << batch->column_name(i);
  }
}

Status MakeIntRowBatch(std::shared_ptr<RowBatch>* out) {
  const int length = 1000;

  // Make the schema
  auto f0 = std::make_shared<Field>("f0", INT32);
  auto f1 = std::make_shared<Field>("f1", INT32);
  std::shared_ptr<Schema> schema(new Schema({f0, f1}));

  // Example data
  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, pool, &a0));
  RETURN_NOT_OK(MakeRandomInt32Array(length, true, pool, &a1));
  out->reset(new RowBatch(schema, length, {a0, a1}));
  return Status::OK();
}

Status MakeListRowBatch(std::shared_ptr<RowBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", LIST_INT32);
  auto f1 = std::make_shared<Field>("f1", LIST_LIST_INT32);
  auto f2 = std::make_shared<Field>("f2", INT32);
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

  // Example data

  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(
      MakeRandomListArray(leaf_values, length, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, length, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(length, include_nulls, pool, &flat_array));
  out->reset(new RowBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeZeroLengthRowBatch(std::shared_ptr<RowBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", LIST_INT32);
  auto f1 = std::make_shared<Field>("f1", LIST_LIST_INT32);
  auto f2 = std::make_shared<Field>("f2", INT32);
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

  // Example data
  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  const bool include_nulls = true;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(MakeRandomListArray(leaf_values, 0, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, 0, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &flat_array));
  out->reset(new RowBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeNonNullRowBatch(std::shared_ptr<RowBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", LIST_INT32);
  auto f1 = std::make_shared<Field>("f1", LIST_LIST_INT32);
  auto f2 = std::make_shared<Field>("f2", INT32);
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

  // Example data
  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;

  RETURN_NOT_OK(MakeRandomInt32Array(1000, true, pool, &leaf_values));
  bool include_nulls = false;
  RETURN_NOT_OK(MakeRandomListArray(leaf_values, 50, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, 50, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &flat_array));
  out->reset(new RowBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeDeeplyNestedList(std::shared_ptr<RowBatch>* out) {
  const int batch_length = 5;
  TypePtr type = INT32;

  MemoryPool* pool = default_memory_pool();
  ArrayPtr array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &array));
  for (int i = 0; i < 63; ++i) {
    type = std::static_pointer_cast<DataType>(std::make_shared<ListType>(type));
    RETURN_NOT_OK(MakeRandomListArray(array, batch_length, include_nulls, pool, &array));
  }

  auto f0 = std::make_shared<Field>("f0", type);
  std::shared_ptr<Schema> schema(new Schema({f0}));
  std::vector<ArrayPtr> arrays = {array};
  out->reset(new RowBatch(schema, batch_length, arrays));
  return Status::OK();
}

INSTANTIATE_TEST_CASE_P(RoundTripTests, TestWriteRowBatch,
    ::testing::Values(&MakeIntRowBatch, &MakeListRowBatch, &MakeNonNullRowBatch,
                            &MakeZeroLengthRowBatch, &MakeDeeplyNestedList));

void TestGetRowBatchSize(std::shared_ptr<RowBatch> batch) {
  MockMemorySource mock_source(1 << 16);
  int64_t mock_header_location;
  int64_t size;
  ASSERT_OK(WriteRowBatch(&mock_source, batch.get(), 0, &mock_header_location));
  ASSERT_OK(GetRowBatchSize(batch.get(), &size));
  ASSERT_EQ(mock_source.GetExtentBytesWritten(), size);
}

TEST_F(TestWriteRowBatch, IntegerGetRowBatchSize) {
  std::shared_ptr<RowBatch> batch;

  ASSERT_OK(MakeIntRowBatch(&batch));
  TestGetRowBatchSize(batch);

  ASSERT_OK(MakeListRowBatch(&batch));
  TestGetRowBatchSize(batch);

  ASSERT_OK(MakeZeroLengthRowBatch(&batch));
  TestGetRowBatchSize(batch);

  ASSERT_OK(MakeNonNullRowBatch(&batch));
  TestGetRowBatchSize(batch);

  ASSERT_OK(MakeDeeplyNestedList(&batch));
  TestGetRowBatchSize(batch);
}

class RecursionLimits : public ::testing::Test, public MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { MemoryMapFixture::TearDown(); }

  Status WriteToMmap(int recursion_level, bool override_level,
      int64_t* header_out = nullptr, std::shared_ptr<Schema>* schema_out = nullptr) {
    const int batch_length = 5;
    TypePtr type = INT32;
    ArrayPtr array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = std::static_pointer_cast<DataType>(std::make_shared<ListType>(type));
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = std::make_shared<Field>("f0", type);
    std::shared_ptr<Schema> schema(new Schema({f0}));
    if (schema_out != nullptr) { *schema_out = schema; }
    std::vector<ArrayPtr> arrays = {array};
    auto batch = std::make_shared<RowBatch>(schema, batch_length, arrays);

    std::string path = "test-write-past-max-recursion";
    const int memory_map_size = 1 << 16;
    MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);
    int64_t header_location;
    int64_t* header_out_param = header_out == nullptr ? &header_location : header_out;
    if (override_level) {
      return WriteRowBatch(
          mmap_.get(), batch.get(), 0, header_out_param, recursion_level + 1);
    } else {
      return WriteRowBatch(mmap_.get(), batch.get(), 0, header_out_param);
    }
  }

 protected:
  std::shared_ptr<MemoryMappedSource> mmap_;
  MemoryPool* pool_;
};

TEST_F(RecursionLimits, WriteLimit) {
  ASSERT_RAISES(Invalid, WriteToMmap((1 << 8) + 1, false));
}

TEST_F(RecursionLimits, ReadLimit) {
  int64_t header_location;
  std::shared_ptr<Schema> schema;
  ASSERT_OK(WriteToMmap(64, true, &header_location, &schema));

  std::shared_ptr<RowBatchReader> reader;
  ASSERT_OK(RowBatchReader::Open(mmap_.get(), header_location, &reader));
  std::shared_ptr<RowBatch> batch_result;
  ASSERT_RAISES(Invalid, reader->GetRowBatch(schema, &batch_result));
}

}  // namespace ipc
}  // namespace arrow

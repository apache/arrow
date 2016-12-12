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

#include "arrow/io/memory.h"
#include "arrow/io/test-common.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/test-common.h"
#include "arrow/ipc/util.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {

class TestWriteRecordBatch : public ::testing::TestWithParam<MakeRecordBatch*>,
                             public io::MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }

  Status RoundTripHelper(const RecordBatch& batch, int memory_map_size,
      std::shared_ptr<RecordBatch>* batch_result) {
    std::string path = "test-write-row-batch";
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    int32_t metadata_length;
    int64_t body_length;

    const int64_t buffer_offset = 0;

    RETURN_NOT_OK(WriteRecordBatch(batch.columns(), batch.num_rows(), buffer_offset,
        mmap_.get(), &metadata_length, &body_length));

    std::shared_ptr<RecordBatchMetadata> metadata;
    RETURN_NOT_OK(ReadRecordBatchMetadata(0, metadata_length, mmap_.get(), &metadata));

    // The buffer offsets start at 0, so we must construct a
    // ReadableFileInterface according to that frame of reference
    std::shared_ptr<Buffer> buffer_payload;
    RETURN_NOT_OK(mmap_->ReadAt(metadata_length, body_length, &buffer_payload));
    io::BufferReader buffer_reader(buffer_payload);

    return ReadRecordBatch(metadata, batch.schema(), &buffer_reader, batch_result);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
};

TEST_P(TestWriteRecordBatch, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue
  std::shared_ptr<RecordBatch> batch_result;
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

INSTANTIATE_TEST_CASE_P(RoundTripTests, TestWriteRecordBatch,
    ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch, &MakeNonNullRecordBatch,
                            &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList,
                            &MakeStringTypesRecordBatch, &MakeStruct));

void TestGetRecordBatchSize(std::shared_ptr<RecordBatch> batch) {
  ipc::MockOutputStream mock;
  int32_t mock_metadata_length = -1;
  int64_t mock_body_length = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(batch->columns(), batch->num_rows(), 0, &mock,
      &mock_metadata_length, &mock_body_length));
  ASSERT_OK(GetRecordBatchSize(batch.get(), &size));
  ASSERT_EQ(mock.GetExtentBytesWritten(), size);
}

TEST_F(TestWriteRecordBatch, IntegerGetRecordBatchSize) {
  std::shared_ptr<RecordBatch> batch;

  ASSERT_OK(MakeIntRecordBatch(&batch));
  TestGetRecordBatchSize(batch);

  ASSERT_OK(MakeListRecordBatch(&batch));
  TestGetRecordBatchSize(batch);

  ASSERT_OK(MakeZeroLengthRecordBatch(&batch));
  TestGetRecordBatchSize(batch);

  ASSERT_OK(MakeNonNullRecordBatch(&batch));
  TestGetRecordBatchSize(batch);

  ASSERT_OK(MakeDeeplyNestedList(&batch));
  TestGetRecordBatchSize(batch);
}

class RecursionLimits : public ::testing::Test, public io::MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }

  Status WriteToMmap(int recursion_level, bool override_level, int32_t* metadata_length,
      int64_t* body_length, std::shared_ptr<Schema>* schema) {
    const int batch_length = 5;
    TypePtr type = int32();
    ArrayPtr array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = list(type);
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = field("f0", type);

    *schema = std::shared_ptr<Schema>(new Schema({f0}));

    std::vector<ArrayPtr> arrays = {array};
    auto batch = std::make_shared<RecordBatch>(*schema, batch_length, arrays);

    std::string path = "test-write-past-max-recursion";
    const int memory_map_size = 1 << 16;
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    if (override_level) {
      return WriteRecordBatch(batch->columns(), batch->num_rows(), 0, mmap_.get(),
          metadata_length, body_length, recursion_level + 1);
    } else {
      return WriteRecordBatch(batch->columns(), batch->num_rows(), 0, mmap_.get(),
          metadata_length, body_length);
    }
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
};

TEST_F(RecursionLimits, WriteLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;
  ASSERT_RAISES(
      Invalid, WriteToMmap((1 << 8) + 1, false, &metadata_length, &body_length, &schema));
}

TEST_F(RecursionLimits, ReadLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;
  ASSERT_OK(WriteToMmap(64, true, &metadata_length, &body_length, &schema));

  std::shared_ptr<RecordBatchMetadata> metadata;
  ASSERT_OK(ReadRecordBatchMetadata(0, metadata_length, mmap_.get(), &metadata));

  std::shared_ptr<Buffer> payload;
  ASSERT_OK(mmap_->ReadAt(metadata_length, body_length, &payload));

  io::BufferReader reader(payload);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_RAISES(Invalid, ReadRecordBatch(metadata, schema, &reader, &batch));
}

}  // namespace ipc
}  // namespace arrow

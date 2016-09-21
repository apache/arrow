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

#include "arrow/test-util.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

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

    int64_t body_end_offset;
    int64_t header_end_offset;

    RETURN_NOT_OK(WriteRecordBatch(batch.columns(), batch.num_rows(), mmap_.get(),
        &body_end_offset, &header_end_offset));

    std::shared_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(RecordBatchReader::Open(mmap_.get(), header_end_offset, &reader));

    RETURN_NOT_OK(reader->GetRecordBatch(batch.schema(), batch_result));
    return Status::OK();
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
  int64_t mock_header_offset = -1;
  int64_t mock_body_offset = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(batch->columns(), batch->num_rows(), &mock,
      &mock_body_offset, &mock_header_offset));
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

  Status WriteToMmap(int recursion_level, bool override_level,
      int64_t* header_out = nullptr, std::shared_ptr<Schema>* schema_out = nullptr) {
    const int batch_length = 5;
    TypePtr type = kInt32;
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
    auto batch = std::make_shared<RecordBatch>(schema, batch_length, arrays);

    std::string path = "test-write-past-max-recursion";
    const int memory_map_size = 1 << 16;
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    int64_t body_offset;
    int64_t header_offset;

    int64_t* header_out_param = header_out == nullptr ? &header_offset : header_out;
    if (override_level) {
      return WriteRecordBatch(batch->columns(), batch->num_rows(), mmap_.get(),
          &body_offset, header_out_param, recursion_level + 1);
    } else {
      return WriteRecordBatch(batch->columns(), batch->num_rows(), mmap_.get(),
          &body_offset, header_out_param);
    }
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
};

TEST_F(RecursionLimits, WriteLimit) {
  ASSERT_RAISES(Invalid, WriteToMmap((1 << 8) + 1, false));
}

TEST_F(RecursionLimits, ReadLimit) {
  int64_t header_offset = -1;
  std::shared_ptr<Schema> schema;
  ASSERT_OK(WriteToMmap(64, true, &header_offset, &schema));

  std::shared_ptr<RecordBatchReader> reader;
  ASSERT_OK(RecordBatchReader::Open(mmap_.get(), header_offset, &reader));
  std::shared_ptr<RecordBatch> batch_result;
  ASSERT_RAISES(Invalid, reader->GetRecordBatch(schema, &batch_result));
}

}  // namespace ipc
}  // namespace arrow

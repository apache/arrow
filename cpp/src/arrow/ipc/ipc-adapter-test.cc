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
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/test-common.h"
#include "arrow/ipc/util.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {

class IpcTestFixture : public io::MemoryMapFixture {
 public:
  Status RoundTripHelper(const RecordBatch& batch, int memory_map_size,
      std::shared_ptr<RecordBatch>* batch_result) {
    std::string path = "test-write-row-batch";
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    int32_t metadata_length;
    int64_t body_length;

    const int64_t buffer_offset = 0;

    RETURN_NOT_OK(WriteRecordBatch(
        batch, buffer_offset, mmap_.get(), &metadata_length, &body_length, pool_));

    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));
    auto metadata = std::make_shared<RecordBatchMetadata>(message);

    // The buffer offsets start at 0, so we must construct a
    // ReadableFileInterface according to that frame of reference
    std::shared_ptr<Buffer> buffer_payload;
    RETURN_NOT_OK(mmap_->ReadAt(metadata_length, body_length, &buffer_payload));
    io::BufferReader buffer_reader(buffer_payload);

    return ReadRecordBatch(*metadata, batch.schema(), &buffer_reader, batch_result);
  }

  void CheckRoundtrip(const RecordBatch& batch, int64_t buffer_size) {
    std::shared_ptr<RecordBatch> batch_result;

    ASSERT_OK(RoundTripHelper(batch, 1 << 16, &batch_result));
    EXPECT_EQ(batch.num_rows(), batch_result->num_rows());

    ASSERT_TRUE(batch.schema()->Equals(batch_result->schema()));
    ASSERT_EQ(batch.num_columns(), batch_result->num_columns())
        << batch.schema()->ToString()
        << " result: " << batch_result->schema()->ToString();

    for (int i = 0; i < batch.num_columns(); ++i) {
      const auto& left = *batch.column(i);
      const auto& right = *batch_result->column(i);
      if (!left.Equals(right)) {
        std::stringstream pp_result;
        std::stringstream pp_expected;

        ASSERT_OK(PrettyPrint(left, 0, &pp_expected));
        ASSERT_OK(PrettyPrint(right, 0, &pp_result));

        FAIL() << "Index: " << i << " Expected: " << pp_expected.str()
               << "\nGot: " << pp_result.str();
      }
    }
  }

  void CheckRoundtrip(const std::shared_ptr<Array>& array, int64_t buffer_size) {
    auto f0 = arrow::field("f0", array->type());
    std::vector<std::shared_ptr<Field>> fields = {f0};
    auto schema = std::make_shared<Schema>(fields);

    RecordBatch batch(schema, 0, {array});
    CheckRoundtrip(batch, buffer_size);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
};

class TestWriteRecordBatch : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }
};

class TestRecordBatchParam : public ::testing::TestWithParam<MakeRecordBatch*>,
                             public IpcTestFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }
  using IpcTestFixture::RoundTripHelper;
  using IpcTestFixture::CheckRoundtrip;
};

TEST_P(TestRecordBatchParam, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  CheckRoundtrip(*batch, 1 << 20);
}

TEST_P(TestRecordBatchParam, SliceRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  // Skip the zero-length case
  if (batch->num_rows() < 2) { return; }

  auto sliced_batch = batch->Slice(2, 10);
  CheckRoundtrip(*sliced_batch, 1 << 20);
}

TEST_P(TestRecordBatchParam, ZeroLengthArrays) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  std::shared_ptr<RecordBatch> zero_length_batch;
  if (batch->num_rows() > 2) {
    zero_length_batch = batch->Slice(2, 0);
  } else {
    zero_length_batch = batch->Slice(0, 0);
  }

  CheckRoundtrip(*zero_length_batch, 1 << 20);

  // ARROW-544: check binary array
  std::shared_ptr<MutableBuffer> value_offsets;
  ASSERT_OK(AllocateBuffer(pool_, sizeof(int32_t), &value_offsets));
  *reinterpret_cast<int32_t*>(value_offsets->mutable_data()) = 0;

  std::shared_ptr<Array> bin_array = std::make_shared<BinaryArray>(0, value_offsets,
      std::make_shared<Buffer>(nullptr, 0), std::make_shared<Buffer>(nullptr, 0));

  // null value_offsets
  std::shared_ptr<Array> bin_array2 = std::make_shared<BinaryArray>(0, nullptr, nullptr);

  CheckRoundtrip(bin_array, 1 << 20);
  CheckRoundtrip(bin_array2, 1 << 20);
}

INSTANTIATE_TEST_CASE_P(
    RoundTripTests, TestRecordBatchParam,
    ::testing::Values(&MakeIntRecordBatch, &MakeStringTypesRecordBatch,
        &MakeNonNullRecordBatch, &MakeZeroLengthRecordBatch, &MakeListRecordBatch,
        &MakeDeeplyNestedList, &MakeStruct, &MakeUnion, &MakeDictionary));

void TestGetRecordBatchSize(std::shared_ptr<RecordBatch> batch) {
  ipc::MockOutputStream mock;
  int32_t mock_metadata_length = -1;
  int64_t mock_body_length = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(
      *batch, 0, &mock, &mock_metadata_length, &mock_body_length, default_memory_pool()));
  ASSERT_OK(GetRecordBatchSize(*batch, &size));
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
    std::shared_ptr<Array> array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = list(type);
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = field("f0", type);

    *schema = std::shared_ptr<Schema>(new Schema({f0}));

    std::vector<std::shared_ptr<Array>> arrays = {array};
    auto batch = std::make_shared<RecordBatch>(*schema, batch_length, arrays);

    std::string path = "test-write-past-max-recursion";
    const int memory_map_size = 1 << 16;
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    if (override_level) {
      return WriteRecordBatch(*batch, 0, mmap_.get(), metadata_length, body_length, pool_,
          recursion_level + 1);
    } else {
      return WriteRecordBatch(
          *batch, 0, mmap_.get(), metadata_length, body_length, pool_);
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

  std::shared_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));
  auto metadata = std::make_shared<RecordBatchMetadata>(message);

  std::shared_ptr<Buffer> payload;
  ASSERT_OK(mmap_->ReadAt(metadata_length, body_length, &payload));

  io::BufferReader reader(payload);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_RAISES(Invalid, ReadRecordBatch(*metadata, schema, &reader, &batch));
}

}  // namespace ipc
}  // namespace arrow

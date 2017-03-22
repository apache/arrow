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

#include "arrow/array.h"
#include "arrow/io/memory.h"
#include "arrow/io/test-common.h"
#include "arrow/ipc/api.h"
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

void CompareBatch(const RecordBatch& left, const RecordBatch& right) {
  if (!left.schema()->Equals(right.schema())) {
    FAIL() << "Left schema: " << left.schema()->ToString()
           << "\nRight schema: " << right.schema()->ToString();
  }
  ASSERT_EQ(left.num_columns(), right.num_columns())
      << left.schema()->ToString() << " result: " << right.schema()->ToString();
  EXPECT_EQ(left.num_rows(), right.num_rows());
  for (int i = 0; i < left.num_columns(); ++i) {
    EXPECT_TRUE(left.column(i)->Equals(right.column(i)))
        << "Idx: " << i << " Name: " << left.column_name(i);
  }
}

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

class TestSchemaMetadata : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema& schema, DictionaryMemo* memo) {
    std::shared_ptr<Buffer> buffer;
    ASSERT_OK(WriteSchemaMessage(schema, memo, &buffer));

    std::shared_ptr<Message> message;
    ASSERT_OK(Message::Open(buffer, 0, &message));

    ASSERT_EQ(Message::SCHEMA, message->type());

    auto schema_msg = std::make_shared<SchemaMetadata>(message);
    ASSERT_EQ(schema.num_fields(), schema_msg->num_fields());

    DictionaryMemo empty_memo;

    std::shared_ptr<Schema> schema2;
    ASSERT_OK(schema_msg->GetSchema(empty_memo, &schema2));

    AssertSchemaEqual(schema, *schema2);
  }
};

const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();

TEST_F(TestSchemaMetadata, PrimitiveFields) {
  auto f0 = std::make_shared<Field>("f0", std::make_shared<Int8Type>());
  auto f1 = std::make_shared<Field>("f1", std::make_shared<Int16Type>(), false);
  auto f2 = std::make_shared<Field>("f2", std::make_shared<Int32Type>());
  auto f3 = std::make_shared<Field>("f3", std::make_shared<Int64Type>());
  auto f4 = std::make_shared<Field>("f4", std::make_shared<UInt8Type>());
  auto f5 = std::make_shared<Field>("f5", std::make_shared<UInt16Type>());
  auto f6 = std::make_shared<Field>("f6", std::make_shared<UInt32Type>());
  auto f7 = std::make_shared<Field>("f7", std::make_shared<UInt64Type>());
  auto f8 = std::make_shared<Field>("f8", std::make_shared<FloatType>());
  auto f9 = std::make_shared<Field>("f9", std::make_shared<DoubleType>(), false);
  auto f10 = std::make_shared<Field>("f10", std::make_shared<BooleanType>());

  Schema schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10});
  DictionaryMemo memo;

  CheckRoundtrip(schema, &memo);
}

TEST_F(TestSchemaMetadata, NestedFields) {
  auto type = std::make_shared<ListType>(std::make_shared<Int32Type>());
  auto f0 = std::make_shared<Field>("f0", type);

  std::shared_ptr<StructType> type2(new StructType({std::make_shared<Field>("k1", INT32),
      std::make_shared<Field>("k2", INT32), std::make_shared<Field>("k3", INT32)}));
  auto f1 = std::make_shared<Field>("f1", type2);

  Schema schema({f0, f1});
  DictionaryMemo memo;

  CheckRoundtrip(schema, &memo);
}

#define BATCH_CASES()                                                                    \
  ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch, &MakeNonNullRecordBatch,  \
      &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList, &MakeStringTypesRecordBatch,    \
      &MakeStruct, &MakeUnion, &MakeDictionary, &MakeDates, &MakeTimestamps, &MakeTimes, \
      &MakeFWBinary);

class IpcTestFixture : public io::MemoryMapFixture {
 public:
  Status DoStandardRoundTrip(const RecordBatch& batch, bool zero_data,
      std::shared_ptr<RecordBatch>* batch_result) {
    int32_t metadata_length;
    int64_t body_length;

    const int64_t buffer_offset = 0;

    if (zero_data) { RETURN_NOT_OK(ZeroMemoryMap(mmap_.get())); }
    RETURN_NOT_OK(mmap_->Seek(0));

    RETURN_NOT_OK(WriteRecordBatch(
        batch, buffer_offset, mmap_.get(), &metadata_length, &body_length, pool_));

    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));
    auto metadata = std::make_shared<RecordBatchMetadata>(message);

    // The buffer offsets start at 0, so we must construct a
    // RandomAccessFile according to that frame of reference
    std::shared_ptr<Buffer> buffer_payload;
    RETURN_NOT_OK(mmap_->ReadAt(metadata_length, body_length, &buffer_payload));
    io::BufferReader buffer_reader(buffer_payload);

    return ReadRecordBatch(*metadata, batch.schema(), &buffer_reader, batch_result);
  }

  Status DoLargeRoundTrip(
      const RecordBatch& batch, bool zero_data, std::shared_ptr<RecordBatch>* result) {
    int32_t metadata_length;
    int64_t body_length;

    const int64_t buffer_offset = 0;

    if (zero_data) { RETURN_NOT_OK(ZeroMemoryMap(mmap_.get())); }
    RETURN_NOT_OK(mmap_->Seek(0));

    RETURN_NOT_OK(WriteLargeRecordBatch(
        batch, buffer_offset, mmap_.get(), &metadata_length, &body_length, pool_));
    return ReadLargeRecordBatch(batch.schema(), 0, mmap_.get(), result);
  }

  void CheckReadResult(const RecordBatch& result, const RecordBatch& expected) {
    EXPECT_EQ(expected.num_rows(), result.num_rows());

    ASSERT_TRUE(expected.schema()->Equals(result.schema()));
    ASSERT_EQ(expected.num_columns(), result.num_columns())
        << expected.schema()->ToString() << " result: " << result.schema()->ToString();

    for (int i = 0; i < expected.num_columns(); ++i) {
      const auto& left = *expected.column(i);
      const auto& right = *result.column(i);
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

  void CheckRoundtrip(const RecordBatch& batch, int64_t buffer_size) {
    std::string path = "test-write-row-batch";
    ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(buffer_size, path, &mmap_));

    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(DoStandardRoundTrip(batch, true, &result));
    CheckReadResult(*result, batch);

    ASSERT_OK(DoLargeRoundTrip(batch, true, &result));
    CheckReadResult(*result, batch);
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

class TestIpcRoundTrip : public ::testing::TestWithParam<MakeRecordBatch*>,
                         public IpcTestFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }
};

TEST_P(TestIpcRoundTrip, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  CheckRoundtrip(*batch, 1 << 20);
}

TEST_P(TestIpcRoundTrip, SliceRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  // Skip the zero-length case
  if (batch->num_rows() < 2) { return; }

  auto sliced_batch = batch->Slice(2, 10);
  CheckRoundtrip(*sliced_batch, 1 << 20);
}

TEST_P(TestIpcRoundTrip, ZeroLengthArrays) {
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
      int64_t* body_length, std::shared_ptr<RecordBatch>* batch,
      std::shared_ptr<Schema>* schema) {
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
    *batch = std::make_shared<RecordBatch>(*schema, batch_length, arrays);

    std::string path = "test-write-past-max-recursion";
    const int memory_map_size = 1 << 20;
    io::MemoryMapFixture::InitMemoryMap(memory_map_size, path, &mmap_);

    if (override_level) {
      return WriteRecordBatch(**batch, 0, mmap_.get(), metadata_length, body_length,
          pool_, recursion_level + 1);
    } else {
      return WriteRecordBatch(
          **batch, 0, mmap_.get(), metadata_length, body_length, pool_);
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
  std::shared_ptr<RecordBatch> batch;
  ASSERT_RAISES(Invalid,
      WriteToMmap((1 << 8) + 1, false, &metadata_length, &body_length, &batch, &schema));
}

TEST_F(RecursionLimits, ReadLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;

  const int recursion_depth = 64;

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(WriteToMmap(
      recursion_depth, true, &metadata_length, &body_length, &batch, &schema));

  std::shared_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));
  auto metadata = std::make_shared<RecordBatchMetadata>(message);

  std::shared_ptr<Buffer> payload;
  ASSERT_OK(mmap_->ReadAt(metadata_length, body_length, &payload));

  io::BufferReader reader(payload);

  std::shared_ptr<RecordBatch> result;
  ASSERT_RAISES(Invalid, ReadRecordBatch(*metadata, schema, &reader, &result));
}

TEST_F(RecursionLimits, StressLimit) {
  auto CheckDepth = [this](int recursion_depth, bool* it_works) {
    int32_t metadata_length = -1;
    int64_t body_length = -1;
    std::shared_ptr<Schema> schema;
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(WriteToMmap(
        recursion_depth, true, &metadata_length, &body_length, &batch, &schema));

    std::shared_ptr<Message> message;
    ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));
    auto metadata = std::make_shared<RecordBatchMetadata>(message);

    std::shared_ptr<Buffer> payload;
    ASSERT_OK(mmap_->ReadAt(metadata_length, body_length, &payload));

    io::BufferReader reader(payload);

    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(ReadRecordBatch(*metadata, schema, recursion_depth + 1, &reader, &result));
    *it_works = result->Equals(*batch);
  };

  bool it_works = false;
  CheckDepth(100, &it_works);
  ASSERT_TRUE(it_works);

  CheckDepth(500, &it_works);
  ASSERT_TRUE(it_works);
}

class TestFileFormat : public ::testing::TestWithParam<MakeRecordBatch*> {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    buffer_ = std::make_shared<PoolBuffer>(pool_);
    sink_.reset(new io::BufferOutputStream(buffer_));
  }
  void TearDown() {}

  Status RoundTripHelper(const BatchVector& in_batches, BatchVector* out_batches) {
    // Write the file
    std::shared_ptr<FileWriter> writer;
    RETURN_NOT_OK(FileWriter::Open(sink_.get(), in_batches[0]->schema(), &writer));

    const int num_batches = static_cast<int>(in_batches.size());

    for (const auto& batch : in_batches) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    RETURN_NOT_OK(writer->Close());
    RETURN_NOT_OK(sink_->Close());

    // Current offset into stream is the end of the file
    int64_t footer_offset;
    RETURN_NOT_OK(sink_->Tell(&footer_offset));

    // Open the file
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    std::shared_ptr<FileReader> reader;
    RETURN_NOT_OK(FileReader::Open(buf_reader, footer_offset, &reader));

    EXPECT_EQ(num_batches, reader->num_record_batches());
    for (int i = 0; i < num_batches; ++i) {
      std::shared_ptr<RecordBatch> chunk;
      RETURN_NOT_OK(reader->GetRecordBatch(i, &chunk));
      out_batches->emplace_back(chunk);
    }

    return Status::OK();
  }

 protected:
  MemoryPool* pool_;

  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<PoolBuffer> buffer_;
};

TEST_P(TestFileFormat, RoundTrip) {
  std::shared_ptr<RecordBatch> batch1;
  std::shared_ptr<RecordBatch> batch2;
  ASSERT_OK((*GetParam())(&batch1));  // NOLINT clang-tidy gtest issue
  ASSERT_OK((*GetParam())(&batch2));  // NOLINT clang-tidy gtest issue

  std::vector<std::shared_ptr<RecordBatch>> in_batches = {batch1, batch2};
  std::vector<std::shared_ptr<RecordBatch>> out_batches;

  ASSERT_OK(RoundTripHelper(in_batches, &out_batches));

  // Compare batches
  for (size_t i = 0; i < in_batches.size(); ++i) {
    CompareBatch(*in_batches[i], *out_batches[i]);
  }
}

class TestStreamFormat : public ::testing::TestWithParam<MakeRecordBatch*> {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    buffer_ = std::make_shared<PoolBuffer>(pool_);
    sink_.reset(new io::BufferOutputStream(buffer_));
  }
  void TearDown() {}

  Status RoundTripHelper(
      const RecordBatch& batch, std::vector<std::shared_ptr<RecordBatch>>* out_batches) {
    // Write the file
    std::shared_ptr<StreamWriter> writer;
    RETURN_NOT_OK(StreamWriter::Open(sink_.get(), batch.schema(), &writer));
    int num_batches = 5;
    for (int i = 0; i < num_batches; ++i) {
      RETURN_NOT_OK(writer->WriteRecordBatch(batch));
    }
    RETURN_NOT_OK(writer->Close());
    RETURN_NOT_OK(sink_->Close());

    // Open the file
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);

    std::shared_ptr<StreamReader> reader;
    RETURN_NOT_OK(StreamReader::Open(buf_reader, &reader));

    std::shared_ptr<RecordBatch> chunk;
    while (true) {
      RETURN_NOT_OK(reader->GetNextRecordBatch(&chunk));
      if (chunk == nullptr) { break; }
      out_batches->emplace_back(chunk);
    }
    return Status::OK();
  }

 protected:
  MemoryPool* pool_;

  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<PoolBuffer> buffer_;
};

TEST_P(TestStreamFormat, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  std::vector<std::shared_ptr<RecordBatch>> out_batches;

  ASSERT_OK(RoundTripHelper(*batch, &out_batches));

  // Compare batches. Same
  for (size_t i = 0; i < out_batches.size(); ++i) {
    CompareBatch(*batch, *out_batches[i]);
  }
}

INSTANTIATE_TEST_CASE_P(GenericIpcRoundTripTests, TestIpcRoundTrip, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(FileRoundTripTests, TestFileFormat, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(StreamRoundTripTests, TestStreamFormat, BATCH_CASES());

TEST_F(TestIpcRoundTrip, LargeRecordBatch) {
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

  BooleanBuilder builder(default_memory_pool());
  ASSERT_OK(builder.Reserve(length));
  ASSERT_OK(builder.Advance(length));

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  auto f0 = arrow::field("f0", array->type());
  std::vector<std::shared_ptr<Field>> fields = {f0};
  auto schema = std::make_shared<Schema>(fields);

  RecordBatch batch(schema, 0, {array});

  std::string path = "test-write-large-record_batch";

  // 512 MB
  constexpr int64_t kBufferSize = 1 << 29;

  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::shared_ptr<RecordBatch> result;
  ASSERT_OK(DoLargeRoundTrip(batch, false, &result));
  CheckReadResult(*result, batch);

  // Fails if we try to write this with the normal code path
  ASSERT_RAISES(Invalid, DoStandardRoundTrip(batch, false, &result));
}

void CheckBatchDictionaries(const RecordBatch& batch) {
  // Check that dictionaries that should be the same are the same
  auto schema = batch.schema();

  const auto& t0 = static_cast<const DictionaryType&>(*schema->field(0)->type);
  const auto& t1 = static_cast<const DictionaryType&>(*schema->field(1)->type);

  ASSERT_EQ(t0.dictionary().get(), t1.dictionary().get());

  // Same dictionary used for list values
  const auto& t3 = static_cast<const ListType&>(*schema->field(3)->type);
  const auto& t3_value = static_cast<const DictionaryType&>(*t3.value_type());
  ASSERT_EQ(t0.dictionary().get(), t3_value.dictionary().get());
}

TEST_F(TestStreamFormat, DictionaryRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionary(&batch));

  std::vector<std::shared_ptr<RecordBatch>> out_batches;
  ASSERT_OK(RoundTripHelper(*batch, &out_batches));

  CheckBatchDictionaries(*out_batches[0]);
}

TEST_F(TestFileFormat, DictionaryRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionary(&batch));

  std::vector<std::shared_ptr<RecordBatch>> out_batches;
  ASSERT_OK(RoundTripHelper({batch}, &out_batches));

  CheckBatchDictionaries(*out_batches[0]);
}

}  // namespace ipc
}  // namespace arrow

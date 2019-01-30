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
#include <limits>
#include <memory>
#include <ostream>
#include <string>

#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test-common.h"
#include "arrow/ipc/Message_generated.h"  // IWYU pragma: keep
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test-common.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace ipc {

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

class TestSchemaMetadata : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema& schema) {
    std::shared_ptr<Buffer> buffer;
    ASSERT_OK(SerializeSchema(schema, default_memory_pool(), &buffer));

    std::shared_ptr<Schema> result;
    io::BufferReader reader(buffer);
    ASSERT_OK(ReadSchema(&reader, &result));
    AssertSchemaEqual(schema, *result);
  }
};

TEST(TestMessage, Equals) {
  std::string metadata = "foo";
  std::string body = "bar";

  auto b1 = std::make_shared<Buffer>(metadata);
  auto b2 = std::make_shared<Buffer>(metadata);
  auto b3 = std::make_shared<Buffer>(body);
  auto b4 = std::make_shared<Buffer>(body);

  Message msg1(b1, b3);
  Message msg2(b2, b4);
  Message msg3(b1, nullptr);
  Message msg4(b2, nullptr);

  ASSERT_TRUE(msg1.Equals(msg2));
  ASSERT_TRUE(msg3.Equals(msg4));

  ASSERT_FALSE(msg1.Equals(msg3));
  ASSERT_FALSE(msg3.Equals(msg1));

  // same metadata as msg1, different body
  Message msg5(b2, b1);
  ASSERT_FALSE(msg1.Equals(msg5));
  ASSERT_FALSE(msg5.Equals(msg1));
}

TEST(TestMessage, SerializeTo) {
  const int64_t body_length = 64;

  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(flatbuf::CreateMessage(fbb, internal::kCurrentMetadataVersion,
                                    flatbuf::MessageHeader_RecordBatch, 0 /* header */,
                                    body_length));

  std::shared_ptr<Buffer> metadata;
  ASSERT_OK(internal::WriteFlatbufferBuilder(fbb, &metadata));

  std::string body = "abcdef";

  std::unique_ptr<Message> message;
  ASSERT_OK(Message::Open(metadata, std::make_shared<Buffer>(body), &message));

  int64_t output_length = 0;
  int64_t position = 0;

  std::shared_ptr<io::BufferOutputStream> stream;

  {
    const int32_t alignment = 8;

    ASSERT_OK(io::BufferOutputStream::Create(1 << 10, default_memory_pool(), &stream));
    ASSERT_OK(message->SerializeTo(stream.get(), alignment, &output_length));
    ASSERT_OK(stream->Tell(&position));
    ASSERT_EQ(BitUtil::RoundUp(metadata->size() + 4, alignment) + body_length,
              output_length);
    ASSERT_EQ(output_length, position);
  }

  {
    const int32_t alignment = 64;

    ASSERT_OK(io::BufferOutputStream::Create(1 << 10, default_memory_pool(), &stream));
    ASSERT_OK(message->SerializeTo(stream.get(), alignment, &output_length));
    ASSERT_OK(stream->Tell(&position));
    ASSERT_EQ(BitUtil::RoundUp(metadata->size() + 4, alignment) + body_length,
              output_length);
    ASSERT_EQ(output_length, position);
  }
}

TEST(TestMessage, Verify) {
  std::string metadata = "invalid";
  std::string body = "abcdef";

  Message message(std::make_shared<Buffer>(metadata), std::make_shared<Buffer>(body));
  ASSERT_FALSE(message.Verify());
}

const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();

TEST_F(TestSchemaMetadata, PrimitiveFields) {
  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false);
  auto f2 = field("f2", std::make_shared<Int32Type>());
  auto f3 = field("f3", std::make_shared<Int64Type>());
  auto f4 = field("f4", std::make_shared<UInt8Type>());
  auto f5 = field("f5", std::make_shared<UInt16Type>());
  auto f6 = field("f6", std::make_shared<UInt32Type>());
  auto f7 = field("f7", std::make_shared<UInt64Type>());
  auto f8 = field("f8", std::make_shared<FloatType>());
  auto f9 = field("f9", std::make_shared<DoubleType>(), false);
  auto f10 = field("f10", std::make_shared<BooleanType>());

  Schema schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10});
  CheckRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, NestedFields) {
  auto type = list(int32());
  auto f0 = field("f0", type);

  std::shared_ptr<StructType> type2(
      new StructType({field("k1", INT32), field("k2", INT32), field("k3", INT32)}));
  auto f1 = field("f1", type2);

  Schema schema({f0, f1});
  CheckRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, KeyValueMetadata) {
  auto field_metadata = key_value_metadata({{"key", "value"}});
  auto schema_metadata = key_value_metadata({{"foo", "bar"}, {"bizz", "buzz"}});

  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false, field_metadata);

  Schema schema({f0, f1}, schema_metadata);
  CheckRoundtrip(schema);
}

#define BATCH_CASES()                                                                   \
  ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch, &MakeNonNullRecordBatch, \
                    &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList,                  \
                    &MakeStringTypesRecordBatchWithNulls, &MakeStruct, &MakeUnion,      \
                    &MakeDictionary, &MakeDates, &MakeTimestamps, &MakeTimes,           \
                    &MakeFWBinary, &MakeNull, &MakeDecimal, &MakeBooleanBatch);

static int g_file_number = 0;

class IpcTestFixture : public io::MemoryMapFixture {
 public:
  Status DoSchemaRoundTrip(const Schema& schema, std::shared_ptr<Schema>* result) {
    std::shared_ptr<Buffer> serialized_schema;
    RETURN_NOT_OK(SerializeSchema(schema, pool_, &serialized_schema));

    io::BufferReader buf_reader(serialized_schema);
    return ReadSchema(&buf_reader, result);
  }

  Status DoStandardRoundTrip(const RecordBatch& batch,
                             std::shared_ptr<RecordBatch>* batch_result) {
    std::shared_ptr<Buffer> serialized_batch;
    RETURN_NOT_OK(SerializeRecordBatch(batch, pool_, &serialized_batch));

    io::BufferReader buf_reader(serialized_batch);
    return ReadRecordBatch(batch.schema(), &buf_reader, batch_result);
  }

  Status DoLargeRoundTrip(const RecordBatch& batch, bool zero_data,
                          std::shared_ptr<RecordBatch>* result) {
    if (zero_data) {
      RETURN_NOT_OK(ZeroMemoryMap(mmap_.get()));
    }
    RETURN_NOT_OK(mmap_->Seek(0));

    std::shared_ptr<RecordBatchWriter> file_writer;
    RETURN_NOT_OK(RecordBatchFileWriter::Open(mmap_.get(), batch.schema(), &file_writer));
    RETURN_NOT_OK(file_writer->WriteRecordBatch(batch, true));
    RETURN_NOT_OK(file_writer->Close());

    int64_t offset;
    RETURN_NOT_OK(mmap_->Tell(&offset));

    std::shared_ptr<RecordBatchFileReader> file_reader;
    RETURN_NOT_OK(RecordBatchFileReader::Open(mmap_.get(), offset, &file_reader));

    return file_reader->ReadRecordBatch(0, result);
  }

  void CheckReadResult(const RecordBatch& result, const RecordBatch& expected) {
    EXPECT_EQ(expected.num_rows(), result.num_rows());

    ASSERT_TRUE(expected.schema()->Equals(*result.schema()));
    ASSERT_EQ(expected.num_columns(), result.num_columns())
        << expected.schema()->ToString() << " result: " << result.schema()->ToString();

    CompareBatchColumnsDetailed(result, expected);
  }

  void CheckRoundtrip(const RecordBatch& batch, int64_t buffer_size) {
    std::stringstream ss;
    ss << "test-write-row-batch-" << g_file_number++;
    ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(buffer_size, ss.str(), &mmap_));

    std::shared_ptr<Schema> schema_result;
    ASSERT_OK(DoSchemaRoundTrip(*batch.schema(), &schema_result));
    ASSERT_TRUE(batch.schema()->Equals(*schema_result));

    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(DoStandardRoundTrip(batch, &result));
    CheckReadResult(*result, batch);

    ASSERT_OK(DoLargeRoundTrip(batch, true, &result));
    CheckReadResult(*result, batch);
  }

  void CheckRoundtrip(const std::shared_ptr<Array>& array, int64_t buffer_size) {
    auto f0 = arrow::field("f0", array->type());
    std::vector<std::shared_ptr<Field>> fields = {f0};
    auto schema = std::make_shared<Schema>(fields);

    auto batch = RecordBatch::Make(schema, 0, {array});
    CheckRoundtrip(*batch, buffer_size);
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

TEST_F(TestIpcRoundTrip, MetadataVersion) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntRecordBatch(&batch));

  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(1 << 16, "test-metadata", &mmap_));

  int32_t metadata_length;
  int64_t body_length;

  const int64_t buffer_offset = 0;

  ASSERT_OK(WriteRecordBatch(*batch, buffer_offset, mmap_.get(), &metadata_length,
                             &body_length, pool_));

  std::unique_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

  ASSERT_EQ(MetadataVersion::V4, message->metadata_version());
}

TEST_P(TestIpcRoundTrip, SliceRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  // Skip the zero-length case
  if (batch->num_rows() < 2) {
    return;
  }

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
  std::shared_ptr<Buffer> value_offsets;
  ASSERT_OK(AllocateBuffer(pool_, sizeof(int32_t), &value_offsets));
  *reinterpret_cast<int32_t*>(value_offsets->mutable_data()) = 0;

  std::shared_ptr<Array> bin_array = std::make_shared<BinaryArray>(
      0, value_offsets, std::make_shared<Buffer>(nullptr, 0),
      std::make_shared<Buffer>(nullptr, 0));

  // null value_offsets
  std::shared_ptr<Array> bin_array2 = std::make_shared<BinaryArray>(0, nullptr, nullptr);

  CheckRoundtrip(bin_array, 1 << 20);
  CheckRoundtrip(bin_array2, 1 << 20);
}

TEST_F(TestWriteRecordBatch, SliceTruncatesBuffers) {
  auto CheckArray = [this](const std::shared_ptr<Array>& array) {
    auto f0 = field("f0", array->type());
    auto schema = ::arrow::schema({f0});
    auto batch = RecordBatch::Make(schema, array->length(), {array});
    auto sliced_batch = batch->Slice(0, 5);

    int64_t full_size;
    int64_t sliced_size;

    ASSERT_OK(GetRecordBatchSize(*batch, &full_size));
    ASSERT_OK(GetRecordBatchSize(*sliced_batch, &sliced_size));
    ASSERT_TRUE(sliced_size < full_size) << sliced_size << " " << full_size;

    // make sure we can write and read it
    this->CheckRoundtrip(*sliced_batch, 1 << 20);
  };

  std::shared_ptr<Array> a0, a1;
  auto pool = default_memory_pool();

  // Integer
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  CheckArray(a0);

  // String / Binary
  {
    auto s = MakeRandomBinaryArray<StringBuilder, char>(500, false, pool, &a0);
    ASSERT_TRUE(s.ok());
  }
  CheckArray(a0);

  // Boolean
  ASSERT_OK(MakeRandomBooleanArray(10000, false, &a0));
  CheckArray(a0);

  // List
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  ASSERT_OK(MakeRandomListArray(a0, 200, false, pool, &a1));
  CheckArray(a1);

  // Struct
  auto struct_type = struct_({field("f0", a0->type())});
  std::vector<std::shared_ptr<Array>> struct_children = {a0};
  a1 = std::make_shared<StructArray>(struct_type, a0->length(), struct_children);
  CheckArray(a1);

  // Sparse Union
  auto union_type = union_({field("f0", a0->type())}, {0});
  std::vector<int32_t> type_ids(a0->length());
  std::shared_ptr<Buffer> ids_buffer;
  ASSERT_OK(CopyBufferFromVector(type_ids, default_memory_pool(), &ids_buffer));
  a1 =
      std::make_shared<UnionArray>(union_type, a0->length(), struct_children, ids_buffer);
  CheckArray(a1);

  // Dense union
  auto dense_union_type = union_({field("f0", a0->type())}, {0}, UnionMode::DENSE);
  std::vector<int32_t> type_offsets;
  for (int32_t i = 0; i < a0->length(); ++i) {
    type_offsets.push_back(i);
  }
  std::shared_ptr<Buffer> offsets_buffer;
  ASSERT_OK(CopyBufferFromVector(type_offsets, default_memory_pool(), &offsets_buffer));
  a1 = std::make_shared<UnionArray>(dense_union_type, a0->length(), struct_children,
                                    ids_buffer, offsets_buffer);
  CheckArray(a1);
}

void TestGetRecordBatchSize(std::shared_ptr<RecordBatch> batch) {
  io::MockOutputStream mock;
  int32_t mock_metadata_length = -1;
  int64_t mock_body_length = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(*batch, 0, &mock, &mock_metadata_length, &mock_body_length,
                             default_memory_pool()));
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
    auto type = int32();
    std::shared_ptr<Array> array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = list(type);
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = field("f0", type);

    *schema = ::arrow::schema({f0});

    *batch = RecordBatch::Make(*schema, batch_length, {array});

    std::stringstream ss;
    ss << "test-write-past-max-recursion-" << g_file_number++;
    const int memory_map_size = 1 << 20;
    RETURN_NOT_OK(io::MemoryMapFixture::InitMemoryMap(memory_map_size, ss.str(), &mmap_));

    if (override_level) {
      return WriteRecordBatch(**batch, 0, mmap_.get(), metadata_length, body_length,
                              pool_, recursion_level + 1);
    } else {
      return WriteRecordBatch(**batch, 0, mmap_.get(), metadata_length, body_length,
                              pool_);
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
  ASSERT_RAISES(Invalid, WriteToMmap((1 << 8) + 1, false, &metadata_length, &body_length,
                                     &batch, &schema));
}

TEST_F(RecursionLimits, ReadLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;

  const int recursion_depth = 64;

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                        &schema));

  std::unique_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

  io::BufferReader reader(message->body());

  std::shared_ptr<RecordBatch> result;
  ASSERT_RAISES(Invalid, ReadRecordBatch(*message->metadata(), schema, &reader, &result));
}

// Test fails with a structured exception on Windows + Debug
#if !defined(_WIN32) || defined(NDEBUG)
TEST_F(RecursionLimits, StressLimit) {
  auto CheckDepth = [this](int recursion_depth, bool* it_works) {
    int32_t metadata_length = -1;
    int64_t body_length = -1;
    std::shared_ptr<Schema> schema;
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                          &schema));

    std::unique_ptr<Message> message;
    ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

    io::BufferReader reader(message->body());
    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(ReadRecordBatch(*message->metadata(), schema, recursion_depth + 1, &reader,
                              &result));
    *it_works = result->Equals(*batch);
  };

  bool it_works = false;
  CheckDepth(100, &it_works);
  ASSERT_TRUE(it_works);

// Mitigate Valgrind's slowness
#if !defined(ARROW_VALGRIND)
  CheckDepth(500, &it_works);
  ASSERT_TRUE(it_works);
#endif
}
#endif  // !defined(_WIN32) || defined(NDEBUG)

class TestFileFormat : public ::testing::TestWithParam<MakeRecordBatch*> {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    ASSERT_OK(AllocateResizableBuffer(pool_, 0, &buffer_));
    sink_.reset(new io::BufferOutputStream(buffer_));
  }
  void TearDown() {}

  Status RoundTripHelper(const BatchVector& in_batches, BatchVector* out_batches) {
    // Write the file
    std::shared_ptr<RecordBatchWriter> writer;
    RETURN_NOT_OK(
        RecordBatchFileWriter::Open(sink_.get(), in_batches[0]->schema(), &writer));

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
    std::shared_ptr<RecordBatchFileReader> reader;
    RETURN_NOT_OK(RecordBatchFileReader::Open(buf_reader.get(), footer_offset, &reader));

    EXPECT_EQ(num_batches, reader->num_record_batches());
    for (int i = 0; i < num_batches; ++i) {
      std::shared_ptr<RecordBatch> chunk;
      RETURN_NOT_OK(reader->ReadRecordBatch(i, &chunk));
      out_batches->emplace_back(chunk);
    }

    return Status::OK();
  }

 protected:
  MemoryPool* pool_;

  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<ResizableBuffer> buffer_;
};

TEST_P(TestFileFormat, RoundTrip) {
  std::shared_ptr<RecordBatch> batch1;
  std::shared_ptr<RecordBatch> batch2;
  ASSERT_OK((*GetParam())(&batch1));  // NOLINT clang-tidy gtest issue
  ASSERT_OK((*GetParam())(&batch2));  // NOLINT clang-tidy gtest issue

  BatchVector in_batches = {batch1, batch2};
  BatchVector out_batches;

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
    ASSERT_OK(AllocateResizableBuffer(pool_, 0, &buffer_));
    sink_.reset(new io::BufferOutputStream(buffer_));
  }
  void TearDown() {}

  Status RoundTripHelper(const BatchVector& batches, BatchVector* out_batches) {
    // Write the file
    std::shared_ptr<RecordBatchWriter> writer;
    RETURN_NOT_OK(
        RecordBatchStreamWriter::Open(sink_.get(), batches[0]->schema(), &writer));

    for (const auto& batch : batches) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    RETURN_NOT_OK(writer->Close());
    RETURN_NOT_OK(sink_->Close());

    // Open the file
    io::BufferReader buf_reader(buffer_);

    std::shared_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(RecordBatchStreamReader::Open(&buf_reader, &reader));
    return reader->ReadAll(out_batches);
  }

 protected:
  MemoryPool* pool_;

  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<ResizableBuffer> buffer_;
};

TEST_P(TestStreamFormat, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  BatchVector out_batches;

  ASSERT_OK(RoundTripHelper({batch, batch, batch}, &out_batches));

  // Compare batches. Same
  for (size_t i = 0; i < out_batches.size(); ++i) {
    CompareBatch(*batch, *out_batches[i]);
  }
}

INSTANTIATE_TEST_CASE_P(GenericIpcRoundTripTests, TestIpcRoundTrip, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(FileRoundTripTests, TestFileFormat, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(StreamRoundTripTests, TestStreamFormat, BATCH_CASES());

// This test uses uninitialized memory

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))
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

  auto batch = RecordBatch::Make(schema, length, {array});

  std::string path = "test-write-large-record_batch";

  // 512 MB
  constexpr int64_t kBufferSize = 1 << 29;
  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::shared_ptr<RecordBatch> result;
  ASSERT_OK(DoLargeRoundTrip(*batch, false, &result));
  CheckReadResult(*result, *batch);

  ASSERT_EQ(length, result->num_rows());
}
#endif

void CheckBatchDictionaries(const RecordBatch& batch) {
  // Check that dictionaries that should be the same are the same
  auto schema = batch.schema();

  const auto& t0 = checked_cast<const DictionaryType&>(*schema->field(0)->type());
  const auto& t1 = checked_cast<const DictionaryType&>(*schema->field(1)->type());

  ASSERT_EQ(t0.dictionary().get(), t1.dictionary().get());

  // Same dictionary used for list values
  const auto& t3 = checked_cast<const ListType&>(*schema->field(3)->type());
  const auto& t3_value = checked_cast<const DictionaryType&>(*t3.value_type());
  ASSERT_EQ(t0.dictionary().get(), t3_value.dictionary().get());
}

TEST_F(TestStreamFormat, DictionaryRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionary(&batch));

  BatchVector out_batches;
  ASSERT_OK(RoundTripHelper({batch}, &out_batches));

  CheckBatchDictionaries(*out_batches[0]);
}

TEST_F(TestStreamFormat, WriteTable) {
  std::shared_ptr<RecordBatch> b1, b2, b3;
  ASSERT_OK(MakeIntRecordBatch(&b1));
  ASSERT_OK(MakeIntRecordBatch(&b2));
  ASSERT_OK(MakeIntRecordBatch(&b3));

  BatchVector out_batches;
  ASSERT_OK(RoundTripHelper({b1, b2, b3}, &out_batches));

  ASSERT_TRUE(b1->Equals(*out_batches[0]));
  ASSERT_TRUE(b2->Equals(*out_batches[1]));
  ASSERT_TRUE(b3->Equals(*out_batches[2]));
}

TEST_F(TestFileFormat, DictionaryRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionary(&batch));

  BatchVector out_batches;
  ASSERT_OK(RoundTripHelper({batch}, &out_batches));

  CheckBatchDictionaries(*out_batches[0]);
}

class TestTensorRoundTrip : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }

  void CheckTensorRoundTrip(const Tensor& tensor) {
    int32_t metadata_length;
    int64_t body_length;

    const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
    const int elem_size = type.bit_width() / 8;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(WriteTensor(tensor, mmap_.get(), &metadata_length, &body_length));

    const int64_t expected_body_length = elem_size * tensor.size();
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<Tensor> result;
    ASSERT_OK(ReadTensor(mmap_.get(), &result));

    ASSERT_EQ(result->data()->size(), expected_body_length);
    ASSERT_TRUE(tensor.Equals(*result));
  }
};

TEST_F(TestTensorRoundTrip, BasicRoundtrip) {
  std::string path = "test-write-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};
  int64_t size = 24;

  std::vector<int64_t> values;
  randint(size, 0, 100, &values);

  auto data = Buffer::Wrap(values);

  Tensor t0(int64(), data, shape, strides, dim_names);
  Tensor t_no_dims(int64(), data, {}, {}, {});
  Tensor t_zero_length_dim(int64(), data, {0}, {8}, {"foo"});

  CheckTensorRoundTrip(t0);
  CheckTensorRoundTrip(t_no_dims);
  CheckTensorRoundTrip(t_zero_length_dim);

  int64_t serialized_size;
  ASSERT_OK(GetTensorSize(t0, &serialized_size));
  ASSERT_TRUE(serialized_size > static_cast<int64_t>(size * sizeof(int64_t)));

  // ARROW-2840: Check that padding/alignment minded
  std::vector<int64_t> shape_2 = {1, 1};
  std::vector<int64_t> strides_2 = {8, 8};
  Tensor t0_not_multiple_64(int64(), data, shape_2, strides_2, dim_names);
  CheckTensorRoundTrip(t0_not_multiple_64);
}

TEST_F(TestTensorRoundTrip, NonContiguous) {
  std::string path = "test-write-tensor-strided";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::vector<int64_t> values;
  randint(24, 0, 100, &values);

  auto data = Buffer::Wrap(values);
  Tensor tensor(int64(), data, {4, 3}, {48, 16});

  CheckTensorRoundTrip(tensor);
}

class TestSparseTensorRoundTrip : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }

  template <typename SparseIndexType>
  void CheckSparseTensorRoundTrip(const SparseTensorImpl<SparseIndexType>& tensor) {
    GTEST_FAIL();
  }
};

template <>
void TestSparseTensorRoundTrip::CheckSparseTensorRoundTrip<SparseCOOIndex>(
    const SparseTensorImpl<SparseCOOIndex>& tensor) {
  const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
  const int elem_size = type.bit_width() / 8;

  int32_t metadata_length;
  int64_t body_length;

  ASSERT_OK(mmap_->Seek(0));

  ASSERT_OK(WriteSparseTensor(tensor, mmap_.get(), &metadata_length, &body_length,
                              default_memory_pool()));

  const auto& sparse_index = checked_cast<const SparseCOOIndex&>(*tensor.sparse_index());
  const int64_t indices_length = elem_size * sparse_index.indices()->size();
  const int64_t data_length = elem_size * tensor.non_zero_length();
  const int64_t expected_body_length = indices_length + data_length;
  ASSERT_EQ(expected_body_length, body_length);

  ASSERT_OK(mmap_->Seek(0));

  std::shared_ptr<SparseTensor> result;
  ASSERT_OK(ReadSparseTensor(mmap_.get(), &result));

  const auto& resulted_sparse_index =
      checked_cast<const SparseCOOIndex&>(*result->sparse_index());
  ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
  ASSERT_EQ(result->data()->size(), data_length);
  ASSERT_TRUE(result->Equals(*result));
}

template <>
void TestSparseTensorRoundTrip::CheckSparseTensorRoundTrip<SparseCSRIndex>(
    const SparseTensorImpl<SparseCSRIndex>& tensor) {
  const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
  const int elem_size = type.bit_width() / 8;

  int32_t metadata_length;
  int64_t body_length;

  ASSERT_OK(mmap_->Seek(0));

  ASSERT_OK(WriteSparseTensor(tensor, mmap_.get(), &metadata_length, &body_length,
                              default_memory_pool()));

  const auto& sparse_index = checked_cast<const SparseCSRIndex&>(*tensor.sparse_index());
  const int64_t indptr_length = elem_size * sparse_index.indptr()->size();
  const int64_t indices_length = elem_size * sparse_index.indices()->size();
  const int64_t data_length = elem_size * tensor.non_zero_length();
  const int64_t expected_body_length = indptr_length + indices_length + data_length;
  ASSERT_EQ(expected_body_length, body_length);

  ASSERT_OK(mmap_->Seek(0));

  std::shared_ptr<SparseTensor> result;
  ASSERT_OK(ReadSparseTensor(mmap_.get(), &result));

  const auto& resulted_sparse_index =
      checked_cast<const SparseCSRIndex&>(*result->sparse_index());
  ASSERT_EQ(resulted_sparse_index.indptr()->data()->size(), indptr_length);
  ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
  ASSERT_EQ(result->data()->size(), data_length);
  ASSERT_TRUE(result->Equals(*result));
}

TEST_F(TestSparseTensorRoundTrip, WithSparseCOOIndex) {
  std::string path = "test-write-sparse-coo-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::vector<int64_t> shape = {2, 3, 4};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  SparseTensorImpl<SparseCOOIndex> st(t);

  CheckSparseTensorRoundTrip(st);
}

TEST_F(TestSparseTensorRoundTrip, WithSparseCSRIndex) {
  std::string path = "test-write-sparse-csr-matrix";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK(io::MemoryMapFixture::InitMemoryMap(kBufferSize, path, &mmap_));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  SparseTensorImpl<SparseCSRIndex> st(t);

  CheckSparseTensorRoundTrip(st);
}

TEST(TestRecordBatchStreamReader, MalformedInput) {
  const std::string empty_str = "";
  const std::string garbage_str = "12345678";

  auto empty = std::make_shared<Buffer>(empty_str);
  auto garbage = std::make_shared<Buffer>(garbage_str);

  std::shared_ptr<RecordBatchReader> batch_reader;

  io::BufferReader empty_reader(empty);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&empty_reader, &batch_reader));

  io::BufferReader garbage_reader(garbage);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&garbage_reader, &batch_reader));
}

}  // namespace ipc
}  // namespace arrow

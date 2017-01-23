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
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/file.h"
#include "arrow/ipc/stream.h"
#include "arrow/ipc/test-common.h"
#include "arrow/ipc/util.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {

void CompareBatch(const RecordBatch& left, const RecordBatch& right) {
  ASSERT_TRUE(left.schema()->Equals(right.schema()));
  ASSERT_EQ(left.num_columns(), right.num_columns())
      << left.schema()->ToString() << " result: " << right.schema()->ToString();
  EXPECT_EQ(left.num_rows(), right.num_rows());
  for (int i = 0; i < left.num_columns(); ++i) {
    EXPECT_TRUE(left.column(i)->Equals(right.column(i)))
        << "Idx: " << i << " Name: " << left.column_name(i);
  }
}

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

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

#define BATCH_CASES()                                                                   \
  ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch, &MakeNonNullRecordBatch, \
      &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList, &MakeStringTypesRecordBatch,   \
      &MakeStruct);

INSTANTIATE_TEST_CASE_P(FileRoundTripTests, TestFileFormat, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(StreamRoundTripTests, TestStreamFormat, BATCH_CASES());

class TestFileFooter : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema& schema, const std::vector<FileBlock>& dictionaries,
      const std::vector<FileBlock>& record_batches) {
    auto buffer = std::make_shared<PoolBuffer>();
    io::BufferOutputStream stream(buffer);

    ASSERT_OK(WriteFileFooter(schema, dictionaries, record_batches, &stream));

    std::unique_ptr<FileFooter> footer;
    ASSERT_OK(FileFooter::Open(buffer, &footer));

    ASSERT_EQ(MetadataVersion::V2, footer->version());

    // Check schema
    std::shared_ptr<Schema> schema2;
    ASSERT_OK(footer->GetSchema(&schema2));
    AssertSchemaEqual(schema, *schema2);

    // Check blocks
    ASSERT_EQ(dictionaries.size(), footer->num_dictionaries());
    ASSERT_EQ(record_batches.size(), footer->num_record_batches());

    for (int i = 0; i < footer->num_dictionaries(); ++i) {
      CheckBlocks(dictionaries[i], footer->dictionary(i));
    }

    for (int i = 0; i < footer->num_record_batches(); ++i) {
      CheckBlocks(record_batches[i], footer->record_batch(i));
    }
  }

  void CheckBlocks(const FileBlock& left, const FileBlock& right) {
    ASSERT_EQ(left.offset, right.offset);
    ASSERT_EQ(left.metadata_length, right.metadata_length);
    ASSERT_EQ(left.body_length, right.body_length);
  }

 private:
  std::shared_ptr<Schema> example_schema_;
};

TEST_F(TestFileFooter, Basics) {
  auto f0 = std::make_shared<Field>("f0", std::make_shared<Int8Type>());
  auto f1 = std::make_shared<Field>("f1", std::make_shared<Int16Type>());
  Schema schema({f0, f1});

  std::vector<FileBlock> dictionaries;
  dictionaries.emplace_back(8, 92, 900);
  dictionaries.emplace_back(1000, 100, 1900);
  dictionaries.emplace_back(3000, 100, 2900);

  std::vector<FileBlock> record_batches;
  record_batches.emplace_back(6000, 100, 900);
  record_batches.emplace_back(7000, 100, 1900);
  record_batches.emplace_back(9000, 100, 2900);
  record_batches.emplace_back(12000, 100, 3900);

  CheckRoundtrip(schema, dictionaries, record_batches);
}

}  // namespace ipc
}  // namespace arrow

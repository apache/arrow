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

  Status InitMemoryMap(int64_t size) {
    std::string path = "test-write-row-batch";
    MemoryMapFixture::CreateFile(path, size);
    return MemoryMappedSource::Open(path, MemorySource::READ_WRITE, &mmap_);
  }

  Status RoundTripHelper(const RowBatch& batch, int memory_map_size,
      std::shared_ptr<RowBatch>* batch_result) {
    InitMemoryMap(memory_map_size);
    int64_t header_location;
    RETURN_NOT_OK(WriteRowBatch(mmap_.get(), &batch, 0, &header_location));

    std::shared_ptr<RowBatchReader> reader;
    RETURN_NOT_OK(RowBatchReader::Open(mmap_.get(), header_location, &reader));

    // TODO(emkornfield): why does this require a smart pointer for schema?
    RETURN_NOT_OK(reader->GetRowBatch(batch.schema(), batch_result));
    return Status::OK();
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<MemoryMappedSource> mmap_;
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
        << i << batch->column_name(i);
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
  RETURN_NOT_OK(MakeRandomInt32Array(1000, true, pool, &leaf_values));
  RETURN_NOT_OK(MakeRandomListArray(leaf_values, length, pool, &list_array));
  RETURN_NOT_OK(MakeRandomListArray(list_array, length, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(length, true, pool, &flat_array));
  out->reset(new RowBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

INSTANTIATE_TEST_CASE_P(RoundTripTests, TestWriteRowBatch,
    ::testing::Values(&MakeIntRowBatch, &MakeListRowBatch));

// TODO(emkornfield) More tests
// Test primitive and lists with zero elements
// Tests lists and primitives with no nulls
// String  type
}  // namespace ipc
}  // namespace arrow

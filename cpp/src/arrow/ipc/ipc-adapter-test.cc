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
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/ipc/adapter.h"
#include "arrow/ipc/memory.h"
#include "arrow/ipc/test-common.h"

#include "arrow/test-util.h"
#include "arrow/types/primitive.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

class TestWriteRowBatch : public ::testing::Test, public MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { MemoryMapFixture::TearDown(); }

  void InitMemoryMap(int64_t size) {
    std::string path = "test-write-row-batch";
    MemoryMapFixture::CreateFile(path, size);
    ASSERT_OK(MemoryMappedSource::Open(path, MemorySource::READ_WRITE, &mmap_));
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<MemoryMappedSource> mmap_;
};

const auto INT32 = std::make_shared<Int32Type>();

TEST_F(TestWriteRowBatch, IntegerRoundTrip) {
  const int length = 1000;

  // Make the schema
  auto f0 = std::make_shared<Field>("f0", INT32);
  auto f1 = std::make_shared<Field>("f1", INT32);
  std::shared_ptr<Schema> schema(new Schema({f0, f1}));

  // Example data

  auto data = std::make_shared<PoolBuffer>(pool_);
  ASSERT_OK(data->Resize(length * sizeof(int32_t)));
  test::rand_uniform_int(length, 0, 0, std::numeric_limits<int32_t>::max(),
      reinterpret_cast<int32_t*>(data->mutable_data()));

  auto null_bitmap = std::make_shared<PoolBuffer>(pool_);
  int null_bytes = util::bytes_for_bits(length);
  ASSERT_OK(null_bitmap->Resize(null_bytes));
  test::random_bytes(null_bytes, 0, null_bitmap->mutable_data());

  auto a0 = std::make_shared<Int32Array>(length, data);
  auto a1 = std::make_shared<Int32Array>(
      length, data, test::bitmap_popcount(null_bitmap->data(), length), null_bitmap);

  RowBatch batch(schema, length, {a0, a1});

  // TODO(wesm): computing memory requirements for a row batch
  // 64k is plenty of space
  InitMemoryMap(1 << 16);

  int64_t header_location;
  ASSERT_OK(WriteRowBatch(mmap_.get(), &batch, 0, &header_location));

  std::shared_ptr<RowBatchReader> result;
  ASSERT_OK(RowBatchReader::Open(mmap_.get(), header_location, &result));

  std::shared_ptr<RowBatch> batch_result;
  ASSERT_OK(result->GetRowBatch(schema, &batch_result));
  EXPECT_EQ(batch.num_rows(), batch_result->num_rows());

  for (int i = 0; i < batch.num_columns(); ++i) {
    EXPECT_TRUE(batch.column(i)->Equals(batch_result->column(i))) << i
                                                                  << batch.column_name(i);
  }
}

}  // namespace ipc
}  // namespace arrow

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

#ifndef ARROW_TEST_COMMON_H
#define ARROW_TEST_COMMON_H

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/test-util.h"

namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    random_seed_ = 0;
  }

  template <typename ArrayType>
  std::shared_ptr<Array> MakePrimitive(int64_t length, int64_t null_count = 0) {
    auto data = std::make_shared<PoolBuffer>(pool_);
    const int64_t data_nbytes = length * sizeof(typename ArrayType::value_type);
    EXPECT_OK(data->Resize(data_nbytes));

    // Fill with random data
    test::random_bytes(data_nbytes, random_seed_++, data->mutable_data());

    auto null_bitmap = std::make_shared<PoolBuffer>(pool_);
    EXPECT_OK(null_bitmap->Resize(BitUtil::BytesForBits(length)));
    return std::make_shared<ArrayType>(length, data, null_bitmap, null_count);
  }

 protected:
  uint32_t random_seed_;
  MemoryPool* pool_;
};

class TestBuilder : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    type_ = TypePtr(new UInt8Type());
    builder_.reset(new UInt8Builder(pool_));
    builder_nn_.reset(new UInt8Builder(pool_));
  }

 protected:
  MemoryPool* pool_;

  TypePtr type_;
  std::unique_ptr<ArrayBuilder> builder_;
  std::unique_ptr<ArrayBuilder> builder_nn_;
};

}  // namespace arrow

#endif  // ARROW_TEST_COMMON_H_

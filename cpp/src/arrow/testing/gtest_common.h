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

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util_base.h"

namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    random_seed_ = 0;
  }

  std::shared_ptr<Buffer> MakeRandomNullBitmap(int64_t length, int64_t null_count) {
    const int64_t null_nbytes = BitUtil::BytesForBits(length);

    std::shared_ptr<Buffer> null_bitmap;
    ARROW_EXPECT_OK(AllocateBuffer(pool_, null_nbytes, &null_bitmap));
    memset(null_bitmap->mutable_data(), 255, null_nbytes);
    for (int64_t i = 0; i < null_count; i++) {
      BitUtil::ClearBit(null_bitmap->mutable_data(), i * (length / null_count));
    }
    return null_bitmap;
  }

 protected:
  uint32_t random_seed_;
  MemoryPool* pool_;
};

class TestBuilder : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
};

}  // namespace arrow

#endif  // ARROW_TEST_COMMON_H_

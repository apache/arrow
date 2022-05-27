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

#include "jni/dataset/jni_util.h"

#include <gtest/gtest.h>

#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace dataset {
namespace jni {

class MyListener : public ReservationListener {
 public:
  Status OnReservation(int64_t size) override {
    bytes_reserved_ += size;
    reservation_count_++;
    return arrow::Status::OK();
  }

  Status OnRelease(int64_t size) override {
    bytes_reserved_ -= size;
    release_count_++;
    return arrow::Status::OK();
  }

  int64_t bytes_reserved() { return bytes_reserved_; }

  int32_t reservation_count() const { return reservation_count_; }

  int32_t release_count() const { return release_count_; }

 private:
  int64_t bytes_reserved_;
  int32_t reservation_count_;
  int32_t release_count_;
};

TEST(ReservationListenableMemoryPool, Basic) {
  auto pool = MemoryPool::CreateDefault();
  auto listener = std::make_shared<MyListener>();
  ReservationListenableMemoryPool rlp(pool.get(), listener);

  uint8_t* data;
  ASSERT_OK(rlp.Allocate(100, &data));

  uint8_t* data2;
  ASSERT_OK(rlp.Allocate(100, &data2));

  rlp.Free(data, 100);
  rlp.Free(data2, 100);

  ASSERT_EQ(200, rlp.max_memory());
  ASSERT_EQ(200, pool->max_memory());
}

TEST(ReservationListenableMemoryPool, Listener) {
  auto pool = MemoryPool::CreateDefault();
  auto listener = std::make_shared<MyListener>();
  ReservationListenableMemoryPool rlp(pool.get(), listener);

  uint8_t* data;
  ASSERT_OK(rlp.Allocate(100, &data));

  uint8_t* data2;
  ASSERT_OK(rlp.Allocate(100, &data2));

  ASSERT_EQ(200, rlp.bytes_allocated());
  ASSERT_EQ(512 * 1024, listener->bytes_reserved());

  rlp.Free(data, 100);
  rlp.Free(data2, 100);

  ASSERT_EQ(0, rlp.bytes_allocated());
  ASSERT_EQ(0, listener->bytes_reserved());
  ASSERT_EQ(1, listener->reservation_count());
  ASSERT_EQ(1, listener->release_count());
}

TEST(ReservationListenableMemoryPool, BlockSize) {
  auto pool = MemoryPool::CreateDefault();
  auto listener = std::make_shared<MyListener>();
  ReservationListenableMemoryPool rlp(pool.get(), listener, 100);

  uint8_t* data;
  ASSERT_OK(rlp.Allocate(100, &data));

  ASSERT_EQ(100, rlp.bytes_allocated());
  ASSERT_EQ(100, listener->bytes_reserved());

  rlp.Free(data, 100);

  ASSERT_EQ(0, rlp.bytes_allocated());
  ASSERT_EQ(0, listener->bytes_reserved());
}

TEST(ReservationListenableMemoryPool, BlockSize2) {
  auto pool = MemoryPool::CreateDefault();
  auto listener = std::make_shared<MyListener>();
  ReservationListenableMemoryPool rlp(pool.get(), listener, 99);

  uint8_t* data;
  ASSERT_OK(rlp.Allocate(100, &data));

  ASSERT_EQ(100, rlp.bytes_allocated());
  ASSERT_EQ(198, listener->bytes_reserved());

  rlp.Free(data, 100);

  ASSERT_EQ(0, rlp.bytes_allocated());
  ASSERT_EQ(0, listener->bytes_reserved());

  ASSERT_EQ(1, listener->reservation_count());
  ASSERT_EQ(1, listener->release_count());
}

}  // namespace jni
}  // namespace dataset
}  // namespace arrow

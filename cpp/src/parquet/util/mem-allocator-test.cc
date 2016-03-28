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

#include <gtest/gtest.h>

#include "parquet/exception.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

TEST(TestAllocator, AllocateFree) {
  TrackingAllocator allocator;

  uint8_t* data = allocator.Malloc(100);
  ASSERT_TRUE(nullptr != data);
  data[99] = 55;
  allocator.Free(data, 100);

  data = allocator.Malloc(0);
  ASSERT_EQ(nullptr, data);
  allocator.Free(data, 0);

  data = allocator.Malloc(1);
  ASSERT_THROW(allocator.Free(data, 2), ParquetException);
  ASSERT_NO_THROW(allocator.Free(data, 1));

  int64_t to_alloc = std::numeric_limits<int64_t>::max();
  ASSERT_THROW(allocator.Malloc(to_alloc), ParquetException);
}

TEST(TestAllocator, TotalMax) {
  TrackingAllocator allocator;
  ASSERT_EQ(0, allocator.TotalMemory());
  ASSERT_EQ(0, allocator.MaxMemory());

  uint8_t* data = allocator.Malloc(100);
  ASSERT_EQ(100, allocator.TotalMemory());
  ASSERT_EQ(100, allocator.MaxMemory());

  uint8_t* data2 = allocator.Malloc(10);
  ASSERT_EQ(110, allocator.TotalMemory());
  ASSERT_EQ(110, allocator.MaxMemory());

  allocator.Free(data, 100);
  ASSERT_EQ(10, allocator.TotalMemory());
  ASSERT_EQ(110, allocator.MaxMemory());

  allocator.Free(data2, 10);
  ASSERT_EQ(0, allocator.TotalMemory());
  ASSERT_EQ(110, allocator.MaxMemory());
}

} // namespace parquet

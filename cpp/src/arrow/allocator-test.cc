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
#include <new>

#include <gtest/gtest.h>

#include "arrow/allocator.h"
#include "arrow/memory_pool.h"

namespace arrow {

TEST(stl_allocator, MemoryTracking) {
  auto pool = default_memory_pool();
  stl_allocator<uint64_t> alloc;
  uint64_t* data = alloc.allocate(100);

  ASSERT_EQ(100 * sizeof(uint64_t), pool->bytes_allocated());

  alloc.deallocate(data, 100);
  ASSERT_EQ(0, pool->bytes_allocated());
}

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || defined(ARROW_JEMALLOC))

TEST(stl_allocator, TestOOM) {
  stl_allocator<uint64_t> alloc;
  uint64_t to_alloc = std::numeric_limits<uint64_t>::max();
  ASSERT_THROW(alloc.allocate(to_alloc), std::bad_alloc);
}

TEST(stl_allocator, FreeLargeMemory) {
  stl_allocator<uint8_t> alloc;

  uint8_t* data = alloc.allocate(100);

#ifndef NDEBUG
  EXPECT_EXIT(alloc.deallocate(data, 120), ::testing::ExitedWithCode(1),
              ".*Check failed: \\(bytes_allocated_\\) >= \\(size\\)");
#endif

  alloc.deallocate(data, 100);
}

TEST(stl_allocator, MaxMemory) {
  DefaultMemoryPool pool;

  ASSERT_EQ(0, pool.max_memory());
  stl_allocator<uint8_t> alloc(&pool);
  uint8_t* data = alloc.allocate(100);
  uint8_t* data2 = alloc.allocate(100);

  alloc.deallocate(data, 100);
  alloc.deallocate(data2, 100);

  ASSERT_EQ(200, pool.max_memory());
}

#endif  // ARROW_VALGRIND

}  // namespace arrow

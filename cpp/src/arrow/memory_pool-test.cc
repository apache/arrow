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

#include "gtest/gtest.h"

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-util.h"

namespace arrow {

TEST(DefaultMemoryPool, MemoryTracking) {
  MemoryPool* pool = default_memory_pool();

  uint8_t* data;
  ASSERT_OK(pool->Allocate(100, &data));
  EXPECT_EQ(static_cast<uint64_t>(0), reinterpret_cast<uint64_t>(data) % 64);
  ASSERT_EQ(100, pool->bytes_allocated());

  pool->Free(data, 100);
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(DefaultMemoryPool, OOM) {
  MemoryPool* pool = default_memory_pool();

  uint8_t* data;
  int64_t to_alloc = std::numeric_limits<int64_t>::max();
  ASSERT_RAISES(OutOfMemory, pool->Allocate(to_alloc, &data));
}

// Death tests and valgrind are known to not play well 100% of the time. See
// googletest documentation
#ifndef ARROW_VALGRIND

TEST(DefaultMemoryPoolDeathTest, FreeLargeMemory) {
  MemoryPool* pool = default_memory_pool();

  uint8_t* data;
  ASSERT_OK(pool->Allocate(100, &data));

#ifndef NDEBUG
  EXPECT_EXIT(pool->Free(data, 120), ::testing::ExitedWithCode(1),
      ".*Check failed: \\(bytes_allocated_\\) >= \\(size\\)");
#endif

  pool->Free(data, 100);
}

#endif  // ARROW_VALGRIND

}  // namespace arrow

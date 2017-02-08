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

#include "arrow/memory_pool-test.h"

#include <cstdint>
#include <limits>

namespace arrow {

class TestDefaultMemoryPool : public ::arrow::test::TestMemoryPoolBase {
 public:
  ::arrow::MemoryPool* memory_pool() override { return ::arrow::default_memory_pool(); }
};

TEST_F(TestDefaultMemoryPool, MemoryTracking) {
  this->TestMemoryTracking();
}

TEST_F(TestDefaultMemoryPool, OOM) {
#ifndef ADDRESS_SANITIZER
  this->TestOOM();
#endif
}

TEST_F(TestDefaultMemoryPool, Reallocate) {
  this->TestReallocate();
}

// Death tests and valgrind are known to not play well 100% of the time. See
// googletest documentation
#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))

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

TEST(DefaultMemoryPoolDeathTest, MaxMemory) {
  DefaultMemoryPool pool;

  ASSERT_EQ(0, pool.max_memory());

  uint8_t* data;
  ASSERT_OK(pool.Allocate(100, &data));

  uint8_t* data2;
  ASSERT_OK(pool.Allocate(100, &data2));

  pool.Free(data, 100);
  pool.Free(data2, 100);

  ASSERT_EQ(200, pool.max_memory());
}

#endif  // ARROW_VALGRIND

}  // namespace arrow

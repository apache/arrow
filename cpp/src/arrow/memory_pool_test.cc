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

#include <algorithm>
#include <cstdint>

#include <gtest/gtest.h>

#include "arrow/memory_pool.h"
#include "arrow/memory_pool_test.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/config.h"

namespace arrow {

struct DefaultMemoryPoolFactory {
  static MemoryPool* memory_pool() { return default_memory_pool(); }
};

struct SystemMemoryPoolFactory {
  static MemoryPool* memory_pool() { return system_memory_pool(); }
};

#ifdef ARROW_JEMALLOC
struct JemallocMemoryPoolFactory {
  static MemoryPool* memory_pool() {
    MemoryPool* pool;
    ABORT_NOT_OK(jemalloc_memory_pool(&pool));
    return pool;
  }
};
#endif

#ifdef ARROW_MIMALLOC
struct MimallocMemoryPoolFactory {
  static MemoryPool* memory_pool() {
    MemoryPool* pool;
    ABORT_NOT_OK(mimalloc_memory_pool(&pool));
    return pool;
  }
};
#endif

template <typename Factory>
class TestMemoryPool : public ::arrow::TestMemoryPoolBase {
 public:
  MemoryPool* memory_pool() override { return Factory::memory_pool(); }
};

TYPED_TEST_SUITE_P(TestMemoryPool);

TYPED_TEST_P(TestMemoryPool, MemoryTracking) { this->TestMemoryTracking(); }

TYPED_TEST_P(TestMemoryPool, OOM) {
#ifndef ADDRESS_SANITIZER
  this->TestOOM();
#endif
}

TYPED_TEST_P(TestMemoryPool, Reallocate) { this->TestReallocate(); }

REGISTER_TYPED_TEST_SUITE_P(TestMemoryPool, MemoryTracking, OOM, Reallocate);

INSTANTIATE_TYPED_TEST_SUITE_P(Default, TestMemoryPool, DefaultMemoryPoolFactory);
INSTANTIATE_TYPED_TEST_SUITE_P(System, TestMemoryPool, SystemMemoryPoolFactory);

#ifdef ARROW_JEMALLOC
INSTANTIATE_TYPED_TEST_SUITE_P(Jemalloc, TestMemoryPool, JemallocMemoryPoolFactory);
#endif

#ifdef ARROW_MIMALLOC
INSTANTIATE_TYPED_TEST_SUITE_P(Mimalloc, TestMemoryPool, MimallocMemoryPoolFactory);
#endif

TEST(DefaultMemoryPool, Identity) {
  // The default memory pool is pointer-identical to one of the backend-specific pools.
  MemoryPool* pool = default_memory_pool();
  std::vector<MemoryPool*> specific_pools = {system_memory_pool()};
#ifdef ARROW_JEMALLOC
  specific_pools.push_back(nullptr);
  ASSERT_OK(jemalloc_memory_pool(&specific_pools.back()));
#endif
#ifdef ARROW_MIMALLOC
  specific_pools.push_back(nullptr);
  ASSERT_OK(mimalloc_memory_pool(&specific_pools.back()));
#endif
  ASSERT_NE(std::find(specific_pools.begin(), specific_pools.end(), pool),
            specific_pools.end());
}

// Death tests and valgrind are known to not play well 100% of the time. See
// googletest documentation
#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))

TEST(DefaultMemoryPoolDeathTest, MaxMemory) {
  MemoryPool* pool = default_memory_pool();
  uint8_t* data1;
  uint8_t* data2;

  ASSERT_OK(pool->Allocate(100, &data1));
  ASSERT_OK(pool->Allocate(50, &data2));
  pool->Free(data2, 50);
  ASSERT_OK(pool->Allocate(100, &data2));
  pool->Free(data1, 100);
  pool->Free(data2, 100);

  ASSERT_EQ(200, pool->max_memory());
}

#endif  // ARROW_VALGRIND

TEST(LoggingMemoryPool, Logging) {
  auto pool = MemoryPool::CreateDefault();

  LoggingMemoryPool lp(pool.get());

  uint8_t* data;
  ASSERT_OK(lp.Allocate(100, &data));

  uint8_t* data2;
  ASSERT_OK(lp.Allocate(100, &data2));

  lp.Free(data, 100);
  lp.Free(data2, 100);

  ASSERT_EQ(200, lp.max_memory());
  ASSERT_EQ(200, pool->max_memory());
}

TEST(ProxyMemoryPool, Logging) {
  auto pool = MemoryPool::CreateDefault();

  ProxyMemoryPool pp(pool.get());

  uint8_t* data;
  ASSERT_OK(pool->Allocate(100, &data));

  uint8_t* data2;
  ASSERT_OK(pp.Allocate(300, &data2));

  ASSERT_EQ(400, pool->bytes_allocated());
  ASSERT_EQ(300, pp.bytes_allocated());

  pool->Free(data, 100);
  pp.Free(data2, 300);

  ASSERT_EQ(0, pool->bytes_allocated());
  ASSERT_EQ(0, pp.bytes_allocated());
}

TEST(Jemalloc, SetDirtyPageDecayMillis) {
  // ARROW-6910
#ifdef ARROW_JEMALLOC
  ASSERT_OK(jemalloc_set_decay_ms(0));
#else
  ASSERT_RAISES(Invalid, jemalloc_set_decay_ms(0));
#endif
}

TEST(Jemalloc, GetAllocationStats) {
  size_t allocated, active, metadata, resident, mapped = 0;
#ifdef ARROW_JEMALLOC
  auto pool = MemoryPool::CreateDefault();
  uint8_t* data;

  ASSERT_OK(pool->Allocate(42, &data));
  ASSERT_OK(jemalloc_get_stat("stats.allocated", &allocated));
  ASSERT_OK(jemalloc_get_stat("stats.active", &active));
  ASSERT_OK(jemalloc_get_stat("stats.metadata", &metadata));
  ASSERT_OK(jemalloc_get_stat("stats.resident", &resident));
  ASSERT_OK(jemalloc_get_stat("stats.mapped", &mapped));

  // TODO
  ASSERT_EQ(71424, allocated);
  ASSERT_EQ(131072, active);
  ASSERT_EQ(2814368, metadata);
  ASSERT_EQ(2899968, resident);
  ASSERT_EQ(6422528, mapped);

#else
  ASSERT_RAISES(IOError, jemalloc_get_stat("stats.allocated", allocated));
  ASSERT_RAISES(IOError, jemalloc_get_stat("stats.active", active));
  ASSERT_RAISES(IOError, jemalloc_get_stat("stats.metadata", metadata));
  ASSERT_RAISES(IOError, jemalloc_get_stat("stats.resident", resident));
  ASSERT_RAISES(IOError, jemalloc_get_stat("stats.mapped", mapped));
#endif
}

}  // namespace arrow

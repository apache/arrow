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
#include "arrow/util/logging.h"

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
#ifdef ARROW_JEMALLOC
  uint8_t* data;
  size_t allocated, active, metadata, resident, mapped, retained, allocated0, active0,
      metadata0, resident0, mapped0, retained0;
  uint64_t thread_allocatedp, thread_deallocatedp, thread_allocatedp0,
      thread_deallocatedp0, thread_allocated, thread_deallocated, thread_peak_read,
      thread_allocated0, thread_deallocated0, thread_peak_read0;
  auto pool = default_memory_pool();
  ABORT_NOT_OK(jemalloc_memory_pool(&pool));
  ASSERT_EQ("jemalloc", pool->backend_name());

  // Record stats before allocating
  ASSERT_OK(jemalloc_get_stat("stats.allocated", allocated0));
  ASSERT_OK(jemalloc_get_stat("stats.active", active0));
  ASSERT_OK(jemalloc_get_stat("stats.metadata", metadata0));
  ASSERT_OK(jemalloc_get_stat("stats.resident", resident0));
  ASSERT_OK(jemalloc_get_stat("stats.mapped", mapped0));
  ASSERT_OK(jemalloc_get_stat("stats.retained", retained0));
  ASSERT_OK(jemalloc_get_stat("thread.allocated", thread_allocated0));
  ASSERT_OK(jemalloc_get_stat("thread.deallocated", thread_deallocated0));
  ASSERT_OK(jemalloc_get_stat("thread.peak.read", thread_peak_read0));
  ASSERT_OK(jemalloc_get_statp("thread.allocatedp", thread_allocatedp0));
  ASSERT_OK(jemalloc_get_statp("thread.deallocatedp", thread_deallocatedp0));

  // Allocate memory
  ASSERT_OK(jemalloc_set_decay_ms(10000));
  ASSERT_OK(pool->Allocate(1256, &data));
  ASSERT_EQ(1256, pool->bytes_allocated());
  ASSERT_OK(pool->Reallocate(1256, 1214, &data));
  ASSERT_EQ(1214, pool->bytes_allocated());

  // Record stats after allocating
  ASSERT_OK(jemalloc_get_stat("stats.allocated", allocated));
  ASSERT_OK(jemalloc_get_stat("stats.active", active));
  ASSERT_OK(jemalloc_get_stat("stats.metadata", metadata));
  ASSERT_OK(jemalloc_get_stat("stats.resident", resident));
  ASSERT_OK(jemalloc_get_stat("stats.mapped", mapped));
  ASSERT_OK(jemalloc_get_stat("stats.retained", retained));
  ASSERT_OK(jemalloc_get_stat("thread.allocated", thread_allocated));
  ASSERT_OK(jemalloc_get_stat("thread.deallocated", thread_deallocated));
  ASSERT_OK(jemalloc_get_stat("thread.peak.read", thread_peak_read));
  ASSERT_OK(jemalloc_get_statp("thread.allocatedp", thread_allocatedp));
  ASSERT_OK(jemalloc_get_statp("thread.deallocatedp", thread_deallocatedp));

  // Reading stats via value return is equivalent to pointer passing
  ASSERT_EQ(thread_allocated, thread_allocatedp);
  ASSERT_EQ(thread_deallocated, thread_deallocatedp);
  ASSERT_EQ(thread_allocated0, thread_allocatedp0);
  ASSERT_EQ(thread_deallocated0, thread_deallocatedp0);

  // Check allocated stats pre-allocation
  ASSERT_EQ(71424, allocated0);
  ASSERT_EQ(131072, active0);
  ASSERT_EQ(2814368, metadata0);
  ASSERT_EQ(2899968, resident0);
  ASSERT_EQ(6422528, mapped0);
  ASSERT_EQ(0, retained0);

  // Check allocated stats change due to allocation
  ASSERT_EQ(81920, allocated - allocated0);
  ASSERT_EQ(81920, active - active0);
  ASSERT_EQ(384, metadata - metadata0);
  ASSERT_EQ(98304, resident - resident0);
  ASSERT_EQ(81920, mapped - mapped0);
  ASSERT_EQ(0, retained - retained0);

  ASSERT_EQ(1280, thread_peak_read - thread_peak_read0);
  ASSERT_EQ(2560, thread_allocated - thread_allocated0);
  ASSERT_EQ(1280, thread_deallocated - thread_deallocated0);

  // Resetting thread peak read metric
  ASSERT_OK(pool->Allocate(12560, &data));
  ASSERT_OK(jemalloc_get_stat("thread.peak.read", thread_peak_read));
  ASSERT_EQ(15616, thread_peak_read);
  ASSERT_OK(jemalloc_peak_reset());
  ASSERT_OK(pool->Allocate(1256, &data));
  ASSERT_OK(jemalloc_get_stat("thread.peak.read", thread_peak_read));
  ASSERT_EQ(1280, thread_peak_read);

  // Print statistics to stdout
  ASSERT_OK(jemalloc_stats_print("ax"));
#else
  size_t allocated;
  uint64_t thread_peak_read, stats_allocated, stats_allocatedp;
  ASSERT_RAISES(Invalid, jemalloc_get_stat("thread.peak.read", &thread_peak_read));
  ASSERT_RAISES(Invalid, jemalloc_get_stat("stats.allocated", &allocated));
  ASSERT_RAISES(Invalid, jemalloc_get_stat("stats.allocated", &stats_allocated));
  ASSERT_RAISES(Invalid, jemalloc_get_statp("stats.allocatedp", &stats_allocatedp));
  ASSERT_RAISES(Invalid, jemalloc_peak_reset());
  ASSERT_RAISES(Invalid, jemalloc_stats_print("ax"));
#endif
}

}  // namespace arrow

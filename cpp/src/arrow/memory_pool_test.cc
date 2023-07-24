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

TYPED_TEST_P(TestMemoryPool, Alignment) { this->TestAlignment(); }

REGISTER_TYPED_TEST_SUITE_P(TestMemoryPool, MemoryTracking, OOM, Reallocate, Alignment);

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

// TODO: is this still a death test?
TEST(DefaultMemoryPoolDeathTest, Statistics) {
  MemoryPool* pool = default_memory_pool();
  uint8_t* data1;
  uint8_t* data2;

  ASSERT_OK(pool->Allocate(100, &data1));
  ASSERT_OK(pool->Allocate(50, &data2));
  pool->Free(data2, 50);

  ASSERT_EQ(150, pool->max_memory());
  ASSERT_EQ(150, pool->total_bytes_allocated());
  ASSERT_EQ(100, pool->bytes_allocated());
  ASSERT_EQ(2, pool->num_allocations());

  ASSERT_OK(pool->Reallocate(100, 150, &data1));  // Grow data1

  ASSERT_EQ(150, pool->max_memory());
  ASSERT_EQ(150 + 50, pool->total_bytes_allocated());
  ASSERT_EQ(150, pool->bytes_allocated());
  ASSERT_EQ(3, pool->num_allocations());

  ASSERT_OK(pool->Reallocate(150, 50, &data1));  // Shrink data1

  ASSERT_EQ(150, pool->max_memory());
  ASSERT_EQ(200, pool->total_bytes_allocated());
  ASSERT_EQ(50, pool->bytes_allocated());
  ASSERT_EQ(4, pool->num_allocations());

  pool->Free(data1, 50);

  ASSERT_EQ(150, pool->max_memory());
  ASSERT_EQ(200, pool->total_bytes_allocated());
  ASSERT_EQ(0, pool->bytes_allocated());
  ASSERT_EQ(4, pool->num_allocations());
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
  ASSERT_RAISES(NotImplemented, jemalloc_set_decay_ms(0));
#endif
}

TEST(Jemalloc, GetAllocationStats) {
#ifdef ARROW_JEMALLOC
  uint8_t* data;
  int64_t allocated, active, metadata, resident, mapped, retained, allocated0, active0,
      metadata0, resident0, mapped0, retained0;
  int64_t thread_allocated, thread_deallocated, thread_peak_read, thread_allocated0,
      thread_deallocated0, thread_peak_read0;

  MemoryPool* pool = nullptr;
  ABORT_NOT_OK(jemalloc_memory_pool(&pool));
  ASSERT_EQ("jemalloc", pool->backend_name());

  // Record stats before allocating
  ASSERT_OK_AND_ASSIGN(allocated0, jemalloc_get_stat("stats.allocated"));
  ASSERT_OK_AND_ASSIGN(active0, jemalloc_get_stat("stats.active"));
  ASSERT_OK_AND_ASSIGN(metadata0, jemalloc_get_stat("stats.metadata"));
  ASSERT_OK_AND_ASSIGN(resident0, jemalloc_get_stat("stats.resident"));
  ASSERT_OK_AND_ASSIGN(mapped0, jemalloc_get_stat("stats.mapped"));
  ASSERT_OK_AND_ASSIGN(retained0, jemalloc_get_stat("stats.retained"));
  ASSERT_OK_AND_ASSIGN(thread_allocated0, jemalloc_get_stat("thread.allocated"));
  ASSERT_OK_AND_ASSIGN(thread_deallocated0, jemalloc_get_stat("thread.deallocated"));
  ASSERT_OK_AND_ASSIGN(thread_peak_read0, jemalloc_get_stat("thread.peak.read"));

  // Allocate memory
  ASSERT_OK(pool->Allocate(1025, &data));
  ASSERT_EQ(pool->bytes_allocated(), 1025);
  ASSERT_OK(pool->Reallocate(1025, 1023, &data));
  ASSERT_EQ(pool->bytes_allocated(), 1023);

  // Record stats after allocating
  ASSERT_OK_AND_ASSIGN(allocated, jemalloc_get_stat("stats.allocated"));
  ASSERT_OK_AND_ASSIGN(active, jemalloc_get_stat("stats.active"));
  ASSERT_OK_AND_ASSIGN(metadata, jemalloc_get_stat("stats.metadata"));
  ASSERT_OK_AND_ASSIGN(resident, jemalloc_get_stat("stats.resident"));
  ASSERT_OK_AND_ASSIGN(mapped, jemalloc_get_stat("stats.mapped"));
  ASSERT_OK_AND_ASSIGN(retained, jemalloc_get_stat("stats.retained"));
  ASSERT_OK_AND_ASSIGN(thread_allocated, jemalloc_get_stat("thread.allocated"));
  ASSERT_OK_AND_ASSIGN(thread_deallocated, jemalloc_get_stat("thread.deallocated"));
  ASSERT_OK_AND_ASSIGN(thread_peak_read, jemalloc_get_stat("thread.peak.read"));
  pool->Free(data, 1023);

  // Check allocated stats pre-allocation
  ASSERT_GT(allocated0, 0);
  ASSERT_GT(active0, 0);
  ASSERT_GT(metadata0, 0);
  ASSERT_GT(resident0, 0);
  ASSERT_GT(mapped0, 0);
  ASSERT_GE(retained0, 0);

  // Check allocated stats change due to allocation
  ASSERT_NEAR(allocated - allocated0, 70000, 50000);
  ASSERT_NEAR(active - active0, 100000, 90000);
  ASSERT_NEAR(metadata - metadata0, 500, 460);
  ASSERT_NEAR(resident - resident0, 120000, 110000);
  ASSERT_NEAR(mapped - mapped0, 100000, 90000);
  ASSERT_NEAR(retained - retained0, 0, 40000);

  ASSERT_NEAR(thread_peak_read - thread_peak_read0, 1024, 700);
  ASSERT_NEAR(thread_allocated - thread_allocated0, 2500, 500);
  ASSERT_EQ(thread_deallocated - thread_deallocated0, 1280);

  // Resetting thread peak read metric
  ASSERT_OK(pool->Allocate(100000, &data));
  ASSERT_OK_AND_ASSIGN(thread_peak_read, jemalloc_get_stat("thread.peak.read"));
  ASSERT_NEAR(thread_peak_read, 100000, 50000);
  pool->Free(data, 100000);
  ASSERT_OK(jemalloc_peak_reset());

  ASSERT_OK(pool->Allocate(1256, &data));
  ASSERT_OK_AND_ASSIGN(thread_peak_read, jemalloc_get_stat("thread.peak.read"));
  ASSERT_NEAR(thread_peak_read, 1256, 100);
  pool->Free(data, 1256);

  // Print statistics to stderr
  ASSERT_OK(jemalloc_stats_print("J"));

  // Read statistics into std::string
  ASSERT_OK_AND_ASSIGN(std::string stats, jemalloc_stats_string("Jax"));

  // Read statistics into std::string with a lambda
  std::string stats2;
  auto write_cb = [&stats2](const char* str) { stats2.append(str); };
  ASSERT_OK(jemalloc_stats_print(write_cb, "Jax"));

  ASSERT_EQ(stats.rfind("{\"jemalloc\":{\"version\"", 0), 0);
  ASSERT_EQ(stats2.rfind("{\"jemalloc\":{\"version\"", 0), 0);
  ASSERT_EQ(stats.substr(0, 100), stats2.substr(0, 100));
#else
  std::string stats;
  auto write_cb = [&stats](const char* str) { stats.append(str); };
  ASSERT_RAISES(NotImplemented, jemalloc_get_stat("thread.peak.read"));
  ASSERT_RAISES(NotImplemented, jemalloc_get_stat("stats.allocated"));
  ASSERT_RAISES(NotImplemented, jemalloc_get_stat("stats.allocated"));
  ASSERT_RAISES(NotImplemented, jemalloc_get_stat("stats.allocatedp"));
  ASSERT_RAISES(NotImplemented, jemalloc_peak_reset());
  ASSERT_RAISES(NotImplemented, jemalloc_stats_print(write_cb, "Jax"));
  ASSERT_RAISES(NotImplemented, jemalloc_stats_print("ax"));
#endif
}

}  // namespace arrow

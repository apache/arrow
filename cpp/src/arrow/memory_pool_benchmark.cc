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

#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

#include "benchmark/benchmark.h"

namespace arrow {

struct SystemAlloc {
  static Result<MemoryPool*> GetAllocator() { return system_memory_pool(); }
};

#ifdef ARROW_JEMALLOC
struct Jemalloc {
  static Result<MemoryPool*> GetAllocator() {
    MemoryPool* pool;
    RETURN_NOT_OK(jemalloc_memory_pool(&pool));
    return pool;
  }
};
#endif

#ifdef ARROW_MIMALLOC
struct Mimalloc {
  static Result<MemoryPool*> GetAllocator() {
    MemoryPool* pool;
    RETURN_NOT_OK(mimalloc_memory_pool(&pool));
    return pool;
  }
};
#endif

static void TouchCacheLines(uint8_t* data, int64_t nbytes) {
  uint8_t total = 0;
  while (nbytes > 0) {
    total += *data;
    data += 64;
    nbytes -= 64;
  }
  benchmark::DoNotOptimize(total);
}

// Benchmark the cost of accessing always the same memory area.
// This gives us a lower bound of the potential difference between
// AllocateTouchDeallocate and AllocateDeallocate.
static void TouchArea(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t nbytes = state.range(0);
  MemoryPool* pool = default_memory_pool();
  uint8_t* data;
  ARROW_CHECK_OK(pool->Allocate(nbytes, &data));

  for (auto _ : state) {
    TouchCacheLines(data, nbytes);
  }

  pool->Free(data, nbytes);
}

// Benchmark the raw cost of allocating memory.
// Note this is a best case situation: we always allocate and deallocate exactly
// the same size, without any other allocator traffic.  However, it can be
// representative of workloads where we routinely create and destroy
// temporary buffers for intermediate computation results.
template <typename Alloc>
static void AllocateDeallocate(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t nbytes = state.range(0);
  MemoryPool* pool = *Alloc::GetAllocator();

  for (auto _ : state) {
    uint8_t* data;
    ARROW_CHECK_OK(pool->Allocate(nbytes, &data));
    pool->Free(data, nbytes);
  }
}

// Benchmark the cost of allocating memory plus accessing it.
template <typename Alloc>
static void AllocateTouchDeallocate(
    benchmark::State& state) {  // NOLINT non-const reference
  const int64_t nbytes = state.range(0);
  MemoryPool* pool = *Alloc::GetAllocator();

  for (auto _ : state) {
    uint8_t* data;
    ARROW_CHECK_OK(pool->Allocate(nbytes, &data));
    TouchCacheLines(data, nbytes);
    pool->Free(data, nbytes);
  }
}

#define BENCHMARK_ALLOCATE_ARGS \
  ->RangeMultiplier(16)->Range(4096, 16 * 1024 * 1024)->ArgName("size")->UseRealTime()

#define BENCHMARK_ALLOCATE(benchmark_func, template_param) \
  BENCHMARK_TEMPLATE(benchmark_func, template_param) BENCHMARK_ALLOCATE_ARGS

BENCHMARK(TouchArea) BENCHMARK_ALLOCATE_ARGS;

BENCHMARK_ALLOCATE(AllocateDeallocate, SystemAlloc);
BENCHMARK_ALLOCATE(AllocateTouchDeallocate, SystemAlloc);

#ifdef ARROW_JEMALLOC
BENCHMARK_ALLOCATE(AllocateDeallocate, Jemalloc);
BENCHMARK_ALLOCATE(AllocateTouchDeallocate, Jemalloc);
#endif

#ifdef ARROW_MIMALLOC
BENCHMARK_ALLOCATE(AllocateDeallocate, Mimalloc);
BENCHMARK_ALLOCATE(AllocateTouchDeallocate, Mimalloc);
#endif

}  // namespace arrow

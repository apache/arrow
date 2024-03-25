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

#include "arrow/config.h"  // for ARROW_JEMALLOC, ARROW_MIMALLOC
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

#include "benchmark/benchmark.h"

namespace arrow {

static constexpr int64_t kCacheLineSize = 64;

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
    data += kCacheLineSize;
    nbytes -= kCacheLineSize;
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
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * nbytes);
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
  state.SetItemsProcessed(state.iterations());
  // SetBytesProcessed() would give nonsensical figures since the data is not
  // actually processed.
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
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * nbytes);
}

// 256 kiB: typical max size for a scratch space (L2-sized)
static constexpr int64_t kMaxReallocationSize = 256 << 10;
// 4 kiB: typical increment when resizing a scratch space
static constexpr int64_t kReallocationIncrement = 4096;

template <typename Alloc, bool Copy>
static void BenchmarkReallocateGrowing(benchmark::State& state) {
  MemoryPool* pool = *Alloc::GetAllocator();
  pool->ReleaseUnused();
  int64_t nb_reallocs = 0;
  int64_t nb_inplace_reallocs = 0;
  for (auto _ : state) {
    const auto alloc_before = pool->bytes_allocated();
    uint8_t* data;
    int64_t size = 0;
    ARROW_CHECK_OK(pool->Allocate(size, &data));
    while (size < kMaxReallocationSize) {
      const auto old_data = data;
      int64_t new_size = size + kReallocationIncrement;
      if constexpr (Copy) {
        ARROW_CHECK_OK(pool->Reallocate(size, new_size, &data));
      } else {
        ARROW_CHECK_OK(pool->ReallocateNoCopy(size, new_size, &data));
      }
      ++nb_reallocs;
      nb_inplace_reallocs += (data == old_data);
      size = new_size;
    }
    pool->Free(data, size);
    ARROW_CHECK_EQ(pool->bytes_allocated(), alloc_before);
  }
  state.SetItemsProcessed(nb_reallocs);
  state.counters["percent_in_place"] = 100.0 * nb_inplace_reallocs / nb_reallocs;
}

template <typename Alloc>
static void ReallocateGrowing(benchmark::State& state) {
  BenchmarkReallocateGrowing<Alloc, /*Copy=*/true>(state);
}

template <typename Alloc>
static void ReallocateGrowingNoCopy(benchmark::State& state) {
  BenchmarkReallocateGrowing<Alloc, /*Copy=*/false>(state);
}

template <typename Alloc, bool Copy>
static void BenchmarkReallocateShrinking(benchmark::State& state) {
  MemoryPool* pool = *Alloc::GetAllocator();
  pool->ReleaseUnused();

  int64_t nb_reallocs = 0;
  int64_t nb_inplace_reallocs = 0;
  for (auto _ : state) {
    const auto alloc_before = pool->bytes_allocated();
    uint8_t* data;
    int64_t size = kMaxReallocationSize;
    ARROW_CHECK_OK(pool->Allocate(size, &data));
    while (size >= kReallocationIncrement) {
      const auto old_data = data;
      int64_t new_size = size - kReallocationIncrement;
      if constexpr (Copy) {
        ARROW_CHECK_OK(pool->Reallocate(size, new_size, &data));
      } else {
        ARROW_CHECK_OK(pool->ReallocateNoCopy(size, new_size, &data));
      }
      ++nb_reallocs;
      nb_inplace_reallocs += (data == old_data);
      size = new_size;
    }
    pool->Free(data, size);
    ARROW_CHECK_EQ(pool->bytes_allocated(), alloc_before);
  }
  state.SetItemsProcessed(nb_reallocs);
  state.counters["percent_in_place"] = 100.0 * nb_inplace_reallocs / nb_reallocs;
}

template <typename Alloc>
static void ReallocateShrinking(benchmark::State& state) {
  BenchmarkReallocateShrinking<Alloc, /*Copy=*/true>(state);
}

template <typename Alloc>
static void ReallocateShrinkingNoCopy(benchmark::State& state) {
  BenchmarkReallocateShrinking<Alloc, /*Copy=*/false>(state);
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

BENCHMARK_TEMPLATE(ReallocateGrowing, SystemAlloc);
BENCHMARK_TEMPLATE(ReallocateGrowingNoCopy, SystemAlloc);
#ifdef ARROW_JEMALLOC
BENCHMARK_TEMPLATE(ReallocateGrowing, Jemalloc);
BENCHMARK_TEMPLATE(ReallocateGrowingNoCopy, Jemalloc);
#endif
#ifdef ARROW_MIMALLOC
BENCHMARK_TEMPLATE(ReallocateGrowing, Mimalloc);
BENCHMARK_TEMPLATE(ReallocateGrowingNoCopy, Mimalloc);
#endif

BENCHMARK_TEMPLATE(ReallocateShrinking, SystemAlloc);
BENCHMARK_TEMPLATE(ReallocateShrinkingNoCopy, SystemAlloc);
#ifdef ARROW_JEMALLOC
BENCHMARK_TEMPLATE(ReallocateShrinking, Jemalloc);
BENCHMARK_TEMPLATE(ReallocateShrinkingNoCopy, Jemalloc);
#endif
#ifdef ARROW_MIMALLOC
BENCHMARK_TEMPLATE(ReallocateShrinking, Mimalloc);
BENCHMARK_TEMPLATE(ReallocateShrinkingNoCopy, Mimalloc);
#endif

}  // namespace arrow

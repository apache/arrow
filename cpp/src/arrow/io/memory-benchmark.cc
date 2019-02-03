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

#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif

#include <iostream>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/test-util.h"
#include "arrow/util/cpu-info.h"

#include "benchmark/benchmark.h"

namespace arrow {

static const int kNumCores = internal::CpuInfo::GetInstance()->num_cores();
constexpr size_t kMemoryPerCore = 32 * 1024 * 1024;
using BufferPtr = std::shared_ptr<Buffer>;

using VectorType = __m128i;

// See http://codearcana.com/posts/2013/05/18/achieving-maximum-memory-bandwidth.html
// for the usage of stream loads/writes. Or section 6.1, page 47 of
// https://akkadia.org/drepper/cpumemory.pdf .

static void Read(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(src);
  (void)dst;

  for (size_t i = 0; i < size / sizeof(VectorType); i++)
    benchmark::DoNotOptimize(_mm_stream_load_si128(&simd[i]));
}

static void Write(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(dst);
  const VectorType ones = _mm_set1_epi32(1);
  (void)src;

  for (size_t i = 0; i < size / sizeof(VectorType); i++) _mm_stream_si128(&simd[i], ones);
}

static void ReadWrite(void* src, void* dst, size_t size) {
  auto src_simd = static_cast<VectorType*>(src);
  auto dst_simd = static_cast<VectorType*>(dst);

  for (size_t i = 0; i < size / sizeof(VectorType); i++)
    _mm_stream_si128(&dst_simd[i], _mm_stream_load_si128(&src_simd[i]));
}

using ApplyFn = decltype(Read);

template <ApplyFn Apply>
static void MemoryBandwidth(benchmark::State& state) {  // NOLINT non-const reference
  const size_t buffer_size = kMemoryPerCore;
  BufferPtr src, dst;

  ABORT_NOT_OK(AllocateBuffer(buffer_size, &src));
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &dst));
  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    Apply(src->mutable_data(), dst->mutable_data(), buffer_size);
  }

  state.SetBytesProcessed(state.iterations() * buffer_size);
}

// `UseRealTime` is required due to threads, otherwise the cumulative CPU time
// is used which will skew the results by the number of threads.
BENCHMARK_TEMPLATE(MemoryBandwidth, Read)->ThreadRange(1, kNumCores)->UseRealTime();
BENCHMARK_TEMPLATE(MemoryBandwidth, Write)->ThreadRange(1, kNumCores)->UseRealTime();
BENCHMARK_TEMPLATE(MemoryBandwidth, ReadWrite)->ThreadRange(1, kNumCores)->UseRealTime();

static void ParallelMemoryCopy(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t n_threads = state.range(0);
  const int64_t buffer_size = kMemoryPerCore;

  std::shared_ptr<Buffer> src, dst;
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &src));
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &dst));

  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    io::FixedSizeBufferWriter writer(dst);
    writer.set_memcopy_threads(static_cast<int>(n_threads));
    ABORT_NOT_OK(writer.Write(src->data(), src->size()));
  }

  state.SetBytesProcessed(int64_t(state.iterations()) * buffer_size);
  state.counters["threads"] = static_cast<double>(n_threads);
}

BENCHMARK(ParallelMemoryCopy)->RangeMultiplier(2)->Range(1, kNumCores)->UseRealTime();

}  // namespace arrow

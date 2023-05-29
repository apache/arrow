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

#include <iostream>

#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/simd.h"

#include "benchmark/benchmark.h"

namespace arrow {

using internal::CpuInfo;
static const CpuInfo* cpu_info = CpuInfo::GetInstance();

static const int kNumCores = cpu_info->num_cores();
static const int64_t kL1Size = cpu_info->CacheSize(CpuInfo::CacheLevel::L1);
static const int64_t kL2Size = cpu_info->CacheSize(CpuInfo::CacheLevel::L2);
static const int64_t kL3Size = cpu_info->CacheSize(CpuInfo::CacheLevel::L3);

constexpr size_t kMemoryPerCore = 32 * 1024 * 1024;
using BufferPtr = std::shared_ptr<Buffer>;

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
#ifndef _MSC_VER

#ifdef ARROW_HAVE_SSE4_2

#ifdef ARROW_HAVE_AVX512

using VectorType = __m512i;
#define VectorSet _mm512_set1_epi32
#define VectorLoad _mm512_stream_load_si512
#define VectorLoadAsm(SRC, DST) \
  asm volatile("vmovaps %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm512_stream_load_si512
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("vmovntdqa %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm512_stream_si512

#else

#ifdef ARROW_HAVE_AVX2

using VectorType = __m256i;
#define VectorSet _mm256_set1_epi32
#define VectorLoad _mm256_stream_load_si256
#define VectorLoadAsm(SRC, DST) \
  asm volatile("vmovaps %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm256_stream_load_si256
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("vmovntdqa %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm256_stream_si256

#else  // ARROW_HAVE_AVX2 not set

using VectorType = __m128i;
#define VectorSet _mm_set1_epi32
#define VectorLoad _mm_stream_load_si128
#define VectorLoadAsm(SRC, DST) \
  asm volatile("movaps %[src], %[dst]" : [dst] "=x"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm_stream_load_si128
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("movntdqa %[src], %[dst]" : [dst] "=x"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm_stream_si128

#endif  // ARROW_HAVE_AVX2
#endif  // ARROW_HAVE_AVX512

static void Read(void* src, void* dst, size_t size) {
  const auto simd = static_cast<VectorType*>(src);
  VectorType a, b, c, d;
  (void)dst;

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorLoadAsm(simd[i], a);
    VectorLoadAsm(simd[i + 1], b);
    VectorLoadAsm(simd[i + 2], c);
    VectorLoadAsm(simd[i + 3], d);
  }

  memset(&a, 0, sizeof(a));
  memset(&b, 0, sizeof(b));
  memset(&c, 0, sizeof(c));
  memset(&d, 0, sizeof(d));

  auto result = a + b + c + d;
  benchmark::DoNotOptimize(result);
}

// See http://codearcana.com/posts/2013/05/18/achieving-maximum-memory-bandwidth.html
// for the usage of stream loads/writes. Or section 6.1, page 47 of
// https://akkadia.org/drepper/cpumemory.pdf .
static void StreamRead(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(src);
  VectorType a, b, c, d;
  (void)dst;

  memset(&a, 0, sizeof(a));
  memset(&b, 0, sizeof(b));
  memset(&c, 0, sizeof(c));
  memset(&d, 0, sizeof(d));

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamLoadAsm(simd[i], a);
    VectorStreamLoadAsm(simd[i + 1], b);
    VectorStreamLoadAsm(simd[i + 2], c);
    VectorStreamLoadAsm(simd[i + 3], d);
  }

  auto result = a + b + c + d;
  benchmark::DoNotOptimize(result);
}

static void StreamWrite(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(dst);
  const VectorType ones = VectorSet(1);
  (void)src;

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamWrite(&simd[i], ones);
    VectorStreamWrite(&simd[i + 1], ones);
    VectorStreamWrite(&simd[i + 2], ones);
    VectorStreamWrite(&simd[i + 3], ones);
  }
}

static void StreamReadWrite(void* src, void* dst, size_t size) {
  auto src_simd = static_cast<VectorType*>(src);
  auto dst_simd = static_cast<VectorType*>(dst);

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamWrite(&dst_simd[i], VectorStreamLoad(&src_simd[i]));
    VectorStreamWrite(&dst_simd[i + 1], VectorStreamLoad(&src_simd[i + 1]));
    VectorStreamWrite(&dst_simd[i + 2], VectorStreamLoad(&src_simd[i + 2]));
    VectorStreamWrite(&dst_simd[i + 3], VectorStreamLoad(&src_simd[i + 3]));
  }
}

#endif  // ARROW_HAVE_SSE4_2

#ifdef ARROW_HAVE_NEON

using VectorType = uint8x16_t;
using VectorTypeDual = uint8x16x2_t;

#define VectorSet vdupq_n_u8
#define VectorLoadAsm vld1q_u8

static void armv8_stream_load_pair(VectorType* src, VectorType* dst) {
  asm volatile("LDNP %[reg1], %[reg2], [%[from]]\n\t"
               : [reg1] "+r"(*dst), [reg2] "+r"(*(dst + 1))
               : [from] "r"(src));
}

static void armv8_stream_store_pair(VectorType* src, VectorType* dst) {
  asm volatile("STNP %[reg1], %[reg2], [%[to]]\n\t"
               : [to] "+r"(dst)
               : [reg1] "r"(*src), [reg2] "r"(*(src + 1))
               : "memory");
}

static void armv8_stream_ldst_pair(VectorType* src, VectorType* dst) {
  asm volatile(
      "LDNP q1, q2, [%[from]]\n\t"
      "STNP q1, q2, [%[to]]\n\t"
      : [from] "+r"(src), [to] "+r"(dst)
      :
      : "memory", "v0", "v1", "v2", "v3");
}

static void Read(void* src, void* dst, size_t size) {
  const auto simd = static_cast<uint8_t*>(src);
  VectorType a;
  (void)dst;

  memset(&a, 0, sizeof(a));

  for (size_t i = 0; i < size; i += sizeof(VectorType)) {
    a = VectorLoadAsm(simd + i);
  }

  benchmark::DoNotOptimize(a);
}

// See http://codearcana.com/posts/2013/05/18/achieving-maximum-memory-bandwidth.html
// for the usage of stream loads/writes. Or section 6.1, page 47 of
// https://akkadia.org/drepper/cpumemory.pdf .
static void StreamRead(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(src);
  VectorType a[2];
  (void)dst;

  memset(&a, 0, sizeof(VectorTypeDual));

  for (size_t i = 0; i < size / sizeof(VectorType); i += 2) {
    armv8_stream_load_pair(simd + i, a);
  }

  benchmark::DoNotOptimize(a);
}

static void StreamWrite(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(dst);
  VectorType ones[2];
  (void)src;

  ones[0] = VectorSet(1);
  ones[1] = VectorSet(1);

  for (size_t i = 0; i < size / sizeof(VectorType); i += 2) {
    armv8_stream_store_pair(static_cast<VectorType*>(ones), simd + i);
  }
}

static void StreamReadWrite(void* src, void* dst, size_t size) {
  auto src_simd = static_cast<VectorType*>(src);
  auto dst_simd = static_cast<VectorType*>(dst);

  for (size_t i = 0; i < size / sizeof(VectorType); i += 2) {
    armv8_stream_ldst_pair(src_simd + i, dst_simd + i);
  }
}

#endif  // ARROW_HAVE_NEON

static void PlatformMemcpy(void* src, void* dst, size_t size) { memcpy(src, dst, size); }

using ApplyFn = decltype(Read);

template <ApplyFn Apply>
static void MemoryBandwidth(benchmark::State& state) {  // NOLINT non-const reference
  const size_t buffer_size = state.range(0);
  BufferPtr src, dst;

  dst = *AllocateBuffer(buffer_size);
  src = *AllocateBuffer(buffer_size);
  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    Apply(src->mutable_data(), dst->mutable_data(), buffer_size);
  }

  state.SetBytesProcessed(state.iterations() * buffer_size);
}

#ifdef ARROW_HAVE_SSE4_2
static void SetCacheBandwidthArgs(benchmark::internal::Benchmark* bench) {
  auto cache_sizes = {kL1Size, kL2Size, kL3Size};
  for (auto size : cache_sizes) {
    bench->Arg(size / 2);
    bench->Arg(size);
    bench->Arg(size * 2);
  }

  bench->ArgName("size");
}

BENCHMARK_TEMPLATE(MemoryBandwidth, Read)->Apply(SetCacheBandwidthArgs);
#endif  // ARROW_HAVE_SSE4_2

static void SetMemoryBandwidthArgs(benchmark::internal::Benchmark* bench) {
  // `UseRealTime` is required due to threads, otherwise the cumulative CPU time
  // is used which will skew the results by the number of threads.
  bench->Arg(kMemoryPerCore)->ThreadRange(1, kNumCores)->UseRealTime();
}

BENCHMARK_TEMPLATE(MemoryBandwidth, StreamRead)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, StreamWrite)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, StreamReadWrite)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, PlatformMemcpy)->Apply(SetMemoryBandwidthArgs);

#endif  // _MSC_VER
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

static void ParallelMemoryCopy(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t n_threads = state.range(0);
  const int64_t buffer_size = kMemoryPerCore;

  auto src = *AllocateBuffer(buffer_size);
  std::shared_ptr<Buffer> dst = *AllocateBuffer(buffer_size);

  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    io::FixedSizeBufferWriter writer(dst);
    writer.set_memcopy_threads(static_cast<int>(n_threads));
    ABORT_NOT_OK(writer.Write(src->data(), src->size()));
  }

  state.SetBytesProcessed(int64_t(state.iterations()) * buffer_size);
}

BENCHMARK(ParallelMemoryCopy)
    ->RangeMultiplier(2)
    ->Range(1, kNumCores)
    ->ArgName("threads")
    ->UseRealTime();

static void BenchmarkBufferOutputStream(
    const std::string& datum,
    benchmark::State& state) {  // NOLINT non-const reference
  const void* raw_data = datum.data();
  int64_t raw_nbytes = static_cast<int64_t>(datum.size());
  // Write approx. 32 MB to each BufferOutputStream
  int64_t num_raw_values = (1 << 25) / raw_nbytes;
  for (auto _ : state) {
    auto stream = *io::BufferOutputStream::Create(1024);
    for (int64_t i = 0; i < num_raw_values; ++i) {
      ABORT_NOT_OK(stream->Write(raw_data, raw_nbytes));
    }
    ABORT_NOT_OK(stream->Finish());
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * num_raw_values * raw_nbytes);
}

static void BufferOutputStreamTinyWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 8-byte datum
  return BenchmarkBufferOutputStream("abdefghi", state);
}

static void BufferOutputStreamSmallWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 700-byte datum
  std::string datum;
  for (int i = 0; i < 100; ++i) {
    datum += "abcdefg";
  }
  return BenchmarkBufferOutputStream(datum, state);
}

static void BufferOutputStreamLargeWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 1.5MB datum
  std::string datum(1500000, 'x');
  return BenchmarkBufferOutputStream(datum, state);
}

BENCHMARK(BufferOutputStreamTinyWrites)->UseRealTime();
BENCHMARK(BufferOutputStreamSmallWrites)->UseRealTime();
BENCHMARK(BufferOutputStreamLargeWrites)->UseRealTime();

}  // namespace arrow

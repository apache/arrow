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
#include <cstring>
#include <random>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/spaced_internal.h"
#include "arrow/util/spaced_simd_internal.h"

namespace arrow::util::internal {
namespace {

template <typename Uint>
using ExpandRightwardFunc = void (*)(Uint*, int, int, const uint8_t*, int64_t);

template <typename Uint>
using ExpandFunc = void (*)(const Uint*, int, int, const uint8_t*, int64_t, Uint*);

// Either independent slots valid with probability `p`, or (p < 0) runs of
// valid and null slots with the given mean lengths.
struct ValidityPattern {
  double p;
  int mean_valid;
  int mean_null;
};

constexpr ValidityPattern Bernoulli(double p) { return {p, 0, 0}; }
constexpr ValidityPattern Runs(int mean_valid, int mean_null) {
  return {-1, mean_valid, mean_null};
}

std::vector<uint8_t> MakeBitmap(const ValidityPattern& pattern, int n) {
  std::mt19937_64 rng(42);
  std::vector<uint8_t> bitmap(bit_util::BytesForBits(n));
  if (pattern.p >= 0) {
    std::bernoulli_distribution valid(pattern.p);
    for (int i = 0; i < n; ++i) bit_util::SetBitTo(bitmap.data(), i, valid(rng));
  } else {
    std::geometric_distribution<int> valid_len(1.0 / pattern.mean_valid);
    std::geometric_distribution<int> null_len(1.0 / pattern.mean_null);
    bool valid = true;
    for (int i = 0; i < n;) {
      const int len = 1 + (valid ? valid_len(rng) : null_len(rng));
      for (int end = std::min(n, i + len); i < end; ++i) {
        bit_util::SetBitTo(bitmap.data(), i, valid);
      }
      valid = !valid;
    }
  }
  return bitmap;
}

constexpr int kNumValues = 2048;

template <typename Uint>
struct ExpandInput {
  explicit ExpandInput(ValidityPattern pattern)
      : bitmap(MakeBitmap(pattern, kNumValues)), storage(2 * kNumValues + kGap) {
    int num_valid = 0;
    for (int i = 0; i < kNumValues; ++i) {
      num_valid += bit_util::GetBit(bitmap.data(), i);
    }
    null_count = kNumValues - num_valid;
    for (int i = 0; i < kNumValues; ++i) dense()[i] = static_cast<Uint>(i + 1);
  }

  // The dense values and the output in a single allocation, 1 KiB apart modulo
  // 4 KiB: the copies are much slower when the source and the destination are
  // a few bytes apart modulo 4 KiB (4K aliasing), which separate allocations
  // of the same size can easily be.
  static constexpr int kGap = 1024 / sizeof(Uint);
  const Uint* dense() const { return storage.data(); }
  Uint* dense() { return storage.data(); }
  Uint* output() { return storage.data() + kNumValues + kGap; }

  std::vector<uint8_t> bitmap;
  std::vector<Uint> storage;
  int null_count;
};

bool SkipUnsupported(benchmark::State& state, int64_t cpu_flags) {
  if (::arrow::internal::CpuInfo::GetInstance()->IsSupported(cpu_flags)) return false;
  state.SkipWithMessage("CPU not supported");
  return true;
}

// In place, as after decoding the dense values: the timing includes copying
// them to the buffer before each expansion.
template <typename Uint>
void BM_SpacedExpandRightward(benchmark::State& state, ExpandRightwardFunc<Uint> expand,
                              int64_t cpu_flags, ValidityPattern pattern) {
  if (SkipUnsupported(state, cpu_flags)) return;
  ExpandInput<Uint> input(pattern);
  for (auto _ : state) {
    std::memcpy(input.output(), input.dense(),
                sizeof(Uint) * (kNumValues - input.null_count));
    expand(input.output(), kNumValues, input.null_count, input.bitmap.data(), 0);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kNumValues);
}

// Out of place, straight from the dense values.
template <typename Uint>
void BM_SpacedExpand(benchmark::State& state, ExpandFunc<Uint> expand, int64_t cpu_flags,
                     ValidityPattern pattern) {
  if (SkipUnsupported(state, cpu_flags)) return;
  ExpandInput<Uint> input(pattern);
  for (auto _ : state) {
    expand(input.dense(), kNumValues, input.null_count, input.bitmap.data(), 0,
           input.output());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kNumValues);
}

#define BENCHMARK_SPACED_EXPAND_PATTERNS(BM, NAME, FUNC, FLAGS)              \
  BENCHMARK_CAPTURE(BM, NAME##_p0_02, FUNC, FLAGS, Bernoulli(0.02));         \
  BENCHMARK_CAPTURE(BM, NAME##_p0_10, FUNC, FLAGS, Bernoulli(0.10));         \
  BENCHMARK_CAPTURE(BM, NAME##_p0_50, FUNC, FLAGS, Bernoulli(0.50));         \
  BENCHMARK_CAPTURE(BM, NAME##_p0_90, FUNC, FLAGS, Bernoulli(0.90));         \
  BENCHMARK_CAPTURE(BM, NAME##_p0_95, FUNC, FLAGS, Bernoulli(0.95));         \
  BENCHMARK_CAPTURE(BM, NAME##_p0_99, FUNC, FLAGS, Bernoulli(0.99));         \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v2_n2, FUNC, FLAGS, Runs(2, 2));         \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v8_n8, FUNC, FLAGS, Runs(8, 8));         \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v32_n32, FUNC, FLAGS, Runs(32, 32));     \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v64_n2, FUNC, FLAGS, Runs(64, 2));       \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v128_n8, FUNC, FLAGS, Runs(128, 8));     \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v256_n256, FUNC, FLAGS, Runs(256, 256)); \
  BENCHMARK_CAPTURE(BM, NAME##_runs_v1000_n1, FUNC, FLAGS, Runs(1000, 1))

// Both directions, both widths
#define BENCHMARK_SPACED_EXPAND_TARGET(TARGET, FLAGS)                                \
  BENCHMARK_SPACED_EXPAND_PATTERNS(BM_SpacedExpandRightward, TARGET##_32,            \
                                   &SpacedExpandRightward##TARGET<uint32_t>, FLAGS); \
  BENCHMARK_SPACED_EXPAND_PATTERNS(BM_SpacedExpandRightward, TARGET##_64,            \
                                   &SpacedExpandRightward##TARGET<uint64_t>, FLAGS); \
  BENCHMARK_SPACED_EXPAND_PATTERNS(BM_SpacedExpand, TARGET##_32,                     \
                                   &SpacedExpand##TARGET<uint32_t>, FLAGS);          \
  BENCHMARK_SPACED_EXPAND_PATTERNS(BM_SpacedExpand, TARGET##_64,                     \
                                   &SpacedExpand##TARGET<uint64_t>, FLAGS)

// Scalar: run based, None: word driver without SIMD
BENCHMARK_SPACED_EXPAND_TARGET(Scalar, 0);
BENCHMARK_SPACED_EXPAND_TARGET(None, 0);

#if defined(ARROW_HAVE_NEON)
BENCHMARK_SPACED_EXPAND_TARGET(Neon, 0);
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
BENCHMARK_SPACED_EXPAND_TARGET(Sse4_2, ::arrow::internal::CpuInfo::SSE4_2);
#endif

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
BENCHMARK_SPACED_EXPAND_TARGET(Avx2, ::arrow::internal::CpuInfo::AVX2);
#endif

#if defined(ARROW_HAVE_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX512)
BENCHMARK_SPACED_EXPAND_TARGET(Avx512, ::arrow::internal::CpuInfo::AVX512);
#endif

#undef BENCHMARK_SPACED_EXPAND_TARGET
#undef BENCHMARK_SPACED_EXPAND_PATTERNS

}  // namespace
}  // namespace arrow::util::internal

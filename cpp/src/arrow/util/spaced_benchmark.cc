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
#include "arrow/util/spaced_internal.h"
#include "arrow/util/spaced_simd_internal.h"

namespace arrow::util::internal {
namespace {

template <typename Uint>
using ExpandFunc = void (*)(Uint*, int, int, const uint8_t*, int64_t);

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

// The timing includes copying the dense values back in place before each
// expansion.
template <typename Uint>
void BM_SpacedExpandRightward(benchmark::State& state, ExpandFunc<Uint> expand,
                              ValidityPattern pattern) {
  constexpr int kNumValues = 2048;
  const auto bitmap = MakeBitmap(pattern, kNumValues);
  int num_valid = 0;
  for (int i = 0; i < kNumValues; ++i) {
    num_valid += bit_util::GetBit(bitmap.data(), i);
  }
  const int null_count = kNumValues - num_valid;

  std::vector<Uint> dense(kNumValues);
  for (int i = 0; i < kNumValues; ++i) dense[i] = static_cast<Uint>(i + 1);
  std::vector<Uint> buffer(kNumValues);

  for (auto _ : state) {
    std::memcpy(buffer.data(), dense.data(), sizeof(Uint) * num_valid);
    expand(buffer.data(), kNumValues, null_count, bitmap.data(), 0);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kNumValues);
}

#define BENCHMARK_SPACED_EXPAND_PATTERNS(NAME, FUNC)                                    \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_02, FUNC, Bernoulli(0.02));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_10, FUNC, Bernoulli(0.10));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_50, FUNC, Bernoulli(0.50));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_90, FUNC, Bernoulli(0.90));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_95, FUNC, Bernoulli(0.95));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_p0_99, FUNC, Bernoulli(0.99));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v2_n2, FUNC, Runs(2, 2));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v8_n8, FUNC, Runs(8, 8));     \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v32_n32, FUNC, Runs(32, 32)); \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v64_n2, FUNC, Runs(64, 2));   \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v128_n8, FUNC, Runs(128, 8)); \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v256_n256, FUNC,              \
                    Runs(256, 256));                                                    \
  BENCHMARK_CAPTURE(BM_SpacedExpandRightward, NAME##_runs_v1000_n1, FUNC, Runs(1000, 1))

BENCHMARK_SPACED_EXPAND_PATTERNS(Scalar32, &SpacedExpandRightwardScalar<uint32_t>);
BENCHMARK_SPACED_EXPAND_PATTERNS(Scalar64, &SpacedExpandRightwardScalar<uint64_t>);

#if defined(ARROW_HAVE_NEON)
BENCHMARK_SPACED_EXPAND_PATTERNS(Neon32, &SpacedExpandRightwardNeon<uint32_t>);
BENCHMARK_SPACED_EXPAND_PATTERNS(Neon64, &SpacedExpandRightwardNeon<uint64_t>);
#elif defined(ARROW_HAVE_SSE4_2)
BENCHMARK_SPACED_EXPAND_PATTERNS(Sse4_2_32, &SpacedExpandRightwardSse4_2<uint32_t>);
BENCHMARK_SPACED_EXPAND_PATTERNS(Sse4_2_64, &SpacedExpandRightwardSse4_2<uint64_t>);
#endif

#undef BENCHMARK_SPACED_EXPAND_PATTERNS

}  // namespace
}  // namespace arrow::util::internal

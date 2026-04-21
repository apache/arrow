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

// PFOR encoding/decoding benchmarks.
//
// Data distributions are inspired by Snowflake's NumericComprBenchmark.cpp,
// covering the key archetypes that exercise PFOR's cost model differently:
//   - Constant: bit_width=0, best case
//   - Sequential: small range, ideal FOR
//   - SmallRange: clustered random, good FOR compression
//   - HighBaseSmallRange: high absolute values, small delta range (timestamps)
//   - WithOutliers: tests exception handling path
//   - Random: worst case, full bit-width
//   - TPC-DS DateSk/StoreSk/ItemSk/Quantity: realistic surrogate key distributions

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/util/pfor/pfor.h"
#include "arrow/util/pfor/pfor_wrapper.h"

namespace arrow::util::pfor {
namespace {

// ======================================================================
// Data Generators

using Int32Gen = std::vector<int32_t> (*)(int64_t);
using Int64Gen = std::vector<int64_t> (*)(int64_t);

template <typename T>
std::vector<T> GenConstant(int64_t n) {
  return std::vector<T>(n, static_cast<T>(42));
}

template <typename T>
std::vector<T> GenSequential(int64_t n) {
  std::vector<T> v(n);
  std::iota(v.begin(), v.end(), static_cast<T>(0));
  return v;
}

template <typename T>
std::vector<T> GenSmallRange(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> dist(100000, 200000);
  for (auto& x : v) x = dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenHighBaseSmallRange(int64_t n) {
  std::vector<T> v(n);
  const T kBase = static_cast<T>(1704067200);
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> dist(0, 1000);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenWithOutliers(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<T> small_dist(1000, 1255);
  for (auto& x : v) x = small_dist(rng);
  std::uniform_int_distribution<int64_t> pos_dist(0, n - 1);
  int num_outliers = std::max(static_cast<int64_t>(1), n / 100);
  for (int i = 0; i < num_outliers; ++i) {
    v[pos_dist(rng)] = static_cast<T>(std::numeric_limits<T>::max() / 2 + i);
  }
  return v;
}

template <typename T>
std::vector<T> GenRandom(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(99);
  std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(),
                                        std::numeric_limits<T>::max());
  for (auto& x : v) x = dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenTpcdsSoldDateSk(int64_t n) {
  std::vector<T> v(n);
  const T kBase = 2450815;
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> dist(0, 1820);
  for (auto& x : v) x = kBase + dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenTpcdsStoreSk(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> dist(1, 1000);
  for (auto& x : v) x = dist(rng);
  return v;
}

template <typename T>
std::vector<T> GenTpcdsItemSk(int64_t n) {
  std::vector<T> v(n);
  const T kMax = 100000;
  std::mt19937_64 rng(12345);
  std::exponential_distribution<double> exp_dist(0.00005);
  for (auto& x : v) {
    T val = static_cast<T>(exp_dist(rng));
    x = std::min(static_cast<T>(val + 1), kMax);
  }
  return v;
}

template <typename T>
std::vector<T> GenTpcdsQuantity(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<T> small_dist(1, 10);
  std::uniform_int_distribution<T> large_dist(11, 100);
  std::uniform_int_distribution<int> chance(0, 99);
  for (auto& x : v) {
    x = (chance(rng) < 90) ? small_dist(rng) : large_dist(rng);
  }
  return v;
}

// ======================================================================
// Benchmark Core

template <typename T>
void BM_PforEncodeImpl(benchmark::State& state,
                       std::vector<T> (*generator)(int64_t)) {
  const int64_t num_values = state.range(0);
  auto values = generator(num_values);

  size_t max_size = PforWrapper<T>::GetMaxCompressedSize(num_values);
  std::vector<char> compressed(max_size);

  for (auto _ : state) {
    size_t comp_size = max_size;
    PforWrapper<T>::Encode(values.data(), num_values,
                           compressed.data(), &comp_size);
    benchmark::DoNotOptimize(comp_size);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * num_values *
                          static_cast<int64_t>(sizeof(T)));
  state.SetItemsProcessed(state.iterations() * num_values);

  // Report compression ratio
  size_t comp_size = max_size;
  PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size);
  state.counters["CompRatio%"] = benchmark::Counter(
      100.0 * static_cast<double>(comp_size) /
      static_cast<double>(num_values * sizeof(T)));
}

template <typename T>
void BM_PforDecodeImpl(benchmark::State& state,
                       std::vector<T> (*generator)(int64_t)) {
  const int64_t num_values = state.range(0);
  auto values = generator(num_values);

  size_t max_size = PforWrapper<T>::GetMaxCompressedSize(num_values);
  std::vector<char> compressed(max_size);
  size_t comp_size = max_size;
  PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size);

  std::vector<T> decoded(num_values);

  for (auto _ : state) {
    PforWrapper<T>::Decode(decoded.data(), num_values,
                           compressed.data(), comp_size);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * num_values *
                          static_cast<int64_t>(sizeof(T)));
  state.SetItemsProcessed(state.iterations() * num_values);
}

// ======================================================================
// Non-template wrappers to avoid comma-in-macro issues with BENCHMARK_CAPTURE

void BM_PforEncodeInt32(benchmark::State& state, Int32Gen gen) {
  BM_PforEncodeImpl<int32_t>(state, gen);
}
void BM_PforDecodeInt32(benchmark::State& state, Int32Gen gen) {
  BM_PforDecodeImpl<int32_t>(state, gen);
}
void BM_PforEncodeInt64(benchmark::State& state, Int64Gen gen) {
  BM_PforEncodeImpl<int64_t>(state, gen);
}
void BM_PforDecodeInt64(benchmark::State& state, Int64Gen gen) {
  BM_PforDecodeImpl<int64_t>(state, gen);
}

// ======================================================================
// Benchmark sizes: 1K, 10K, 100K, 1M

static void CustomArgs(benchmark::internal::Benchmark* b) {
  for (int64_t n : {1024, 10240, 102400, 1048576}) {
    b->Arg(n);
  }
}

// ======================================================================
// INT32 Encode

BENCHMARK_CAPTURE(BM_PforEncodeInt32, Constant, &GenConstant<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Sequential, &GenSequential<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, SmallRange, &GenSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, HighBaseSmallRange,
                  &GenHighBaseSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, WithOutliers, &GenWithOutliers<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Random, &GenRandom<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsSoldDateSk,
                  &GenTpcdsSoldDateSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsStoreSk, &GenTpcdsStoreSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsItemSk, &GenTpcdsItemSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsQuantity,
                  &GenTpcdsQuantity<int32_t>)
    ->Apply(CustomArgs);

// INT32 Decode

BENCHMARK_CAPTURE(BM_PforDecodeInt32, Constant, &GenConstant<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Sequential, &GenSequential<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, SmallRange, &GenSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, HighBaseSmallRange,
                  &GenHighBaseSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, WithOutliers, &GenWithOutliers<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Random, &GenRandom<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsSoldDateSk,
                  &GenTpcdsSoldDateSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsStoreSk, &GenTpcdsStoreSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsItemSk, &GenTpcdsItemSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsQuantity,
                  &GenTpcdsQuantity<int32_t>)
    ->Apply(CustomArgs);

// ======================================================================
// INT64 Encode

BENCHMARK_CAPTURE(BM_PforEncodeInt64, Constant, &GenConstant<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Sequential, &GenSequential<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, SmallRange, &GenSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, HighBaseSmallRange,
                  &GenHighBaseSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, WithOutliers, &GenWithOutliers<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Random, &GenRandom<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, TpcdsSoldDateSk,
                  &GenTpcdsSoldDateSk<int64_t>)
    ->Apply(CustomArgs);

// INT64 Decode

BENCHMARK_CAPTURE(BM_PforDecodeInt64, Constant, &GenConstant<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Sequential, &GenSequential<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, SmallRange, &GenSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, HighBaseSmallRange,
                  &GenHighBaseSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, WithOutliers, &GenWithOutliers<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Random, &GenRandom<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, TpcdsSoldDateSk,
                  &GenTpcdsSoldDateSk<int64_t>)
    ->Apply(CustomArgs);

}  // namespace
}  // namespace arrow::util::pfor

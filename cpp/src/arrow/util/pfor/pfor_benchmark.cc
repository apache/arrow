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
// Data distributions cover the key archetypes that exercise PFOR's cost model
// differently:
//   - Constant: bit_width=0, best case
//   - Sequential: small range, ideal FOR
//   - SmallRange: clustered random, good FOR compression
//   - HighBaseSmallRange: high absolute values, small delta range (timestamps)
//   - WithOutliers: tests exception handling path
//   - Random: worst case, full bit-width
//   - TPC-DS DateSk/StoreSk/ItemSk/Quantity: realistic surrogate key distributions
//
// A second group covers columns with structure between neighbouring values, or
// with a cluster the minimum is not part of. These are what separate a plan
// that differences the values from one that packs them, and a frame of
// reference placed by a search from one pinned to the minimum:
//   - TrendJitter/EventMillis: timestamps, regular and bursty
//   - Sawtooth: a climb cut by a halving, the case for patching both sides
//   - MeasurementSeries/RandomWalk: bounded rate of change, no trend
//   - SortedKeys/IdsWithGaps: monotonic, with and without jumps
//   - SensorDropouts/LowSentinel: a low sentinel below the cluster
//   - Bimodal: two clusters, so no one window covers the vector

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/util/logging.h"
#include "arrow/util/pfor/pfor_internal.h"
#include "arrow/util/pfor/pfor_wrapper_internal.h"

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

// ----------------------------------------------------------------------
// Distributions with structure between neighbouring values, or with a
// cluster the minimum is not part of. The generators above are all either
// unordered or perfectly regular, so none of them separates a plan that
// differences from one that does not, and none of them puts the frame of
// reference anywhere but the minimum.

/// A timestamp column with a fixed sampling interval and jitter: the
/// differences cluster tightly around the interval, the values do not.
template <typename T>
std::vector<T> GenTrendJitter(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(7);
  std::normal_distribution<double> jitter(0.0, 40.0);
  double t = 1704067200.0;
  for (auto& x : v) {
    t += 3.0;
    x = static_cast<T>(t + jitter(rng));
  }
  return v;
}

/// A TCP congestion window: a linear climb cut by a sharp halving. Almost
/// every difference is the same small number and a few are large and
/// negative, which is the shape a two-sided frame plus patching handles at
/// close to no cost and a one-sided one cannot.
template <typename T>
std::vector<T> GenSawtooth(int64_t n) {
  std::vector<T> v(n);
  T cur = 1000;
  for (int64_t i = 0; i < n; ++i) {
    if (i % 200 == 199) {
      cur /= 2;
    } else {
      cur += 12;
    }
    v[i] = cur;
  }
  return v;
}

/// A continuous quantity sampled over time, so the rate of change is bounded
/// but the level wanders: differencing wins, though not by much.
template <typename T>
std::vector<T> GenMeasurement(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(11);
  std::normal_distribution<double> noise(0.0, 1.0);
  double level = 0.0;
  double rate = 0.0;
  for (int64_t i = 0; i < n; ++i) {
    rate = 0.95 * rate + noise(rng);
    level += rate;
    v[i] = static_cast<T>(4000.0 * level / 50.0) + static_cast<T>(500000);
  }
  return v;
}

/// A sorted surrogate key column, which is what a clustered index or a
/// sorted-by-key file produces.
template <typename T>
std::vector<T> GenSortedKeys(int64_t n) {
  auto v = GenTpcdsItemSk<T>(n);
  std::sort(v.begin(), v.end());
  return v;
}

/// A measurement series punctuated by a low sentinel standing in for "no
/// reading". The sentinel sits far below the cluster, so a frame pinned to
/// the minimum has to widen to reach it and no value can ever be patched.
template <typename T>
std::vector<T> GenSensorDropouts(int64_t n) {
  auto v = GenMeasurement<T>(n);
  std::mt19937_64 rng(3);
  std::uniform_int_distribution<int64_t> pos(0, n - 1);
  for (int64_t i = 0; i < std::max<int64_t>(1, n / 200); ++i) {
    v[pos(rng)] = static_cast<T>(-999999);
  }
  return v;
}

/// Monotonic ids with occasional large jumps, as a sequence shared between
/// writers or restarted produces.
template <typename T>
std::vector<T> GenIdsWithGaps(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(5);
  std::uniform_int_distribution<int> step(1, 6);
  std::uniform_int_distribution<int> jump(0, 199);
  T cur = 1000000;
  for (auto& x : v) {
    cur += static_cast<T>(step(rng));
    if (jump(rng) == 0) cur += 100000;
    x = cur;
  }
  return v;
}

/// Prices on a random walk: small signed steps around a high base, and no
/// trend for the differences to line up on.
template <typename T>
std::vector<T> GenRandomWalk(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(13);
  std::uniform_int_distribution<int> step(-25, 25);
  T cur = 1500000;
  for (auto& x : v) {
    cur += static_cast<T>(step(rng));
    x = cur;
  }
  return v;
}

/// Event arrival times in milliseconds since the epoch, bursty, so the gaps
/// are skewed rather than clustered.
template <typename T>
std::vector<T> GenEventMillis(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(17);
  std::exponential_distribution<double> gap(0.4);
  double t = 1704067200.0;
  for (auto& x : v) {
    t += gap(rng);
    x = static_cast<T>(t);
  }
  return v;
}

/// A tight unordered cluster with a low "missing" sentinel: the frame cannot
/// sit at the minimum and differencing has nothing to exploit either, so
/// this is the column that has to come out no worse than before.
template <typename T>
std::vector<T> GenLowSentinel(int64_t n) {
  auto v = GenSmallRange<T>(n);
  std::mt19937_64 rng(23);
  std::uniform_int_distribution<int64_t> pos(0, n - 1);
  for (int64_t i = 0; i < std::max<int64_t>(1, n / 200); ++i) {
    v[pos(rng)] = static_cast<T>(-1000);
  }
  return v;
}

/// Two interleaved clusters far apart, so no single window covers the vector
/// and the cheaper cluster is worth framing on its own.
template <typename T>
std::vector<T> GenBimodal(int64_t n) {
  std::vector<T> v(n);
  std::mt19937_64 rng(19);
  std::uniform_int_distribution<T> lo(1000, 1100);
  std::uniform_int_distribution<T> hi(900000, 900100);
  std::uniform_int_distribution<int> pick(0, 9);
  for (auto& x : v) {
    x = pick(rng) < 8 ? lo(rng) : hi(rng);
  }
  return v;
}

// ======================================================================
// Benchmark Core

template <typename T>
void BM_PforEncodeImpl(benchmark::State& state, std::vector<T> (*generator)(int64_t)) {
  const int64_t num_values = state.range(0);
  auto values = generator(num_values);

  int64_t max_size =
      PforWrapper<T>::GetMaxCompressedSize(static_cast<int32_t>(num_values)).ValueOrDie();
  std::vector<uint8_t> compressed(max_size);

  for (auto _ : state) {
    int64_t comp_size = max_size;
    ARROW_CHECK_OK(
        PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size));
    benchmark::DoNotOptimize(comp_size);
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * num_values *
                          static_cast<int64_t>(sizeof(T)));
  state.SetItemsProcessed(state.iterations() * num_values);

  // Report compression ratio
  int64_t comp_size = max_size;
  ARROW_CHECK_OK(
      PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size));
  state.counters["CompRatio%"] =
      benchmark::Counter(100.0 * static_cast<double>(comp_size) /
                         static_cast<double>(num_values * sizeof(T)));
}

template <typename T>
void BM_PforDecodeImpl(benchmark::State& state, std::vector<T> (*generator)(int64_t)) {
  const int64_t num_values = state.range(0);
  auto values = generator(num_values);

  int64_t max_size =
      PforWrapper<T>::GetMaxCompressedSize(static_cast<int32_t>(num_values)).ValueOrDie();
  std::vector<uint8_t> compressed(max_size);
  int64_t comp_size = max_size;
  ARROW_CHECK_OK(
      PforWrapper<T>::Encode(values.data(), num_values, compressed.data(), &comp_size));

  std::vector<T> decoded(num_values);

  for (auto _ : state) {
    ARROW_CHECK_OK(
        PforWrapper<T>::Decode(compressed.data(), comp_size, num_values, decoded.data()));
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

BENCHMARK_CAPTURE(BM_PforEncodeInt32, Constant, &GenConstant<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Sequential, &GenSequential<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, SmallRange, &GenSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, HighBaseSmallRange, &GenHighBaseSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, WithOutliers, &GenWithOutliers<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Random, &GenRandom<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsSoldDateSk, &GenTpcdsSoldDateSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsStoreSk, &GenTpcdsStoreSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsItemSk, &GenTpcdsItemSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, TpcdsQuantity, &GenTpcdsQuantity<int32_t>)
    ->Apply(CustomArgs);

BENCHMARK_CAPTURE(BM_PforEncodeInt32, TrendJitter, &GenTrendJitter<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Sawtooth, &GenSawtooth<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, MeasurementSeries, &GenMeasurement<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, SortedKeys, &GenSortedKeys<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, SensorDropouts, &GenSensorDropouts<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, IdsWithGaps, &GenIdsWithGaps<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, RandomWalk, &GenRandomWalk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, EventMillis, &GenEventMillis<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, LowSentinel, &GenLowSentinel<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt32, Bimodal, &GenBimodal<int32_t>)->Apply(CustomArgs);

// INT32 Decode

BENCHMARK_CAPTURE(BM_PforDecodeInt32, Constant, &GenConstant<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Sequential, &GenSequential<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, SmallRange, &GenSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, HighBaseSmallRange, &GenHighBaseSmallRange<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, WithOutliers, &GenWithOutliers<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Random, &GenRandom<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsSoldDateSk, &GenTpcdsSoldDateSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsStoreSk, &GenTpcdsStoreSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsItemSk, &GenTpcdsItemSk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, TpcdsQuantity, &GenTpcdsQuantity<int32_t>)
    ->Apply(CustomArgs);

BENCHMARK_CAPTURE(BM_PforDecodeInt32, TrendJitter, &GenTrendJitter<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Sawtooth, &GenSawtooth<int32_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, MeasurementSeries, &GenMeasurement<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, SortedKeys, &GenSortedKeys<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, SensorDropouts, &GenSensorDropouts<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, IdsWithGaps, &GenIdsWithGaps<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, RandomWalk, &GenRandomWalk<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, EventMillis, &GenEventMillis<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, LowSentinel, &GenLowSentinel<int32_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt32, Bimodal, &GenBimodal<int32_t>)->Apply(CustomArgs);

// ======================================================================
// INT64 Encode

BENCHMARK_CAPTURE(BM_PforEncodeInt64, Constant, &GenConstant<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Sequential, &GenSequential<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, SmallRange, &GenSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, HighBaseSmallRange, &GenHighBaseSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, WithOutliers, &GenWithOutliers<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Random, &GenRandom<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, TpcdsSoldDateSk, &GenTpcdsSoldDateSk<int64_t>)
    ->Apply(CustomArgs);

BENCHMARK_CAPTURE(BM_PforEncodeInt64, TrendJitter, &GenTrendJitter<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Sawtooth, &GenSawtooth<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, MeasurementSeries, &GenMeasurement<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, SortedKeys, &GenSortedKeys<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, SensorDropouts, &GenSensorDropouts<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, IdsWithGaps, &GenIdsWithGaps<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, RandomWalk, &GenRandomWalk<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, EventMillis, &GenEventMillis<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, LowSentinel, &GenLowSentinel<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforEncodeInt64, Bimodal, &GenBimodal<int64_t>)->Apply(CustomArgs);

// INT64 Decode

BENCHMARK_CAPTURE(BM_PforDecodeInt64, Constant, &GenConstant<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Sequential, &GenSequential<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, SmallRange, &GenSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, HighBaseSmallRange, &GenHighBaseSmallRange<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, WithOutliers, &GenWithOutliers<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Random, &GenRandom<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, TpcdsSoldDateSk, &GenTpcdsSoldDateSk<int64_t>)
    ->Apply(CustomArgs);

BENCHMARK_CAPTURE(BM_PforDecodeInt64, TrendJitter, &GenTrendJitter<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Sawtooth, &GenSawtooth<int64_t>)->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, MeasurementSeries, &GenMeasurement<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, SortedKeys, &GenSortedKeys<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, SensorDropouts, &GenSensorDropouts<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, IdsWithGaps, &GenIdsWithGaps<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, RandomWalk, &GenRandomWalk<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, EventMillis, &GenEventMillis<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, LowSentinel, &GenLowSentinel<int64_t>)
    ->Apply(CustomArgs);
BENCHMARK_CAPTURE(BM_PforDecodeInt64, Bimodal, &GenBimodal<int64_t>)->Apply(CustomArgs);

}  // namespace
}  // namespace arrow::util::pfor

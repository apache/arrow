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

#include "benchmark/benchmark.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/datum.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x5EA4C42;
constexpr int64_t kNeedleToValueRatio = 4;
constexpr int64_t kValuesRunLength = 16;
constexpr int64_t kNeedlesRunLength = 8;
constexpr int32_t kStringMinLength = 8;
constexpr int32_t kStringMaxLength = 24;

void SetSearchSortedQuickArgs(benchmark::internal::Benchmark* bench) {
  bench->Unit(benchmark::kMicrosecond);
  for (const auto size : std::vector<int64_t>{kL1Size, kL2Size}) {
    bench->Arg(size);
  }
}

void SetSearchSortedArgs(benchmark::internal::Benchmark* bench) {
  bench->Unit(benchmark::kMicrosecond);
  for (const auto size : kMemorySizes) {
    bench->Arg(size);
  }
}

int64_t Int64LengthFromBytes(int64_t size_bytes) {
  return std::max<int64_t>(1, size_bytes / static_cast<int64_t>(sizeof(int64_t)));
}

int64_t NeedleLengthFromBytes(int64_t size_bytes) {
  return std::max<int64_t>(1, Int64LengthFromBytes(size_bytes) / kNeedleToValueRatio);
}

int64_t StringLengthFromBytes(int64_t size_bytes) {
  const int64_t average_length = (kStringMinLength + kStringMaxLength) / 2;
  return std::max<int64_t>(1, size_bytes / average_length);
}

std::shared_ptr<Int64Array> BuildSortedInt64Values(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed);
  const auto length = Int64LengthFromBytes(size_bytes);
  const auto max_value = std::max<int64_t>(length / 8, 1);

  auto values = std::static_pointer_cast<Int64Array>(rand.Int64(length, 0, max_value, 0.0));
  std::vector<int64_t> data(values->raw_values(), values->raw_values() + values->length());
  std::ranges::sort(data);

  Int64Builder builder;
  ABORT_NOT_OK(builder.AppendValues(data));
  return std::static_pointer_cast<Int64Array>(builder.Finish().ValueOrDie());
}

std::shared_ptr<Int64Array> BuildInt64Needles(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed + 1);
  const auto length = NeedleLengthFromBytes(size_bytes);
  const auto max_value = std::max<int64_t>(Int64LengthFromBytes(size_bytes) / 8, 1);
  return std::static_pointer_cast<Int64Array>(rand.Int64(length, 0, max_value, 0.0));
}

std::shared_ptr<StringArray> BuildSortedStringValues(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed + 2);
  const auto length = StringLengthFromBytes(size_bytes);
  auto values = std::static_pointer_cast<StringArray>(
      rand.String(length, kStringMinLength, kStringMaxLength, 0.0));

  std::vector<std::string> data;
  data.reserve(static_cast<size_t>(values->length()));
  for (int64_t index = 0; index < values->length(); ++index) {
    data.push_back(values->GetString(index));
  }
  std::ranges::sort(data);

  StringBuilder builder;
  ABORT_NOT_OK(builder.AppendValues(data));
  return std::static_pointer_cast<StringArray>(builder.Finish().ValueOrDie());
}

std::shared_ptr<StringArray> BuildStringNeedles(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed + 3);
  const auto length = std::max<int64_t>(1, StringLengthFromBytes(size_bytes) / 4);
  return std::static_pointer_cast<StringArray>(
      rand.String(length, kStringMinLength, kStringMaxLength, 0.0));
}

std::shared_ptr<BinaryArray> BuildSortedBinaryValues(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed + 4);
  const auto length = StringLengthFromBytes(size_bytes);
  const auto unique = std::max<int64_t>(1, length / 8);
  auto values = std::static_pointer_cast<BinaryArray>(
      rand.BinaryWithRepeats(length, unique, kStringMinLength, kStringMaxLength, 0.0));

  std::vector<std::string> data;
  data.reserve(static_cast<size_t>(values->length()));
  for (int64_t index = 0; index < values->length(); ++index) {
    data.emplace_back(values->GetView(index));
  }
  std::ranges::sort(data);

  BinaryBuilder builder;
  ABORT_NOT_OK(builder.AppendValues(data));
  return std::static_pointer_cast<BinaryArray>(builder.Finish().ValueOrDie());
}

std::shared_ptr<BinaryArray> BuildBinaryNeedles(int64_t size_bytes) {
  random::RandomArrayGenerator rand(kSeed + 5);
  const auto length = std::max<int64_t>(1, StringLengthFromBytes(size_bytes) / 4);
  const auto unique = std::max<int64_t>(1, length / 2);
  return std::static_pointer_cast<BinaryArray>(
      rand.BinaryWithRepeats(length, unique, kStringMinLength, kStringMaxLength, 0.0));
}

std::shared_ptr<Int64Array> BuildRunHeavyInt64Values(int64_t logical_length,
                                                     int64_t run_length) {
  Int64Builder builder;
  ABORT_NOT_OK(builder.Reserve(logical_length));
  for (int64_t index = 0; index < logical_length; ++index) {
    builder.UnsafeAppend(index / run_length);
  }
  return std::static_pointer_cast<Int64Array>(builder.Finish().ValueOrDie());
}

std::shared_ptr<Int64Array> BuildRunHeavyInt64NeedlesWithNullRuns(int64_t logical_length,
                                                                  int64_t run_length) {
  std::vector<int64_t> data(static_cast<size_t>(logical_length), 0);
  std::vector<bool> is_valid(static_cast<size_t>(logical_length), true);

  for (int64_t index = 0; index < logical_length; ++index) {
    const int64_t run_index = index / run_length;
    if (run_index % 4 == 0) {
      is_valid[static_cast<size_t>(index)] = false;
      continue;
    }
    data[static_cast<size_t>(index)] = run_index / 2;
  }

  Int64Builder builder;
  ABORT_NOT_OK(builder.AppendValues(data, is_valid));
  return std::static_pointer_cast<Int64Array>(builder.Finish().ValueOrDie());
}

std::shared_ptr<Int64Array> BuildInt64NeedlesWithNullRuns(int64_t size_bytes,
                                                          int64_t run_length) {
  return BuildRunHeavyInt64NeedlesWithNullRuns(NeedleLengthFromBytes(size_bytes),
                                               run_length);
}

std::shared_ptr<Array> BuildRunEndEncodedInt64Values(int64_t size_bytes, int64_t run_length) {
  auto values = BuildRunHeavyInt64Values(Int64LengthFromBytes(size_bytes), run_length);
  return RunEndEncode(Datum(values), RunEndEncodeOptions{int32()}).ValueOrDie().make_array();
}

std::shared_ptr<Array> BuildRunEndEncodedInt64Needles(int64_t size_bytes, int64_t run_length) {
  auto needles = BuildRunHeavyInt64Values(NeedleLengthFromBytes(size_bytes), run_length);
  return RunEndEncode(Datum(needles), RunEndEncodeOptions{int32()})
      .ValueOrDie()
      .make_array();
}

std::shared_ptr<Array> BuildRunEndEncodedInt64NeedlesWithNullRuns(int64_t size_bytes,
                                  int64_t run_length) {
  auto needles =
    BuildRunHeavyInt64NeedlesWithNullRuns(NeedleLengthFromBytes(size_bytes), run_length);
  return RunEndEncode(Datum(needles), RunEndEncodeOptions{int32()})
    .ValueOrDie()
    .make_array();
}

void SetBenchmarkCounters(benchmark::State& state, const Datum& values, const Datum& needles) {
  const auto values_length = values.length();
  const auto needles_length = needles.length();
  state.counters["values_length"] = static_cast<double>(values_length);
  state.counters["needles_length"] = static_cast<double>(needles_length);
  state.SetItemsProcessed(state.iterations() * needles_length);
}

void RunSearchSortedBenchmark(benchmark::State& state, const Datum& values,
                              const Datum& needles, SearchSortedOptions::Side side) {
  const SearchSortedOptions options(side);
  for (auto _ : state) {
    auto result = SearchSorted(values, needles, options);
    ABORT_NOT_OK(result.status());
    benchmark::DoNotOptimize(result.ValueUnsafe());
  }
  SetBenchmarkCounters(state, values, needles);
}

static void BM_SearchSortedInt64ArrayNeedles(benchmark::State& state,
                                             SearchSortedOptions::Side side) {
  const Datum values(BuildSortedInt64Values(state.range(0)));
  const Datum needles(BuildInt64Needles(state.range(0)));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedInt64ScalarNeedle(benchmark::State& state,
                                             SearchSortedOptions::Side side) {
  const auto values_array = BuildSortedInt64Values(state.range(0));
  const auto scalar_index = values_array->length() / 2;
  const Datum values(values_array);
  const Datum needles(std::make_shared<Int64Scalar>(values_array->Value(scalar_index)));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedRunEndEncodedValues(benchmark::State& state,
                                               SearchSortedOptions::Side side) {
  const Datum values(BuildRunEndEncodedInt64Values(state.range(0), kValuesRunLength));
  const Datum needles(BuildInt64Needles(state.range(0)));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedRunEndEncodedValuesAndNeedles(
    benchmark::State& state, SearchSortedOptions::Side side) {
  const Datum values(BuildRunEndEncodedInt64Values(state.range(0), kValuesRunLength));
  const Datum needles(BuildRunEndEncodedInt64Needles(state.range(0), kNeedlesRunLength));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedInt64NeedlesWithNullRuns(benchmark::State& state,
                                                    SearchSortedOptions::Side side) {
  const Datum values(BuildSortedInt64Values(state.range(0)));
  const Datum needles(BuildInt64NeedlesWithNullRuns(state.range(0), kNeedlesRunLength));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedRunEndEncodedNeedlesWithNullRuns(
    benchmark::State& state, SearchSortedOptions::Side side) {
  const Datum values(BuildRunEndEncodedInt64Values(state.range(0), kValuesRunLength));
  const Datum needles(
      BuildRunEndEncodedInt64NeedlesWithNullRuns(state.range(0), kNeedlesRunLength));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedStringArrayNeedles(benchmark::State& state,
                                              SearchSortedOptions::Side side) {
  const Datum values(BuildSortedStringValues(state.range(0)));
  const Datum needles(BuildStringNeedles(state.range(0)));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedStringScalarNeedle(benchmark::State& state,
                                              SearchSortedOptions::Side side) {
  const auto values_array = BuildSortedStringValues(state.range(0));
  const auto scalar_index = values_array->length() / 2;
  const Datum values(values_array);
  const Datum needles(std::make_shared<StringScalar>(values_array->GetString(scalar_index)));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedBinaryScalarNeedle(benchmark::State& state,
                                              SearchSortedOptions::Side side) {
  const auto values_array = BuildSortedBinaryValues(state.range(0));
  const auto scalar_index = values_array->length() / 2;
  const Datum values(values_array);
  const Datum needles(std::make_shared<BinaryScalar>(
      std::string(values_array->GetView(scalar_index))));
  RunSearchSortedBenchmark(state, values, needles, side);
}

static void BM_SearchSortedInt64ArrayNeedlesQuick(benchmark::State& state,
                                                  SearchSortedOptions::Side side) {
  BM_SearchSortedInt64ArrayNeedles(state, side);
}

static void BM_SearchSortedRunEndEncodedValuesAndNeedlesQuick(
    benchmark::State& state, SearchSortedOptions::Side side) {
  BM_SearchSortedRunEndEncodedValuesAndNeedles(state, side);
}

static void BM_SearchSortedInt64NeedlesWithNullRunsQuick(
    benchmark::State& state, SearchSortedOptions::Side side) {
  BM_SearchSortedInt64NeedlesWithNullRuns(state, side);
}

static void BM_SearchSortedRunEndEncodedNeedlesWithNullRunsQuick(
    benchmark::State& state, SearchSortedOptions::Side side) {
  BM_SearchSortedRunEndEncodedNeedlesWithNullRuns(state, side);
}

// Primitive-array and REE cases are the main baselines for the kernel TODOs around
// SIMD batched search, vectorized REE writeback, and future parallel needle traversal.

BENCHMARK_CAPTURE(BM_SearchSortedInt64ArrayNeedles, left, SearchSortedOptions::Left)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64ArrayNeedles, right, SearchSortedOptions::Right)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64ScalarNeedle, left, SearchSortedOptions::Left)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64ScalarNeedle, right, SearchSortedOptions::Right)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedValues, left, SearchSortedOptions::Left)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedValues, right, SearchSortedOptions::Right)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedValuesAndNeedles, left,
                  SearchSortedOptions::Left)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedValuesAndNeedles, right,
                  SearchSortedOptions::Right)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64NeedlesWithNullRuns, left,
          SearchSortedOptions::Left)
  ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64NeedlesWithNullRuns, right,
          SearchSortedOptions::Right)
  ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedNeedlesWithNullRuns, left,
          SearchSortedOptions::Left)
  ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedNeedlesWithNullRuns, right,
          SearchSortedOptions::Right)
  ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedStringArrayNeedles, left, SearchSortedOptions::Left)
    ->Apply(SetSearchSortedArgs);
BENCHMARK_CAPTURE(BM_SearchSortedStringArrayNeedles, right, SearchSortedOptions::Right)
    ->Apply(SetSearchSortedArgs);

// String and binary scalar cases specifically exercise the direct scalar fast path that
// avoids boxing a scalar needle into a temporary one-element array.
BENCHMARK_CAPTURE(BM_SearchSortedStringScalarNeedle, left, SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedStringScalarNeedle, right, SearchSortedOptions::Right)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedBinaryScalarNeedle, left, SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedBinaryScalarNeedle, right, SearchSortedOptions::Right)
  ->Apply(SetSearchSortedQuickArgs);

// Lightweight L1/L2 regressions keep a fast local loop for future optimization work.
BENCHMARK_CAPTURE(BM_SearchSortedInt64ArrayNeedlesQuick, left, SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedInt64NeedlesWithNullRunsQuick, left,
                  SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedValuesAndNeedlesQuick, left,
          SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);
BENCHMARK_CAPTURE(BM_SearchSortedRunEndEncodedNeedlesWithNullRunsQuick, left,
                  SearchSortedOptions::Left)
  ->Apply(SetSearchSortedQuickArgs);

}  // namespace compute
}  // namespace arrow
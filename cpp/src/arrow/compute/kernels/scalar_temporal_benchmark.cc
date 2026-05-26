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

#include <functional>

#include "benchmark/benchmark.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {

namespace compute {

constexpr auto kSeed = 0x94378165;

std::vector<int64_t> g_data_sizes = {kL2Size};
static constexpr int64_t kInt64Min = -2000000000;  // 1906-08-16 20:26:40
static constexpr int64_t kInt64Max = 2000000000;   // 2033-05-18 03:33:20

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
    bench->Args({static_cast<ArgsType>(kL2Size), inverse_null_proportion});
  }
}

using UnaryRoundingOp = Result<Datum>(const Datum&, const RoundTemporalOptions,
                                      ExecContext*);
using UnaryOp = Result<Datum>(const Datum&, ExecContext*);
using BinaryOp = Result<Datum>(const Datum&, const Datum&, ExecContext*);

template <UnaryRoundingOp& Op, std::shared_ptr<DataType>& timestamp_type,
          RoundTemporalOptions& options>
static void BenchmarkTemporalRounding(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array =
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion);
  EXPECT_OK_AND_ASSIGN(auto timestamp_array, array->View(timestamp_type));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(timestamp_array, options, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

template <UnaryOp& Op, std::shared_ptr<DataType>& timestamp_type>
static void BenchmarkTemporal(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array =
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion);
  EXPECT_OK_AND_ASSIGN(auto timestamp_array, array->View(timestamp_type));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(timestamp_array, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

template <BinaryOp& Op, std::shared_ptr<DataType>& timestamp_type>
static void BenchmarkTemporalBinary(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(timestamp_type);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.ArrayOf(timestamp_type, args.size, args.null_proportion);
  auto rhs = rand.ArrayOf(timestamp_type, args.size, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, rhs, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

template <std::shared_ptr<DataType>& timestamp_type>
static void BenchmarkStrftime(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array =
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion);
  EXPECT_OK_AND_ASSIGN(auto timestamp_array, array->View(timestamp_type));

  auto options = StrftimeOptions();
  for (auto _ : state) {
    ABORT_NOT_OK(Strftime(timestamp_array, options, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

template <std::shared_ptr<DataType>& timestamp_type>
static void BenchmarkStrptime(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array =
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion);
  EXPECT_OK_AND_ASSIGN(auto timestamp_array, array->View(timestamp_type));
  auto strftime_options = StrftimeOptions("%Y-%m-%dT%H:%M:%S");
  EXPECT_OK_AND_ASSIGN(auto string_array,
                       Strftime(timestamp_array, strftime_options, ctx));
  auto strptime_options = StrptimeOptions("%Y-%m-%dT%H:%M:%S", TimeUnit::MICRO, true);

  for (auto _ : state) {
    ABORT_NOT_OK(Strptime(string_array, strptime_options, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

static void BenchmarkAssumeTimezone(benchmark::State& state) {
  RegressionArgs args(state);
  ExecContext* ctx = default_exec_context();

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array =
      rand.Numeric<Int64Type>(array_size, kInt64Min, kInt64Max, args.null_proportion);
  EXPECT_OK_AND_ASSIGN(auto timestamp_array, array->View(timestamp(TimeUnit::NANO)));

  auto options = AssumeTimezoneOptions(
      "Pacific/Marquesas", AssumeTimezoneOptions::Ambiguous::AMBIGUOUS_LATEST,
      AssumeTimezoneOptions::Nonexistent::NONEXISTENT_EARLIEST);
  for (auto _ : state) {
    ABORT_NOT_OK(AssumeTimezone(timestamp_array, options, ctx));
  }

  state.SetItemsProcessed(state.iterations() * array_size);
}

auto zoned = timestamp(TimeUnit::NANO, "Pacific/Marquesas");
auto non_zoned = timestamp(TimeUnit::NANO);
auto time32_type = time32(TimeUnit::MILLI);
auto time64_type = time64(TimeUnit::NANO);
auto date32_type = date32();
auto date64_type = date64();

#define DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(OPTIONS)                              \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, CeilTemporal, zoned, OPTIONS)      \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, FloorTemporal, zoned, OPTIONS)     \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, RoundTemporal, zoned, OPTIONS)     \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, CeilTemporal, non_zoned, OPTIONS)  \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, FloorTemporal, non_zoned, OPTIONS) \
      ->Apply(SetArgs);                                                            \
  BENCHMARK_TEMPLATE(BenchmarkTemporalRounding, RoundTemporal, non_zoned, OPTIONS) \
      ->Apply(SetArgs);

#define DECLARE_TEMPORAL_BENCHMARKS(OP)                                 \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, non_zoned)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, zoned)->Apply(SetArgs);

#define DECLARE_TEMPORAL_BENCHMARKS_ZONED(OP) \
  BENCHMARK_TEMPLATE(BenchmarkTemporal, OP, zoned)->Apply(SetArgs);

#define DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(OP)             \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, non_zoned)->Apply(SetArgs);   \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, zoned)->Apply(SetArgs);       \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, date64_type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, date32_type)->Apply(SetArgs);

#define DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(OP)       \
  DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(OP);                  \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, time32_type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BenchmarkTemporalBinary, OP, time64_type)->Apply(SetArgs);

// Temporal rounding benchmarks
auto round_1_minute = RoundTemporalOptions(1, CalendarUnit::MINUTE);
auto round_10_minute = RoundTemporalOptions(10, CalendarUnit::MINUTE);
auto round_1_week = RoundTemporalOptions(1, CalendarUnit::WEEK);
auto round_10_week = RoundTemporalOptions(10, CalendarUnit::WEEK);
auto round_1_month = RoundTemporalOptions(1, CalendarUnit::MONTH);
auto round_10_month = RoundTemporalOptions(10, CalendarUnit::MONTH);

DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_minute);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_week);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_1_month);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_minute);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_week);
DECLARE_TEMPORAL_ROUNDING_BENCHMARKS(round_10_month);

// Temporal component extraction
DECLARE_TEMPORAL_BENCHMARKS(Year);
DECLARE_TEMPORAL_BENCHMARKS(IsLeapYear);
DECLARE_TEMPORAL_BENCHMARKS(Month);
DECLARE_TEMPORAL_BENCHMARKS(Day);
DECLARE_TEMPORAL_BENCHMARKS(DayOfYear);
DECLARE_TEMPORAL_BENCHMARKS_ZONED(IsDaylightSavings);
DECLARE_TEMPORAL_BENCHMARKS(USYear);
DECLARE_TEMPORAL_BENCHMARKS(ISOYear);
DECLARE_TEMPORAL_BENCHMARKS(ISOWeek);
DECLARE_TEMPORAL_BENCHMARKS(USWeek);
DECLARE_TEMPORAL_BENCHMARKS(Quarter);
DECLARE_TEMPORAL_BENCHMARKS(Hour);
DECLARE_TEMPORAL_BENCHMARKS(Minute);
DECLARE_TEMPORAL_BENCHMARKS(Second);
DECLARE_TEMPORAL_BENCHMARKS(Millisecond);
DECLARE_TEMPORAL_BENCHMARKS(Microsecond);
DECLARE_TEMPORAL_BENCHMARKS(Nanosecond);
DECLARE_TEMPORAL_BENCHMARKS(Subsecond);

// Other temporal benchmarks
BENCHMARK_TEMPLATE(BenchmarkStrftime, non_zoned)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchmarkStrftime, zoned)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchmarkStrptime, non_zoned)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchmarkStrptime, zoned)->Apply(SetArgs);
BENCHMARK(BenchmarkAssumeTimezone)->Apply(SetArgs);

// binary temporal benchmarks
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(YearsBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(QuartersBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(MonthsBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(MonthDayNanoBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(WeeksBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(DayTimeBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_AND_TIMESTAMPS(DaysBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(HoursBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(MinutesBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(SecondsBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(MillisecondsBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(MicrosecondsBetween);
DECLARE_TEMPORAL_BINARY_BENCHMARKS_DATES_TIMES_AND_TIMESTAMPS(NanosecondsBetween);
}  // namespace compute
}  // namespace arrow

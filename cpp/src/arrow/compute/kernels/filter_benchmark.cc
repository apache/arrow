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

#include "arrow/compute/kernels/filter.h"

#include "arrow/compute/benchmark_util.h"
#include "arrow/compute/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;

static void FilterInt64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));
  auto filter = std::static_pointer_cast<BooleanArray>(
      rand.Boolean(array_size, 0.75, args.null_proportion));

  FunctionContext ctx;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Filter(&ctx, Datum(array), Datum(filter), &out));
    benchmark::DoNotOptimize(out);
  }
}

static void FilterFixedSizeList1Int64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto int_array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));
  auto array = std::make_shared<FixedSizeListArray>(
      fixed_size_list(int64(), 1), array_size, int_array, int_array->null_bitmap(),
      int_array->null_count());
  auto filter = std::static_pointer_cast<BooleanArray>(
      rand.Boolean(array_size, 0.75, args.null_proportion));

  FunctionContext ctx;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Filter(&ctx, Datum(array), Datum(filter), &out));
    benchmark::DoNotOptimize(out);
  }
}

static void FilterString(benchmark::State& state) {
  RegressionArgs args(state);

  int32_t string_min_length = 0, string_max_length = 128;
  int32_t string_mean_length = (string_max_length + string_min_length) / 2;
  // for an array of 50% null strings, we need to generate twice as many strings
  // to ensure that they have an average of args.size total characters
  auto array_size =
      static_cast<int64_t>(args.size / string_mean_length / (1 - args.null_proportion));

  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = std::static_pointer_cast<StringArray>(rand.String(
      array_size, string_min_length, string_max_length, args.null_proportion));
  auto filter = std::static_pointer_cast<BooleanArray>(
      rand.Boolean(array_size, 0.75, args.null_proportion));

  FunctionContext ctx;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Filter(&ctx, Datum(array), Datum(filter), &out));
    benchmark::DoNotOptimize(out);
  }
}

BENCHMARK(FilterInt64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(FilterFixedSizeList1Int64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(FilterString)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

}  // namespace compute
}  // namespace arrow

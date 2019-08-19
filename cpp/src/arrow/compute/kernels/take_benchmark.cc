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

#include "arrow/compute/kernels/take.h"

#include "arrow/compute/benchmark_util.h"
#include "arrow/compute/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;

static void TakeBenchmark(benchmark::State& state, const std::shared_ptr<Array>& values,
                          const std::shared_ptr<Array>& indices) {
  FunctionContext ctx;
  TakeOptions options;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Take(&ctx, Datum(values), Datum(indices), options, &out));
    benchmark::DoNotOptimize(out);
  }
}

static void TakeInt64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto values = rand.Int64(array_size, -100, 100, args.null_proportion);

  auto indices = rand.Int32(static_cast<int32_t>(array_size), 0,
                            static_cast<int32_t>(array_size - 1), args.null_proportion);

  TakeBenchmark(state, values, indices);
}

static void TakeFixedSizeList1Int64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto int_array = rand.Int64(array_size, -100, 100, args.null_proportion);
  auto values = std::make_shared<FixedSizeListArray>(
      fixed_size_list(int64(), 1), array_size, int_array, int_array->null_bitmap(),
      int_array->null_count());

  auto indices = rand.Int32(static_cast<int32_t>(array_size), 0,
                            static_cast<int32_t>(array_size - 1), args.null_proportion);

  TakeBenchmark(state, values, indices);
}

static void TakeInt64VsFilter(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto values = rand.Int64(array_size, -100, 100, args.null_proportion);

  auto filter = std::static_pointer_cast<BooleanArray>(
      rand.Boolean(array_size, 0.75, args.null_proportion));

  Int32Builder indices_builder;
  ABORT_NOT_OK(indices_builder.Resize(array_size));

  for (int64_t i = 0; i < array_size; ++i) {
    if (filter->IsNull(i)) {
      indices_builder.UnsafeAppendNull();
    } else if (filter->Value(i)) {
      indices_builder.UnsafeAppend(static_cast<int32_t>(i));
    }
  }

  std::shared_ptr<Array> indices;
  ABORT_NOT_OK(indices_builder.Finish(&indices));
  TakeBenchmark(state, values, indices);
}

static void TakeString(benchmark::State& state) {
  RegressionArgs args(state);

  int32_t string_min_length = 0, string_max_length = 128;
  int32_t string_mean_length = (string_max_length + string_min_length) / 2;
  // for an array of 50% null strings, we need to generate twice as many strings
  // to ensure that they have an average of args.size total characters
  auto array_size =
      static_cast<int64_t>(args.size / string_mean_length / (1 - args.null_proportion));

  auto rand = random::RandomArrayGenerator(kSeed);
  auto values = std::static_pointer_cast<StringArray>(rand.String(
      array_size, string_min_length, string_max_length, args.null_proportion));

  auto indices = rand.Int32(static_cast<int32_t>(array_size), 0,
                            static_cast<int32_t>(array_size - 1), args.null_proportion);

  TakeBenchmark(state, values, indices);
}

BENCHMARK(TakeInt64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TakeFixedSizeList1Int64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TakeInt64VsFilter)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TakeString)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 1})
    ->Args({1 << 23, 1})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {
constexpr auto kSeed = 0x0ff1ce;

static void ArraySortIndicesBenchmark(benchmark::State& state,
                                      const std::shared_ptr<Array>& values) {
  for (auto _ : state) {
    ABORT_NOT_OK(SortIndices(*values).status());
  }
  state.SetItemsProcessed(state.iterations() * values->length());
}

static void ArraySortIndicesInt64Count(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto values = rand.Int64(array_size, -100, 100, args.null_proportion);

  ArraySortIndicesBenchmark(state, values);
}

static void ArraySortIndicesInt64Compare(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto values = rand.Int64(array_size, min, max, args.null_proportion);

  ArraySortIndicesBenchmark(state, values);
}

static void TableSortIndicesBenchmark(benchmark::State& state,
                                      const std::shared_ptr<Table>& table,
                                      const SortOptions& options) {
  for (auto _ : state) {
    ABORT_NOT_OK(SortIndices(*table, options).status());
  }
  state.SetItemsProcessed(state.iterations() * table->num_rows());
}

static void TableSortIndicesInt64Count(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto values = rand.Int64(array_size, -100, 100, args.null_proportion);
  std::vector<std::shared_ptr<Field>> fields = {{field("int64", int64())}};
  auto table = Table::Make(schema(fields), {values}, array_size);
  SortOptions options({SortKey("int64", SortOrder::Ascending)});

  TableSortIndicesBenchmark(state, table, options);
}

static void TableSortIndicesInt64Compare(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto values = rand.Int64(array_size, min, max, args.null_proportion);
  std::vector<std::shared_ptr<Field>> fields = {{field("int64", int64())}};
  auto table = Table::Make(schema(fields), {values}, array_size);
  SortOptions options({SortKey("int64", SortOrder::Ascending)});

  TableSortIndicesBenchmark(state, table, options);
}

static void TableSortIndicesInt64Int64(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);

  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto values1 = rand.Int64(array_size, min, max, args.null_proportion);
  auto values2 = rand.Int64(array_size, min, max, args.null_proportion);
  std::vector<std::shared_ptr<Field>> fields = {
      {field("int64-1", int64())},
      {field("int64-2", int64())},
  };
  auto table = Table::Make(schema(fields), {values1, values2}, array_size);
  SortOptions options({
      SortKey("int64-1", SortOrder::Ascending),
      SortKey("int64-2", SortOrder::Ascending),
  });

  TableSortIndicesBenchmark(state, table, options);
}

BENCHMARK(ArraySortIndicesInt64Count)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(ArraySortIndicesInt64Compare)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TableSortIndicesInt64Count)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TableSortIndicesInt64Compare)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TableSortIndicesInt64Int64)
    ->Apply(RegressionSetArgs)
    ->Args({1 << 20, 100})
    ->Args({1 << 23, 100})
    ->MinTime(1.0)
    ->Unit(benchmark::TimeUnit::kNanosecond);

}  // namespace compute
}  // namespace arrow

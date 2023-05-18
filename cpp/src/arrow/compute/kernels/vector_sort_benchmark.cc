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
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;
constexpr int32_t kDictionarySize = 24;  // a typical dictionary size

//
// Array sort/rank benchmark helpers
//

struct SortRunner {
  explicit SortRunner(benchmark::State&) {}

  Status operator()(const std::shared_ptr<Array>& values) const {
    return SortIndices(*values).status();
  }
  Status operator()(const std::shared_ptr<ChunkedArray>& values) const {
    return SortIndices(*values).status();
  }
};

struct RankRunner {
  explicit RankRunner(benchmark::State& state) {
    options = RankOptions::Defaults();
    options.tiebreaker = static_cast<RankOptions::Tiebreaker>(state.range(2));
  }

  RankOptions options;

  Status operator()(const std::shared_ptr<Array>& values) const {
    return CallFunction("rank", {values}, &options).status();
  }

  Status operator()(const std::shared_ptr<ChunkedArray>& values) const {
    return CallFunction("rank", {values}, &options).status();
  }
};

template <typename Runner, typename ArrayLike>
static void ArraySortFuncBenchmark(benchmark::State& state, const Runner& runner,
                                   const std::shared_ptr<ArrayLike>& values) {
  for (auto _ : state) {
    ABORT_NOT_OK(runner(values));
  }
  state.SetItemsProcessed(state.iterations() * values->length());
}

// Compute the number of values in a StringArray based on a target `num_bytes`
static int64_t GetStringArraySize(int64_t num_bytes, int32_t min_length,
                                  int32_t max_length, double null_proportion,
                                  int num_chunks = 1) {
  // The array_size is derived from these two equations:
  //
  //     num_bytes = array_size * (1.0 - null_proportion) * string_mean_length
  //     string_mean_length = (max_length + min_length) / 2.0
  const double valid_proportion = 1.0 - null_proportion;
  const double array_size =
      (valid_proportion > std::numeric_limits<double>::epsilon())
          ? (num_bytes * 2) / (valid_proportion * (max_length + min_length))
          : (num_bytes * 2) / (max_length + min_length);
  return static_cast<int64_t>(std::round(array_size / num_chunks));
}

template <typename Runner>
static void ArraySortFuncInt64Benchmark(benchmark::State& state, const Runner& runner,
                                        int64_t min, int64_t max) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto values = rand.Int64(array_size, min, max, args.null_proportion);

  ArraySortFuncBenchmark(state, runner, values);
}

template <typename Runner>
static void ArraySortFuncInt64DictBenchmark(benchmark::State& state, const Runner& runner,
                                            int64_t min, int64_t max, int32_t dict_size) {
  RegressionArgs args(state);

  double null_proportion = args.null_proportion;
  double values_null_proportion = null_proportion / 2;
  if (1.0 - null_proportion < std::numeric_limits<double>::epsilon()) {
    values_null_proportion = null_proportion = 1.0;
  }
  double indices_null_proportion = null_proportion - values_null_proportion;

  const int64_t array_size = args.size / sizeof(int64_t);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto dict_values = rand.Int64(dict_size, min, max, values_null_proportion);
  auto dict_indices = rand.Int64(array_size, 0, dict_size - 1, indices_null_proportion);
  auto dict_array = *DictionaryArray::FromArrays(dict_indices, dict_values);

  ArraySortFuncBenchmark(state, runner, dict_array);
}

template <typename Runner>
static void ArraySortFuncStringBenchmark(benchmark::State& state, const Runner& runner,
                                         int32_t min_length, int32_t max_length) {
  RegressionArgs args(state);

  const int64_t array_size =
      GetStringArraySize(args.size, min_length, max_length, args.null_proportion);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto values = rand.String(array_size, min_length, max_length, args.null_proportion);

  ArraySortFuncBenchmark(state, runner, values);
}

template <typename Runner>
static void ArraySortFuncStringDictBenchmark(benchmark::State& state,
                                             const Runner& runner, int32_t min_length,
                                             int32_t max_length, int32_t dict_size) {
  RegressionArgs args(state);

  double null_proportion = args.null_proportion;
  double values_null_proportion = null_proportion / 2;
  if (1.0 - null_proportion < std::numeric_limits<double>::epsilon()) {
    values_null_proportion = null_proportion = 1.0;
  }
  double indices_null_proportion = null_proportion - values_null_proportion;

  const int64_t array_size =
      GetStringArraySize(args.size, min_length, max_length, null_proportion);

  auto rand = random::RandomArrayGenerator(kSeed);
  auto dict_values =
      rand.String(dict_size, min_length, max_length, values_null_proportion);
  auto dict_indices = rand.Int64(array_size, 0, dict_size - 1, indices_null_proportion);
  auto dict_array = *DictionaryArray::FromArrays(dict_indices, dict_values);

  ArraySortFuncBenchmark(state, runner, dict_array);
}

template <typename Runner>
static void ArraySortFuncBoolBenchmark(benchmark::State& state, const Runner& runner) {
  RegressionArgs args(state);

  const int64_t array_size = args.size * 8;
  auto rand = random::RandomArrayGenerator(kSeed);
  auto values = rand.Boolean(array_size, 0.5, args.null_proportion);

  ArraySortFuncBenchmark(state, runner, values);
}

template <typename Runner>
static void ChunkedArraySortFuncInt64Benchmark(benchmark::State& state,
                                               const Runner& runner, int64_t min,
                                               int64_t max) {
  RegressionArgs args(state);

  const int64_t n_chunks = 10;
  const int64_t array_size = args.size / n_chunks / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  ArrayVector chunks;
  for (int64_t i = 0; i < n_chunks; ++i) {
    chunks.push_back(rand.Int64(array_size, min, max, args.null_proportion));
  }

  ArraySortFuncBenchmark(state, runner, std::make_shared<ChunkedArray>(chunks));
}

template <typename Runner>
static void ChunkedArraySortFuncStringBenchmark(benchmark::State& state,
                                                const Runner& runner, int32_t min_length,
                                                int32_t max_length) {
  RegressionArgs args(state);

  const auto n_chunks = 10;
  const auto array_chunk_size = GetStringArraySize(args.size, min_length, max_length,
                                                   args.null_proportion, n_chunks);
  auto rand = random::RandomArrayGenerator(kSeed);

  ArrayVector chunks;
  for (auto i = 0; i < n_chunks; ++i) {
    auto values =
        rand.String(array_chunk_size, min_length, max_length, args.null_proportion);
    chunks.push_back(std::move(values));
  }

  ArraySortFuncBenchmark(state, runner, std::make_shared<ChunkedArray>(chunks));
}

static void ArraySortIndicesInt64Narrow(benchmark::State& state) {
  ArraySortFuncInt64Benchmark(state, SortRunner(state), -100, 100);
}

static void ArrayRankInt64Narrow(benchmark::State& state) {
  ArraySortFuncInt64Benchmark(state, RankRunner(state), -100, 100);
}

static void ChunkedArrayRankInt64Narrow(benchmark::State& state) {
  ChunkedArraySortFuncInt64Benchmark(state, RankRunner(state), -100, 100);
}

static void ArraySortIndicesInt64Wide(benchmark::State& state) {
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  ArraySortFuncInt64Benchmark(state, SortRunner(state), min, max);
}

static void ArraySortIndicesInt64WideDict(benchmark::State& state) {
  const auto dict_size = kDictionarySize;
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  ArraySortFuncInt64DictBenchmark(state, SortRunner(state), min, max, dict_size);
}

static void ArrayRankInt64Wide(benchmark::State& state) {
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  ArraySortFuncInt64Benchmark(state, RankRunner(state), min, max);
}

static void ChunkedArrayRankInt64Wide(benchmark::State& state) {
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  ChunkedArraySortFuncInt64Benchmark(state, RankRunner(state), min, max);
}

static void ArraySortIndicesBool(benchmark::State& state) {
  ArraySortFuncBoolBenchmark(state, SortRunner(state));
}

static void ArraySortIndicesStringNarrow(benchmark::State& state) {
  const auto min_length = 0;
  const auto max_length = 16;
  ArraySortFuncStringBenchmark(state, SortRunner(state), min_length, max_length);
}

static void ArraySortIndicesStringWide(benchmark::State& state) {
  const auto min_length = 0;
  const auto max_length = 64;
  ArraySortFuncStringBenchmark(state, SortRunner(state), min_length, max_length);
}

static void ArraySortIndicesStringWideDict(benchmark::State& state) {
  const auto dict_size = kDictionarySize;
  const auto min_length = 0;
  const auto max_length = 64;
  ArraySortFuncStringDictBenchmark(state, SortRunner(state), min_length, max_length,
                                   dict_size);
}

static void ArrayRankStringNarrow(benchmark::State& state) {
  const auto min_length = 0;
  const auto max_length = 16;
  ArraySortFuncStringBenchmark(state, RankRunner(state), min_length, max_length);
}

static void ArrayRankStringWide(benchmark::State& state) {
  const auto min_length = 0;
  const auto max_length = 64;
  ArraySortFuncStringBenchmark(state, RankRunner(state), min_length, max_length);
}

static void ChunkedArraySortIndicesInt64Narrow(benchmark::State& state) {
  ChunkedArraySortFuncInt64Benchmark(state, SortRunner(state), -100, 100);
}

static void ChunkedArraySortIndicesInt64Wide(benchmark::State& state) {
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  ChunkedArraySortFuncInt64Benchmark(state, SortRunner(state), min, max);
}

static void ChunkedArraySortIndicesString(benchmark::State& state) {
  const auto min_length = 0;
  const auto max_length = 32;
  ChunkedArraySortFuncStringBenchmark(state, SortRunner(state), min_length, max_length);
}

//
// Record batch and table sort benchmark helpers
//

static void DatumSortIndicesBenchmark(benchmark::State& state, const Datum& datum,
                                      const SortOptions& options) {
  for (auto _ : state) {
    ABORT_NOT_OK(SortIndices(datum, options).status());
  }
}

// Extract benchmark args from benchmark::State
struct RecordBatchSortIndicesArgs {
  // the number of records
  const int64_t num_records;

  // proportion of nulls in generated arrays
  const double null_proportion;

  // the number of columns
  const int64_t num_columns;

  // Extract args
  explicit RecordBatchSortIndicesArgs(benchmark::State& state)
      : num_records(state.range(0)),
        null_proportion(ComputeNullProportion(state.range(1))),
        num_columns(state.range(2)),
        state_(state) {}

  ~RecordBatchSortIndicesArgs() {
    state_.counters["columns"] = static_cast<double>(num_columns);
    state_.counters["null_percent"] = null_proportion * 100;
    state_.SetItemsProcessed(state_.iterations() * num_records);
  }

 protected:
  double ComputeNullProportion(int64_t inverse_null_proportion) {
    if (inverse_null_proportion == 0) {
      return 0.0;
    } else {
      return std::min(1., 1. / static_cast<double>(inverse_null_proportion));
    }
  }

  benchmark::State& state_;
};

struct TableSortIndicesArgs : public RecordBatchSortIndicesArgs {
  // the number of chunks in each generated column
  const int64_t num_chunks;

  // Extract args
  explicit TableSortIndicesArgs(benchmark::State& state)
      : RecordBatchSortIndicesArgs(state), num_chunks(state.range(3)) {}

  ~TableSortIndicesArgs() { state_.counters["chunks"] = static_cast<double>(num_chunks); }
};

struct BatchOrTableBenchmarkData {
  std::shared_ptr<Schema> schema;
  std::vector<SortKey> sort_keys;
  ChunkedArrayVector columns;
};

BatchOrTableBenchmarkData MakeBatchOrTableBenchmarkDataInt64(
    const RecordBatchSortIndicesArgs& args, int64_t num_chunks, int64_t min_value,
    int64_t max_value) {
  auto rand = random::RandomArrayGenerator(kSeed);
  FieldVector fields;
  BatchOrTableBenchmarkData data;

  for (int64_t i = 0; i < args.num_columns; ++i) {
    auto name = std::to_string(i);
    fields.push_back(field(name, int64()));
    auto order = (i % 2) == 0 ? SortOrder::Ascending : SortOrder::Descending;
    data.sort_keys.emplace_back(name, order);
    ArrayVector chunks;
    if ((args.num_records % num_chunks) != 0) {
      Status::Invalid("The number of chunks (", num_chunks,
                      ") must be "
                      "a multiple of the number of records (",
                      args.num_records, ")")
          .Abort();
    }
    auto num_records_in_array = args.num_records / num_chunks;
    for (int64_t j = 0; j < num_chunks; ++j) {
      chunks.push_back(
          rand.Int64(num_records_in_array, min_value, max_value, args.null_proportion));
    }
    ASSIGN_OR_ABORT(auto chunked_array, ChunkedArray::Make(chunks, int64()));
    data.columns.push_back(chunked_array);
  }

  data.schema = schema(fields);
  return data;
}

static void RecordBatchSortIndicesInt64(benchmark::State& state, int64_t min,
                                        int64_t max) {
  RecordBatchSortIndicesArgs args(state);

  auto data = MakeBatchOrTableBenchmarkDataInt64(args, /*num_chunks=*/1, min, max);
  ArrayVector columns;
  for (const auto& chunked : data.columns) {
    ARROW_CHECK_EQ(chunked->num_chunks(), 1);
    columns.push_back(chunked->chunk(0));
  }

  auto batch = RecordBatch::Make(data.schema, args.num_records, columns);
  SortOptions options(data.sort_keys);
  DatumSortIndicesBenchmark(state, Datum(*batch), options);
}

static void TableSortIndicesInt64(benchmark::State& state, int64_t min, int64_t max) {
  TableSortIndicesArgs args(state);

  auto data = MakeBatchOrTableBenchmarkDataInt64(args, args.num_chunks, min, max);
  auto table = Table::Make(data.schema, data.columns, args.num_records);
  SortOptions options(data.sort_keys);
  DatumSortIndicesBenchmark(state, Datum(*table), options);
}

static void RecordBatchSortIndicesInt64Narrow(benchmark::State& state) {
  RecordBatchSortIndicesInt64(state, -100, 100);
}

static void RecordBatchSortIndicesInt64Wide(benchmark::State& state) {
  RecordBatchSortIndicesInt64(state, std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max());
}

static void TableSortIndicesInt64Narrow(benchmark::State& state) {
  TableSortIndicesInt64(state, -100, 100);
}

static void TableSortIndicesInt64Wide(benchmark::State& state) {
  TableSortIndicesInt64(state, std::numeric_limits<int64_t>::min(),
                        std::numeric_limits<int64_t>::max());
}

//
// Sort benchmark declarations
//

void ArraySortIndicesSetArgs(benchmark::internal::Benchmark* bench) {
  // 2 benchmark arguments: size, inverse null proportion
  bench->Unit(benchmark::kNanosecond);
  bench->Apply(RegressionSetArgs);
  bench->Args({1 << 20, 100});
  bench->Args({1 << 23, 100});
}

BENCHMARK(ArraySortIndicesInt64Narrow)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesInt64Wide)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesInt64WideDict)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesBool)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesStringNarrow)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesStringWide)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ArraySortIndicesStringWideDict)->Apply(ArraySortIndicesSetArgs);

BENCHMARK(ChunkedArraySortIndicesInt64Narrow)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ChunkedArraySortIndicesInt64Wide)->Apply(ArraySortIndicesSetArgs);
BENCHMARK(ChunkedArraySortIndicesString)->Apply(ArraySortIndicesSetArgs);

BENCHMARK(RecordBatchSortIndicesInt64Narrow)
    ->ArgsProduct({
        {1 << 20},      // the number of records
        {100, 4, 0},    // inverse null proportion
        {16, 8, 2, 1},  // the number of columns
    })
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(RecordBatchSortIndicesInt64Wide)
    ->ArgsProduct({
        {1 << 20},      // the number of records
        {100, 4, 0},    // inverse null proportion
        {16, 8, 2, 1},  // the number of columns
    })
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TableSortIndicesInt64Narrow)
    ->ArgsProduct({
        {1 << 20},      // the number of records
        {100, 4, 0},    // inverse null proportion
        {16, 8, 2, 1},  // the number of columns
        {32, 4, 1},     // the number of chunks
    })
    ->Unit(benchmark::TimeUnit::kNanosecond);

BENCHMARK(TableSortIndicesInt64Wide)
    ->ArgsProduct({
        {1 << 20},      // the number of records
        {100, 4, 0},    // inverse null proportion
        {16, 8, 2, 1},  // the number of columns
        {32, 4, 1},     // the number of chunks
    })
    ->Unit(benchmark::TimeUnit::kNanosecond);

//
// Rank benchmark declarations
//

void ArrayRankSetArgs(benchmark::internal::Benchmark* bench) {
  // 3 benchmark arguments: size, inverse null proportion, rank tiebreaker
  bench->Unit(benchmark::kNanosecond);
  bench->ArgNames({"", "", "tiebreaker"});

  // Use only a subset of kInverseNullProportions as the cartesian product of
  // arguments is large already.
  const std::vector<ArgsType> inverse_null_proportions{10, 1, 0};
  // Don't bother with Max as it should have the same perf as Min
  const std::vector<RankOptions::Tiebreaker> tie_breakers{
      RankOptions::Min, RankOptions::First, RankOptions::Dense};

  for (const auto inverse_null_proportion : kInverseNullProportions) {
    for (const auto tie_breaker : tie_breakers) {
      bench->Args({static_cast<ArgsType>(kL1Size), inverse_null_proportion,
                   static_cast<ArgsType>(tie_breaker)});
    }
  }
  for (const auto tie_breaker : tie_breakers) {
    bench->Args({1 << 20, 100, static_cast<ArgsType>(tie_breaker)});
  }
  for (const auto tie_breaker : tie_breakers) {
    bench->Args({1 << 23, 100, static_cast<ArgsType>(tie_breaker)});
  }
}

BENCHMARK(ArrayRankInt64Narrow)->Apply(ArrayRankSetArgs);
BENCHMARK(ArrayRankInt64Wide)->Apply(ArrayRankSetArgs);
BENCHMARK(ArrayRankStringNarrow)->Apply(ArrayRankSetArgs);
BENCHMARK(ArrayRankStringWide)->Apply(ArrayRankSetArgs);
BENCHMARK(ChunkedArrayRankInt64Narrow)->Apply(ArrayRankSetArgs);
BENCHMARK(ChunkedArrayRankInt64Wide)->Apply(ArrayRankSetArgs);

}  // namespace compute
}  // namespace arrow

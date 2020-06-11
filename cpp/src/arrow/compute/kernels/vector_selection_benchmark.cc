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
#include "arrow/compute/benchmark_util.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;

struct FilterParams {
  // proportion of nulls in the values array
  const double values_null_proportion;

  // proportion of true in filter
  const double selected_proportion;

  // proportion of nulls in the filter
  const double filter_null_proportion;
};

std::vector<int64_t> g_data_sizes = {kL2Size};

// The benchmark state parameter references this vector of cases. Test high and
// low selectivity filters.
std::vector<FilterParams> g_filter_params = {
    {0., 0.95, 0.05},   {0., 0.10, 0.05},   {0.001, 0.95, 0.05}, {0.001, 0.10, 0.05},
    {0.01, 0.95, 0.05}, {0.01, 0.10, 0.05}, {0.1, 0.95, 0.05},   {0.1, 0.10, 0.05},
    {0.9, 0.95, 0.05},  {0.9, 0.10, 0.05}};

// RAII struct to handle some of the boilerplate in filter
struct FilterArgs {
  // size of memory tested (per iteration) in bytes
  const int64_t size;

  double values_null_proportion = 0.;
  double selected_proportion = 0.;
  double filter_null_proportion = 0.;

  FilterArgs(benchmark::State& state, bool filter_has_nulls)
      : size(state.range(0)), state_(state) {
    auto params = g_filter_params[state.range(1)];
    values_null_proportion = params.values_null_proportion;
    selected_proportion = params.selected_proportion;
    filter_null_proportion = filter_has_nulls ? params.filter_null_proportion : 0;
  }

  ~FilterArgs() {
    state_.counters["size"] = static_cast<double>(size);
    state_.counters["select%"] = selected_proportion * 100;
    state_.counters["data null%"] = values_null_proportion * 100;
    state_.counters["mask null%"] = filter_null_proportion * 100;
    state_.SetBytesProcessed(state_.iterations() * size);
  }

 private:
  benchmark::State& state_;
};

struct TakeBenchmark {
  benchmark::State& state;
  RegressionArgs args;
  random::RandomArrayGenerator rand;
  bool indices_have_nulls;
  bool monotonic_indices = false;

  TakeBenchmark(benchmark::State& state, bool indices_have_nulls,
                bool monotonic_indices = false)
      : state(state),
        args(state, /*size_is_bytes=*/false),
        rand(kSeed),
        indices_have_nulls(indices_have_nulls),
        monotonic_indices(monotonic_indices) {}

  void Int64() {
    auto values = rand.Int64(args.size, -100, 100, args.null_proportion);
    Bench(values);
  }

  void FSLInt64() {
    auto int_array = rand.Int64(args.size, -100, 100, args.null_proportion);
    auto values = std::make_shared<FixedSizeListArray>(
        fixed_size_list(int64(), 1), args.size, int_array, int_array->null_bitmap(),
        int_array->null_count());
    Bench(values);
  }

  void String() {
    int32_t string_min_length = 0, string_max_length = 32;
    auto values = std::static_pointer_cast<StringArray>(rand.String(
        args.size, string_min_length, string_max_length, args.null_proportion));
    Bench(values);
  }

  void Bench(const std::shared_ptr<Array>& values) {
    double indices_null_proportion = indices_have_nulls ? args.null_proportion : 0;
    auto indices =
        rand.Int32(values->length(), 0, static_cast<int32_t>(values->length() - 1),
                   indices_null_proportion);

    if (monotonic_indices) {
      auto arg_sorter = *SortToIndices(*indices);
      indices = *Take(*indices, *arg_sorter);
    }

    for (auto _ : state) {
      ABORT_NOT_OK(Take(values, indices).status());
    }
  }
};

struct FilterBenchmark {
  benchmark::State& state;
  FilterArgs args;
  random::RandomArrayGenerator rand;
  bool filter_has_nulls;

  FilterBenchmark(benchmark::State& state, bool filter_has_nulls)
      : state(state),
        args(state, filter_has_nulls),
        rand(kSeed),
        filter_has_nulls(filter_has_nulls) {}

  void Int64() {
    const int64_t array_size = args.size / sizeof(int64_t);
    auto values = std::static_pointer_cast<NumericArray<Int64Type>>(
        rand.Int64(array_size, -100, 100, args.values_null_proportion));
    Bench(values);
  }

  void FSLInt64() {
    const int64_t array_size = args.size / sizeof(int64_t);
    auto int_array = std::static_pointer_cast<NumericArray<Int64Type>>(
        rand.Int64(array_size, -100, 100, args.values_null_proportion));
    auto values = std::make_shared<FixedSizeListArray>(
        fixed_size_list(int64(), 1), array_size, int_array, int_array->null_bitmap(),
        int_array->null_count());
    Bench(values);
  }

  void String() {
    int32_t string_min_length = 0, string_max_length = 32;
    int32_t string_mean_length = (string_max_length + string_min_length) / 2;
    // for an array of 50% null strings, we need to generate twice as many strings
    // to ensure that they have an average of args.size total characters
    int64_t array_size = args.size;
    if (args.values_null_proportion < 1) {
      array_size = static_cast<int64_t>(args.size / string_mean_length /
                                        (1 - args.values_null_proportion));
    }
    auto values = std::static_pointer_cast<StringArray>(rand.String(
        array_size, string_min_length, string_max_length, args.values_null_proportion));
    Bench(values);
  }

  void Bench(const std::shared_ptr<Array>& values) {
    auto filter = rand.Boolean(values->length(), args.selected_proportion,
                               args.filter_null_proportion);
    for (auto _ : state) {
      ABORT_NOT_OK(Filter(values, filter).status());
    }
  }
};

static void FilterInt64FilterNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).Int64();
}

static void FilterInt64FilterWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).Int64();
}

static void FilterFSLInt64FilterNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).FSLInt64();
}

static void FilterFSLInt64FilterWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).FSLInt64();
}

static void FilterStringFilterNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).String();
}

static void FilterStringFilterWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).String();
}

static void TakeInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, false).Int64();
}

static void TakeInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, true).Int64();
}

static void TakeInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).Int64();
}

static void TakeFSLInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, false).FSLInt64();
}

static void TakeFSLInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, true).FSLInt64();
}

static void TakeFSLInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).FSLInt64();
}

static void TakeStringRandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, false).String();
}

static void TakeStringRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, true).String();
}

static void TakeStringMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).FSLInt64();
}

void FilterSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (int i = 0; i < static_cast<int>(g_filter_params.size()); ++i) {
      bench->Args({static_cast<ArgsType>(size), i});
    }
  }
}

BENCHMARK(FilterInt64FilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterInt64FilterWithNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterFSLInt64FilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterFSLInt64FilterWithNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterStringFilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterStringFilterWithNulls)->Apply(FilterSetArgs);

void TakeSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (auto nulls : std::vector<ArgsType>({1000, 10, 2, 1, 0})) {
      bench->Args({static_cast<ArgsType>(size), nulls});
    }
  }
}

BENCHMARK(TakeInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeFSLInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeFSLInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeFSLInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeStringRandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeStringRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeStringMonotonicIndices)->Apply(TakeSetArgs);

}  // namespace compute
}  // namespace arrow

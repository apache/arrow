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

#include <cmath>
#include <cstdint>
#include <sstream>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

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

constexpr double kDefaultTakeSelectionFactor = 1.0;
constexpr double kSmallTakeSelectionFactor = 0.05;

std::vector<int64_t> g_data_sizes = {kL2Size};

// The benchmark state parameter references this vector of cases. Test high and
// low selectivity filters.

// clang-format off
std::vector<FilterParams> g_filter_params = {
  {0., 0.999, 0.05},
  {0., 0.50, 0.05},
  {0., 0.01, 0.05},
  {0.001, 0.999, 0.05},
  {0.001, 0.50, 0.05},
  {0.001, 0.01, 0.05},
  {0.01, 0.999, 0.05},
  {0.01, 0.50, 0.05},
  {0.01, 0.01, 0.05},
  {0.1, 0.999, 0.05},
  {0.1, 0.50, 0.05},
  {0.1, 0.01, 0.05},
  {0.9, 0.999, 0.05},
  {0.9, 0.50, 0.05},
  {0.9, 0.01, 0.05}
};
// clang-format on

// RAII struct to handle some of the boilerplate in filter
struct FilterArgs {
  // size of memory tested (per iteration) in bytes
  int64_t size;

  // What to call the "size" that's reported in the console output, for result
  // interpretability.
  std::string size_name = "size";

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
    state_.counters[size_name] = static_cast<double>(size);
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
  double selection_factor;
  bool indices_have_nulls;
  bool monotonic_indices = false;

  TakeBenchmark(benchmark::State& state, bool indices_have_nulls,
                bool monotonic_indices = false)
      : TakeBenchmark(state, /*selection_factor=*/kDefaultTakeSelectionFactor,
                      indices_have_nulls, monotonic_indices) {}

  TakeBenchmark(benchmark::State& state, double selection_factor, bool indices_have_nulls,
                bool monotonic_indices = false)
      : state(state),
        args(state, /*size_is_bytes=*/false),
        rand(kSeed),
        selection_factor(selection_factor),
        indices_have_nulls(indices_have_nulls),
        monotonic_indices(monotonic_indices) {}

  static constexpr int kStringMinLength = 0;
  static constexpr int kStringMaxLength = 32;
  static constexpr int kByteWidthRange = 2;

  template <typename GenChunk>
  std::shared_ptr<ChunkedArray> GenChunkedArray(int64_t num_chunks,
                                                GenChunk&& gen_chunk) {
    const int64_t chunk_length =
        std::llround(args.size / static_cast<double>(num_chunks));
    ArrayVector chunks;
    for (int64_t i = 0; i < num_chunks; ++i) {
      const int64_t fitting_chunk_length =
          std::min(chunk_length, args.size - i * chunk_length);
      chunks.push_back(gen_chunk(fitting_chunk_length));
    }
    return std::make_shared<ChunkedArray>(std::move(chunks));
  }

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

  void FixedSizeBinary() {
    const auto byte_width = static_cast<int32_t>(state.range(kByteWidthRange));
    auto values = rand.FixedSizeBinary(args.size, byte_width, args.null_proportion);
    Bench(values);
    state.counters["byte_width"] = byte_width;
  }

  void String() {
    auto values = std::static_pointer_cast<StringArray>(
        rand.String(args.size, kStringMinLength, kStringMaxLength, args.null_proportion));
    Bench(values);
  }

  void ChunkedInt64(int64_t num_chunks, bool chunk_indices_too) {
    auto chunked_array = GenChunkedArray(num_chunks, [this](int64_t chunk_length) {
      return rand.Int64(chunk_length, -100, 100, args.null_proportion);
    });
    BenchChunked(chunked_array, chunk_indices_too);
  }

  void ChunkedFSB(int64_t num_chunks, bool chunk_indices_too) {
    const auto byte_width = static_cast<int32_t>(state.range(kByteWidthRange));
    auto chunked_array =
        GenChunkedArray(num_chunks, [this, byte_width](int64_t chunk_length) {
          return rand.FixedSizeBinary(chunk_length, byte_width, args.null_proportion);
        });
    BenchChunked(chunked_array, chunk_indices_too);
    state.counters["byte_width"] = byte_width;
  }

  void ChunkedString(int64_t num_chunks, bool chunk_indices_too) {
    auto chunked_array = GenChunkedArray(num_chunks, [this](int64_t chunk_length) {
      return std::static_pointer_cast<StringArray>(rand.String(
          chunk_length, kStringMinLength, kStringMaxLength, args.null_proportion));
    });
    BenchChunked(chunked_array, chunk_indices_too);
  }

  void Bench(const std::shared_ptr<Array>& values) {
    const double indices_null_proportion = indices_have_nulls ? args.null_proportion : 0;
    const int64_t num_indices = static_cast<int64_t>(selection_factor * values->length());
    auto indices = rand.Int32(num_indices, 0, static_cast<int32_t>(values->length() - 1),
                              indices_null_proportion);

    if (monotonic_indices) {
      auto arg_sorter = *SortIndices(*indices);
      indices = *Take(*indices, *arg_sorter);
    }

    for (auto _ : state) {
      ABORT_NOT_OK(Take(values, indices));
    }
    state.SetItemsProcessed(state.iterations() * num_indices);
    state.counters["selection_factor"] = selection_factor;
  }

  void BenchChunked(const std::shared_ptr<ChunkedArray>& values, bool chunk_indices_too) {
    double indices_null_proportion = indices_have_nulls ? args.null_proportion : 0;
    const int64_t num_indices = static_cast<int64_t>(selection_factor * values->length());
    auto indices = rand.Int32(num_indices, 0, static_cast<int32_t>(values->length() - 1),
                              indices_null_proportion);

    if (monotonic_indices) {
      auto arg_sorter = *SortIndices(*indices);
      indices = *Take(*indices, *arg_sorter);
    }
    std::shared_ptr<ChunkedArray> chunked_indices;
    if (chunk_indices_too) {
      // Here we choose for indices chunks to have roughly the same length
      // as values chunks, but there may be less of them if selection_factor < 1.0.
      // The alternative is to have the same number of chunks, but with a potentially
      // much smaller (and irrealistic) length.
      std::vector<std::shared_ptr<Array>> indices_chunks;
      // Make sure there are at least two chunks of indices
      const auto max_chunk_length = indices->length() / 2 + 1;
      int64_t offset = 0;
      for (int i = 0; i < values->num_chunks(); ++i) {
        const auto chunk_length = std::min(max_chunk_length, values->chunk(i)->length());
        auto chunk = indices->Slice(offset, chunk_length);
        indices_chunks.push_back(std::move(chunk));
        offset += chunk_length;
        if (offset >= indices->length()) {
          break;
        }
      }
      chunked_indices = std::make_shared<ChunkedArray>(std::move(indices_chunks));
      ARROW_CHECK_EQ(chunked_indices->length(), num_indices);
      ARROW_CHECK_GT(chunked_indices->num_chunks(), 1);
    }

    if (chunk_indices_too) {
      for (auto _ : state) {
        ABORT_NOT_OK(Take(values, chunked_indices));
      }
    } else {
      for (auto _ : state) {
        ABORT_NOT_OK(Take(values, indices));
      }
    }
    state.SetItemsProcessed(state.iterations() * num_indices);
    state.counters["selection_factor"] = selection_factor;
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
    auto values = rand.Int64(array_size, -100, 100, args.values_null_proportion);
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

  void FixedSizeBinary() {
    const int32_t byte_width = static_cast<int32_t>(state.range(2));
    const int64_t array_size = args.size / byte_width;
    auto values =
        rand.FixedSizeBinary(array_size, byte_width, args.values_null_proportion);
    Bench(values);
    state.counters["byte_width"] = byte_width;
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
      ABORT_NOT_OK(Filter(values, filter));
    }
    state.SetItemsProcessed(state.iterations() * values->length());
  }

  void BenchRecordBatch() {
    const int64_t total_data_cells = 10000000;
    const int64_t num_columns = state.range(0);
    const int64_t num_rows = total_data_cells / num_columns;

    auto col_data = rand.Float64(num_rows, 0, 1);

    auto filter =
        rand.Boolean(num_rows, args.selected_proportion, args.filter_null_proportion);

    int64_t output_length =
        internal::GetFilterOutputSize(*filter->data(), FilterOptions::DROP);

    // HACK: set FilterArgs.size to the number of selected data cells *
    // sizeof(double) for accurate memory processing performance
    args.size = output_length * num_columns * sizeof(double);
    args.size_name = "extracted_size";
    state.counters["num_cols"] = static_cast<double>(num_columns);

    std::vector<std::shared_ptr<Array>> columns;
    std::vector<std::shared_ptr<Field>> fields;
    for (int64_t i = 0; i < num_columns; ++i) {
      std::stringstream ss;
      ss << "f" << i;
      fields.push_back(::arrow::field(ss.str(), float64()));
      columns.push_back(col_data);
    }

    auto batch = RecordBatch::Make(schema(fields), num_rows, columns);
    for (auto _ : state) {
      ABORT_NOT_OK(Filter(batch, filter));
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
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

static void FilterFixedSizeBinaryFilterNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).FixedSizeBinary();
}

static void FilterFixedSizeBinaryFilterWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).FixedSizeBinary();
}

static void FilterStringFilterNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).String();
}

static void FilterStringFilterWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).String();
}

static void FilterRecordBatchNoNulls(benchmark::State& state) {
  FilterBenchmark(state, false).BenchRecordBatch();
}

static void FilterRecordBatchWithNulls(benchmark::State& state) {
  FilterBenchmark(state, true).BenchRecordBatch();
}

static void TakeInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false).Int64();
}

static void TakeInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true).Int64();
}

static void TakeInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).Int64();
}

static void TakeFixedSizeBinaryRandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false).FixedSizeBinary();
}

static void TakeFixedSizeBinaryRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true).FixedSizeBinary();
}

static void TakeFixedSizeBinaryMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true)
      .FixedSizeBinary();
}

static void TakeFSLInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false).FSLInt64();
}

static void TakeFSLInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true).FSLInt64();
}

static void TakeFSLInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).FSLInt64();
}

static void TakeStringRandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false).String();
}

static void TakeStringRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true).String();
}

static void TakeStringMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true).FSLInt64();
}

static void TakeChunkedChunkedInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedInt64FewRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/true)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedInt64(
          /*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedInt64FewMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedInt64(
          /*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedFSBRandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false)
      .ChunkedFSB(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedFSBRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true)
      .ChunkedFSB(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedFSBMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedFSB(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedStringRandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false)
      .ChunkedString(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedStringRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true)
      .ChunkedString(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedStringFewRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/true)
      .ChunkedString(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedStringMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedString(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedChunkedStringFewMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedString(/*num_chunks=*/100, /*chunk_indices_too=*/true);
}

static void TakeChunkedFlatInt64RandomIndicesNoNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/false);
}

static void TakeChunkedFlatInt64RandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/true)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/false);
}

static void TakeChunkedFlatInt64FewRandomIndicesWithNulls(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/true)
      .ChunkedInt64(/*num_chunks=*/100, /*chunk_indices_too=*/false);
}

static void TakeChunkedFlatInt64MonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedInt64(
          /*num_chunks=*/100, /*chunk_indices_too=*/false);
}

static void TakeChunkedFlatInt64FewMonotonicIndices(benchmark::State& state) {
  TakeBenchmark(state, /*selection_factor=*/kSmallTakeSelectionFactor,
                /*indices_with_nulls=*/false, /*monotonic=*/true)
      .ChunkedInt64(
          /*num_chunks=*/100, /*chunk_indices_too=*/false);
}

void FilterSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (int i = 0; i < static_cast<int>(g_filter_params.size()); ++i) {
      bench->Args({static_cast<ArgsType>(size), i});
    }
  }
}

void FilterFSBSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (int i = 0; i < static_cast<int>(g_filter_params.size()); ++i) {
      // FixedSizeBinary of primitive sizes (powers of two up to 32)
      // have a faster path.
      for (int32_t byte_width : {8, 9}) {
        bench->Args({static_cast<ArgsType>(size), i, byte_width});
      }
    }
  }
}

BENCHMARK(FilterInt64FilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterInt64FilterWithNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterFixedSizeBinaryFilterNoNulls)->Apply(FilterFSBSetArgs);
BENCHMARK(FilterFixedSizeBinaryFilterWithNulls)->Apply(FilterFSBSetArgs);
BENCHMARK(FilterFSLInt64FilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterFSLInt64FilterWithNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterStringFilterNoNulls)->Apply(FilterSetArgs);
BENCHMARK(FilterStringFilterWithNulls)->Apply(FilterSetArgs);

void FilterRecordBatchSetArgs(benchmark::internal::Benchmark* bench) {
  for (auto num_cols : std::vector<int>({10, 50, 100})) {
    for (int i = 0; i < static_cast<int>(g_filter_params.size()); ++i) {
      bench->Args({num_cols, i});
    }
  }
}
BENCHMARK(FilterRecordBatchNoNulls)->Apply(FilterRecordBatchSetArgs);
BENCHMARK(FilterRecordBatchWithNulls)->Apply(FilterRecordBatchSetArgs);

void TakeSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (auto nulls : std::vector<ArgsType>({1000, 10, 2, 1, 0})) {
      bench->Args({static_cast<ArgsType>(size), nulls});
    }
  }
}

void TakeFSBSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (auto nulls : std::vector<ArgsType>({1000, 10, 2, 1, 0})) {
      // FixedSizeBinary of primitive sizes (powers of two up to 32)
      // have a faster path.
      for (int32_t byte_width : {8, 9}) {
        bench->Args({static_cast<ArgsType>(size), nulls, byte_width});
      }
    }
  }
}

// Flat values x Flat indices
BENCHMARK(TakeInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeFixedSizeBinaryRandomIndicesNoNulls)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeFixedSizeBinaryRandomIndicesWithNulls)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeFixedSizeBinaryMonotonicIndices)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeFSLInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeFSLInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeFSLInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeStringRandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeStringRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeStringMonotonicIndices)->Apply(TakeSetArgs);

// Chunked values x Chunked indices
BENCHMARK(TakeChunkedChunkedInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedInt64FewRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedInt64FewMonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedFSBRandomIndicesNoNulls)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeChunkedChunkedFSBRandomIndicesWithNulls)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeChunkedChunkedFSBMonotonicIndices)->Apply(TakeFSBSetArgs);
BENCHMARK(TakeChunkedChunkedStringRandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedStringRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedStringFewRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedStringMonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedChunkedStringFewMonotonicIndices)->Apply(TakeSetArgs);

// Chunked values x Flat indices
BENCHMARK(TakeChunkedFlatInt64RandomIndicesNoNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedFlatInt64RandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedFlatInt64FewRandomIndicesWithNulls)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedFlatInt64MonotonicIndices)->Apply(TakeSetArgs);
BENCHMARK(TakeChunkedFlatInt64FewMonotonicIndices)->Apply(TakeSetArgs);

}  // namespace compute
}  // namespace arrow

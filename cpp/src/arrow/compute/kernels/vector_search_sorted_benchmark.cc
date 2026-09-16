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
#include <cmath>
#include <concepts>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_vector.h"
#include "arrow/datum.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x5EA4C42;
constexpr int32_t kStringMinLength = 8;
constexpr int32_t kStringMaxLength = 20;

struct DenseBenchmarkArgs {
  int64_t logical_length_;
  double null_probability_;
  SearchSortedOptions::Side side_;

  explicit DenseBenchmarkArgs(const benchmark::State& state)
      : logical_length_(state.range(0)), null_probability_(state.range(1) / 100.0) {
    side_ = std::array{SearchSortedOptions::Left, SearchSortedOptions::Right}.at(
        state.range(2));
  }

  static void SetArgs(benchmark::internal::Benchmark* bench) {
    bench->Unit(benchmark::kMicrosecond);
    bench->ArgNames({"length", "null_probability", "side"});
    for (const auto size : std::vector<int64_t>{kL1Size, kL2Size}) {
      for (const double null_probability : {0.0, 0.5, 0.9}) {
        for (const bool right_side : {false, true}) {
          bench->Args(
              {size / 4, static_cast<int64_t>(null_probability * 100), right_side});
        }
      }
    }
  };
};

struct DenseBenchmarkBase {
  double null_probability_;
  random::RandomArrayGenerator rand_;

  explicit DenseBenchmarkBase(const DenseBenchmarkArgs& args)
      : null_probability_(args.null_probability_), rand_(kSeed) {}
};

struct Int64Benchmark : public DenseBenchmarkBase {
  using DenseBenchmarkBase::DenseBenchmarkBase;

  std::shared_ptr<Array> BuildSortedValues(int64_t length) {
    auto values = std::static_pointer_cast<Int64Array>(RandomArray(length));
    std::vector<int64_t> data(values->raw_values(),
                              values->raw_values() + values->length());
    std::ranges::sort(data);

    Int64Builder builder;
    ABORT_NOT_OK(builder.AppendValues(data));
    return builder.Finish().ValueOrDie();
  }

  std::shared_ptr<Array> BuildNeedles(int64_t length) {
    // NOTE it's not required to have the same number of needles as values;
    // it just makes the benchmarks simpler.
    return RandomArray(length);
  }

 protected:
  std::shared_ptr<Array> RandomArray(int64_t length) {
    return rand_.Int64(length, 0, /*max=*/length * 4, null_probability_);
  }
};

struct StringBenchmark : public DenseBenchmarkBase {
  using DenseBenchmarkBase::DenseBenchmarkBase;

  std::shared_ptr<Array> BuildSortedValues(int64_t length) {
    auto values = std::static_pointer_cast<StringArray>(
        rand_.String(length, kStringMinLength, kStringMaxLength, null_probability_));

    std::vector<std::string_view> data;
    data.reserve(static_cast<size_t>(values->length()));
    for (int64_t index = 0; index < values->length(); ++index) {
      data.push_back(values->GetView(index));
    }
    std::ranges::sort(data);

    StringBuilder builder;
    ABORT_NOT_OK(builder.Reserve(values->length()));
    ABORT_NOT_OK(builder.ReserveData(values->total_values_length()));
    for (const auto view : data) {
      builder.UnsafeAppend(view);
    }
    return std::static_pointer_cast<StringArray>(builder.Finish().ValueOrDie());
  }

  std::shared_ptr<Array> BuildNeedles(int64_t length) {
    return rand_.String(length, kStringMinLength, kStringMaxLength, null_probability_);
  }
};

template <typename DenseBenchmark, bool kREENeedles>
  requires std::derived_from<DenseBenchmark, DenseBenchmarkBase>
struct REEBenchmark {
  static constexpr int64_t kAverageRunLength = 50;
  int64_t logical_length_;
  int64_t physical_length_;
  DenseBenchmark dense_benchmark_;

  explicit REEBenchmark(const DenseBenchmarkArgs& args) : dense_benchmark_(args) {}

  std::shared_ptr<Array> BuildSortedValues(int64_t logical_length) {
    return Encode(dense_benchmark_.BuildSortedValues(logical_length / kAverageRunLength),
                  logical_length);
  }

  std::shared_ptr<Array> BuildNeedles(int64_t logical_length) {
    if constexpr (kREENeedles) {
      return Encode(dense_benchmark_.BuildNeedles(logical_length / kAverageRunLength),
                    logical_length);
    } else {
      return dense_benchmark_.BuildNeedles(logical_length);
    }
  }

 protected:
  std::shared_ptr<Array> Encode(std::shared_ptr<Array> values, int64_t logical_length) {
    return dense_benchmark_.rand_.RunEndEncoded(values, logical_length);
  }
};

template <typename DenseBenchmark>
using REEValuesDenseNeedlesBenchmark =
    REEBenchmark<DenseBenchmark, /*kREENeedles=*/false>;

template <typename DenseBenchmark>
using REEValuesREENeedlesBenchmark = REEBenchmark<DenseBenchmark, /*kREENeedles=*/true>;

struct NoOpChunker {
  Datum operator()(std::shared_ptr<Array> array) { return array; }
};

struct StaticChunker {
  static constexpr int kNumChunks = 8;

  Datum operator()(std::shared_ptr<Array> array) {
    ArrayVector chunks;
    int64_t chunk_start = 0;
    for (int64_t i = 0; i < kNumChunks; ++i) {
      int64_t chunk_end = ceil(static_cast<double>(i + 1) / kNumChunks * array->length());
      chunks.push_back(
          array->SliceSafe(chunk_start, chunk_end - chunk_start).ValueOrDie());
      chunk_start = chunk_end;
    }
    ARROW_CHECK_EQ(chunk_start, array->length());
    auto chunked_array = ChunkedArray::Make(std::move(chunks)).ValueOrDie();
    ARROW_CHECK_EQ(chunked_array->length(), array->length());
    return chunked_array;
  }
};

void SetBenchmarkCounters(benchmark::State& state, const Datum& values,
                          const Datum& needles, SearchSortedOptions::Side side) {
  const auto needles_length = needles.length();
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
  SetBenchmarkCounters(state, values, needles, side);
}

template <typename Benchmark, typename Chunker>
void RunSearchSortedBenchmark(benchmark::State& state, DenseBenchmarkArgs args,
                              Benchmark benchmark, SearchSortedOptions::Side side) {
  Chunker chunker;
  RunSearchSortedBenchmark(state,
                           chunker(benchmark.BuildSortedValues(args.logical_length_)),
                           chunker(benchmark.BuildNeedles(args.logical_length_)), side);
}

template <typename Benchmark, typename Chunker>
void RunSearchSortedBenchmark(benchmark::State& state, DenseBenchmarkArgs args) {
  RunSearchSortedBenchmark<Benchmark, Chunker>(state, args, Benchmark(args), args.side_);
}

static void SearchSortedDenseInt64Array(benchmark::State& state) {
  RunSearchSortedBenchmark<Int64Benchmark, NoOpChunker>(state, DenseBenchmarkArgs(state));
}

static void SearchSortedDenseStringArray(benchmark::State& state) {
  RunSearchSortedBenchmark<StringBenchmark, NoOpChunker>(state,
                                                         DenseBenchmarkArgs(state));
}

static void SearchSortedDenseInt64ChunkedArray(benchmark::State& state) {
  RunSearchSortedBenchmark<Int64Benchmark, StaticChunker>(state,
                                                          DenseBenchmarkArgs(state));
}

static void SearchSortedDenseStringChunkedArray(benchmark::State& state) {
  RunSearchSortedBenchmark<StringBenchmark, StaticChunker>(state,
                                                           DenseBenchmarkArgs(state));
}

static void SearchSortedREEInt64Array(benchmark::State& state) {
  RunSearchSortedBenchmark<REEValuesDenseNeedlesBenchmark<Int64Benchmark>, NoOpChunker>(
      state, DenseBenchmarkArgs(state));
}

static void SearchSortedREEInt64ChunkedArray(benchmark::State& state) {
  RunSearchSortedBenchmark<REEValuesDenseNeedlesBenchmark<Int64Benchmark>, StaticChunker>(
      state, DenseBenchmarkArgs(state));
}

static void SearchSortedREEInt64ArrayREENeedles(benchmark::State& state) {
  RunSearchSortedBenchmark<REEValuesREENeedlesBenchmark<Int64Benchmark>, NoOpChunker>(
      state, DenseBenchmarkArgs(state));
}

static void SearchSortedREEInt64ChunkedArrayREENeedles(benchmark::State& state) {
  RunSearchSortedBenchmark<REEValuesREENeedlesBenchmark<Int64Benchmark>, StaticChunker>(
      state, DenseBenchmarkArgs(state));
}

BENCHMARK(SearchSortedDenseInt64Array)->Apply(DenseBenchmarkArgs::SetArgs);
BENCHMARK(SearchSortedDenseStringArray)->Apply(DenseBenchmarkArgs::SetArgs);
BENCHMARK(SearchSortedDenseInt64ChunkedArray)->Apply(DenseBenchmarkArgs::SetArgs);
BENCHMARK(SearchSortedDenseStringChunkedArray)->Apply(DenseBenchmarkArgs::SetArgs);

BENCHMARK(SearchSortedREEInt64Array)->Apply(DenseBenchmarkArgs::SetArgs);
BENCHMARK(SearchSortedREEInt64ChunkedArray)->Apply(DenseBenchmarkArgs::SetArgs);

BENCHMARK(SearchSortedREEInt64ArrayREENeedles)->Apply(DenseBenchmarkArgs::SetArgs);
BENCHMARK(SearchSortedREEInt64ChunkedArrayREENeedles)->Apply(DenseBenchmarkArgs::SetArgs);

}  // namespace compute
}  // namespace arrow

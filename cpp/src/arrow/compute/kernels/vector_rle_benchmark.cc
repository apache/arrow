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
#include <random>
#include <sstream>

#include "arrow/array/builder_base.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

struct RunLengthDistribution {
  std::vector<size_t> run_lengths;
  std::discrete_distribution<int> discrete_distribution;
};

static constexpr size_t BENCHMARK_ARRAY_SIZE = 1000000;

struct ValuesSet {
  ValuesSet(std::shared_ptr<DataType> type,
            std::initializer_list<const char*> json_values)
      : type{std::move(type)} {
    std::transform(json_values.begin(), json_values.end(), std::back_inserter(values),
                   [this](const char* json) { return ScalarFromJSON(this->type, json); });
  }

  std::shared_ptr<DataType> type;
  std::vector<std::shared_ptr<Scalar>> values;
};

static std::shared_ptr<Array> GenerateArray(ValuesSet values,
                                            RunLengthDistribution& distribution) {
  std::mt19937 random_engine(/*seed = */ 1337);

  size_t position = 0;
  size_t run_index = 0;

  auto builder = *MakeBuilder(values.type);

  while (position != BENCHMARK_ARRAY_SIZE) {
    const int random_index = distribution.discrete_distribution(random_engine);
    const Scalar& value = *values.values[run_index % values.values.size()];
    size_t run_length = distribution.run_lengths.at(random_index);
    run_length = std::min(run_length, BENCHMARK_ARRAY_SIZE - position);
    ARROW_EXPECT_OK(builder->AppendScalar(value, run_length));

    position += run_length;
    run_index++;
  }
  return builder->Finish().ValueOrDie();
}

static void RLEEncodeBenchmark(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);

  ExecContext ctx;
  for (auto _ : state) {
    RunLengthEncode(array, &ctx).ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

static void RLEDecodeBenchmark(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto encoded_array = RunLengthEncode(array).ValueOrDie();

  ExecContext ctx;
  for (auto _ : state) {
    RunLengthDecode(encoded_array, &ctx).ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

static void RLEFilterBenchmark(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto filter =
      GenerateArray(ValuesSet(boolean(), {"true", "false", "null"}), distribution);
  auto encoded_array = RunLengthEncode(array).ValueOrDie();
  auto encoded_filter = RunLengthEncode(filter).ValueOrDie();

  ExecContext ctx;

  for (auto _ : state) {
    CallFunction("filter", {Datum(encoded_array), Datum(encoded_filter)}, NULLPTR, &ctx)
        .ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

static void RLEFilterBaseline(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto filter =
      GenerateArray(ValuesSet(boolean(), {"true", "false", "null"}), distribution);

  ExecContext ctx;

  for (auto _ : state) {
    CallFunction("filter", {Datum(array), Datum(filter)}, NULLPTR, &ctx)
        .ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

static RunLengthDistribution only_single_distribition{{1}, {1}};
static RunLengthDistribution only_1000_distribition{{1000}, {1}};
static RunLengthDistribution only_100_distribition{{100}, {1}};
static RunLengthDistribution only_10_distribition{{100}, {1}};
static RunLengthDistribution equally_12345_distribition{{1, 2, 3, 4, 5}, {5,4,3,2,1}};
static RunLengthDistribution equally_mixed_distribition{{1, 10, 100, 1000},
                                                        {1000, 100, 10, 1}};

BENCHMARK_CAPTURE(RLEEncodeBenchmark, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEDecodeBenchmark, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);

BENCHMARK_CAPTURE(RLEFilterBenchmark, int_single, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEFilterBenchmark, int_10, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_12345_distribition);
BENCHMARK_CAPTURE(RLEFilterBenchmark, int_12345, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_10_distribition);
BENCHMARK_CAPTURE(RLEFilterBenchmark, int_100, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_100_distribition);
BENCHMARK_CAPTURE(RLEFilterBenchmark, int_1000, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_1000_distribition);
BENCHMARK_CAPTURE(RLEFilterBenchmark, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_single, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_12345, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_12345_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_10, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_10_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_100, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_100_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_1000, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_1000_distribition);
BENCHMARK_CAPTURE(RLEFilterBaseline, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);

}  // namespace compute
}  // namespace arrow

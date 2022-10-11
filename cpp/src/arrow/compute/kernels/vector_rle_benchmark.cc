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
#include "arrow/util/rle_encoding.h"

namespace arrow {
namespace compute {

struct RunLengthDistribution {
  std::vector<size_t> run_lengths;
  std::discrete_distribution<int> discrete_distribution;
};

static constexpr size_t BENCHMARK_ARRAY_SIZE = 1000000;

const RunLengthEncodeOptions benchmark_encode_options(int32());

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
    RunLengthEncode(array, benchmark_encode_options, &ctx).ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
static void RLEEncodeReference(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  // the parquet encoder does not support nulls on this level
  ARROW_CHECK(!array->data()->MayHaveNulls());

  // this benchmark currently only supports 32-bit values
  ARROW_CHECK_EQ(values.type->bit_width(), 32);

  // The parquet encoder requires a buffer upfront, create one for the maxmimum possible
  // output size.
  size_t buffer_size =
      util::RleEncoder::MaxBufferSize(values.type->bit_width(), array->length());
  std::vector<uint8_t> output_buffer(buffer_size);

  const uint32_t* input_buffer = array->data()->GetValues<uint32_t>(1);

  ExecContext ctx;
  for (auto _ : state) {
    util::RleEncoder encoder(output_buffer.data(), buffer_size, values.type->bit_width());
    for (int64_t index = 0; index < array->length(); index++) {
      // don't check for success since this may affect performance. The same operation is
      // checked in the preparation phase of RLEDecodeReference.
      /*bool result =*/encoder.Put(input_buffer[index]);
      /*ARROW_CHECK(result);*/
    }
    encoder.Flush();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

static void RLEDecodeBenchmark(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto encoded_array = RunLengthEncode(array, benchmark_encode_options).ValueOrDie();

  ExecContext ctx;
  for (auto _ : state) {
    RunLengthDecode(encoded_array, &ctx).ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
static void RLEDecodeReference(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  /* the parquet encoder does not support nulls on this level */
  ARROW_CHECK(!array->data()->MayHaveNulls());

  /* this benchmark currently only supports 32-bit values */
  ARROW_CHECK_EQ(values.type->bit_width(), 32);

  /* The parquet encoder requires a buffer upfront, create one for the maxmimum possible
   * output size. */
  size_t encoded_buffer_size =
      util::RleEncoder::MaxBufferSize(values.type->bit_width(), array->length());
  std::vector<uint8_t> encoded_buffer(encoded_buffer_size);
  std::vector<uint32_t> output_buffer(array->length());

  const uint32_t* input_buffer = array->data()->GetValues<uint32_t>(1);

  util::RleEncoder encoder(encoded_buffer.data(), encoded_buffer_size,
                           values.type->bit_width());
  for (int64_t index = 0; index < array->length(); index++) {
    bool result = encoder.Put(input_buffer[index]);
    ARROW_CHECK(result);
  }
  encoder.Flush();

  ExecContext ctx;
  for (auto _ : state) {
    util::RleDecoder decoder(encoded_buffer.data(), encoded_buffer_size,
                             values.type->bit_width());
    decoder.GetBatch(output_buffer.data(), array->length());
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

static void RLEFilterBenchmark(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto filter =
      GenerateArray(ValuesSet(boolean(), {"true", "false", "null"}), distribution);
  auto encoded_array = RunLengthEncode(array, benchmark_encode_options).ValueOrDie();
  auto encoded_filter = RunLengthEncode(filter, benchmark_encode_options).ValueOrDie();

  ExecContext ctx;

  for (auto _ : state) {
    CallFunction("filter", {Datum(encoded_array), Datum(encoded_filter)}, NULLPTR, &ctx)
        .ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
static void RLEFilterReference(benchmark::State& state, ValuesSet values,
                               RunLengthDistribution& distribution) {
  auto array = GenerateArray(values, distribution);
  auto filter =
      GenerateArray(ValuesSet(boolean(), {"true", "false", "null"}), distribution);

  ExecContext ctx;

  for (auto _ : state) {
    CallFunction("filter", {Datum(array), Datum(filter)}, NULLPTR, &ctx).ValueOrDie();
  }

  state.counters["rows_per_second"] =
      benchmark::Counter(static_cast<double>(state.iterations() * BENCHMARK_ARRAY_SIZE),
                         benchmark::Counter::kIsRate);
}
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

static RunLengthDistribution only_single_distribition{{1}, {1}};
static RunLengthDistribution only_1000_distribition{{1000}, {1}};
static RunLengthDistribution only_100_distribition{{100}, {1}};
static RunLengthDistribution only_10_distribition{{100}, {1}};
static RunLengthDistribution equally_12345_distribition{{1, 2, 3, 4, 5}, {5, 4, 3, 2, 1}};
static RunLengthDistribution equally_mixed_distribition{{1, 10, 100, 1000},
                                                        {1000, 100, 10, 1}};

BENCHMARK_CAPTURE(RLEEncodeBenchmark, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEEncodeBenchmark, int_nonnull_mixed, ValuesSet(uint32(), {"1", "2"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEEncodeBenchmark, int_nonnull_single, ValuesSet(uint32(), {"1", "2"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEEncodeBenchmark, int_nonnull_1000, ValuesSet(uint32(), {"1", "2"}),
                  only_1000_distribition);

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
BENCHMARK_CAPTURE(RLEEncodeReference, int_nonnull_mixed, ValuesSet(uint32(), {"1", "2"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEEncodeReference, int_nonnull_single, ValuesSet(uint32(), {"1", "2"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEEncodeReference, int_nonnull_1000, ValuesSet(uint32(), {"1", "2"}),
                  only_1000_distribition);
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

BENCHMARK_CAPTURE(RLEDecodeBenchmark, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEDecodeBenchmark, int_nonnull_mixed, ValuesSet(uint32(), {"1", "2"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEDecodeBenchmark, int_nonnull_single, ValuesSet(uint32(), {"1", "2"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEDecodeBenchmark, int_nonnull_1000, ValuesSet(uint32(), {"1", "2"}),
                  only_1000_distribition);

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
BENCHMARK_CAPTURE(RLEDecodeReference, int_nonnull_mixed, ValuesSet(uint32(), {"1", "2"}),
                  equally_mixed_distribition);
BENCHMARK_CAPTURE(RLEDecodeReference, int_nonnull_single, ValuesSet(uint32(), {"1", "2"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEDecodeReference, int_nonnull_1000, ValuesSet(uint32(), {"1", "2"}),
                  only_1000_distribition);
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

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

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
BENCHMARK_CAPTURE(RLEFilterReference, int_single, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_single_distribition);
BENCHMARK_CAPTURE(RLEFilterReference, int_12345, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_12345_distribition);
BENCHMARK_CAPTURE(RLEFilterReference, int_10, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_10_distribition);
BENCHMARK_CAPTURE(RLEFilterReference, int_100, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_100_distribition);
BENCHMARK_CAPTURE(RLEFilterReference, int_1000, ValuesSet(uint32(), {"1", "2", "null"}),
                  only_1000_distribition);
BENCHMARK_CAPTURE(RLEFilterReference, int_mixed, ValuesSet(uint32(), {"1", "2", "null"}),
                  equally_mixed_distribition);
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

}  // namespace compute
}  // namespace arrow

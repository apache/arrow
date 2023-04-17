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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;

static void SetLookupBenchmarkString(benchmark::State& state,
                                     const std::string& func_name,
                                     const int64_t value_set_length) {
  // As the set lookup functions don't support duplicate values in the value_set,
  // we need to choose random generation parameters that minimize the risk of
  // duplicates (including nulls).
  const int64_t array_length = 1 << 18;
  const int32_t value_min_size = (value_set_length < 64) ? 2 : 10;
  const int32_t value_max_size = 32;
  const double null_probability = 0.2 / value_set_length;
  random::RandomArrayGenerator rng(kSeed);

  auto values =
      rng.String(array_length, value_min_size, value_max_size, null_probability);
  auto value_set =
      rng.String(value_set_length, value_min_size, value_max_size, null_probability);
  ABORT_NOT_OK(CallFunction(func_name, {values, value_set}));
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(func_name, {values, value_set}));
  }
  state.SetItemsProcessed(state.iterations() * array_length);
  state.SetBytesProcessed(state.iterations() * values->data()->buffers[2]->size());
}

template <typename Type>
static void SetLookupBenchmarkNumeric(benchmark::State& state,
                                      const std::string& func_name,
                                      const int64_t value_set_length) {
  const int64_t array_length = 1 << 18;
  const int64_t value_min = 0;
  const int64_t value_max = std::numeric_limits<typename Type::c_type>::max();
  const double null_probability = 0.1 / value_set_length;
  random::RandomArrayGenerator rng(kSeed);

  auto values = rng.Numeric<Type>(array_length, value_min, value_max, null_probability);
  auto value_set =
      rng.Numeric<Type>(value_set_length, value_min, value_max, null_probability);
  ABORT_NOT_OK(CallFunction(func_name, {values, value_set}));
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(func_name, {values, value_set}));
  }
  state.SetItemsProcessed(state.iterations() * array_length);
  state.SetBytesProcessed(state.iterations() * values->data()->buffers[1]->size());
}

static void IndexInStringSmallSet(benchmark::State& state) {
  SetLookupBenchmarkString(state, "index_in_meta_binary", state.range(0));
}

static void IsInStringSmallSet(benchmark::State& state) {
  SetLookupBenchmarkString(state, "is_in_meta_binary", state.range(0));
}

static void IndexInStringLargeSet(benchmark::State& state) {
  SetLookupBenchmarkString(state, "index_in_meta_binary", 1 << 10);
}

static void IsInStringLargeSet(benchmark::State& state) {
  SetLookupBenchmarkString(state, "is_in_meta_binary", 1 << 10);
}

static void IndexInInt8SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int8Type>(state, "index_in_meta_binary", state.range(0));
}

static void IndexInInt16SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int16Type>(state, "index_in_meta_binary", state.range(0));
}

static void IndexInInt32SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int32Type>(state, "index_in_meta_binary", state.range(0));
}

static void IndexInInt64SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int64Type>(state, "index_in_meta_binary", state.range(0));
}

static void IsInInt8SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int8Type>(state, "is_in_meta_binary", state.range(0));
}

static void IsInInt16SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int16Type>(state, "is_in_meta_binary", state.range(0));
}

static void IsInInt32SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int32Type>(state, "is_in_meta_binary", state.range(0));
}

static void IsInInt64SmallSet(benchmark::State& state) {
  SetLookupBenchmarkNumeric<Int64Type>(state, "is_in_meta_binary", state.range(0));
}

BENCHMARK(IndexInStringSmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IsInStringSmallSet)->RangeMultiplier(4)->Range(2, 64);

BENCHMARK(IndexInStringLargeSet);
BENCHMARK(IsInStringLargeSet);

// XXX For Int8, the value_set length has to be capped at a lower value
// in order to avoid duplicates.
BENCHMARK(IndexInInt8SmallSet)->RangeMultiplier(4)->Range(2, 8);
BENCHMARK(IndexInInt16SmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IndexInInt32SmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IndexInInt64SmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IsInInt8SmallSet)->RangeMultiplier(4)->Range(2, 8);
BENCHMARK(IsInInt16SmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IsInInt32SmallSet)->RangeMultiplier(4)->Range(2, 64);
BENCHMARK(IsInInt64SmallSet)->RangeMultiplier(4)->Range(2, 64);

}  // namespace compute
}  // namespace arrow

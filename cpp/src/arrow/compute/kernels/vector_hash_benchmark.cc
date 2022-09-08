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

#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

#include "arrow/compute/api.h"

namespace arrow {
namespace compute {

static void BuildDictionary(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1024;

  std::vector<int64_t> values;
  std::vector<bool> is_valid;
  for (int64_t i = 0; i < iterations; i++) {
    for (int64_t j = 0; j < i; j++) {
      is_valid.push_back((i + j) % 9 != 0);
      values.push_back(j);
    }
  }

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int64Type, int64_t>(is_valid, values, &arr);

  while (state.KeepRunning()) {
    ABORT_NOT_OK(DictionaryEncode(arr).status());
  }
  state.counters["null_percent"] =
      static_cast<double>(arr->null_count()) / arr->length() * 100;
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BuildStringDictionary(
    benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1024 * 64;
  // Pre-render strings
  std::vector<std::string> data;

  int64_t total_bytes = 0;
  for (int64_t i = 0; i < iterations; i++) {
    std::stringstream ss;
    ss << i;
    auto val = ss.str();
    data.push_back(val);
    total_bytes += static_cast<int64_t>(val.size());
  }

  std::shared_ptr<Array> arr;
  ArrayFromVector<StringType, std::string>(data, &arr);

  while (state.KeepRunning()) {
    ABORT_NOT_OK(DictionaryEncode(arr).status());
  }
  state.SetBytesProcessed(state.iterations() * total_bytes);
  state.SetItemsProcessed(state.iterations() * data.size());
}

struct HashBenchCase {
  int64_t length;
  int64_t num_unique;
  double null_probability;
};

template <typename Type>
struct HashParams {
  using T = typename Type::c_type;

  HashBenchCase params;

  void GenerateTestData(std::shared_ptr<Array>* arr) const {
    std::vector<int64_t> draws;
    std::vector<T> values;
    std::vector<bool> is_valid;
    randint<int64_t>(params.length, 0, params.num_unique, &draws);
    for (int64_t draw : draws) {
      values.push_back(static_cast<T>(draw));
    }
    if (params.null_probability > 0) {
      random_is_valid(params.length, params.null_probability, &is_valid);
      ArrayFromVector<Type, T>(is_valid, values, arr);
    } else {
      ArrayFromVector<Type, T>(values, arr);
    }
  }

  void SetMetadata(benchmark::State& state) const {
    state.counters["null_percent"] = params.null_probability * 100;
    state.counters["num_unique"] = static_cast<double>(params.num_unique);
    state.SetBytesProcessed(state.iterations() * params.length * sizeof(T));
    state.SetItemsProcessed(state.iterations() * params.length);
  }
};

template <>
struct HashParams<StringType> {
  HashBenchCase params;
  int32_t byte_width;
  void GenerateTestData(std::shared_ptr<Array>* arr) const {
    std::vector<int64_t> draws;
    randint<int64_t>(params.length, 0, params.num_unique, &draws);

    const int64_t total_bytes = this->byte_width * params.num_unique;
    std::vector<uint8_t> uniques(total_bytes);
    const uint32_t seed = 0;
    random_bytes(total_bytes, seed, uniques.data());

    std::vector<bool> is_valid;
    if (params.null_probability > 0) {
      random_is_valid(params.length, params.null_probability, &is_valid);
    }

    StringBuilder builder;
    for (int64_t i = 0; i < params.length; ++i) {
      if (params.null_probability == 0 || is_valid[i]) {
        ABORT_NOT_OK(builder.Append(uniques.data() + this->byte_width * draws[i],
                                    this->byte_width));
      } else {
        ABORT_NOT_OK(builder.AppendNull());
      }
    }
    ABORT_NOT_OK(builder.Finish(arr));
  }

  void SetMetadata(benchmark::State& state) const {
    state.counters["null_percent"] = params.null_probability * 100;
    state.counters["num_unique"] = static_cast<double>(params.num_unique);
    state.SetBytesProcessed(state.iterations() * params.length * byte_width);
    state.SetItemsProcessed(state.iterations() * params.length);
  }
};

template <typename ParamType>
void BenchUnique(benchmark::State& state, const ParamType& params) {
  std::shared_ptr<Array> arr;
  params.GenerateTestData(&arr);

  while (state.KeepRunning()) {
    ABORT_NOT_OK(Unique(arr).status());
  }
  params.SetMetadata(state);
}

template <typename ParamType>
void BenchDictionaryEncode(benchmark::State& state, const ParamType& params) {
  std::shared_ptr<Array> arr;
  params.GenerateTestData(&arr);
  while (state.KeepRunning()) {
    ABORT_NOT_OK(DictionaryEncode(arr).status());
  }
  params.SetMetadata(state);
}

constexpr int kHashBenchmarkLength = 1 << 22;

// clang-format off
std::vector<HashBenchCase> uint8_bench_cases = {
  {kHashBenchmarkLength, 200, 0},
  {kHashBenchmarkLength, 200, 0.001},
  {kHashBenchmarkLength, 200, 0.01},
  {kHashBenchmarkLength, 200, 0.1},
  {kHashBenchmarkLength, 200, 0.5},
  {kHashBenchmarkLength, 200, 0.99},
  {kHashBenchmarkLength, 200, 1}
};
// clang-format on

static void UniqueUInt8(benchmark::State& state) {
  BenchUnique(state, HashParams<UInt8Type>{uint8_bench_cases[state.range(0)]});
}

// clang-format off
std::vector<HashBenchCase> general_bench_cases = {
  {kHashBenchmarkLength, 100, 0},
  {kHashBenchmarkLength, 100, 0.001},
  {kHashBenchmarkLength, 100, 0.01},
  {kHashBenchmarkLength, 100, 0.1},
  {kHashBenchmarkLength, 100, 0.5},
  {kHashBenchmarkLength, 100, 0.99},
  {kHashBenchmarkLength, 100, 1},
  {kHashBenchmarkLength, 100000, 0},
  {kHashBenchmarkLength, 100000, 0.001},
  {kHashBenchmarkLength, 100000, 0.01},
  {kHashBenchmarkLength, 100000, 0.1},
  {kHashBenchmarkLength, 100000, 0.5},
  {kHashBenchmarkLength, 100000, 0.99},
  {kHashBenchmarkLength, 100000, 1},
};
// clang-format on

static void UniqueInt64(benchmark::State& state) {
  BenchUnique(state, HashParams<Int64Type>{general_bench_cases[state.range(0)]});
}

static void UniqueString10bytes(benchmark::State& state) {
  // Byte strings with 10 bytes each
  BenchUnique(state, HashParams<StringType>{general_bench_cases[state.range(0)], 10});
}

static void UniqueString100bytes(benchmark::State& state) {
  // Byte strings with 100 bytes each
  BenchUnique(state, HashParams<StringType>{general_bench_cases[state.range(0)], 100});
}

void HashSetArgs(benchmark::internal::Benchmark* bench) {
  for (int i = 0; i < static_cast<int>(general_bench_cases.size()); ++i) {
    bench->Arg(i);
  }
}

BENCHMARK(BuildDictionary);
BENCHMARK(BuildStringDictionary);

BENCHMARK(UniqueInt64)->Apply(HashSetArgs);
BENCHMARK(UniqueString10bytes)->Apply(HashSetArgs);
BENCHMARK(UniqueString100bytes)->Apply(HashSetArgs);

void UInt8SetArgs(benchmark::internal::Benchmark* bench) {
  for (int i = 0; i < static_cast<int>(uint8_bench_cases.size()); ++i) {
    bench->Arg(i);
  }
}

BENCHMARK(UniqueUInt8)->Apply(UInt8SetArgs);

}  // namespace compute
}  // namespace arrow

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

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/hash.h"

namespace arrow {
namespace compute {

static void BuildDictionary(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1024;

  std::vector<int64_t> values;
  std::vector<bool> is_valid;
  for (int64_t i = 0; i < iterations; i++) {
    for (int64_t j = 0; j < i; j++) {
      is_valid.push_back((i + j) % 9 == 0);
      values.push_back(j);
    }
  }

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int64Type, int64_t>(is_valid, values, &arr);

  FunctionContext ctx;

  while (state.KeepRunning()) {
    Datum out;
    ABORT_NOT_OK(DictionaryEncode(&ctx, Datum(arr), &out));
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
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

  FunctionContext ctx;

  while (state.KeepRunning()) {
    Datum out;
    ABORT_NOT_OK(DictionaryEncode(&ctx, Datum(arr), &out));
  }
  // Assuming a string here needs on average 2 bytes
  state.SetBytesProcessed(state.iterations() * total_bytes);
}

template <typename Type>
struct HashParams {
  using T = typename Type::c_type;

  double null_percent;

  void GenerateTestData(const int64_t length, const int64_t num_unique,
                        std::shared_ptr<Array>* arr) const {
    std::vector<int64_t> draws;
    std::vector<T> values;
    std::vector<bool> is_valid;
    randint<int64_t>(length, 0, num_unique, &draws);
    for (int64_t draw : draws) {
      values.push_back(static_cast<T>(draw));
    }

    if (this->null_percent > 0) {
      random_is_valid(length, this->null_percent, &is_valid);
      ArrayFromVector<Type, T>(is_valid, values, arr);
    } else {
      ArrayFromVector<Type, T>(values, arr);
    }
  }

  int64_t GetBytesProcessed(int64_t length) const { return length * sizeof(T); }
};

template <>
struct HashParams<StringType> {
  double null_percent;
  int32_t byte_width;
  void GenerateTestData(const int64_t length, const int64_t num_unique,
                        std::shared_ptr<Array>* arr) const {
    std::vector<int64_t> draws;
    randint<int64_t>(length, 0, num_unique, &draws);

    const int64_t total_bytes = this->byte_width * num_unique;
    std::vector<uint8_t> uniques(total_bytes);
    const uint32_t seed = 0;
    random_bytes(total_bytes, seed, uniques.data());

    std::vector<bool> is_valid;
    if (this->null_percent > 0) {
      random_is_valid(length, this->null_percent, &is_valid);
    }

    StringBuilder builder;
    for (int64_t i = 0; i < length; ++i) {
      if (this->null_percent == 0 || is_valid[i]) {
        ABORT_NOT_OK(builder.Append(uniques.data() + this->byte_width * draws[i],
                                    this->byte_width));
      } else {
        ABORT_NOT_OK(builder.AppendNull());
      }
    }
    ABORT_NOT_OK(builder.Finish(arr));
  }

  int64_t GetBytesProcessed(int64_t length) const { return length * byte_width; }
};

template <typename ParamType>
void BenchUnique(benchmark::State& state, const ParamType& params, int64_t length,
                 int64_t num_unique) {
  std::shared_ptr<Array> arr;
  params.GenerateTestData(length, num_unique, &arr);

  FunctionContext ctx;
  while (state.KeepRunning()) {
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(Unique(&ctx, Datum(arr), &out));
  }
  state.SetBytesProcessed(state.iterations() * params.GetBytesProcessed(length));
}

template <typename ParamType>
void BenchDictionaryEncode(benchmark::State& state, const ParamType& params,
                           int64_t length, int64_t num_unique) {
  std::shared_ptr<Array> arr;
  params.GenerateTestData(length, num_unique, &arr);

  FunctionContext ctx;
  while (state.KeepRunning()) {
    Datum out;
    ABORT_NOT_OK(DictionaryEncode(&ctx, Datum(arr), &out));
  }
  state.SetBytesProcessed(state.iterations() * params.GetBytesProcessed(length));
}

static void UniqueUInt8NoNulls(benchmark::State& state) {
  BenchUnique(state, HashParams<UInt8Type>{0}, state.range(0), state.range(1));
}

static void UniqueUInt8WithNulls(benchmark::State& state) {
  BenchUnique(state, HashParams<UInt8Type>{0.05}, state.range(0), state.range(1));
}

static void UniqueInt64NoNulls(benchmark::State& state) {
  BenchUnique(state, HashParams<Int64Type>{0}, state.range(0), state.range(1));
}

static void UniqueInt64WithNulls(benchmark::State& state) {
  BenchUnique(state, HashParams<Int64Type>{0.05}, state.range(0), state.range(1));
}

static void UniqueString10bytes(benchmark::State& state) {
  // Byte strings with 10 bytes each
  BenchUnique(state, HashParams<StringType>{0.05, 10}, state.range(0), state.range(1));
}

static void UniqueString100bytes(benchmark::State& state) {
  // Byte strings with 100 bytes each
  BenchUnique(state, HashParams<StringType>{0.05, 100}, state.range(0), state.range(1));
}

BENCHMARK(BuildDictionary);
BENCHMARK(BuildStringDictionary);

constexpr int kHashBenchmarkLength = 1 << 22;

#define ADD_HASH_ARGS(WHAT) \
  WHAT->Args({kHashBenchmarkLength, 1 << 10})->Args({kHashBenchmarkLength, 10 * 1 << 10})

ADD_HASH_ARGS(BENCHMARK(UniqueInt64NoNulls));
ADD_HASH_ARGS(BENCHMARK(UniqueInt64WithNulls));
ADD_HASH_ARGS(BENCHMARK(UniqueString10bytes));
ADD_HASH_ARGS(BENCHMARK(UniqueString100bytes));

BENCHMARK(UniqueUInt8NoNulls)
    ->Args({kHashBenchmarkLength, 200})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(UniqueUInt8WithNulls)
    ->Args({kHashBenchmarkLength, 200})
    ->Unit(benchmark::kMicrosecond);

}  // namespace compute
}  // namespace arrow

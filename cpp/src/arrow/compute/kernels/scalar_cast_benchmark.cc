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

#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;

template <typename InputType, typename CType = typename InputType::c_type>
static void BenchmarkNumericCast(benchmark::State& state,
                                 std::shared_ptr<DataType> to_type,
                                 const CastOptions& options, CType min, CType max) {
  GenericItemsArgs args(state);
  random::RandomArrayGenerator rand(kSeed);
  auto array = rand.Numeric<InputType>(args.size, min, max, args.null_proportion);
  for (auto _ : state) {
    ABORT_NOT_OK(Cast(array, to_type, options).status());
  }
}

template <typename InputType, typename CType = typename InputType::c_type>
static void BenchmarkFloatingToIntegerCast(benchmark::State& state,
                                           std::shared_ptr<DataType> from_type,
                                           std::shared_ptr<DataType> to_type,
                                           const CastOptions& options, CType min,
                                           CType max) {
  GenericItemsArgs args(state);
  random::RandomArrayGenerator rand(kSeed);
  auto array = rand.Numeric<InputType>(args.size, min, max, args.null_proportion);

  std::shared_ptr<Array> values_as_float = *Cast(*array, from_type);

  for (auto _ : state) {
    ABORT_NOT_OK(Cast(values_as_float, to_type, options).status());
  }
}

std::vector<int64_t> g_data_sizes = {kL2Size};

void CastSetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : g_data_sizes) {
    for (auto nulls : std::vector<ArgsType>({1000, 10, 2, 1, 0})) {
      bench->Args({static_cast<ArgsType>(size), nulls});
    }
  }
}

static constexpr int32_t kInt32Min = std::numeric_limits<int32_t>::min();
static constexpr int32_t kInt32Max = std::numeric_limits<int32_t>::max();

static void CastInt64ToInt32Safe(benchmark::State& state) {
  BenchmarkNumericCast<Int64Type>(state, int32(), CastOptions::Safe(), kInt32Min,
                                  kInt32Max);
}

static void CastInt64ToInt32Unsafe(benchmark::State& state) {
  BenchmarkNumericCast<Int64Type>(state, int32(), CastOptions::Unsafe(), kInt32Min,
                                  kInt32Max);
}

static void CastUInt32ToInt32Safe(benchmark::State& state) {
  BenchmarkNumericCast<UInt32Type>(state, int32(), CastOptions::Safe(), 0, kInt32Max);
}

static void CastInt64ToDoubleSafe(benchmark::State& state) {
  BenchmarkNumericCast<Int64Type>(state, float64(), CastOptions::Safe(), 0, 1000);
}

static void CastInt64ToDoubleUnsafe(benchmark::State& state) {
  BenchmarkNumericCast<Int64Type>(state, float64(), CastOptions::Unsafe(), 0, 1000);
}

static void CastDoubleToInt32Safe(benchmark::State& state) {
  BenchmarkFloatingToIntegerCast<Int32Type>(state, float64(), int32(),
                                            CastOptions::Safe(), -1000, 1000);
}

static void CastDoubleToInt32Unsafe(benchmark::State& state) {
  BenchmarkFloatingToIntegerCast<Int32Type>(state, float64(), int32(),
                                            CastOptions::Unsafe(), -1000, 1000);
}

BENCHMARK(CastInt64ToInt32Safe)->Apply(CastSetArgs);
BENCHMARK(CastInt64ToInt32Unsafe)->Apply(CastSetArgs);
BENCHMARK(CastUInt32ToInt32Safe)->Apply(CastSetArgs);

BENCHMARK(CastInt64ToDoubleSafe)->Apply(CastSetArgs);
BENCHMARK(CastInt64ToDoubleUnsafe)->Apply(CastSetArgs);
BENCHMARK(CastDoubleToInt32Safe)->Apply(CastSetArgs);
BENCHMARK(CastDoubleToInt32Unsafe)->Apply(CastSetArgs);

}  // namespace compute
}  // namespace arrow

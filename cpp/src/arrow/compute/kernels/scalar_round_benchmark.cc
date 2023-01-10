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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

// Use a fixed hash to ensure consistent results from run to run.
constexpr auto kSeed = 0x94378165;

template <typename ArrowType, RoundMode Mode, typename CType = typename ArrowType::c_type>
static void RoundArrayBenchmark(benchmark::State& state, const std::string& func_name) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(kSeed);

  // Choose values so as to avoid overflow on all ops and types.
  auto min = static_cast<CType>(6);
  auto max = static_cast<CType>(min + 15);
  auto val = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));
  RoundOptions options;
  options.round_mode = static_cast<RoundMode>(Mode);

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(func_name, {val}, &options));
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

void SetRoundArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"size", "inverse_null_proportion"});

  for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
    bench->Args({static_cast<ArgsType>(kL2Size), inverse_null_proportion});
  }
}

template <typename ArrowType, RoundMode Mode>
static void Ceil(benchmark::State& state) {
  RoundArrayBenchmark<ArrowType, Mode>(state, "ceil");
}

template <typename ArrowType, RoundMode Mode>
static void Floor(benchmark::State& state) {
  RoundArrayBenchmark<ArrowType, Mode>(state, "floor");
}

template <typename ArrowType, RoundMode Mode>
static void Round(benchmark::State& state) {
  RoundArrayBenchmark<ArrowType, Mode>(state, "round");
}

template <typename ArrowType, RoundMode Mode>
static void Trunc(benchmark::State& state) {
  RoundArrayBenchmark<ArrowType, Mode>(state, "trunc");
}

#ifdef ALL_ROUND_BENCHMARKS
#define DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, TYPE)                              \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::DOWN)->Apply(SetRoundArgs);                  \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::UP)->Apply(SetRoundArgs);                    \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::TOWARDS_ZERO)->Apply(SetRoundArgs);          \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::TOWARDS_INFINITY)->Apply(SetRoundArgs);      \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_DOWN)->Apply(SetRoundArgs);             \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_UP)->Apply(SetRoundArgs);               \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TOWARDS_ZERO)->Apply(SetRoundArgs);     \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TOWARDS_INFINITY)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TO_EVEN)->Apply(SetRoundArgs);          \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TO_ODD)->Apply(SetRoundArgs)
#else
#define DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, TYPE)                          \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::DOWN)->Apply(SetRoundArgs);              \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TOWARDS_ZERO)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(OP, TYPE, RoundMode::HALF_TO_ODD)->Apply(SetRoundArgs)
#endif

#define DECLARE_ROUND_BENCHMARKS(OP)                       \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, Int64Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, Int32Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, Int16Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, Int8Type);   \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, UInt64Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, UInt32Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, UInt16Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, UInt8Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, FloatType);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(OP, DoubleType);

DECLARE_ROUND_BENCHMARKS(Ceil);
DECLARE_ROUND_BENCHMARKS(Floor);
DECLARE_ROUND_BENCHMARKS(Round);
DECLARE_ROUND_BENCHMARKS(Trunc);

}  // namespace compute
}  // namespace arrow

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
namespace {

// Use a fixed hash to ensure consistent results from run to run.
constexpr auto kSeed = 0x94378165;

using NoOptionUnaryOp = Result<Datum>(const Datum&, ExecContext*);
using UnaryOp = Result<Datum>(const Datum&, RoundOptions, ExecContext*);
using BinaryOp = Result<Datum>(const Datum&, const Datum&, RoundBinaryOptions,
                               ExecContext*);

template <NoOptionUnaryOp& Op, typename ArrowType,
          typename CType = typename ArrowType::c_type>
static void RoundDerivativesArrayBenchmark(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(kSeed);

  // Choose values so as to avoid overflow on all ops and types.
  auto min = static_cast<CType>(6);
  auto max = static_cast<CType>(min + 15);
  auto val = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(val, NULLPTR));
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

template <UnaryOp& Op, typename ArrowType, RoundMode Mode,
          typename CType = typename ArrowType::c_type>
static void RoundArrayBenchmark(benchmark::State& state) {
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
  options.ndigits = 1;

  for (auto _ : state) {
    ABORT_NOT_OK(Op(val, options, NULLPTR));
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

template <BinaryOp& Op, typename ArrowType, RoundMode Mode,
          typename CType = typename ArrowType::c_type>
static void RoundBinaryArrayBenchmark(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(kSeed);

  // Choose values so as to avoid overflow on all ops and types.
  auto min = static_cast<CType>(6);
  auto max = static_cast<CType>(min + 15);
  auto val = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  auto val_ndigits = rand.Int32(array_size, -6, 6, args.null_proportion);

  RoundBinaryOptions options;
  options.round_mode = static_cast<RoundMode>(Mode);

  for (auto _ : state) {
    ABORT_NOT_OK(Op(val, val_ndigits, options, NULLPTR));
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

void SetRoundArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"size", "inverse_null_proportion"});

  for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
    bench->Args({static_cast<ArgsType>(kL2Size), inverse_null_proportion});
  }
}

#define DECLARE_BASIC_BENCHMARKS(BENCHMARK, OP)                       \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int64Type)->Apply(SetRoundArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int32Type)->Apply(SetRoundArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int16Type)->Apply(SetRoundArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int8Type)->Apply(SetRoundArgs);   \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt64Type)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt32Type)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt16Type)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt8Type)->Apply(SetRoundArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, FloatType)->Apply(SetRoundArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, DoubleType)->Apply(SetRoundArgs);

#ifdef ALL_ROUND_BENCHMARKS
#define DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, TYPE)                     \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::DOWN)->Apply(SetRoundArgs);         \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::UP)->Apply(SetRoundArgs);           \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::TOWARDS_ZERO)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::TOWARDS_INFINITY)                   \
      ->Apply(SetRoundArgs);                                                             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_DOWN)->Apply(SetRoundArgs);    \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_UP)->Apply(SetRoundArgs);      \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TOWARDS_ZERO)                  \
      ->Apply(SetRoundArgs);                                                             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TOWARDS_INFINITY)              \
      ->Apply(SetRoundArgs);                                                             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TO_EVEN)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TO_ODD)->Apply(SetRoundArgs)
#else
#define DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, TYPE)             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::DOWN)->Apply(SetRoundArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TOWARDS_ZERO)          \
      ->Apply(SetRoundArgs);                                                     \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, TYPE, RoundMode::HALF_TO_ODD)->Apply(SetRoundArgs)
#endif

#define DECLARE_ROUND_BENCHMARKS(BENCHMARK, OP)                       \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, Int64Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, Int32Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, Int16Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, Int8Type);   \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, UInt64Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, UInt32Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, UInt16Type); \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, UInt8Type);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, FloatType);  \
  DECLARE_ROUND_BENCHMARKS_WITH_ROUNDMODE(BENCHMARK, OP, DoubleType);

DECLARE_BASIC_BENCHMARKS(RoundDerivativesArrayBenchmark, Ceil);
DECLARE_BASIC_BENCHMARKS(RoundDerivativesArrayBenchmark, Floor);
DECLARE_BASIC_BENCHMARKS(RoundDerivativesArrayBenchmark, Trunc);

DECLARE_ROUND_BENCHMARKS(RoundArrayBenchmark, Round);
DECLARE_ROUND_BENCHMARKS(RoundBinaryArrayBenchmark, RoundBinary);

}  // namespace
}  // namespace compute
}  // namespace arrow

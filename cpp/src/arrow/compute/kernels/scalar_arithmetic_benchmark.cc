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

constexpr auto kSeed = 0x94378165;

using BinaryOp = Result<Datum>(const Datum&, const Datum&, ArithmeticOptions,
                               ExecContext*);

template <BinaryOp& Op, typename ArrowType, typename CType = typename ArrowType::c_type>
static void ArrayScalarKernel(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  Datum fifteen(CType(15));
  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, fifteen, ArithmeticOptions(), nullptr).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

template <BinaryOp& Op, typename ArrowType, typename CType = typename ArrowType::c_type>
static void ArrayArrayKernel(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));
  auto rhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, rhs, ArithmeticOptions(), nullptr).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (const auto size : {kL1Size, kL2Size}) {
    for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
      bench->Args({static_cast<ArgsType>(size), inverse_null_proportion});
    }
  }
}

#define DECLARE_ARITHMETIC_BENCHMARKS(BENCHMARK, OP)             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int64Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int32Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int16Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int8Type)->Apply(SetArgs);   \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt64Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt32Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt16Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt8Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, FloatType)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, DoubleType)->Apply(SetArgs)

DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Add);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Add);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Subtract);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Subtract);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Multiply);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Multiply);

}  // namespace compute
}  // namespace arrow

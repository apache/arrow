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
#include "arrow/compute/benchmark_util.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;

template <typename ArrowType, typename CType = typename ArrowType::c_type>
static void AddArrayScalarKernel(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  for (auto _ : state) {
    ABORT_NOT_OK(Add(lhs, Datum(CType(15))).status());
  }
}

template <typename ArrowType, typename CType = typename ArrowType::c_type>
static void AddArrayArrayKernel(benchmark::State& state) {
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
    ABORT_NOT_OK(Add(lhs, rhs).status());
  }
}

BENCHMARK_TEMPLATE(AddArrayArrayKernel, Int64Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, Int32Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, Int16Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, Int8Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, UInt64Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, UInt32Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, UInt16Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, UInt8Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, FloatType)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayArrayKernel, DoubleType)->Apply(RegressionSetArgs);

BENCHMARK_TEMPLATE(AddArrayScalarKernel, Int64Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, Int32Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, Int16Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, Int8Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, UInt64Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, UInt32Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, UInt16Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, UInt8Type)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, FloatType)->Apply(RegressionSetArgs);
BENCHMARK_TEMPLATE(AddArrayScalarKernel, DoubleType)->Apply(RegressionSetArgs);

}  // namespace compute
}  // namespace arrow

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

using BinaryOp = Result<Datum>(const Datum&, const Datum&, ExecContext*);

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
    ABORT_NOT_OK(Op(lhs, fifteen, nullptr).status());
  }
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
    ABORT_NOT_OK(Op(lhs, rhs, nullptr).status());
  }
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->Unit(benchmark::kMicrosecond);

  for (const auto size : kMemorySizes) {
    for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
      bench->Args({static_cast<ArgsType>(size), inverse_null_proportion});
    }
  }
}

// Add (Array, Array)
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Add, DoubleType)->Apply(SetArgs);

// Add (Array, Scalar)
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Add, DoubleType)->Apply(SetArgs);

// Subtract (Array, Array)
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Subtract, DoubleType)->Apply(SetArgs);

// Subtract (Array, Scalar)
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Subtract, DoubleType)->Apply(SetArgs);

// Multiply (Array, Array)
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, Multiply, DoubleType)->Apply(SetArgs);

// Multiply (Array, Scalar)
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayScalarKernel, Multiply, DoubleType)->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow

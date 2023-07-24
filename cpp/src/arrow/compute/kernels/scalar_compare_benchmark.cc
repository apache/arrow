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

template <typename Type>
static void BenchArrayScalar(benchmark::State& state, const std::string& op) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto scalar = *rand.ArrayOf(ty, 1, 0)->GetScalar(0);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(op, {array, Datum(scalar)}).status());
  }
}

template <typename Type>
static void BenchArrayArray(benchmark::State& state, const std::string& op) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto rhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(op, {lhs, rhs}).status());
  }
}

static void GreaterArrayArrayInt64(benchmark::State& state) {
  BenchArrayArray<Int64Type>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayScalarInt64(benchmark::State& state) {
  BenchArrayScalar<Int64Type>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayArrayString(benchmark::State& state) {
  BenchArrayArray<StringType>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayScalarString(benchmark::State& state) {
  BenchArrayScalar<StringType>(state, CompareOperatorToFunctionName(GREATER));
}

static void MaxElementWiseArrayArrayInt64(benchmark::State& state) {
  BenchArrayArray<Int64Type>(state, "max_element_wise");
}

static void MaxElementWiseArrayScalarInt64(benchmark::State& state) {
  BenchArrayScalar<Int64Type>(state, "max_element_wise");
}

static void MaxElementWiseArrayArrayString(benchmark::State& state) {
  BenchArrayArray<StringType>(state, "max_element_wise");
}

static void MaxElementWiseArrayScalarString(benchmark::State& state) {
  BenchArrayScalar<StringType>(state, "max_element_wise");
}

BENCHMARK(GreaterArrayArrayInt64)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarInt64)->Apply(RegressionSetArgs);

BENCHMARK(GreaterArrayArrayString)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarString)->Apply(RegressionSetArgs);

BENCHMARK(MaxElementWiseArrayArrayInt64)->Apply(RegressionSetArgs);
BENCHMARK(MaxElementWiseArrayScalarInt64)->Apply(RegressionSetArgs);

BENCHMARK(MaxElementWiseArrayArrayString)->Apply(RegressionSetArgs);
BENCHMARK(MaxElementWiseArrayScalarString)->Apply(RegressionSetArgs);

}  // namespace compute
}  // namespace arrow

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

template <CompareOperator op, typename Type>
static void CompareArrayScalar(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto scalar = *rand.ArrayOf(ty, 1, 0)->GetScalar(0);
  for (auto _ : state) {
    ABORT_NOT_OK(
        CallFunction(CompareOperatorToFunctionName(op), {array, Datum(scalar)}).status());
  }
}

template <CompareOperator op, typename Type>
static void CompareArrayArray(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto rhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(CompareOperatorToFunctionName(op), {lhs, rhs}).status());
  }
}

static void GreaterArrayArrayInt64(benchmark::State& state) {
  CompareArrayArray<GREATER, Int64Type>(state);
}

static void GreaterArrayScalarInt64(benchmark::State& state) {
  CompareArrayScalar<GREATER, Int64Type>(state);
}

static void GreaterArrayArrayString(benchmark::State& state) {
  CompareArrayArray<GREATER, StringType>(state);
}

static void GreaterArrayScalarString(benchmark::State& state) {
  CompareArrayScalar<GREATER, StringType>(state);
}

BENCHMARK(GreaterArrayArrayInt64)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarInt64)->Apply(RegressionSetArgs);

BENCHMARK(GreaterArrayArrayString)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarString)->Apply(RegressionSetArgs);

}  // namespace compute
}  // namespace arrow

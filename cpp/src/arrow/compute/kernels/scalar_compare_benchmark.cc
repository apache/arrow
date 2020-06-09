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

static void CompareArrayScalarKernel(benchmark::State& state) {
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));

  CompareOptions ge{GREATER_EQUAL};

  for (auto _ : state) {
    ABORT_NOT_OK(Compare(array, Datum(int64_t(0)), ge).status());
  }
}

static void CompareArrayArrayKernel(benchmark::State& state) {
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));
  auto rhs = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));

  CompareOptions ge(GREATER_EQUAL);
  for (auto _ : state) {
    ABORT_NOT_OK(Compare(lhs, rhs, ge).status());
  }
}

BENCHMARK(CompareArrayScalarKernel)->Apply(RegressionSetArgs);
BENCHMARK(CompareArrayArrayKernel)->Apply(RegressionSetArgs);

}  // namespace compute
}  // namespace arrow

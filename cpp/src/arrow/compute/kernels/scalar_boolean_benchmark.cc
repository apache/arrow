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

using BooleanBinaryOp = Result<Datum>(const Datum&, const Datum&, ExecContext*);

template <BooleanBinaryOp& Op>
static void ArrayArrayKernel(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size * 8;

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.Boolean(array_size, /*true_probability=*/0.5, args.null_proportion);
  auto rhs = rand.Boolean(array_size, /*true_probability=*/0.5, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, rhs, nullptr).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {kL1Size, kL2Size});
}

BENCHMARK_TEMPLATE(ArrayArrayKernel, And)->Apply(SetArgs);
BENCHMARK_TEMPLATE(ArrayArrayKernel, KleeneAnd)->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow

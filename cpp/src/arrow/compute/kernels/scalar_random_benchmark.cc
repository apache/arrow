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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

static void RandomKernel(benchmark::State& state, bool is_seed) {
  const int64_t length = state.range(0);
  const auto options =
      is_seed ? RandomOptions::FromSeed(42) : RandomOptions::FromSystemRandom();
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("random", ExecBatch({}, length), &options).status());
  }
  state.SetItemsProcessed(state.iterations() * length);
}

static void RandomKernelSystem(benchmark::State& state) {
  RandomKernel(state, /*is_seed=*/false);
}

static void RandomKernelSeed(benchmark::State& state) {
  RandomKernel(state, /*is_seed=*/true);
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t length : {1, 64, 1024, 65536}) {
    bench->Arg(length);
  }
}

BENCHMARK(RandomKernelSystem)->Apply(SetArgs);
BENCHMARK(RandomKernelSeed)->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow

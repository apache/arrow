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

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/sum.h"

namespace arrow {
namespace compute {

static void BenchmarkSum(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1UL << 12;

  std::vector<int64_t> values;
  std::vector<bool> is_valid;
  for (int64_t i = 0; i < iterations; i++) {
    for (int64_t j = 0; j < i; j++) {
      is_valid.push_back(true);
      values.push_back(j);
    }
  }

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int64Type, int64_t>(is_valid, values, &arr);

  FunctionContext ctx;

  while (state.KeepRunning()) {
    Datum out;
    ABORT_NOT_OK(Sum(&ctx, Datum(arr), &out));
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
}

BENCHMARK(BenchmarkSum)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

}  // namespace compute
}  // namespace arrow

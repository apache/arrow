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

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/tdigest.h"

namespace arrow {
namespace util {

static constexpr uint32_t kDelta = 100;
static constexpr uint32_t kBufferSize = 500;

static void BenchmarkTDigest(benchmark::State& state) {
  const size_t items = state.range(0);
  std::vector<double> values;
  random_real(items, 0x11223344, -12345678.0, 12345678.0, &values);

  for (auto _ : state) {
    arrow::internal::TDigest td(kDelta, kBufferSize);
    for (double value : values) {
      td.Add(value);
    }
    auto quantile = td.Quantile(0);
    benchmark::DoNotOptimize(quantile);
  }
  state.SetItemsProcessed(state.iterations() * items);
}

BENCHMARK(BenchmarkTDigest)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);

}  // namespace util
}  // namespace arrow

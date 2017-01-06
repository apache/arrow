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

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"

namespace arrow {

constexpr int64_t kFinalSize = 256;

static void BM_BuildPrimitiveArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    Int64Builder builder(default_memory_pool(), arrow::int64());
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      builder.Append(data.data(), data.size(), nullptr);
    }
    std::shared_ptr<Array> out;
    builder.Finish(&out);
  }
  state.SetBytesProcessed(
      state.iterations() * data.size() * sizeof(int64_t) * kFinalSize);
}

BENCHMARK(BM_BuildPrimitiveArrayNoNulls)->Repetitions(3)->Unit(benchmark::kMillisecond);

static void BM_BuildVectorNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    std::vector<int64_t> builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      builder.insert(builder.end(), data.cbegin(), data.cend());
    }
  }
  state.SetBytesProcessed(
      state.iterations() * data.size() * sizeof(int64_t) * kFinalSize);
}

BENCHMARK(BM_BuildVectorNoNulls)->Repetitions(3)->Unit(benchmark::kMillisecond);

}  // namespace arrow

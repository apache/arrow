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

#include "arrow/compute/exec_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {
namespace detail {

constexpr int32_t kSeed = 0xfede4a7e;

void BM_ExecBatchIterator(benchmark::State& state) {
  // Measure overhead related to deconstructing vector<Datum> into a sequence of ExecBatch
  random::RandomArrayGenerator rag(kSeed);

  const int64_t length = 1 << 20;
  const int num_fields = 10;

  std::vector<Datum> args(num_fields);
  for (int i = 0; i < num_fields; ++i) {
    args[i] = rag.Int64(length, 0, 100)->data();
  }

  for (auto _ : state) {
    std::unique_ptr<ExecBatchIterator> it =
        *ExecBatchIterator::Make(args, state.range(0));
    ExecBatch batch;
    while (it->Next(&batch)) {
      for (int i = 0; i < num_fields; ++i) {
        auto data = batch.values[i].array()->buffers[1]->data();
        benchmark::DoNotOptimize(data);
      }
      continue;
    }
    benchmark::DoNotOptimize(batch);
  }

  state.SetItemsProcessed(state.iterations());
}

void BM_DatumSlice(benchmark::State& state) {
  // Measure overhead related to deconstructing vector<Datum> into a sequence of ExecBatch
  random::RandomArrayGenerator rag(kSeed);

  const int64_t length = 1000;

  int num_datums = 1000;
  std::vector<Datum> datums(num_datums);
  for (int i = 0; i < num_datums; ++i) {
    datums[i] = rag.Int64(length, 0, 100)->data();
  }

  for (auto _ : state) {
    for (const Datum& datum : datums) {
      auto slice = datum.array()->Slice(16, 64);
      benchmark::DoNotOptimize(slice);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_datums);
}

BENCHMARK(BM_DatumSlice);
BENCHMARK(BM_ExecBatchIterator)->RangeMultiplier(2)->Range(256, 32768);

}  // namespace detail
}  // namespace compute
}  // namespace arrow

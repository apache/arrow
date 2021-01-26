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

#include <string>

#include "arrow/buffer.h"
#include "arrow/csv/options.h"
#include "arrow/csv/reader.h"
#include "arrow/csv/test_common.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/slow.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

static ReadOptions CreateReadOptions(bool use_threads, bool use_async) {
  auto result = csv::ReadOptions::Defaults();
  result.use_threads = use_threads;
  result.legacy_blocking_reads = !use_async;
  // Simulate larger files by using smaller block files so the impact of multiple
  // blocks is seen but we don't have to spend the time waiting on the large I/O
  result.block_size = (1 << 20) / 100;
  return result;
}

static std::shared_ptr<io::SlowInputStream> CreateStreamReader(
    std::shared_ptr<Buffer> buffer, int64_t latency_ms) {
  auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
  return std::make_shared<io::SlowInputStream>(buffer_reader, latency_ms);
}

static void BenchmarkReader(benchmark::State& state, bool use_threads, bool use_async) {
  constexpr int kNumberOfThreads = 6;
  auto latency_ms = state.range(0);
  auto num_rows = state.range(1);
  auto num_files = state.range(2);
  if (num_files > (kNumberOfThreads - 1) && use_threads && !use_async) {
    state.SkipWithError("Would deadlock");
  }
  auto input_buffer = *MakeSampleCsvBuffer(num_rows);
  // Hard coding # of threads so we don't deadlock if there are too few cores
  ASSIGN_OR_ABORT(auto thread_pool, internal::ThreadPool::Make(6));
  io::AsyncContext async_context(thread_pool.get());
  while (state.KeepRunning()) {
    std::vector<Future<std::shared_ptr<Table>>> table_futures;
    for (int i = 0; i < num_files; i++) {
      auto stream_reader = CreateStreamReader(input_buffer, latency_ms);
      auto table_reader = *csv::TableReader::Make(
          default_memory_pool(), async_context, stream_reader,
          CreateReadOptions(use_threads, use_async), csv::ParseOptions::Defaults(),
          csv::ConvertOptions::Defaults());
      if (use_async) {
        table_futures.push_back(table_reader->ReadAsync());
      } else {
        ASSERT_OK_AND_ASSIGN(auto table_future,
                             async_context.executor->Submit(
                                 [table_reader] { return table_reader->Read(); }));
        table_futures.push_back(table_future);
      }
    }
    auto combined = All(table_futures);
    ASSIGN_OR_ABORT(auto result, combined.result());
    for (auto&& table : result) {
      ABORT_NOT_OK(table);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_rows);
}

static void SerialReader(benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReader(state, false, false);
}

static void ThreadedReader(benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReader(state, true, false);
}

static void AsyncReader(benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkReader(state, true, true);
}

BENCHMARK(SerialReader)->ArgsProduct({{0, 20}, {1000}, {1, 5, 20}})->UseRealTime();
BENCHMARK(ThreadedReader)->ArgsProduct({{0, 20}, {1000}, {1, 5, 20}})->UseRealTime();
BENCHMARK(AsyncReader)->ArgsProduct({{0, 20}, {1000}, {1, 5, 20}})->UseRealTime();

}  // namespace csv
}  // namespace arrow

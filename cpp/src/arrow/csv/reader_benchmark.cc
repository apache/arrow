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
#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

static void BenchmarkReader(benchmark::State& state, bool use_threads, bool use_async) {
  auto read_options = csv::ReadOptions::Defaults();
  auto parse_options = csv::ParseOptions::Defaults();
  auto convert_options = csv::ConvertOptions::Defaults();
  read_options.use_threads = use_threads;
  read_options.legacy_blocking_reads = !use_async;
  auto input_buffer = *MakeSampleCsvBuffer(state.range(0));
  auto input_reader = std::make_shared<io::BufferReader>(input_buffer);
  while (state.KeepRunning()) {
    auto reader = *csv::TableReader::Make(default_memory_pool(), input_reader,
                                          read_options, parse_options, convert_options);
    ABORT_NOT_OK(reader->Read());
    ABORT_NOT_OK(input_reader->Seek(0));
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
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

BENCHMARK(SerialReader)->Arg(10000)->Arg(100000)->Arg(1000000)->UseRealTime();
BENCHMARK(ThreadedReader)->Arg(10000)->Arg(100000)->Arg(1000000)->UseRealTime();
BENCHMARK(AsyncReader)->Arg(10000)->Arg(100000)->Arg(1000000)->UseRealTime();

}  // namespace csv
}  // namespace arrow

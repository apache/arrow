// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "benchmark/benchmark.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/task_group.h>
#include <arrow/util/thread_pool.h>

#include <cstdlib>
#include <iostream>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;
namespace ds = arrow::dataset;
namespace csv = arrow::csv;

const int NUM_FILES = 5;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

static csv::ReadOptions MakeReadOptions(bool use_threads, bool blocking_reads) {
  csv::ReadOptions result;
  result.use_threads = use_threads;
  result.legacy_blocking_reads = blocking_reads;
  return result;
}

static csv::ParseOptions MakeParseOptions() {
  csv::ParseOptions result;
  return result;
}

static csv::ConvertOptions MakeConvertOptions() {
  csv::ConvertOptions result;
  return result;
}

static arrow::Future<> TestFileRead(benchmark::State& state, int file_index,
                                    fs::FileSystem& filesystem, bool threaded_reader,
                                    bool blocking_reads) {
  auto path = std::to_string(file_index) + ".csv";
  auto input_stream = filesystem.OpenInputStream(path).ValueOrDie();
  auto reader = csv::TableReader::Make(arrow::default_memory_pool(), input_stream,
                                       MakeReadOptions(threaded_reader, blocking_reads),
                                       MakeParseOptions(), MakeConvertOptions())
                    .ValueOrDie();
  return reader->ReadAsync().Then([reader](const std::shared_ptr<Table>& table) {
    if (table->num_rows() != 100000) {
      return arrow::Status::Invalid("Expected 100,000 rows but only got " +
                                    std::to_string(table->num_rows()));
    }
    return arrow::Status::OK();
  });
}

static void SerialTestFileSystem(benchmark::State& state, fs::FileSystem& filesystem,
                                 bool threaded_reader, bool blocking_reads) {
  for (auto file_index = 0; file_index < NUM_FILES; file_index++) {
    ABORT_ON_FAILURE(
        TestFileRead(state, file_index, filesystem, threaded_reader, blocking_reads)
            .result()
            .status());
  }
}

static void ThreadedTestFileSystem(benchmark::State& state, fs::FileSystem& filesystem,
                                   bool threaded_reader, bool blocking_reads) {
  if (blocking_reads) {
    auto task_group =
        arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool());
    task_group->Append([&] {
      for (auto file_index = 0; file_index < NUM_FILES; file_index++) {
        task_group->Append([&, file_index] {
          return TestFileRead(state, file_index, filesystem, threaded_reader,
                              blocking_reads)
              .result()
              .status();
        });
      }
      return arrow::Status::OK();
    });
    ABORT_ON_FAILURE(task_group->Finish());
  } else {
    std::vector<arrow::Future<>> futures;
    for (auto file_index = 0; file_index < NUM_FILES; file_index++) {
      futures.push_back(
          TestFileRead(state, file_index, filesystem, threaded_reader, blocking_reads));
    }
    ABORT_ON_FAILURE(arrow::All(futures).result().status());
  }
}

static void TestFileSystem(benchmark::State& state, fs::FileSystem& filesystem,
                           bool threaded_outer, bool threaded_reader,
                           bool blocking_reads) {
  for (auto _ : state) {
    if (threaded_outer) {
      ThreadedTestFileSystem(state, filesystem, threaded_reader, blocking_reads);
    } else {
      SerialTestFileSystem(state, filesystem, threaded_reader, blocking_reads);
    }
  }
}

static void TestLocalFileSystem(benchmark::State& state, bool threaded_outer,
                                bool threaded_reader, bool blocking_reads) {
  std::string local_path;
  auto local_fs = fs::SubTreeFileSystem("/home/pace/dev/data/csv",
                                        std::make_shared<fs::LocalFileSystem>());

  TestFileSystem(state, local_fs, threaded_outer, threaded_reader, blocking_reads);
}

static void TestArtificallySlowFileSystem(benchmark::State& state, bool threaded_outer,
                                          bool threaded_reader, bool blocking_reads) {
  std::string local_path;
  auto local_fs = std::make_shared<fs::SubTreeFileSystem>(
      "/home/pace/dev/data/csv", std::make_shared<fs::LocalFileSystem>());
  auto slow_fs = fs::SlowFileSystem(local_fs, 0.05);

  TestFileSystem(state, slow_fs, threaded_outer, threaded_reader, blocking_reads);
}

// static void TestS3FileSystem(benchmark::State& state, bool threaded_outer,
//                              bool threaded_reader, bool blocking_reads) {
//   auto s3_fs = fs::S3FileSystem(MakeS3Options());
// }

static void LocalFsSerialOuterSerialInner(benchmark::State& state) {
  TestLocalFileSystem(state, false, false, true);
}

static void LocalFsSerialOuterThreadedInner(benchmark::State& state) {
  TestLocalFileSystem(state, false, true, true);
}

static void LocalFsSerialOuterAsyncInner(benchmark::State& state) {
  TestLocalFileSystem(state, false, true, false);
}

static void LocalFsThreadedOuterSerialInner(benchmark::State& state) {
  TestLocalFileSystem(state, true, false, true);
}

static void LocalFsThreadedOuterAsyncInner(benchmark::State& state) {
  TestLocalFileSystem(state, true, true, false);
}

static void SlowFsSerialOuterSerialInner(benchmark::State& state) {
  TestArtificallySlowFileSystem(state, false, false, true);
}

static void SlowFsSerialOuterThreadedInner(benchmark::State& state) {
  TestArtificallySlowFileSystem(state, false, true, true);
}

static void SlowFsSerialOuterAsyncInner(benchmark::State& state) {
  TestArtificallySlowFileSystem(state, false, true, false);
}

static void SlowFsThreadedOuterSerialInner(benchmark::State& state) {
  TestArtificallySlowFileSystem(state, true, false, true);
}

static void SlowFsThreadedOuterAsyncInner(benchmark::State& state) {
  TestArtificallySlowFileSystem(state, true, true, false);
}

BENCHMARK(LocalFsSerialOuterSerialInner);
BENCHMARK(LocalFsSerialOuterThreadedInner)->UseRealTime();
BENCHMARK(LocalFsThreadedOuterSerialInner)->UseRealTime();
BENCHMARK(LocalFsSerialOuterAsyncInner)->UseRealTime();
BENCHMARK(LocalFsThreadedOuterAsyncInner)->UseRealTime();
// BENCHMARK(SlowFsSerialOuterSerialInner);
// BENCHMARK(SlowFsSerialOuterThreadedInner)->UseRealTime();
// BENCHMARK(SlowFsThreadedOuterSerialInner)->UseRealTime();
// BENCHMARK(SlowFsSerialOuterAsyncInner)->UseRealTime();
// BENCHMARK(SlowFsThreadedOuterAsyncInner)->UseRealTime();
BENCHMARK_MAIN();

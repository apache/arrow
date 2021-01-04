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

static void TestFileRead(benchmark::State& state, int file_index,
                         fs::FileSystem& filesystem, bool threaded_reader,
                         bool blocking_reads) {
  auto path = std::to_string(file_index) + ".csv";
  auto input_stream = filesystem.OpenInputStream(path).ValueOrDie();
  auto reader = csv::TableReader::Make(arrow::default_memory_pool(), input_stream,
                                       MakeReadOptions(threaded_reader, blocking_reads),
                                       MakeParseOptions(), MakeConvertOptions())
                    .ValueOrDie();
  auto table = reader->Read().ValueOrDie();
  if (table->num_rows() != 100000) {
    std::cerr << "Expected 100,000 rows but only got " << table->num_rows() << std::endl;
    abort();
  }
}

static void SerialTestFileSystem(benchmark::State& state, fs::FileSystem& filesystem,
                                 bool threaded_reader, bool blocking_reads) {
  for (auto file_index = 0; file_index < NUM_FILES; file_index++) {
    TestFileRead(state, file_index, filesystem, threaded_reader, blocking_reads);
  }
}

static void ThreadedTestFileSystem(benchmark::State& state, fs::FileSystem& filesystem,
                                   bool threaded_reader, bool blocking_reads) {
  auto task_group =
      arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool());
  task_group->Append([&] {
    for (auto file_index = 0; file_index < NUM_FILES; file_index++) {
      task_group->Append([&, file_index] {
        TestFileRead(state, file_index, filesystem, threaded_reader, blocking_reads);
        return arrow::Status::OK();
      });
    }
    return arrow::Status::OK();
  });
  ABORT_ON_FAILURE(task_group->Finish());
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

// BENCHMARK(LocalFsSerialOuterSerialInner);
// BENCHMARK(LocalFsSerialOuterThreadedInner)->UseRealTime();
// BENCHMARK(LocalFsThreadedOuterSerialInner)->UseRealTime();
BENCHMARK(LocalFsSerialOuterAsyncInner)->UseRealTime();
BENCHMARK_MAIN();

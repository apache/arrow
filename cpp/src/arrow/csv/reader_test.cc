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

#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/csv/options.h"
#include "arrow/csv/reader.h"
#include "arrow/csv/test_common.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

void StressTableReader(
    std::function<Result<std::shared_ptr<TableReader>>(std::shared_ptr<io::InputStream>)>
        reader_factory) {
  const int NTASKS = 100;
  const int NROWS = 1000;
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS));

  std::vector<Future<std::shared_ptr<Table>>> task_futures(NTASKS);
  for (int i = 0; i < NTASKS; i++) {
    auto input = std::make_shared<io::BufferReader>(table_buffer);
    ASSERT_OK_AND_ASSIGN(auto reader, reader_factory(input));
    task_futures[i] = reader->ReadAsync();
  }
  auto combined_future = All(task_futures);
  combined_future.Wait();

  ASSERT_OK_AND_ASSIGN(std::vector<Result<std::shared_ptr<Table>>> results,
                       combined_future.result());
  for (auto&& result : results) {
    ASSERT_OK_AND_ASSIGN(auto table, result);
    ASSERT_EQ(NROWS, table->num_rows());
  }
}

void TestNestedParallelism(
    std::shared_ptr<internal::ThreadPool> thread_pool,
    std::function<Result<std::shared_ptr<TableReader>>(std::shared_ptr<io::InputStream>)>
        reader_factory) {
  const int NROWS = 1000;
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS));
  auto input = std::make_shared<io::BufferReader>(table_buffer);
  ASSERT_OK_AND_ASSIGN(auto reader, reader_factory(input));

  Future<std::shared_ptr<Table>> table_future;

  auto read_task = [&reader, &table_future]() mutable {
    table_future = reader->ReadAsync();
    return Status::OK();
  };
  ASSERT_OK_AND_ASSIGN(auto future, thread_pool->Submit(read_task));
  ASSERT_TRUE(future.Wait(1));

  if (future.is_finished()) {
    ASSERT_TRUE(table_future.Wait(1));
    if (table_future.is_finished()) {
      ASSERT_OK_AND_ASSIGN(auto table, table_future.result());
      ASSERT_EQ(table->num_rows(), NROWS);
    }
  }
}  // namespace csv

TEST(SerialReaderTests, Stress) {
  auto task_factory = [](std::shared_ptr<io::InputStream> input_stream) {
    return TableReader::Make(default_memory_pool(), input_stream, ReadOptions::Defaults(),
                             ParseOptions::Defaults(), ConvertOptions::Defaults());
  };
  StressTableReader(task_factory);
}

TEST(SerialReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  auto task_factory = [](std::shared_ptr<io::InputStream> input_stream) {
    return TableReader::Make(default_memory_pool(), input_stream, ReadOptions::Defaults(),
                             ParseOptions::Defaults(), ConvertOptions::Defaults());
  };
  TestNestedParallelism(thread_pool, task_factory);
}

// Temporarily disabled until a way to shove the thread pool into the reader is devised
// TEST(ThreadedReaderTests, Stress) {
//   ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
//   auto task_factory = [&thread_pool](std::shared_ptr<io::InputStream> input_stream)
//       -> Result<std::shared_ptr<TableReader>> {
//     auto table_reader = std::make_shared<ThreadedTableReader>(
//         default_memory_pool(), input_stream, ReadOptions::Defaults(),
//         ParseOptions::Defaults(), ConvertOptions::Defaults(), thread_pool.get());
//     RETURN_NOT_OK(table_reader->Init());
//     return table_reader;
//   };
//   StressTableReader(task_factory);
// }

// Simulates deadlock that exists with ThreadedReaderTests
// TEST(ThreadedReaderTests, NestedParallelism) {
//   ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
//   auto task_factory = [&thread_pool](std::shared_ptr<io::InputStream> input_stream)
//       -> Result<std::shared_ptr<TableReader>> {
//     auto table_reader = std::make_shared<ThreadedTableReader>(
//         default_memory_pool(), input_stream, ReadOptions::Defaults(),
//         ParseOptions::Defaults(), ConvertOptions::Defaults(), thread_pool.get());
//     RETURN_NOT_OK(table_reader->Init());
//     return table_reader;
//   };
//   TestNestedParallelism(thread_pool, task_factory);
// }

// Temporarily disabled until a way to shove the thread pool into the reader is devised
// TEST(AsyncReaderTests, Stress) {
//   ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
//   auto task_factory = [&thread_pool](std::shared_ptr<io::InputStream> input_stream)
//       -> Result<std::shared_ptr<TableReader>> {
//     auto table_reader = std::make_shared<AsyncThreadedTableReader>(
//         default_memory_pool(), input_stream, ReadOptions::Defaults(),
//         ParseOptions::Defaults(), ConvertOptions::Defaults(), thread_pool.get());
//     RETURN_NOT_OK(table_reader->Init());
//     return table_reader;
//   };
//   StressTableReader(task_factory);
// }

// Temporarily disabled until a way to shove the thread pool into the reader is devised
// TEST(AsyncReaderTests, NestedParallelism) {
//   ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
//   auto task_factory = [&thread_pool](std::shared_ptr<io::InputStream> input_stream)
//       -> Result<std::shared_ptr<TableReader>> {
//     auto table_reader = std::make_shared<AsyncThreadedTableReader>(
//         default_memory_pool(), input_stream, ReadOptions::Defaults(),
//         ParseOptions::Defaults(), ConvertOptions::Defaults(), thread_pool.get());
//     RETURN_NOT_OK(table_reader->Init());
//     return table_reader;
//   };
//   TestNestedParallelism(thread_pool, task_factory);
// }

}  // namespace csv
}  // namespace arrow

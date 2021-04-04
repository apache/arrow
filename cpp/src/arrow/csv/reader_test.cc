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
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using RecordBatchGenerator = AsyncGenerator<std::shared_ptr<RecordBatch>>;

namespace csv {

// Allows the streaming reader to be used in tests that expect a table reader
class StreamingReaderAsTableReader : public TableReader {
 public:
  explicit StreamingReaderAsTableReader(std::shared_ptr<StreamingReader> reader)
      : reader_(std::move(reader)) {}
  virtual ~StreamingReaderAsTableReader() = default;
  virtual Result<std::shared_ptr<Table>> Read() {
    auto table_fut = ReadAsync();
    auto table_res = table_fut.result();
    ARROW_ASSIGN_OR_RAISE(auto table, table_res);
    return table;
  }
  virtual Future<std::shared_ptr<Table>> ReadAsync() {
    auto reader = reader_;
    RecordBatchGenerator rb_generator = [reader]() { return reader->ReadNextAsync(); };
    return CollectAsyncGenerator(rb_generator).Then([](const RecordBatchVector& rbs) {
      return Table::FromRecordBatches(rbs);
    });
  }

 private:
  std::shared_ptr<StreamingReader> reader_;
};

using TableReaderFactory =
    std::function<Result<std::shared_ptr<TableReader>>(std::shared_ptr<io::InputStream>)>;

void StressTableReader(TableReaderFactory reader_factory) {
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

void StressInvalidTableReader(TableReaderFactory reader_factory) {
  const int NTASKS = 100;
  const int NROWS = 1000;
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS, false));

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
    ASSERT_RAISES(Invalid, result);
  }
}

void TestNestedParallelism(std::shared_ptr<internal::ThreadPool> thread_pool,
                           TableReaderFactory reader_factory) {
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

  ASSERT_FINISHES_OK(future);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto table, table_future);
  ASSERT_EQ(table->num_rows(), NROWS);
}  // namespace csv

TableReaderFactory MakeSerialFactory() {
  return [](std::shared_ptr<io::InputStream> input_stream) {
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 1 << 10;
    read_options.use_threads = false;
    return TableReader::Make(io::default_io_context(), input_stream, read_options,
                             ParseOptions::Defaults(), ConvertOptions::Defaults());
  };
}

TEST(SerialReaderTests, Stress) { StressTableReader(MakeSerialFactory()); }
TEST(SerialReaderTests, StressInvalid) { StressInvalidTableReader(MakeSerialFactory()); }
TEST(SerialReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  TestNestedParallelism(thread_pool, MakeSerialFactory());
}

Result<TableReaderFactory> MakeAsyncFactory(
    std::shared_ptr<internal::ThreadPool> thread_pool = nullptr) {
  if (!thread_pool) {
    ARROW_ASSIGN_OR_RAISE(thread_pool, internal::ThreadPool::Make(1));
  }
  return [thread_pool](std::shared_ptr<io::InputStream> input_stream)
             -> Result<std::shared_ptr<TableReader>> {
    ReadOptions read_options = ReadOptions::Defaults();
    read_options.use_threads = true;
    read_options.block_size = 1 << 10;
    auto table_reader =
        TableReader::Make(io::IOContext(thread_pool.get()), input_stream, read_options,
                          ParseOptions::Defaults(), ConvertOptions::Defaults());
    return table_reader;
  };
}

TEST(AsyncReaderTests, Stress) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  StressTableReader(table_factory);
}
TEST(AsyncReaderTests, StressInvalid) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  StressInvalidTableReader(table_factory);
}
TEST(AsyncReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory(thread_pool));
  TestNestedParallelism(thread_pool, table_factory);
}

Result<TableReaderFactory> MakeStreamingFactory() {
  return [](std::shared_ptr<io::InputStream> input_stream)
             -> Result<std::shared_ptr<TableReader>> {
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 1 << 10;
    ARROW_ASSIGN_OR_RAISE(
        auto streaming_reader,
        StreamingReader::Make(io::default_io_context(), input_stream, read_options,
                              ParseOptions::Defaults(), ConvertOptions::Defaults()));
    return std::make_shared<StreamingReaderAsTableReader>(std::move(streaming_reader));
  };
}

TEST(StreamingReaderTests, Stress) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeStreamingFactory());
  StressTableReader(table_factory);
}
TEST(StreamingReaderTests, StressInvalid) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeStreamingFactory());
  StressInvalidTableReader(table_factory);
}
TEST(StreamingReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeStreamingFactory());
  TestNestedParallelism(thread_pool, table_factory);
}

}  // namespace csv
}  // namespace arrow

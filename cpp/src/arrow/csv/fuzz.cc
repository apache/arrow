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
#include <functional>
#include <memory>
#include <optional>

#include "arrow/buffer.h"
#include "arrow/csv/reader.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/fuzz_internal.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"

namespace arrow::csv {

using ::arrow::internal::GetCpuThreadPool;
using ::arrow::io::InputStream;

Status FuzzCsvReader(const uint8_t* data, int64_t size) {
  // Since the Fuzz-allocated data is not owned, any task that outlives the TableReader
  // may try to read memory that has been deallocated. Hence we wait for all pending
  // tasks to end before leaving.
  struct TaskGuard {
    ~TaskGuard() { GetCpuThreadPool()->WaitForIdle(); }
  };

  auto io_context = arrow::io::default_io_context();

  auto read_options = ReadOptions::Defaults();
  // Make chunking more likely to exercise chunked reading and optional parallelization.
  // Most files in the seed corpus are currently in the 4-10 kB range.
  read_options.block_size = 1000;
  auto parse_options = ParseOptions::Defaults();
  auto convert_options = ConvertOptions::Defaults();
  convert_options.auto_dict_encode = true;
  // This is the default value, but we might want to turn this knob to have a better
  // mix of dict-encoded and non-dict-encoded columns when reading.
  convert_options.auto_dict_max_cardinality = 50;

  // TODO should we also test non-inferring table read?

  auto read_table_serial = [=](std::shared_ptr<InputStream> input) mutable -> Status {
    read_options.use_threads = false;
    ARROW_ASSIGN_OR_RAISE(auto table_reader,
                          TableReader::Make(io_context, input, read_options,
                                            parse_options, convert_options));
    ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
    return table->ValidateFull();
  };

  auto read_table_threaded = [=](std::shared_ptr<InputStream> input) mutable -> Status {
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(auto table_reader,
                          TableReader::Make(io_context, input, read_options,
                                            parse_options, convert_options));
    ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
    return table->ValidateFull();
  };

  auto read_streaming = [=](std::shared_ptr<InputStream> input) mutable -> Status {
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(
        auto reader, StreamingReader::Make(io_context, input, read_options, parse_options,
                                           convert_options));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    return table->ValidateFull();
  };

  auto count_rows = [=](std::shared_ptr<InputStream> input) mutable -> Status {
    read_options.use_threads = true;
    parse_options.newlines_in_values = false;
    auto fut = CountRowsAsync(io_context, input, GetCpuThreadPool(), read_options,
                              parse_options);
    return fut.status();
  };

  auto count_rows_allow_newlines =
      [=](std::shared_ptr<InputStream> input) mutable -> Status {
    read_options.use_threads = true;
    parse_options.newlines_in_values = true;
    auto fut = CountRowsAsync(io_context, input, GetCpuThreadPool(), read_options,
                              parse_options);
    return fut.status();
  };

  using ReadFunc = decltype(std::function(read_table_serial));

  // Test all reader types regardless of outcome
  Status st;
  for (auto read_func : {ReadFunc(read_table_serial), ReadFunc(read_table_threaded),
                         ReadFunc(read_streaming), ReadFunc(count_rows),
                         ReadFunc(count_rows_allow_newlines)}) {
    auto input =
        std::make_shared<::arrow::io::BufferReader>(std::make_shared<Buffer>(data, size));
    TaskGuard task_guard;
    st &= read_func(input);
  }
  return st;
}

}  // namespace arrow::csv

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::csv::FuzzCsvReader(data, static_cast<int64_t>(size));
  arrow::internal::LogFuzzStatus(status, data, size);
  return 0;
}

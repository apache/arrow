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
#include <memory>
#include <optional>

#include "arrow/buffer.h"
#include "arrow/csv/reader.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"

namespace arrow::csv {

Status FuzzCsvReader(const uint8_t* data, int64_t size) {
  // Since the Fuzz-allocated data is not owned, any task that outlives the TableReader
  // may try to read memory that has been deallocated. Hence we wait for all pending
  // tasks to end before leaving.
  struct TaskGuard {
    ~TaskGuard() { ::arrow::internal::GetCpuThreadPool()->WaitForIdle(); }
  };

  auto io_context = arrow::io::default_io_context();

  auto read_options = ReadOptions::Defaults();
  // Make chunking more likely
  read_options.block_size = 4096;
  auto parse_options = ParseOptions::Defaults();
  auto convert_options = ConvertOptions::Defaults();
  convert_options.auto_dict_encode = true;

  auto input_stream =
      std::make_shared<::arrow::io::BufferReader>(std::make_shared<Buffer>(data, size));

  // TODO test other reader types
  {
    ARROW_ASSIGN_OR_RAISE(auto table_reader,
                          TableReader::Make(io_context, input_stream, read_options,
                                            parse_options, convert_options));
    TaskGuard task_guard;
    ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
    RETURN_NOT_OK(table->ValidateFull());
  }
  return Status::OK();
}

}  // namespace arrow::csv

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::csv::FuzzCsvReader(data, static_cast<int64_t>(size));
  ARROW_UNUSED(status);
  return 0;
}

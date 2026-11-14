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
#include "arrow/util/value_parsing.h"

namespace arrow::csv {

using ::arrow::internal::GetCpuThreadPool;
using ::arrow::io::InputStream;

namespace {

// Packed header prefixing the fuzz payload, used to explore the CSV reader's
// option space (see https://github.com/google/oss-fuzz/issues/14437 for why
// fuzz target parameters have to be encoded inside the payload itself).
//
// All option values are total functions of the header bytes, so every possible
// input leads to a well-defined option set. Inputs shorter than the header are
// read with the original hardcoded options, preserving the fuzzer's historical
// behavior on legacy corpus entries.
constexpr int64_t kFuzzHeaderSize = 8;

struct FuzzCsvOptions {
  ReadOptions read;
  ParseOptions parse;
  ConvertOptions convert;
};

FuzzCsvOptions MakeFuzzCsvOptions(const uint8_t* header) {
  FuzzCsvOptions opts;

  // --- ParseOptions ---
  const char kDelimiters[] = {',', '\t', ';', '|', ' '};
  opts.parse.delimiter = kDelimiters[header[0] % 5];
  opts.parse.quoting = !(header[1] & 1);
  opts.parse.quote_char = (header[1] & 2) ? '\'' : '"';
  opts.parse.double_quote = !(header[1] & 4);
  opts.parse.escaping = (header[2] & 1);
  opts.parse.escape_char = (header[2] & 2) ? '"' : '\\';
  opts.parse.newlines_in_values = (header[4] & 1);
  opts.parse.ignore_empty_lines = !(header[4] & 2);
  opts.parse.pad_short_rows = (header[4] & 4);
  opts.parse.ignore_extra_columns = (header[4] & 8);

  // --- ReadOptions ---
  const int32_t kBlockSizes[] = {1024, 4096, 65536, 1 << 20};
  opts.read.block_size = kBlockSizes[(header[3] >> 1) % 4];
  opts.read.use_threads = (header[3] & 1);
  opts.read.skip_rows = (header[7] & 1) ? 1 : 0;
  opts.read.autogenerate_column_names = (header[7] & 2);

  // --- ConvertOptions ---
  opts.convert.strings_can_be_null = (header[5] & 1);
  opts.convert.quoted_strings_can_be_null = !(header[5] & 2);
  opts.convert.auto_dict_encode = (header[5] & 4);
  const int32_t kDictMaxCardinalities[] = {50, 1000, 1000000};
  opts.convert.auto_dict_max_cardinality = kDictMaxCardinalities[(header[5] >> 3) % 3];
  opts.convert.decimal_point = (header[6] & 1) ? ',' : '.';
  opts.convert.check_utf8 = !(header[6] & 2);
  if (header[6] & 4) {
    opts.convert.timestamp_parsers.push_back(TimestampParser::MakeStrptime("%Y-%m-%d"));
  }
  if (header[6] & 8) {
    opts.convert.timestamp_parsers.push_back(TimestampParser::MakeStrptime("%m/%d/%Y"));
  }

  return opts;
}

Status ReadAllWays(io::IOContext io_context, const FuzzCsvOptions& opts,
                   std::shared_ptr<InputStream> input) {
  Status st;

  // 1. Full table read, single-threaded
  {
    auto read_options = opts.read;
    read_options.use_threads = false;
    ARROW_ASSIGN_OR_RAISE(
        auto table_reader,
        TableReader::Make(io_context, input, read_options, opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
    st &= table->ValidateFull();
  }

  // 2. Full table read, multi-threaded
  {
    auto read_options = opts.read;
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(
        auto table_reader,
        TableReader::Make(io_context, input, read_options, opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
    st &= table->ValidateFull();
  }

  // 3. Streaming (incremental) read
  {
    auto read_options = opts.read;
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        StreamingReader::Make(io_context, input, read_options, opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    st &= table->ValidateFull();
  }

  // 4. Row counting, with both values for `newlines_in_values` (row counting
  //    scans rows without conversion, so it exercises parsing on its own)
  for (const bool newlines_in_values : {false, true}) {
    auto parse_options = opts.parse;
    parse_options.newlines_in_values = newlines_in_values;
    auto fut =
        CountRowsAsync(io_context, input, GetCpuThreadPool(), opts.read, parse_options);
    st &= fut.status();
  }

  return st;
}

}  // namespace

Status FuzzCsvReader(const uint8_t* data, int64_t size) {
  // Since the Fuzz-allocated data is not owned, any task that outlives the
  // TableReader may try to read memory that has been deallocated. Hence we
  // wait for all pending tasks to end before leaving.
  struct TaskGuard {
    ~TaskGuard() { GetCpuThreadPool()->WaitForIdle(); }
  };
  TaskGuard task_guard;

  auto io_context = arrow::io::default_io_context();

  FuzzCsvOptions opts;
  const uint8_t* payload = data;
  int64_t payload_size = size;
  if (size >= kFuzzHeaderSize) {
    opts = MakeFuzzCsvOptions(data);
    payload += kFuzzHeaderSize;
    payload_size -= kFuzzHeaderSize;
    RETURN_NOT_OK(opts.parse.Validate());
    RETURN_NOT_OK(opts.read.Validate());
    RETURN_NOT_OK(opts.convert.Validate());
  } else {
    // Legacy behavior for inputs without a full header: the original
    // hardcoded options. Small block sizes make chunking more likely to be
    // exercised, as most legacy corpus entries are in the 4-10 kB range.
    opts.read = ReadOptions::Defaults();
    opts.read.block_size = 1000;
    opts.parse = ParseOptions::Defaults();
    opts.convert = ConvertOptions::Defaults();
    opts.convert.auto_dict_encode = true;
    opts.convert.auto_dict_max_cardinality = 50;
  }

  return ReadAllWays(io_context, opts,
                     std::make_shared<::arrow::io::BufferReader>(
                         std::make_shared<Buffer>(payload, payload_size)));
}

}  // namespace arrow::csv

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::csv::FuzzCsvReader(data, static_cast<int64_t>(size));
  return arrow::internal::LogFuzzStatus(status, data, size);
}

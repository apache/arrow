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

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <sstream>

#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/csv/fuzz_header_internal.h"
#include "arrow/csv/reader.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/fuzz_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/value_parsing.h"

namespace arrow::csv {

using ::arrow::internal::GetCpuThreadPool;
using ::arrow::io::InputStream;

namespace {

struct FuzzCsvOptions {
  ReadOptions read;
  ParseOptions parse;
  ConvertOptions convert;
};

FuzzCsvOptions MakeFuzzCsvOptions(const internal::FuzzCsvHeader& header) {
  FuzzCsvOptions opts;

  // --- ParseOptions ---
  // The "id" fields store an index in the corresponding candidate arrays,
  // modulo the array length (see csv/fuzz_header_internal.h).
  opts.parse.delimiter =
      internal::kFuzzCsvDelimiters[header.parse_1.delimiter_id %
                                   internal::kFuzzCsvDelimiters.size()];
  opts.parse.quoting = header.parse_1.quoting;
  opts.parse.quote_char =
      internal::kFuzzCsvQuoteChars[header.parse_1.quote_char_id %
                                   internal::kFuzzCsvQuoteChars.size()];
  opts.parse.double_quote = header.parse_1.double_quote;
  opts.parse.escaping = header.parse_1.escaping;
  opts.parse.escape_char =
      internal::kFuzzCsvEscapeChars[header.parse_1.escape_char_id %
                                    internal::kFuzzCsvEscapeChars.size()];
  opts.parse.newlines_in_values = header.parse_2.newlines_in_values;
  opts.parse.ignore_empty_lines = header.parse_2.ignore_empty_lines;
  opts.parse.pad_short_rows = header.parse_2.pad_short_rows;
  opts.parse.ignore_extra_columns = header.parse_2.ignore_extra_columns;

  // --- ReadOptions ---
  opts.read.block_size =
      internal::kFuzzCsvBlockSizes[header.read.block_size_id %
                                   internal::kFuzzCsvBlockSizes.size()];
  opts.read.use_threads = header.read.use_threads;
  opts.read.skip_rows = header.read.skip_rows;
  opts.read.autogenerate_column_names = header.read.autogenerate_column_names;

  // --- ConvertOptions ---
  opts.convert = ConvertOptions::Defaults();
  opts.convert.strings_can_be_null = header.convert_1.strings_can_be_null;
  opts.convert.quoted_strings_can_be_null = header.convert_1.quoted_strings_can_be_null;
  opts.convert.auto_dict_encode = header.convert_1.auto_dict_encode;
  opts.convert.auto_dict_max_cardinality = internal::kFuzzCsvDictMaxCardinalities
      [header.convert_1.auto_dict_max_cardinality_id %
       internal::kFuzzCsvDictMaxCardinalities.size()];
  opts.convert.decimal_point = header.convert_1.decimal_point_is_comma ? ',' : '.';
  opts.convert.check_utf8 = header.convert_1.check_utf8;
  if (header.convert_1.add_strptime_ymd_parser) {
    opts.convert.timestamp_parsers.push_back(TimestampParser::MakeStrptime("%Y-%m-%d"));
  }
  if (header.convert_2.add_strptime_mdy_parser) {
    opts.convert.timestamp_parsers.push_back(TimestampParser::MakeStrptime("%m/%d/%Y"));
  }

  return opts;
}

// Read the payload using several complementary reading strategies.
// Each strategy is given a fresh input stream, as a reader consumes the
// stream that it is passed.
Status ReadAllWays(io::IOContext io_context, const FuzzCsvOptions& opts,
                   std::span<const uint8_t> payload) {
  Status st;

  auto make_input = [&]() -> std::shared_ptr<InputStream> {
    return std::make_shared<::arrow::io::BufferReader>(
        std::make_shared<Buffer>(payload.data(), static_cast<int64_t>(payload.size())));
  };

  // The tables read below are kept around for differential testing.
  std::shared_ptr<Table> serial_table, threaded_table, streaming_table;

  // 1. Full table read, single-threaded
  auto read_table_serial = [&]() -> Status {
    auto read_options = opts.read;
    read_options.use_threads = false;
    ARROW_ASSIGN_OR_RAISE(auto table_reader,
                          TableReader::Make(io_context, make_input(), read_options,
                                            opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(serial_table, table_reader->Read());
    return serial_table->ValidateFull();
  };

  // 2. Full table read, multi-threaded
  auto read_table_threaded = [&]() -> Status {
    auto read_options = opts.read;
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(auto table_reader,
                          TableReader::Make(io_context, make_input(), read_options,
                                            opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(threaded_table, table_reader->Read());
    return threaded_table->ValidateFull();
  };

  // 3. Streaming (incremental) read
  auto read_streaming = [&]() -> Status {
    auto read_options = opts.read;
    read_options.use_threads = true;
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          StreamingReader::Make(io_context, make_input(), read_options,
                                                opts.parse, opts.convert));
    ARROW_ASSIGN_OR_RAISE(streaming_table, reader->ToTable());
    return streaming_table->ValidateFull();
  };

  // Test all reader types regardless of outcome
  st &= read_table_serial();
  st &= read_table_threaded();
  st &= read_streaming();

  // 4. Differential test: if all three table reads above succeeded, they
  //    should return equal tables (comparison ignores chunking, and NaN
  //    values compare equal as they parse deterministically). Any mismatch
  //    is a reader bug, so abort, like the Parquet encoding fuzzer does.
  if (serial_table && threaded_table && streaming_table) {
    auto equal_options = EqualOptions{}.nans_equal(true);
    if (!serial_table->Equals(*threaded_table, equal_options) ||
        !serial_table->Equals(*streaming_table, equal_options)) {
      std::stringstream ss;
      ss << "CSV reader inconsistency detected: the tables returned by the\n"
         << "single-threaded, multi-threaded and streaming CSV readers differ.\n"
         << "  serial schema:     " << serial_table->schema()->ToString() << "\n"
         << "  threaded schema:   " << threaded_table->schema()->ToString() << "\n"
         << "  streaming schema:  " << streaming_table->schema()->ToString() << "\n"
         << "  serial rows: " << serial_table->num_rows()
         << ", threaded rows: " << threaded_table->num_rows()
         << ", streaming rows: " << streaming_table->num_rows();
      ARROW_CHECK_OK(Status::Invalid(ss.str()));
    }
  }

  // 5. Row counting, with both values for `newlines_in_values` (row counting
  //    scans rows without conversion, so it exercises parsing on its own)
  for (const bool newlines_in_values : {false, true}) {
    auto parse_options = opts.parse;
    parse_options.newlines_in_values = newlines_in_values;
    auto fut = CountRowsAsync(io_context, make_input(), GetCpuThreadPool(), opts.read,
                              parse_options);
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

  std::span<const uint8_t> input(data, static_cast<size_t>(size));
  std::span<const uint8_t> payload = input;
  FuzzCsvOptions opts;

  auto maybe_header = internal::ParseFuzzCsvHeader(input);
  if (maybe_header.ok()) {
    opts = MakeFuzzCsvOptions(*maybe_header);
    payload = input.subspan(internal::kFuzzCsvHeaderSize);
    RETURN_NOT_OK(opts.parse.Validate());
    RETURN_NOT_OK(opts.read.Validate());
    RETURN_NOT_OK(opts.convert.Validate());
  } else {
    // Legacy behavior for inputs that don't start with the expected magic
    // (e.g. legacy corpus entries): the original hardcoded options. Small
    // block sizes make chunking more likely to be exercised, as most legacy
    // corpus entries are in the 4-10 kB range.
    opts.read = ReadOptions::Defaults();
    opts.read.block_size = 1000;
    opts.parse = ParseOptions::Defaults();
    opts.convert = ConvertOptions::Defaults();
    opts.convert.auto_dict_encode = true;
    opts.convert.auto_dict_max_cardinality = 50;
  }

  return ReadAllWays(io_context, opts, payload);
}

}  // namespace arrow::csv

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::csv::FuzzCsvReader(data, static_cast<int64_t>(size));
  return arrow::internal::LogFuzzStatus(status, data, size);
}

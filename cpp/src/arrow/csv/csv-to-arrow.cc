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

#include "arrow/csv/options.h"
#include "arrow/csv/reader.h"
#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/io-util.h"

#include <iostream>

#include <gflags/gflags.h>

// CLI options

// Parsing options
DEFINE_string(delimiter, ",", "Field delimiter");
DEFINE_bool(quoting, true, "Use quoting");
DEFINE_string(quote_char, "\"", "Quoting character (if `quoting` is true)");
DEFINE_bool(double_quote, true, "Quote inside a value is double-quoted");
DEFINE_bool(escaping, false, "Use escaping");
DEFINE_string(escape_char, "\\", "Escaping character (if `escaping` is true)");
DEFINE_bool(newlines_in_values, false,
            "Values are allowed to contain CR (0x0d) and LF (0x0a) characters");
DEFINE_bool(ignore_empty_lines, true, "Ignore empty lines");
DEFINE_int32(
    header_rows, 1,
    "Number of header rows to skip (including the first row containing column names)");

// Conversion options
DEFINE_bool(check_utf8, true, "Check UTF8 validity of string columns");

// Read options
DEFINE_bool(use_threads, true, "Use the global CPU thread pool");
DEFINE_int32(block_size, 1 << 20, "Block size");

// Tool options
DEFINE_bool(verbose, true, "Verbose output");

namespace arrow {
namespace csv {

static Status InitParseOptions(ParseOptions& options) {
  options.delimiter = FLAGS_delimiter.at(0);
  options.quoting = FLAGS_quoting;
  options.quote_char = FLAGS_quote_char.at(0);
  options.double_quote = FLAGS_double_quote;
  options.escaping = FLAGS_escaping;
  options.escape_char = FLAGS_escape_char.at(0);
  options.newlines_in_values = FLAGS_newlines_in_values;
  options.ignore_empty_lines = FLAGS_ignore_empty_lines;
  options.header_rows = FLAGS_header_rows;
  return Status::OK();
}

static Status InitConvertOptions(ConvertOptions& options) {
  options.check_utf8 = FLAGS_check_utf8;
  return Status::OK();
}

static Status InitReadOptions(ReadOptions& options) {
  options.use_threads = FLAGS_use_threads;
  options.block_size = FLAGS_block_size;
  return Status::OK();
}

static Status Run(int argc, char** argv) {
  gflags::SetUsageMessage("csv-to-arrow [FLAGS] input.csv  > output.arrow");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    std::cerr << "Usage: " << gflags::ProgramUsage() << std::endl;
    return Status::Invalid("Input filename missing");
  }

  // Instantiate and handle options
  auto parse_options = ParseOptions::Defaults();
  ARROW_RETURN_NOT_OK(InitParseOptions(parse_options));
  auto read_options = ReadOptions::Defaults();
  ARROW_RETURN_NOT_OK(InitReadOptions(read_options));
  auto convert_options = ConvertOptions::Defaults();
  ARROW_RETURN_NOT_OK(InitConvertOptions(convert_options));

  // Instantiate reading
  std::string input_filename(argv[1]);
  std::shared_ptr<io::ReadableFile> input_file;
  ARROW_RETURN_NOT_OK(io::ReadableFile::Open(input_filename, &input_file));
  auto pool = default_memory_pool();
  std::shared_ptr<TableReader> reader;
  ARROW_RETURN_NOT_OK(TableReader::Make(pool, input_file, read_options, parse_options,
                                        convert_options, &reader));

  // Instantiate writing, read from input and write to output
  io::StdoutStream stdout;
  std::shared_ptr<ipc::RecordBatchWriter> writer;
  bool schema_written = false;
  std::shared_ptr<arrow::Table> table;
  while (!reader->Read(&table).IsInvalid()) {
    if (!schema_written) {
      RETURN_NOT_OK(ipc::RecordBatchFileWriter::Open(&stdout, table->schema(), &writer));
      schema_written = true;
    }
    std::shared_ptr<RecordBatch> batch_out;
    TableBatchReader table_reader(*table);
    while (table_reader.ReadNext(&batch_out).ok()) {
      if (batch_out == nullptr) {
        break;
      }
      ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch_out));
    }
  }

  // Cleanup
  ARROW_RETURN_NOT_OK(writer->Close());
  gflags::ShutDownCommandLineFlags();
  return Status::OK();
}

}  // namespace csv
}  // namespace arrow

int main(int argc, char** argv) {
  arrow::Status status = arrow::csv::Run(argc, argv);
  if (!status.ok()) {
    std::cerr << "Could not convert to file: " << status.ToString() << std::endl;
  }
  return 0;
}

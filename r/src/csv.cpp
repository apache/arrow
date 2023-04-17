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

#include "./arrow_types.h"

#include "./safe-call-into-r.h"

#include <arrow/csv/reader.h>
#include <arrow/csv/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/util/value_parsing.h>

// [[arrow::export]]
std::shared_ptr<arrow::csv::WriteOptions> csv___WriteOptions__initialize(
    cpp11::list options) {
  auto res =
      std::make_shared<arrow::csv::WriteOptions>(arrow::csv::WriteOptions::Defaults());
  res->include_header = cpp11::as_cpp<bool>(options["include_header"]);
  res->batch_size = cpp11::as_cpp<int>(options["batch_size"]);
  res->io_context = MainRThread::GetInstance().CancellableIOContext();
  res->null_string = cpp11::as_cpp<std::string>(options["null_string"]);
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::ReadOptions> csv___ReadOptions__initialize(
    cpp11::list options) {
  auto res =
      std::make_shared<arrow::csv::ReadOptions>(arrow::csv::ReadOptions::Defaults());
  res->use_threads = cpp11::as_cpp<bool>(options["use_threads"]);
  res->block_size = cpp11::as_cpp<int>(options["block_size"]);
  res->skip_rows = cpp11::as_cpp<int>(options["skip_rows"]);
  res->skip_rows_after_names = cpp11::as_cpp<int>(options["skip_rows_after_names"]);
  res->column_names = cpp11::as_cpp<std::vector<std::string>>(options["column_names"]);
  res->autogenerate_column_names =
      cpp11::as_cpp<bool>(options["autogenerate_column_names"]);

  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::ParseOptions> csv___ParseOptions__initialize(
    cpp11::list options) {
  auto res =
      std::make_shared<arrow::csv::ParseOptions>(arrow::csv::ParseOptions::Defaults());
  res->delimiter = cpp11::as_cpp<char>(options["delimiter"]);
  res->quoting = cpp11::as_cpp<bool>(options["quoting"]);
  res->quote_char = cpp11::as_cpp<char>(options["quote_char"]);
  res->double_quote = cpp11::as_cpp<bool>(options["double_quote"]);
  res->escape_char = cpp11::as_cpp<char>(options["escape_char"]);
  res->newlines_in_values = cpp11::as_cpp<bool>(options["newlines_in_values"]);
  res->ignore_empty_lines = cpp11::as_cpp<bool>(options["ignore_empty_lines"]);
  return res;
}

// [[arrow::export]]
SEXP csv___ReadOptions__column_names(
    const std::shared_ptr<arrow::csv::ReadOptions>& options) {
  if (options->autogenerate_column_names) {
    return R_NilValue;
  }

  return cpp11::as_sexp(options->column_names);
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::ConvertOptions> csv___ConvertOptions__initialize(
    cpp11::list options) {
  auto res = std::make_shared<arrow::csv::ConvertOptions>(
      arrow::csv::ConvertOptions::Defaults());
  res->check_utf8 = cpp11::as_cpp<bool>(options["check_utf8"]);
  // Recognized spellings for null values
  res->null_values = cpp11::as_cpp<std::vector<std::string>>(options["null_values"]);
  // Whether string / binary columns can have null values.
  // If true, then strings in "null_values" are considered null for string columns.
  // If false, then all strings are valid string values.
  res->strings_can_be_null = cpp11::as_cpp<bool>(options["strings_can_be_null"]);

  res->true_values = cpp11::as_cpp<std::vector<std::string>>(options["true_values"]);
  res->false_values = cpp11::as_cpp<std::vector<std::string>>(options["false_values"]);

  SEXP col_types = options["col_types"];
  if (Rf_inherits(col_types, "Schema")) {
    auto schema = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(col_types);
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> column_types;
    for (const auto& field : schema->fields()) {
      column_types.insert(std::make_pair(field->name(), field->type()));
    }
    res->column_types = column_types;
  }

  res->auto_dict_encode = cpp11::as_cpp<bool>(options["auto_dict_encode"]);
  res->auto_dict_max_cardinality =
      cpp11::as_cpp<int>(options["auto_dict_max_cardinality"]);
  res->include_columns =
      cpp11::as_cpp<std::vector<std::string>>(options["include_columns"]);
  res->include_missing_columns = cpp11::as_cpp<bool>(options["include_missing_columns"]);

  SEXP op_timestamp_parsers = options["timestamp_parsers"];
  if (!Rf_isNull(op_timestamp_parsers)) {
    std::vector<std::shared_ptr<arrow::TimestampParser>> timestamp_parsers;

    // if we have a character vector, convert to arrow::StrptimeTimestampParser
    if (TYPEOF(op_timestamp_parsers) == STRSXP) {
      cpp11::strings s_timestamp_parsers(op_timestamp_parsers);
      for (cpp11::r_string s : s_timestamp_parsers) {
        timestamp_parsers.push_back(arrow::TimestampParser::MakeStrptime(s));
      }

    } else if (TYPEOF(op_timestamp_parsers) == VECSXP) {
      cpp11::list lst_parsers(op_timestamp_parsers);

      for (SEXP x : lst_parsers) {
        // handle scalar string and TimestampParser instances
        if (TYPEOF(x) == STRSXP && XLENGTH(x) == 1) {
          timestamp_parsers.push_back(
              arrow::TimestampParser::MakeStrptime(CHAR(STRING_ELT(x, 0))));
        } else if (Rf_inherits(x, "TimestampParser")) {
          timestamp_parsers.push_back(
              cpp11::as_cpp<std::shared_ptr<arrow::TimestampParser>>(x));
        } else {
          cpp11::stop(
              "unsupported timestamp parser, must be a scalar string or a "
              "<TimestampParser> object");
        }
      }

    } else {
      cpp11::stop(
          "unsupported timestamp parser, must be character vector of strptime "
          "specifications, or a list of <TimestampParser> objects");
    }
    res->timestamp_parsers = timestamp_parsers;
  }

  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::TableReader> csv___TableReader__Make(
    const std::shared_ptr<arrow::io::InputStream>& input,
    const std::shared_ptr<arrow::csv::ReadOptions>& read_options,
    const std::shared_ptr<arrow::csv::ParseOptions>& parse_options,
    const std::shared_ptr<arrow::csv::ConvertOptions>& convert_options) {
  return ValueOrStop(arrow::csv::TableReader::Make(
      MainRThread::GetInstance().CancellableIOContext(), input, *read_options,
      *parse_options, *convert_options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> csv___TableReader__Read(
    const std::shared_ptr<arrow::csv::TableReader>& table_reader) {
  auto result = RunWithCapturedRIfPossible<std::shared_ptr<arrow::Table>>(
      [&]() { return table_reader->Read(); });
  return ValueOrStop(result);
}

// [[arrow::export]]
std::string TimestampParser__kind(const std::shared_ptr<arrow::TimestampParser>& parser) {
  return parser->kind();
}

// [[arrow::export]]
std::string TimestampParser__format(
    const std::shared_ptr<arrow::TimestampParser>& parser) {
  return parser->format();
}

// [[arrow::export]]
std::shared_ptr<arrow::TimestampParser> TimestampParser__MakeStrptime(
    std::string format) {
  return arrow::TimestampParser::MakeStrptime(format);
}

// [[arrow::export]]
std::shared_ptr<arrow::TimestampParser> TimestampParser__MakeISO8601() {
  return arrow::TimestampParser::MakeISO8601();
}

// [[arrow::export]]
void csv___WriteCSV__Table(const std::shared_ptr<arrow::Table>& table,
                           const std::shared_ptr<arrow::csv::WriteOptions>& write_options,
                           const std::shared_ptr<arrow::io::OutputStream>& stream) {
  StopIfNotOk(arrow::csv::WriteCSV(*table, *write_options, stream.get()));
}

// [[arrow::export]]
void csv___WriteCSV__RecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    const std::shared_ptr<arrow::csv::WriteOptions>& write_options,
    const std::shared_ptr<arrow::io::OutputStream>& stream) {
  StopIfNotOk(arrow::csv::WriteCSV(*record_batch, *write_options, stream.get()));
}

// [[arrow::export]]
void csv___WriteCSV__RecordBatchReader(
    const std::shared_ptr<arrow::RecordBatchReader>& reader,
    const std::shared_ptr<arrow::csv::WriteOptions>& write_options,
    const std::shared_ptr<arrow::io::OutputStream>& stream) {
  StopIfNotOk(arrow::csv::WriteCSV(reader, *write_options, stream.get()));
}

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

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/csv/reader.h>

// [[arrow::export]]
std::shared_ptr<arrow::csv::ReadOptions> csv___ReadOptions__initialize(
    cpp11::list options) {
  auto res =
      std::make_shared<arrow::csv::ReadOptions>(arrow::csv::ReadOptions::Defaults());
  res->use_threads = cpp11::as_cpp<bool>(options["use_threads"]);
  res->block_size = cpp11::as_cpp<int>(options["block_size"]);
  res->skip_rows = cpp11::as_cpp<int>(options["skip_rows"]);
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
  // TODO: there are more conversion options available:
  // // Optional per-column types (disabling type inference on those columns)
  // std::unordered_map<std::string, std::shared_ptr<DataType>> column_types;
  // // Recognized spellings for boolean values
  // std::vector<std::string> true_values;
  // std::vector<std::string> false_values;

  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::TableReader> csv___TableReader__Make(
    const std::shared_ptr<arrow::io::InputStream>& input,
    const std::shared_ptr<arrow::csv::ReadOptions>& read_options,
    const std::shared_ptr<arrow::csv::ParseOptions>& parse_options,
    const std::shared_ptr<arrow::csv::ConvertOptions>& convert_options) {
  return ValueOrStop(arrow::csv::TableReader::Make(arrow::default_memory_pool(), input,
                                                   *read_options, *parse_options,
                                                   *convert_options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> csv___TableReader__Read(
    const std::shared_ptr<arrow::csv::TableReader>& table_reader) {
  return ValueOrStop(table_reader->Read());
}

#endif

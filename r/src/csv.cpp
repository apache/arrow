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

using Rcpp::CharacterVector;
using Rcpp::List_;

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
std::shared_ptr<arrow::csv::ReadOptions> csv___ReadOptions__initialize(List_ options) {
  auto res =
      std::make_shared<arrow::csv::ReadOptions>(arrow::csv::ReadOptions::Defaults());
  res->use_threads = options["use_threads"];
  res->block_size = options["block_size"];
  return res;
}

inline char get_char(CharacterVector x) { return CHAR(STRING_ELT(x, 0))[0]; }

// [[arrow::export]]
std::shared_ptr<arrow::csv::ParseOptions> csv___ParseOptions__initialize(List_ options) {
  auto res =
      std::make_shared<arrow::csv::ParseOptions>(arrow::csv::ParseOptions::Defaults());
  res->delimiter = get_char(options["delimiter"]);
  res->quoting = options["quoting"];
  res->quote_char = get_char(options["quote_char"]);
  res->double_quote = options["double_quote"];
  res->escape_char = get_char(options["escape_char"]);
  res->newlines_in_values = options["newlines_in_values"];
  res->header_rows = options["header_rows"];
  res->ignore_empty_lines = options["ignore_empty_lines"];
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::ConvertOptions> csv___ConvertOptions__initialize(
    List_ options) {
  auto res = std::make_shared<arrow::csv::ConvertOptions>(
      arrow::csv::ConvertOptions::Defaults());
  res->check_utf8 = options["check_utf8"];
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::csv::TableReader> csv___TableReader__Make(
    const std::shared_ptr<arrow::io::InputStream>& input,
    const std::shared_ptr<arrow::csv::ReadOptions>& read_options,
    const std::shared_ptr<arrow::csv::ParseOptions>& parse_options,
    const std::shared_ptr<arrow::csv::ConvertOptions>& convert_options) {
  std::shared_ptr<arrow::csv::TableReader> table_reader;
  STOP_IF_NOT_OK(arrow::csv::TableReader::Make(arrow::default_memory_pool(), input,
                                               *read_options, *parse_options,
                                               *convert_options, &table_reader));
  return table_reader;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> csv___TableReader__Read(
    const std::shared_ptr<arrow::csv::TableReader>& table_reader) {
  std::shared_ptr<arrow::Table> table;
  STOP_IF_NOT_OK(table_reader->Read(&table));
  return table;
}

#endif

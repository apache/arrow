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
#if defined(ARROW_R_WITH_JSON)

#include <arrow/json/reader.h>

// [[json::export]]
std::shared_ptr<arrow::json::ReadOptions> json___ReadOptions__initialize(bool use_threads,
                                                                         int block_size) {
  auto res =
      std::make_shared<arrow::json::ReadOptions>(arrow::json::ReadOptions::Defaults());
  res->use_threads = use_threads;
  res->block_size = block_size;
  return res;
}

// [[json::export]]
std::shared_ptr<arrow::json::ParseOptions> json___ParseOptions__initialize1(
    bool newlines_in_values) {
  auto res =
      std::make_shared<arrow::json::ParseOptions>(arrow::json::ParseOptions::Defaults());
  res->newlines_in_values = newlines_in_values;
  return res;
}

// [[json::export]]
std::shared_ptr<arrow::json::ParseOptions> json___ParseOptions__initialize2(
    bool newlines_in_values, const std::shared_ptr<arrow::Schema>& explicit_schema) {
  auto res =
      std::make_shared<arrow::json::ParseOptions>(arrow::json::ParseOptions::Defaults());
  res->newlines_in_values = newlines_in_values;
  res->explicit_schema = explicit_schema;
  return res;
}

// [[json::export]]
std::shared_ptr<arrow::json::TableReader> json___TableReader__Make(
    const std::shared_ptr<arrow::io::InputStream>& input,
    const std::shared_ptr<arrow::json::ReadOptions>& read_options,
    const std::shared_ptr<arrow::json::ParseOptions>& parse_options) {
  return ValueOrStop(arrow::json::TableReader::Make(gc_memory_pool(), input,
                                                    *read_options, *parse_options));
}

// [[json::export]]
std::shared_ptr<arrow::Table> json___TableReader__Read(
    const std::shared_ptr<arrow::json::TableReader>& table_reader) {
  return ValueOrStop(table_reader->Read());
}

#endif

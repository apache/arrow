// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// This example showcases various ways to work with Datasets. It's
// intended to be paired with the documentation.

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include "arrow/acero/exec_plan.h"
#include "arrow/compute/expression.h"

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/io/stdio.h>

#include <arrow/filesystem/filesystem.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include <arrow/util/vector.h>

#include <iostream>
#include <vector>

namespace ds = arrow::dataset;
namespace cp = arrow::compute;

char kLeftRelationCsvData[] = R"csv(lkey,shared,ldistinct
1,4,7
2,5,8
11,20,21
3,6,9)csv";

char kRightRelationCsvData[] = R"csv(rkey,shared,rdistinct
1,10,13
124,10,11
2,11,14
3,12,15)csv";

arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> CreateDataSetFromCSVData(
    bool is_left) {
  const arrow::io::IOContext& io_context = arrow::io::default_io_context();
  std::shared_ptr<arrow::io::InputStream> input;
  std::string csv_data = is_left ? kLeftRelationCsvData : kRightRelationCsvData;
  std::cout << csv_data << std::endl;
  std::string_view sv = csv_data;
  input = std::make_shared<arrow::io::BufferReader>(sv);
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Instantiate TableReader from input stream and options
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::csv::TableReader> table_reader,
                        arrow::csv::TableReader::Make(io_context, input, read_options,
                                                      parse_options, convert_options));

  // Read table from CSV file
  ARROW_ASSIGN_OR_RAISE(auto maybe_table, table_reader->Read());
  auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(maybe_table);
  arrow::Result<std::shared_ptr<arrow::dataset::InMemoryDataset>> result(std::move(ds));
  return result;
}

arrow::Status DoHashJoin() {
  arrow::dataset::internal::Initialize();

  ARROW_ASSIGN_OR_RAISE(auto l_dataset, CreateDataSetFromCSVData(true));
  ARROW_ASSIGN_OR_RAISE(auto r_dataset, CreateDataSetFromCSVData(false));

  auto l_options = std::make_shared<arrow::dataset::ScanOptions>();
  // create empty projection: "default" projection where each field is mapped to a
  // field_ref
  l_options->projection = cp::project({}, {});

  auto r_options = std::make_shared<arrow::dataset::ScanOptions>();
  // create empty projection: "default" projection where each field is mapped to a
  // field_ref
  r_options->projection = cp::project({}, {});

  // construct the scan node
  auto l_scan_node_options = arrow::dataset::ScanNodeOptions{l_dataset, l_options};
  auto r_scan_node_options = arrow::dataset::ScanNodeOptions{r_dataset, r_options};

  arrow::acero::Declaration left{"scan", std::move(l_scan_node_options)};
  arrow::acero::Declaration right{"scan", std::move(r_scan_node_options)};

  arrow::acero::HashJoinNodeOptions join_opts{arrow::acero::JoinType::INNER,
                                              /*in_left_keys=*/{"lkey"},
                                              /*in_right_keys=*/{"rkey"},
                                              /*filter*/ arrow::compute::literal(true),
                                              /*output_suffix_for_left*/ "_l",
                                              /*output_suffix_for_right*/ "_r"};

  arrow::acero::Declaration hashjoin{
      "hashjoin", {std::move(left), std::move(right)}, join_opts};

  // expected columns l_a, l_b
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> response_table,
                        arrow::acero::DeclarationToTable(std::move(hashjoin)));

  std::cout << "Results : " << response_table->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = DoHashJoin();
  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

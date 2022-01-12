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
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/csv/api.h>

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

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

std::string GetDataAsCsvString(std::string relation) {
  std::string data_str = "";
  if (relation == "l") {
    data_str = R"csv(lkey,shared,ldistinct
1,4,7
2,5,8
11,20,21
3,6,9)csv";
  } else if (relation == "r") {
    data_str = R"csv(rkey,shared,rdistinct
1,10,13
124,10,11
2,11,14
3,12,15)csv";
  } else {
    return data_str;
  }
  return data_str;
}

arrow::Result<std::shared_ptr<arrow::Table>> GetTableFromExecBatches(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<arrow::compute::ExecBatch>& exec_batches) {
  arrow::RecordBatchVector batches;
  for (const auto& batch : exec_batches) {
    ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema));
    batches.push_back(std::move(rb));
  }
  return arrow::Table::FromRecordBatches(schema, batches);
}

arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> CreateDataSetFromCSVData(
    std::string relation) {
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  std::shared_ptr<arrow::io::InputStream> input;
  std::string csv_data = GetDataAsCsvString(relation);
  std::cout << "CSV DATA : " << relation << std::endl;
  std::cout << csv_data << std::endl;
  arrow::util::string_view sv = csv_data;
  input = std::make_shared<arrow::io::BufferReader>(sv);
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Instantiate TableReader from input stream and options
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::csv::TableReader> table_reader,
                        arrow::csv::TableReader::Make(io_context, input, read_options,
                                                      parse_options, convert_options));

  std::shared_ptr<arrow::csv::TableReader> reader = table_reader;

  // Read table from CSV file
  ARROW_ASSIGN_OR_RAISE(auto maybe_table, reader->Read());
  auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(maybe_table);
  arrow::Result<std::shared_ptr<arrow::dataset::InMemoryDataset>> result(std::move(ds));
  return result;
}

cp::Expression Materialize(std::vector<std::string> names) {
  std::vector<cp::Expression> exprs;
  for (const auto& name : names) {
    exprs.push_back(cp::field_ref(name));
  }

  return cp::project(exprs, names);
}

arrow::Status DoHashJoin() {
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());

  arrow::dataset::internal::Initialize();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  cp::ExecNode* left_source;
  cp::ExecNode* right_source;

  ARROW_ASSIGN_OR_RAISE(auto l_dataset, CreateDataSetFromCSVData("l"));
  ARROW_ASSIGN_OR_RAISE(auto r_dataset, CreateDataSetFromCSVData("r"));

  auto l_options = std::make_shared<arrow::dataset::ScanOptions>();
  // sync scanning is not supported by ScanNode
  l_options->use_async = true;
  l_options->projection = Materialize({});  // create empty projection

  auto r_options = std::make_shared<arrow::dataset::ScanOptions>();
  // sync scanning is not supported by ScanNode
  r_options->use_async = true;
  r_options->projection = Materialize({});  // create empty projection

  // construct the scan node
  auto l_scan_node_options = arrow::dataset::ScanNodeOptions{l_dataset, l_options};
  auto r_scan_node_options = arrow::dataset::ScanNodeOptions{r_dataset, r_options};

  ARROW_ASSIGN_OR_RAISE(left_source,
                        cp::MakeExecNode("scan", plan.get(), {}, l_scan_node_options));
  ARROW_ASSIGN_OR_RAISE(right_source,
                        cp::MakeExecNode("scan", plan.get(), {}, r_scan_node_options));

  arrow::compute::HashJoinNodeOptions join_opts{
      arrow::compute::JoinType::INNER,
      /*left_keys=*/{"lkey"},
      /*right_keys=*/{"rkey"},         arrow::compute::literal(true), "_l", "_r"};

  ARROW_ASSIGN_OR_RAISE(
      auto hashjoin,
      cp::MakeExecNode("hashjoin", plan.get(), {left_source, right_source}, join_opts));

  ARROW_ASSIGN_OR_RAISE(std::ignore, cp::MakeExecNode("sink", plan.get(), {hashjoin},
                                                      cp::SinkNodeOptions{&sink_gen}));
  // expected columns l_a, l_b
  std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
      hashjoin->output_schema(), std::move(sink_gen), exec_context.memory_pool());

  // // validate the ExecPlan
  ABORT_ON_FAILURE(plan->Validate());
  // // // start the ExecPlan
  ABORT_ON_FAILURE(plan->StartProducing());

  // // // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;

  ARROW_ASSIGN_OR_RAISE(response_table,
                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

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

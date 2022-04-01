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

// TODO: prune these
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/tpch_node.h>
#if defined(ARROW_R_WITH_DATASET)
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>

namespace compute = ::arrow::compute;
namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;

static std::shared_ptr<compute::ExecNode> MakeExecNodeOrStop(
    const std::string& factory_name, compute::ExecPlan* plan,
    std::vector<compute::ExecNode*> inputs, const compute::ExecNodeOptions& options) {
  return std::shared_ptr<compute::ExecNode>(
      ValueOrStop(compute::MakeExecNode(factory_name, plan, std::move(inputs), options)),
      [](...) {
        // empty destructor: ExecNode lifetime is managed by an ExecPlan
      });
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> Tpch_Dbgen(
    const std::shared_ptr<compute::ExecPlan>& plan, double scale_factor,
    std::string table_name) {
  auto gen =
      ValueOrStop(arrow::compute::internal::TpchGen::Make(plan.get(), scale_factor, 1000000));

  compute::ExecNode* table;
  if (table_name == "part") {
    table = ValueOrStop(gen->Part());
  } else if (table_name == "supplier") {
    table = ValueOrStop(gen->Supplier());
  } else if (table_name == "partsupp") {
    table = ValueOrStop(gen->PartSupp());
  } else if (table_name == "customer") {
    table = ValueOrStop(gen->Customer());
  } else if (table_name == "nation") {
    table = ValueOrStop(gen->Nation());
  } else if (table_name == "lineitem") {
    table = ValueOrStop(gen->Lineitem());
  } else if (table_name == "region") {
    table = ValueOrStop(gen->Region());
  } else if (table_name == "orders") {
    table = ValueOrStop(gen->Orders());
  } else {
    cpp11::stop("That's not a valid table name");
  }

  arrow::AsyncGenerator<arrow::util::optional<compute::ExecBatch>> sink_gen;

  MakeExecNodeOrStop("sink", plan.get(), {table}, compute::SinkNodeOptions{&sink_gen});

  StopIfNotOk(plan->Validate());
  StopIfNotOk(plan->StartProducing());

  // If the generator is destroyed before being completely drained, inform plan
  std::shared_ptr<void> stop_producing{nullptr, [plan](...) {
                                         bool not_finished_yet =
                                             plan->finished().TryAddCallback([&plan] {
                                               return [plan](const arrow::Status&) {};
                                             });

                                         if (not_finished_yet) {
                                           plan->StopProducing();
                                         }
                                       }};

  return compute::MakeGeneratorReader(
      table->output_schema(), [stop_producing, plan, sink_gen] { return sink_gen(); },
      gc_memory_pool());
}

void Queue_Write_One_Table(const std::shared_ptr<compute::ExecPlan>& plan,
                           std::string table_name, compute::ExecNode* table,
                           std::string base_path,
                           const std::shared_ptr<fs::FileSystem>& filesystem) {
  auto format = std::make_shared<ds::ParquetFileFormat>();

  auto partitioning_factory = arrow::dataset::HivePartitioning::MakeFactory();
  std::vector<std::shared_ptr<arrow::Field>> fields;
  auto partitioning = ValueOrStop(partitioning_factory->Finish(arrow::schema(fields)));

  ds::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = format->DefaultWriteOptions();
  write_options.existing_data_behavior =
      ds::ExistingDataBehavior::kDeleteMatchingPartitions;
  write_options.filesystem = filesystem;
  write_options.base_dir = base_path + "/" + table_name;
  write_options.partitioning = partitioning;
  write_options.basename_template = "data-{i}.parquet";
  write_options.max_partitions = 1024;

  const ds::WriteNodeOptions options =
      ds::WriteNodeOptions{write_options, table->output_schema()};

  MakeExecNodeOrStop("write", plan.get(), {table}, options);
}

// [[arrow::export]]
void Tpch_Dbgen_Write(const std::shared_ptr<compute::ExecPlan>& plan, double scale_factor,
                      const std::shared_ptr<fs::FileSystem>& filesystem,
                      std::string base_dir, std::string folder_name) {
  arrow::dataset::internal::Initialize();

  auto gen =
      ValueOrStop(arrow::compute::internal::TpchGen::Make(plan.get(), scale_factor, 1000000));

  auto base_path = base_dir + folder_name;
  (void)filesystem->CreateDir(base_path);

  static std::string tables[] = {"part",   "supplier", "partsupp", "customer",
                                 "nation", "lineitem", "region",   "orders"};
  for (auto& table_name : tables) {
    compute::ExecNode* table;
    if (table_name == "part") {
      table = ValueOrStop(gen->Part());
    } else if (table_name == "supplier") {
      table = ValueOrStop(gen->Supplier());
    } else if (table_name == "partsupp") {
      table = ValueOrStop(gen->PartSupp());
    } else if (table_name == "customer") {
      table = ValueOrStop(gen->Customer());
    } else if (table_name == "nation") {
      table = ValueOrStop(gen->Nation());
    } else if (table_name == "lineitem") {
      table = ValueOrStop(gen->Lineitem());
    } else if (table_name == "region") {
      table = ValueOrStop(gen->Region());
    } else if (table_name == "orders") {
      table = ValueOrStop(gen->Orders());
    } else {
      cpp11::stop("That's not a valid table name");
    }
    Queue_Write_One_Table(plan, table_name, table, base_path, filesystem);
  }
  StopIfNotOk(plan->Validate());
  StopIfNotOk(plan->StartProducing());
  StopIfNotOk(plan->finished().status());
}

#endif

#endif

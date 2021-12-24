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

#include <memory>
#include <utility>

#include <arrow/compute/api.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/ir_consumer.h>
#include <arrow/compute/exec/test_util.h>

#include <arrow/csv/api.h>

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>
#include <arrow/dataset/dataset_writer.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/io/slow.h>
#include <arrow/io/transform.h>
#include <arrow/io/stdio.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <arrow/ipc/api.h>

#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

// Demonstrate various operators in Arrow Streaming Execution Engine

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

#define CHECK_AND_RETURN(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    } else {                                       \
      return EXIT_SUCCESS;                         \
    }                                              \
  } while (0);

#define CHECK_AND_CONTINUE(expr)                   \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

constexpr char kSep[] = "******";

#define PRINT_BLOCK(msg)                                                     \
  std::cout << "" << std::endl;                                              \
  std::cout << "\t" << kSep << " " << msg << " " << kSep << std::endl; \
  std::cout << "" << std::endl;

#define PRINT_LINE(msg) std::cout << msg << std::endl;

namespace cp = ::arrow::compute;

std::shared_ptr<arrow::Array> GetArrayFromJSON(
    const std::shared_ptr<arrow::DataType>& type, arrow::util::string_view json) {
    std::shared_ptr<arrow::Array> out;
    ABORT_ON_FAILURE(arrow::ipc::internal::json::ArrayFromJSON(type, json, &out));
  return out;
}

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromJSON(
    const std::shared_ptr<arrow::Schema>& schema, arrow::util::string_view json) {
  // Parse as a StructArray
  auto struct_type = struct_(schema->fields());
  std::shared_ptr<arrow::Array> struct_array = GetArrayFromJSON(struct_type, json);

  // Convert StructArray to RecordBatch
  return *arrow::RecordBatch::FromStructArray(struct_array);
}

std::shared_ptr<arrow::Table> GetTableFromJSON(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::string>& json) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (const std::string& batch_json : json) {
    batches.push_back(GetRecordBatchFromJSON(schema, batch_json));
  }
  return *arrow::Table::FromRecordBatches(schema, std::move(batches));
}

std::shared_ptr<arrow::Table> CreateTable() {
    auto schema =
        arrow::schema({arrow::field("a", arrow::int64()),
        arrow::field("b", arrow::int64()),
        arrow::field("c", arrow::int64())});
    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ABORT_ON_FAILURE(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ABORT_ON_FAILURE(builder.Finish(&array_a));
    builder.Reset();
    ABORT_ON_FAILURE(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ABORT_ON_FAILURE(builder.Finish(&array_b));
    builder.Reset();
    ABORT_ON_FAILURE(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ABORT_ON_FAILURE(builder.Finish(&array_c));
    return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

std::string GetDataAsCsvString() {
    std::string data_str = "";

    data_str.append("a,b\n");
    data_str.append("1,null\n");
    data_str.append("2,true\n");
    data_str.append("null,true\n");
    data_str.append("3,false\n");
    data_str.append("null,true\n");
    data_str.append("4,false\n");
    data_str.append("5,null\n");
    data_str.append("6,false\n");
    data_str.append("7,false\n");
    data_str.append("8,true\n");

    return data_str;
}

arrow::Status CreateDataSetFromCSVData(
    std::shared_ptr<arrow::dataset::InMemoryDataset> &dataset) {
    arrow::io::IOContext io_context = arrow::io::default_io_context();
    std::shared_ptr<arrow::io::InputStream> input;
    std::string csv_data = GetDataAsCsvString();
    arrow::util::string_view sv = csv_data;
    input = std::make_shared<arrow::io::BufferReader>(sv);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::csv::TableReader> table_reader,
      arrow::csv::TableReader::Make(io_context,
                                    input,
                                    read_options,
                                    parse_options,
                                    convert_options));

    std::shared_ptr<arrow::csv::TableReader> reader = table_reader;

    // Read table from CSV file
    ARROW_ASSIGN_OR_RAISE(auto maybe_table,
      reader->Read());
    auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(maybe_table);
    dataset = std::move(ds);
    return arrow::Status::OK();
}

std::shared_ptr<arrow::dataset::Dataset> CreateDataset() {
    std::shared_ptr<arrow::dataset::InMemoryDataset> im_dataset;
    ABORT_ON_FAILURE(CreateDataSetFromCSVData(im_dataset));
    return im_dataset;
}

arrow::Status exec_plan_end_to_end_sample() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                            cp::ExecPlan::Make(&exec_context));

    std::shared_ptr<arrow::dataset::Dataset> dataset = CreateDataset();

    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    // sync scanning is not supported by ScanNode
    options->use_async = true;
    // specify the filter
    cp::Expression b_is_true = cp::field_ref("b");
    options->filter = b_is_true;
    // for now, specify the projection as the full project expression (eventually this can
    // just be a list of materialized field names)

    cp::Expression a_times_2 = cp::call("multiply", {cp::field_ref("a"), cp::literal(2)});
    options->projection =
        cp::call("make_struct", {a_times_2}, cp::MakeStructOptions{{"a * 2"}});

    // // construct the scan node
    cp::ExecNode* scan;

    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

    ARROW_ASSIGN_OR_RAISE(scan,
                            cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

    // pipe the scan node into a filter node
    cp::ExecNode* filter;
    ARROW_ASSIGN_OR_RAISE(filter, cp::MakeExecNode("filter", plan.get(), {scan},
                                                    cp::FilterNodeOptions{b_is_true}));

    cp::ExecNode* project;

    ARROW_ASSIGN_OR_RAISE(project,
                            cp::MakeExecNode("augmented_project", plan.get(), {filter},
                                            cp::ProjectNodeOptions{{a_times_2}}));

    // // finally, pipe the project node into a sink node
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * sink,
                            cp::MakeExecNode("sink",
                            plan.get(), {project},
                            cp::SinkNodeOptions{&sink_gen}));

    ABORT_ON_FAILURE(sink->Validate());

    // // translate sink_gen (async) to sink_reader (sync)
    std::shared_ptr<arrow::RecordBatchReader> sink_reader =
        cp::MakeGeneratorReader(arrow::schema({arrow::field("a * 2", arrow::int32())}),
                                std::move(sink_gen), exec_context.memory_pool());

    // // validate the plan
    ABORT_ON_FAILURE(plan->Validate());
    PRINT_LINE("Exec Plan created: " << plan->ToString());
    // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table,
                            arrow::Table::FromRecordBatchReader(sink_reader.get()));

    std::cout << "Results : " << response_table->ToString() << std::endl;

    // // stop producing
    plan->StopProducing();

    // // plan mark finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

cp::Expression Materialize(std::vector<std::string> names,
                           bool include_aug_fields = false) {
    if (include_aug_fields) {
        for (auto aug_name : {"__fragment_index",
            "__batch_index", "__last_in_fragment"}) {
        names.emplace_back(aug_name);
        }
    }

    std::vector<cp::Expression> exprs;
    for (const auto& name : names) {
        exprs.push_back(cp::field_ref(name));
    }

    return cp::project(exprs, names);
}

arrow::Status consume(std::shared_ptr<arrow::Schema> schema,
    std::function<arrow::Future<arrow::util::optional<cp::ExecBatch>>()>* sink_gen) {
    auto iterator = MakeGeneratorIterator(*sink_gen);
    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto exec_batch, iterator.Next());
        if (!exec_batch.has_value()) {
            break;
        }
        ARROW_ASSIGN_OR_RAISE(auto record_batch, exec_batch->ToRecordBatch(schema));
        std::cout << record_batch->ToString() << '\n';
    }
    return arrow::Status::OK();
}


arrow::Status scan_sink_node_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    // Execution plan created
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<cp::ExecPlan> plan, cp::ExecPlan::Make(&exec_context));

    std::shared_ptr<arrow::dataset::Dataset> dataset = CreateDataset();

    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    // sync scanning is not supported by ScanNode
    options->use_async = true;
    options->projection = Materialize({});  // create empty projection

    // construct the scan node
    cp::ExecNode* scan;
    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

    ARROW_ASSIGN_OR_RAISE(scan,
                            cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    cp::ExecNode* sink;

    ARROW_ASSIGN_OR_RAISE(
        sink, cp::MakeExecNode("sink", plan.get(), {scan},
        cp::SinkNodeOptions{&sink_gen}));

    // // translate sink_gen (async) to sink_reader (sync)
    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        dataset->schema(), std::move(sink_gen), exec_context.memory_pool());

    // validate the ExecPlan
    ABORT_ON_FAILURE(plan->Validate());
    PRINT_LINE("ExecPlan created : " << plan->ToString());
    // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
                            arrow::Table::FromRecordBatchReader(sink_reader.get()));

    PRINT_LINE("Results : " << response_table->ToString());

    // // stop producing
    plan->StopProducing();
    // // plan mark finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

cp::ExecBatch GetExecBatchFromJSON(const std::vector<arrow::ValueDescr>& descrs,
                                   arrow::util::string_view json) {
  auto fields = ::arrow::internal::MapVector(
      [](const arrow::ValueDescr& descr) { return arrow::field("", descr.type); },
      descrs);

  cp::ExecBatch batch{*GetRecordBatchFromJSON(arrow::schema(std::move(fields)), json)};

  auto value_it = batch.values.begin();
  for (const auto& descr : descrs) {
    if (descr.shape == arrow::ValueDescr::SCALAR) {
      if (batch.length == 0) {
        *value_it = arrow::MakeNullScalar(value_it->type());
      } else {
        *value_it = value_it->make_array()->GetScalar(0).ValueOrDie();
      }
    }
    ++value_it;
  }

  return batch;
}

struct BatchesWithSchema {
  std::vector<cp::ExecBatch> batches;
  std::shared_ptr<arrow::Schema> schema;

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen(bool parallel) const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](cp::ExecBatch batch) { return arrow::util::make_optional(std::move(batch)); },
        batches);

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen;

    if (parallel) {
      // emulate batches completing initial decode-after-scan on a cpu thread
      gen = arrow::MakeBackgroundGenerator(
                arrow::MakeVectorIterator(std::move(opt_batches)),
                ::arrow::internal::GetCpuThreadPool())
                .ValueOrDie();

      // ensure that callbacks are not executed immediately on a background thread
      gen = arrow::MakeTransferredGenerator(std::move(gen),
                                            ::arrow::internal::GetCpuThreadPool());
    } else {
      gen = arrow::MakeVectorGenerator(std::move(opt_batches));
    }

    return gen;
  }
};

BatchesWithSchema MakeBasicBatches() {
  BatchesWithSchema out;
  out.batches = {GetExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[null, true], [4, false]]"),
                 GetExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[5, null], [6, false], [7, false]]")};
  out.schema = arrow::schema(
      {arrow::field("a", arrow::int32()), arrow::field("b", arrow::boolean())});
  return out;
}

BatchesWithSchema MakeSortTestBasicBatches() {
  BatchesWithSchema out;
  out.batches = {
      GetExecBatchFromJSON(
          {arrow::int32(), arrow::int32(), arrow::int32(), arrow::int32()},
          "[[1, 3, 0, 2], [121, 101, 120, 12], [10, 110, 210, 121], [51, 101, 2, 34]]"),
      GetExecBatchFromJSON(
          {arrow::int32(), arrow::int32(), arrow::int32(), arrow::int32()},
          "[[11, 31, 1, 12], [12, 101, 120, 12], [0, 110, 210, 11], [51, 10, 2, 3]]")
  };
  out.schema = arrow::schema(
      {arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()),
       arrow::field("c", arrow::int32()), arrow::field("d", arrow::int32())});
  return out;
}

BatchesWithSchema MakeGroupableBatches(int multiplicity = 1) {
  BatchesWithSchema out;

  out.batches = {GetExecBatchFromJSON({arrow::int32(), arrow::utf8()}, R"([
                   [12, "alfa"],
                   [7,  "beta"],
                   [3,  "alfa"]
                 ])"),
                 GetExecBatchFromJSON({arrow::int32(), arrow::utf8()}, R"([
                   [-2, "alfa"],
                   [-1, "gama"],
                   [3,  "alfa"]
                 ])"),
                 GetExecBatchFromJSON({arrow::int32(), arrow::utf8()}, R"([
                   [5,  "gama"],
                   [3,  "beta"],
                   [-8, "alfa"]
                 ])")};

  size_t batch_count = out.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out.batches.push_back(out.batches[i]);
    }
  }

  out.schema = arrow::schema(
      {arrow::field("i32", arrow::int32()), arrow::field("str", arrow::utf8())});

  return out;
}

std::shared_ptr<arrow::internal::ThreadPool> MakeIOThreadPool() {
  auto maybe_pool = arrow::internal::ThreadPool::MakeEternal(/*threads=*/8);
  if (!maybe_pool.ok()) {
    maybe_pool.status().Abort("Failed to create global IO thread pool");
  }
  return *std::move(maybe_pool);
}

arrow::internal::ThreadPool* GetIOThreadPool() {
  static std::shared_ptr<arrow::internal::ThreadPool> pool = MakeIOThreadPool();
  return pool.get();
}

arrow::Status source_sink_example() {
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
  cp::ExecPlan::Make(&exec_context));

  auto basic_data = MakeBasicBatches();

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options =
      cp::SourceNodeOptions{basic_data.schema, basic_data.gen(false)};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  cp::ExecNode* sink;

  ARROW_ASSIGN_OR_RAISE(sink, cp::MakeExecNode("sink", plan.get(), {source},
                                               cp::SinkNodeOptions{&sink_gen}));

  // // // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
      basic_data.schema, std::move(sink_gen), exec_context.memory_pool());

  // // validate the ExecPlan
  ABORT_ON_FAILURE(plan->Validate());
  PRINT_LINE("Exec Plan Created: " << plan->ToString());
  // // // start the ExecPlan
  ABORT_ON_FAILURE(plan->StartProducing());

  // // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;

  ARROW_ASSIGN_OR_RAISE(response_table,
                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

  PRINT_LINE("Results : " << response_table->ToString());

  // // plan stop producing
  plan->StopProducing();
  // // plan mark finished
  plan->finished().Wait();

  return arrow::Status::OK();
}

arrow::Status scan_filter_sink_example() {
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());

  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  std::shared_ptr<arrow::dataset::Dataset> dataset = CreateDataset();

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // sync scanning is not supported by ScanNode
  options->use_async = true;
  // specify the filter
  cp::Expression b_is_true = cp::field_ref("b");
  options->filter = b_is_true;
  // empty projection
  options->projection = Materialize({});

  // construct the scan node
  PRINT_LINE("Initialized Scanning Options");

  cp::ExecNode* scan;

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
  PRINT_LINE("Scan node options created");

  ARROW_ASSIGN_OR_RAISE(scan,
                        cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

  // pipe the scan node into a filter node
  cp::ExecNode* filter;
  ARROW_ASSIGN_OR_RAISE(filter, cp::MakeExecNode("filter", plan.get(), {scan},
                                                 cp::FilterNodeOptions{b_is_true}));

  // // finally, pipe the project node into a sink node
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * sink,
      cp::MakeExecNode("sink", plan.get(), {filter}, cp::SinkNodeOptions{&sink_gen}));

  ABORT_ON_FAILURE(sink->Validate());
  // // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
      dataset->schema(), std::move(sink_gen), exec_context.memory_pool());

  // // validate the ExecPlan
  ABORT_ON_FAILURE(plan->Validate());
  PRINT_LINE("Exec Plan created " << plan->ToString());
  // // start the ExecPlan
  ABORT_ON_FAILURE(plan->StartProducing());

  // // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;
  ARROW_ASSIGN_OR_RAISE(response_table,
                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

  PRINT_LINE("Results : " << response_table->ToString());
  // // plan stop producing
  plan->StopProducing();
  // /// plan marked finished
  plan->finished().Wait();

  return arrow::Status::OK();
}

arrow::Status scan_project_sink_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
    cp::ExecPlan::Make(&exec_context));

    std::shared_ptr<arrow::dataset::Dataset> dataset = CreateDataset();

    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    // sync scanning is not supported by ScanNode
    options->use_async = true;
    // projection
    cp::Expression a_times_2 = cp::call("multiply", {cp::field_ref("a"),
    cp::literal(2)}); options->projection =
        cp::call("make_struct", {a_times_2}, cp::MakeStructOptions{{"a * 2"}});

    cp::ExecNode *scan;

    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

    ARROW_ASSIGN_OR_RAISE(scan, cp::MakeExecNode("scan", plan.get(), {},
    scan_node_options));

    cp::ExecNode *project;
    ARROW_ASSIGN_OR_RAISE(project, cp::MakeExecNode("project", plan.get(), {scan},
                                                    cp::ProjectNodeOptions{{a_times_2}}));

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * sink,
                          cp::MakeExecNode("sink", plan.get(), {project},
                                           cp::SinkNodeOptions{&sink_gen}));

    ABORT_ON_FAILURE(sink->Validate());

    // // translate sink_gen (async) to sink_reader (sync)
    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        arrow::schema({arrow::field("a * 2", arrow::int32())}),
        std::move(sink_gen), exec_context.memory_pool());

    // // validate the ExecPlan
    ABORT_ON_FAILURE(plan->Validate());

    PRINT_LINE("Exec Plan Created : " << plan->ToString());

    // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    PRINT_LINE("Results : " << response_table->ToString());

    // // plan stop producing
    plan->StopProducing();
    // // plan marked finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

arrow::Status source_aggregate_sink_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));

    auto basic_data = MakeBasicBatches();

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    auto source_node_options = cp::SourceNodeOptions{
        basic_data.schema, basic_data.gen(true)};

    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source, cp::MakeExecNode("source",
                                                                  plan.get(), {},
                                                                  source_node_options));

    cp::CountOptions options(cp::CountOptions::ONLY_VALID);
    auto aggregate_options = cp::AggregateNodeOptions{
        /*aggregates=*/{{"hash_count", &options}},
        /*targets=*/{"a"},
        /*names=*/{"count(a)"},
        /*keys=*/{"b"}};
    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * aggregate,
                          cp::MakeExecNode("aggregate", plan.get(), {source},
                          aggregate_options));

    cp::ExecNode *sink;

    ARROW_ASSIGN_OR_RAISE(sink, cp::MakeExecNode("sink", plan.get(), {aggregate},
                                                 cp::SinkNodeOptions{&sink_gen}));

    // // // translate sink_gen (async) to sink_reader (sync)
    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        arrow::schema({
            arrow::field("count(a)", arrow::int32()),
            arrow::field("b", arrow::boolean()),
        }),
        std::move(sink_gen),
        exec_context.memory_pool());

    // // validate the ExecPlan
    ABORT_ON_FAILURE(plan->Validate());
    PRINT_LINE("ExecPlan created : " << plan->ToString());
    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    PRINT_LINE("Results : " << response_table->ToString());

    //plan stop producing
    plan->StopProducing();
    // plan mark finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

arrow::Status source_consuming_sink_node_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));

    auto basic_data = MakeBasicBatches();

    auto source_node_options = cp::SourceNodeOptions{
        basic_data.schema, basic_data.gen(true)};

    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source, cp::MakeExecNode("source",
                                                                  plan.get(), {},
                                                                  source_node_options));

    std::atomic<uint32_t> batches_seen{0};
    arrow::Future<> finish = arrow::Future<>::Make();
    struct CustomSinkNodeConsumer : public cp::SinkNodeConsumer {
        CustomSinkNodeConsumer(std::atomic<uint32_t> *batches_seen, arrow::Future<>
        finish)
            : batches_seen(batches_seen), finish(std::move(finish)) {}

        arrow::Status Consume(cp::ExecBatch batch) override {
            (*batches_seen)++;
            return arrow::Status::OK();
        }

        arrow::Future<> Finish() override { return finish; }

        std::atomic<uint32_t> *batches_seen;
        arrow::Future<> finish;
    };
    std::shared_ptr<CustomSinkNodeConsumer> consumer =
        std::make_shared<CustomSinkNodeConsumer>(&batches_seen, finish);

    cp::ExecNode *consuming_sink;

    ARROW_ASSIGN_OR_RAISE(consuming_sink, MakeExecNode("consuming_sink", plan.get(),
    {source}, cp::ConsumingSinkNodeOptions(consumer)));

    ABORT_ON_FAILURE(consuming_sink->Validate());

    ABORT_ON_FAILURE(plan->Validate());
    PRINT_LINE("Exec Plan created: " << plan->ToString());
    // plan start producing
    ABORT_ON_FAILURE(plan->StartProducing());
    // Source should finish fairly quickly
    ABORT_ON_FAILURE(source->finished().status());
    PRINT_LINE("Source Finished!");
    // Mark consumption complete, plan should finish
    arrow::Status finish_status;
    //finish.Wait();
    finish.MarkFinished(finish_status);
    ABORT_ON_FAILURE(plan->finished().status());
    ABORT_ON_FAILURE(finish_status);

    return arrow::Status::OK();
}

arrow::Status source_order_by_sink_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
     ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));

    auto basic_data = MakeSortTestBasicBatches();

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    auto source_node_options = cp::SourceNodeOptions{
        basic_data.schema, basic_data.gen(true)};
    ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
    cp::MakeExecNode("source", plan.get(), {}, source_node_options));

    cp::ExecNode *sink;

    ARROW_ASSIGN_OR_RAISE(sink,
    cp::MakeExecNode("order_by_sink", plan.get(),
    {source}, cp::OrderBySinkNodeOptions{
        cp::SortOptions{{cp::SortKey{"a",
        cp::SortOrder::Descending}}}, &sink_gen}));

    // // // translate sink_gen (async) to sink_reader (sync)
    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        basic_data.schema,
        std::move(sink_gen),
        exec_context.memory_pool());

    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    std::cout << "Results : " << response_table->ToString() << std::endl;

    return arrow::Status::OK();
}

arrow::Status source_hash_join_sink_example() {
    auto input = MakeGroupableBatches();

    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    cp::ExecNode *left_source;
    cp::ExecNode *right_source;
    for (auto source : {&left_source, &right_source}) {
        ARROW_ASSIGN_OR_RAISE(
            *source,
            MakeExecNode("source", plan.get(), {},
                                  cp::SourceNodeOptions{
                                      input.schema,
                                      input.gen(/*parallel=*/true)}));
    }
    // TODO: decide whether to keep the filters or remove

    // ARROW_ASSIGN_OR_RAISE(
    //     auto left_filter,
    //     cp::MakeExecNode("filter", plan.get(), {left_source},
    //                      cp::FilterNodeOptions{
    //                          cp::greater_equal(
    //                              cp::field_ref("i32"),
    //                              cp::literal(-1))}));
    // ARROW_ASSIGN_OR_RAISE(
    //     auto right_filter,
    //     cp::MakeExecNode("filter", plan.get(), {right_source},
    //                      cp::FilterNodeOptions{
    //                          cp::less_equal(
    //                              cp::field_ref("i32"),
    //                              cp::literal(2))}));
    // PRINT_LINE("left and right filter nodes created");

    cp::HashJoinNodeOptions join_opts{cp::JoinType::INNER,
                                      /*left_keys=*/{"str"},
                                      /*right_keys=*/{"str"}, cp::literal(true), "l_",
                                      "r_"};

    ARROW_ASSIGN_OR_RAISE(
        auto hashjoin,
        cp::MakeExecNode("hashjoin", plan.get(), {left_source, right_source},
        join_opts));

    ARROW_ASSIGN_OR_RAISE(std::ignore, cp::MakeExecNode("sink", plan.get(), {hashjoin},
                                                        cp::SinkNodeOptions{&sink_gen}));
    // expected columns i32, str, l_str, r_str

    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        arrow::schema({arrow::field("i32", arrow::int32()),
                       arrow::field("str", arrow::utf8()),
                       arrow::field("l_str", arrow::utf8()),
                       arrow::field("r_str", arrow::utf8())}),
        std::move(sink_gen),
        exec_context.memory_pool());

    // // // validate the ExecPlan
    ABORT_ON_FAILURE(plan->Validate());
    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    PRINT_LINE("Results : " << response_table->ToString());

    // // plan stop producing
    plan->StopProducing();
    // // plan mark finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

arrow::Status source_kselect_example() {
    auto input = MakeGroupableBatches();

    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    ARROW_ASSIGN_OR_RAISE(
        cp::ExecNode * source,
        cp::MakeExecNode("source",
            plan.get(), {},
                cp::SourceNodeOptions{
                    input.schema,
                    input.gen(/*parallel=*/true)}));

    cp::SelectKOptions options = cp::SelectKOptions::TopKDefault(/*k=*/2, {"i32"});

    ARROW_ASSIGN_OR_RAISE(
        cp::ExecNode * k_sink_node,
        cp::MakeExecNode("select_k_sink",
            plan.get(), {source},
                cp::SelectKSinkNodeOptions{options, &sink_gen}));

    k_sink_node->finished().Wait();

    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        arrow::schema({arrow::field("i32", arrow::int32()),
                       arrow::field("str", arrow::utf8())}),
        std::move(sink_gen),
        exec_context.memory_pool());

    // // // validate the ExecPlan
    ABORT_ON_FAILURE(plan->Validate());
    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    PRINT_LINE("Results : " << response_table->ToString());

    // // plan stop proudcing
    plan->StopProducing();
    // // plan mark finished
    plan->finished().Wait();

    return arrow::Status::OK();
}

arrow::Status scan_filter_write_example() {
    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    // ensure arrow::dataset node factories are in the registry
    arrow::dataset::internal::Initialize();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
     cp::ExecPlan::Make(&exec_context));

    std::shared_ptr<arrow::dataset::Dataset> dataset = CreateDataset();

    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    // sync scanning is not supported by ScanNode
    options->use_async = true;
    // specify the filter
    //cp::Expression b_is_true = cp::field_ref("b");
    //options->filter = b_is_true;
    // empty projection
    options->projection = Materialize({});

    cp::ExecNode *scan;

    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

    ARROW_ASSIGN_OR_RAISE(scan, cp::MakeExecNode("scan", plan.get(), {},
    scan_node_options));

    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    std::string root_path = "";
    std::string uri = "file:///Users/vibhatha/sandbox/test";
    std::shared_ptr<arrow::fs::FileSystem> filesystem =
    arrow::fs::FileSystemFromUri(uri, &root_path).ValueOrDie();

    auto base_path = root_path + "/parquet_dataset";
    ABORT_ON_FAILURE(filesystem->DeleteDir(base_path));
    ABORT_ON_FAILURE(filesystem->CreateDir(base_path));

    // The partition schema determines which fields are part of the partitioning.
    auto partition_schema = arrow::schema({arrow::field("a", arrow::int32())});
    // We'll use Hive-style partitioning,
    // which creates directories with "key=value" pairs.

    auto partitioning =
    std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    // We'll write Parquet files.
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = format->DefaultWriteOptions();
    write_options.filesystem = filesystem;
    write_options.base_dir = base_path;
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet";

    arrow::dataset::WriteNodeOptions write_node_options {write_options,
    dataset->schema()};

    PRINT_LINE("Write Options created");

    ARROW_ASSIGN_OR_RAISE(cp::ExecNode *wr, cp::MakeExecNode("write", plan.get(),
    {scan}, write_node_options));

    ABORT_ON_FAILURE(wr->Validate());
    ABORT_ON_FAILURE(plan->Validate());
    PRINT_LINE("Execution Plan Created : " << plan->ToString());
    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());
    plan->finished().Wait();
    return arrow::Status::OK();
}

arrow::Status source_union_sink_example() {
    auto basic_data = MakeBasicBatches();

    cp::ExecContext exec_context(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool());

    std::shared_ptr<cp::ExecPlan> plan =
    cp::ExecPlan::Make(&exec_context).ValueOrDie();
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

    cp::Declaration union_node{"union", cp::ExecNodeOptions{}};
    cp::Declaration lhs{"source",
                  cp::SourceNodeOptions{basic_data.schema,
                                    basic_data.gen(/*parallel=*/false)}};
    lhs.label = "lhs";
    cp::Declaration rhs{"source",
                    cp::SourceNodeOptions{basic_data.schema,
                                      basic_data.gen(/*parallel=*/false)}};
    rhs.label = "rhs";
    union_node.inputs.emplace_back(lhs);
    union_node.inputs.emplace_back(rhs);

    cp::CountOptions options(cp::CountOptions::ONLY_VALID);
    ARROW_ASSIGN_OR_RAISE(auto declr,
    cp::Declaration::Sequence(
            {
                union_node,
                {"aggregate", cp::AggregateNodeOptions{
                  /*aggregates=*/{{"count", &options}},
                  /*targets=*/{"a"},
                  /*names=*/{"count(a)"},
                  /*keys=*/{}}},
                {"sink", cp::SinkNodeOptions{&sink_gen}},
            })
            .AddToPlan(plan.get()));

    ABORT_ON_FAILURE(declr->Validate());

    ABORT_ON_FAILURE(plan->Validate());
    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        arrow::schema({arrow::field("count(a)", arrow::int32())}),
        std::move(sink_gen),
        exec_context.memory_pool());

    // // // start the ExecPlan
    ABORT_ON_FAILURE(plan->StartProducing());

    // // collect sink_reader into a Table
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
    arrow::Table::FromRecordBatchReader(sink_reader.get()));

    std::cout << "Results : " << response_table->ToString() << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char** argv) {
  PRINT_BLOCK("Scan Sink Example");
  CHECK_AND_CONTINUE(scan_sink_node_example());
  PRINT_BLOCK("Exec Plan End-to-End Example");
  CHECK_AND_CONTINUE(exec_plan_end_to_end_sample());
  PRINT_BLOCK("Source Sink Example")
  CHECK_AND_CONTINUE(source_sink_example());
  PRINT_BLOCK("Scan Filter Sink Example");
  CHECK_AND_CONTINUE(scan_filter_sink_example());
  PRINT_BLOCK("Scan Project Sink Example");
  CHECK_AND_CONTINUE(scan_project_sink_example());
  PRINT_BLOCK("Source Aggregate Sink Example");
  CHECK_AND_CONTINUE(source_aggregate_sink_example());
  PRINT_BLOCK("Source Consuming-Sink Example");
  CHECK_AND_CONTINUE(source_consuming_sink_node_example());
  PRINT_BLOCK("Source Ordered-By-Sink Example");
  CHECK_AND_CONTINUE(source_order_by_sink_example());
  PRINT_BLOCK("Source HashJoin Example");
  CHECK_AND_CONTINUE(source_hash_join_sink_example());
  PRINT_BLOCK("Source KSelect Example");
  CHECK_AND_CONTINUE(source_kselect_example());
  PRINT_BLOCK("Scan Filter Write Example");
  CHECK_AND_CONTINUE(scan_filter_write_example());
  PRINT_LINE("Catalog Source Sink Example");
  CHECK_AND_RETURN(source_union_sink_example());
}

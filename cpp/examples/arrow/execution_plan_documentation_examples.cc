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

// (Doc section: Execution Plan Documentation Example)

#include <arrow/array.h>
#include <arrow/builder.h>

#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>

#include <arrow/csv/api.h>

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <arrow/ipc/api.h>

#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

#include <iostream>
#include <memory>
#include <utility>

// Demonstrate various operators in Arrow Streaming Execution Engine

namespace cp = ::arrow::compute;

constexpr char kSep[] = "******";

void PrintBlock(const std::string& msg) {
  std::cout << "\n\t" << kSep << " " << msg << " " << kSep << "\n" << std::endl;
}

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<TYPE>::ArrayType;
  using ARROW_BUILDER_TYPE = typename arrow::TypeTraits<TYPE>::BuilderType;
  ARROW_BUILDER_TYPE builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  std::shared_ptr<ARROW_ARRAY_TYPE> array;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

template <class TYPE>
arrow::Result<std::shared_ptr<arrow::Array>> GetBinaryArrayDataSample(
    const std::vector<std::string>& values) {
  using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<TYPE>::ArrayType;
  using ARROW_BUILDER_TYPE = typename arrow::TypeTraits<TYPE>::BuilderType;
  ARROW_BUILDER_TYPE builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  std::shared_ptr<ARROW_ARRAY_TYPE> array;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSampleRecordBatch(
    const arrow::ArrayVector array_vector, const arrow::FieldVector& field_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto struct_result,
                        arrow::StructArray::Make(array_vector, field_vector));
  return record_batch->FromStructArray(struct_result);
}

/// \brief Create a sample table
/// The table's contents will be:
/// a, b
/// 1,null
/// 2,true
/// null,true
/// 3,false
/// null,true
/// 4,false
/// 5,null
/// 6,false
/// 7,false
/// 8,true
/// \return The created table

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  auto null_long = std::numeric_limits<int64_t>::quiet_NaN();
  ARROW_ASSIGN_OR_RAISE(auto int64_array,
                        GetArrayDataSample<arrow::Int64Type>(
                            {1, 2, null_long, 3, null_long, 4, 5, 6, 7, 8}));

  arrow::BooleanBuilder boolean_builder;
  std::shared_ptr<arrow::BooleanArray> bool_array;

  std::vector<uint8_t> bool_values = {false, true,  true,  false, true,
                                      false, false, false, false, true};
  std::vector<bool> is_valid = {false, true,  true, true, true,
                                true,  false, true, true, true};

  ARROW_RETURN_NOT_OK(boolean_builder.Reserve(10));

  ARROW_RETURN_NOT_OK(boolean_builder.AppendValues(bool_values, is_valid));

  ARROW_RETURN_NOT_OK(boolean_builder.Finish(&bool_array));

  auto record_batch =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("a", arrow::int64()),
                                              arrow::field("b", arrow::boolean())}),
                               10, {int64_array, bool_array});
  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches({record_batch}));
  return table;
}

/// \brief Create a sample dataset
/// \return An in-memory dataset based on GetTable()
arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> GetDataset() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());
  auto ds = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  return ds;
}

arrow::Result<cp::ExecBatch> GetExecBatchFromVectors(
    const arrow::FieldVector& field_vector, const arrow::ArrayVector& array_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto res_batch, GetSampleRecordBatch(array_vector, field_vector));
  cp::ExecBatch batch{*res_batch};
  return batch;
}

// (Doc section: BatchesWithSchema Definition)
struct BatchesWithSchema {
  std::vector<cp::ExecBatch> batches;
  std::shared_ptr<arrow::Schema> schema;
  // This method uses internal arrow utilities to
  // convert a vector of record batches to an AsyncGenerator of optional batches
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen() const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](cp::ExecBatch batch) { return arrow::util::make_optional(std::move(batch)); },
        batches);
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen;
    gen = arrow::MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }
};
// (Doc section: BatchesWithSchema Definition)

// (Doc section: MakeBasicBatches Definition)
arrow::Result<BatchesWithSchema> MakeBasicBatches() {
  BatchesWithSchema out;
  auto field_vector = {arrow::field("a", arrow::int32()),
                       arrow::field("b", arrow::boolean())};
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int32Type>({0, 4}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int, GetArrayDataSample<arrow::Int32Type>({5, 6, 7}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int, GetArrayDataSample<arrow::Int32Type>({8, 9, 10}));

  ARROW_ASSIGN_OR_RAISE(auto b1_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b2_bool,
                        GetArrayDataSample<arrow::BooleanType>({true, false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b3_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true, false}));

  ARROW_ASSIGN_OR_RAISE(auto b1,
                        GetExecBatchFromVectors(field_vector, {b1_int, b1_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b2,
                        GetExecBatchFromVectors(field_vector, {b2_int, b2_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b3,
                        GetExecBatchFromVectors(field_vector, {b3_int, b3_bool}));

  out.batches = {b1, b2, b3};
  out.schema = arrow::schema(field_vector);
  return out;
}
// (Doc section: MakeBasicBatches Definition)

arrow::Result<BatchesWithSchema> MakeSortTestBasicBatches() {
  BatchesWithSchema out;
  auto field = arrow::field("a", arrow::int32());
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int32Type>({1, 3, 0, 2}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int,
                        GetArrayDataSample<arrow::Int32Type>({121, 101, 120, 12}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int,
                        GetArrayDataSample<arrow::Int32Type>({10, 110, 210, 121}));
  ARROW_ASSIGN_OR_RAISE(auto b4_int,
                        GetArrayDataSample<arrow::Int32Type>({51, 101, 2, 34}));
  ARROW_ASSIGN_OR_RAISE(auto b5_int,
                        GetArrayDataSample<arrow::Int32Type>({11, 31, 1, 12}));
  ARROW_ASSIGN_OR_RAISE(auto b6_int,
                        GetArrayDataSample<arrow::Int32Type>({12, 101, 120, 12}));
  ARROW_ASSIGN_OR_RAISE(auto b7_int,
                        GetArrayDataSample<arrow::Int32Type>({0, 110, 210, 11}));
  ARROW_ASSIGN_OR_RAISE(auto b8_int,
                        GetArrayDataSample<arrow::Int32Type>({51, 10, 2, 3}));

  ARROW_ASSIGN_OR_RAISE(auto b1, GetExecBatchFromVectors({field}, {b1_int}));
  ARROW_ASSIGN_OR_RAISE(auto b2, GetExecBatchFromVectors({field}, {b2_int}));
  ARROW_ASSIGN_OR_RAISE(auto b3,
                        GetExecBatchFromVectors({field, field}, {b3_int, b8_int}));
  ARROW_ASSIGN_OR_RAISE(auto b4,
                        GetExecBatchFromVectors({field, field, field, field},
                                                {b4_int, b5_int, b6_int, b7_int}));
  out.batches = {b1, b2, b3, b4};
  out.schema = arrow::schema({field});
  return out;
}

arrow::Result<BatchesWithSchema> MakeGroupableBatches(int multiplicity = 1) {
  BatchesWithSchema out;
  auto fields = {arrow::field("i32", arrow::int32()), arrow::field("str", arrow::utf8())};
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int32Type>({12, 7, 3}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int, GetArrayDataSample<arrow::Int32Type>({-2, -1, 3}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int, GetArrayDataSample<arrow::Int32Type>({5, 3, -8}));
  ARROW_ASSIGN_OR_RAISE(auto b1_str, GetBinaryArrayDataSample<arrow::StringType>(
                                         {"alpha", "beta", "alpha"}));
  ARROW_ASSIGN_OR_RAISE(auto b2_str, GetBinaryArrayDataSample<arrow::StringType>(
                                         {"alpha", "gamma", "alpha"}));
  ARROW_ASSIGN_OR_RAISE(auto b3_str, GetBinaryArrayDataSample<arrow::StringType>(
                                         {"gamma", "beta", "alpha"}));
  ARROW_ASSIGN_OR_RAISE(auto b1, GetExecBatchFromVectors(fields, {b1_int, b1_str}));
  ARROW_ASSIGN_OR_RAISE(auto b2, GetExecBatchFromVectors(fields, {b2_int, b2_str}));
  ARROW_ASSIGN_OR_RAISE(auto b3, GetExecBatchFromVectors(fields, {b3_int, b3_str}));
  out.batches = {b1, b2, b3};

  size_t batch_count = out.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out.batches.push_back(out.batches[i]);
    }
  }

  out.schema = arrow::schema(fields);
  return out;
}

arrow::Status ExecutePlanAndCollectAsTable(
    cp::ExecContext& exec_context, std::shared_ptr<cp::ExecPlan> plan,
    std::shared_ptr<arrow::Schema> schema,
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen) {
  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema, std::move(sink_gen), exec_context.memory_pool());

  // validate the ExecPlan
  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << "ExecPlan created : " << plan->ToString() << std::endl;
  // start the ExecPlan
  ARROW_RETURN_NOT_OK(plan->StartProducing());

  // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;

  ARROW_ASSIGN_OR_RAISE(response_table,
                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

  std::cout << "Results : " << response_table->ToString() << std::endl;

  // stop producing
  plan->StopProducing();
  // plan mark finished
  auto future = plan->finished();
  return future.status();
}

// (Doc section: Scan Example)

/// \brief An example demonstrating a scan and sink node
/// \param exec_context The execution context to run the plan in
///
/// Scan-Sink
/// This example shows how scan operation can be applied on a dataset.
/// There are operations that can be applied on the scan (project, filter)
/// and the input data can be processed. The output is obtained as a table
/// via the sink node.
arrow::Status ScanSinkExample(cp::ExecContext& exec_context) {
  // Execution plan created
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  options->projection = cp::project({}, {});  // create empty projection

  // construct the scan node
  cp::ExecNode* scan;
  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  ARROW_ASSIGN_OR_RAISE(scan,
                        cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {scan}, cp::SinkNodeOptions{&sink_gen}));

  return ExecutePlanAndCollectAsTable(exec_context, plan, dataset->schema(), sink_gen);
}
// (Doc section: Scan Example)

// (Doc section: Source Example)

/// \brief An example demonstrating a source and sink node
/// \param exec_context The execution context to run the plan in
///
/// Source-Sink Example
/// This example shows how a source and sink can be used
/// in an execution plan. This includes source node receiving data
/// and the sink node emits the data as an output represented in
/// a table.
arrow::Status SourceSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {source}, cp::SinkNodeOptions{&sink_gen}));

  return ExecutePlanAndCollectAsTable(exec_context, plan, basic_data.schema, sink_gen);
}
// (Doc section: Source Example)

// (Doc section: Table Source Example)

/// \brief An example showing a table source node
/// \param exec_context The execution context to run the plan in
///
/// TableSource-Sink Example
/// This example shows how a table_source and sink can be used
/// in an execution plan. This includes a table source node
/// receiving data from a table and the sink node emits
/// the data to a generator which we collect into a table.
arrow::Status TableSourceSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  int max_batch_size = 2;
  auto table_source_options = cp::TableSourceNodeOptions{table, max_batch_size};

  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * source,
      cp::MakeExecNode("table_source", plan.get(), {}, table_source_options));

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {source}, cp::SinkNodeOptions{&sink_gen}));

  return ExecutePlanAndCollectAsTable(exec_context, plan, table->schema(), sink_gen);
}
// (Doc section: Table Source Example)

// (Doc section: Filter Example)

/// \brief An example showing a filter node
/// \param exec_context The execution context to run the plan in
///
/// Source-Filter-Sink
/// This example shows how a filter can be used in an execution plan,
/// along with the source and sink operations. The output from the
/// exeuction plan is obtained as a table via the sink node.
arrow::Status ScanFilterSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // specify the filter.  This filter removes all rows where the
  // value of the "a" column is greater than 3.
  cp::Expression filter_opt = cp::greater(cp::field_ref("a"), cp::literal(3));
  // set filter for scanner : on-disk / push-down filtering.
  // This step can be skipped if you are not reading from disk.
  options->filter = filter_opt;
  // empty projection
  options->projection = cp::project({}, {});

  // construct the scan node
  std::cout << "Initialized Scanning Options" << std::endl;

  cp::ExecNode* scan;

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
  std::cout << "Scan node options created" << std::endl;

  ARROW_ASSIGN_OR_RAISE(scan,
                        cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

  // pipe the scan node into the filter node
  // Need to set the filter in scan node options and filter node options.
  // At scan node it is used for on-disk / push-down filtering.
  // At filter node it is used for in-memory filtering.
  cp::ExecNode* filter;
  ARROW_ASSIGN_OR_RAISE(filter, cp::MakeExecNode("filter", plan.get(), {scan},
                                                 cp::FilterNodeOptions{filter_opt}));

  // finally, pipe the filter node into a sink node
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {filter}, cp::SinkNodeOptions{&sink_gen}));

  return ExecutePlanAndCollectAsTable(exec_context, plan, dataset->schema(), sink_gen);
}

// (Doc section: Filter Example)

// (Doc section: Project Example)

/// \brief An example showing a project node
/// \param exec_context The execution context to run the plan in
///
/// Scan-Project-Sink
/// This example shows how Scan operation can be used to load the data
/// into the execution plan, how project operation can be applied on the
/// data stream and how the output is obtained as a table via the sink node.
arrow::Status ScanProjectSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // projection
  cp::Expression a_times_2 = cp::call("multiply", {cp::field_ref("a"), cp::literal(2)});
  options->projection = cp::project({}, {});

  cp::ExecNode* scan;

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  ARROW_ASSIGN_OR_RAISE(scan,
                        cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

  cp::ExecNode* project;
  ARROW_ASSIGN_OR_RAISE(project, cp::MakeExecNode("project", plan.get(), {scan},
                                                  cp::ProjectNodeOptions{{a_times_2}}));
  // schema after projection => multiply(a, 2): int64
  std::cout << "Schema after projection : \n"
            << project->output_schema()->ToString() << std::endl;

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {project}, cp::SinkNodeOptions{&sink_gen}));
  auto schema = arrow::schema({arrow::field("a * 2", arrow::int32())});

  return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}

// (Doc section: Project Example)

// (Doc section: Scalar Aggregate Example)

/// \brief An example showing an aggregation node to aggregate an entire table
/// \param exec_context The execution context to run the plan in
///
/// Source-Aggregation-Sink
/// This example shows how an aggregation operation can be applied on a
/// execution plan resulting a scalar output. The source node loads the
/// data and the aggregation (counting unique types in column 'a')
/// is applied on this data. The output is obtained from the sink node as a table.
arrow::Status SourceScalarAggregateSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));
  auto aggregate_options = cp::AggregateNodeOptions{/*aggregates=*/{{"sum", nullptr}},
                                                    /*targets=*/{"a"},
                                                    /*names=*/{"sum(a)"}};
  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * aggregate,
      cp::MakeExecNode("aggregate", plan.get(), {source}, aggregate_options));

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {aggregate}, cp::SinkNodeOptions{&sink_gen}));
  auto schema = arrow::schema({arrow::field("sum(a)", arrow::int32())});

  return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}
// (Doc section: Scalar Aggregate Example)

// (Doc section: Group Aggregate Example)

/// \brief An example showing an aggregation node to perform a group-by operation
/// \param exec_context The execution context to run the plan in
///
/// Source-Aggregation-Sink
/// This example shows how an aggregation operation can be applied on a
/// execution plan resulting a grouped output. The source node loads the
/// data and the aggregation (counting unique types in column 'a') is
/// applied on this data. The output is obtained from the sink node as a table.
arrow::Status SourceGroupAggregateSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));
  cp::CountOptions options(cp::CountOptions::ONLY_VALID);
  auto aggregate_options =
      cp::AggregateNodeOptions{/*aggregates=*/{{"hash_count", &options}},
                               /*targets=*/{"a"},
                               /*names=*/{"count(a)"},
                               /*keys=*/{"b"}};
  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * aggregate,
      cp::MakeExecNode("aggregate", plan.get(), {source}, aggregate_options));

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {aggregate}, cp::SinkNodeOptions{&sink_gen}));
  auto schema = arrow::schema({
      arrow::field("count(a)", arrow::int32()),
      arrow::field("b", arrow::boolean()),
  });

  return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}
// (Doc section: Group Aggregate Example)

// (Doc section: ConsumingSink Example)

/// \brief An example showing a consuming sink node
/// \param exec_context The execution context to run the plan in
///
/// Source-ConsumingSink
/// This example shows how the data can be consumed within the execution plan
/// by using a ConsumingSink node. There is no data output from this execution plan.
arrow::Status SourceConsumingSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  std::atomic<uint32_t> batches_seen{0};
  arrow::Future<> finish = arrow::Future<>::Make();
  struct CustomSinkNodeConsumer : public cp::SinkNodeConsumer {
    CustomSinkNodeConsumer(std::atomic<uint32_t>* batches_seen, arrow::Future<> finish)
        : batches_seen(batches_seen), finish(std::move(finish)) {}

    arrow::Status Consume(cp::ExecBatch batch) override {
      (*batches_seen)++;
      return arrow::Status::OK();
    }

    arrow::Future<> Finish() override { return finish; }

    std::atomic<uint32_t>* batches_seen;
    arrow::Future<> finish;
  };
  std::shared_ptr<CustomSinkNodeConsumer> consumer =
      std::make_shared<CustomSinkNodeConsumer>(&batches_seen, finish);

  cp::ExecNode* consuming_sink;

  ARROW_ASSIGN_OR_RAISE(consuming_sink,
                        MakeExecNode("consuming_sink", plan.get(), {source},
                                     cp::ConsumingSinkNodeOptions(consumer)));

  ARROW_RETURN_NOT_OK(consuming_sink->Validate());

  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << "Exec Plan created: " << plan->ToString() << std::endl;
  // plan start producing
  ARROW_RETURN_NOT_OK(plan->StartProducing());
  // Source should finish fairly quickly
  ARROW_RETURN_NOT_OK(source->finished().status());
  std::cout << "Source Finished!" << std::endl;
  // Mark consumption complete, plan should finish
  finish.MarkFinished(arrow::Status::OK());
  ARROW_RETURN_NOT_OK(plan->finished().status());
  return arrow::Status::OK();
}
// (Doc section: ConsumingSink Example)

// (Doc section: OrderBySink Example)

/// \brief An example showing an order-by node
/// \param exec_context The execution context to run the plan in
///
/// Source-OrderBySink
/// In this example, the data enters through the source node
/// and the data is ordered in the sink node. The order can be
/// ASCENDING or DESCENDING and it is configurable. The output
/// is obtained as a table from the sink node.
arrow::Status SourceOrderBySinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeSortTestBasicBatches());

  std::cout << "basic data created" << std::endl;

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};
  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  ARROW_RETURN_NOT_OK(cp::MakeExecNode(
      "order_by_sink", plan.get(), {source},
      cp::OrderBySinkNodeOptions{
          cp::SortOptions{{cp::SortKey{"a", cp::SortOrder::Descending}}}, &sink_gen}));

  return ExecutePlanAndCollectAsTable(exec_context, plan, basic_data.schema, sink_gen);
}

// (Doc section: OrderBySink Example)

// (Doc section: HashJoin Example)

/// \brief An example showing a hash join node
/// \param exec_context The execution context to run the plan in
///
/// Source-HashJoin-Sink
/// This example shows how source node gets the data and how a self-join
/// is applied on the data. The join options are configurable. The output
/// is obtained as a table via the sink node.
arrow::Status SourceHashJoinSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto input, MakeGroupableBatches());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  cp::ExecNode* left_source;
  cp::ExecNode* right_source;
  for (auto source : {&left_source, &right_source}) {
    ARROW_ASSIGN_OR_RAISE(*source,
                          MakeExecNode("source", plan.get(), {},
                                       cp::SourceNodeOptions{input.schema, input.gen()}));
  }

  cp::HashJoinNodeOptions join_opts{
      cp::JoinType::INNER,
      /*left_keys=*/{"str"},
      /*right_keys=*/{"str"}, cp::literal(true), "l_", "r_"};

  ARROW_ASSIGN_OR_RAISE(
      auto hashjoin,
      cp::MakeExecNode("hashjoin", plan.get(), {left_source, right_source}, join_opts));

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("sink", plan.get(), {hashjoin}, cp::SinkNodeOptions{&sink_gen}));
  // expected columns i32, str, l_str, r_str
  auto schema = arrow::schema(
      {arrow::field("i32", arrow::int32()), arrow::field("str", arrow::utf8()),
       arrow::field("l_str", arrow::utf8()), arrow::field("r_str", arrow::utf8())});

  return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}

// (Doc section: HashJoin Example)

// (Doc section: KSelect Example)

/// \brief An example showing a select-k node
/// \param exec_context The execution context to run the plan in
///
/// Source-KSelect
/// This example shows how K number of elements can be selected
/// either from the top or bottom. The output node is a modified
/// sink node where output can be obtained as a table.
arrow::Status SourceKSelectExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto input, MakeGroupableBatches());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * source,
      cp::MakeExecNode("source", plan.get(), {},
                       cp::SourceNodeOptions{input.schema, input.gen()}));

  cp::SelectKOptions options = cp::SelectKOptions::TopKDefault(/*k=*/2, {"i32"});

  ARROW_RETURN_NOT_OK(cp::MakeExecNode("select_k_sink", plan.get(), {source},
                                       cp::SelectKSinkNodeOptions{options, &sink_gen}));

  auto schema = arrow::schema(
      {arrow::field("i32", arrow::int32()), arrow::field("str", arrow::utf8())});

  return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}

// (Doc section: KSelect Example)

// (Doc section: Write Example)

/// \brief An example showing a write node
/// \param exec_context The execution context to run the plan in
/// \param file_path The destination to write to
///
/// Scan-Filter-Write
/// This example shows how scan node can be used to load the data
/// and after processing how it can be written to disk.
arrow::Status ScanFilterWriteExample(cp::ExecContext& exec_context,
                                     const std::string& file_path) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(&exec_context));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // empty projection
  options->projection = cp::project({}, {});

  cp::ExecNode* scan;

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  ARROW_ASSIGN_OR_RAISE(scan,
                        cp::MakeExecNode("scan", plan.get(), {}, scan_node_options));

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  std::string root_path = "";
  std::string uri = "file://" + file_path;
  std::shared_ptr<arrow::fs::FileSystem> filesystem =
      arrow::fs::FileSystemFromUri(uri, &root_path).ValueOrDie();

  auto base_path = root_path + "/parquet_dataset";
  // Uncomment the following line, if run repeatedly
  // ARROW_RETURN_NOT_OK(filesystem->DeleteDirContents(base_path));
  ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));

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

  arrow::dataset::WriteNodeOptions write_node_options{write_options, dataset->schema()};

  ARROW_RETURN_NOT_OK(cp::MakeExecNode("write", plan.get(), {scan}, write_node_options));

  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << "Execution Plan Created : " << plan->ToString() << std::endl;
  // // // start the ExecPlan
  ARROW_RETURN_NOT_OK(plan->StartProducing());
  auto future = plan->finished();
  ARROW_RETURN_NOT_OK(future.status());
  future.Wait();
  return arrow::Status::OK();
}

// (Doc section: Write Example)

// (Doc section: Union Example)

/// \brief An example showing a union node
/// \param exec_context The execution context to run the plan in
///
/// Source-Union-Sink
/// This example shows how a union operation can be applied on two
/// data sources. The output is obtained as a table via the sink
/// node.
arrow::Status SourceUnionSinkExample(cp::ExecContext& exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  std::shared_ptr<cp::ExecPlan> plan = cp::ExecPlan::Make(&exec_context).ValueOrDie();
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;

  cp::Declaration union_node{"union", cp::ExecNodeOptions{}};
  cp::Declaration lhs{"source",
                      cp::SourceNodeOptions{basic_data.schema, basic_data.gen()}};
  lhs.label = "lhs";
  cp::Declaration rhs{"source",
                      cp::SourceNodeOptions{basic_data.schema, basic_data.gen()}};
  rhs.label = "rhs";
  union_node.inputs.emplace_back(lhs);
  union_node.inputs.emplace_back(rhs);

  cp::CountOptions options(cp::CountOptions::ONLY_VALID);
  ARROW_ASSIGN_OR_RAISE(
      auto declr, cp::Declaration::Sequence({
                                                union_node,
                                                {"sink", cp::SinkNodeOptions{&sink_gen}},
                                            })
                      .AddToPlan(plan.get()));

  ARROW_RETURN_NOT_OK(declr->Validate());

  ARROW_RETURN_NOT_OK(plan->Validate());
  return ExecutePlanAndCollectAsTable(exec_context, plan, basic_data.schema, sink_gen);
}

// (Doc section: Union Example)

enum ExampleMode {
  SOURCE_SINK = 0,
  TABLE_SOURCE_SINK = 1,
  SCAN = 2,
  FILTER = 3,
  PROJECT = 4,
  SCALAR_AGGREGATION = 5,
  GROUP_AGGREGATION = 6,
  CONSUMING_SINK = 7,
  ORDER_BY_SINK = 8,
  HASHJOIN = 9,
  KSELECT = 10,
  WRITE = 11,
  UNION = 12,
};

int main(int argc, char** argv) {
  if (argc < 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string base_save_path = argv[1];
  int mode = std::atoi(argv[2]);
  arrow::Status status;
  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();
  // execution context
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());
  switch (mode) {
    case SOURCE_SINK:
      PrintBlock("Source Sink Example");
      status = SourceSinkExample(exec_context);
      break;
    case TABLE_SOURCE_SINK:
      PrintBlock("Table Source Sink Example");
      status = TableSourceSinkExample(exec_context);
      break;
    case SCAN:
      PrintBlock("Scan Example");
      status = ScanSinkExample(exec_context);
      break;
    case FILTER:
      PrintBlock("Filter Example");
      status = ScanFilterSinkExample(exec_context);
      break;
    case PROJECT:
      PrintBlock("Project Example");
      status = ScanProjectSinkExample(exec_context);
      break;
    case GROUP_AGGREGATION:
      PrintBlock("Aggregate Example");
      status = SourceGroupAggregateSinkExample(exec_context);
      break;
    case SCALAR_AGGREGATION:
      PrintBlock("Aggregate Example");
      status = SourceScalarAggregateSinkExample(exec_context);
      break;
    case CONSUMING_SINK:
      PrintBlock("Consuming-Sink Example");
      status = SourceConsumingSinkExample(exec_context);
      break;
    case ORDER_BY_SINK:
      PrintBlock("OrderBy Example");
      status = SourceOrderBySinkExample(exec_context);
      break;
    case HASHJOIN:
      PrintBlock("HashJoin Example");
      status = SourceHashJoinSinkExample(exec_context);
      break;
    case KSELECT:
      PrintBlock("KSelect Example");
      status = SourceKSelectExample(exec_context);
      break;
    case WRITE:
      PrintBlock("Write Example");
      status = ScanFilterWriteExample(exec_context, base_save_path);
      break;
    case UNION:
      PrintBlock("Union Example");
      status = SourceUnionSinkExample(exec_context);
      break;
    default:
      break;
  }

  if (status.ok()) {
    return EXIT_SUCCESS;
  } else {
    std::cout << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
}

// (Doc section: Execution Plan Documentation Example)

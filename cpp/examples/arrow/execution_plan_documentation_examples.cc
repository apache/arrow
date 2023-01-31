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
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

template <class TYPE>
arrow::Result<std::shared_ptr<arrow::Array>> GetBinaryArrayDataSample(
    const std::vector<std::string>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
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
/// a,b
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
  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> gen() const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](cp::ExecBatch batch) { return std::make_optional(std::move(batch)); },
        batches);
    arrow::AsyncGenerator<std::optional<cp::ExecBatch>> gen;
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

arrow::Status ExecutePlanAndCollectAsTable(cp::Declaration plan) {
  // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;
  ARROW_ASSIGN_OR_RAISE(response_table, cp::DeclarationToTable(std::move(plan)));

  std::cout << "Results : " << response_table->ToString() << std::endl;

  return arrow::Status::OK();
}

// (Doc section: Scan Example)

/// \brief An example demonstrating a scan and sink node
///
/// Scan-Table
/// This example shows how scan operation can be applied on a dataset.
/// There are operations that can be applied on the scan (project, filter)
/// and the input data can be processed. The output is obtained as a table
arrow::Status ScanSinkExample() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  options->projection = cp::project({}, {});  // create empty projection

  // construct the scan node
  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  cp::Declaration scan{"scan", std::move(scan_node_options)};

  return ExecutePlanAndCollectAsTable(std::move(scan));
}
// (Doc section: Scan Example)

// (Doc section: Source Example)

/// \brief An example demonstrating a source and sink node
///
/// Source-Table Example
/// This example shows how a custom source can be used
/// in an execution plan. This includes source node using pregenerated
/// data and collecting it into a table.
///
/// This sort of custom souce is often not needed.  In most cases you can
/// use a scan (for a dataset source) or a source like table_source, array_vector_source,
/// exec_batch_source, or record_batch_source (for in-memory data)
arrow::Status SourceSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  cp::Declaration source{"source", std::move(source_node_options)};

  return ExecutePlanAndCollectAsTable(std::move(source));
}
// (Doc section: Source Example)

// (Doc section: Table Source Example)

/// \brief An example showing a table source node
///
/// TableSource-Table Example
/// This example shows how a table_source can be used
/// in an execution plan. This includes a table source node
/// receiving data from a table.  This plan simply collects the
/// data back into a table but nodes could be added that modify
/// or transform the data as well (as is shown in later examples)
arrow::Status TableSourceSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());

  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;
  int max_batch_size = 2;
  auto table_source_options = cp::TableSourceNodeOptions{table, max_batch_size};

  cp::Declaration source{"table_source", std::move(table_source_options)};

  return ExecutePlanAndCollectAsTable(std::move(source));
}
// (Doc section: Table Source Example)

// (Doc section: Filter Example)

/// \brief An example showing a filter node
///
/// Source-Filter-Table
/// This example shows how a filter can be used in an execution plan,
/// to filter data from a source. The output from the exeuction plan
/// is collected into a table.
arrow::Status ScanFilterSinkExample() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // specify the filter.  This filter removes all rows where the
  // value of the "a" column is greater than 3.
  cp::Expression filter_expr = cp::greater(cp::field_ref("a"), cp::literal(3));
  // set filter for scanner : on-disk / push-down filtering.
  // This step can be skipped if you are not reading from disk.
  options->filter = filter_expr;
  // empty projection
  options->projection = cp::project({}, {});

  // construct the scan node
  std::cout << "Initialized Scanning Options" << std::endl;

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
  std::cout << "Scan node options created" << std::endl;

  cp::Declaration scan{"scan", std::move(scan_node_options)};

  // pipe the scan node into the filter node
  // Need to set the filter in scan node options and filter node options.
  // At scan node it is used for on-disk / push-down filtering.
  // At filter node it is used for in-memory filtering.
  cp::Declaration filter{
      "filter", {std::move(scan)}, cp::FilterNodeOptions(std::move(filter_expr))};

  return ExecutePlanAndCollectAsTable(std::move(filter));
}

// (Doc section: Filter Example)

// (Doc section: Project Example)

/// \brief An example showing a project node
///
/// Scan-Project-Table
/// This example shows how a Scan operation can be used to load the data
/// into the execution plan, how a project operation can be applied on the
/// data stream and how the output is collected into a table
arrow::Status ScanProjectSinkExample() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // projection
  cp::Expression a_times_2 = cp::call("multiply", {cp::field_ref("a"), cp::literal(2)});
  options->projection = cp::project({}, {});

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  cp::Declaration scan{"scan", std::move(scan_node_options)};
  cp::Declaration project{
      "project", {std::move(scan)}, cp::ProjectNodeOptions({a_times_2})};

  return ExecutePlanAndCollectAsTable(std::move(project));
}

// (Doc section: Project Example)

// (Doc section: Scalar Aggregate Example)

/// \brief An example showing an aggregation node to aggregate an entire table
///
/// Source-Aggregation-Table
/// This example shows how an aggregation operation can be applied on a
/// execution plan resulting in a scalar output. The source node loads the
/// data and the aggregation (counting unique types in column 'a')
/// is applied on this data. The output is collected into a table (that will
/// have exactly one row)
arrow::Status SourceScalarAggregateSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  cp::Declaration source{"source", std::move(source_node_options)};
  auto aggregate_options =
      cp::AggregateNodeOptions{/*aggregates=*/{{"sum", nullptr, "a", "sum(a)"}}};
  cp::Declaration aggregate{
      "aggregate", {std::move(source)}, std::move(aggregate_options)};

  return ExecutePlanAndCollectAsTable(std::move(aggregate));
}
// (Doc section: Scalar Aggregate Example)

// (Doc section: Group Aggregate Example)

/// \brief An example showing an aggregation node to perform a group-by operation
///
/// Source-Aggregation-Table
/// This example shows how an aggregation operation can be applied on a
/// execution plan resulting in grouped output. The source node loads the
/// data and the aggregation (counting unique types in column 'a') is
/// applied on this data. The output is collected into a table that will contain
/// one row for each unique combination of group keys.
arrow::Status SourceGroupAggregateSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  cp::Declaration source{"source", std::move(source_node_options)};
  auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
  auto aggregate_options =
      cp::AggregateNodeOptions{/*aggregates=*/{{"hash_count", options, "a", "count(a)"}},
                               /*keys=*/{"b"}};
  cp::Declaration aggregate{
      "aggregate", {std::move(source)}, std::move(aggregate_options)};

  return ExecutePlanAndCollectAsTable(std::move(aggregate));
}
// (Doc section: Group Aggregate Example)

// (Doc section: ConsumingSink Example)

/// \brief An example showing a consuming sink node
///
/// Source-Consuming-Sink
/// This example shows how the data can be consumed within the execution plan
/// by using a ConsumingSink node. There is no data output from this execution plan.
arrow::Status SourceConsumingSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  cp::Declaration source{"source", std::move(source_node_options)};

  std::atomic<uint32_t> batches_seen{0};
  arrow::Future<> finish = arrow::Future<>::Make();
  struct CustomSinkNodeConsumer : public cp::SinkNodeConsumer {
    CustomSinkNodeConsumer(std::atomic<uint32_t>* batches_seen, arrow::Future<> finish)
        : batches_seen(batches_seen), finish(std::move(finish)) {}

    arrow::Status Init(const std::shared_ptr<arrow::Schema>& schema,
                       cp::BackpressureControl* backpressure_control,
                       cp::ExecPlan* plan) override {
      // This will be called as the plan is started (before the first call to Consume)
      // and provides the schema of the data coming into the node, controls for pausing /
      // resuming input, and a pointer to the plan itself which can be used to access
      // other utilities such as the thread indexer or async task scheduler.
      return arrow::Status::OK();
    }

    arrow::Status Consume(cp::ExecBatch batch) override {
      (*batches_seen)++;
      return arrow::Status::OK();
    }

    arrow::Future<> Finish() override {
      // Here you can perform whatever (possibly async) cleanup is needed, e.g. closing
      // output file handles and flushing remaining work
      return arrow::Future<>::MakeFinished();
    }

    std::atomic<uint32_t>* batches_seen;
    arrow::Future<> finish;
  };
  std::shared_ptr<CustomSinkNodeConsumer> consumer =
      std::make_shared<CustomSinkNodeConsumer>(&batches_seen, finish);

  cp::Declaration consuming_sink{"consuming_sink",
                                 {std::move(source)},
                                 cp::ConsumingSinkNodeOptions(std::move(consumer))};

  // Since we are consuming the data within the plan there is no output and we simply
  // run the plan to completion instead of collecting into a table.
  ARROW_RETURN_NOT_OK(cp::DeclarationToStatus(std::move(consuming_sink)));

  std::cout << "The consuming sink node saw " << batches_seen.load() << " batches"
            << std::endl;
  return arrow::Status::OK();
}
// (Doc section: ConsumingSink Example)

// (Doc section: OrderBySink Example)

arrow::Status ExecutePlanAndCollectAsTableWithCustomSink(
    std::shared_ptr<cp::ExecPlan> plan, std::shared_ptr<arrow::Schema> schema,
    arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen) {
  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema, std::move(sink_gen), arrow::default_memory_pool());

  // validate the ExecPlan
  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << "ExecPlan created : " << plan->ToString() << std::endl;
  // start the ExecPlan
  plan->StartProducing();

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

/// \brief An example showing an order-by node
///
/// Source-OrderBy-Sink
/// In this example, the data enters through the source node
/// and the data is ordered in the sink node. The order can be
/// ASCENDING or DESCENDING and it is configurable. The output
/// is obtained as a table from the sink node.
arrow::Status SourceOrderBySinkExample() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(*cp::threaded_exec_context()));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeSortTestBasicBatches());

  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};
  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  ARROW_RETURN_NOT_OK(cp::MakeExecNode(
      "order_by_sink", plan.get(), {source},
      cp::OrderBySinkNodeOptions{
          cp::SortOptions{{cp::SortKey{"a", cp::SortOrder::Descending}}}, &sink_gen}));

  return ExecutePlanAndCollectAsTableWithCustomSink(plan, basic_data.schema, sink_gen);
}

// (Doc section: OrderBySink Example)

// (Doc section: HashJoin Example)

/// \brief An example showing a hash join node
///
/// Source-HashJoin-Table
/// This example shows how source node gets the data and how a self-join
/// is applied on the data. The join options are configurable. The output
/// is collected into a table.
arrow::Status SourceHashJoinSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto input, MakeGroupableBatches());

  cp::Declaration left{"source", cp::SourceNodeOptions{input.schema, input.gen()}};
  cp::Declaration right{"source", cp::SourceNodeOptions{input.schema, input.gen()}};

  cp::HashJoinNodeOptions join_opts{
      cp::JoinType::INNER,
      /*left_keys=*/{"str"},
      /*right_keys=*/{"str"}, cp::literal(true), "l_", "r_"};

  cp::Declaration hashjoin{
      "hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

  return ExecutePlanAndCollectAsTable(std::move(hashjoin));
}

// (Doc section: HashJoin Example)

// (Doc section: KSelect Example)

/// \brief An example showing a select-k node
///
/// Source-KSelect
/// This example shows how K number of elements can be selected
/// either from the top or bottom. The output node is a modified
/// sink node where output can be obtained as a table.
arrow::Status SourceKSelectExample() {
  ARROW_ASSIGN_OR_RAISE(auto input, MakeGroupableBatches());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(*cp::threaded_exec_context()));
  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * source,
      cp::MakeExecNode("source", plan.get(), {},
                       cp::SourceNodeOptions{input.schema, input.gen()}));

  cp::SelectKOptions options = cp::SelectKOptions::TopKDefault(/*k=*/2, {"i32"});

  ARROW_RETURN_NOT_OK(cp::MakeExecNode("select_k_sink", plan.get(), {source},
                                       cp::SelectKSinkNodeOptions{options, &sink_gen}));

  auto schema = arrow::schema(
      {arrow::field("i32", arrow::int32()), arrow::field("str", arrow::utf8())});

  return ExecutePlanAndCollectAsTableWithCustomSink(plan, schema, sink_gen);
}

// (Doc section: KSelect Example)

// (Doc section: Write Example)

/// \brief An example showing a write node
/// \param file_path The destination to write to
///
/// Scan-Filter-Write
/// This example shows how scan node can be used to load the data
/// and after processing how it can be written to disk.
arrow::Status ScanFilterWriteExample(const std::string& file_path) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> dataset, GetDataset());

  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  // empty projection
  options->projection = cp::project({}, {});

  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};

  cp::Declaration scan{"scan", std::move(scan_node_options)};

  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

  std::string root_path = "";
  std::string uri = "file://" + file_path;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::fs::FileSystem> filesystem,
                        arrow::fs::FileSystemFromUri(uri, &root_path));

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

  arrow::dataset::WriteNodeOptions write_node_options{write_options};

  cp::Declaration write{"write", {std::move(scan)}, std::move(write_node_options)};

  // Since the write node has no output we simply run the plan to completion and the
  // data should be written
  ARROW_RETURN_NOT_OK(cp::DeclarationToStatus(std::move(write)));

  std::cout << "Dataset written to " << base_path << std::endl;
  return arrow::Status::OK();
}

// (Doc section: Write Example)

// (Doc section: Union Example)

/// \brief An example showing a union node
///
/// Source-Union-Table
/// This example shows how a union operation can be applied on two
/// data sources. The output is collected into a table.
arrow::Status SourceUnionSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  cp::Declaration lhs{"source",
                      cp::SourceNodeOptions{basic_data.schema, basic_data.gen()}};
  lhs.label = "lhs";
  cp::Declaration rhs{"source",
                      cp::SourceNodeOptions{basic_data.schema, basic_data.gen()}};
  rhs.label = "rhs";
  cp::Declaration union_plan{
      "union", {std::move(lhs), std::move(rhs)}, cp::ExecNodeOptions{}};

  return ExecutePlanAndCollectAsTable(std::move(union_plan));
}

// (Doc section: Union Example)

// (Doc section: Table Sink Example)

/// \brief An example showing a table sink node
///
/// TableSink Example
/// This example shows how a table_sink can be used
/// in an execution plan. This includes a source node
/// receiving data as batches and the table sink node
/// which emits the output as a table.
arrow::Status TableSinkExample() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(*cp::threaded_exec_context()));

  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};

  ARROW_ASSIGN_OR_RAISE(cp::ExecNode * source,
                        cp::MakeExecNode("source", plan.get(), {}, source_node_options));

  std::shared_ptr<arrow::Table> output_table;
  auto table_sink_options = cp::TableSinkNodeOptions{&output_table};

  ARROW_RETURN_NOT_OK(
      cp::MakeExecNode("table_sink", plan.get(), {source}, table_sink_options));
  // validate the ExecPlan
  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << "ExecPlan created : " << plan->ToString() << std::endl;
  // start the ExecPlan
  plan->StartProducing();

  // Wait for the plan to finish
  auto finished = plan->finished();
  RETURN_NOT_OK(finished.status());
  std::cout << "Results : " << output_table->ToString() << std::endl;
  return arrow::Status::OK();
}

// (Doc section: Table Sink Example)

// (Doc section: RecordBatchReaderSource Example)

/// \brief An example showing the usage of a RecordBatchReader as the data source.
///
/// RecordBatchReaderSourceSink Example
/// This example shows how a record_batch_reader_source can be used
/// in an execution plan. This includes the source node
/// receiving data from a TableRecordBatchReader.

arrow::Status RecordBatchReaderSourceSinkExample() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());
  std::shared_ptr<arrow::RecordBatchReader> reader =
      std::make_shared<arrow::TableBatchReader>(table);
  cp::Declaration reader_source{"record_batch_reader_source",
                                cp::RecordBatchReaderSourceNodeOptions{reader}};
  return ExecutePlanAndCollectAsTable(std::move(reader_source));
}

// (Doc section: RecordBatchReaderSource Example)

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
  TABLE_SOURCE_TABLE_SINK = 13,
  RECORD_BATCH_READER_SOURCE = 14
};

int main(int argc, char** argv) {
  if (argc < 3) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string base_save_path = argv[1];
  int mode = std::atoi(argv[2]);
  arrow::Status status;
  // ensure arrow::dataset node factories are in the registry
  arrow::dataset::internal::Initialize();
  switch (mode) {
    case SOURCE_SINK:
      PrintBlock("Source Sink Example");
      status = SourceSinkExample();
      break;
    case TABLE_SOURCE_SINK:
      PrintBlock("Table Source Sink Example");
      status = TableSourceSinkExample();
      break;
    case SCAN:
      PrintBlock("Scan Example");
      status = ScanSinkExample();
      break;
    case FILTER:
      PrintBlock("Filter Example");
      status = ScanFilterSinkExample();
      break;
    case PROJECT:
      PrintBlock("Project Example");
      status = ScanProjectSinkExample();
      break;
    case GROUP_AGGREGATION:
      PrintBlock("Aggregate Example");
      status = SourceGroupAggregateSinkExample();
      break;
    case SCALAR_AGGREGATION:
      PrintBlock("Aggregate Example");
      status = SourceScalarAggregateSinkExample();
      break;
    case CONSUMING_SINK:
      PrintBlock("Consuming-Sink Example");
      status = SourceConsumingSinkExample();
      break;
    case ORDER_BY_SINK:
      PrintBlock("OrderBy Example");
      status = SourceOrderBySinkExample();
      break;
    case HASHJOIN:
      PrintBlock("HashJoin Example");
      status = SourceHashJoinSinkExample();
      break;
    case KSELECT:
      PrintBlock("KSelect Example");
      status = SourceKSelectExample();
      break;
    case WRITE:
      PrintBlock("Write Example");
      status = ScanFilterWriteExample(base_save_path);
      break;
    case UNION:
      PrintBlock("Union Example");
      status = SourceUnionSinkExample();
      break;
    case TABLE_SOURCE_TABLE_SINK:
      PrintBlock("TableSink Example");
      status = TableSinkExample();
      break;
    case RECORD_BATCH_READER_SOURCE:
      PrintBlock("RecordBatchReaderSource Example");
      status = RecordBatchReaderSourceSinkExample();
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

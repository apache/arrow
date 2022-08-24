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

#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/compute/exec/options.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

#include <iostream>
#include <optional>

namespace compute = ::arrow::compute;

std::shared_ptr<compute::FunctionOptions> make_compute_options(std::string func_name,
                                                               cpp11::list options);

std::shared_ptr<arrow::KeyValueMetadata> strings_to_kvm(cpp11::strings metadata);

// [[arrow::export]]
std::shared_ptr<compute::ExecPlan> ExecPlan_create(bool use_threads) {
  static compute::ExecContext threaded_context{gc_memory_pool(),
                                               arrow::internal::GetCpuThreadPool()};
  auto plan = ValueOrStop(
      compute::ExecPlan::Make(use_threads ? &threaded_context : gc_context()));
  return plan;
}

std::shared_ptr<compute::ExecNode> MakeExecNodeOrStop(
    const std::string& factory_name, compute::ExecPlan* plan,
    std::vector<compute::ExecNode*> inputs, const compute::ExecNodeOptions& options) {
  return std::shared_ptr<compute::ExecNode>(
      ValueOrStop(compute::MakeExecNode(factory_name, plan, std::move(inputs), options)),
      [](...) {
        // empty destructor: ExecNode lifetime is managed by an ExecPlan
      });
}

// This class is a special RecordBatchReader that holds a reference to the
// underlying exec plan so that (1) it can request that the ExecPlan *stop*
// producing when this object is deleted and (2) it can defer requesting
// the ExecPlan to *start* producing until the first batch has been pulled.
// This allows it to be transformed (e.g., using map_batches() or head())
// and queried (i.e., used as input to another ExecPlan), at the R level
// while maintaining the ability for the entire plan to be executed at once
// (e.g., to support user-defined functions) or never executed at all (e.g.,
// to support printing a nested ExecPlan without having to execute it).
class ExecPlanReader : public arrow::RecordBatchReader {
 public:
  enum ExecPlanReaderStatus { PLAN_NOT_STARTED, PLAN_RUNNING, PLAN_FINISHED };

  ExecPlanReader(const std::shared_ptr<arrow::compute::ExecPlan>& plan,
                 const std::shared_ptr<arrow::Schema>& schema,
                 arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen)
      : schema_(schema), plan_(plan), sink_gen_(sink_gen), status_(PLAN_NOT_STARTED) {}

  std::string PlanStatus() const {
    switch (status_) {
      case PLAN_NOT_STARTED:
        return "PLAN_NOT_STARTED";
      case PLAN_RUNNING:
        return "PLAN_RUNNING";
      case PLAN_FINISHED:
        return "PLAN_FINISHED";
      default:
        return "UNKNOWN";
    }
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch_out) override {
    // TODO(ARROW-11841) check a StopToken to potentially cancel this plan

    // If this is the first batch getting pulled, tell the exec plan to
    // start producing
    if (status_ == PLAN_NOT_STARTED) {
      ARROW_RETURN_NOT_OK(StartProducing());
    }

    // If we've closed the reader, keep sending nullptr
    // (consistent with what most RecordBatchReader subclasses do)
    if (status_ == PLAN_FINISHED) {
      batch_out->reset();
      return arrow::Status::OK();
    }

    auto out = sink_gen_().result();
    if (!out.ok()) {
      StopProducing();
      return out.status();
    }

    if (out.ValueUnsafe()) {
      auto batch_result = out.ValueUnsafe()->ToRecordBatch(schema_, gc_memory_pool());
      if (!batch_result.ok()) {
        StopProducing();
        return batch_result.status();
      }

      *batch_out = batch_result.ValueUnsafe();
    } else {
      batch_out->reset();
      StopProducing();
    }

    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    StopProducing();
    return arrow::Status::OK();
  }

  const std::shared_ptr<arrow::compute::ExecPlan>& Plan() const { return plan_; }

  ~ExecPlanReader() { StopProducing(); }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::compute::ExecPlan> plan_;
  arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen_;
  int status_;

  arrow::Status StartProducing() {
    ARROW_RETURN_NOT_OK(plan_->StartProducing());
    status_ = PLAN_RUNNING;
    return arrow::Status::OK();
  }

  void StopProducing() {
    if (status_ == PLAN_RUNNING) {
      // We're done with the plan, but it may still need some time
      // to finish and clean up after itself. To do this, we give a
      // callable with its own copy of the shared_ptr<ExecPlan> so
      // that it can delete itself when it is safe to do so.
      std::shared_ptr<arrow::compute::ExecPlan> plan(plan_);
      bool not_finished_yet = plan_->finished().TryAddCallback(
          [&plan] { return [plan](const arrow::Status&) {}; });

      if (not_finished_yet) {
        plan_->StopProducing();
      }
    }

    status_ = PLAN_FINISHED;
    plan_.reset();
    sink_gen_ = arrow::MakeEmptyGenerator<std::optional<compute::ExecBatch>>();
  }
};

// [[arrow::export]]
cpp11::list ExecPlanReader__batches(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  auto result = RunWithCapturedRIfPossible<arrow::RecordBatchVector>(
      [&]() { return reader->ToRecordBatches(); });
  return arrow::r::to_r_list(ValueOrStop(result));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_ExecPlanReader(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  auto result = RunWithCapturedRIfPossible<std::shared_ptr<arrow::Table>>(
      [&]() { return reader->ToTable(); });

  return ValueOrStop(result);
}

// [[arrow::export]]
std::shared_ptr<compute::ExecPlan> ExecPlanReader__Plan(
    const std::shared_ptr<ExecPlanReader>& reader) {
  if (reader->PlanStatus() == "PLAN_FINISHED") {
    cpp11::stop("Can't extract ExecPlan from a finished ExecPlanReader");
  }

  return reader->Plan();
}

// [[arrow::export]]
std::string ExecPlanReader__PlanStatus(const std::shared_ptr<ExecPlanReader>& reader) {
  return reader->PlanStatus();
}

// [[arrow::export]]
std::shared_ptr<ExecPlanReader> ExecPlan_run(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<compute::ExecNode>& final_node, cpp11::list sort_options,
    cpp11::strings metadata, int64_t head = -1) {
  // For now, don't require R to construct SinkNodes.
  // Instead, just pass the node we should collect as an argument.
  arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;

  // Sorting uses a different sink node; there is no general sort yet
  if (sort_options.size() > 0) {
    if (head >= 0) {
      // Use the SelectK node to take only what we need
      MakeExecNodeOrStop(
          "select_k_sink", plan.get(), {final_node.get()},
          compute::SelectKSinkNodeOptions{
              arrow::compute::SelectKOptions(
                  head, std::dynamic_pointer_cast<compute::SortOptions>(
                            make_compute_options("sort_indices", sort_options))
                            ->sort_keys),
              &sink_gen});
    } else {
      MakeExecNodeOrStop("order_by_sink", plan.get(), {final_node.get()},
                         compute::OrderBySinkNodeOptions{
                             *std::dynamic_pointer_cast<compute::SortOptions>(
                                 make_compute_options("sort_indices", sort_options)),
                             &sink_gen});
    }
  } else {
    MakeExecNodeOrStop("sink", plan.get(), {final_node.get()},
                       compute::SinkNodeOptions{&sink_gen});
  }

  StopIfNotOk(plan->Validate());

  // Attach metadata to the schema
  auto out_schema = final_node->output_schema();
  if (metadata.size() > 0) {
    auto kv = strings_to_kvm(metadata);
    out_schema = out_schema->WithMetadata(kv);
  }

  return std::make_shared<ExecPlanReader>(plan, out_schema, sink_gen);
}

// [[arrow::export]]
std::string ExecPlan_ToString(const std::shared_ptr<compute::ExecPlan>& plan) {
  return plan->ToString();
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ExecNode_output_schema(
    const std::shared_ptr<compute::ExecNode>& node) {
  return node->output_schema();
}

#if defined(ARROW_R_WITH_DATASET)

#include <arrow/dataset/file_base.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

// [[dataset::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Scan(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<ds::Dataset>& dataset,
    const std::shared_ptr<compute::Expression>& filter,
    std::vector<std::string> materialized_field_names) {
  arrow::dataset::internal::Initialize();

  // TODO: pass in FragmentScanOptions
  auto options = std::make_shared<ds::ScanOptions>();

  options->use_threads = arrow::r::GetBoolOption("arrow.use_threads", true);

  options->dataset_schema = dataset->schema();

  options->filter = *filter;

  // ScanNode needs to know which fields to materialize (and which are unnecessary)
  std::vector<compute::Expression> exprs;
  for (const auto& name : materialized_field_names) {
    exprs.push_back(compute::field_ref(name));
  }

  options->projection =
      call("make_struct", std::move(exprs),
           compute::MakeStructOptions{std::move(materialized_field_names)});

  return MakeExecNodeOrStop("scan", plan.get(), {},
                            ds::ScanNodeOptions{dataset, options});
}

// [[dataset::export]]
void ExecPlan_Write(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<compute::ExecNode>& final_node, cpp11::strings metadata,
    const std::shared_ptr<ds::FileWriteOptions>& file_write_options,
    const std::shared_ptr<fs::FileSystem>& filesystem, std::string base_dir,
    const std::shared_ptr<ds::Partitioning>& partitioning, std::string basename_template,
    arrow::dataset::ExistingDataBehavior existing_data_behavior, int max_partitions,
    uint32_t max_open_files, uint64_t max_rows_per_file, uint64_t min_rows_per_group,
    uint64_t max_rows_per_group) {
  arrow::dataset::internal::Initialize();

  // TODO(ARROW-16200): expose FileSystemDatasetWriteOptions in R
  // and encapsulate this logic better
  ds::FileSystemDatasetWriteOptions opts;
  opts.file_write_options = file_write_options;
  opts.existing_data_behavior = existing_data_behavior;
  opts.filesystem = filesystem;
  opts.base_dir = base_dir;
  opts.partitioning = partitioning;
  opts.basename_template = basename_template;
  opts.max_partitions = max_partitions;
  opts.max_open_files = max_open_files;
  opts.max_rows_per_file = max_rows_per_file;
  opts.min_rows_per_group = min_rows_per_group;
  opts.max_rows_per_group = max_rows_per_group;

  auto kv = strings_to_kvm(metadata);
  MakeExecNodeOrStop("write", final_node->plan(), {final_node.get()},
                     ds::WriteNodeOptions{std::move(opts), std::move(kv)});

  StopIfNotOk(plan->Validate());

  arrow::Status result = RunWithCapturedRIfPossibleVoid([&]() {
    RETURN_NOT_OK(plan->StartProducing());
    RETURN_NOT_OK(plan->finished().status());
    return arrow::Status::OK();
  });

  StopIfNotOk(result);
}

#endif

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Filter(
    const std::shared_ptr<compute::ExecNode>& input,
    const std::shared_ptr<compute::Expression>& filter) {
  return MakeExecNodeOrStop("filter", input->plan(), {input.get()},
                            compute::FilterNodeOptions{*filter});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Project(
    const std::shared_ptr<compute::ExecNode>& input,
    const std::vector<std::shared_ptr<compute::Expression>>& exprs,
    std::vector<std::string> names) {
  // We have shared_ptrs of expressions but need the Expressions
  std::vector<compute::Expression> expressions;
  for (auto expr : exprs) {
    expressions.push_back(*expr);
  }
  return MakeExecNodeOrStop(
      "project", input->plan(), {input.get()},
      compute::ProjectNodeOptions{std::move(expressions), std::move(names)});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Aggregate(
    const std::shared_ptr<compute::ExecNode>& input, cpp11::list options,
    std::vector<std::string> key_names) {
  std::vector<arrow::compute::Aggregate> aggregates;

  for (cpp11::list name_opts : options) {
    auto function = cpp11::as_cpp<std::string>(name_opts["fun"]);
    auto opts = make_compute_options(function, name_opts["options"]);
    auto target = cpp11::as_cpp<std::string>(name_opts["target"]);
    auto name = cpp11::as_cpp<std::string>(name_opts["name"]);

    aggregates.push_back(arrow::compute::Aggregate{std::move(function), opts,
                                                   std::move(target), std::move(name)});
  }

  std::vector<arrow::FieldRef> keys;
  for (auto&& name : key_names) {
    keys.emplace_back(std::move(name));
  }
  return MakeExecNodeOrStop(
      "aggregate", input->plan(), {input.get()},
      compute::AggregateNodeOptions{std::move(aggregates), std::move(keys)});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Join(
    const std::shared_ptr<compute::ExecNode>& input, int type,
    const std::shared_ptr<compute::ExecNode>& right_data,
    std::vector<std::string> left_keys, std::vector<std::string> right_keys,
    std::vector<std::string> left_output, std::vector<std::string> right_output,
    std::string output_suffix_for_left, std::string output_suffix_for_right) {
  std::vector<arrow::FieldRef> left_refs, right_refs, left_out_refs, right_out_refs;
  for (auto&& name : left_keys) {
    left_refs.emplace_back(std::move(name));
  }
  for (auto&& name : right_keys) {
    right_refs.emplace_back(std::move(name));
  }
  for (auto&& name : left_output) {
    left_out_refs.emplace_back(std::move(name));
  }
  if (type != 0 && type != 2) {
    // Don't include out_refs in semi/anti join
    for (auto&& name : right_output) {
      right_out_refs.emplace_back(std::move(name));
    }
  }

  // TODO: we should be able to use this enum directly
  compute::JoinType join_type;
  if (type == 0) {
    join_type = compute::JoinType::LEFT_SEMI;
  } else if (type == 1) {
    // Not readily called from R bc dplyr::semi_join is LEFT_SEMI
    join_type = compute::JoinType::RIGHT_SEMI;
  } else if (type == 2) {
    join_type = compute::JoinType::LEFT_ANTI;
  } else if (type == 3) {
    // Not readily called from R bc dplyr::semi_join is LEFT_SEMI
    join_type = compute::JoinType::RIGHT_ANTI;
  } else if (type == 4) {
    join_type = compute::JoinType::INNER;
  } else if (type == 5) {
    join_type = compute::JoinType::LEFT_OUTER;
  } else if (type == 6) {
    join_type = compute::JoinType::RIGHT_OUTER;
  } else if (type == 7) {
    join_type = compute::JoinType::FULL_OUTER;
  } else {
    cpp11::stop("todo");
  }

  return MakeExecNodeOrStop(
      "hashjoin", input->plan(), {input.get(), right_data.get()},
      compute::HashJoinNodeOptions{
          join_type, std::move(left_refs), std::move(right_refs),
          std::move(left_out_refs), std::move(right_out_refs), compute::literal(true),
          std::move(output_suffix_for_left), std::move(output_suffix_for_right)});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Union(
    const std::shared_ptr<compute::ExecNode>& input,
    const std::shared_ptr<compute::ExecNode>& right_data) {
  return MakeExecNodeOrStop("union", input->plan(), {input.get(), right_data.get()}, {});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_SourceNode(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  arrow::compute::SourceNodeOptions options{
      /*output_schema=*/reader->schema(),
      /*generator=*/ValueOrStop(
          compute::MakeReaderGenerator(reader, arrow::internal::GetCpuThreadPool()))};

  return MakeExecNodeOrStop("source", plan.get(), {}, options);
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_TableSourceNode(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<arrow::Table>& table) {
  arrow::compute::TableSourceNodeOptions options{/*table=*/table,
                                                 // TODO: make batch_size configurable
                                                 /*batch_size=*/1048576};

  return MakeExecNodeOrStop("table_source", plan.get(), {}, options);
}

#if defined(ARROW_R_WITH_SUBSTRAIT)

#include <arrow/engine/substrait/api.h>

// Just for example usage until a C++ method is available that implements
// a RecordBatchReader output (ARROW-15849)
class AccumulatingConsumer : public compute::SinkNodeConsumer {
 public:
  const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches() { return batches_; }

  arrow::Status Init(const std::shared_ptr<arrow::Schema>& schema,
                     compute::BackpressureControl* backpressure_control) override {
    schema_ = schema;
    return arrow::Status::OK();
  }

  arrow::Status Consume(compute::ExecBatch batch) override {
    auto record_batch = batch.ToRecordBatch(schema_);
    ARROW_RETURN_NOT_OK(record_batch);
    batches_.push_back(record_batch.ValueUnsafe());

    return arrow::Status::OK();
  }

  arrow::Future<> Finish() override { return arrow::Future<>::MakeFinished(); }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

// Expose these so that it's easier to write tests

// [[substrait::export]]
std::string substrait__internal__SubstraitToJSON(
    const std::shared_ptr<arrow::Buffer>& serialized_plan) {
  return ValueOrStop(arrow::engine::internal::SubstraitToJSON("Plan", *serialized_plan));
}

// [[substrait::export]]
std::shared_ptr<arrow::Buffer> substrait__internal__SubstraitFromJSON(
    std::string substrait_json) {
  return ValueOrStop(arrow::engine::internal::SubstraitFromJSON("Plan", substrait_json));
}

// [[substrait::export]]
std::shared_ptr<arrow::Table> ExecPlan_run_substrait(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<arrow::Buffer>& serialized_plan) {
  std::vector<std::shared_ptr<AccumulatingConsumer>> consumers;

  std::function<std::shared_ptr<compute::SinkNodeConsumer>()> consumer_factory = [&] {
    consumers.emplace_back(new AccumulatingConsumer());
    return consumers.back();
  };

  arrow::Result<std::vector<compute::Declaration>> maybe_decls =
      ValueOrStop(arrow::engine::DeserializePlans(*serialized_plan, consumer_factory));
  std::vector<compute::Declaration> decls = std::move(ValueOrStop(maybe_decls));

  // For now, the Substrait plan must include a 'read' that points to
  // a Parquet file (instead of using a source node create in Arrow)
  for (const compute::Declaration& decl : decls) {
    auto node = decl.AddToPlan(plan.get());
    StopIfNotOk(node.status());
  }

  StopIfNotOk(plan->Validate());
  StopIfNotOk(plan->StartProducing());
  StopIfNotOk(plan->finished().status());

  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  for (const auto& consumer : consumers) {
    for (const auto& batch : consumer->batches()) {
      all_batches.push_back(batch);
    }
  }

  return ValueOrStop(arrow::Table::FromRecordBatches(std::move(all_batches)));
}

#endif

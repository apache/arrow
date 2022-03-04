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

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/compute/exec/options.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/optional.h>
#include <arrow/util/thread_pool.h>

#include <iostream>

namespace compute = ::arrow::compute;

std::shared_ptr<compute::FunctionOptions> make_compute_options(std::string func_name,
                                                               cpp11::list options);

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

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> ExecPlan_run(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<compute::ExecNode>& final_node, cpp11::list sort_options,
    int64_t head = -1) {
  // For now, don't require R to construct SinkNodes.
  // Instead, just pass the node we should collect as an argument.
  arrow::AsyncGenerator<arrow::util::optional<compute::ExecBatch>> sink_gen;

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
      final_node->output_schema(),
      [stop_producing, plan, sink_gen] { return sink_gen(); }, gc_memory_pool());
}

// [[arrow::export]]
void ExecPlan_StopProducing(const std::shared_ptr<compute::ExecPlan>& plan) {
  plan->StopProducing();
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ExecNode_output_schema(
    const std::shared_ptr<compute::ExecNode>& node) {
  return node->output_schema();
}

#if defined(ARROW_R_WITH_DATASET)

#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

// [[dataset::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Scan(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<arrow::dataset::Dataset>& dataset,
    const std::shared_ptr<compute::Expression>& filter,
    std::vector<std::string> materialized_field_names) {
  arrow::dataset::internal::Initialize();

  // TODO: pass in FragmentScanOptions
  auto options = std::make_shared<arrow::dataset::ScanOptions>();

  options->use_threads = arrow::r::GetBoolOption("arrow.use_threads", true);

  options->dataset_schema = dataset->schema();

  // ScanNode needs the filter to do predicate pushdown and skip partitions
  options->filter = ValueOrStop(filter->Bind(*dataset->schema()));

  // ScanNode needs to know which fields to materialize (and which are unnecessary)
  std::vector<compute::Expression> exprs;
  for (const auto& name : materialized_field_names) {
    exprs.push_back(compute::field_ref(name));
  }

  options->projection =
      ValueOrStop(call("make_struct", std::move(exprs),
                       compute::MakeStructOptions{std::move(materialized_field_names)})
                      .Bind(*dataset->schema()));

  return MakeExecNodeOrStop("scan", plan.get(), {},
                            arrow::dataset::ScanNodeOptions{dataset, options});
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
    std::vector<std::string> target_names, std::vector<std::string> out_field_names,
    std::vector<std::string> key_names) {
  std::vector<arrow::compute::internal::Aggregate> aggregates;
  std::vector<std::shared_ptr<arrow::compute::FunctionOptions>> keep_alives;

  for (cpp11::list name_opts : options) {
    auto name = cpp11::as_cpp<std::string>(name_opts[0]);
    auto opts = make_compute_options(name, name_opts[1]);

    aggregates.push_back(
        arrow::compute::internal::Aggregate{std::move(name), opts.get()});
    keep_alives.push_back(std::move(opts));
  }

  std::vector<arrow::FieldRef> targets, keys;
  for (auto&& name : target_names) {
    targets.emplace_back(std::move(name));
  }
  for (auto&& name : key_names) {
    keys.emplace_back(std::move(name));
  }
  return MakeExecNodeOrStop(
      "aggregate", input->plan(), {input.get()},
      compute::AggregateNodeOptions{std::move(aggregates), std::move(targets),
                                    std::move(out_field_names), std::move(keys)});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Join(
    const std::shared_ptr<compute::ExecNode>& input, int type,
    const std::shared_ptr<compute::ExecNode>& right_data,
    std::vector<std::string> left_keys, std::vector<std::string> right_keys,
    std::vector<std::string> left_output, std::vector<std::string> right_output) {
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
      compute::HashJoinNodeOptions{join_type, std::move(left_refs), std::move(right_refs),
                                   std::move(left_out_refs), std::move(right_out_refs)});
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

#endif

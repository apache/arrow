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
#include <arrow/table.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

namespace compute = ::arrow::compute;

std::shared_ptr<compute::FunctionOptions> make_compute_options(std::string func_name,
                                                               cpp11::list options);

template <typename T>
void AddKeepalive(compute::ExecPlan* plan, T keepalive) {
  struct Callback {
    void operator()(const arrow::Status&) && {}
    T keepalive;
  };
  plan->finished().AddCallback(Callback{std::move(keepalive)});
}

// [[arrow::export]]
std::shared_ptr<compute::ExecPlan> ExecPlan_create(bool use_threads) {
  auto executor = use_threads ? arrow::internal::GetCpuThreadPool() : nullptr;
  auto context = std::make_shared<compute::ExecContext>(gc_memory_pool(), executor);
  auto plan = ValueOrStop(compute::ExecPlan::Make(context.get()));
  AddKeepalive(plan.get(), std::move(context));
  return plan;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> ExecPlan_run(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<compute::ExecNode>& final_node) {
  // For now, don't require R to construct SinkNodes.
  // Instead, just pass the node we should collect as an argument.
  auto sink_gen = compute::MakeSinkNode(final_node.get(), "sink");

  StopIfNotOk(plan->Validate());
  StopIfNotOk(plan->StartProducing());

  std::shared_ptr<arrow::RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      final_node->output_schema(), std::move(sink_gen), gc_memory_pool());

  plan->finished().Wait();
  return ValueOrStop(arrow::Table::FromRecordBatchReader(sink_reader.get()));
}

std::shared_ptr<compute::ExecNode> ExecNodeOrStop(
    arrow::Result<compute::ExecNode*> maybe_node) {
  return std::shared_ptr<compute::ExecNode>(ValueOrStop(maybe_node), [](...) {
    // empty destructor: ExecNode lifetime is managed by an ExecPlan
  });
}

#if defined(ARROW_R_WITH_DATASET)

#include <arrow/dataset/scanner.h>

// [[dataset::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Scan(
    const std::shared_ptr<compute::ExecPlan>& plan,
    const std::shared_ptr<arrow::dataset::Dataset>& dataset,
    const std::shared_ptr<compute::Expression>& filter,
    std::vector<std::string> materialized_field_names) {
  // TODO: pass in FragmentScanOptions
  auto options = std::make_shared<arrow::dataset::ScanOptions>();

  options->use_async = true;

  options->dataset_schema = dataset->schema();

  // ScanNode needs the filter to do predicate pushdown and skip partitions
  options->filter = ValueOrStop(filter->Bind(*dataset->schema()));

  // ScanNode needs to know which fields to materialize (and which are unnecessary)
  std::vector<compute::Expression> exprs;
  for (const auto& name : materialized_field_names) {
    exprs.push_back(compute::field_ref(name));
  }

  options->projection =
      ValueOrStop(call("project", std::move(exprs),
                       compute::ProjectOptions{std::move(materialized_field_names)})
                      .Bind(*dataset->schema()));

  return ExecNodeOrStop(arrow::dataset::MakeScanNode(plan.get(), dataset, options));
}

#endif

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_Filter(
    const std::shared_ptr<compute::ExecNode>& input,
    const std::shared_ptr<compute::Expression>& filter) {
  return ExecNodeOrStop(
      compute::MakeFilterNode(input.get(), /*label=*/"filter", *filter));
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
  return ExecNodeOrStop(compute::MakeProjectNode(
      input.get(), /*label=*/"project", std::move(expressions), std::move(names)));
}

// [[arrow::export]]
std::shared_ptr<compute::ExecNode> ExecNode_ScalarAggregate(
    const std::shared_ptr<compute::ExecNode>& input, cpp11::list options,
    std::vector<std::string> target_names, std::vector<std::string> out_field_names) {
  std::vector<arrow::compute::internal::Aggregate> aggregates;

  for (cpp11::list name_opts : options) {
    auto name = cpp11::as_cpp<std::string>(name_opts[0]);
    auto opts = make_compute_options(name, name_opts[1]);

    aggregates.push_back(
        arrow::compute::internal::Aggregate{std::move(name), opts.get()});

    AddKeepalive(input->plan(), std::move(opts));
  }

  std::vector<arrow::FieldRef> targets;
  for (auto&& name : target_names) {
    targets.emplace_back(std::move(name));
  }
  return ExecNodeOrStop(compute::MakeScalarAggregateNode(
      input.get(), /*label=*/"scalar_agg", std::move(aggregates), std::move(targets),
      std::move(out_field_names)));
}

#endif

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

#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

using compute::ExecSpan;
using compute::ExecValue;
using compute::Function;
using compute::FunctionOptions;
using compute::Grouper;
using compute::HashAggregateKernel;
using compute::Kernel;
using compute::KernelContext;
using compute::KernelInitArgs;
using compute::KernelState;
using compute::RowSegmenter;
using compute::ScalarAggregateKernel;
using compute::Segment;

namespace acero {
namespace aggregate {

Status GroupByNode::Init() {
  output_task_group_id_ = plan_->query_context()->RegisterTaskGroup(
      [this](size_t, int64_t task_id) { return OutputNthBatch(task_id); },
      [](size_t) { return Status::OK(); });
  return Status::OK();
}

Result<AggregateNodeArgs<HashAggregateKernel>> GroupByNode::MakeAggregateNodeArgs(
    const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggs,
    ExecContext* ctx, const bool is_cpu_parallel) {
  // Find input field indices for key fields
  std::vector<int> key_field_ids(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, keys[i].FindOne(*input_schema));
    key_field_ids[i] = match[0];
  }

  // Find input field indices for segment key fields
  std::vector<int> segment_key_field_ids(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, segment_keys[i].FindOne(*input_schema));
    segment_key_field_ids[i] = match[0];
  }

  // Check key fields and segment key fields are disjoint
  std::unordered_set<int> key_field_id_set(key_field_ids.begin(), key_field_ids.end());
  for (const auto& segment_key_field_id : segment_key_field_ids) {
    if (key_field_id_set.find(segment_key_field_id) != key_field_id_set.end()) {
      return Status::Invalid("Group-by aggregation with field '",
                             input_schema->field(segment_key_field_id)->name(),
                             "' as both key and segment key");
    }
  }

  // Find input field indices for aggregates
  std::vector<std::vector<int>> agg_src_fieldsets(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    const auto& target_fieldset = aggs[i].target;
    for (const auto& target : target_fieldset) {
      ARROW_ASSIGN_OR_RAISE(auto match, target.FindOne(*input_schema));
      agg_src_fieldsets[i].push_back(match[0]);
    }
  }

  // Build vector of aggregate source field data types
  std::vector<std::vector<TypeHolder>> agg_src_types(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    for (const auto& agg_src_field_id : agg_src_fieldsets[i]) {
      agg_src_types[i].push_back(input_schema->field(agg_src_field_id)->type().get());
    }
  }

  // Build vector of segment key field data types
  std::vector<TypeHolder> segment_key_types(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    auto segment_key_field_id = segment_key_field_ids[i];
    segment_key_types[i] = input_schema->field(segment_key_field_id)->type().get();
  }

  ARROW_ASSIGN_OR_RAISE(auto segmenter, RowSegmenter::Make(std::move(segment_key_types),
                                                           /*nullable_keys=*/false, ctx));

  // Construct aggregates
  ARROW_ASSIGN_OR_RAISE(auto agg_kernels, GetKernels(ctx, aggs, agg_src_types));

  if (is_cpu_parallel) {
    if (segment_keys.size() > 0) {
      return Status::NotImplemented(
          "Segmented aggregation in a multi-threaded execution context");
    }

    for (auto kernel : agg_kernels) {
      if (kernel->ordered) {
        return Status::NotImplemented(
            "Using ordered aggregator in multiple threaded execution is not supported");
      }
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto agg_states,
                        InitKernels(agg_kernels, ctx, aggs, agg_src_types));

  ARROW_ASSIGN_OR_RAISE(
      FieldVector agg_result_fields,
      ResolveKernels(aggs, agg_kernels, agg_states, ctx, agg_src_types));

  // Build field vector for output schema
  FieldVector output_fields{keys.size() + segment_keys.size() + aggs.size()};

  // First output is segment keys, followed by keys, followed by aggregates themselves
  // This matches the behavior described by Substrait and also tends to be the behavior
  // in SQL engines
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    int segment_key_field_id = segment_key_field_ids[i];
    output_fields[i] = input_schema->field(segment_key_field_id);
  }
  size_t base = segment_keys.size();
  for (size_t i = 0; i < keys.size(); ++i) {
    int key_field_id = key_field_ids[i];
    output_fields[base + i] = input_schema->field(key_field_id);
  }
  base += keys.size();
  for (size_t i = 0; i < aggs.size(); ++i) {
    output_fields[base + i] = agg_result_fields[i]->WithName(aggs[i].name);
  }

  return AggregateNodeArgs<HashAggregateKernel>{schema(std::move(output_fields)),
                                                std::move(key_field_ids),
                                                std::move(segment_key_field_ids),
                                                std::move(segmenter),
                                                std::move(agg_src_fieldsets),
                                                std::move(aggs),
                                                std::move(agg_kernels),
                                                std::move(agg_src_types),
                                                /*states=*/{}};
}

Result<ExecNode*> GroupByNode::Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                    const ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "GroupByNode"));

  auto input = inputs[0];
  const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
  const auto& keys = aggregate_options.keys;
  const auto& segment_keys = aggregate_options.segment_keys;
  auto aggs = aggregate_options.aggregates;
  bool is_cpu_parallel = plan->query_context()->executor()->GetCapacity() > 1;

  const auto& input_schema = input->output_schema();
  auto exec_ctx = plan->query_context()->exec_context();
  ARROW_ASSIGN_OR_RAISE(
      auto args, MakeAggregateNodeArgs(input_schema, keys, segment_keys, aggs, exec_ctx,
                                       is_cpu_parallel));

  return input->plan()->EmplaceNode<GroupByNode>(
      input, std::move(args.output_schema), std::move(args.grouping_key_field_ids),
      std::move(args.segment_key_field_ids), std::move(args.segmenter),
      std::move(args.kernel_intypes), std::move(args.target_fieldsets),
      std::move(args.aggregates), std::move(args.kernels));
}

Status GroupByNode::ResetKernelStates() {
  auto ctx = plan()->query_context()->exec_context();
  ARROW_RETURN_NOT_OK(InitKernels(agg_kernels_, ctx, aggs_, agg_src_types_));
  return Status::OK();
}

Status GroupByNode::Consume(ExecSpan batch) {
  size_t thread_index = plan_->query_context()->GetThreadIndex();
  if (thread_index >= local_states_.size()) {
    return Status::IndexError("thread index ", thread_index, " is out of range [0, ",
                              local_states_.size(), ")");
  }

  auto state = &local_states_[thread_index];
  RETURN_NOT_OK(InitLocalStateIfNeeded(state));

  // Create a batch with key columns
  std::vector<ExecValue> keys(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    keys[i] = batch[key_field_ids_[i]];
  }
  ExecSpan key_batch(std::move(keys), batch.length);

  // Create a batch with group ids
  ARROW_ASSIGN_OR_RAISE(Datum id_batch, state->grouper->Consume(key_batch));

  // Execute aggregate kernels
  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Consume"}});
    auto ctx = plan_->query_context()->exec_context();
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(state->agg_states[i].get());

    std::vector<ExecValue> column_values;
    for (const int field : agg_src_fieldsets_[i]) {
      column_values.push_back(batch[field]);
    }
    column_values.emplace_back(*id_batch.array());
    ExecSpan agg_batch(std::move(column_values), batch.length);
    RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, state->grouper->num_groups()));
    RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_batch));
  }

  return Status::OK();
}

Status GroupByNode::Merge() {
  arrow::util::tracing::Span span;
  START_COMPUTE_SPAN(span, "Merge",
                     {{"group_by", ToStringExtra(0)}, {"node.label", label()}});
  ThreadLocalState* state0 = &local_states_[0];
  for (size_t i = 1; i < local_states_.size(); ++i) {
    ThreadLocalState* state = &local_states_[i];
    if (!state->grouper) {
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, state->grouper->GetUniques());
    ARROW_ASSIGN_OR_RAISE(Datum transposition,
                          state0->grouper->Consume(ExecSpan(other_keys)));
    state->grouper.reset();

    for (size_t span_i = 0; span_i < agg_kernels_.size(); ++span_i) {
      arrow::util::tracing::Span span_item;
      START_COMPUTE_SPAN(
          span_item, aggs_[span_i].function,
          {{"function.name", aggs_[span_i].function},
           {"function.options",
            aggs_[span_i].options ? aggs_[span_i].options->ToString() : "<NULLPTR>"},
           {"function.kind", std::string(kind_name()) + "::Merge"}});

      auto ctx = plan_->query_context()->exec_context();
      KernelContext batch_ctx{ctx};
      DCHECK(state0->agg_states[span_i]);
      batch_ctx.SetState(state0->agg_states[span_i].get());

      RETURN_NOT_OK(
          agg_kernels_[span_i]->resize(&batch_ctx, state0->grouper->num_groups()));
      RETURN_NOT_OK(agg_kernels_[span_i]->merge(
          &batch_ctx, std::move(*state->agg_states[span_i]), *transposition.array()));
      state->agg_states[span_i].reset();
    }
  }
  return Status::OK();
}

Result<ExecBatch> GroupByNode::Finalize() {
  arrow::util::tracing::Span span;
  START_COMPUTE_SPAN(span, "Finalize",
                     {{"group_by", ToStringExtra(0)}, {"node.label", label()}});

  ThreadLocalState* state = &local_states_[0];
  // If we never got any batches, then state won't have been initialized
  RETURN_NOT_OK(InitLocalStateIfNeeded(state));

  // Allocate a batch for output
  ExecBatch out_data{{}, state->grouper->num_groups()};
  out_data.values.resize(agg_kernels_.size() + key_field_ids_.size() +
                         segment_key_field_ids_.size());

  // Segment keys come first
  PlaceFields(out_data, 0, segmenter_values_);
  // Followed by keys
  ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, state->grouper->GetUniques());
  std::move(out_keys.values.begin(), out_keys.values.end(),
            out_data.values.begin() + segment_key_field_ids_.size());
  // And finally, the aggregates themselves
  std::size_t base = segment_key_field_ids_.size() + key_field_ids_.size();
  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    arrow::util::tracing::Span span_item;
    START_COMPUTE_SPAN(span_item, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Finalize"}});
    KernelContext batch_ctx{plan_->query_context()->exec_context()};
    batch_ctx.SetState(state->agg_states[i].get());
    RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out_data.values[i + base]));
    state->agg_states[i].reset();
  }
  state->grouper.reset();

  return out_data;
}

Status GroupByNode::OutputNthBatch(int64_t n) {
  int64_t batch_size = output_batch_size();
  return output_->InputReceived(this, out_data_.Slice(batch_size * n, batch_size));
}

Status GroupByNode::OutputResult(bool is_last) {
  // To simplify merging, ensure that the first grouper is nonempty
  for (size_t i = 0; i < local_states_.size(); i++) {
    if (local_states_[i].grouper) {
      std::swap(local_states_[i], local_states_[0]);
      break;
    }
  }

  RETURN_NOT_OK(Merge());
  ARROW_ASSIGN_OR_RAISE(out_data_, Finalize());

  int64_t num_output_batches = bit_util::CeilDiv(out_data_.length, output_batch_size());
  total_output_batches_ += static_cast<int>(num_output_batches);
  if (is_last) {
    ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_output_batches_));
    RETURN_NOT_OK(plan_->query_context()->StartTaskGroup(output_task_group_id_,
                                                         num_output_batches));
  } else {
    for (int64_t i = 0; i < num_output_batches; i++) {
      ARROW_RETURN_NOT_OK(OutputNthBatch(i));
    }
    ARROW_RETURN_NOT_OK(ResetKernelStates());
  }
  return Status::OK();
}

Status GroupByNode::InputReceived(ExecNode* input, ExecBatch batch) {
  auto scope = TraceInputReceived(batch);

  DCHECK_EQ(input, inputs_[0]);

  auto handler = [this](const ExecBatch& full_batch, const Segment& segment) {
    if (!segment.extends && segment.offset == 0) RETURN_NOT_OK(OutputResult(false));
    auto exec_batch = full_batch.Slice(segment.offset, segment.length);
    auto batch = ExecSpan(exec_batch);
    RETURN_NOT_OK(Consume(batch));
    RETURN_NOT_OK(
        ExtractSegmenterValues(&segmenter_values_, exec_batch, segment_key_field_ids_));
    if (!segment.is_open) RETURN_NOT_OK(OutputResult(false));
    return Status::OK();
  };
  ARROW_RETURN_NOT_OK(
      HandleSegments(segmenter_.get(), batch, segment_key_field_ids_, handler));

  if (input_counter_.Increment()) {
    ARROW_RETURN_NOT_OK(OutputResult(/*is_last=*/true));
  }
  return Status::OK();
}

Status GroupByNode::InputFinished(ExecNode* input, int total_batches) {
  auto scope = TraceFinish();
  DCHECK_EQ(input, inputs_[0]);

  if (input_counter_.SetTotal(total_batches)) {
    RETURN_NOT_OK(OutputResult(/*is_last=*/true));
  }
  return Status::OK();
}

std::string GroupByNode::ToStringExtra(int indent) const {
  std::stringstream ss;
  const auto input_schema = inputs_[0]->output_schema();
  ss << "keys=[";
  for (size_t i = 0; i < key_field_ids_.size(); i++) {
    if (i > 0) ss << ", ";
    ss << '"' << input_schema->field(key_field_ids_[i])->name() << '"';
  }
  ss << "], ";
  AggregatesToString(&ss, *input_schema, aggs_, agg_src_fieldsets_, indent);
  return ss.str();
}

Status GroupByNode::InitLocalStateIfNeeded(ThreadLocalState* state) {
  // Get input schema
  auto input_schema = inputs_[0]->output_schema();

  if (state->grouper != nullptr) return Status::OK();

  // Build vector of key field data types
  std::vector<TypeHolder> key_types(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    auto key_field_id = key_field_ids_[i];
    key_types[i] = input_schema->field(key_field_id)->type().get();
  }

  // Construct grouper
  ARROW_ASSIGN_OR_RAISE(state->grouper,
                        Grouper::Make(key_types, plan_->query_context()->exec_context()));

  // Build vector of aggregate source field data types
  std::vector<std::vector<TypeHolder>> agg_src_types(agg_kernels_.size());
  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    for (const auto& field_id : agg_src_fieldsets_[i]) {
      agg_src_types[i].emplace_back(input_schema->field(field_id)->type().get());
    }
  }

  ARROW_ASSIGN_OR_RAISE(state->agg_states,
                        InitKernels(agg_kernels_, plan_->query_context()->exec_context(),
                                    aggs_, agg_src_types));

  return Status::OK();
}

}  // namespace aggregate
}  // namespace acero
}  // namespace arrow

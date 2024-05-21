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
#include <thread>
#include <unordered_set>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
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

Result<AggregateNodeArgs<ScalarAggregateKernel>>
ScalarAggregateNode::MakeAggregateNodeArgs(const std::shared_ptr<Schema>& input_schema,
                                           const std::vector<FieldRef>& keys,
                                           const std::vector<FieldRef>& segment_keys,
                                           const std::vector<Aggregate>& aggs,
                                           ExecContext* exec_ctx, size_t concurrency,
                                           bool is_cpu_parallel) {
  // Copy (need to modify options pointer below)
  std::vector<Aggregate> aggregates(aggs);
  std::vector<int> segment_field_ids(segment_keys.size());
  std::vector<TypeHolder> segment_key_types(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(FieldPath match, segment_keys[i].FindOne(*input_schema));
    if (match.indices().size() > 1) {
      // ARROW-18369: Support nested references as segment ids
      return Status::Invalid("Nested references cannot be used as segment ids");
    }
    segment_field_ids[i] = match[0];
    segment_key_types[i] = input_schema->field(match[0])->type().get();
  }

  ARROW_ASSIGN_OR_RAISE(auto segmenter,
                        RowSegmenter::Make(std::move(segment_key_types),
                                           /*nullable_keys=*/false, exec_ctx));

  std::vector<std::vector<TypeHolder>> kernel_intypes(aggregates.size());
  std::vector<const ScalarAggregateKernel*> kernels(aggregates.size());
  std::vector<std::vector<std::unique_ptr<KernelState>>> states(kernels.size());
  FieldVector fields(kernels.size() + segment_keys.size());

  // Output the segment keys first, followed by the aggregates
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(fields[i], segment_keys[i].GetOne(*input_schema));
  }

  std::vector<std::vector<int>> target_fieldsets(kernels.size());
  std::size_t base = segment_keys.size();
  for (size_t i = 0; i < kernels.size(); ++i) {
    const auto& target_fieldset = aggregates[i].target;
    for (const auto& target : target_fieldset) {
      ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(*input_schema));
      target_fieldsets[i].push_back(match[0]);
    }

    ARROW_ASSIGN_OR_RAISE(auto function,
                          exec_ctx->func_registry()->GetFunction(aggregates[i].function));

    if (function->kind() != Function::SCALAR_AGGREGATE) {
      if (function->kind() == Function::HASH_AGGREGATE) {
        return Status::Invalid("The provided function (", aggregates[i].function,
                               ") is a hash aggregate function.  Since there are no "
                               "keys to group by, a scalar aggregate function was "
                               "expected (normally these do not start with hash_)");
      }
      return Status::Invalid("The provided function(", aggregates[i].function,
                             ") is not an aggregate function");
    }

    std::vector<TypeHolder> in_types;
    for (const auto& target : target_fieldsets[i]) {
      in_types.emplace_back(input_schema->field(target)->type().get());
    }
    kernel_intypes[i] = in_types;
    ARROW_ASSIGN_OR_RAISE(const Kernel* kernel,
                          function->DispatchExact(kernel_intypes[i]));
    const auto* agg_kernel = static_cast<const ScalarAggregateKernel*>(kernel);
    if (is_cpu_parallel && agg_kernel->ordered) {
      return Status::NotImplemented(
          "Using ordered aggregator in multiple threaded execution is not supported");
    }

    kernels[i] = agg_kernel;

    if (aggregates[i].options == nullptr) {
      DCHECK(!function->doc().options_required);
      const auto* default_options = function->default_options();
      if (default_options) {
        aggregates[i].options = default_options->Copy();
      }
    }

    KernelContext kernel_ctx{exec_ctx};
    states[i].resize(concurrency);
    RETURN_NOT_OK(Kernel::InitAll(
        &kernel_ctx,
        KernelInitArgs{kernels[i], kernel_intypes[i], aggregates[i].options.get()},
        &states[i]));

    // pick one to resolve the kernel signature
    kernel_ctx.SetState(states[i][0].get());
    ARROW_ASSIGN_OR_RAISE(auto out_type, kernels[i]->signature->out_type().Resolve(
                                             &kernel_ctx, kernel_intypes[i]));

    fields[base + i] = field(aggregates[i].name, out_type.GetSharedPtr());
  }

  return AggregateNodeArgs<ScalarAggregateKernel>{
      schema(std::move(fields)),
      /*grouping_key_field_ids=*/{}, std::move(segment_field_ids),
      std::move(segmenter),          std::move(target_fieldsets),
      std::move(aggregates),         std::move(kernels),
      std::move(kernel_intypes),     std::move(states)};
}

Result<ExecNode*> ScalarAggregateNode::Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                            const ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ScalarAggregateNode"));

  const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
  auto aggregates = aggregate_options.aggregates;
  const auto& keys = aggregate_options.keys;
  const auto& segment_keys = aggregate_options.segment_keys;
  const auto concurrency = plan->query_context()->max_concurrency();
  // We can't use concurrency == 1 because that include I/O concurrency
  const bool is_cpu_parallel = plan->query_context()->executor()->GetCapacity() > 1;

  if (keys.size() > 0) {
    return Status::Invalid("Scalar aggregation with some key");
  }
  if (is_cpu_parallel && segment_keys.size() > 0) {
    return Status::NotImplemented("Segmented aggregation in a multi-threaded plan");
  }

  const auto& input_schema = inputs[0]->output_schema();
  auto exec_ctx = plan->query_context()->exec_context();

  ARROW_ASSIGN_OR_RAISE(
      auto args, MakeAggregateNodeArgs(input_schema, keys, segment_keys, aggregates,
                                       exec_ctx, concurrency, is_cpu_parallel));

  if (is_cpu_parallel) {
    for (auto& kernel : args.kernels) {
      if (kernel->ordered) {
        return Status::NotImplemented(
            "Using ordered aggregator in multiple threaded execution is not supported");
      }
    }
  }

  return plan->EmplaceNode<ScalarAggregateNode>(
      plan, std::move(inputs), std::move(args.output_schema), std::move(args.segmenter),
      std::move(args.segment_key_field_ids), std::move(args.target_fieldsets),
      std::move(args.aggregates), std::move(args.kernels), std::move(args.kernel_intypes),
      std::move(args.states));
}

Status ScalarAggregateNode::DoConsume(const ExecSpan& batch, size_t thread_index) {
  for (size_t i = 0; i < kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Consume"}});
    KernelContext batch_ctx{plan()->query_context()->exec_context()};
    DCHECK_LT(thread_index, states_[i].size());
    batch_ctx.SetState(states_[i][thread_index].get());

    std::vector<ExecValue> column_values;
    for (const int field : target_fieldsets_[i]) {
      column_values.push_back(batch.values[field]);
    }
    ExecSpan column_batch{std::move(column_values), batch.length};
    RETURN_NOT_OK(kernels_[i]->consume(&batch_ctx, column_batch));
  }
  return Status::OK();
}

Status ScalarAggregateNode::InputReceived(ExecNode* input, ExecBatch batch) {
  auto scope = TraceInputReceived(batch);
  DCHECK_EQ(input, inputs_[0]);

  auto thread_index = plan_->query_context()->GetThreadIndex();
  auto handler = [this, thread_index](const ExecBatch& full_batch,
                                      const Segment& segment) {
    // (1) The segment is starting of a new segment group and points to
    // the beginning of the batch, then it means no data in the batch belongs
    // to the current segment group. We can output and reset kernel states.
    if (!segment.extends && segment.offset == 0) RETURN_NOT_OK(OutputResult(false));

    // We add segment to the current segment group aggregation
    auto exec_batch = full_batch.Slice(segment.offset, segment.length);
    RETURN_NOT_OK(DoConsume(ExecSpan(exec_batch), thread_index));
    RETURN_NOT_OK(
        ExtractSegmenterValues(&segmenter_values_, exec_batch, segment_field_ids_));

    // If the segment closes the current segment group, we can output segment group
    // aggregation.
    if (!segment.is_open) RETURN_NOT_OK(OutputResult(false));

    return Status::OK();
  };
  RETURN_NOT_OK(HandleSegments(segmenter_.get(), batch, segment_field_ids_, handler));

  if (input_counter_.Increment()) {
    RETURN_NOT_OK(OutputResult(/*is_last=*/true));
  }
  return Status::OK();
}

Status ScalarAggregateNode::InputFinished(ExecNode* input, int total_batches) {
  auto scope = TraceFinish();
  EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
  DCHECK_EQ(input, inputs_[0]);
  if (input_counter_.SetTotal(total_batches)) {
    RETURN_NOT_OK(OutputResult(/*is_last=*/true));
  }
  return Status::OK();
}

std::string ScalarAggregateNode::ToStringExtra(int indent) const {
  std::stringstream ss;
  const auto input_schema = inputs_[0]->output_schema();
  AggregatesToString(&ss, *input_schema, aggs_, target_fieldsets_);
  return ss.str();
}

Status ScalarAggregateNode::ResetKernelStates() {
  auto exec_ctx = plan()->query_context()->exec_context();
  for (size_t i = 0; i < kernels_.size(); ++i) {
    states_[i].resize(plan()->query_context()->max_concurrency());
    KernelContext kernel_ctx{exec_ctx};
    RETURN_NOT_OK(Kernel::InitAll(
        &kernel_ctx,
        KernelInitArgs{kernels_[i], kernel_intypes_[i], aggs_[i].options.get()},
        &states_[i]));
  }
  return Status::OK();
}

Status ScalarAggregateNode::OutputResult(bool is_last) {
  ExecBatch batch{{}, 1};
  batch.values.resize(kernels_.size() + segment_field_ids_.size());

  // First, insert segment keys
  PlaceFields(batch, /*base=*/0, segmenter_values_);

  // Followed by aggregate values
  std::size_t base = segment_field_ids_.size();
  for (size_t i = 0; i < kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Finalize"}});
    KernelContext ctx{plan()->query_context()->exec_context()};
    ARROW_ASSIGN_OR_RAISE(auto merged, ScalarAggregateKernel::MergeAll(
                                           kernels_[i], &ctx, std::move(states_[i])));
    RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &batch.values[base + i]));
  }

  ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
  total_output_batches_++;
  if (is_last) {
    ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_output_batches_));
  } else {
    ARROW_RETURN_NOT_OK(ResetKernelStates());
  }
  return Status::OK();
}

}  // namespace aggregate
}  // namespace acero
}  // namespace arrow

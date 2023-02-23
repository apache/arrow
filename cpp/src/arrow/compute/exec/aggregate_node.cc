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

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/query_context.h"
#include "arrow/compute/exec/util.h"
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

namespace compute {

namespace {

namespace {

std::vector<TypeHolder> ExtendWithGroupIdType(const std::vector<TypeHolder>& in_types) {
  std::vector<TypeHolder> aggr_in_types;
  aggr_in_types.reserve(in_types.size() + 1);
  aggr_in_types = in_types;
  aggr_in_types.emplace_back(uint32());
  return aggr_in_types;
}

Result<const HashAggregateKernel*> GetKernel(ExecContext* ctx, const Aggregate& aggregate,
                                             const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);
  ARROW_ASSIGN_OR_RAISE(auto function,
                        ctx->func_registry()->GetFunction(aggregate.function));
  if (function->kind() != Function::HASH_AGGREGATE) {
    if (function->kind() == Function::SCALAR_AGGREGATE) {
      return Status::Invalid("The provided function (", aggregate.function,
                             ") is a scalar aggregate function.  Since there are "
                             "keys to group by, a hash aggregate function was "
                             "expected (normally these start with hash_)");
    }
    return Status::Invalid("The provided function(", aggregate.function,
                           ") is not an aggregate function");
  }
  ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact(aggr_in_types));
  return static_cast<const HashAggregateKernel*>(kernel);
}

Result<std::unique_ptr<KernelState>> InitKernel(const HashAggregateKernel* kernel,
                                                ExecContext* ctx,
                                                const Aggregate& aggregate,
                                                const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);

  KernelContext kernel_ctx{ctx};
  const auto* options =
      arrow::internal::checked_cast<const FunctionOptions*>(aggregate.options.get());
  if (options == nullptr) {
    // use known default options for the named function if possible
    auto maybe_function = ctx->func_registry()->GetFunction(aggregate.function);
    if (maybe_function.ok()) {
      options = maybe_function.ValueOrDie()->default_options();
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      auto state,
      kernel->init(&kernel_ctx, KernelInitArgs{kernel, aggr_in_types, options}));
  return std::move(state);
}

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_types.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(kernels[i], GetKernel(ctx, aggregates[i], in_types[i]));
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(states[i],
                          InitKernel(kernels[i], ctx, aggregates[i], in_types[i]));
  }
  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<std::vector<TypeHolder>>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    const auto aggr_in_types = ExtendWithGroupIdType(types[i]);
    ARROW_ASSIGN_OR_RAISE(
        auto type, kernels[i]->signature->out_type().Resolve(&kernel_ctx, aggr_in_types));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

}  // namespace

void AggregatesToString(std::stringstream* ss, const Schema& input_schema,
                        const std::vector<Aggregate>& aggs,
                        const std::vector<std::vector<int>>& target_fieldsets,
                        int indent = 0) {
  *ss << "aggregates=[" << std::endl;
  for (size_t i = 0; i < aggs.size(); i++) {
    for (int j = 0; j < indent; ++j) *ss << "  ";
    *ss << '\t' << aggs[i].function << '(';
    const auto& target = target_fieldsets[i];
    if (target.size() == 0) {
      *ss << "*";
    } else {
      *ss << input_schema.field(target[0])->name();
      for (size_t k = 1; k < target.size(); k++) {
        *ss << ", " << input_schema.field(target[k])->name();
      }
    }
    if (aggs[i].options) {
      *ss << ", " << aggs[i].options->ToString();
    }
    *ss << ")," << std::endl;
  }
  for (int j = 0; j < indent; ++j) *ss << "  ";
  *ss << ']';
}

class ScalarAggregateNode : public ExecNode, public TracedNode {
 public:
  ScalarAggregateNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                      std::shared_ptr<Schema> output_schema,
                      std::vector<std::vector<int>> target_fieldsets,
                      std::vector<Aggregate> aggs,
                      std::vector<const ScalarAggregateKernel*> kernels,
                      std::vector<std::vector<std::unique_ptr<KernelState>>> states)
      : ExecNode(plan, std::move(inputs), {"target"},
                 /*output_schema=*/std::move(output_schema)),
        TracedNode(this),
        target_fieldsets_(std::move(target_fieldsets)),
        aggs_(std::move(aggs)),
        kernels_(std::move(kernels)),
        states_(std::move(states)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ScalarAggregateNode"));

    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    auto aggregates = aggregate_options.aggregates;

    const auto& input_schema = *inputs[0]->output_schema();
    auto exec_ctx = plan->query_context()->exec_context();

    std::vector<const ScalarAggregateKernel*> kernels(aggregates.size());
    std::vector<std::vector<std::unique_ptr<KernelState>>> states(kernels.size());
    FieldVector fields(kernels.size());
    std::vector<std::vector<int>> target_fieldsets(kernels.size());

    for (size_t i = 0; i < kernels.size(); ++i) {
      const auto& target_fieldset = aggregate_options.aggregates[i].target;
      for (const auto& target : target_fieldset) {
        ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(input_schema));
        target_fieldsets[i].push_back(match[0]);
      }

      ARROW_ASSIGN_OR_RAISE(
          auto function, exec_ctx->func_registry()->GetFunction(aggregates[i].function));

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
        in_types.emplace_back(input_schema.field(target)->type().get());
      }
      ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact(in_types));
      kernels[i] = static_cast<const ScalarAggregateKernel*>(kernel);

      if (aggregates[i].options == nullptr) {
        DCHECK(!function->doc().options_required);
        const auto* default_options = function->default_options();
        if (default_options) {
          aggregates[i].options = default_options->Copy();
        }
      }

      KernelContext kernel_ctx{exec_ctx};
      states[i].resize(plan->query_context()->max_concurrency());
      RETURN_NOT_OK(Kernel::InitAll(
          &kernel_ctx, KernelInitArgs{kernels[i], in_types, aggregates[i].options.get()},
          &states[i]));

      // pick one to resolve the kernel signature
      kernel_ctx.SetState(states[i][0].get());
      ARROW_ASSIGN_OR_RAISE(auto out_type, kernels[i]->signature->out_type().Resolve(
                                               &kernel_ctx, in_types));

      fields[i] = field(aggregate_options.aggregates[i].name, out_type.GetSharedPtr());
    }

    return plan->EmplaceNode<ScalarAggregateNode>(
        plan, std::move(inputs), schema(std::move(fields)), std::move(target_fieldsets),
        std::move(aggregates), std::move(kernels), std::move(states));
  }

  const char* kind_name() const override { return "ScalarAggregateNode"; }

  Status DoConsume(const ExecSpan& batch, size_t thread_index) {
    for (size_t i = 0; i < kernels_.size(); ++i) {
      util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Consume"}});
      KernelContext batch_ctx{plan()->query_context()->exec_context()};
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

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);

    auto thread_index = plan_->query_context()->GetThreadIndex();

    ARROW_RETURN_NOT_OK(DoConsume(ExecSpan(batch), thread_index));

    if (input_counter_.Increment()) {
      return Finish();
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    DCHECK_EQ(input, inputs_[0]);
    if (input_counter_.SetTotal(total_batches)) {
      return Finish();
    }
    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    // Scalar aggregates will only output a single batch
    return output_->InputFinished(this, 1);
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  Status StopProducingImpl() override { return Status::OK(); }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    const auto input_schema = inputs_[0]->output_schema();
    AggregatesToString(&ss, *input_schema, aggs_, target_fieldsets_);
    return ss.str();
  }

 private:
  Status Finish() {
    auto scope = TraceFinish();
    ExecBatch batch{{}, 1};
    batch.values.resize(kernels_.size());

    for (size_t i = 0; i < kernels_.size(); ++i) {
      util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Finalize"}});
      KernelContext ctx{plan()->query_context()->exec_context()};
      ARROW_ASSIGN_OR_RAISE(auto merged, ScalarAggregateKernel::MergeAll(
                                             kernels_[i], &ctx, std::move(states_[i])));
      RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &batch.values[i]));
    }

    return output_->InputReceived(this, std::move(batch));
  }

  const std::vector<std::vector<int>> target_fieldsets_;
  const std::vector<Aggregate> aggs_;
  const std::vector<const ScalarAggregateKernel*> kernels_;

  std::vector<std::vector<std::unique_ptr<KernelState>>> states_;

  AtomicCounter input_counter_;
};

class GroupByNode : public ExecNode, public TracedNode {
 public:
  GroupByNode(ExecNode* input, std::shared_ptr<Schema> output_schema,
              std::vector<int> key_field_ids,
              std::vector<std::vector<int>> agg_src_fieldsets,
              std::vector<Aggregate> aggs,
              std::vector<const HashAggregateKernel*> agg_kernels)
      : ExecNode(input->plan(), {input}, {"groupby"}, std::move(output_schema)),
        TracedNode(this),
        key_field_ids_(std::move(key_field_ids)),
        agg_src_fieldsets_(std::move(agg_src_fieldsets)),
        aggs_(std::move(aggs)),
        agg_kernels_(std::move(agg_kernels)) {}

  Status Init() override {
    output_task_group_id_ = plan_->query_context()->RegisterTaskGroup(
        [this](size_t, int64_t task_id) { return OutputNthBatch(task_id); },
        [](size_t) { return Status::OK(); });
    return Status::OK();
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "GroupByNode"));

    auto input = inputs[0];
    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    const auto& keys = aggregate_options.keys;
    // Copy (need to modify options pointer below)
    auto aggs = aggregate_options.aggregates;

    // Get input schema
    auto input_schema = input->output_schema();

    // Find input field indices for key fields
    std::vector<int> key_field_ids(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto match, keys[i].FindOne(*input_schema));
      key_field_ids[i] = match[0];
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

    auto ctx = plan->query_context()->exec_context();

    // Construct aggregates
    ARROW_ASSIGN_OR_RAISE(auto agg_kernels, GetKernels(ctx, aggs, agg_src_types));

    ARROW_ASSIGN_OR_RAISE(auto agg_states,
                          InitKernels(agg_kernels, ctx, aggs, agg_src_types));

    ARROW_ASSIGN_OR_RAISE(
        FieldVector agg_result_fields,
        ResolveKernels(aggs, agg_kernels, agg_states, ctx, agg_src_types));

    // Build field vector for output schema
    FieldVector output_fields{keys.size() + aggs.size()};

    // Aggregate fields come before key fields to match the behavior of GroupBy function
    for (size_t i = 0; i < aggs.size(); ++i) {
      output_fields[i] =
          agg_result_fields[i]->WithName(aggregate_options.aggregates[i].name);
    }
    size_t base = aggs.size();
    for (size_t i = 0; i < keys.size(); ++i) {
      int key_field_id = key_field_ids[i];
      output_fields[base + i] = input_schema->field(key_field_id);
    }

    return input->plan()->EmplaceNode<GroupByNode>(
        input, schema(std::move(output_fields)), std::move(key_field_ids),
        std::move(agg_src_fieldsets), std::move(aggs), std::move(agg_kernels));
  }

  const char* kind_name() const override { return "GroupByNode"; }

  Status Consume(ExecSpan batch) {
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
      util::tracing::Span span;
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

  Status Merge() {
    util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Merge",
                       {{"group_by", ToStringExtra()}, {"node.label", label()}});
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

      for (size_t i = 0; i < agg_kernels_.size(); ++i) {
        util::tracing::Span span;
        START_COMPUTE_SPAN(
            span, aggs_[i].function,
            {{"function.name", aggs_[i].function},
             {"function.options",
              aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
             {"function.kind", std::string(kind_name()) + "::Merge"}});

        auto ctx = plan_->query_context()->exec_context();
        KernelContext batch_ctx{ctx};
        DCHECK(state0->agg_states[i]);
        batch_ctx.SetState(state0->agg_states[i].get());

        RETURN_NOT_OK(agg_kernels_[i]->resize(&batch_ctx, state0->grouper->num_groups()));
        RETURN_NOT_OK(agg_kernels_[i]->merge(&batch_ctx, std::move(*state->agg_states[i]),
                                             *transposition.array()));
        state->agg_states[i].reset();
      }
    }
    return Status::OK();
  }

  Result<ExecBatch> Finalize() {
    util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Finalize",
                       {{"group_by", ToStringExtra()}, {"node.label", label()}});

    ThreadLocalState* state = &local_states_[0];
    // If we never got any batches, then state won't have been initialized
    RETURN_NOT_OK(InitLocalStateIfNeeded(state));

    ExecBatch out_data{{}, state->grouper->num_groups()};
    out_data.values.resize(agg_kernels_.size() + key_field_ids_.size());

    // Aggregate fields come before key fields to match the behavior of GroupBy function
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Finalize"}});
      KernelContext batch_ctx{plan_->query_context()->exec_context()};
      batch_ctx.SetState(state->agg_states[i].get());
      RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out_data.values[i]));
      state->agg_states[i].reset();
    }

    ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, state->grouper->GetUniques());
    std::move(out_keys.values.begin(), out_keys.values.end(),
              out_data.values.begin() + agg_kernels_.size());
    state->grouper.reset();
    return out_data;
  }

  Status OutputNthBatch(int64_t n) {
    int64_t batch_size = output_batch_size();
    return output_->InputReceived(this, out_data_.Slice(batch_size * n, batch_size));
  }

  Status OutputResult() {
    auto scope = TraceFinish();
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
    RETURN_NOT_OK(output_->InputFinished(this, static_cast<int>(num_output_batches)));
    return plan_->query_context()->StartTaskGroup(output_task_group_id_,
                                                  num_output_batches);
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);

    DCHECK_EQ(input, inputs_[0]);

    ARROW_RETURN_NOT_OK(Consume(ExecSpan(batch)));

    if (input_counter_.Increment()) {
      return OutputResult();
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);

    if (input_counter_.SetTotal(total_batches)) {
      return OutputResult();
    }
    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    local_states_.resize(plan_->query_context()->max_concurrency());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-16260)
    // Without spillover there is way to handle backpressure in this node
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-16260)
    // Without spillover there is way to handle backpressure in this node
  }

  Status StopProducingImpl() override { return Status::OK(); }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
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

 private:
  struct ThreadLocalState {
    std::unique_ptr<Grouper> grouper;
    std::vector<std::unique_ptr<KernelState>> agg_states;
  };

  ThreadLocalState* GetLocalState() {
    size_t thread_index = plan_->query_context()->GetThreadIndex();
    return &local_states_[thread_index];
  }

  Status InitLocalStateIfNeeded(ThreadLocalState* state) {
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
    ARROW_ASSIGN_OR_RAISE(
        state->grouper, Grouper::Make(key_types, plan_->query_context()->exec_context()));

    // Build vector of aggregate source field data types
    std::vector<std::vector<TypeHolder>> agg_src_types(agg_kernels_.size());
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      for (const auto& field_id : agg_src_fieldsets_[i]) {
        agg_src_types[i].emplace_back(input_schema->field(field_id)->type().get());
      }
    }

    ARROW_ASSIGN_OR_RAISE(
        state->agg_states,
        InitKernels(agg_kernels_, plan_->query_context()->exec_context(), aggs_,
                    agg_src_types));

    return Status::OK();
  }

  int output_batch_size() const {
    int result =
        static_cast<int>(plan_->query_context()->exec_context()->exec_chunksize());
    if (result < 0) {
      result = 32 * 1024;
    }
    return result;
  }

  int output_task_group_id_;

  const std::vector<int> key_field_ids_;
  const std::vector<std::vector<int>> agg_src_fieldsets_;
  const std::vector<Aggregate> aggs_;
  const std::vector<const HashAggregateKernel*> agg_kernels_;

  AtomicCounter input_counter_;

  std::vector<ThreadLocalState> local_states_;
  ExecBatch out_data_;
};

}  // namespace

namespace internal {

void RegisterAggregateNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(
      "aggregate",
      [](ExecPlan* plan, std::vector<ExecNode*> inputs,
         const ExecNodeOptions& options) -> Result<ExecNode*> {
        const auto& aggregate_options =
            checked_cast<const AggregateNodeOptions&>(options);

        if (aggregate_options.keys.empty()) {
          // construct scalar agg node
          return ScalarAggregateNode::Make(plan, std::move(inputs), options);
        }
        return GroupByNode::Make(plan, std::move(inputs), options);
      }));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

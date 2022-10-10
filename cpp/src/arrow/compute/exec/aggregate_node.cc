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
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/aggregate.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
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

/// \brief A gated shared mutex is similar to a shared mutex, in that it allows either
/// multiple shared readers or a unique writer access to the mutex, except that a waiting
/// writer gates future readers by preventing them from reacquiring shared access until it
/// has acquired and released the mutex. This is useful for ensuring a writer is never
/// starved by readers.
struct GatedSharedMutex {
  std::mutex gate;
  std::shared_mutex mutex;
};

/// \brief Acquires unique access to a gatex mutex. This is useful for a unique writer.
class GatedUniqueLock {
 public:
  // acquires the gate first, to ensure future readers will wait for its release
  explicit GatedUniqueLock(GatedSharedMutex& gated_shared_mutex)
      : lock_gate_(gated_shared_mutex.gate), lock_mutex_(gated_shared_mutex.mutex) {}

 private:
  std::unique_lock<std::mutex> lock_gate_;
  std::unique_lock<std::shared_mutex> lock_mutex_;
};

/// \brief Acquires shared access to a gatex mutex. This is useful for a shared reader.
class GatedSharedLock {
  struct TouchGate {
    explicit TouchGate(GatedSharedMutex& gated_shared_mutex) {
      std::unique_lock lock_gate(gated_shared_mutex.gate);
    }
  };

 public:
  // acquires and immediately releases the gate first, to ensure no writer is waiting
  explicit GatedSharedLock(GatedSharedMutex& gated_shared_mutex)
      : touch_gate_(gated_shared_mutex), lock_mutex_(gated_shared_mutex.mutex) {}

 private:
  TouchGate touch_gate_;
  std::shared_lock<std::shared_mutex> lock_mutex_;
};

void AggregatesToString(std::stringstream* ss, const Schema& input_schema,
                        const std::vector<Aggregate>& aggs,
                        const std::vector<int>& target_field_ids, int indent = 0) {
  *ss << "aggregates=[" << std::endl;
  for (size_t i = 0; i < aggs.size(); i++) {
    for (int j = 0; j < indent; ++j) *ss << "  ";
    *ss << '\t' << aggs[i].function << '('
        << input_schema.field(target_field_ids[i])->name();
    if (aggs[i].options) {
      *ss << ", " << aggs[i].options->ToString();
    }
    *ss << ")," << std::endl;
  }
  for (int j = 0; j < indent; ++j) *ss << "  ";
  *ss << ']';
}

ExecBatch SelectBatchValues(const ExecBatch& batch, const std::vector<int>& ids) {
  std::vector<Datum> values(ids.size());
  for (size_t i = 0; i < ids.size(); i++) {
    values[i] = batch.values[ids[i]];
  }
  return ExecBatch(std::move(values), batch.length);
}

template <typename BatchHandler>
Status HandleSegments(std::unique_ptr<GroupingSegmenter>& segmenter,
                      const ExecBatch& batch, const std::vector<int>& ids,
                      const BatchHandler& handle_batch) {
  int64_t offset = 0;
  ARROW_RETURN_NOT_OK(segmenter->Reset());
  auto segment_batch = SelectBatchValues(batch, ids);
  do {
    ARROW_ASSIGN_OR_RAISE(auto segment, segmenter->GetNextSegment(segment_batch, offset));
    if (segment.offset >= segment_batch.length) break;
    auto batch_slice = batch.Slice(segment.offset, segment.length);
    ARROW_RETURN_NOT_OK(handle_batch(ExecSpan(batch_slice), segment.is_open));
    offset = segment.offset + segment.length;
  } while (true);
  return Status::OK();
}

class ScalarAggregateNode : public ExecNode {
 public:
  ScalarAggregateNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                      std::shared_ptr<Schema> output_schema,
                      std::vector<int> target_field_ids,
                      std::vector<int> segment_field_ids,
                      std::unique_ptr<GroupingSegmenter> segmenter,
                      std::vector<Aggregate> aggs,
                      std::vector<const ScalarAggregateKernel*> kernels,
                      std::vector<std::vector<std::unique_ptr<KernelState>>> states)
      : ExecNode(plan, std::move(inputs), {"target"},
                 /*output_schema=*/std::move(output_schema),
                 /*num_outputs=*/1),
        target_field_ids_(std::move(target_field_ids)),
        segment_field_ids_(std::move(segment_field_ids)),
        segmenter_(std::move(segmenter)),
        aggs_(std::move(aggs)),
        kernels_(std::move(kernels)),
        states_(std::move(states)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ScalarAggregateNode"));

    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    auto aggregates = aggregate_options.aggregates;
    const auto& segment_keys = aggregate_options.segment_keys;

    const auto& input_schema = *inputs[0]->output_schema();
    auto exec_ctx = plan->exec_context();

    std::vector<int> segment_field_ids(segment_keys.size());
    std::vector<TypeHolder> segment_key_types(segment_keys.size());
    for (size_t i = 0; i < segment_keys.size(); i++) {
      ARROW_ASSIGN_OR_RAISE(auto match, segment_keys[i].FindOne(input_schema));
      segment_field_ids[i] = match[0];
      segment_key_types[i] = input_schema.field(match[0])->type().get();
    }

    ARROW_ASSIGN_OR_RAISE(
        auto segmenter, GroupingSegmenter::Make(std::move(segment_key_types), exec_ctx));

    std::vector<const ScalarAggregateKernel*> kernels(aggregates.size());
    std::vector<std::vector<std::unique_ptr<KernelState>>> states(kernels.size());
    FieldVector fields(kernels.size());
    std::vector<int> target_field_ids(kernels.size());

    for (size_t i = 0; i < kernels.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          auto match,
          FieldRef(aggregate_options.aggregates[i].target).FindOne(input_schema));
      target_field_ids[i] = match[0];

      ARROW_ASSIGN_OR_RAISE(
          auto function, exec_ctx->func_registry()->GetFunction(aggregates[i].function));

      if (function->kind() != Function::SCALAR_AGGREGATE) {
        return Status::Invalid("Provided non ScalarAggregateFunction ",
                               aggregates[i].function);
      }

      TypeHolder in_type(input_schema.field(target_field_ids[i])->type().get());
      ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact({in_type}));
      kernels[i] = static_cast<const ScalarAggregateKernel*>(kernel);

      if (aggregates[i].options == nullptr) {
        aggregates[i].options = function->default_options()->Copy();
      }

      KernelContext kernel_ctx{exec_ctx};
      states[i].resize(plan->max_concurrency());
      RETURN_NOT_OK(Kernel::InitAll(&kernel_ctx,
                                    KernelInitArgs{kernels[i],
                                                   {
                                                       in_type,
                                                   },
                                                   aggregates[i].options.get()},
                                    &states[i]));

      // pick one to resolve the kernel signature
      kernel_ctx.SetState(states[i][0].get());
      ARROW_ASSIGN_OR_RAISE(auto out_type, kernels[i]->signature->out_type().Resolve(
                                               &kernel_ctx, {in_type}));

      fields[i] = field(aggregate_options.aggregates[i].name, out_type.GetSharedPtr());
    }

    return plan->EmplaceNode<ScalarAggregateNode>(
        plan, std::move(inputs), schema(std::move(fields)), std::move(target_field_ids),
        std::move(segment_field_ids), std::move(segmenter), std::move(aggregates),
        std::move(kernels), std::move(states));
  }

  const char* kind_name() const override { return "ScalarAggregateNode"; }

  Status DoConsume(const ExecSpan& batch, size_t thread_index) {
    GatedSharedLock lock(gated_shared_mutex_);
    util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Consume",
                       {{"aggregate", ToStringExtra()},
                        {"node.label", label()},
                        {"batch.length", batch.length}});
    for (size_t i = 0; i < kernels_.size(); ++i) {
      util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Consume"}});
      KernelContext batch_ctx{plan()->exec_context()};
      batch_ctx.SetState(states_[i][thread_index].get());

      ExecSpan single_column_batch{{batch.values[target_field_ids_[i]]}, batch.length};
      RETURN_NOT_OK(kernels_[i]->consume(&batch_ctx, single_column_batch));
    }
    return Status::OK();
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(span, span_, "InputReceived",
                                   {{"aggregate", ToStringExtra()},
                                    {"node.label", label()},
                                    {"batch.length", batch.length}});

    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    DCHECK_EQ(input, inputs_[0]);

    if (ErrorIfNotOk(HandleSegments(segmenter_, batch, segment_field_ids_,
                                    [this](const ExecSpan& batch, bool is_open) {
                                      auto thread_index = plan_->GetThreadIndex();
                                      RETURN_NOT_OK(DoConsume(batch, thread_index));
                                      if (!is_open) {
                                        RETURN_NOT_OK(OutputResult(false));
                                      }
                                      return Status::OK();
                                    }))) {
      return;
    }

    if (input_counter_.Increment()) {
      ErrorIfNotOk(OutputResult(true));
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    EVENT(span_, "ErrorReceived", {{"error", error.message()}});
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    EVENT(span_, "InputFinished", {{"batches.length", total_batches}});
    DCHECK_EQ(input, inputs_[0]);
    if (input_counter_.SetTotal(total_batches)) {
      ErrorIfNotOk(OutputResult(true));
    }
  }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.label", label()},
                        {"node.detail", ToString()},
                        {"node.kind", kind_name()}});
    // Scalar aggregates will only output a single batch
    outputs_[0]->InputFinished(this, 1);
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    EVENT(span_, "StopProducing");
    if (input_counter_.Cancel()) {
      finished_.MarkFinished();
    }
    inputs_[0]->StopProducing(this);
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    const auto input_schema = inputs_[0]->output_schema();
    AggregatesToString(&ss, *input_schema, aggs_, target_field_ids_);
    return ss.str();
  }

 private:
  Status ReconstructAggregates() {
    const auto& input_schema = *inputs()[0]->output_schema();
    auto exec_ctx = plan()->exec_context();
    for (size_t i = 0; i < kernels_.size(); ++i) {
      TypeHolder in_type(input_schema.field(target_field_ids_[i])->type().get());
      KernelContext kernel_ctx{exec_ctx};
      RETURN_NOT_OK(Kernel::InitAll(&kernel_ctx,
                                    KernelInitArgs{kernels_[i],
                                                   {
                                                       in_type,
                                                   },
                                                   aggs_[i].options.get()},
                                    &states_[i]));
    }
    return Status::OK();
  }

  Status OutputResult(bool is_last = false) {
    GatedUniqueLock lock(gated_shared_mutex_);
    util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Finish",
                       {{"aggregate", ToStringExtra()}, {"node.label", label()}});
    ExecBatch batch{{}, 1};
    batch.values.resize(kernels_.size());

    for (size_t i = 0; i < kernels_.size(); ++i) {
      util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Finalize"}});
      KernelContext ctx{plan()->exec_context()};
      ARROW_ASSIGN_OR_RAISE(auto merged, ScalarAggregateKernel::MergeAll(
                                             kernels_[i], &ctx, std::move(states_[i])));
      RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &batch.values[i]));
    }

    outputs_[0]->InputReceived(this, std::move(batch));
    if (is_last) {
      finished_.MarkFinished();
    } else {
      ARROW_RETURN_NOT_OK(ReconstructAggregates());
    }
    return Status::OK();
  }

  const std::vector<int> target_field_ids_;
  const std::vector<int> segment_field_ids_;
  std::unique_ptr<GroupingSegmenter> segmenter_;
  const std::vector<Aggregate> aggs_;
  const std::vector<const ScalarAggregateKernel*> kernels_;

  std::vector<std::vector<std::unique_ptr<KernelState>>> states_;

  AtomicCounter input_counter_;
  GatedSharedMutex gated_shared_mutex_;
};

class GroupByNode : public ExecNode {
 public:
  GroupByNode(ExecNode* input, std::shared_ptr<Schema> output_schema, ExecContext* ctx,
              std::vector<int> key_field_ids, std::vector<int> segment_key_field_ids,
              std::unique_ptr<GroupingSegmenter> segmenter,
              std::vector<int> agg_src_field_ids, std::vector<TypeHolder> agg_src_types,
              std::vector<Aggregate> aggs,
              std::vector<const HashAggregateKernel*> agg_kernels)
      : ExecNode(input->plan(), {input}, {"groupby"}, std::move(output_schema),
                 /*num_outputs=*/1),
        ctx_(ctx),
        segmenter_(std::move(segmenter)),
        key_field_ids_(std::move(key_field_ids)),
        segment_key_field_ids_(std::move(segment_key_field_ids)),
        agg_src_field_ids_(std::move(agg_src_field_ids)),
        agg_src_types_(std::move(agg_src_types)),
        aggs_(std::move(aggs)),
        agg_kernels_(std::move(agg_kernels)) {}

  Status Init() override {
    output_task_group_id_ = plan_->RegisterTaskGroup(
        [this](size_t, int64_t task_id) {
          OutputNthBatch(task_id);
          return Status::OK();
        },
        [this](size_t) {
          finished_.MarkFinished();
          return Status::OK();
        });
    return Status::OK();
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "GroupByNode"));

    auto input = inputs[0];
    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    const auto& keys = aggregate_options.keys;
    const auto& segment_keys = aggregate_options.segment_keys;
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

    // Find input field indices for segment key fields
    std::vector<int> segment_key_field_ids(segment_keys.size());
    for (size_t i = 0; i < segment_keys.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto match, segment_keys[i].FindOne(*input_schema));
      segment_key_field_ids[i] = match[0];
    }

    // Find input field indices for aggregates
    std::vector<int> agg_src_field_ids(aggs.size());
    for (size_t i = 0; i < aggs.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto match, aggs[i].target.FindOne(*input_schema));
      agg_src_field_ids[i] = match[0];
    }

    // Build vector of aggregate source field data types
    std::vector<TypeHolder> agg_src_types(aggs.size());
    for (size_t i = 0; i < aggs.size(); ++i) {
      auto agg_src_field_id = agg_src_field_ids[i];
      agg_src_types[i] = input_schema->field(agg_src_field_id)->type().get();
    }

    // Build vector of segment key field data types
    std::vector<TypeHolder> segment_key_types(segment_keys.size());
    for (size_t i = 0; i < segment_keys.size(); ++i) {
      auto segment_key_field_id = segment_key_field_ids[i];
      segment_key_types[i] = input_schema->field(segment_key_field_id)->type().get();
    }

    auto ctx = input->plan()->exec_context();

    ARROW_ASSIGN_OR_RAISE(auto segmenter,
                          GroupingSegmenter::Make(std::move(segment_key_types), ctx));

    // Construct aggregates
    ARROW_ASSIGN_OR_RAISE(auto agg_kernels,
                          internal::GetKernels(ctx, aggs, agg_src_types));

    ARROW_ASSIGN_OR_RAISE(auto agg_states,
                          internal::InitKernels(agg_kernels, ctx, aggs, agg_src_types));

    ARROW_ASSIGN_OR_RAISE(
        FieldVector agg_result_fields,
        internal::ResolveKernels(aggs, agg_kernels, agg_states, ctx, agg_src_types));

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
    base += keys.size();
    for (size_t i = 0; i < segment_keys.size(); ++i) {
      int segment_key_field_id = segment_key_field_ids[i];
      output_fields[base + i] = input_schema->field(segment_key_field_id);
    }

    return input->plan()->EmplaceNode<GroupByNode>(
        input, schema(std::move(output_fields)), ctx, std::move(key_field_ids),
        std::move(segment_key_field_ids), std::move(segmenter),
        std::move(agg_src_field_ids), std::move(agg_src_types), std::move(aggs),
        std::move(agg_kernels));
  }

  Status ReconstructAggregates() {
    auto ctx = plan()->exec_context();

    ARROW_ASSIGN_OR_RAISE(agg_kernels_, internal::GetKernels(ctx, aggs_, agg_src_types_));

    ARROW_ASSIGN_OR_RAISE(
        auto agg_states, internal::InitKernels(agg_kernels_, ctx, aggs_, agg_src_types_));

    return Status::OK();
  }

  const char* kind_name() const override { return "GroupByNode"; }

  Status Consume(ExecSpan batch) {
    GatedSharedLock lock(gated_shared_mutex_);
    util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Consume",
                       {{"group_by", ToStringExtra()},
                        {"node.label", label()},
                        {"batch.length", batch.length}});
    size_t thread_index = plan_->GetThreadIndex();
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
      KernelContext kernel_ctx{ctx_};
      kernel_ctx.SetState(state->agg_states[i].get());

      ExecSpan agg_batch({batch[agg_src_field_ids_[i]], ExecValue(*id_batch.array())},
                         batch.length);
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
        KernelContext batch_ctx{ctx_};
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
      KernelContext batch_ctx{ctx_};
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

  void OutputNthBatch(int64_t n) {
    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    int64_t batch_size = output_batch_size();
    outputs_[0]->InputReceived(this, out_data_.Slice(batch_size * n, batch_size));
  }

  Status OutputResult(bool is_last = false) {
    GatedUniqueLock lock(gated_shared_mutex_);
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
    outputs_[0]->InputFinished(this, static_cast<int>(num_output_batches));
    if (is_last) {
      RETURN_NOT_OK(plan_->StartTaskGroup(output_task_group_id_, num_output_batches));
    } else {
      for (int64_t i = 0; i < num_output_batches; i++) {
        OutputNthBatch(i);
      }
      ARROW_RETURN_NOT_OK(ReconstructAggregates());
    }
    return Status::OK();
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(span, span_, "InputReceived",
                                   {{"group_by", ToStringExtra()},
                                    {"node.label", label()},
                                    {"batch.length", batch.length}});

    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    DCHECK_EQ(input, inputs_[0]);

    if (ErrorIfNotOk(HandleSegments(segmenter_, batch, segment_key_field_ids_,
                                    [this](const ExecSpan& batch, bool is_open) {
                                      RETURN_NOT_OK(Consume(batch));
                                      if (!is_open) {
                                        RETURN_NOT_OK(OutputResult(false));
                                      }
                                      return Status::OK();
                                    }))) {
      return;
    }

    if (input_counter_.Increment()) {
      ErrorIfNotOk(OutputResult(true));
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    EVENT(span_, "ErrorReceived", {{"error", error.message()}});

    DCHECK_EQ(input, inputs_[0]);

    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    EVENT(span_, "InputFinished", {{"batches.length", total_batches}});

    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    DCHECK_EQ(input, inputs_[0]);

    if (input_counter_.SetTotal(total_batches)) {
      ErrorIfNotOk(OutputResult(true));
    }
  }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.label", label()},
                        {"node.detail", ToString()},
                        {"node.kind", kind_name()}});

    local_states_.resize(plan_->max_concurrency());
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

  void StopProducing(ExecNode* output) override {
    EVENT(span_, "StopProducing");
    DCHECK_EQ(output, outputs_[0]);

    if (input_counter_.Cancel()) finished_.MarkFinished();
    inputs_[0]->StopProducing(this);
  }

  void StopProducing() override { StopProducing(outputs_[0]); }

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
    AggregatesToString(&ss, *input_schema, aggs_, agg_src_field_ids_, indent);
    return ss.str();
  }

 private:
  struct ThreadLocalState {
    std::unique_ptr<Grouper> grouper;
    std::vector<std::unique_ptr<KernelState>> agg_states;
  };

  ThreadLocalState* GetLocalState() {
    size_t thread_index = plan_->GetThreadIndex();
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
    ARROW_ASSIGN_OR_RAISE(state->grouper, Grouper::Make(key_types, ctx_));

    // Build vector of aggregate source field data types
    std::vector<TypeHolder> agg_src_types(agg_kernels_.size());
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      auto agg_src_field_id = agg_src_field_ids_[i];
      agg_src_types[i] = input_schema->field(agg_src_field_id)->type().get();
    }

    ARROW_ASSIGN_OR_RAISE(state->agg_states, internal::InitKernels(agg_kernels_, ctx_,
                                                                   aggs_, agg_src_types));

    return Status::OK();
  }

  int output_batch_size() const {
    int result = static_cast<int>(ctx_->exec_chunksize());
    if (result < 0) {
      result = 32 * 1024;
    }
    return result;
  }

  ExecContext* ctx_;
  int output_task_group_id_;
  std::unique_ptr<GroupingSegmenter> segmenter_;

  const std::vector<int> key_field_ids_;
  const std::vector<int> segment_key_field_ids_;
  const std::vector<int> agg_src_field_ids_;
  const std::vector<TypeHolder> agg_src_types_;
  const std::vector<Aggregate> aggs_;
  std::vector<const HashAggregateKernel*> agg_kernels_;

  AtomicCounter input_counter_;

  std::vector<ThreadLocalState> local_states_;
  ExecBatch out_data_;
  GatedSharedMutex gated_shared_mutex_;
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

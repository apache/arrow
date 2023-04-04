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

// This file implements both regular and segmented group-by aggregation, which is a
// generalization of ordered aggregation in which the key columns are not required to be
// ordered.
//
// In (regular) group-by aggregation, the input rows are partitioned into groups using a
// set of columns called keys, where in a given group each row has the same values for
// these columns. In segmented group-by aggregation, a second set of columns called
// segment-keys is used to refine the partitioning. However, segment-keys are different in
// that they partition only consecutive rows into a single group. Such a partition of
// consecutive rows is called a segment group. For example, consider a column X with
// values [A, A, B, A] at row-indices [0, 1, 2]. A regular group-by aggregation with keys
// [X] yields a row-index partitioning [[0, 1, 3], [2]] whereas a segmented-group-by
// aggregation with segment-keys [X] yields [[0, 1], [1], [3]].
//
// The implementation first segments the input using the segment-keys, then groups by the
// keys. When a segment group end is reached while scanning the input, output is pushed
// and the accumulating state is cleared. If no segment-keys are given, then the entire
// input is taken as one segment group. One batch per segment group is sent to output.

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

struct ARROW_ACERO_EXPORT AggregateNodeArgs {
  std::shared_ptr<Schema> output_schema;
  std::vector<int> grouping_key_field_ids;
  std::vector<int> segment_key_field_ids;
  std::unique_ptr<RowSegmenter> segmenter;
  std::vector<std::vector<int>> target_fieldsets;
  std::vector<Aggregate> aggregates;
  std::vector<const Kernel*> kernels;
  std::vector<std::vector<TypeHolder>> kernel_intypes;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states;
};

/// \brief Make the arguments of an aggregate node
///
/// \param[in] input_schema the schema of the input to the node
/// \param[in] keys the grouping keys for the aggregation
/// \param[in] segment_keys the segmenting keys for the aggregation
/// \param[in] aggregates the aggregates for the aggregation
/// \param[in] num_states_per_kernel number of states per kernel for the aggregation
/// \param[in] exec_ctx the execution context for the aggregation
ARROW_ACERO_EXPORT Result<AggregateNodeArgs> MakeAggregateNodeArgs(
    const Schema& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggregates,
    size_t num_states_per_kernel = 1, ExecContext* exec_ctx = default_exec_context());

}  // namespace aggregate

namespace {

std::vector<TypeHolder> ExtendWithGroupIdType(const std::vector<TypeHolder>& in_types) {
  std::vector<TypeHolder> aggr_in_types;
  aggr_in_types.reserve(in_types.size() + 1);
  aggr_in_types = in_types;
  aggr_in_types.emplace_back(uint32());
  return aggr_in_types;
}

void DefaultAggregateOptions(Aggregate* aggregate_ptr,
                             const std::shared_ptr<Function> function) {
  Aggregate& aggregate = *aggregate_ptr;
  if (aggregate.options == nullptr) {
    DCHECK(!function->doc().options_required);
    const auto* default_options = function->default_options();
    if (default_options) {
      aggregate.options = default_options->Copy();
    }
  }
}

using GetKernel = std::function<Result<const Kernel*>(ExecContext*, Aggregate*,
                                                      const std::vector<TypeHolder>&)>;

Result<const Kernel*> GetScalarAggregateKernel(ExecContext* ctx, Aggregate* aggregate_ptr,
                                               const std::vector<TypeHolder>& in_types) {
  Aggregate& aggregate = *aggregate_ptr;
  ARROW_ASSIGN_OR_RAISE(auto function,
                        ctx->func_registry()->GetFunction(aggregate.function));
  if (function->kind() != Function::SCALAR_AGGREGATE) {
    if (function->kind() == Function::HASH_AGGREGATE) {
      return Status::Invalid("The provided function (", aggregate.function,
                             ") is a hash aggregate function.  Since there are no "
                             "keys to group by, a scalar aggregate function was "
                             "expected (normally these do not start with hash_)");
    }
    return Status::Invalid("The provided function(", aggregate.function,
                           ") is not an aggregate function");
  }
  DefaultAggregateOptions(&aggregate, function);
  return function->DispatchExact(in_types);
}

Result<const Kernel*> GetHashAggregateKernel(ExecContext* ctx, Aggregate* aggregate_ptr,
                                             const std::vector<TypeHolder>& in_types) {
  Aggregate& aggregate = *aggregate_ptr;
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
  DefaultAggregateOptions(&aggregate, function);
  return function->DispatchExact(aggr_in_types);
}

using InitKernel = std::function<Result<std::unique_ptr<KernelState>>(
    const Kernel*, ExecContext*, const Aggregate&, const std::vector<TypeHolder>&)>;

Result<std::unique_ptr<KernelState>> InitScalarAggregateKernel(
    const Kernel* kernel, ExecContext* ctx, const Aggregate& aggregate,
    const std::vector<TypeHolder>& in_types) {
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
      auto state, kernel->init(&kernel_ctx, KernelInitArgs{kernel, in_types, options}));
  return std::move(state);
}

Result<std::unique_ptr<KernelState>> InitHashAggregateKernel(
    const Kernel* kernel, ExecContext* ctx, const Aggregate& aggregate,
    const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);
  return InitScalarAggregateKernel(kernel, ctx, aggregate, std::move(aggr_in_types));
}

Result<std::vector<const Kernel*>> GetKernels(
    GetKernel get_kernel, ExecContext* ctx, std::vector<Aggregate>* aggregates_ptr,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<Aggregate>& aggregates = *aggregates_ptr;
  if (aggregates.size() != in_types.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const Kernel*> kernels(in_types.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(kernels[i], get_kernel(ctx, &aggregates[i], in_types[i]));
  }
  return kernels;
}

template <typename KernelType>
Result<std::vector<std::vector<std::unique_ptr<KernelState>>>> InitKernels(
    InitKernel init_kernel, const std::vector<const KernelType*>& kernels,
    ExecContext* ctx, size_t num_states_per_kernel,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<std::vector<std::unique_ptr<KernelState>>> states(kernels.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    states[i].resize(num_states_per_kernel);
    for (size_t j = 0; j < num_states_per_kernel; j++) {
      ARROW_ASSIGN_OR_RAISE(states[i][j],
                            init_kernel(kernels[i], ctx, aggregates[i], in_types[i]));
    }
  }
  return std::move(states);
}

Result<std::shared_ptr<Field>> ResolveKernel(const Aggregate& aggregate,
                                             const Kernel* kernel,
                                             const std::unique_ptr<KernelState>& state,
                                             ExecContext* ctx,
                                             const std::vector<TypeHolder>& types) {
  KernelContext kernel_ctx{ctx};
  kernel_ctx.SetState(state.get());

  ARROW_ASSIGN_OR_RAISE(auto type,
                        kernel->signature->out_type().Resolve(&kernel_ctx, types));
  return field(aggregate.function, type.GetSharedPtr());
}

using ResolveKernels = std::function<Result<FieldVector>(
    const std::vector<Aggregate>&, const std::vector<const Kernel*>&,
    const std::vector<std::vector<std::unique_ptr<KernelState>>>&, ExecContext*,
    const std::vector<std::vector<TypeHolder>>&)>;

Result<FieldVector> ResolveScalarAggregateKernels(
    const std::vector<Aggregate>& aggregates, const std::vector<const Kernel*>& kernels,
    const std::vector<std::vector<std::unique_ptr<KernelState>>>& states,
    ExecContext* ctx, const std::vector<std::vector<TypeHolder>>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(
        fields[i], ResolveKernel(aggregates[i], kernels[i], states[i][0], ctx, types[i]));
  }
  return fields;
}

Result<FieldVector> ResolveHashAggregateKernels(
    const std::vector<Aggregate>& aggregates, const std::vector<const Kernel*>& kernels,
    const std::vector<std::vector<std::unique_ptr<KernelState>>>& states,
    ExecContext* ctx, const std::vector<std::vector<TypeHolder>>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    const auto aggr_in_types = ExtendWithGroupIdType(types[i]);
    ARROW_ASSIGN_OR_RAISE(fields[i], ResolveKernel(aggregates[i], kernels[i],
                                                   states[i][0], ctx, aggr_in_types));
  }
  return fields;
}

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

// Extract segments from a batch and run the given handler on them.  Note that the
// handle may be called on open segments which are not yet finished.  Typically a
// handler should accumulate those open segments until a closed segment is reached.
template <typename BatchHandler>
Status HandleSegments(RowSegmenter* segmenter, const ExecBatch& batch,
                      const std::vector<int>& ids, const BatchHandler& handle_batch) {
  int64_t offset = 0;
  ARROW_ASSIGN_OR_RAISE(auto segment_exec_batch, batch.SelectValues(ids));
  ExecSpan segment_batch(segment_exec_batch);

  while (true) {
    ARROW_ASSIGN_OR_RAISE(compute::Segment segment,
                          segmenter->GetNextSegment(segment_batch, offset));
    if (segment.offset >= segment_batch.length) break;  // condition of no-next-segment
    ARROW_RETURN_NOT_OK(handle_batch(batch, segment));
    offset = segment.offset + segment.length;
  }
  return Status::OK();
}

/// @brief Extract values of segment keys from a segment batch
/// @param[out] values_ptr Vector to store the extracted segment key values
/// @param[in] input_batch Segment batch. Must have the a constant value for segment key
/// @param[in] field_ids Segment key field ids
Status ExtractSegmenterValues(std::vector<Datum>* values_ptr,
                              const ExecBatch& input_batch,
                              const std::vector<int>& field_ids) {
  DCHECK_GT(input_batch.length, 0);
  std::vector<Datum>& values = *values_ptr;
  int64_t row = input_batch.length - 1;
  values.clear();
  values.resize(field_ids.size());
  for (size_t i = 0; i < field_ids.size(); i++) {
    const Datum& value = input_batch.values[field_ids[i]];
    if (value.is_scalar()) {
      values[i] = value;
    } else if (value.is_array()) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, value.make_array()->GetScalar(row));
      values[i] = scalar;
    } else {
      DCHECK(false);
    }
  }
  return Status::OK();
}

void PlaceFields(ExecBatch& batch, size_t base, std::vector<Datum>& values) {
  DCHECK_LE(base + values.size(), batch.values.size());
  for (size_t i = 0; i < values.size(); i++) {
    batch.values[base + i] = values[i];
  }
}

class ScalarAggregateNode : public ExecNode, public TracedNode {
 public:
  ScalarAggregateNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                      std::shared_ptr<Schema> output_schema,
                      std::unique_ptr<RowSegmenter> segmenter,
                      std::vector<int> segment_field_ids,
                      std::vector<std::vector<int>> target_fieldsets,
                      std::vector<Aggregate> aggs,
                      std::vector<const ScalarAggregateKernel*> kernels,
                      std::vector<std::vector<TypeHolder>> kernel_intypes,
                      std::vector<std::vector<std::unique_ptr<KernelState>>> states)
      : ExecNode(plan, std::move(inputs), {"target"},
                 /*output_schema=*/std::move(output_schema)),
        TracedNode(this),
        segmenter_(std::move(segmenter)),
        segment_field_ids_(std::move(segment_field_ids)),
        target_fieldsets_(std::move(target_fieldsets)),
        aggs_(std::move(aggs)),
        kernels_(std::move(kernels)),
        kernel_intypes_(std::move(kernel_intypes)),
        states_(std::move(states)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ScalarAggregateNode"));

    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    auto aggregates = aggregate_options.aggregates;
    const auto& keys = aggregate_options.keys;
    const auto& segment_keys = aggregate_options.segment_keys;

    if (keys.size() > 0) {
      return Status::Invalid("Scalar aggregation with some key");
    }
    if (plan->query_context()->exec_context()->executor()->GetCapacity() > 1 &&
        segment_keys.size() > 0) {
      return Status::NotImplemented("Segmented aggregation in a multi-threaded plan");
    }

    const auto& input_schema = *inputs[0]->output_schema();
    auto exec_ctx = plan->query_context()->exec_context();

    ARROW_ASSIGN_OR_RAISE(auto args,
                          aggregate::MakeAggregateNodeArgs(
                              input_schema, keys, segment_keys, aggregates,
                              plan->query_context()->max_concurrency(), exec_ctx));

    std::vector<const ScalarAggregateKernel*> kernels;
    kernels.reserve(args.kernels.size());
    for (auto kernel : args.kernels) {
      kernels.push_back(static_cast<const ScalarAggregateKernel*>(kernel));
    }
    return plan->EmplaceNode<ScalarAggregateNode>(
        plan, std::move(inputs), std::move(args.output_schema), std::move(args.segmenter),
        std::move(args.segment_key_field_ids), std::move(args.target_fieldsets),
        std::move(args.aggregates), std::move(kernels), std::move(args.kernel_intypes),
        std::move(args.states));
  }

  const char* kind_name() const override { return "ScalarAggregateNode"; }

  Status DoConsume(const ExecSpan& batch, size_t thread_index) {
    for (size_t i = 0; i < kernels_.size(); ++i) {
      arrow::util::tracing::Span span;
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

  Status InputFinished(ExecNode* input, int total_batches) override {
    auto scope = TraceFinish();
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    DCHECK_EQ(input, inputs_[0]);
    if (input_counter_.SetTotal(total_batches)) {
      RETURN_NOT_OK(OutputResult(/*is_last=*/true));
    }
    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
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
  Status ResetKernelStates() {
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

  Status OutputResult(bool is_last) {
    ExecBatch batch{{}, 1};
    batch.values.resize(kernels_.size() + segment_field_ids_.size());

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
      RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &batch.values[i]));
    }
    PlaceFields(batch, kernels_.size(), segmenter_values_);

    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
    total_output_batches_++;
    if (is_last) {
      ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_output_batches_));
    } else {
      ARROW_RETURN_NOT_OK(ResetKernelStates());
    }
    return Status::OK();
  }

  // A segmenter for the segment-keys
  std::unique_ptr<RowSegmenter> segmenter_;
  // Field indices corresponding to the segment-keys
  const std::vector<int> segment_field_ids_;
  // Holds the value of segment keys of the most recent input batch
  // The values are updated everytime an input batch is processed
  std::vector<Datum> segmenter_values_;

  const std::vector<std::vector<int>> target_fieldsets_;
  const std::vector<Aggregate> aggs_;
  const std::vector<const ScalarAggregateKernel*> kernels_;

  // Input type holders for each kernel, used for state initialization
  std::vector<std::vector<TypeHolder>> kernel_intypes_;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states_;

  AtomicCounter input_counter_;
  /// \brief Total number of output batches produced
  int total_output_batches_ = 0;
};

class GroupByNode : public ExecNode, public TracedNode {
 public:
  GroupByNode(ExecNode* input, std::shared_ptr<Schema> output_schema,
              std::vector<int> key_field_ids, std::vector<int> segment_key_field_ids,
              std::unique_ptr<RowSegmenter> segmenter,
              std::vector<std::vector<TypeHolder>> agg_src_types,
              std::vector<std::vector<int>> agg_src_fieldsets,
              std::vector<Aggregate> aggs,
              std::vector<const HashAggregateKernel*> agg_kernels)
      : ExecNode(input->plan(), {input}, {"groupby"}, std::move(output_schema)),
        TracedNode(this),
        segmenter_(std::move(segmenter)),
        key_field_ids_(std::move(key_field_ids)),
        segment_key_field_ids_(std::move(segment_key_field_ids)),
        agg_src_types_(std::move(agg_src_types)),
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

    const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
    auto aggregates = aggregate_options.aggregates;
    const auto& keys = aggregate_options.keys;
    const auto& segment_keys = aggregate_options.segment_keys;

    if (plan->query_context()->exec_context()->executor()->GetCapacity() > 1 &&
        segment_keys.size() > 0) {
      return Status::NotImplemented("Segmented aggregation in a multi-threaded plan");
    }

    const auto& input_schema = *inputs[0]->output_schema();
    auto exec_ctx = plan->query_context()->exec_context();

    ARROW_ASSIGN_OR_RAISE(auto args,
                          aggregate::MakeAggregateNodeArgs(
                              input_schema, keys, segment_keys, aggregates,
                              plan->query_context()->max_concurrency(), exec_ctx));

    std::vector<const HashAggregateKernel*> kernels;
    kernels.reserve(args.kernels.size());
    for (auto kernel : args.kernels) {
      kernels.push_back(static_cast<const HashAggregateKernel*>(kernel));
    }
    return inputs[0]->plan()->EmplaceNode<GroupByNode>(
        inputs[0], std::move(args.output_schema), std::move(args.grouping_key_field_ids),
        std::move(args.segment_key_field_ids), std::move(args.segmenter),
        std::move(args.kernel_intypes), std::move(args.target_fieldsets),
        std::move(args.aggregates), std::move(kernels));
  }

  Status ResetKernelStates() {
    auto ctx = plan()->query_context()->exec_context();
    ARROW_RETURN_NOT_OK(InitKernels(InitHashAggregateKernel, agg_kernels_, ctx,
                                    /*num_states_per_kernel=*/1, aggs_, agg_src_types_));
    return Status::OK();
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
      arrow::util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Consume"}});
      auto ctx = plan_->query_context()->exec_context();
      KernelContext kernel_ctx{ctx};
      kernel_ctx.SetState(state->agg_states[i][0].get());

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
    arrow::util::tracing::Span span;
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
        arrow::util::tracing::Span span;
        START_COMPUTE_SPAN(
            span, aggs_[i].function,
            {{"function.name", aggs_[i].function},
             {"function.options",
              aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
             {"function.kind", std::string(kind_name()) + "::Merge"}});

        auto ctx = plan_->query_context()->exec_context();
        KernelContext batch_ctx{ctx};
        DCHECK(state0->agg_states[i][0]);
        batch_ctx.SetState(state0->agg_states[i][0].get());

        RETURN_NOT_OK(agg_kernels_[i]->resize(&batch_ctx, state0->grouper->num_groups()));
        RETURN_NOT_OK(agg_kernels_[i]->merge(
            &batch_ctx, std::move(*state->agg_states[i][0]), *transposition.array()));
        state->agg_states[i][0].reset();
      }
    }
    return Status::OK();
  }

  Result<ExecBatch> Finalize() {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, "Finalize",
                       {{"group_by", ToStringExtra()}, {"node.label", label()}});

    ThreadLocalState* state = &local_states_[0];
    // If we never got any batches, then state won't have been initialized
    RETURN_NOT_OK(InitLocalStateIfNeeded(state));

    ExecBatch out_data{{}, state->grouper->num_groups()};
    out_data.values.resize(agg_kernels_.size() + key_field_ids_.size() +
                           segment_key_field_ids_.size());

    // Aggregate fields come before key fields to match the behavior of GroupBy function
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      arrow::util::tracing::Span span;
      START_COMPUTE_SPAN(span, aggs_[i].function,
                         {{"function.name", aggs_[i].function},
                          {"function.options",
                           aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                          {"function.kind", std::string(kind_name()) + "::Finalize"}});
      KernelContext batch_ctx{plan_->query_context()->exec_context()};
      batch_ctx.SetState(state->agg_states[i][0].get());
      RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out_data.values[i]));
      state->agg_states[i][0].reset();
    }

    ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, state->grouper->GetUniques());
    std::move(out_keys.values.begin(), out_keys.values.end(),
              out_data.values.begin() + agg_kernels_.size());
    PlaceFields(out_data, agg_kernels_.size() + key_field_ids_.size(), segmenter_values_);
    state->grouper.reset();
    return out_data;
  }

  Status OutputNthBatch(int64_t n) {
    int64_t batch_size = output_batch_size();
    return output_->InputReceived(this, out_data_.Slice(batch_size * n, batch_size));
  }

  Status OutputResult(bool is_last) {
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

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
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

  Status InputFinished(ExecNode* input, int total_batches) override {
    auto scope = TraceFinish();
    DCHECK_EQ(input, inputs_[0]);

    if (input_counter_.SetTotal(total_batches)) {
      RETURN_NOT_OK(OutputResult(/*is_last=*/true));
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
    // Without spillover there is no way to handle backpressure in this node
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-16260)
    // Without spillover there is no way to handle backpressure in this node
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
    std::vector<std::vector<std::unique_ptr<KernelState>>> agg_states;
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

    ARROW_ASSIGN_OR_RAISE(state->agg_states,
                          InitKernels(InitHashAggregateKernel, agg_kernels_,
                                      plan_->query_context()->exec_context(),
                                      /*num_states_per_kernel=*/1, aggs_, agg_src_types));

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
  /// \brief A segmenter for the segment-keys
  std::unique_ptr<RowSegmenter> segmenter_;
  /// \brief Holds values of the current batch that were selected for the segment-keys
  std::vector<Datum> segmenter_values_;

  const std::vector<int> key_field_ids_;
  /// \brief Field indices corresponding to the segment-keys
  const std::vector<int> segment_key_field_ids_;
  /// \brief Types of input fields per aggregate
  const std::vector<std::vector<TypeHolder>> agg_src_types_;
  const std::vector<std::vector<int>> agg_src_fieldsets_;
  const std::vector<Aggregate> aggs_;
  const std::vector<const HashAggregateKernel*> agg_kernels_;

  AtomicCounter input_counter_;
  /// \brief Total number of output batches produced
  int total_output_batches_ = 0;

  std::vector<ThreadLocalState> local_states_;
  ExecBatch out_data_;
};

}  // namespace

namespace aggregate {

Result<AggregateNodeArgs> MakeAggregateNodeArgs(const Schema& input_schema,
                                                const std::vector<FieldRef>& keys,
                                                const std::vector<FieldRef>& segment_keys,
                                                const std::vector<Aggregate>& aggs,
                                                size_t num_states_per_kernel,
                                                ExecContext* exec_ctx) {
  std::vector<Aggregate> aggregates(aggs);
  GetKernel get_kernel = keys.empty() ? GetScalarAggregateKernel : GetHashAggregateKernel;
  InitKernel init_kernel =
      keys.empty() ? InitScalarAggregateKernel : InitHashAggregateKernel;
  ResolveKernels resolve_kernels =
      keys.empty() ? ResolveScalarAggregateKernels : ResolveHashAggregateKernels;

  // Find input field indices for key fields
  std::vector<int> key_field_ids(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, keys[i].FindOne(input_schema));
    if (match.indices().size() > 1) {
      // ARROW-18369: Support nested references as segment ids
      return Status::Invalid("Nested references cannot be used as segment ids");
    }
    key_field_ids[i] = match[0];
  }

  std::vector<int> segment_field_ids(segment_keys.size());
  std::vector<TypeHolder> segment_key_types(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(FieldPath match, segment_keys[i].FindOne(input_schema));
    if (match.indices().size() > 1) {
      // ARROW-18369: Support nested references as segment ids
      return Status::Invalid("Nested references cannot be used as segment ids");
    }
    segment_field_ids[i] = match[0];
    segment_key_types[i] = input_schema.field(match[0])->type().get();
  }

  ARROW_ASSIGN_OR_RAISE(auto segmenter,
                        RowSegmenter::Make(std::move(segment_key_types),
                                           /*nullable_keys=*/false, exec_ctx));

  std::vector<std::vector<TypeHolder>> kernel_intypes(aggregates.size());
  FieldVector fields(aggregates.size() + keys.size() + segment_keys.size());
  std::vector<std::vector<int>> target_fieldsets(aggregates.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    const auto& target_fieldset = aggregates[i].target;
    for (const auto& target : target_fieldset) {
      ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(input_schema));
      target_fieldsets[i].push_back(match[0]);
    }

    std::vector<TypeHolder> in_types;
    for (const auto& target : target_fieldsets[i]) {
      in_types.emplace_back(input_schema.field(target)->type().get());
    }
    kernel_intypes[i] = in_types;
  }

  ARROW_ASSIGN_OR_RAISE(auto kernels,
                        GetKernels(get_kernel, exec_ctx, &aggregates, kernel_intypes));

  ARROW_ASSIGN_OR_RAISE(auto states,
                        InitKernels(init_kernel, kernels, exec_ctx, num_states_per_kernel,
                                    aggregates, kernel_intypes));
  ARROW_ASSIGN_OR_RAISE(auto resolved_fields, resolve_kernels(aggregates, kernels, states,
                                                              exec_ctx, kernel_intypes));

  for (size_t i = 0; i < aggregates.size(); ++i) {
    fields[i] = resolved_fields[i]->WithName(aggregates[i].name);
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(fields[kernels.size() + i], keys[i].GetOne(input_schema));
  }
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(fields[kernels.size() + keys.size() + i],
                          segment_keys[i].GetOne(input_schema));
  }

  return AggregateNodeArgs{schema(std::move(fields)),
                           std::move(key_field_ids),
                           std::move(segment_field_ids),
                           std::move(segmenter),
                           std::move(target_fieldsets),
                           std::move(aggregates),
                           std::move(kernels),
                           std::move(kernel_intypes),
                           std::move(states)};
}

Result<std::shared_ptr<Schema>> MakeOutputSchema(
    const Schema& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggregates,
    ExecContext* exec_ctx) {
  ARROW_ASSIGN_OR_RAISE(
      auto args, MakeAggregateNodeArgs(input_schema, keys, segment_keys, aggregates,
                                       /*num_states_per_kernel=*/1, exec_ctx));
  return std::move(args.output_schema);
}

}  // namespace aggregate

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
}  // namespace acero
}  // namespace arrow

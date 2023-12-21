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

// This API is EXPERIMENTAL.

#pragma once

#include <forward_list>
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
// values [A, A, B, A] at row-indices [0, 1, 2, 3]. A regular group-by aggregation with
// keys [X] yields a row-index partitioning [[0, 1, 3], [2]] whereas a segmented-group-by
// aggregation with segment-keys [X] yields [[0, 1], [2], [3]].
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

template <typename KernelType>
struct AggregateNodeArgs {
  std::shared_ptr<Schema> output_schema;
  std::vector<int> grouping_key_field_ids;
  std::vector<int> segment_key_field_ids;
  std::unique_ptr<RowSegmenter> segmenter;
  std::vector<std::vector<int>> target_fieldsets;
  std::vector<Aggregate> aggregates;
  std::vector<const KernelType*> kernels;
  std::vector<std::vector<TypeHolder>> kernel_intypes;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states;
};

std::vector<TypeHolder> ExtendWithGroupIdType(const std::vector<TypeHolder>& in_types);

Result<const HashAggregateKernel*> GetKernel(ExecContext* ctx, const Aggregate& aggregate,
                                             const std::vector<TypeHolder>& in_types);

Result<std::unique_ptr<KernelState>> InitKernel(const HashAggregateKernel* kernel,
                                                ExecContext* ctx,
                                                const Aggregate& aggregate,
                                                const std::vector<TypeHolder>& in_types);

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types);

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types);

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<std::vector<TypeHolder>>& types);

void AggregatesToString(std::stringstream* ss, const Schema& input_schema,
                        const std::vector<Aggregate>& aggs,
                        const std::vector<std::vector<int>>& target_fieldsets,
                        int indent = 0);

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
                              const std::vector<int>& field_ids);

Result<std::vector<Datum>> ExtractValues(const ExecBatch& input_batch,
                                         const std::vector<int>& field_ids);

void PlaceFields(ExecBatch& batch, size_t base, std::vector<Datum>& values);

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

  static Result<AggregateNodeArgs<ScalarAggregateKernel>> MakeAggregateNodeArgs(
      const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
      const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggs,
      ExecContext* exec_ctx, size_t concurrency, bool is_cpu_parallel);

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options);

  const char* kind_name() const override { return "ScalarAggregateNode"; }

  Status DoConsume(const ExecSpan& batch, size_t thread_index);

  Status InputReceived(ExecNode* input, ExecBatch batch) override;

  Status InputFinished(ExecNode* input, int total_batches) override;

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra(0));
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
  std::string ToStringExtra(int indent) const override;

 private:
  Status ResetKernelStates();

  Status OutputResult(bool is_last);

  // A segmenter for the segment-keys
  std::unique_ptr<RowSegmenter> segmenter_;
  // Field indices corresponding to the segment-keys
  const std::vector<int> segment_field_ids_;
  // Holds the value of segment keys of the most recent input batch
  // The values are updated every time an input batch is processed
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

  Status Init() override;

  static Result<AggregateNodeArgs<HashAggregateKernel>> MakeAggregateNodeArgs(
      const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
      const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggs,
      ExecContext* ctx, const bool is_cpu_parallel);

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options);

  Status ResetKernelStates();

  const char* kind_name() const override { return "GroupByNode"; }

  Status Consume(ExecSpan batch);

  Status Merge();

  Result<ExecBatch> Finalize();

  Status OutputNthBatch(int64_t n);

  Status OutputResult(bool is_last);

  Status InputReceived(ExecNode* input, ExecBatch batch) override;

  Status InputFinished(ExecNode* input, int total_batches) override;

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra(0));
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
  std::string ToStringExtra(int indent) const override;

 private:
  struct ThreadLocalState {
    std::unique_ptr<Grouper> grouper;
    std::vector<std::unique_ptr<KernelState>> agg_states;
  };

  ThreadLocalState* GetLocalState() {
    size_t thread_index = plan_->query_context()->GetThreadIndex();
    return &local_states_[thread_index];
  }

  Status InitLocalStateIfNeeded(ThreadLocalState* state);

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

}  // namespace aggregate
}  // namespace acero
}  // namespace arrow

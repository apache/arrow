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

#include <algorithm>
#include <atomic>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/exec_plan_internal.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/time_series_util.h"
#include "arrow/acero/util.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/util.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"

namespace {
std::vector<std::string> GetInputLabels(
    const arrow::acero::ExecNode::NodeVector& inputs) {
  std::vector<std::string> labels(inputs.size());
  for (size_t i = 0; i < inputs.size(); i++) {
    labels[i] = "input_" + std::to_string(i) + "_label";
  }
  return labels;
}

}  // namespace

namespace arrow::acero {

namespace {

using row_index_t = uint64_t;
using time_unit_t = uint64_t;
using col_index_t = int;
using Task = util::SequencingQueue::Task;

template <Type::type kTypeId>
Result<std::optional<time_unit_t>> ReadTimeValue(const Datum& value, int64_t row) {
  using ArrowType = typename TypeIdTraits<kTypeId>::Type;
  using CType = typename TypeTraits<ArrowType>::CType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  if (value.is_scalar()) {
    const auto& scalar =
        ::arrow::internal::checked_cast<const ScalarType&>(*value.scalar());
    if (!scalar.is_valid) {
      return std::nullopt;
    }
    return NormalizeTime(static_cast<CType>(scalar.value));
  }
  if (value.is_array()) {
    ArraySpan array(*value.array());
    if (array.IsNull(row)) {
      return std::nullopt;
    }
    return NormalizeTime(array.GetValues<CType>(1)[row]);
  }
  return Status::Invalid("SortedMerge sort key must be an array or scalar, but got ",
                         ::arrow::ToString(value.kind()));
}

Result<std::optional<time_unit_t>> ReadTimeValue(const Datum& value, int64_t row) {
  switch (value.type()->id()) {
#define SORTED_MERGE_TIME_CASE(ID) \
  case Type::ID:                   \
    return ReadTimeValue<Type::ID>(value, row)
    SORTED_MERGE_TIME_CASE(INT8);
    SORTED_MERGE_TIME_CASE(INT16);
    SORTED_MERGE_TIME_CASE(INT32);
    SORTED_MERGE_TIME_CASE(INT64);
    SORTED_MERGE_TIME_CASE(UINT8);
    SORTED_MERGE_TIME_CASE(UINT16);
    SORTED_MERGE_TIME_CASE(UINT32);
    SORTED_MERGE_TIME_CASE(UINT64);
    SORTED_MERGE_TIME_CASE(DATE32);
    SORTED_MERGE_TIME_CASE(DATE64);
    SORTED_MERGE_TIME_CASE(TIME32);
    SORTED_MERGE_TIME_CASE(TIME64);
    SORTED_MERGE_TIME_CASE(TIMESTAMP);
#undef SORTED_MERGE_TIME_CASE
    default:
      return Status::Invalid("Unsupported SortedMerge sort-key type ",
                             value.type()->ToString());
  }
}

bool TimeIsLater(const std::optional<time_unit_t>& left,
                 const std::optional<time_unit_t>& right,
                 compute::NullPlacement null_placement) {
  if (left && right) {
    return *left > *right;
  }
  if (!left && !right) {
    return false;
  }
  return null_placement == compute::NullPlacement::AtStart ? left.has_value()
                                                           : !left.has_value();
}

struct PreparedBatch {
  ExecBatch batch;
  std::vector<std::optional<time_unit_t>> times;
};

struct SelectionRun {
  std::shared_ptr<PreparedBatch> source;
  int64_t offset;
  int64_t length;
};

struct Selection {
  std::vector<SelectionRun> runs;
  int64_t length = 0;

  void Append(std::shared_ptr<PreparedBatch> source, int64_t offset, int64_t run_length) {
    runs.push_back({std::move(source), offset, run_length});
    length += run_length;
  }

  bool empty() const { return runs.empty(); }
};

struct FlowAction {
  enum class Kind { None, Pause, Resume };

  void Apply() const {
    if (kind == Kind::Pause) {
      input->PauseProducing(output, counter);
    } else if (kind == Kind::Resume) {
      input->ResumeProducing(output, counter);
    }
  }

  Kind kind = Kind::None;
  ExecNode* input = nullptr;
  ExecNode* output = nullptr;
  int32_t counter = 0;
};

class SortedMergeNode;

/// Sequences and buffers one sorted input.  Buffer contents and row position are
/// protected by SortedMergeNode's coordinator mutex; flow-control state uses its own
/// lock because it is touched before and after coordinator work.
class InputState final : public util::SerialSequencingQueue::Processor {
 public:
  InputState(SortedMergeNode* node, size_t index, ExecNode* input,
             col_index_t time_col_index, compute::NullPlacement null_placement);

  Status InsertBatch(ExecBatch batch);
  Status Process(ExecBatch batch) override;

  // The methods below are called only while the node's coordinator mutex is held.
  bool HasData() const { return !batches_.empty(); }
  bool AllBatchesReceived() const {
    return total_batches_ && received_batches_ == *total_batches_;
  }
  bool Finished() const { return AllBatchesReceived() && !HasData(); }

  const std::shared_ptr<PreparedBatch>& GetLatestBatch() const {
    DCHECK(HasData());
    return batches_.front();
  }

  const std::optional<time_unit_t>& GetLatestTime() const {
    return GetLatestBatch()->times[latest_ref_row_];
  }

  bool Advance(const std::optional<time_unit_t>* upper_bound, int64_t max_length,
               Selection* selection) {
    DCHECK(HasData());
    DCHECK_GT(max_length, 0);
    const row_index_t start = latest_ref_row_;
    std::shared_ptr<PreparedBatch> batch = batches_.front();
    const row_index_t rows_in_batch = static_cast<row_index_t>(batch->batch.length);
    const row_index_t limit =
        std::min(rows_in_batch, start + static_cast<row_index_t>(max_length));

    while (latest_ref_row_ < limit &&
           (upper_bound == nullptr ||
            !TimeIsLater(GetLatestTime(), *upper_bound, null_placement_))) {
      ++latest_ref_row_;
    }
    DCHECK_GT(latest_ref_row_, start);
    selection->Append(batch, static_cast<int64_t>(start),
                      static_cast<int64_t>(latest_ref_row_ - start));
    if (latest_ref_row_ >= rows_in_batch) {
      latest_ref_row_ = 0;
      batches_.pop_front();
      return true;
    }
    return false;
  }

  Status PushSequenced(std::shared_ptr<PreparedBatch> batch) {
    if (total_batches_ && received_batches_ >= *total_batches_) {
      return Status::Invalid("SortedMerge input ", index_,
                             " produced more batches than declared");
    }
    ++received_batches_;
    if (batch->batch.length > 0) {
      batches_.push_back(std::move(batch));
    }
    return Status::OK();
  }

  Status SetTotal(int total_batches) {
    if (total_batches < 0) {
      return Status::Invalid("SortedMerge input ", index_,
                             " reported a negative batch count");
    }
    if (total_batches_) {
      return *total_batches_ == total_batches
                 ? Status::OK()
                 : Status::Invalid("SortedMerge input ", index_,
                                   " changed its total batch count");
    }
    if (received_batches_ > total_batches) {
      return Status::Invalid("SortedMerge input ", index_,
                             " declared fewer batches than it produced");
    }
    total_batches_ = total_batches;
    return Status::OK();
  }

  void ClearBuffered() {
    batches_.clear();
    latest_ref_row_ = 0;
  }

  FlowAction BatchBuffered();
  FlowAction BatchConsumed();
  FlowAction Shutdown();

 private:
  FlowAction SetUpstreamPausedUnlocked(bool paused);
  Result<std::shared_ptr<PreparedBatch>> PrepareBatch(ExecBatch batch);
  Status ValidateTime(const std::optional<time_unit_t>& time);

  static constexpr size_t kLowWatermark = 4;
  static constexpr size_t kHighWatermark = 8;

  SortedMergeNode* node_;
  size_t index_;
  ExecNode* input_;
  col_index_t time_col_index_;
  compute::NullPlacement null_placement_;

  std::unique_ptr<util::SerialSequencingQueue> sequencer_;

  std::deque<std::shared_ptr<PreparedBatch>> batches_;
  int received_batches_ = 0;
  std::optional<int> total_batches_;
  row_index_t latest_ref_row_ = 0;

  std::optional<time_unit_t> last_time_;
  bool saw_null_ = false;
  bool saw_non_null_ = false;

  std::mutex flow_mutex_;
  size_t buffered_batches_ = 0;
  bool upstream_paused_ = false;
  int32_t outgoing_counter_ = 0;
  bool shutdown_ = false;
};

struct InputStateComparator {
  explicit InputStateComparator(compute::NullPlacement null_placement)
      : null_placement(null_placement) {}

  bool operator()(const InputState* lhs, const InputState* rhs) const {
    return TimeIsLater(lhs->GetLatestTime(), rhs->GetLatestTime(), null_placement);
  }

  compute::NullPlacement null_placement;
};

enum class MergeState { Idle, Running, Terminal };
enum class OutputGate { Open, Paused, Flushing };

class SortedMergeNode : public ExecNode {
  static constexpr int64_t kTargetOutputBatchSize = 1024 * 1024;

 public:
  SortedMergeNode(arrow::acero::ExecPlan* plan,
                  std::vector<arrow::acero::ExecNode*> inputs,
                  std::shared_ptr<arrow::Schema> output_schema,
                  arrow::Ordering new_ordering)
      : ExecNode(plan, inputs, GetInputLabels(inputs), std::move(output_schema)),
        ordering_(std::move(new_ordering)) {
    SetLabel("sorted_merge");
  }

  static arrow::Result<arrow::acero::ExecNode*> Make(
      arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs,
      const arrow::acero::ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, static_cast<int>(inputs.size()),
                                         "SortedMergeNode"));

    if (inputs.size() < 1) {
      return Status::Invalid("Constructing a `SortedMergeNode` with < 1 inputs");
    }

    const auto schema = inputs.at(0)->output_schema();
    for (const auto& input : inputs) {
      if (!input->output_schema()->Equals(schema)) {
        return Status::Invalid(
            "SortedMergeNode input schemas must all "
            "match, first schema "
            "was: ",
            schema->ToString(), " got schema: ", input->output_schema()->ToString());
      }
    }

    const auto& order_options =
        arrow::internal::checked_cast<const OrderByNodeOptions&>(options);

    if (order_options.ordering.is_implicit() || order_options.ordering.is_unordered()) {
      return Status::Invalid("`ordering` must be an explicit non-empty ordering");
    }

    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();
    return plan->EmplaceNode<SortedMergeNode>(
        plan, std::move(inputs), std::move(output_schema), order_options.ordering);
  }

  const char* kind_name() const override { return "SortedMergeNode"; }

  const arrow::Ordering& ordering() const override { return ordering_; }

  arrow::Status Init() override {
    if (ordering_.sort_keys().size() != 1) {
      return Status::NotImplemented("SortedMerge supports exactly one sort key");
    }

    const auto& sort_key = ordering_.sort_keys()[0];
    if (sort_key.order != arrow::compute::SortOrder::Ascending) {
      return Status::NotImplemented("Only ascending sort order is supported");
    }

    auto inputs = this->inputs();
    for (size_t i = 0; i < inputs.size(); i++) {
      ExecNode* input = inputs[i];
      const auto& schema = input->output_schema();

      const FieldRef& ref = sort_key.target;
      auto match_res = ref.FindOne(*schema);
      if (!match_res.ok()) {
        return Status::Invalid("Bad sort key : ", match_res.status().message());
      }
      ARROW_ASSIGN_OR_RAISE(auto match, match_res);
      ARROW_DCHECK(match.indices().size() == 1);

      state_.push_back(std::make_unique<InputState>(
          this, i, input, std::move(match.indices()[0]), sort_key.null_placement));
    }
    return Status::OK();
  }

  arrow::Status InputReceived(arrow::acero::ExecNode* input,
                              arrow::ExecBatch batch) override {
    if (terminal_.load()) {
      return Status::OK();
    }
    auto it = std::find(inputs_.begin(), inputs_.end(), input);
    if (it == inputs_.end()) {
      return Status::Invalid("SortedMerge received a batch from an unknown input");
    }
    const size_t index = static_cast<size_t>(it - inputs_.begin());
    return state_[index]->InsertBatch(std::move(batch));
  }

  arrow::Status InputFinished(arrow::acero::ExecNode* input, int total_batches) override {
    if (terminal_.load()) {
      return Status::OK();
    }
    auto it = std::find(inputs_.begin(), inputs_.end(), input);
    if (it == inputs_.end()) {
      return Status::Invalid("SortedMerge received completion from an unknown input");
    }
    const size_t index = static_cast<size_t>(it - inputs_.begin());
    std::optional<Task> task;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (merge_state_ == MergeState::Terminal) {
        return Status::OK();
      }
      ARROW_RETURN_NOT_OK(state_[index]->SetTotal(total_batches));
      task = MaybeStartUnlocked();
    }
    return task ? std::move(*task)() : Status::OK();
  }

  arrow::Status StartProducing() override { return Status::OK(); }

  arrow::Status StopProducingImpl() override {
    EnterTerminal();
    return Status::OK();
  }

  void PauseProducing(arrow::acero::ExecNode* output, int32_t counter) override {
    std::lock_guard lock(coordinator_mutex_);
    if (merge_state_ == MergeState::Terminal || counter <= downstream_counter_) {
      return;
    }
    downstream_counter_ = counter;
    if (output_gate_ != OutputGate::Flushing) {
      output_gate_ = OutputGate::Paused;
    }
  }

  void ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) override {
    std::optional<Task> task;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (merge_state_ == MergeState::Terminal || counter <= downstream_counter_) {
        return;
      }
      downstream_counter_ = counter;
      if (output_gate_ != OutputGate::Flushing) {
        output_gate_ = OutputGate::Open;
      }
      task = MaybeStartUnlocked();
    }
    if (task) {
      Schedule(std::move(*task), "SortedMergeNode::Resume");
    }
  }

  void Schedule(Task task, std::string_view name = "SortedMergeNode::Merge") {
    plan()->query_context()->ScheduleTask(std::move(task), name);
  }

  Result<std::optional<Task>> OnSequenced(size_t input_index,
                                          std::shared_ptr<PreparedBatch> batch) {
    std::lock_guard lock(coordinator_mutex_);
    if (merge_state_ == MergeState::Terminal) {
      return std::nullopt;
    }
    ARROW_RETURN_NOT_OK(state_[input_index]->PushSequenced(std::move(batch)));
    return MaybeStartUnlocked();
  }

  bool IsTerminal() const { return terminal_.load(); }

 protected:
  std::string ToStringExtra(int indent) const override {
    std::stringstream ss;
    ss << "ordering=" << ordering_.ToString();
    return ss.str();
  }

 private:
  void MaybeEnterFlushingUnlocked() {
    if (std::all_of(state_.begin(), state_.end(),
                    [](const auto& input) { return input->AllBatchesReceived(); })) {
      output_gate_ = OutputGate::Flushing;
    }
  }

  bool CanProgressUnlocked() const {
    return std::all_of(state_.begin(), state_.end(), [](const auto& input) {
      return input->HasData() || input->Finished();
    });
  }

  bool FinishedUnlocked() const {
    return std::all_of(state_.begin(), state_.end(),
                       [](const auto& input) { return input->Finished(); });
  }

  std::optional<Task> MaybeStartUnlocked() {
    MaybeEnterFlushingUnlocked();
    if (merge_state_ != MergeState::Idle || output_gate_ == OutputGate::Paused ||
        !CanProgressUnlocked()) {
      return std::nullopt;
    }
    merge_state_ = MergeState::Running;
    return Task([this] { return Drain(); });
  }

  Selection GetNextSelectionUnlocked(std::vector<InputState*>* consumed) {
    DCHECK(CanProgressUnlocked());
    Selection selection;
    std::vector<InputState*> heap;
    heap.reserve(state_.size());
    for (const auto& input : state_) {
      if (input->HasData()) {
        heap.push_back(input.get());
      }
    }
    if (heap.empty()) {
      return selection;
    }

    const auto comp = InputStateComparator(ordering_.sort_keys()[0].null_placement);
    std::make_heap(heap.begin(), heap.end(), comp);

    // Generate rows until we run out of data or reach the target output size.
    while (!heap.empty() && selection.length < kTargetOutputBatchSize) {
      std::pop_heap(heap.begin(), heap.end(), comp);

      auto& next_item = heap.back();
      // pop_heap leaves the remaining heap's earliest input at the front.  The selected
      // input can safely contribute every row up to that timestamp; no other input can
      // contain an earlier row.
      const std::optional<time_unit_t>* upper_bound =
          heap.size() > 1 ? &heap.front()->GetLatestTime() : nullptr;
      bool batch_consumed = next_item->Advance(
          upper_bound, kTargetOutputBatchSize - selection.length, &selection);
      if (batch_consumed) {
        consumed->push_back(next_item);
      }
      if (next_item->Finished()) {
        heap.pop_back();
        continue;
      }
      if (!next_item->HasData()) {
        // We've run out of data on one of the inputs
        break;
      }
      std::push_heap(heap.begin(), heap.end(), comp);
    }
    return selection;
  }

  Result<ExecBatch> Materialize(const Selection& selection) {
    std::vector<Datum> values;
    values.reserve(output_schema_->num_fields());
    for (int column = 0; column < output_schema_->num_fields(); ++column) {
      ARROW_ASSIGN_OR_RAISE(auto builder,
                            MakeBuilder(output_schema_->field(column)->type(),
                                        plan()->query_context()->memory_pool()));
      ARROW_RETURN_NOT_OK(builder->Reserve(selection.length));
      const ArrayData* current_source = nullptr;
      std::optional<ArraySpan> current_span;
      for (const SelectionRun& run : selection.runs) {
        const Datum& source = run.source->batch.values[column];
        if (source.is_scalar()) {
          ARROW_RETURN_NOT_OK(builder->AppendScalar(*source.scalar(), run.length));
          continue;
        }
        if (!source.is_array()) {
          return Status::Invalid("SortedMerge input must be an array or scalar, but got ",
                                 ::arrow::ToString(source.kind()), " in column ", column);
        }

        if (source.array().get() != current_source) {
          current_source = source.array().get();
          current_span.emplace(*source.array());
        }
        Status status = builder->AppendArraySlice(*current_span, run.offset, run.length);
        if (status.IsNotImplemented()) {
          auto source_array = MakeArray(source.array());
          for (int64_t row = run.offset; row < run.offset + run.length; ++row) {
            ARROW_ASSIGN_OR_RAISE(auto scalar, source_array->GetScalar(row));
            ARROW_RETURN_NOT_OK(builder->AppendScalar(*scalar));
          }
        } else {
          ARROW_RETURN_NOT_OK(status);
        }
      }
      ARROW_ASSIGN_OR_RAISE(auto array, builder->Finish());
      values.emplace_back(std::move(array));
    }
    return ExecBatch(std::move(values), selection.length);
  }

  Status Drain() {
    for (;;) {
      std::vector<InputState*> consumed;
      Selection selection;
      int32_t output_index = -1;
      bool finish = false;
      {
        std::lock_guard lock(coordinator_mutex_);
        if (merge_state_ == MergeState::Terminal) {
          return Status::OK();
        }
        DCHECK_EQ(merge_state_, MergeState::Running);
        MaybeEnterFlushingUnlocked();
        if (output_gate_ == OutputGate::Paused || !CanProgressUnlocked()) {
          merge_state_ = MergeState::Idle;
          return Status::OK();
        }

        selection = GetNextSelectionUnlocked(&consumed);
        if (!selection.empty()) {
          // The sole merge task owns output numbering, independent of executor mode.
          output_index = batches_produced_++;
        } else if (FinishedUnlocked()) {
          merge_state_ = MergeState::Terminal;
          terminal_.store(true);
          finish = true;
        } else {
          merge_state_ = MergeState::Idle;
        }
      }

      for (InputState* input : consumed) {
        input->BatchConsumed().Apply();
      }
      if (finish) {
        return FinishNormally();
      }
      if (selection.empty()) {
        return Status::OK();
      }

      auto materialized = Materialize(selection);
      if (!materialized.ok()) {
        EnterTerminal();
        return materialized.status();
      }
      ExecBatch output = std::move(*materialized);
      output.index = output_index;
      Status status = output_->InputReceived(this, std::move(output));
      if (!status.ok()) {
        EnterTerminal();
        return status;
      }
    }
  }

  Status FinishNormally() {
    for (auto& input : state_) {
      input->Shutdown().Apply();
    }
    return output_->InputFinished(this, batches_produced_);
  }

  void EnterTerminal() {
    terminal_.store(true);
    {
      std::lock_guard lock(coordinator_mutex_);
      merge_state_ = MergeState::Terminal;
      for (auto& input : state_) {
        input->ClearBuffered();
      }
    }
    for (auto& input : state_) {
      input->Shutdown().Apply();
    }
  }

  arrow::Ordering ordering_;
  std::vector<std::unique_ptr<InputState>> state_;

  std::mutex coordinator_mutex_;
  MergeState merge_state_ = MergeState::Idle;
  OutputGate output_gate_ = OutputGate::Open;
  int32_t downstream_counter_ = std::numeric_limits<int32_t>::min();
  int32_t batches_produced_ = 0;
  std::atomic<bool> terminal_{false};
};

InputState::InputState(SortedMergeNode* node, size_t index, ExecNode* input,
                       col_index_t time_col_index, compute::NullPlacement null_placement)
    : node_(node),
      index_(index),
      input_(input),
      time_col_index_(time_col_index),
      null_placement_(null_placement),
      sequencer_(util::SerialSequencingQueue::Make(this)) {}

Status InputState::InsertBatch(ExecBatch batch) {
  if (batch.index == compute::kUnsequencedIndex) {
    return Status::Invalid("SortedMerge requires sequenced input");
  }
  return sequencer_->InsertBatch(std::move(batch));
}

Status InputState::Process(ExecBatch batch) {
  if (node_->IsTerminal()) {
    return Status::OK();
  }
  // Sequence the original ExecBatch.  Once its index is current, prepare only the
  // sort-key view needed to select rows; payload columns stay unmaterialized until the
  // complete output selection is known.
  ARROW_ASSIGN_OR_RAISE(auto prepared, PrepareBatch(std::move(batch)));

  // Only sequenced, non-empty batches count toward backpressure.  Counting physical
  // arrivals can deadlock with a reordering input if later batches reach the high
  // watermark while that input still owns the batch which closes the sequencing gap.
  FlowAction buffered = prepared->batch.length == 0 ? FlowAction{} : BatchBuffered();
  ARROW_ASSIGN_OR_RAISE(auto task, node_->OnSequenced(index_, std::move(prepared)));
  buffered.Apply();
  return task ? std::move(*task)() : Status::OK();
}

Result<std::shared_ptr<PreparedBatch>> InputState::PrepareBatch(ExecBatch batch) {
  auto prepared = std::make_shared<PreparedBatch>();
  prepared->batch = std::move(batch);
  prepared->times.reserve(prepared->batch.length);
  const Datum& time_column = prepared->batch.values[time_col_index_];
  for (int64_t row = 0; row < prepared->batch.length; ++row) {
    ARROW_ASSIGN_OR_RAISE(auto time, ReadTimeValue(time_column, row));
    ARROW_RETURN_NOT_OK(ValidateTime(time));
    prepared->times.push_back(time);
  }
  return prepared;
}

Status InputState::ValidateTime(const std::optional<time_unit_t>& time) {
  if (!time) {
    if (null_placement_ == compute::NullPlacement::AtStart && saw_non_null_) {
      return Status::Invalid("SortedMerge input ", index_,
                             " has out-of-order nulls in its sort key");
    }
    saw_null_ = true;
    return Status::OK();
  }
  if ((null_placement_ == compute::NullPlacement::AtEnd && saw_null_) ||
      (last_time_ && *time < *last_time_)) {
    return Status::Invalid("SortedMerge input ", index_,
                           " has out-of-order values in its sort key");
  }
  saw_non_null_ = true;
  last_time_ = *time;
  return Status::OK();
}

FlowAction InputState::SetUpstreamPausedUnlocked(bool paused) {
  if (upstream_paused_ == paused || shutdown_) {
    return {};
  }
  upstream_paused_ = paused;
  return {paused ? FlowAction::Kind::Pause : FlowAction::Kind::Resume, input_, node_,
          ++outgoing_counter_};
}

FlowAction InputState::BatchBuffered() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  ++buffered_batches_;
  return buffered_batches_ >= kHighWatermark ? SetUpstreamPausedUnlocked(true)
                                             : FlowAction{};
}

FlowAction InputState::BatchConsumed() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  DCHECK_GT(buffered_batches_, 0);
  if (buffered_batches_ > 0) {
    --buffered_batches_;
  }
  return buffered_batches_ <= kLowWatermark ? SetUpstreamPausedUnlocked(false)
                                            : FlowAction{};
}

FlowAction InputState::Shutdown() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  shutdown_ = true;
  upstream_paused_ = false;
  buffered_batches_ = 0;
  // Always send a final, newer resume so a delayed pause cannot strand an input.
  return {FlowAction::Kind::Resume, input_, node_, ++outgoing_counter_};
}

}  // namespace

namespace internal {
void RegisterSortedMergeNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("sorted_merge", SortedMergeNode::Make));
}
}  // namespace internal

}  // namespace arrow::acero

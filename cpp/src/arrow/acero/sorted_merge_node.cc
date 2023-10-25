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

#include <arrow/api.h>
#include <atomic>
#include <mutex>
#include <sstream>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>
#include "arrow/acero/concurrent_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/time_series_util.h"
#include "arrow/acero/unmaterialized_table.h"
#include "arrow/acero/util.h"
#include "arrow/array/builder_base.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace {
template <typename Callable>
struct Defer {
  Callable callable;
  explicit Defer(Callable callable_) : callable(std::move(callable_)) {}
  ~Defer() noexcept { callable(); }
};

std::vector<std::string> GetInputLabels(
    const arrow::acero::ExecNode::NodeVector& inputs) {
  std::vector<std::string> labels(inputs.size());
  for (size_t i = 0; i < inputs.size(); i++) {
    labels[i] = "input_" + std::to_string(i) + "_label";
  }
  return labels;
}

template <typename T, typename V = typename T::value_type>
inline typename T::const_iterator std_find(const T& container, const V& val) {
  return std::find(container.begin(), container.end(), val);
}

template <typename T, typename V = typename T::value_type>
inline bool std_has(const T& container, const V& val) {
  return container.end() != std_find(container, val);
}

}  // namespace

namespace arrow::acero {

namespace sorted_merge {

// Each slice is associated with a single input source, so we only need 1 record
// batch per slice
using UnmaterializedSlice = arrow::acero::UnmaterializedSlice<1>;
using UnmaterializedCompositeTable = arrow::acero::UnmaterializedCompositeTable<1>;

using row_index_t = uint64_t;
using time_unit_t = uint64_t;
using col_index_t = int;

#define NEW_TASK true
#define POISON_PILL false

class BackpressureController : public BackpressureControl {
 public:
  BackpressureController(ExecNode* node, ExecNode* output,
                         std::atomic<int32_t>& backpressure_counter)
      : node_(node), output_(output), backpressure_counter_(backpressure_counter) {}

  void Pause() override { node_->PauseProducing(output_, ++backpressure_counter_); }
  void Resume() override { node_->ResumeProducing(output_, ++backpressure_counter_); }

 private:
  ExecNode* node_;
  ExecNode* output_;
  std::atomic<int32_t>& backpressure_counter_;
};

/// InputState correponds to an input. Input record batches are queued up in InputState
/// until processed and turned into output record batches.
class InputState {
 public:
  InputState(size_t index, BackpressureHandler handler,
             const std::shared_ptr<arrow::Schema>& schema, const int time_col_index)
      : index_(index),
        queue_(std::move(handler)),
        schema_(schema),
        time_col_index_(time_col_index),
        time_type_id_(schema_->fields()[time_col_index_]->type()->id()) {}

  template <typename PtrType>
  static arrow::Result<PtrType> Make(size_t index, arrow::acero::ExecNode* input,
                                     arrow::acero::ExecNode* output,
                                     std::atomic<int32_t>& backpressure_counter,
                                     const std::shared_ptr<arrow::Schema>& schema,
                                     const col_index_t time_col_index) {
    constexpr size_t low_threshold = 4, high_threshold = 8;
    std::unique_ptr<arrow::acero::BackpressureControl> backpressure_control =
        std::make_unique<BackpressureController>(input, output, backpressure_counter);
    ARROW_ASSIGN_OR_RAISE(auto handler,
                          BackpressureHandler::Make(input, low_threshold, high_threshold,
                                                    std::move(backpressure_control)));
    return PtrType(new InputState(index, std::move(handler), schema, time_col_index));
  }

  bool IsTimeColumn(col_index_t i) const {
    DCHECK_LT(i, schema_->num_fields());
    return (i == time_col_index_);
  }

  // Gets the latest row index, assuming the queue isn't empty
  row_index_t GetLatestRow() const { return latest_ref_row_; }

  bool Empty() const {
    // cannot be empty if ref row is >0 -- can avoid slow queue lock
    // below
    if (latest_ref_row_ > 0) {
      return false;
    }
    return queue_.Empty();
  }

  size_t index() const { return index_; }

  int total_batches() const { return total_batches_; }

  // Gets latest batch (precondition: must not be empty)
  const std::shared_ptr<arrow::RecordBatch>& GetLatestBatch() const {
    return queue_.UnsyncFront();
  }

#define LATEST_VAL_CASE(id, val)                                   \
  case arrow::Type::id: {                                          \
    using T = typename arrow::TypeIdTraits<arrow::Type::id>::Type; \
    using CType = typename arrow::TypeTraits<T>::CType;            \
    return val(data->GetValues<CType>(1)[row]);                    \
  }

  inline time_unit_t GetLatestTime() const {
    return GetTime(GetLatestBatch().get(), time_type_id_, time_col_index_,
                   latest_ref_row_);
  }

#undef LATEST_VAL_CASE

  bool Finished() const { return batches_processed_ == total_batches_; }

  arrow::Result<std::pair<UnmaterializedSlice, std::shared_ptr<arrow::RecordBatch>>>
  Advance() {
    // Advance the row until a new time is encountered or the record batch
    // ends. This will return a range of {-1, -1} and a nullptr if there is
    // no input

    bool active =
        (latest_ref_row_ > 0 /*short circuit the lock on the queue*/) || !queue_.Empty();

    if (!active) {
      return std::make_pair(UnmaterializedSlice(), nullptr);
    }

    row_index_t start = latest_ref_row_;
    row_index_t end = latest_ref_row_;
    time_unit_t startTime = GetLatestTime();
    std::shared_ptr<arrow::RecordBatch> batch = queue_.UnsyncFront();
    auto rows_in_batch = (row_index_t)batch->num_rows();

    while (GetLatestTime() == startTime) {
      end = ++latest_ref_row_;
      if (latest_ref_row_ >= rows_in_batch) {
        // hit the end of the batch, need to get the next batch if
        // possible.
        ++batches_processed_;
        latest_ref_row_ = 0;
        active &= !queue_.TryPop();
        if (active) {
          DCHECK_GT(queue_.UnsyncFront()->num_rows(),
                    0);  // empty batches disallowed, sanity check
        }
        break;
      }
    }

    UnmaterializedSlice slice;
    slice.num_components = 1;
    slice.components[0] = CompositeEntry{batch.get(), start, end};
    return std::make_pair(slice, batch);
  }

  arrow::Status Push(const std::shared_ptr<arrow::RecordBatch>& rb) {
    if (rb->num_rows() > 0) {
      queue_.Push(rb);
    } else {
      ++batches_processed_;  // don't enqueue empty batches, just record
                             // as processed
    }
    return arrow::Status::OK();
  }

  const std::shared_ptr<arrow::Schema>& get_schema() const { return schema_; }

  void set_total_batches(int n) {
    DCHECK_GE(n, 0);
    DCHECK_EQ(total_batches_, -1) << "Set total batch more than once";
    total_batches_ = n;
  }

 private:
  size_t index_;
  // Pending record batches. The latest is the front. Batches cannot be empty.
  BackpressureConcurrentQueue<std::shared_ptr<arrow::RecordBatch>> queue_;
  // Schema associated with the input
  std::shared_ptr<arrow::Schema> schema_;
  // Total number of batches (only int because InputFinished uses int)
  std::atomic<int> total_batches_{-1};
  // Number of batches processed so far (only int because InputFinished uses
  // int)
  std::atomic<int> batches_processed_{0};
  // Index of the time col
  col_index_t time_col_index_;
  // Type id of the time column
  arrow::Type::type time_type_id_;
  // Index of the latest row reference within; if >0 then queue_ cannot be
  // empty Must be < queue_.front()->num_rows() if queue_ is non-empty
  row_index_t latest_ref_row_ = 0;
  // Time of latest row
  time_unit_t latest_time_ = std::numeric_limits<time_unit_t>::lowest();
};

struct InputStateComparator {
  bool operator()(const std::shared_ptr<InputState>& lhs,
                  const std::shared_ptr<InputState>& rhs) const {
    // True if lhs is ahead of time of rhs
    if (lhs->Finished()) {
      return false;
    }
    if (rhs->Finished()) {
      return false;
    }
    time_unit_t lFirst = lhs->GetLatestTime();
    time_unit_t rFirst = rhs->GetLatestTime();
    return lFirst > rFirst;
  }
};

class SortedMergeNode : public ExecNode {
  static constexpr int64_t kTargetOutputBatchSize = 1024 * 1024;

 public:
  SortedMergeNode(arrow::acero::ExecPlan* plan,
                  std::vector<arrow::acero::ExecNode*> inputs,
                  std::shared_ptr<arrow::Schema> output_schema,
                  arrow::Ordering new_ordering)
      : ExecNode(plan, inputs, GetInputLabels(inputs), std::move(output_schema)),
        ordering_(std::move(new_ordering)),
        input_counter(inputs_.size()),
        output_counter(inputs_.size()),
        process_thread() {
    SetLabel("sorted_merge");
  }

  ~SortedMergeNode() override {
    process_queue.Push(
        POISON_PILL);  // poison pill
                       // We might create a temporary (such as to inspect the output
                       // schema), in which case there isn't anything  to join
    if (process_thread.joinable()) {
      process_thread.join();
    }
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
    auto inputs = this->inputs();
    for (size_t i = 0; i < inputs.size(); i++) {
      ExecNode* input = inputs[i];
      const auto& schema = input->output_schema();
      const auto& sort_key = ordering_.sort_keys()[0];
      if (sort_key.order != arrow::compute::SortOrder::Ascending) {
        return Status::Invalid("Only ascending sort order is supported");
      }

      const auto& ref = sort_key.target;
      if (!ref.IsName()) {
        return Status::Invalid("Ordering must be a name. ", ref.ToString(),
                               " is not a name");
      }

      ARROW_ASSIGN_OR_RAISE(auto input_state,
                            InputState::Make<std::shared_ptr<InputState>>(
                                i, input, this, backpressure_counter, schema,
                                schema->GetFieldIndex(*ref.name())));
      state.push_back(std::move(input_state));
    }
    return Status::OK();
  }

  arrow::Status InputReceived(arrow::acero::ExecNode* input,
                              arrow::ExecBatch batch) override {
    ARROW_DCHECK(std_has(inputs_, input));
    const size_t index = std_find(inputs_, input) - inputs_.begin();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> rb,
                          batch.ToRecordBatch(output_schema_));

    // Push into the queue. Note that we don't need to lock since
    // InputState's ConcurrentQueue manages locking
    input_counter[index] += rb->num_rows();
    ARROW_RETURN_NOT_OK(state[index]->Push(rb));
    process_queue.Push(NEW_TASK);
    return Status::OK();
  }

  arrow::Status InputFinished(arrow::acero::ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std_has(inputs_, input));
    {
      std::lock_guard<std::mutex> guard(gate);
      ARROW_DCHECK(std_has(inputs_, input));
      size_t k = std_find(inputs_, input) - inputs_.begin();
      state.at(k)->set_total_batches(total_batches);
    }
    // Trigger a final process call for stragglers
    process_queue.Push(NEW_TASK);
    return Status::OK();
  }

  arrow::Status StartProducing() override {
    ARROW_ASSIGN_OR_RAISE(process_task, plan_->query_context()->BeginExternalTask(
                                            "SortedMergeNode::ProcessThread"));
    if (!process_task.is_valid()) {
      // Plan has already aborted.  Do not start process thread
      return Status::OK();
    }
    process_thread = std::thread(&SortedMergeNode::StartPoller, this);
    return Status::OK();
  }

  arrow::Status StopProducingImpl() override {
    process_queue.Clear();
    process_queue.Push(POISON_PILL);
    return Status::OK();
  }

  // handled by the backpressure controller
  void PauseProducing(arrow::acero::ExecNode* output, int32_t counter) override {}
  void ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) override {}

 protected:
  std::string ToStringExtra(int indent) const override {
    std::stringstream ss;
    ss << "ordering=" << ordering_.ToString();
    return ss.str();
  }

 private:
  void EndFromProcessThread(arrow::Status st = arrow::Status::OK()) {
    ARROW_CHECK(!cleanup_started);
    for (size_t i = 0; i < input_counter.size(); ++i) {
      ARROW_CHECK(input_counter[i] == output_counter[i])
          << input_counter[i] << " != " << output_counter[i];
    }

    ARROW_UNUSED(
        plan_->query_context()->executor()->Spawn([this, st = std::move(st)]() mutable {
          Defer cleanup([this, &st]() { process_task.MarkFinished(st); });
          if (st.ok()) {
            st = output_->InputFinished(this, batches_produced);
          }
        }));
  }

  bool CheckEnded() {
    bool all_finished = true;
    for (const auto& s : state) {
      all_finished &= s->Finished();
    }
    if (all_finished) {
      EndFromProcessThread();
      return false;
    }
    return true;
  }

  /// Streams the input states in sorted order until we run out of input
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> getNextBatch() {
    DCHECK(!state.empty());
    for (const auto& s : state) {
      if (s->Empty() && !s->Finished()) {
        return nullptr;  // not enough data, wait
      }
    }

    std::vector<std::shared_ptr<InputState>> heap = state;
    // filter out empty states
    heap.erase(std::remove_if(
                   heap.begin(), heap.end(),
                   [](const std::shared_ptr<InputState>& s) { return s->Finished(); }),
               heap.end());
    // Currently we only support one sort key
    const auto sort_col = *ordering_.sort_keys().at(0).target.name();
    const auto comp = InputStateComparator();
    std::make_heap(heap.begin(), heap.end(), comp);

    // Each slice only has one record batch with the same schema as the output
    std::unordered_map<int, std::pair<int, int>> output_col_to_src;
    for (int i = 0; i < output_schema_->num_fields(); i++) {
      output_col_to_src[i] = std::make_pair(0, i);
    }
    UnmaterializedCompositeTable output(output_schema(), 1, std::move(output_col_to_src),
                                        plan()->query_context()->memory_pool());

    // Generate rows until we run out of data or we exceed the target output
    // size
    while (!heap.empty() && output.Size() < kTargetOutputBatchSize) {
      std::pop_heap(heap.begin(), heap.end(), comp);

      auto& next_item = heap.back();
      time_unit_t latest_time = std::numeric_limits<time_unit_t>::min();
      time_unit_t new_time = next_item->GetLatestTime();
      ARROW_CHECK(new_time >= latest_time)
          << "Input state " << next_item->index()
          << " has out of order data. newTime=" << new_time
          << " latestTime=" << latest_time;

      latest_time = new_time;
      ARROW_ASSIGN_OR_RAISE(auto slice_rb, next_item->Advance());
      UnmaterializedSlice& slice = slice_rb.first;
      std::shared_ptr<arrow::RecordBatch>& rb = slice_rb.second;

      if (slice.Size() > 0) {
        output_counter[next_item->index()] += slice.Size();
        output.AddSlice(slice);
        output.AddRecordBatchRef(rb);
      }

      if (next_item->Finished() || next_item->Empty()) {
        heap.pop_back();
      }
      std::make_heap(heap.begin(), heap.end(), comp);
    }

    // Emit the batch
    if (output.Size() == 0) {
      return nullptr;
    }

    auto result = output.Materialize();
    return result;
  }
  /// Gets a batch. Returns true if there is more data to process, false if we
  /// are done or an error occurred
  bool PollOnce() {
    std::lock_guard<std::mutex> guard(gate);
    if (!CheckEnded()) {
      return false;
    }

    // Process batches while we have data
    for (;;) {
      Result<std::shared_ptr<RecordBatch>> result = getNextBatch();

      if (result.ok()) {
        auto out_rb = *result;
        if (!out_rb) {
          break;
        }
        ExecBatch out_b(*out_rb);
        out_b.index = batches_produced++;
        Status st = output_->InputReceived(this, std::move(out_b));
        if (!st.ok()) {
          ARROW_LOG(FATAL) << "Error in output_::InputReceived: " << st.ToString();
          EndFromProcessThread(std::move(st));
        }
      } else {
        EndFromProcessThread(result.status());
        return false;
      }
    }

    // Report to the output the total batch count, if we've already
    // finished everything (there are two places where this can happen:
    // here and InputFinished)
    //
    // It may happen here in cases where InputFinished was called before
    // we were finished producing results (so we didn't know the output
    // size at that time)
    if (!CheckEnded()) {
      return false;
    }

    // There is no more we can do now but there is still work remaining
    // for later when more data arrives.
    return true;
  }

  void EmitBatches() {
    while (true) {
      // Implementation note: If the queue is empty, we will block here
      if (process_queue.Pop() == POISON_PILL) {
        EndFromProcessThread();
      }
      // Either we're out of data or something went wrong
      if (!PollOnce()) {
        return;
      }
    }
  }

  /// The entry point for processThread
  static void StartPoller(SortedMergeNode* node) { node->EmitBatches(); }

  arrow::Ordering ordering_;

  // Each input state corresponds to an input (e.g. a parquet data file)
  std::vector<std::shared_ptr<InputState>> state;
  std::vector<std::atomic_long> input_counter;
  std::vector<std::atomic_long> output_counter;
  std::mutex gate;

  std::atomic<bool> cleanup_started{false};

  // Backpressure counter common to all input states
  std::atomic<int32_t> backpressure_counter;

  std::atomic<int32_t> batches_produced{0};

  // Queue to trigger processing of a given input. False acts as a poison pill
  ConcurrentQueue<bool> process_queue;
  // Once StartProducing is called, we initialize this thread to poll the
  // input states and emit batches
  std::thread process_thread;
  arrow::Future<> process_task;

  // Map arg index --> completion counter
  std::vector<arrow::acero::AtomicCounter> counter_;
  // Map arg index --> data
  std::vector<InputState> accumulation_queue_;
  std::mutex mutex_;
  std::atomic<int> total_batches_{0};
};

#undef NEW_TASK
#undef POISON_PILL

}  // namespace sorted_merge

namespace internal {
void RegisterSortedMergeNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("sorted_merge", sorted_merge::SortedMergeNode::Make));
}
}  // namespace internal

}  // namespace arrow::acero

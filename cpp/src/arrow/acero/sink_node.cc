
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

#include <atomic>
#include <mutex>
#include <optional>
#include <string_view>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/order_by_impl.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"

using namespace std::string_view_literals;  // NOLINT

namespace arrow {

using arrow::internal::checked_cast;

namespace acero {
namespace {

class BackpressureReservoir : public BackpressureMonitor {
 public:
  BackpressureReservoir(uint64_t resume_if_below, uint64_t pause_if_above)
      : bytes_used_(0),
        state_change_counter_(0),
        resume_if_below_(resume_if_below),
        pause_if_above_(pause_if_above) {}

  uint64_t bytes_in_use() override {
    std::lock_guard lg(mutex_);
    return bytes_used_;
  }
  bool is_paused() override {
    std::lock_guard lg(mutex_);
    return state_change_counter_ % 2 == 1;
  }
  bool enabled() const { return pause_if_above_ > 0; }

  int32_t RecordProduced(uint64_t num_bytes) {
    std::lock_guard<std::mutex> lg(mutex_);
    bool was_under = bytes_used_ <= pause_if_above_;
    bytes_used_ += num_bytes;
    if (was_under && bytes_used_ > pause_if_above_) {
      return ++state_change_counter_;
    }
    return -1;
  }

  int32_t RecordConsumed(uint64_t num_bytes) {
    std::lock_guard<std::mutex> lg(mutex_);
    bool was_over = bytes_used_ >= resume_if_below_;
    bytes_used_ -= num_bytes;
    if (was_over && bytes_used_ < resume_if_below_) {
      return ++state_change_counter_;
    }
    return -1;
  }

 private:
  std::mutex mutex_;
  uint64_t bytes_used_;
  int32_t state_change_counter_;
  const uint64_t resume_if_below_;
  const uint64_t pause_if_above_;
};

class SinkNode : public ExecNode,
                 public TracedNode,
                 util::SerialSequencingQueue::Processor {
 public:
  SinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
           AsyncGenerator<std::optional<ExecBatch>>* generator,
           std::shared_ptr<Schema>* schema_out, BackpressureOptions backpressure,
           BackpressureMonitor** backpressure_monitor_out,
           std::optional<bool> sequence_output)
      : ExecNode(plan, std::move(inputs), {"collected"}, {}),
        TracedNode(this),
        backpressure_queue_(backpressure.resume_if_below, backpressure.pause_if_above),
        push_gen_(),
        producer_(push_gen_.producer()),
        node_destroyed_(std::make_shared<bool>(false)) {
    if (backpressure_monitor_out) {
      *backpressure_monitor_out = &backpressure_queue_;
    }
    auto node_destroyed_capture = node_destroyed_;
    if (schema_out) {
      *schema_out = inputs_[0]->output_schema();
    }
    *generator = [this, node_destroyed_capture]() -> Future<std::optional<ExecBatch>> {
      if (*node_destroyed_capture) {
        return Status::Invalid(
            "Attempt to consume data after the plan has been destroyed");
      }
      return push_gen_().Then([this](const std::optional<ExecBatch>& batch) {
        if (batch) {
          RecordBackpressureBytesFreed(*batch);
        }
        return batch;
      });
    };

    bool explicit_sequencing = sequence_output.has_value() && *sequence_output;
    bool sequencing_disabled = sequence_output.has_value() && !*sequence_output;
    if (explicit_sequencing ||
        (!sequencing_disabled && !inputs_[0]->ordering().is_unordered())) {
      sequencer_ = util::SerialSequencingQueue::Make(this);
    }
  }

  ~SinkNode() override { *node_destroyed_ = true; }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "SinkNode"));

    const auto& sink_options = checked_cast<const SinkNodeOptions&>(options);
    RETURN_NOT_OK(ValidateOptions(sink_options));
    return plan->EmplaceNode<SinkNode>(plan, std::move(inputs), sink_options.generator,
                                       sink_options.schema, sink_options.backpressure,
                                       sink_options.backpressure_monitor,
                                       sink_options.sequence_output);
  }

  const char* kind_name() const override { return "SinkNode"; }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
  }

  // sink nodes have no outputs from which to feel backpressure
  [[noreturn]] static void NoOutputs() {
    Unreachable("no outputs; this should never be called");
  }
  [[noreturn]] void ResumeProducing(ExecNode* output, int32_t counter) override {
    NoOutputs();
  }
  [[noreturn]] void PauseProducing(ExecNode* output, int32_t counter) override {
    NoOutputs();
  }

  Status StopProducingImpl() override {
    // An AsyncGenerator must always be finished.  So we go ahead and
    // close the producer.  However, for custom sink nodes, we don't want
    // to bother ordering and sending output so we don't call Finish
    producer_.Close();
    return Status::OK();
  }

  Status Validate() const override {
    ARROW_RETURN_NOT_OK(ExecNode::Validate());
    if (output_ != nullptr) {
      return Status::Invalid("Sink node '", label(), "' has an output");
    }
    if (inputs_[0]->ordering().is_unordered() && !!sequencer_) {
      return Status::Invalid("Sink node '", label(),
                             "' is configured to sequence output but there is no "
                             "meaningful ordering in the input");
    }
    return Status::OK();
  }

  void RecordBackpressureBytesUsed(const ExecBatch& batch) {
    if (backpressure_queue_.enabled()) {
      uint64_t bytes_used = static_cast<uint64_t>(batch.TotalBufferSize());
      auto state_change = backpressure_queue_.RecordProduced(bytes_used);
      if (state_change >= 0) {
        EVENT_ON_CURRENT_SPAN(
            "SinkNode::BackpressureApplied",
            {{"node.label", label()}, {"backpressure.counter", state_change}});
        inputs_[0]->PauseProducing(this, state_change);
      }
    }
  }

  void RecordBackpressureBytesFreed(const ExecBatch& batch) {
    if (backpressure_queue_.enabled()) {
      uint64_t bytes_freed = static_cast<uint64_t>(batch.TotalBufferSize());
      auto state_change = backpressure_queue_.RecordConsumed(bytes_freed);
      if (state_change >= 0) {
        EVENT_ON_CURRENT_SPAN(
            "SinkNode::BackpressureReleased",
            {{"node.label", label()}, {"backpressure.counter", state_change}});
        inputs_[0]->ResumeProducing(this, state_change);
      }
    }
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);

    DCHECK_EQ(input, inputs_[0]);

    RecordBackpressureBytesUsed(batch);
    if (sequencer_) {
      ARROW_RETURN_NOT_OK(sequencer_->InsertBatch(std::move(batch)));
    } else {
      ARROW_RETURN_NOT_OK(Process(std::move(batch)));
    }
    return Status::OK();
  }

  Status Process(ExecBatch batch) override {
    producer_.Push(std::move(batch));

    if (input_counter_.Increment()) {
      return Finish();
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    if (input_counter_.SetTotal(total_batches)) {
      return Finish();
    }
    return Status::OK();
  }

 protected:
  virtual Status Finish() {
    producer_.Close();
    return Status::OK();
  }

  static Status ValidateOptions(const SinkNodeOptions& sink_options) {
    if (!sink_options.generator) {
      return Status::Invalid(
          "`generator` is a required SinkNode option and cannot be null");
    }
    if (sink_options.backpressure.pause_if_above <
        sink_options.backpressure.resume_if_below) {
      return Status::Invalid(
          "`backpressure::pause_if_above` must be >= `backpressure::resume_if_below");
    }
    if (sink_options.backpressure.resume_if_below < 0) {
      return Status::Invalid(
          "`backpressure::pause_if_above and backpressure::resume_if_below must be >= 0. "
          " Set to 0 to disable backpressure.");
    }
    return Status::OK();
  }

  AtomicCounter input_counter_;

  // Needs to be a shared_ptr as the push generator can technically outlive the node
  BackpressureReservoir backpressure_queue_;
  PushGenerator<std::optional<ExecBatch>> push_gen_;
  PushGenerator<std::optional<ExecBatch>>::Producer producer_;
  std::shared_ptr<bool> node_destroyed_;
  std::unique_ptr<util::SerialSequencingQueue> sequencer_;
};

// A sink node that owns consuming the data and will not finish until the consumption
// is finished.  Use SinkNode if you are transferring the ownership of the data to another
// system.  Use ConsumingSinkNode if the data is being consumed within the exec plan (i.e.
// the exec plan should not complete until the consumption has completed).
class ConsumingSinkNode : public ExecNode,
                          public BackpressureControl,
                          public TracedNode,
                          util::SerialSequencingQueue::Processor {
 public:
  ConsumingSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                    std::shared_ptr<SinkNodeConsumer> consumer,
                    std::vector<std::string> names, std::optional<bool> sequence_output)
      : ExecNode(plan, std::move(inputs), {"to_consume"}, {}),
        TracedNode(this),
        consumer_(std::move(consumer)),
        names_(std::move(names)) {
    bool explicit_sequencing = sequence_output.has_value() && *sequence_output;
    bool sequencing_disabled = sequence_output.has_value() && !*sequence_output;
    // If the user explicitly requests sequencing then we configure a sequencer_ even
    // if the input isn't ordered.  This will later lead to a validation failure (which is
    // what the user is asking for in this case)
    if (explicit_sequencing ||
        (!sequencing_disabled && !inputs_[0]->ordering().is_unordered())) {
      sequencer_ = util::SerialSequencingQueue::Make(this);
    }
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "SinkNode"));

    const auto& sink_options = checked_cast<const ConsumingSinkNodeOptions&>(options);
    if (!sink_options.consumer) {
      return Status::Invalid("A SinkNodeConsumer is required");
    }

    return plan->EmplaceNode<ConsumingSinkNode>(
        plan, std::move(inputs), std::move(sink_options.consumer),
        std::move(sink_options.names), sink_options.sequence_output);
  }

  const char* kind_name() const override { return "ConsumingSinkNode"; }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    DCHECK_GT(inputs_.size(), 0);
    auto output_schema = inputs_[0]->output_schema();
    if (names_.size() > 0) {
      int num_fields = output_schema->num_fields();
      if (names_.size() != static_cast<size_t>(num_fields)) {
        return Status::Invalid(
            "A plan was created with custom field names but the number of names did not "
            "match the number of output columns");
      }
      ARROW_ASSIGN_OR_RAISE(output_schema, output_schema->WithNames(names_));
    }
    RETURN_NOT_OK(consumer_->Init(output_schema, this, plan_));
    return Status::OK();
  }

  Status Validate() const override {
    ARROW_RETURN_NOT_OK(ExecNode::Validate());
    if (output_ != nullptr) {
      return Status::Invalid("Consuming sink node '", label(), "' has an output");
    }
    if (inputs_[0]->ordering().is_unordered() && !!sequencer_) {
      return Status::Invalid("Consuming sink node '", label(),
                             "' is configured to sequence output but there is no "
                             "meaningful ordering in the input");
    }
    return Status::OK();
  }

  // sink nodes have no outputs from which to feel backpressure
  [[noreturn]] static void NoOutputs() {
    Unreachable("no outputs; this should never be called");
  }
  [[noreturn]] void ResumeProducing(ExecNode* output, int32_t counter) override {
    NoOutputs();
  }
  [[noreturn]] void PauseProducing(ExecNode* output, int32_t counter) override {
    NoOutputs();
  }

  void Pause() override { inputs_[0]->PauseProducing(this, ++backpressure_counter_); }

  void Resume() override { inputs_[0]->ResumeProducing(this, ++backpressure_counter_); }

  Status StopProducingImpl() override {
    // We do not call the consumer's finish method if ending early.  This might leave us
    // with half-written data files (in a dataset write for example) and that is ok.
    return Status::OK();
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);

    DCHECK_EQ(input, inputs_[0]);

    if (sequencer_) {
      return sequencer_->InsertBatch(std::move(batch));
    }
    return Process(std::move(batch));
  }

  Status Process(ExecBatch batch) override {
    // This can happen if an error was received and the source hasn't yet stopped.  Since
    // we have already called consumer_->Finish we don't want to call consumer_->Consume
    if (input_counter_.Completed()) {
      return Status::OK();
    }

    ARROW_RETURN_NOT_OK(consumer_->Consume(std::move(batch)));
    if (input_counter_.Increment()) {
      Finish();
    }

    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    if (input_counter_.SetTotal(total_batches)) {
      Finish();
    }
    return Status::OK();
  }

 protected:
  void Finish() {
    plan_->query_context()->async_scheduler()->AddSimpleTask(
        [this] { return consumer_->Finish(); }, "ConsumingSinkNode::Finish"sv);
  }

  AtomicCounter input_counter_;
  std::shared_ptr<SinkNodeConsumer> consumer_;
  std::vector<std::string> names_;
  std::atomic<int32_t> backpressure_counter_ = 0;
  std::unique_ptr<util::SerialSequencingQueue> sequencer_;
};
static Result<ExecNode*> MakeTableConsumingSinkNode(ExecPlan* plan,
                                                    std::vector<ExecNode*> inputs,
                                                    const ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "TableConsumingSinkNode"));
  const auto& sink_options = checked_cast<const TableSinkNodeOptions&>(options);
  MemoryPool* pool = plan->query_context()->memory_pool();
  auto tb_consumer =
      std::make_shared<TableSinkNodeConsumer>(sink_options.output_table, pool);
  auto consuming_sink_node_options = ConsumingSinkNodeOptions{tb_consumer};
  consuming_sink_node_options.sequence_output = sink_options.sequence_output;
  consuming_sink_node_options.names = sink_options.names;
  return MakeExecNode("consuming_sink", plan, inputs, consuming_sink_node_options);
}

// A sink node that accumulates inputs, then sorts them before emitting them.
struct OrderBySinkNode final : public SinkNode {
  OrderBySinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                  std::unique_ptr<OrderByImpl> impl,
                  AsyncGenerator<std::optional<ExecBatch>>* generator)
      : SinkNode(plan, std::move(inputs), generator, /*schema=*/nullptr,
                 /*backpressure=*/{},
                 /*backpressure_monitor_out=*/nullptr, /*sequence_output=*/false),
        impl_(std::move(impl)) {}

  const char* kind_name() const override { return "OrderBySinkNode"; }

  // A sink node that accumulates inputs, then sorts them before emitting them.
  static Result<ExecNode*> MakeSort(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                    const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "OrderBySinkNode"));

    const auto& sink_options = checked_cast<const OrderBySinkNodeOptions&>(options);
    if (sink_options.backpressure.should_apply_backpressure()) {
      return Status::Invalid("Backpressure cannot be applied to an OrderBySinkNode");
    }
    RETURN_NOT_OK(ValidateOrderByOptions(sink_options));
    ARROW_ASSIGN_OR_RAISE(
        std::unique_ptr<OrderByImpl> impl,
        OrderByImpl::MakeSort(plan->query_context()->exec_context(),
                              inputs[0]->output_schema(), sink_options.sort_options));
    return plan->EmplaceNode<OrderBySinkNode>(plan, std::move(inputs), std::move(impl),
                                              sink_options.generator);
  }

  static Status ValidateCommonOrderOptions(const SinkNodeOptions& options) {
    if (options.backpressure.should_apply_backpressure()) {
      return Status::Invalid("Backpressure cannot be applied on an ordering sink node");
    }
    return ValidateOptions(options);
  }

  static Status ValidateOrderByOptions(const OrderBySinkNodeOptions& options) {
    if (options.sort_options.sort_keys.empty()) {
      return Status::Invalid("At least one sort key should be specified");
    }
    return ValidateCommonOrderOptions(options);
  }

  // A sink node that receives inputs and then compute top_k/bottom_k.
  static Result<ExecNode*> MakeSelectK(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                       const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "OrderBySinkNode"));

    const auto& sink_options = checked_cast<const SelectKSinkNodeOptions&>(options);
    if (sink_options.backpressure.should_apply_backpressure()) {
      return Status::Invalid("Backpressure cannot be applied to an OrderBySinkNode");
    }
    RETURN_NOT_OK(ValidateSelectKOptions(sink_options));
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<OrderByImpl> impl,
                          OrderByImpl::MakeSelectK(plan->query_context()->exec_context(),
                                                   inputs[0]->output_schema(),
                                                   sink_options.select_k_options));
    return plan->EmplaceNode<OrderBySinkNode>(plan, std::move(inputs), std::move(impl),
                                              sink_options.generator);
  }

  static Status ValidateSelectKOptions(const SelectKSinkNodeOptions& options) {
    if (options.select_k_options.k <= 0) {
      return Status::Invalid("`k` must be > 0");
    }
    return ValidateCommonOrderOptions(options);
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);

    DCHECK_EQ(input, inputs_[0]);

    ARROW_ASSIGN_OR_RAISE(auto record_batch,
                          batch.ToRecordBatch(inputs_[0]->output_schema(),
                                              plan()->query_context()->memory_pool()));

    impl_->InputReceived(std::move(record_batch));
    if (input_counter_.Increment()) {
      return Finish();
    }
    return Status::OK();
  }

 protected:
  Status DoFinish() {
    auto scope = TraceFinish();
    ARROW_ASSIGN_OR_RAISE(Datum sorted, impl_->DoFinish());
    TableBatchReader reader(*sorted.table());
    while (true) {
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(reader.ReadNext(&batch));
      if (!batch) break;
      bool did_push = producer_.Push(ExecBatch(*batch));
      if (!did_push) break;  // producer_ was Closed already
    }
    return Status::OK();
  }

  Status Finish() override {
    arrow::util::tracing::Span span;
    ARROW_RETURN_NOT_OK(DoFinish());
    return SinkNode::Finish();
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    return std::string("by=") + impl_->ToString();
  }

 private:
  std::unique_ptr<OrderByImpl> impl_;
};

}  // namespace

namespace internal {

void RegisterSinkNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("select_k_sink", OrderBySinkNode::MakeSelectK));
  DCHECK_OK(registry->AddFactory("order_by_sink", OrderBySinkNode::MakeSort));
  DCHECK_OK(registry->AddFactory("consuming_sink", ConsumingSinkNode::Make));
  DCHECK_OK(registry->AddFactory("sink", SinkNode::Make));
  DCHECK_OK(registry->AddFactory("table_sink", MakeTableConsumingSinkNode));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow


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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/order_by_impl.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class BackpressureReservoir : public BackpressureMonitor {
 public:
  BackpressureReservoir(uint64_t resume_if_below, uint64_t pause_if_above)
      : bytes_used_(0),
        state_change_counter_(0),
        resume_if_below_(resume_if_below),
        pause_if_above_(pause_if_above) {}

  uint64_t bytes_in_use() const override { return bytes_used_; }
  bool is_paused() const override { return state_change_counter_ % 2 == 1; }
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

class SinkNode : public ExecNode {
 public:
  SinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
           AsyncGenerator<util::optional<ExecBatch>>* generator,
           BackpressureOptions backpressure,
           BackpressureMonitor** backpressure_monitor_out)
      : ExecNode(plan, std::move(inputs), {"collected"}, {},
                 /*num_outputs=*/0),
        backpressure_queue_(backpressure.resume_if_below, backpressure.pause_if_above),
        push_gen_(),
        producer_(push_gen_.producer()),
        node_destroyed_(std::make_shared<bool>(false)) {
    if (backpressure_monitor_out) {
      *backpressure_monitor_out = &backpressure_queue_;
    }
    auto node_destroyed_capture = node_destroyed_;
    *generator = [this, node_destroyed_capture]() -> Future<util::optional<ExecBatch>> {
      if (*node_destroyed_capture) {
        return Status::Invalid(
            "Attempt to consume data after the plan has been destroyed");
      }
      return push_gen_().Then([this](const util::optional<ExecBatch>& batch) {
        if (batch) {
          RecordBackpressureBytesFreed(*batch);
        }
        return batch;
      });
    };
  }

  ~SinkNode() override { *node_destroyed_ = true; }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "SinkNode"));

    const auto& sink_options = checked_cast<const SinkNodeOptions&>(options);
    RETURN_NOT_OK(ValidateOptions(sink_options));
    return plan->EmplaceNode<SinkNode>(plan, std::move(inputs), sink_options.generator,
                                       sink_options.backpressure,
                                       sink_options.backpressure_monitor);
  }

  const char* kind_name() const override { return "SinkNode"; }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.label", label()},
                        {"node.detail", ToString()},
                        {"node.kind", kind_name()}});
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_);
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
  [[noreturn]] void StopProducing(ExecNode* output) override { NoOutputs(); }

  void StopProducing() override {
    EVENT(span_, "StopProducing");

    Finish();
    inputs_[0]->StopProducing(this);
  }

  void RecordBackpressureBytesUsed(const ExecBatch& batch) {
    if (backpressure_queue_.enabled()) {
      uint64_t bytes_used = static_cast<uint64_t>(batch.TotalBufferSize());
      auto state_change = backpressure_queue_.RecordProduced(bytes_used);
      if (state_change >= 0) {
        EVENT(span_, "Backpressure applied", {{"backpressure.counter", state_change}});
        inputs_[0]->PauseProducing(this, state_change);
      }
    }
  }

  void RecordBackpressureBytesFreed(const ExecBatch& batch) {
    if (backpressure_queue_.enabled()) {
      uint64_t bytes_freed = static_cast<uint64_t>(batch.TotalBufferSize());
      auto state_change = backpressure_queue_.RecordConsumed(bytes_freed);
      if (state_change >= 0) {
        EVENT(span_, "Backpressure released", {{"backpressure.counter", state_change}});
        inputs_[0]->ResumeProducing(this, state_change);
      }
    }
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(
        span, span_, "InputReceived",
        {{"node.label", label()}, {"batch.length", batch.length}});

    DCHECK_EQ(input, inputs_[0]);

    RecordBackpressureBytesUsed(batch);
    bool did_push = producer_.Push(std::move(batch));
    if (!did_push) return;  // producer_ was Closed already

    if (input_counter_.Increment()) {
      Finish();
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    EVENT(span_, "ErrorReceived", {{"error", error.message()}});
    DCHECK_EQ(input, inputs_[0]);

    producer_.Push(std::move(error));

    if (input_counter_.Cancel()) {
      Finish();
    }
    inputs_[0]->StopProducing(this);
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    EVENT(span_, "InputFinished", {{"batches.length", total_batches}});
    if (input_counter_.SetTotal(total_batches)) {
      Finish();
    }
  }

 protected:
  virtual void Finish() {
    if (producer_.Close()) {
      finished_.MarkFinished();
    }
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
  PushGenerator<util::optional<ExecBatch>> push_gen_;
  PushGenerator<util::optional<ExecBatch>>::Producer producer_;
  std::shared_ptr<bool> node_destroyed_;
};

// A sink node that owns consuming the data and will not finish until the consumption
// is finished.  Use SinkNode if you are transferring the ownership of the data to another
// system.  Use ConsumingSinkNode if the data is being consumed within the exec plan (i.e.
// the exec plan should not complete until the consumption has completed).
class ConsumingSinkNode : public ExecNode, public BackpressureControl {
 public:
  ConsumingSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                    std::shared_ptr<SinkNodeConsumer> consumer,
                    std::vector<std::string> names)
      : ExecNode(plan, std::move(inputs), {"to_consume"}, {},
                 /*num_outputs=*/0),
        consumer_(std::move(consumer)),
        names_(std::move(names)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "SinkNode"));

    const auto& sink_options = checked_cast<const ConsumingSinkNodeOptions&>(options);
    return plan->EmplaceNode<ConsumingSinkNode>(plan, std::move(inputs),
                                                std::move(sink_options.consumer),
                                                std::move(sink_options.names));
  }

  const char* kind_name() const override { return "ConsumingSinkNode"; }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.label", label()},
                        {"node.detail", ToString()},
                        {"node.kind", kind_name()}});
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_);
    DCHECK_GT(inputs_.size(), 0);
    auto output_schema = inputs_[0]->output_schema();
    if (names_.size() > 0) {
      int num_fields = output_schema->num_fields();
      if (names_.size() != static_cast<size_t>(num_fields)) {
        return Status::Invalid("ConsumingSinkNode with mismatched number of names");
      }
      FieldVector fields(num_fields);
      int i = 0;
      for (const auto& output_field : output_schema->fields()) {
        fields[i] = field(names_[i], output_field->type());
        ++i;
      }
      output_schema = schema(std::move(fields));
    }
    RETURN_NOT_OK(consumer_->Init(output_schema, this));
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
  [[noreturn]] void StopProducing(ExecNode* output) override { NoOutputs(); }

  void Pause() override { inputs_[0]->PauseProducing(this, ++backpressure_counter_); }

  void Resume() override { inputs_[0]->ResumeProducing(this, ++backpressure_counter_); }

  void StopProducing() override {
    EVENT(span_, "StopProducing");
    Finish(Status::OK());
    inputs_[0]->StopProducing(this);
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(
        span, span_, "InputReceived",
        {{"node.label", label()}, {"batch.length", batch.length}});

    DCHECK_EQ(input, inputs_[0]);

    // This can happen if an error was received and the source hasn't yet stopped.  Since
    // we have already called consumer_->Finish we don't want to call consumer_->Consume
    if (input_counter_.Completed()) {
      return;
    }

    Status consumption_status = consumer_->Consume(std::move(batch));
    if (!consumption_status.ok()) {
      if (input_counter_.Cancel()) {
        Finish(std::move(consumption_status));
      }
      inputs_[0]->StopProducing(this);
      return;
    }

    if (input_counter_.Increment()) {
      Finish(Status::OK());
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    EVENT(span_, "ErrorReceived", {{"error", error.message()}});
    DCHECK_EQ(input, inputs_[0]);

    if (input_counter_.Cancel()) Finish(error);

    inputs_[0]->StopProducing(this);
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    EVENT(span_, "InputFinished", {{"batches.length", total_batches}});
    if (input_counter_.SetTotal(total_batches)) {
      Finish(Status::OK());
    }
  }

 protected:
  void Finish(const Status& finish_st) {
    consumer_->Finish().AddCallback([this, finish_st](const Status& st) {
      // Prefer the plan error over the consumer error
      Status final_status = finish_st & st;
      finished_.MarkFinished(std::move(final_status));
    });
  }

  AtomicCounter input_counter_;
  std::shared_ptr<SinkNodeConsumer> consumer_;
  std::vector<std::string> names_;
  int32_t backpressure_counter_ = 0;
};

/**
 * @brief This node is an extension on ConsumingSinkNode
 * to facilitate to get the output from an execution plan
 * as a table. We define a custom SinkNodeConsumer to
 * enable this functionality.
 */

struct TableSinkNodeConsumer : public SinkNodeConsumer {
 public:
  TableSinkNodeConsumer(std::shared_ptr<Table>* out, MemoryPool* pool)
      : out_(out), pool_(pool) {}

  Status Init(const std::shared_ptr<Schema>& schema,
              BackpressureControl* backpressure_control) override {
    // If the user is collecting into a table then backpressure is meaningless
    ARROW_UNUSED(backpressure_control);
    schema_ = schema;
    return Status::OK();
  }

  Status Consume(ExecBatch batch) override {
    std::lock_guard<std::mutex> guard(consume_mutex_);
    ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema_, pool_));
    batches_.push_back(rb);
    return Status::OK();
  }

  Future<> Finish() override {
    ARROW_ASSIGN_OR_RAISE(*out_, Table::FromRecordBatches(batches_));
    return Status::OK();
  }

 private:
  std::shared_ptr<Table>* out_;
  MemoryPool* pool_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  std::mutex consume_mutex_;
};

static Result<ExecNode*> MakeTableConsumingSinkNode(
    compute::ExecPlan* plan, std::vector<compute::ExecNode*> inputs,
    const compute::ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "TableConsumingSinkNode"));
  const auto& sink_options = checked_cast<const TableSinkNodeOptions&>(options);
  MemoryPool* pool = plan->exec_context()->memory_pool();
  auto tb_consumer =
      std::make_shared<TableSinkNodeConsumer>(sink_options.output_table, pool);
  auto consuming_sink_node_options = ConsumingSinkNodeOptions{tb_consumer};
  return MakeExecNode("consuming_sink", plan, inputs, consuming_sink_node_options);
}

// A sink node that accumulates inputs, then sorts them before emitting them.
struct OrderBySinkNode final : public SinkNode {
  OrderBySinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                  std::unique_ptr<OrderByImpl> impl,
                  AsyncGenerator<util::optional<ExecBatch>>* generator)
      : SinkNode(plan, std::move(inputs), generator, /*backpressure=*/{},
                 /*backpressure_monitor_out=*/nullptr),
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
        OrderByImpl::MakeSort(plan->exec_context(), inputs[0]->output_schema(),
                              sink_options.sort_options));
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
    ARROW_ASSIGN_OR_RAISE(
        std::unique_ptr<OrderByImpl> impl,
        OrderByImpl::MakeSelectK(plan->exec_context(), inputs[0]->output_schema(),
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

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(
        span, span_, "InputReceived",
        {{"node.label", label()}, {"batch.length", batch.length}});

    DCHECK_EQ(input, inputs_[0]);

    auto maybe_batch = batch.ToRecordBatch(inputs_[0]->output_schema(),
                                           plan()->exec_context()->memory_pool());
    if (ErrorIfNotOk(maybe_batch.status())) {
      StopProducing();
      if (input_counter_.Cancel()) {
        finished_.MarkFinished(maybe_batch.status());
      }
      return;
    }
    auto record_batch = maybe_batch.MoveValueUnsafe();

    impl_->InputReceived(std::move(record_batch));
    if (input_counter_.Increment()) {
      Finish();
    }
  }

 protected:
  Status DoFinish() {
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

  void Finish() override {
    util::tracing::Span span;
    START_COMPUTE_SPAN_WITH_PARENT(span, span_, "Finish", {{"node.label", label()}});
    Status st = DoFinish();
    if (ErrorIfNotOk(st)) {
      producer_.Push(std::move(st));
    }
    SinkNode::Finish();
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
}  // namespace compute
}  // namespace arrow

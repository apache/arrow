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

#include "arrow/acero/test_nodes.h"

#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/io/interfaces.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using arrow::internal::checked_cast;

namespace acero {

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(
    Iterator<std::optional<ExecBatch>> src, std::string label, double delay_sec,
    bool noisy) {
  struct DelayedIoGenState {
    DelayedIoGenState(Iterator<std::optional<ExecBatch>> batch_it, double delay_sec,
                      std::string label, bool noisy)
        : batch_it(std::move(batch_it)),
          delay_sec(delay_sec),
          label(std::move(label)),
          noisy(noisy) {}
    std::optional<ExecBatch> Next() {
      Result<std::optional<ExecBatch>> opt_batch_res = batch_it.Next();
      if (!opt_batch_res.ok()) {
        return std::nullopt;
      }
      std::optional<ExecBatch> opt_batch = opt_batch_res.ValueOrDie();
      if (!opt_batch) {
        return std::nullopt;
      }
      if (noisy) {
        std::cout << label + ": asking for batch(" + std::to_string(index) + ")\n";
      }
      SleepFor(delay_sec);
      ++index;
      return *opt_batch;
    }

    Iterator<std::optional<ExecBatch>> batch_it;
    double delay_sec;
    std::string label;
    bool noisy;
    std::size_t index = 0;
  };
  auto state = std::make_shared<DelayedIoGenState>(std::move(src), delay_sec,
                                                   std::move(label), noisy);
  return [state]() {
    return DeferNotOk(::arrow::io::default_io_context().executor()->Submit(
        [state]() { return state->Next(); }));
  };
}

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(
    AsyncGenerator<std::optional<ExecBatch>> src, std::string label, double delay_sec,
    bool noisy) {
  return MakeDelayedGen(MakeGeneratorIterator(src), label, delay_sec, noisy);
}

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(BatchesWithSchema src,
                                                        std::string label,
                                                        double delay_sec, bool noisy) {
  std::vector<std::optional<ExecBatch>> opt_batches = ::arrow::internal::MapVector(
      [](ExecBatch batch) { return std::make_optional(std::move(batch)); }, src.batches);
  return MakeDelayedGen(MakeVectorIterator(opt_batches), label, delay_sec, noisy);
}

namespace {

class JitterNode : public ExecNode {
 public:
  struct QueuedBatch {
    int adjusted_order;
    ExecBatch batch;
  };

  JitterNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema, random::SeedType seed,
             int max_jitter_modifier)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        rng_(seed),
        jitter_dist_(0, max_jitter_modifier) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "JitterNode"));
    const auto& jitter_options = checked_cast<const JitterNodeOptions&>(options);
    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();
    return plan->EmplaceNode<JitterNode>(plan, std::move(inputs),
                                         std::move(output_schema), jitter_options.seed,
                                         jitter_options.max_jitter_modifier);
  }

  const char* kind_name() const override { return "JitterNode"; }

  const Ordering& ordering() const override { return inputs_[0]->ordering(); }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    std::vector<QueuedBatch> to_deliver;
    bool should_finish = false;
    {
      std::lock_guard lk(mutex_);
      int current_count = counter_.count();
      int adjusted_count = current_count + jitter_dist_(rng_);
      QueuedBatch queued{adjusted_count, std::move(batch)};
      queue_.push(std::move(queued));
      while (!queue_.empty() && queue_.top().adjusted_order <= current_count) {
        to_deliver.push_back(std::move(queue_.top()));
        queue_.pop();
      }
      if (counter_.Increment()) {
        should_finish = true;
      }
    }
    Dispatch(std::move(to_deliver));
    if (should_finish) {
      Finish();
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    if (counter_.SetTotal(total_batches)) {
      Finish();
    }
    return output_->InputFinished(this, total_batches);
  }

  void Dispatch(std::vector<QueuedBatch> batches) {
    for (auto& queued : batches) {
      std::function<Status()> task = [this, batch = std::move(queued.batch)]() mutable {
        return output_->InputReceived(this, std::move(batch));
      };
      plan_->query_context()->ScheduleTask(std::move(task), "JitterNode::ProcessBatch");
    }
  }

 protected:
  Status StopProducingImpl() override { return Status::OK(); }
  virtual void Finish() {
    std::vector<QueuedBatch> to_deliver;
    while (!queue_.empty()) {
      to_deliver.push_back(std::move(queue_.top()));
      queue_.pop();
    }
    Dispatch(std::move(to_deliver));
  }

 private:
  struct QueuedBatchCompare {
    bool operator()(const QueuedBatch& left, const QueuedBatch& right) const {
      return left.adjusted_order > right.adjusted_order;
    }
  };

  AtomicCounter counter_;
  std::mutex mutex_;
  std::default_random_engine rng_;
  std::uniform_int_distribution<int> jitter_dist_;
  std::priority_queue<QueuedBatch, std::vector<QueuedBatch>, QueuedBatchCompare> queue_;
};

}  // namespace

class GateImpl {
 public:
  void ReleaseAllBatches() {
    std::lock_guard lg(mutex_);
    num_allowed_batches_ = -1;
    NotifyAll();
  }

  void ReleaseOneBatch() {
    std::lock_guard lg(mutex_);
    DCHECK_GE(num_allowed_batches_, 0)
        << "you can't call ReleaseOneBatch() after calling ReleaseAllBatches()";
    num_allowed_batches_++;
    NotifyAll();
  }

  Future<> WaitForNextReleasedBatch() {
    std::lock_guard lg(mutex_);
    if (current_waiter_.is_valid()) {
      return current_waiter_;
    }
    Future<> fut;
    if (num_allowed_batches_ < 0 || num_released_batches_ < num_allowed_batches_) {
      num_released_batches_++;
      return Future<>::MakeFinished();
    }

    current_waiter_ = Future<>::Make();
    return current_waiter_;
  }

 private:
  void NotifyAll() {
    if (current_waiter_.is_valid()) {
      Future<> to_unlock = current_waiter_;
      current_waiter_ = {};
      to_unlock.MarkFinished();
    }
  }

  Future<> current_waiter_;
  int num_released_batches_ = 0;
  int num_allowed_batches_ = 0;
  std::mutex mutex_;
};

std::shared_ptr<Gate> Gate::Make() { return std::make_shared<Gate>(); }

Gate::Gate() : impl_(new GateImpl()) {}

Gate::~Gate() { delete impl_; }

void Gate::ReleaseAllBatches() { impl_->ReleaseAllBatches(); }

void Gate::ReleaseOneBatch() { impl_->ReleaseOneBatch(); }

Future<> Gate::WaitForNextReleasedBatch() { return impl_->WaitForNextReleasedBatch(); }

namespace {

struct GatedNode : public ExecNode, public TracedNode {
  static constexpr auto kKindName = "BackpressureDelayingNode";
  static constexpr const char* kFactoryName = "backpressure_delay";

  static void Register() {
    auto exec_reg = default_exec_factory_registry();
    if (!exec_reg->GetFactory(kFactoryName).ok()) {
      ASSERT_OK(exec_reg->AddFactory(kFactoryName, GatedNode::Make));
    }
  }

  GatedNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
            std::shared_ptr<Schema> output_schema, const GatedNodeOptions& options)
      : ExecNode(plan, inputs, {"input"}, output_schema),
        TracedNode(this),
        gate_(options.gate) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, kKindName));
    auto gated_node_opts = static_cast<const GatedNodeOptions&>(options);
    return plan->EmplaceNode<GatedNode>(plan, inputs, inputs[0]->output_schema(),
                                        gated_node_opts);
  }

  const char* kind_name() const override { return kKindName; }

  const Ordering& ordering() const override { return inputs_[0]->ordering(); }
  Status InputFinished(ExecNode* input, int total_batches) override {
    return output_->InputFinished(this, total_batches);
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

  Status SendBatchesUnlocked(std::unique_lock<std::mutex>&& lock) {
    while (!queued_batches_.empty()) {
      // If we are ready to release the batch, do so immediately.
      Future<> maybe_unlocked = gate_->WaitForNextReleasedBatch();
      bool callback_added = maybe_unlocked.TryAddCallback([this] {
        return [this](const Status& st) {
          DCHECK_OK(st);
          plan_->query_context()->ScheduleTask(
              [this] {
                std::unique_lock lk(mutex_);
                return SendBatchesUnlocked(std::move(lk));
              },
              "GatedNode::ResumeAfterNotify");
        };
      });
      if (callback_added) {
        break;
      }
      // Otherwise, the future is already finished which means the gate is unlocked
      // and we are allowed to send a batch
      ExecBatch next = std::move(queued_batches_.front());
      queued_batches_.pop();
      lock.unlock();
      ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(next)));
      lock.lock();
    }
    return Status::OK();
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);

    // This may be called concurrently by the source and by a restart attempt.  Process
    // one at a time (this critical section should be pretty small)
    std::unique_lock lk(mutex_);
    queued_batches_.push(std::move(batch));

    return SendBatchesUnlocked(std::move(lk));
  }

  Gate* gate_;
  std::queue<ExecBatch> queued_batches_;
  std::mutex mutex_;
};

}  // namespace

void RegisterTestNodes() {
  static std::once_flag registered;
  std::call_once(registered, [] {
    ExecFactoryRegistry* registry = default_exec_factory_registry();
    DCHECK_OK(
        registry->AddFactory(std::string(JitterNodeOptions::kName), JitterNode::Make));
    DCHECK_OK(
        registry->AddFactory(std::string(GatedNodeOptions::kName), GatedNode::Make));
  });
}

}  // namespace acero
}  // namespace arrow

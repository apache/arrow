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

#include "arrow/compute/exec/test_nodes.h"

#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/query_context.h"
#include "arrow/compute/exec/util.h"
#include "arrow/io/interfaces.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

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

void RegisterTestNodes() {
  static std::once_flag registered;
  std::call_once(registered, [] {
    ExecFactoryRegistry* registry = default_exec_factory_registry();
    DCHECK_OK(
        registry->AddFactory(std::string(JitterNodeOptions::kName), JitterNode::Make));
  });
}

}  // namespace compute
}  // namespace arrow

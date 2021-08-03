
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

#include "arrow/compute/exec/exec_plan.h"

#include <mutex>
#include <unordered_map>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class SinkNode : public ExecNode {
 public:
  SinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
           AsyncGenerator<util::optional<ExecBatch>>* generator)
      : ExecNode(plan, std::move(inputs), {"collected"}, {},
                 /*num_outputs=*/0),
        producer_(MakeProducer(generator)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "SinkNode"));

    const auto& sink_options = checked_cast<const SinkNodeOptions&>(options);
    return plan->EmplaceNode<SinkNode>(plan, std::move(inputs), sink_options.generator);
  }

  static PushGenerator<util::optional<ExecBatch>>::Producer MakeProducer(
      AsyncGenerator<util::optional<ExecBatch>>* out_gen) {
    PushGenerator<util::optional<ExecBatch>> push_gen;
    auto out = push_gen.producer();
    *out_gen = [push_gen] {
      // Awful workaround for MSVC 19.0 (Visual Studio 2015) bug.
      // For some types including Future<optional<ExecBatch>>,
      // std::is_convertible<T, T>::value will be false causing
      // SFINAE exclusion of the std::function constructor we need.
      // Definining a convertible (but distinct) type soothes the
      // faulty trait.
      struct ConvertibleToFuture {
        operator Future<util::optional<ExecBatch>>() && {  // NOLINT runtime/explicit
          return std::move(ret);
        }
        Future<util::optional<ExecBatch>> ret;
      };

      return ConvertibleToFuture{push_gen()};
    };
    return out;
  }

  const char* kind_name() override { return "SinkNode"; }

  Status StartProducing() override {
    finished_ = Future<>::Make();
    return Status::OK();
  }

  // sink nodes have no outputs from which to feel backpressure
  [[noreturn]] static void NoOutputs() {
    Unreachable("no outputs; this should never be called");
  }
  [[noreturn]] void ResumeProducing(ExecNode* output) override { NoOutputs(); }
  [[noreturn]] void PauseProducing(ExecNode* output) override { NoOutputs(); }
  [[noreturn]] void StopProducing(ExecNode* output) override { NoOutputs(); }

  void StopProducing() override {
    Finish();
    inputs_[0]->StopProducing(this);
  }

  Future<> finished() override { return finished_; }

  void InputReceived(ExecNode* input, int seq_num, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    bool did_push = producer_.Push(std::move(batch));
    if (!did_push) return;  // producer_ was Closed already

    if (auto total = input_counter_.total()) {
      DCHECK_LE(seq_num, *total);
    }

    if (input_counter_.Increment()) {
      Finish();
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);

    producer_.Push(std::move(error));

    if (input_counter_.Cancel()) {
      Finish();
    }
    inputs_[0]->StopProducing(this);
  }

  void InputFinished(ExecNode* input, int seq_stop) override {
    if (input_counter_.SetTotal(seq_stop)) {
      Finish();
    }
  }

 protected:
  virtual void Finish() {
    if (producer_.Close()) {
      finished_.MarkFinished();
    }
  }

  AtomicCounter input_counter_;
  Future<> finished_ = Future<>::MakeFinished();

  PushGenerator<util::optional<ExecBatch>>::Producer producer_;
};

// A node that reorders inputs according to a tag. To be paired with OrderByNode.
struct ReorderNode final : public SinkNode {
  ReorderNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
              AsyncGenerator<util::optional<ExecBatch>>* generator)
      : SinkNode(plan, std::move(inputs), generator) {}

  const char* kind_name() override { return "ReorderNode"; }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ReorderNode"));

    const auto& sink_options = checked_cast<const SinkNodeOptions&>(options);
    return plan->EmplaceNode<ReorderNode>(plan, std::move(inputs),
                                          sink_options.generator);
  }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    if (input_counter_.Increment()) {
      Finish();
      return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    const auto& tag_scalar = *batch.values.back().scalar();
    const int64_t tag = checked_cast<const Int64Scalar&>(tag_scalar).value;
    batch.values.pop_back();
    PushAvailable();
    if (tag == next_batch_index_) {
      next_batch_index_++;
      producer_.Push(std::move(batch));
    } else {
      batches_.emplace(tag, std::move(batch));
    }
  }

 protected:
  void PushAvailable() {
    decltype(batches_)::iterator it;
    while ((it = batches_.find(next_batch_index_)) != batches_.end()) {
      auto batch = std::move(it->second);
      bool did_push = producer_.Push(std::move(batch));
      batches_.erase(it);
      // producer was Closed already
      if (!did_push) return;
      next_batch_index_++;
    }
  }

  void Finish() override {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      PushAvailable();
    }
    SinkNode::Finish();
  }

 private:
  std::unordered_map<int64_t, ExecBatch> batches_;
  std::mutex mutex_;
  int64_t next_batch_index_ = 0;
};

ExecFactoryRegistry::AddOnLoad kRegisterSink("sink", SinkNode::Make);
ExecFactoryRegistry::AddOnLoad kRegisterReorder("reorder", ReorderNode::Make);

}  // namespace
}  // namespace compute
}  // namespace arrow

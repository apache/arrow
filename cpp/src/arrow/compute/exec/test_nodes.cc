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
#include "arrow/compute/exec/util.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

std::vector<std::string> NumericallyLabeledInputs(int num_inputs) {
  std::vector<std::string> labels;
  for (int i = 0; i < num_inputs; i++) {
    labels.push_back("in_" + std::to_string(i));
  }
  return labels;
}

// Assumes all inputs will generate the same # of batches (only do this
// for example purposes, not a good idea for general execution) and concatenate
// the columns into a wider output table.
//
// Applies backpressure if the queue grows too large
class ConcatNode : public ExecNode {
 public:
  ConcatNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema, int pause_if_above,
             int resume_if_below)
      : ExecNode(plan, std::move(inputs),
                 NumericallyLabeledInputs(static_cast<int>(inputs.size())),
                 std::move(output_schema), 1),
        pause_if_above_(pause_if_above),
        resume_if_below_(resume_if_below) {
    for (std::size_t i = 0; i < inputs_.size(); i++) {
      input_ptrs_into_queue_.push_back(kNone);
      is_input_paused_.push_back(false);
    }
  }

  const char* kind_name() const override { return "concat"; }

  void ErrorReceived(ExecNode* input, Status error) override {}
  void InputFinished(ExecNode* input, int total_batches) override {
    outputs_[0]->InputFinished(this, total_batches);
    if (batch_counter_.SetTotal(total_batches)) {
      finished_.MarkFinished();
    }
  }
  Status StartProducing() override { return Status::OK(); }
  void PauseProducing(ExecNode* output, int32_t counter) override {
    std::unique_lock lk(mutex_);
    if (counter > max_out_pause_counter_) {
      max_out_pause_counter_ = counter;
      PauseAllInputsUnlocked();
    }
  }
  void ResumeProducing(ExecNode* output, int32_t counter) override {
    std::unique_lock lk(mutex_);
    if (counter > max_out_pause_counter_) {
      max_out_pause_counter_ = counter;
      ResumeAllInputsUnlocked();
    }
  }
  void StopProducing(ExecNode* output) override { inputs_[0]->StopProducing(this); }
  void StopProducing() override {}
  void InputReceived(ExecNode* input, ExecBatch batch) override {
    std::unique_lock lk(mutex_);
    std::size_t input_idx =
        std::find(inputs_.begin(), inputs_.end(), input) - inputs_.begin();
    auto itr = input_ptrs_into_queue_[input_idx];
    if (itr == kNone) {
      // Add a new group to the queue, potentially pausing this input if the queue is full
      std::vector<ExecBatch>& next_group = AddNewGroupUnlocked();
      next_group[input_idx] = batch;
      input_ptrs_into_queue_[input_idx] = kNone;

      int num_groups_queued = static_cast<int>(queue_.size());
      if (num_groups_queued > pause_if_above_) {
        PauseIfNeededUnlocked(input_idx);
      }
      return;
    }

    // Add to an existing group, potentially outputting if we fill the group and
    // potentially unpausing if we output
    (*itr)[input_idx] = batch;
    if (AllComplete(*itr)) {
      outputs_[0]->InputReceived(this, CombineBatches(*itr));
      CompleteGroupUnlocked(itr);
      ResumeThoseThatCanBeResumedUnlocked();
      lk.unlock();
      if (batch_counter_.Increment()) {
        finished_.MarkFinished();
      }
    } else {
      // We add one to this queue, potentially pausing this input
      input_ptrs_into_queue_[input_idx]++;
      if (input_ptrs_into_queue_[input_idx] == queue_.end()) {
        input_ptrs_into_queue_[input_idx] = kNone;
        if (static_cast<int>(queue_.size()) > pause_if_above_) {
          PauseIfNeededUnlocked(input_idx);
        }
      }
    }
  }

  void DebugPrintQueue(std::size_t label) {
    std::cout << label << ":[";
    for (std::size_t i = 0; i < inputs_.size(); i++) {
      auto itr = input_ptrs_into_queue_[i];
      if (itr == kNone) {
        std::cout << "<" << queue_.size() << ">";
      } else {
        std::size_t len = itr - queue_.begin();
        std::cout << len;
      }
      if (i < inputs_.size() - 1) {
        std::cout << ",";
      }
    }
    std::cout << "]" << std::endl;
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    const auto& concat_options = checked_cast<const ConcatNodeOptions&>(options);

    std::vector<std::shared_ptr<Field>> output_fields;
    for (const auto& input : inputs) {
      for (const auto& in_field : input->output_schema()->fields()) {
        output_fields.push_back(in_field);
      }
    }
    std::shared_ptr<Schema> output_schema = schema(std::move(output_fields));

    return plan->EmplaceNode<ConcatNode>(
        plan, std::move(inputs), std::move(output_schema), concat_options.pause_if_above,
        concat_options.resume_if_below);
  }

 private:
  void PauseAllInputsUnlocked() {
    pause_counter_++;
    for (auto& input : inputs_) {
      input->PauseProducing(this, pause_counter_);
    }
  }

  void ResumeAllInputsUnlocked() {
    pause_counter_++;
    for (auto& input : inputs_) {
      input->ResumeProducing(this, pause_counter_);
    }
  }

  void PauseIfNeededUnlocked(std::size_t input_idx) {
    if (!is_input_paused_[input_idx]) {
      is_input_paused_[input_idx] = true;
      std::cout << "Pausing input:" + std::to_string(input_idx) + "\n";
      inputs_[input_idx]->PauseProducing(this, pause_counter_++);
    }
  }

  void ResumeThoseThatCanBeResumedUnlocked() {
    for (std::size_t i = 0; i < inputs_.size(); i++) {
      if (!is_input_paused_[i]) {
        continue;
      }
      auto itr = input_ptrs_into_queue_[i];
      int num_queued = static_cast<int>(queue_.size());
      if (itr != kNone) {
        num_queued = static_cast<int>(itr - queue_.begin());
      }
      if (num_queued < resume_if_below_) {
        std::cout << "Resuming input: " + std::to_string(i) + "\n";
        is_input_paused_[i] = false;
        inputs_[i]->ResumeProducing(this, pause_counter_++);
      }
    }
  }

  std::vector<ExecBatch>& AddNewGroupUnlocked() {
    std::vector<ExecBatch> next_group(inputs_.size());
    queue_.push_back(std::move(next_group));
    for (std::size_t i = 0; i < inputs_.size(); i++) {
      if (input_ptrs_into_queue_[i] == kNone) {
        input_ptrs_into_queue_[i] = --queue_.end();
      }
    }
    return *(--queue_.end());
  }

  void CompleteGroupUnlocked(std::deque<std::vector<ExecBatch>>::iterator itr) {
    auto next = itr;
    next++;
    if (next == queue_.end()) {
      next = kNone;
    }
    for (std::size_t i = 0; i < inputs_.size(); i++) {
      if (input_ptrs_into_queue_[i] == itr) {
        input_ptrs_into_queue_[i] = next;
      }
    }
    queue_.erase(itr);
  }

  ExecBatch CombineBatches(const std::vector<ExecBatch>& group) {
    std::vector<Datum> combined;
    int64_t length = -1;
    for (const auto& item : group) {
      DCHECK(length == -1 || length == item.length);
      length = item.length;
      for (const auto& col : item.values) {
        combined.push_back(col);
      }
    }
    return ExecBatch(std::move(combined), length);
  }

  bool AllComplete(const std::vector<ExecBatch>& group) {
    for (const auto& batch : group) {
      if (batch.num_values() == 0) {
        return false;
      }
    }
    return true;
  }

  static inline const std::deque<std::vector<ExecBatch>>::iterator kNone = {};
  std::mutex mutex_;
  std::deque<std::vector<ExecBatch>> queue_;
  std::vector<std::deque<std::vector<ExecBatch>>::iterator> input_ptrs_into_queue_;
  std::vector<bool> is_input_paused_;
  int pause_if_above_;
  int resume_if_below_;
  int pause_counter_ = 1;
  int max_out_pause_counter_ = 0;
  AtomicCounter batch_counter_;
};

void RegisterConcatNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("concat", ConcatNode::Make));
}

// Make a source that is both noisy (prints when it emits)
// and slowed by some delay
AsyncGenerator<std::optional<ExecBatch>> MakeNoisyDelayedGen(BatchesWithSchema src,
                                                             std::string label,
                                                             double delay_sec) {
  std::vector<std::optional<ExecBatch>> opt_batches = ::arrow::internal::MapVector(
      [](ExecBatch batch) { return std::make_optional(std::move(batch)); }, src.batches);
  struct DelayedIoGenState {
    DelayedIoGenState(std::vector<std::optional<ExecBatch>> batches, double delay_sec,
                      std::string label)
        : batches(std::move(batches)), delay_sec(delay_sec), label(std::move(label)) {}
    std::optional<ExecBatch> Next() {
      if (index == batches.size()) {
        return std::nullopt;
      }
      std::cout << label + ": asking for batch(" + std::to_string(index) + ")\n";
      SleepFor(delay_sec);
      return batches[index++];
    }

    std::vector<std::optional<ExecBatch>> batches;
    double delay_sec;
    std::string label;
    std::size_t index = 0;
  };
  auto state = std::make_shared<DelayedIoGenState>(std::move(opt_batches), delay_sec,
                                                   std::move(label));
  return [state]() {
    return DeferNotOk(::arrow::io::default_io_context().executor()->Submit(
        [state]() { return state->Next(); }));
  };
}

}  // namespace compute
}  // namespace arrow

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

#include <sstream>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/accumulation_queue.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class FetchNode : public ExecNode {
 public:
  FetchNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
            std::shared_ptr<Schema> output_schema, int limit)
      : ExecNode(plan, std::move(inputs), {"source"}, std::move(output_schema), 1),
        limit_(limit) {
    accumulation_queue_ = util::OrderedAccumulationQueue::Make(
        [this](std::vector<ExecBatch> batch) {
          return CreateFetchTask(std::move(batch));
        },
        [this](util::OrderedAccumulationQueue::Task task) {
          auto task_wrapper = [task = std::move(task)](int) { return task(); };
          return plan_->ScheduleTask(task_wrapper);
        });
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FetchNode"));

    if (inputs.empty()) {
      return Status::Invalid("Fetch node with no input");
    }
    const std::shared_ptr<Schema>& schema = inputs[0]->output_schema();
    const auto& fetch_options = checked_cast<const FetchNodeOptions&>(options);

    if (fetch_options.limit < 0) {
      return Status::Invalid("FetchOptions::limit must be >= 0");
    }

    return plan->EmplaceNode<FetchNode>(plan, std::move(inputs), schema,
                                        fetch_options.limit);
  }

  const char* kind_name() const override { return "FetchNode"; }

  void ErrorReceived(ExecNode* input, Status error) override {
    outputs_[0]->ErrorReceived(this, std::move(error));
  }
  void InputFinished(ExecNode* input, int total_batches) override {
    if (input_counter_.SetTotal(total_batches)) {
      outputs_[0]->InputFinished(this, total_batches);
      finished_.MarkFinished();
    }
  }
  Status StartProducing() override { return Status::OK(); }
  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }
  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }
  void StopProducing(ExecNode* output) override { inputs_[0]->StopProducing(this); }
  void StopProducing() override {
    if (input_counter_.Cancel()) {
      finished_.MarkFinished();
    }
  }
  const std::vector<int32_t>& ordering() override { return inputs_[0]->ordering(); }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    DCHECK_EQ(input, inputs_[0]);
    Status st = accumulation_queue_->InsertBatch(std::move(batch));
    if (!st.ok()) {
      if (input_counter_.Cancel()) {
        finished_.MarkFinished(st);
      }
      return;
    }
    if (input_counter_.Increment()) {
      finished_.MarkFinished();
    }
  }

  util::OrderedAccumulationQueue::Task CreateFetchTask(std::vector<ExecBatch> batches) {
    int remaining = limit_ - seen_;
    int total_delivered_rows = 0;
    std::vector<ExecBatch> to_deliver;
    for (const auto& batch : batches) {
      if (batch.length > remaining) {
        to_deliver.push_back(batch.Slice(0, remaining));
        total_delivered_rows += remaining;
        remaining = 0;
        break;
      } else {
        total_delivered_rows += batch.length;
        remaining -= batch.length;
        to_deliver.push_back(std::move(batch));
      }
    }
    seen_ += total_delivered_rows;
    auto task = [this, task_batches = std::move(to_deliver)] {
      for (auto& task_batch : task_batches) {
        outputs_[0]->InputReceived(this, std::move(task_batch));
      }
      return Status::OK();
    };
    if (seen_ == limit_) {
      inputs_[0]->StopProducing(this);
      outputs_[0]->InputFinished(this, limit_);
      if (input_counter_.SetTotal(limit_)) {
        finished_.MarkFinished();
      }
    }
    return task;
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "limit=" << limit_;
    return ss.str();
  }

 private:
  int limit_;
  std::unique_ptr<util::OrderedAccumulationQueue> accumulation_queue_;
  int seen_ = 0;
  AtomicCounter input_counter_;
};

}  // namespace

namespace internal {

void RegisterFetchNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("fetch", FetchNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

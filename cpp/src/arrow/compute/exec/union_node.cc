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

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

struct UnionNode : ExecNode {
  UnionNode(ExecNode* lhs_input, ExecNode* rhs_input, ExecContext* ctx)
      : ExecNode(lhs_input->plan(), {lhs_input, rhs_input},
                 {"left_input_union", "right_input_union"},
                 /*output_schema=*/lhs_input->output_schema(),
                 /*num_outputs=*/1),
        ctx_(ctx) {}

  const char* kind_name() override { return "UnionNode"; }

  inline bool IsLeftInput(ExecNode* input) { return input == inputs_[0]; }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);

    if (finished_.is_finished()) {
      return;
    }
    {
      std::unique_lock<std::mutex> lock(mutex_);
      ++batch_count_;
    }
    outputs_[0]->InputReceived(this, seq, std::move(batch));
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));

    StopProducing();
  }

  void InputFinished(ExecNode* input, int num_total) override {
    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);
    {
      std::unique_lock<std::mutex> lk(mutex_);
      ++input_count_;
    }
    std::unique_lock<std::mutex> lk(mutex_);
    if (input_count_ == 2) {
      finished_.MarkFinished();
      outputs_[0]->InputFinished(this, batch_count_);
    }
  }

  Status StartProducing() override {
    finished_ = Future<>::Make();
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    finished_.MarkFinished();

    for (auto&& input : inputs_) {
      input->StopProducing(this);
    }
  }

  void StopProducing() override { inputs_[0]->StopProducing(this); }

  Future<> finished() override { return finished_; }

 private:
  ExecContext* ctx_;
  std::mutex mutex_;
  int batch_count_{0};
  int input_count_{0};
  Future<> finished_ = Future<>::MakeFinished();
};

Result<ExecNode*> MakeUnionNode(ExecNode* lhs_input, ExecNode* rhs_input) {
  auto ctx = lhs_input->plan()->exec_context();
  ExecPlan* plan = lhs_input->plan();
  return plan->EmplaceNode<UnionNode>(lhs_input, rhs_input, ctx);
}

ExecFactoryRegistry::AddOnLoad kRegisterUnion(
    "union",
    [](ExecPlan* plan, std::vector<ExecNode*> inputs,
       const ExecNodeOptions& options) -> Result<ExecNode*> {
      RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 2, "UnionNode"));

      ExecNode* left_input = inputs[0];
      ExecNode* right_input = inputs[1];

      return MakeUnionNode(left_input, right_input);
    });

}  // namespace compute
}  // namespace arrow

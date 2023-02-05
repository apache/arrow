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

#include "arrow/compute/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;
using internal::ToChars;

namespace compute {

namespace {
std::vector<std::string> GetInputLabels(const ExecNode::NodeVector& inputs) {
  std::vector<std::string> labels(inputs.size());
  for (size_t i = 0; i < inputs.size(); i++) {
    labels[i] = "input_" + std::to_string(i) + "_label";
  }
  return labels;
}
}  // namespace

class UnionNode : public ExecNode, public TracedNode {
 public:
  UnionNode(ExecPlan* plan, std::vector<ExecNode*> inputs)
      : ExecNode(plan, inputs, GetInputLabels(inputs),
                 /*output_schema=*/inputs[0]->output_schema()),
        TracedNode(this) {
    bool counter_completed = input_count_.SetTotal(static_cast<int>(inputs.size()));
    ARROW_DCHECK(counter_completed == false);
  }

  const char* kind_name() const override { return "UnionNode"; }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, static_cast<int>(inputs.size()),
                                         "UnionNode"));
    if (inputs.size() < 1) {
      return Status::Invalid("Constructing a `UnionNode` with inputs size less than 1");
    }
    auto schema = inputs.at(0)->output_schema();
    for (auto input : inputs) {
      if (!input->output_schema()->Equals(schema)) {
        return Status::Invalid(
            "UnionNode input schemas must all match, first schema was: ",
            schema->ToString(), " got schema: ", input->output_schema()->ToString());
      }
    }
    return plan->EmplaceNode<UnionNode>(plan, std::move(inputs));
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    NoteInputReceived(batch);
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());

    return output_->InputReceived(this, std::move(batch));
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());

    total_batches_.fetch_add(total_batches);

    if (input_count_.Increment()) {
      return output_->InputFinished(this, total_batches_.load());
    }

    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    for (auto* input : inputs_) {
      input->PauseProducing(this, counter);
    }
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    for (auto* input : inputs_) {
      input->ResumeProducing(this, counter);
    }
  }

  Status StopProducingImpl() override { return Status::OK(); }

 private:
  AtomicCounter input_count_;
  std::atomic<int> total_batches_{0};
};

namespace internal {

void RegisterUnionNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("union", UnionNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

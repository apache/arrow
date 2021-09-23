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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class FilterNode : public ExecNode {
 public:
  FilterNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema, Expression filter)
      : ExecNode(plan, std::move(inputs), /*input_labels=*/{"target"},
                 std::move(output_schema),
                 /*num_outputs=*/1),
        filter_(std::move(filter)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FilterNode"));
    auto schema = inputs[0]->output_schema();

    const auto& filter_options = checked_cast<const FilterNodeOptions&>(options);

    auto filter_expression = filter_options.filter_expression;
    if (!filter_expression.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(filter_expression, filter_expression.Bind(*schema));
    }

    if (filter_expression.type()->id() != Type::BOOL) {
      return Status::TypeError("Filter expression must evaluate to bool, but ",
                               filter_expression.ToString(), " evaluates to ",
                               filter_expression.type()->ToString());
    }

    return plan->EmplaceNode<FilterNode>(plan, std::move(inputs), std::move(schema),
                                         std::move(filter_expression));
  }

  const char* kind_name() const override { return "FilterNode"; }

  Result<ExecBatch> DoFilter(const ExecBatch& target) {
    ARROW_ASSIGN_OR_RAISE(Expression simplified_filter,
                          SimplifyWithGuarantee(filter_, target.guarantee));

    ARROW_ASSIGN_OR_RAISE(Datum mask, ExecuteScalarExpression(simplified_filter, target,
                                                              plan()->exec_context()));

    if (mask.is_scalar()) {
      const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
      if (mask_scalar.is_valid && mask_scalar.value) {
        return target;
      }

      return target.Slice(0, 0);
    }

    // if the values are all scalar then the mask must also be
    DCHECK(!std::all_of(target.values.begin(), target.values.end(),
                        [](const Datum& value) { return value.is_scalar(); }));

    auto values = target.values;
    for (auto& value : values) {
      if (value.is_scalar()) continue;
      ARROW_ASSIGN_OR_RAISE(value, Filter(value, mask, FilterOptions::Defaults()));
    }
    return ExecBatch::Make(std::move(values));
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);
    if (finished_.is_finished()) {
      return;
    }
    auto executor = plan()->exec_context()->executor();
    if (executor) {
      auto maybe_future = executor->Submit([this, batch] {
        auto maybe_filtered = DoFilter(std::move(batch));
        if (ErrorIfNotOk(maybe_filtered.status())) return Status::OK();
        maybe_filtered->guarantee = batch.guarantee;
        this->outputs_[0]->InputReceived(this, maybe_filtered.MoveValueUnsafe());
        return Status::OK();
      });
      if (!maybe_future.ok()) {
        outputs_[0]->ErrorReceived(this, maybe_future.status());
      }

      auto status = task_group_.AddTask(maybe_future.MoveValueUnsafe());
      if (!status.ok()) {
        outputs_[0]->ErrorReceived(this, std::move(status));
      }
    } else {
      auto maybe_filtered = DoFilter(std::move(batch));
      if (ErrorIfNotOk(maybe_filtered.status())) return;
      maybe_filtered->guarantee = batch.guarantee;
      outputs_[0]->InputReceived(this, maybe_filtered.MoveValueUnsafe());
    }
    if (batch_count_.Increment()) {
      task_group_.WaitForTasksToFinish().AddCallback(
          [this](const Status& status) { this->finished_.MarkFinished(status); });
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);

    total_batches_.fetch_add(total_batches);

    outputs_[0]->InputFinished(this, total_batches);
    if (batch_count_.SetTotal(total_batches_.load())) {
      task_group_.WaitForTasksToFinish().AddCallback(
          [this](const Status& status) { this->finished_.MarkFinished(status); });
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
    StopProducing();
  }

  void StopProducing() override {
    if (batch_count_.Cancel()) {
      finished_.MarkFinished();
    }
    inputs_[0]->StopProducing(this);
  }

  Future<> finished() override {
    auto executor = plan()->exec_context()->executor();
    if (executor) {
      return finished_;
    }
    return inputs_[0]->finished();
  }

 protected:
  std::string ToStringExtra() const override { return "filter=" + filter_.ToString(); }

 private:
  Expression filter_;
  AtomicCounter batch_count_;
  std::atomic<int> total_batches_{0};
  Future<> finished_ = Future<>::MakeFinished();
  util::AsyncTaskGroup task_group_;
};
}  // namespace

namespace internal {
void RegisterFilterNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("filter", FilterNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

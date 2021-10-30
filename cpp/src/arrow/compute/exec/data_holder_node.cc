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

#include "arrow/compute/memory_resources.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

class DataHolderManager {
 public:
  explicit DataHolderManager(ExecContext* context)
      : context_(context), gen_(), producer_(gen_.producer()) {}

  Status Push(const std::shared_ptr<RecordBatch>& batch) {
    bool pushed = false;
    auto resources = context_->memory_resources();
    for (auto memory_resource : resources->memory_resources()) {
      auto memory_used = memory_resource->memory_used();
      if (memory_used < memory_resource->memory_limit()) {
        ARROW_ASSIGN_OR_RAISE(auto data_holder, memory_resource->GetDataHolder(batch));
        this->producer_.Push(std::move(data_holder));
        pushed = true;
        break;
      }
    }
    if (!pushed) {
      return Status::Invalid("No memory resource registered at all in the exec_context");
    }
    return Status::OK();
  }
  AsyncGenerator<std::shared_ptr<DataHolder>> generator() { return gen_; }

 public:
  ExecContext* context_;
  PushGenerator<std::shared_ptr<DataHolder>> gen_;
  PushGenerator<std::shared_ptr<DataHolder>>::Producer producer_;
};

class DataHolderNode : public ExecNode {
 public:
  DataHolderNode(ExecPlan* plan, NodeVector inputs, std::vector<std::string> input_labels,
                 std::shared_ptr<Schema> output_schema, int num_outputs)
      : ExecNode(plan, std::move(inputs), input_labels, std::move(output_schema),
                 /*num_outputs=*/num_outputs) {
    executor_ = plan->exec_context()->executor();

    data_holder_manager_ =
        ::arrow::internal::make_unique<DataHolderManager>(plan->exec_context());

    auto status = task_group_.AddTask([this]() -> Result<Future<>> {
      ARROW_DCHECK(executor_ != nullptr);
      return executor_->Submit(this->stop_source_.token(), [this] {
        auto generator = this->data_holder_manager_->generator();
        auto iterator = MakeGeneratorIterator(std::move(generator));
        while (true) {
          ARROW_ASSIGN_OR_RAISE(auto result, iterator.Next());
          if (IsIterationEnd(result)) {
            break;
          }
          ARROW_ASSIGN_OR_RAISE(ExecBatch batch, result->Get());
          this->outputs_[0]->InputReceived(this, batch);
        }
        return Status::OK();
      });
    });
    if (!status.ok()) {
      if (input_counter_.Cancel()) {
        this->Finish(status);
      }
      inputs_[0]->StopProducing(this);
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->InputFinished(this, total_batches);
    if (input_counter_.SetTotal(total_batches)) {
      this->Finish();
    }
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<DataHolderNode>(plan, std::move(inputs),
                                             std::vector<std::string>{"target"},
                                             std::move(schema), /*num_outputs=*/1);
  }

  const char* kind_name() const override { return "DataHolderNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    if (finished_.is_finished()) {
      return;
    }
    auto status = task_group_.AddTask([this, batch]() -> Result<Future<>> {
      return this->executor_->Submit(this->stop_source_.token(), [this, batch]() {
        auto pool = this->plan()->exec_context()->memory_pool();
        ARROW_ASSIGN_OR_RAISE(auto record_batch,
                              batch.ToRecordBatch(this->output_schema(), pool));
        Status status = data_holder_manager_->Push(record_batch);
        if (ErrorIfNotOk(status)) {
          return status;
        }
        if (this->input_counter_.Increment()) {
          this->Finish(status);
        }
        return Status::OK();
      });
    });
    if (!status.ok()) {
      if (input_counter_.Cancel()) {
        this->Finish(status);
      }
      inputs_[0]->StopProducing(this);
      return;
    }
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    if (executor_) {
      this->stop_source_.RequestStop();
    }
    if (input_counter_.Cancel()) {
      this->Finish();
    }
    inputs_[0]->StopProducing(this);
  }

  Future<> finished() override { return finished_; }

  std::string ToStringExtra() const override { return ""; }

 protected:
  void Finish(Status finish_st = Status::OK()) {
    this->data_holder_manager_->producer_.Close();

    task_group_.End().AddCallback([this, finish_st](const Status& st) {
      Status final_status = finish_st & st;
      this->finished_.MarkFinished(final_status);
    });
  }

 protected:
  // Counter for the number of batches received
  AtomicCounter input_counter_;

  // Future to sync finished
  Future<> finished_ = Future<>::Make();

  // The task group for the corresponding batches
  util::AsyncTaskGroup task_group_;

  ::arrow::internal::Executor* executor_;

  // Variable used to cancel remaining tasks in the executor
  StopSource stop_source_;

  std::unique_ptr<DataHolderManager> data_holder_manager_;
};

namespace internal {

void RegisterDataHolderNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("data_holder", DataHolderNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

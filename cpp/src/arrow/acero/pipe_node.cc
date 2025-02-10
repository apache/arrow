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

#include "arrow/acero/pipe_node.h"
#include <iostream>
#include "arrow/acero/concurrent_queue_internal.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"
namespace arrow {

using internal::checked_cast;

namespace acero {

namespace {

struct PipeSourceNode : public PipeSource, public ExecNode {
  PipeSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema, std::string pipe_name,
                 Ordering ordering)
      : ExecNode(plan, {}, {}, std::move(schema)),
        pipe_name_(pipe_name),
        ordering_(std::move(ordering)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName));
    const auto& pipe_source_options = checked_cast<const PipeSourceNodeOptions&>(options);
    return plan->EmplaceNode<PipeSourceNode>(plan, pipe_source_options.output_schema,
                                             pipe_source_options.pipe_name,
                                             pipe_source_options.ordering);
  }

  [[noreturn]] static void NoInputs() {
    Unreachable("no inputs; this should never be called");
  }
  [[noreturn]] Status InputReceived(ExecNode*, ExecBatch) override { NoInputs(); }
  [[noreturn]] Status InputFinished(ExecNode*, int) override { NoInputs(); }

  Status HandleInputReceived(ExecBatch batch) override {
    return output_->InputReceived(this, std::move(batch));
  }
  Status HandleInputFinished(int total_batches) override {
    return output_->InputFinished(this, total_batches);
  }

  const Ordering& ordering() const override { return ordering_; }

  Status StartProducing() override {
    auto st = PipeSource::Validate(ordering());
    if (!st.ok()) {
      return st.WithMessage("Pipe '", pipe_name_, "' error: ", st.message());
    }
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    PipeSource::Pause(counter);
  }
  void ResumeProducing(ExecNode* output, int32_t counter) override {
    PipeSource::Resume(counter);
  }

  Status StopProducingImpl() override { return Status::OK(); }

  static const char kKindName[];
  const char* kind_name() const override { return kKindName; }

  const std::string pipe_name_;
  Ordering ordering_;
};

const char PipeSourceNode::kKindName[] = "PipeSourceNode";

class PipeSinkBackpressureControl : public BackpressureControl {
 public:
  PipeSinkBackpressureControl(ExecNode* node, ExecNode* output)
      : node_(node), output_(output) {}

  void Pause() override { node_->PauseProducing(output_, ++backpressure_counter_); }
  void Resume() override { node_->ResumeProducing(output_, ++backpressure_counter_); }

 private:
  ExecNode* node_;
  ExecNode* output_;
  std::atomic<int32_t> backpressure_counter_;
};

class PipeSinkNode : public ExecNode {
 public:
  PipeSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs, std::string pipe_name)
      : ExecNode(plan, inputs, /*input_labels=*/{pipe_name}, {}) {
    pipe_ = std::make_shared<Pipe>(
        plan, std::move(pipe_name),
        std::make_unique<PipeSinkBackpressureControl>(inputs[0], this),
        inputs[0]->ordering());
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "PipeSinkNode"));
    const auto& pipe_tee_options = checked_cast<const PipeSinkNodeOptions&>(options);
    return plan->EmplaceNode<PipeSinkNode>(plan, std::move(inputs),
                                           pipe_tee_options.pipe_name);
  }
  static const char kKindName[];
  const char* kind_name() const override { return kKindName; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);
    return pipe_->InputReceived(batch);
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    return pipe_->InputFinished(total_batches);
  }

  Status Init() { return pipe_->Init(inputs_[0]->output_schema()); }

  Status StartProducing() override { return Status::OK(); }

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

 protected:
  Status StopProducingImpl() override { return Status::OK(); }

  std::string ToStringExtra(int indent = 0) const override {
    // std::string ret="pipe_tee(";
    // bool first=true;
    // for(auto &stream:streams_){
    //   if(!first)
    //     ret+=",";
    //   ret+=stream->declaration_.label;
    //   first=false;
    // }
    // ret+=")";
    return "pipe_tee";
  }

 protected:
  std::shared_ptr<Pipe> pipe_;
};

const char PipeSinkNode::kKindName[] = "PipeSinkNode";

class PipeTeeNode : public PipeSource, public PipeSinkNode {
 public:
  PipeTeeNode(ExecPlan* plan, std::vector<ExecNode*> inputs, std::string pipe_name)
      : PipeSinkNode(plan, inputs, pipe_name) {
    output_schema_ = inputs[0]->output_schema();
    PipeSinkNode::pipe_->addSource(this);
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "PipeTeeNode"));
    const auto& pipe_tee_options = checked_cast<const PipeSinkNodeOptions&>(options);
    return plan->EmplaceNode<PipeTeeNode>(plan, std::move(inputs),
                                          pipe_tee_options.pipe_name);
  }
  static const char kKindName[];
  const char* kind_name() const override { return kKindName; }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    PipeSource::Pause(counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    PipeSource::Resume(counter);
  }

  Status HandleInputReceived(ExecBatch batch) override {
    return output_->InputReceived(this, std::move(batch));
  }
  Status HandleInputFinished(int total_batches) override {
    return output_->InputFinished(this, total_batches);
  }

 protected:
  Status StopProducingImpl() override { return Status::OK(); }

  std::string ToStringExtra(int indent = 0) const override {
    // std::string ret="pipe_tee(";
    // bool first=true;
    // for(auto &stream:streams_){
    //   if(!first)
    //     ret+=",";
    //   ret+=stream->declaration_.label;
    //   first=false;
    // }
    // ret+=")";
    return "pipe_tee";
  }
};

const char PipeTeeNode::kKindName[] = "PipeTeeNode";

}  // namespace

PipeSource::PipeSource() {}
void PipeSource::Initialize(Pipe* pipe) { pipe_ = pipe; }

void PipeSource::Pause(int32_t counter) { pipe_->Pause(this, counter); }
void PipeSource::Resume(int32_t counter) { pipe_->Resume(this, counter); }

Status PipeSource::Validate(const Ordering& ordering) {
  if (!pipe_) {
    return Status::Invalid("Pipe does not have sink");
  }
  if (!ordering.IsSuborderOf(pipe_->ordering()))
    return Status::Invalid("Pipe source ordering is not subordering of pipe sink");

  return Status::OK();
}

Pipe::Pipe(ExecPlan* plan, std::string pipe_name,
           std::unique_ptr<BackpressureControl> ctrl, Ordering ordering)
    : plan_(plan), ordering_(ordering), pipe_name_(pipe_name), ctrl_(std::move(ctrl)) {}

const Ordering& Pipe::ordering() const { return ordering_; }

// Called from pipe_source nodes
void Pipe::Pause(PipeSource* output, int counter) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!paused_[output]) {
    paused_[output] = true;
    if (0 == paused_count_++) {
      ctrl_->Pause();
    }
  }
}

// Called from pipe_source nodes
void Pipe::Resume(PipeSource* output, int counter) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (paused_[output]) {
    paused_[output] = false;
    if (0 == --paused_count_) {
      ctrl_->Resume();
    }
  }
}

// Called from pipe_sink
Status Pipe::InputReceived(ExecBatch batch) {
  for (auto& source_node : source_nodes_) {
    plan_->query_context()->ScheduleTask(
        [source_node, batch]() mutable {
          return source_node->HandleInputReceived(batch);
        },
        "Pipe::InputReceived");
  }
  if (last_source_node_) return last_source_node_->HandleInputReceived(batch);
  // No consumers registered;
  return Status::OK();
}
// Called from pipe_sink
Status Pipe::InputFinished(int total_batches) {
  for (auto& source_node : source_nodes_) {
    plan_->query_context()->ScheduleTask(
        [source_node, total_batches]() {
          return source_node->HandleInputFinished(total_batches);
        },
        "Pipe::HandleInputFinished");
  }
  if (last_source_node_) return last_source_node_->HandleInputFinished(total_batches);
  // No consumers registered;
  return Status::OK();
}

void Pipe::addSource(PipeSource* source) {
  source->Initialize(this);
  // First added source is handled in receiving task. All additional sources are delivered
  // in their own sutmit tasks
  if (!last_source_node_)
    last_source_node_ = source;
  else {
    source_nodes_.push_back(source);
  }
}

Status Pipe::Init(const std::shared_ptr<Schema> schema) {
  for (auto node : plan_->nodes()) {
    if (node->kind_name() == PipeSourceNode::kKindName) {
      if (!schema->Equals(node->output_schema())) {
        return Status::Invalid("Pipe sechma does not match for " + pipe_name_);
      }
      PipeSourceNode* pipe_source = checked_cast<PipeSourceNode*>(node);
      if (pipe_source->pipe_name_ == pipe_name_) {
        addSource(pipe_source);
        // std::cout << std::string(node->kind_name()) + ":! " + node->label()
        //           << std::endl;
      }
      // else {
      //   std::cout << std::string(node->kind_name()) + ":+ " + node->label()
      //             << std::endl;
      // }
    }
    // else {
    //   std::cout << std::string(node->kind_name()) + ":- " + node->label() <<
    //   std::endl;
    // }
  }
  return Status::OK();
}

namespace internal {
void RegisterPipeNodes(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("pipe_source", PipeSourceNode::Make));
  DCHECK_OK(registry->AddFactory("pipe_sink", PipeSinkNode::Make));
  DCHECK_OK(registry->AddFactory("pipe_tee", PipeTeeNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow

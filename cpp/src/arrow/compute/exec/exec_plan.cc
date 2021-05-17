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
#include <unordered_set>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

namespace {

struct ExecPlanImpl : public ExecPlan {
  ExecPlanImpl() = default;

  ~ExecPlanImpl() override = default;

  ExecNode* AddNode(std::unique_ptr<ExecNode> node) {
    if (node->num_inputs() == 0) {
      sources_.push_back(node.get());
    }
    if (node->num_outputs() == 0) {
      sinks_.push_back(node.get());
    }
    nodes_.push_back(std::move(node));
    return nodes_.back().get();
  }

  Status Validate() const {
    if (nodes_.empty()) {
      return Status::Invalid("ExecPlan has no node");
    }
    for (const auto& node : nodes_) {
      RETURN_NOT_OK(node->Validate());
    }
    return Status::OK();
  }

  Status StartProducing() {
    ARROW_ASSIGN_OR_RAISE(auto sorted_nodes, ReverseTopoSort());
    Status st;
    auto it = sorted_nodes.begin();
    while (it != sorted_nodes.end() && st.ok()) {
      st &= (*it++)->StartProducing();
    }
    if (!st.ok()) {
      // Stop nodes that successfully started, in reverse order
      // (`it` now points after the node that failed starting, so need to rewind)
      --it;
      while (it != sorted_nodes.begin()) {
        (*--it)->StopProducing();
      }
    }
    return st;
  }

  Result<NodeVector> ReverseTopoSort() {
    struct TopoSort {
      const std::vector<std::unique_ptr<ExecNode>>& nodes;
      std::unordered_set<ExecNode*> visited;
      NodeVector sorted;

      explicit TopoSort(const std::vector<std::unique_ptr<ExecNode>>& nodes)
          : nodes(nodes) {
        visited.reserve(nodes.size());
        sorted.reserve(nodes.size());
      }

      Status Sort() {
        for (const auto& node : nodes) {
          RETURN_NOT_OK(Visit(node.get()));
        }
        DCHECK_EQ(sorted.size(), nodes.size());
        DCHECK_EQ(visited.size(), nodes.size());
        return Status::OK();
      }

      Status Visit(ExecNode* node) {
        if (visited.count(node) != 0) {
          return Status::OK();
        }

        for (auto input : node->inputs()) {
          // Ensure that producers are inserted before this consumer
          RETURN_NOT_OK(Visit(input));
        }

        visited.insert(node);
        sorted.push_back(node);
        return Status::OK();
      }

      NodeVector Reverse() {
        std::reverse(sorted.begin(), sorted.end());
        return std::move(sorted);
      }
    } topo_sort(nodes_);

    RETURN_NOT_OK(topo_sort.Sort());
    return topo_sort.Reverse();
  }

  std::vector<std::unique_ptr<ExecNode>> nodes_;
  NodeVector sources_, sinks_;
};

ExecPlanImpl* ToDerived(ExecPlan* ptr) { return checked_cast<ExecPlanImpl*>(ptr); }

const ExecPlanImpl* ToDerived(const ExecPlan* ptr) {
  return checked_cast<const ExecPlanImpl*>(ptr);
}

util::optional<int> GetNodeIndex(const std::vector<ExecNode*>& nodes,
                                 const ExecNode* node) {
  for (int i = 0; i < static_cast<int>(nodes.size()); ++i) {
    if (nodes[i] == node) return i;
  }
  return util::nullopt;
}

}  // namespace

Result<std::shared_ptr<ExecPlan>> ExecPlan::Make() {
  return std::make_shared<ExecPlanImpl>();
}

ExecNode* ExecPlan::AddNode(std::unique_ptr<ExecNode> node) {
  return ToDerived(this)->AddNode(std::move(node));
}

const ExecPlan::NodeVector& ExecPlan::sources() const {
  return ToDerived(this)->sources_;
}

const ExecPlan::NodeVector& ExecPlan::sinks() const { return ToDerived(this)->sinks_; }

Status ExecPlan::Validate() { return ToDerived(this)->Validate(); }

Status ExecPlan::StartProducing() { return ToDerived(this)->StartProducing(); }

ExecNode::ExecNode(ExecPlan* plan, std::string label, NodeVector inputs,
                   std::vector<std::string> input_labels, BatchDescr output_descr,
                   int num_outputs)
    : plan_(plan),
      label_(std::move(label)),
      inputs_(std::move(inputs)),
      input_labels_(std::move(input_labels)),
      output_descr_(std::move(output_descr)),
      num_outputs_(num_outputs) {
  for (auto input : inputs_) {
    input->outputs_.push_back(this);
  }
}

Status ExecNode::Validate() const {
  if (inputs_.size() != input_labels_.size()) {
    return Status::Invalid("Invalid number of inputs for '", label(), "' (expected ",
                           num_inputs(), ", actual ", input_labels_.size(), ")");
  }

  if (static_cast<int>(outputs_.size()) != num_outputs_) {
    return Status::Invalid("Invalid number of outputs for '", label(), "' (expected ",
                           num_outputs(), ", actual ", outputs_.size(), ")");
  }

  for (auto out : outputs_) {
    auto input_index = GetNodeIndex(out->inputs(), this);
    if (!input_index) {
      return Status::Invalid("Node '", label(), "' outputs to node '", out->label(),
                             "' but is not listed as an input.");
    }
  }

  return Status::OK();
}

struct GeneratorNode : ExecNode {
  GeneratorNode(ExecPlan* plan, std::string label, ExecNode::BatchDescr output_descr,
                AsyncGenerator<util::optional<ExecBatch>> generator)
      : ExecNode(plan, std::move(label), {}, {}, std::move(output_descr),
                 /*num_outputs=*/1),
        generator_(std::move(generator)) {}

  const char* kind_name() override { return "GeneratorNode"; }

  void InputReceived(ExecNode*, int, compute::ExecBatch) override { DCHECK(false); }
  void ErrorReceived(ExecNode*, Status) override { DCHECK(false); }
  void InputFinished(ExecNode*, int) override { DCHECK(false); }

  Status StartProducing() override {
    if (!generator_) {
      return Status::Invalid("Restarted GeneratorNode '", label(), "'");
    }
    GenerateOne(std::unique_lock<std::mutex>{mutex_});
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    std::unique_lock<std::mutex> lock(mutex_);
    generator_ = nullptr;  // null function
  }

  void StopProducing() override { StopProducing(outputs_[0]); }

 private:
  void GenerateOne(std::unique_lock<std::mutex>&& lock) {
    if (!generator_) {
      // Stopped
      return;
    }

    auto fut = generator_();
    const auto batch_index = next_batch_index_++;

    lock.unlock();
    fut.AddCallback([batch_index, this](const Result<util::optional<ExecBatch>>& res) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!res.ok()) {
        for (auto out : outputs_) {
          out->ErrorReceived(this, res.status());
        }
        return;
      }

      const auto& batch = *res;
      if (IsIterationEnd(batch)) {
        lock.unlock();
        for (auto out : outputs_) {
          out->InputFinished(this, batch_index);
        }
        return;
      }

      lock.unlock();
      for (auto out : outputs_) {
        out->InputReceived(this, batch_index, compute::ExecBatch(*batch));
      }
      lock.lock();

      GenerateOne(std::move(lock));
    });
  }

  std::mutex mutex_;
  AsyncGenerator<util::optional<ExecBatch>> generator_;
  int next_batch_index_ = 0;
};

ExecNode* MakeSourceNode(ExecPlan* plan, std::string label,
                         ExecNode::BatchDescr output_descr,
                         AsyncGenerator<util::optional<ExecBatch>> generator) {
  return plan->EmplaceNode<GeneratorNode>(plan, std::move(label), std::move(output_descr),
                                          std::move(generator));
}

struct FilterNode : ExecNode {
  FilterNode(ExecNode* input, std::string label, Expression filter)
      : ExecNode(input->plan(), std::move(label), {input}, {"target"},
                 /*output_descr=*/{input->output_descr()},
                 /*num_outputs=*/1),
        filter_(std::move(filter)) {}

  const char* kind_name() override { return "FilterNode"; }

  Result<ExecBatch> DoFilter(const ExecBatch& target) {
    // XXX get a non-default exec context
    ARROW_ASSIGN_OR_RAISE(Datum mask, ExecuteScalarExpression(filter_, target));

    if (mask.is_scalar()) {
      const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
      if (mask_scalar.is_valid && mask_scalar.value) {
        return target;
      }

      return target.Slice(0, 0);
    }

    auto values = target.values;
    for (auto& value : values) {
      if (value.is_scalar()) continue;
      ARROW_ASSIGN_OR_RAISE(value, Filter(value, mask, FilterOptions::Defaults()));
    }
    return ExecBatch::Make(std::move(values));
  }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    auto maybe_filtered = DoFilter(std::move(batch));
    if (!maybe_filtered.ok()) {
      outputs_[0]->ErrorReceived(this, maybe_filtered.status());
      inputs_[0]->StopProducing(this);
      return;
    }

    outputs_[0]->InputReceived(this, seq, maybe_filtered.MoveValueUnsafe());
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
    inputs_[0]->StopProducing(this);
  }

  void InputFinished(ExecNode* input, int seq) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->InputFinished(this, seq);
    inputs_[0]->StopProducing(this);
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    inputs_[0]->StopProducing(this);
  }

  void StopProducing() override { StopProducing(outputs_[0]); }

 private:
  Expression filter_;
};

ExecNode* MakeFilterNode(ExecNode* input, std::string label, Expression filter) {
  return input->plan()->EmplaceNode<FilterNode>(input, std::move(label),
                                                std::move(filter));
}

struct SinkNode : ExecNode {
  SinkNode(ExecNode* input, std::string label,
           AsyncGenerator<util::optional<ExecBatch>>* generator)
      : ExecNode(input->plan(), std::move(label), {input}, {"collected"}, {},
                 /*num_outputs=*/0),
        producer_(MakeProducer(generator)) {}

  static PushGenerator<util::optional<ExecBatch>>::Producer MakeProducer(
      AsyncGenerator<util::optional<ExecBatch>>* out_gen) {
    PushGenerator<util::optional<ExecBatch>> gen;
    auto out = gen.producer();
    *out_gen = std::move(gen);
    return out;
  }

  const char* kind_name() override { return "SinkNode"; }

  Status StartProducing() override { return Status::OK(); }

  // sink nodes have no outputs from which to feel backpressure
  static void NoOutputs() { DCHECK(false) << "no outputs; this should never be called"; }
  void ResumeProducing(ExecNode* output) override { NoOutputs(); }
  void PauseProducing(ExecNode* output) override { NoOutputs(); }
  void StopProducing(ExecNode* output) override { NoOutputs(); }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    StopProducingUnlocked();
  }

  void InputReceived(ExecNode* input, int seq_num, ExecBatch exec_batch) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) return;

    // TODO would be nice to factor this out in a ReorderQueue
    if (seq_num <= static_cast<int>(received_batches_.size())) {
      received_batches_.resize(seq_num + 1);
      emitted_.resize(seq_num + 1, false);
    }
    received_batches_[seq_num] = std::move(exec_batch);
    ++num_received_;

    if (seq_num != num_emitted_) {
      // Cannot emit yet as there is a hole at `num_emitted_`
      DCHECK_GT(seq_num, num_emitted_);
      return;
    }

    if (num_received_ == emit_stop_) {
      StopProducingUnlocked();
    }

    // Emit batches in order as far as possible
    // First collect these batches, then unlock before producing.
    const auto seq_start = seq_num;
    while (seq_num < static_cast<int>(emitted_.size()) && !emitted_[seq_num]) {
      emitted_[seq_num] = true;
      ++seq_num;
    }
    DCHECK_GT(seq_num, seq_start);
    // By moving the values now, we make sure another thread won't emit the same values
    // below
    std::vector<ExecBatch> to_emit(
        std::make_move_iterator(received_batches_.begin() + seq_start),
        std::make_move_iterator(received_batches_.begin() + seq_num));

    lock.unlock();
    for (auto&& batch : to_emit) {
      producer_.Push(std::move(batch));
    }
    lock.lock();

    DCHECK_EQ(seq_start, num_emitted_);  // num_emitted_ wasn't bumped in the meantime
    num_emitted_ = seq_num;
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    // XXX do we care about properly sequencing the error?
    producer_.Push(std::move(error));
    std::unique_lock<std::mutex> lock(mutex_);
    StopProducingUnlocked();
  }

  void InputFinished(ExecNode* input, int seq_stop) override {
    std::unique_lock<std::mutex> lock(mutex_);
    DCHECK_GE(seq_stop, static_cast<int>(received_batches_.size()));
    received_batches_.reserve(seq_stop);
    emit_stop_ = seq_stop;
    if (emit_stop_ == num_received_) {
      DCHECK_EQ(emit_stop_, num_emitted_);
      StopProducingUnlocked();
    }
  }

 private:
  void StopProducingUnlocked() {
    if (!stopped_) {
      stopped_ = true;
      producer_.Close();
      inputs_[0]->StopProducing(this);
    }
  }

  std::mutex mutex_;
  std::vector<ExecBatch> received_batches_;
  std::vector<bool> emitted_;

  int num_received_ = 0;
  int num_emitted_ = 0;
  int emit_stop_ = -1;
  bool stopped_ = false;

  PushGenerator<util::optional<ExecBatch>>::Producer producer_;
};

AsyncGenerator<util::optional<ExecBatch>> MakeSinkNode(ExecNode* input,
                                                       std::string label);

}  // namespace compute
}  // namespace arrow

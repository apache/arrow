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

#include <unordered_set>

#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace engine {

namespace {

struct ExecPlanImpl : public ExecPlan {
  ExecPlanImpl() = default;

  ~ExecPlanImpl() = default;

  void AddNode(std::unique_ptr<ExecNode> node) {
    if (node->num_inputs() == 0) {
      sources_.push_back(node.get());
    }
    if (node->num_outputs() == 0) {
      sinks_.push_back(node.get());
    }
    nodes_.push_back(std::move(node));
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
    struct ReverseTopoSort {
      const std::vector<std::unique_ptr<ExecNode>>& nodes;
      std::unordered_set<ExecNode*> visited;
      std::unordered_set<ExecNode*> visiting;
      NodeVector sorted;

      explicit ReverseTopoSort(const std::vector<std::unique_ptr<ExecNode>>& nodes)
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
        DCHECK_EQ(visiting.size(), 0);
        return Status::OK();
      }

      Status Visit(ExecNode* node) {
        if (visited.count(node) != 0) {
          return Status::OK();
        }
        if (!visiting.insert(node).second) {
          // Insertion failed => node is already being visited
          return Status::Invalid("Cycle detected in execution plan");
        }
        for (const auto& out : node->outputs()) {
          RETURN_NOT_OK(Visit(out.output));
        }
        visiting.erase(node);
        visited.insert(node);
        sorted.push_back(node);
        return Status::OK();
      }
    } topo_sort(nodes_);

    RETURN_NOT_OK(topo_sort.Sort());
    return std::move(topo_sort.sorted);
  }

  std::vector<std::unique_ptr<ExecNode>> nodes_;
  NodeVector sources_;
  NodeVector sinks_;
};

ExecPlanImpl* ToDerived(ExecPlan* ptr) { return checked_cast<ExecPlanImpl*>(ptr); }

const ExecPlanImpl* ToDerived(const ExecPlan* ptr) {
  return checked_cast<const ExecPlanImpl*>(ptr);
}

}  // namespace

Result<std::shared_ptr<ExecPlan>> ExecPlan::Make() {
  return std::make_shared<ExecPlanImpl>();
}

void ExecPlan::AddNode(std::unique_ptr<ExecNode> node) {
  ToDerived(this)->AddNode(std::move(node));
}

const ExecPlan::NodeVector& ExecPlan::sources() const {
  return ToDerived(this)->sources_;
}

const ExecPlan::NodeVector& ExecPlan::sinks() const { return ToDerived(this)->sinks_; }

Status ExecPlan::Validate() { return ToDerived(this)->Validate(); }

Status ExecPlan::StartProducing() { return ToDerived(this)->StartProducing(); }

ExecNode::~ExecNode() = default;

ExecNode::ExecNode(ExecPlan* plan, std::string label)
    : plan_(plan), label_(std::move(label)) {}

Status ExecNode::Validate() const {
  if (inputs_.size() != static_cast<size_t>(num_inputs())) {
    return Status::Invalid("Invalid number of inputs for '", label(), "' (expected ",
                           num_inputs(), ", actual ", inputs_.size(), ")");
  }
  if (input_descrs_.size() != static_cast<size_t>(num_inputs())) {
    return Status::Invalid("Invalid number of input descrs for '", label(),
                           "' (expected ", num_inputs(), ", actual ",
                           input_descrs_.size(), ")");
  }
  if (outputs_.size() != static_cast<size_t>(num_outputs())) {
    return Status::Invalid("Invalid number of outputs for '", label(), "' (expected ",
                           num_outputs(), ", actual ", outputs_.size(), ")");
  }
  if (output_descrs_.size() != static_cast<size_t>(num_outputs())) {
    return Status::Invalid("Invalid number of output descrs for '", label(),
                           "' (expected ", num_outputs(), ", actual ",
                           output_descrs_.size(), ")");
  }
  for (size_t i = 0; i < outputs_.size(); ++i) {
    const auto& out = outputs_[i];
    if (out.input_index >= static_cast<int>(out.output->inputs_.size()) ||
        out.input_index >= static_cast<int>(out.output->input_descrs_.size()) ||
        this != out.output->inputs_[out.input_index]) {
      return Status::Invalid("Output node configuration for '", label(),
                             "' inconsistent with input node configuration for '",
                             out.output->label(), "'");
    }
    const auto& out_descr = output_descrs_[i];
    const auto& in_descr = out.output->input_descrs_[out.input_index];
    if (in_descr != out_descr) {
      return Status::Invalid(
          "Output node produces batches with type '", ValueDescr::ToString(out_descr),
          "' inconsistent with input node configuration for '", out.output->label(), "'");
    }
  }
  return Status::OK();
}

void ExecNode::PauseProducing() {
  for (const auto& node : inputs_) {
    node->PauseProducing();
  }
}

void ExecNode::ResumeProducing() {
  for (const auto& node : inputs_) {
    node->ResumeProducing();
  }
}

}  // namespace engine
}  // namespace arrow

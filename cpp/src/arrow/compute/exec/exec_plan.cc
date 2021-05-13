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
      std::unordered_set<ExecNode*> visiting;
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
        DCHECK_EQ(visiting.size(), 0);
        return Status::OK();
      }

      Status Visit(ExecNode* node) {
        if (visited.count(node) != 0) {
          return Status::OK();
        }

        auto it_success = visiting.insert(node);
        if (!it_success.second) {
          // Insertion failed => node is already being visited
          return Status::Invalid("Cycle detected in execution plan");
        }

        for (auto input : node->inputs()) {
          // Ensure that producers are inserted before this consumer
          RETURN_NOT_OK(Visit(input));
        }

        visiting.erase(it_success.first);
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

ExecNode::ExecNode(ExecPlan* plan, std::string label,
                   std::vector<BatchDescr> input_descrs,
                   std::vector<std::string> input_labels, BatchDescr output_descr,
                   int num_outputs)
    : plan_(plan),
      label_(std::move(label)),
      input_descrs_(std::move(input_descrs)),
      input_labels_(std::move(input_labels)),
      output_descr_(std::move(output_descr)),
      num_outputs_(num_outputs) {}

Status ExecNode::Validate() const {
  if (inputs_.size() != input_descrs_.size()) {
    return Status::Invalid("Invalid number of inputs for '", label(), "' (expected ",
                           num_inputs(), ", actual ", inputs_.size(), ")");
  }

  if (static_cast<int>(outputs_.size()) != num_outputs_) {
    return Status::Invalid("Invalid number of outputs for '", label(), "' (expected ",
                           num_outputs(), ", actual ", outputs_.size(), ")");
  }

  DCHECK_EQ(input_descrs_.size(), input_labels_.size());

  for (auto out : outputs_) {
    auto input_index = GetNodeIndex(out->inputs(), this);
    if (!input_index) {
      return Status::Invalid("Node '", label(), "' outputs to node '", out->label(),
                             "' but is not listed as an input.");
    }

    const auto& in_descr = out->input_descrs_[*input_index];
    if (in_descr != output_descr_) {
      return Status::Invalid(
          "Node '", label(), "' (bound to input ", input_labels_[*input_index],
          ") produces batches with type '", ValueDescr::ToString(output_descr_),
          "' inconsistent with consumer '", out->label(), "' which accepts '",
          ValueDescr::ToString(in_descr), "'");
    }
  }

  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

// NOTES:
// - ExecBatches only have arrays or scalars
// - data streams may be ordered, so add input number?
// - node to combine input needs to reorder

namespace arrow {
namespace compute {

class ExecNode;

class ARROW_EXPORT ExecPlan : public std::enable_shared_from_this<ExecPlan> {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecPlan() = default;

  /// Make an empty exec plan
  static Result<std::shared_ptr<ExecPlan>> Make();

  ExecNode* AddNode(std::unique_ptr<ExecNode> node);

  template <typename Node, typename... Args>
  ExecNode* EmplaceNode(Args&&... args) {
    return AddNode(std::unique_ptr<Node>(new Node{std::forward<Args>(args)...}));
  }

  /// The initial inputs
  const NodeVector& sources() const;

  /// The final outputs
  const NodeVector& sinks() const;

  // XXX API question:
  // There are clearly two phases in the ExecPlan lifecycle:
  // - one construction phase where AddNode() and ExecNode::AddInput() is called
  //   (with optional validation at the end)
  // - one execution phase where the nodes are topo-sorted and then started
  //
  // => Should we separate out those APIs? e.g. have a ExecPlanBuilder
  // for the first phase.

  Status Validate();

  /// Start producing on all nodes
  ///
  /// Nodes are started in reverse topological order, such that any node
  /// is started before all of its inputs.
  Status StartProducing();

  // XXX should we also have `void StopProducing()`?

 protected:
  ExecPlan() = default;
};

class ARROW_EXPORT ExecNode {
 public:
  using NodeVector = std::vector<ExecNode*>;
  using BatchDescr = std::vector<ValueDescr>;

  virtual ~ExecNode() = default;

  virtual const char* kind_name() = 0;

  // The number of inputs/outputs expected by this node
  int num_inputs() const { return static_cast<int>(input_descrs_.size()); }
  int num_outputs() const { return num_outputs_; }

  /// This node's predecessors in the exec plan
  const NodeVector& inputs() const { return inputs_; }

  /// The datatypes accepted by this node for each input
  const std::vector<BatchDescr>& input_descrs() const { return input_descrs_; }

  /// \brief Labels identifying the function of each input.
  ///
  /// For example, FilterNode accepts "target" and "filter" inputs.
  const std::vector<std::string>& input_labels() const { return input_labels_; }

  /// This node's successors in the exec plan
  const NodeVector& outputs() const { return outputs_; }

  /// The datatypes for batches produced by this node
  const BatchDescr& output_descr() const { return output_descr_; }

  /// This node's exec plan
  ExecPlan* plan() { return plan_; }

  /// \brief An optional label, for display and debugging
  ///
  /// There is no guarantee that this value is non-empty or unique.
  const std::string& label() const { return label_; }

  void AddInput(ExecNode* input) {
    inputs_.push_back(input);
    input->outputs_.push_back(this);
  }

  Status Validate() const;

  /// Upstream API:
  /// These functions are called by input nodes that want to inform this node
  /// about an updated condition (a new input batch, an error, an impeding
  /// end of stream).
  ///
  /// Implementation rules:
  /// - these may be called anytime after StartProducing() has succeeded
  ///   (and even during or after StopProducing())
  /// - these may be called concurrently
  /// - these are allowed to call back into PauseProducing(), ResumeProducing()
  ///   and StopProducing()

  /// Transfer input batch to ExecNode
  virtual void InputReceived(ExecNode* input, int seq_num, compute::ExecBatch batch) = 0;

  /// Signal error to ExecNode
  virtual void ErrorReceived(ExecNode* input, Status error) = 0;

  /// Mark the inputs finished after the given number of batches.
  ///
  /// This may be called before all inputs are received.  This simply fixes
  /// the total number of incoming batches for an input, so that the ExecNode
  /// knows when it has received all input, regardless of order.
  virtual void InputFinished(ExecNode* input, int seq_stop) = 0;

  /// Lifecycle API:
  /// - start / stop to initiate and terminate production
  /// - pause / resume to apply backpressure
  ///
  /// Implementation rules:
  /// - StartProducing() should not recurse into the inputs, as it is
  ///   handled by ExecPlan::StartProducing()
  /// - PauseProducing(), ResumeProducing(), StopProducing() may be called
  ///   concurrently (but only after StartProducing() has returned successfully)
  /// - PauseProducing(), ResumeProducing(), StopProducing() may be called
  ///   by the downstream nodes' InputReceived(), ErrorReceived(), InputFinished()
  ///   methods
  /// - StopProducing() should recurse into the inputs
  /// - StopProducing() must be idempotent

  // XXX What happens if StartProducing() calls an output's InputReceived()
  // synchronously, and InputReceived() decides to call back into StopProducing()
  // (or PauseProducing()) because it received enough data?
  //
  // Right now, since synchronous calls happen in both directions (input to
  // output and then output to input), a node must be careful to be reentrant
  // against synchronous calls from its output, *and* also concurrent calls from
  // other threads.  The most reliable solution is to update the internal state
  // first, and notify outputs only at the end.
  //
  // Alternate rules:
  // - StartProducing(), ResumeProducing() can call synchronously into
  //   its ouputs' consuming methods (InputReceived() etc.)
  // - InputReceived(), ErrorReceived(), InputFinished() can call asynchronously
  //   into its inputs' PauseProducing(), StopProducing()
  //
  // Alternate API:
  // - InputReceived(), ErrorReceived(), InputFinished() return a ProductionHint
  //   enum: either None (default), PauseProducing, ResumeProducing, StopProducing
  // - A method allows passing a ProductionHint asynchronously from an output node
  //   (replacing PauseProducing(), ResumeProducing(), StopProducing())

  /// \brief Start producing
  ///
  /// This must only be called once.  If this fails, then other lifecycle
  /// methods must not be called.
  ///
  /// This is typically called automatically by ExecPlan::StartProducing().
  virtual Status StartProducing() = 0;

  /// \brief Pause producing temporarily
  ///
  /// This call is a hint that an output node is currently not willing
  /// to receive data.
  ///
  /// This may be called any number of times after StartProducing() succeeds.
  /// However, the node is still free to produce data (which may be difficult
  /// to prevent anyway if data is produced using multiple threads).
  virtual void PauseProducing(ExecNode* output) = 0;

  /// \brief Resume producing after a temporary pause
  ///
  /// This call is a hint that an output node is willing to receive data again.
  ///
  /// This may be called any number of times after StartProducing() succeeds.
  /// This may also be called concurrently with PauseProducing(), which suggests
  /// the implementation may use an atomic counter.
  virtual void ResumeProducing(ExecNode* output) = 0;

  /// \brief Stop producing definitively to a single output
  ///
  /// This call is a hint that an output node has completed and is not willing
  /// to not receive any further data.
  virtual void StopProducing(ExecNode* output) = 0;

  /// \brief Stop producing definitively
  virtual void StopProducing() = 0;

 protected:
  ExecNode(ExecPlan* plan, std::string label, std::vector<BatchDescr> input_descrs,
           std::vector<std::string> input_labels, BatchDescr output_descr,
           int num_outputs);

  ExecPlan* plan_;

  std::string label_;

  std::vector<BatchDescr> input_descrs_;
  std::vector<std::string> input_labels_;
  NodeVector inputs_;

  BatchDescr output_descr_;
  int num_outputs_;
  NodeVector outputs_;
};

}  // namespace compute
}  // namespace arrow

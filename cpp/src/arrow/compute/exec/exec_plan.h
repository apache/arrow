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

  void AddNode(std::unique_ptr<ExecNode> node);

  /// The initial inputs
  const NodeVector& sources() const;

  /// The final outputs
  const NodeVector& sinks() const;

  // XXX API question:
  // There are clearly two phases in the ExecPlan lifecycle:
  // - one construction phase where AddNode() and ExecNode::Bind() is called
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
  struct OutputNode {
    ExecNode* output;
    // Index of corresponding input in `output` node
    int input_index;
  };

  using NodeVector = std::vector<ExecNode*>;
  using OutputNodeVector = std::vector<OutputNode>;
  using BatchDescr = std::vector<ValueDescr>;

  virtual ~ExecNode();

  virtual const char* kind_name() = 0;
  // The number of inputs and outputs expected by this node
  // XXX should these simply return `input_descrs_.size()`
  // (`output_descrs_.size()` respectively)?
  virtual int num_inputs() const = 0;
  virtual int num_outputs() const = 0;

  /// This node's predecessors in the exec plan
  const NodeVector& inputs() const { return inputs_; }

  /// The datatypes for each input
  // XXX Should it be std::vector<DataType>?
  const std::vector<BatchDescr>& input_descrs() const { return input_descrs_; }

  /// This node's successors in the exec plan
  const OutputNodeVector& outputs() const { return outputs_; }

  /// The datatypes for each output
  // XXX Should it be std::vector<DataType>?
  const std::vector<BatchDescr>& output_descrs() const { return output_descrs_; }

  /// This node's exec plan
  ExecPlan* plan() { return plan_; }
  std::shared_ptr<ExecPlan> plan_ref() { return plan_->shared_from_this(); }

  /// \brief An optional label, for display and debugging
  ///
  /// There is no guarantee that this value is non-empty or unique.
  const std::string& label() const { return label_; }

  int AddInput(ExecNode* node) {
    inputs_.push_back(node);
    return static_cast<int>(inputs_.size() - 1);
  }

  void AddOutput(ExecNode* node, int input_index) {
    outputs_.push_back({node, input_index});
  }

  static void Bind(ExecNode* input, ExecNode* output) {
    input->AddOutput(output, output->AddInput(input));
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
  virtual void InputReceived(int input_index, int seq_num, compute::ExecBatch batch) = 0;

  /// Signal error to ExecNode
  virtual void ErrorReceived(int input_index, Status error) = 0;

  /// Mark the inputs finished after the given number of batches.
  ///
  /// This may be called before all inputs are received.  This simply fixes
  /// the total number of incoming batches for an input, so that the ExecNode
  /// knows when it has received all input, regardless of order.
  virtual void InputFinished(int input_index, int seq_stop) = 0;

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

  // TODO PauseProducing() etc. should probably take the index of the output which calls
  // them?

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
  /// to prevent anyway if data is producer using multiple threads).
  virtual void PauseProducing();

  /// \brief Resume producing after a temporary pause
  ///
  /// This call is a hint that an output node is willing to receive data again.
  ///
  /// This may be called any number of times after StartProducing() succeeds.
  /// This may also be called concurrently with PauseProducing(), which suggests
  /// the implementation may use an atomic counter.
  virtual void ResumeProducing();

  /// \brief Stop producing definitively
  virtual void StopProducing() = 0;

 protected:
  ExecNode(ExecPlan* plan, std::string label);

  ExecPlan* plan_;
  std::string label_;
  NodeVector inputs_;
  OutputNodeVector outputs_;
  std::vector<BatchDescr> input_descrs_;
  std::vector<BatchDescr> output_descrs_;
};

}  // namespace compute
}  // namespace arrow

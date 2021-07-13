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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class ARROW_EXPORT ExecPlan : public std::enable_shared_from_this<ExecPlan> {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecPlan() = default;

  ExecContext* exec_context() const { return exec_context_; }

  /// Make an empty exec plan
  static Result<std::shared_ptr<ExecPlan>> Make(ExecContext* = default_exec_context());

  ExecNode* AddNode(std::unique_ptr<ExecNode> node);

  template <typename Node, typename... Args>
  Node* EmplaceNode(Args&&... args) {
    std::unique_ptr<Node> node{new Node{std::forward<Args>(args)...}};
    auto out = node.get();
    AddNode(std::move(node));
    return out;
  }

  /// The initial inputs
  const NodeVector& sources() const;

  /// The final outputs
  const NodeVector& sinks() const;

  Status Validate();

  /// \brief Start producing on all nodes
  ///
  /// Nodes are started in reverse topological order, such that any node
  /// is started before all of its inputs.
  Status StartProducing();

  /// \brief Stop producing on all nodes
  ///
  /// Nodes are stopped in topological order, such that any node
  /// is stopped before all of its outputs.
  void StopProducing();

  /// \brief A future which will be marked finished when all nodes have stopped producing.
  Future<> finished();

 protected:
  ExecContext* exec_context_;
  explicit ExecPlan(ExecContext* exec_context) : exec_context_(exec_context) {}
};

class ARROW_EXPORT ExecNode {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecNode() = default;

  virtual const char* kind_name() = 0;

  // The number of inputs/outputs expected by this node
  int num_inputs() const { return static_cast<int>(inputs_.size()); }
  int num_outputs() const { return num_outputs_; }

  /// This node's predecessors in the exec plan
  const NodeVector& inputs() const { return inputs_; }

  /// \brief Labels identifying the function of each input.
  const std::vector<std::string>& input_labels() const { return input_labels_; }

  /// This node's successors in the exec plan
  const NodeVector& outputs() const { return outputs_; }

  /// The datatypes for batches produced by this node
  const std::shared_ptr<Schema>& output_schema() const { return output_schema_; }

  /// This node's exec plan
  ExecPlan* plan() { return plan_; }

  /// \brief An optional label, for display and debugging
  ///
  /// There is no guarantee that this value is non-empty or unique.
  const std::string& label() const { return label_; }

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
  virtual void InputReceived(ExecNode* input, int seq_num, ExecBatch batch) = 0;

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
  /// to receive any further data.
  virtual void StopProducing(ExecNode* output) = 0;

  /// \brief Stop producing definitively to all outputs
  virtual void StopProducing() = 0;

  /// \brief A future which will be marked finished when this node has stopped producing.
  virtual Future<> finished() = 0;

 protected:
  ExecNode(ExecPlan* plan, std::string label, NodeVector inputs,
           std::vector<std::string> input_labels, std::shared_ptr<Schema> output_schema,
           int num_outputs);

  ExecPlan* plan_;
  std::string label_;

  NodeVector inputs_;
  std::vector<std::string> input_labels_;

  std::shared_ptr<Schema> output_schema_;
  int num_outputs_;
  NodeVector outputs_;
};

/// \brief Adapt an AsyncGenerator<ExecBatch> as a source node
///
/// plan->exec_context()->executor() is used to parallelize pushing to
/// outputs, if provided.
ARROW_EXPORT
ExecNode* MakeSourceNode(ExecPlan* plan, std::string label,
                         std::shared_ptr<Schema> output_schema,
                         std::function<Future<util::optional<ExecBatch>>()>);

/// \brief Add a sink node which forwards to an AsyncGenerator<ExecBatch>
///
/// Emitted batches will not be ordered.
ARROW_EXPORT
std::function<Future<util::optional<ExecBatch>>()> MakeSinkNode(ExecNode* input,
                                                                std::string label);

/// \brief Wrap an ExecBatch generator in a RecordBatchReader.
///
/// The RecordBatchReader does not impose any ordering on emitted batches.
ARROW_EXPORT
std::shared_ptr<RecordBatchReader> MakeGeneratorReader(
    std::shared_ptr<Schema>, std::function<Future<util::optional<ExecBatch>>()>,
    MemoryPool*);

/// \brief Make a node which excludes some rows from batches passed through it
///
/// The filter Expression will be evaluated against each batch which is pushed to
/// this node. Any rows for which the filter does not evaluate to `true` will be excluded
/// in the batch emitted by this node.
///
/// If the filter is not already bound, it will be bound against the input's schema.
ARROW_EXPORT
Result<ExecNode*> MakeFilterNode(ExecNode* input, std::string label, Expression filter);

/// \brief Make a node which executes expressions on input batches, producing new batches.
///
/// Each expression will be evaluated against each batch which is pushed to
/// this node to produce a corresponding output column.
///
/// If exprs are not already bound, they will be bound against the input's schema.
/// If names are not provided, the string representations of exprs will be used.
ARROW_EXPORT
Result<ExecNode*> MakeProjectNode(ExecNode* input, std::string label,
                                  std::vector<Expression> exprs,
                                  std::vector<std::string> names = {});

ARROW_EXPORT
Result<ExecNode*> MakeScalarAggregateNode(ExecNode* input, std::string label,
                                          std::vector<internal::Aggregate> aggregates);

}  // namespace compute
}  // namespace arrow

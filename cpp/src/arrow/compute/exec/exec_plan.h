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

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/async_util.h"
#include "arrow/util/cancel.h"
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

  std::string ToString() const;

 protected:
  ExecContext* exec_context_;
  explicit ExecPlan(ExecContext* exec_context) : exec_context_(exec_context) {}
};

class ARROW_EXPORT ExecNode {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecNode() = default;

  virtual const char* kind_name() const = 0;

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
  void SetLabel(std::string label) { label_ = std::move(label); }

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
  virtual void InputReceived(ExecNode* input, ExecBatch batch) = 0;

  /// Signal error to ExecNode
  virtual void ErrorReceived(ExecNode* input, Status error) = 0;

  /// Mark the inputs finished after the given number of batches.
  ///
  /// This may be called before all inputs are received.  This simply fixes
  /// the total number of incoming batches for an input, so that the ExecNode
  /// knows when it has received all input, regardless of order.
  virtual void InputFinished(ExecNode* input, int total_batches) = 0;

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

  std::string ToString() const;

 protected:
  ExecNode(ExecPlan* plan, NodeVector inputs, std::vector<std::string> input_labels,
           std::shared_ptr<Schema> output_schema, int num_outputs);

  // A helper method to send an error status to all outputs.
  // Returns true if the status was an error.
  bool ErrorIfNotOk(Status status);

  /// Provide extra info to include in the string representation.
  virtual std::string ToStringExtra() const;

  ExecPlan* plan_;
  std::string label_;

  NodeVector inputs_;
  std::vector<std::string> input_labels_;

  std::shared_ptr<Schema> output_schema_;
  int num_outputs_;
  NodeVector outputs_;
};

/// \brief MapNode is an ExecNode type class which process a task like filter/project
/// (See SubmitTask method) to each given ExecBatch object, which have one input, one
/// output, and are pure functions on the input
///
/// A simple parallel runner is created with a "map_fn" which is just a function that
/// takes a batch in and returns a batch.  This simple parallel runner also needs an
/// executor (use simple synchronous runner if there is no executor)

class MapNode : public ExecNode {
 public:
  MapNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
          std::shared_ptr<Schema> output_schema, bool async_mode);

  void ErrorReceived(ExecNode* input, Status error) override;

  void InputFinished(ExecNode* input, int total_batches) override;

  Status StartProducing() override;

  void PauseProducing(ExecNode* output) override;

  void ResumeProducing(ExecNode* output) override;

  void StopProducing(ExecNode* output) override;

  void StopProducing() override;

  Future<> finished() override;

 protected:
  void SubmitTask(std::function<Result<ExecBatch>(ExecBatch)> map_fn, ExecBatch batch);

  void Finish(Status finish_st = Status::OK());

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
};

/// \brief An extensible registry for factories of ExecNodes
class ARROW_EXPORT ExecFactoryRegistry {
 public:
  using Factory = std::function<Result<ExecNode*>(ExecPlan*, std::vector<ExecNode*>,
                                                  const ExecNodeOptions&)>;

  virtual ~ExecFactoryRegistry() = default;

  /// \brief Get the named factory from this registry
  ///
  /// will raise if factory_name is not found
  virtual Result<Factory> GetFactory(const std::string& factory_name) = 0;

  /// \brief Add a factory to this registry with the provided name
  ///
  /// will raise if factory_name is already in the registry
  virtual Status AddFactory(std::string factory_name, Factory factory) = 0;
};

/// The default registry, which includes built-in factories.
ARROW_EXPORT
ExecFactoryRegistry* default_exec_factory_registry();

/// \brief Construct an ExecNode using the named factory
inline Result<ExecNode*> MakeExecNode(
    const std::string& factory_name, ExecPlan* plan, std::vector<ExecNode*> inputs,
    const ExecNodeOptions& options,
    ExecFactoryRegistry* registry = default_exec_factory_registry()) {
  ARROW_ASSIGN_OR_RAISE(auto factory, registry->GetFactory(factory_name));
  return factory(plan, std::move(inputs), options);
}

/// \brief Helper class for declaring sets of ExecNodes efficiently
///
/// A Declaration represents an unconstructed ExecNode (and potentially more since its
/// inputs may also be Declarations). The node can be constructed and added to a plan
/// with Declaration::AddToPlan, which will recursively construct any inputs as necessary.
struct ARROW_EXPORT Declaration {
  using Input = util::Variant<ExecNode*, Declaration>;

  Declaration(std::string factory_name, std::vector<Input> inputs,
              std::shared_ptr<ExecNodeOptions> options, std::string label)
      : factory_name{std::move(factory_name)},
        inputs{std::move(inputs)},
        options{std::move(options)},
        label{std::move(label)} {}

  template <typename Options>
  Declaration(std::string factory_name, std::vector<Input> inputs, Options options)
      : factory_name{std::move(factory_name)},
        inputs{std::move(inputs)},
        options{std::make_shared<Options>(std::move(options))},
        label{this->factory_name} {}

  template <typename Options>
  Declaration(std::string factory_name, Options options)
      : factory_name{std::move(factory_name)},
        inputs{},
        options{std::make_shared<Options>(std::move(options))},
        label{this->factory_name} {}

  /// \brief Convenience factory for the common case of a simple sequence of nodes.
  ///
  /// Each of decls will be appended to the inputs of the subsequent declaration,
  /// and the final modified declaration will be returned.
  ///
  /// Without this convenience factory, constructing a sequence would require explicit,
  /// difficult-to-read nesting:
  ///
  ///     Declaration{"n3",
  ///                   {
  ///                       Declaration{"n2",
  ///                                   {
  ///                                       Declaration{"n1",
  ///                                                   {
  ///                                                       Declaration{"n0", N0Opts{}},
  ///                                                   },
  ///                                                   N1Opts{}},
  ///                                   },
  ///                                   N2Opts{}},
  ///                   },
  ///                   N3Opts{}};
  ///
  /// An equivalent Declaration can be constructed more tersely using Sequence:
  ///
  ///     Declaration::Sequence({
  ///         {"n0", N0Opts{}},
  ///         {"n1", N1Opts{}},
  ///         {"n2", N2Opts{}},
  ///         {"n3", N3Opts{}},
  ///     });
  static Declaration Sequence(std::vector<Declaration> decls);

  Result<ExecNode*> AddToPlan(ExecPlan* plan, ExecFactoryRegistry* registry =
                                                  default_exec_factory_registry()) const;

  std::string factory_name;
  std::vector<Input> inputs;
  std::shared_ptr<ExecNodeOptions> options;
  std::string label;
};

/// \brief Wrap an ExecBatch generator in a RecordBatchReader.
///
/// The RecordBatchReader does not impose any ordering on emitted batches.
ARROW_EXPORT
std::shared_ptr<RecordBatchReader> MakeGeneratorReader(
    std::shared_ptr<Schema>, std::function<Future<util::optional<ExecBatch>>()>,
    MemoryPool*);

constexpr int kDefaultBackgroundMaxQ = 32;
constexpr int kDefaultBackgroundQRestart = 16;

/// \brief Make a generator of RecordBatchReaders
///
/// Useful as a source node for an Exec plan
ARROW_EXPORT
Result<std::function<Future<util::optional<ExecBatch>>()>> MakeReaderGenerator(
    std::shared_ptr<RecordBatchReader> reader, arrow::internal::Executor* io_executor,
    int max_q = kDefaultBackgroundMaxQ, int q_restart = kDefaultBackgroundQRestart);

}  // namespace compute
}  // namespace arrow

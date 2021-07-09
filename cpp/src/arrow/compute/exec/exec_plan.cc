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
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

namespace {

struct ExecPlanImpl : public ExecPlan {
  explicit ExecPlanImpl(ExecContext* exec_context) : ExecPlan(exec_context) {}

  ~ExecPlanImpl() override {
    if (started_ && !finished_.is_finished()) {
      ARROW_LOG(WARNING) << "Plan was destroyed before finishing";
      StopProducing();
      finished().Wait();
    }
  }

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
    if (started_) {
      return Status::Invalid("restarted ExecPlan");
    }
    started_ = true;

    // producers precede consumers
    sorted_nodes_ = TopoSort();

    std::vector<Future<>> futures;

    Status st = Status::OK();

    using rev_it = std::reverse_iterator<NodeVector::iterator>;
    for (rev_it it(sorted_nodes_.end()), end(sorted_nodes_.begin()); it != end; ++it) {
      auto node = *it;

      st = node->StartProducing();
      if (!st.ok()) {
        // Stop nodes that successfully started, in reverse order
        stopped_ = true;
        StopProducingImpl(it.base(), sorted_nodes_.end());
        break;
      }

      futures.push_back(node->finished());
    }

    finished_ = AllComplete(std::move(futures));
    return st;
  }

  void StopProducing() {
    DCHECK(started_) << "stopped an ExecPlan which never started";
    stopped_ = true;

    StopProducingImpl(sorted_nodes_.begin(), sorted_nodes_.end());
  }

  template <typename It>
  void StopProducingImpl(It begin, It end) {
    for (auto it = begin; it != end; ++it) {
      auto node = *it;
      node->StopProducing();
    }
  }

  NodeVector TopoSort() {
    struct Impl {
      const std::vector<std::unique_ptr<ExecNode>>& nodes;
      std::unordered_set<ExecNode*> visited;
      NodeVector sorted;

      explicit Impl(const std::vector<std::unique_ptr<ExecNode>>& nodes) : nodes(nodes) {
        visited.reserve(nodes.size());
        sorted.resize(nodes.size());

        for (const auto& node : nodes) {
          Visit(node.get());
        }

        DCHECK_EQ(visited.size(), nodes.size());
      }

      void Visit(ExecNode* node) {
        if (visited.count(node) != 0) return;

        for (auto input : node->inputs()) {
          // Ensure that producers are inserted before this consumer
          Visit(input);
        }

        sorted[visited.size()] = node;
        visited.insert(node);
      }
    };

    return std::move(Impl{nodes_}.sorted);
  }

  Future<> finished_ = Future<>::MakeFinished();
  bool started_ = false, stopped_ = false;
  std::vector<std::unique_ptr<ExecNode>> nodes_;
  NodeVector sources_, sinks_;
  NodeVector sorted_nodes_;
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

Result<std::shared_ptr<ExecPlan>> ExecPlan::Make(ExecContext* ctx) {
  return std::shared_ptr<ExecPlan>(new ExecPlanImpl{ctx});
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

void ExecPlan::StopProducing() { ToDerived(this)->StopProducing(); }

Future<> ExecPlan::finished() { return ToDerived(this)->finished_; }

ExecNode::ExecNode(ExecPlan* plan, std::string label, NodeVector inputs,
                   std::vector<std::string> input_labels,
                   std::shared_ptr<Schema> output_schema, int num_outputs)
    : plan_(plan),
      label_(std::move(label)),
      inputs_(std::move(inputs)),
      input_labels_(std::move(input_labels)),
      output_schema_(std::move(output_schema)),
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

struct SourceNode : ExecNode {
  SourceNode(ExecPlan* plan, std::string label, std::shared_ptr<Schema> output_schema,
             AsyncGenerator<util::optional<ExecBatch>> generator)
      : ExecNode(plan, std::move(label), {}, {}, std::move(output_schema),
                 /*num_outputs=*/1),
        generator_(std::move(generator)) {}

  const char* kind_name() override { return "SourceNode"; }

  [[noreturn]] static void NoInputs() {
    DCHECK(false) << "no inputs; this should never be called";
    std::abort();
  }
  [[noreturn]] void InputReceived(ExecNode*, int, ExecBatch) override { NoInputs(); }
  [[noreturn]] void ErrorReceived(ExecNode*, Status) override { NoInputs(); }
  [[noreturn]] void InputFinished(ExecNode*, int) override { NoInputs(); }

  Status StartProducing() override {
    DCHECK(!stop_requested_) << "Restarted SourceNode";

    CallbackOptions options;
    if (auto executor = plan()->exec_context()->executor()) {
      // These options will transfer execution to the desired Executor if necessary.
      // This can happen for in-memory scans where batches didn't require
      // any CPU work to decode. Otherwise, parsing etc should have already
      // been placed us on the desired Executor and no queues will be pushed to.
      options.executor = executor;
      options.should_schedule = ShouldSchedule::IfDifferentExecutor;
    }

    finished_ = Loop([this, options] {
                  std::unique_lock<std::mutex> lock(mutex_);
                  int seq = batch_count_++;
                  if (stop_requested_) {
                    return Future<ControlFlow<int>>::MakeFinished(Break(seq));
                  }
                  lock.unlock();

                  return generator_().Then(
                      [=](const util::optional<ExecBatch>& batch) -> ControlFlow<int> {
                        std::unique_lock<std::mutex> lock(mutex_);
                        if (IsIterationEnd(batch) || stop_requested_) {
                          stop_requested_ = true;
                          return Break(seq);
                        }
                        lock.unlock();

                        outputs_[0]->InputReceived(this, seq, *batch);
                        return Continue();
                      },
                      [=](const Status& error) -> ControlFlow<int> {
                        // NB: ErrorReceived is independent of InputFinished, but
                        // ErrorReceived will usually prompt StopProducing which will
                        // prompt InputFinished. ErrorReceived may still be called from a
                        // node which was requested to stop (indeed, the request to stop
                        // may prompt an error).
                        std::unique_lock<std::mutex> lock(mutex_);
                        stop_requested_ = true;
                        lock.unlock();
                        outputs_[0]->ErrorReceived(this, error);
                        return Break(seq);
                      },
                      options);
                }).Then([&](int seq) { outputs_[0]->InputFinished(this, seq); });

    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_requested_ = true;
  }

  Future<> finished() override { return finished_; }

 private:
  std::mutex mutex_;
  bool stop_requested_{false};
  int batch_count_{0};
  Future<> finished_ = Future<>::MakeFinished();
  AsyncGenerator<util::optional<ExecBatch>> generator_;
};

ExecNode* MakeSourceNode(ExecPlan* plan, std::string label,
                         std::shared_ptr<Schema> output_schema,
                         AsyncGenerator<util::optional<ExecBatch>> generator) {
  return plan->EmplaceNode<SourceNode>(plan, std::move(label), std::move(output_schema),
                                       std::move(generator));
}

struct FilterNode : ExecNode {
  FilterNode(ExecNode* input, std::string label, Expression filter)
      : ExecNode(input->plan(), std::move(label), {input}, {"target"},
                 /*output_schema=*/input->output_schema(),
                 /*num_outputs=*/1),
        filter_(std::move(filter)) {}

  const char* kind_name() override { return "FilterNode"; }

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

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    auto maybe_filtered = DoFilter(std::move(batch));
    if (!maybe_filtered.ok()) {
      outputs_[0]->ErrorReceived(this, maybe_filtered.status());
      return;
    }

    maybe_filtered->guarantee = batch.guarantee;
    outputs_[0]->InputReceived(this, seq, maybe_filtered.MoveValueUnsafe());
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int seq) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->InputFinished(this, seq);
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override { inputs_[0]->StopProducing(this); }

  Future<> finished() override { return inputs_[0]->finished(); }

 private:
  Expression filter_;
};

Result<ExecNode*> MakeFilterNode(ExecNode* input, std::string label, Expression filter) {
  if (!filter.IsBound()) {
    ARROW_ASSIGN_OR_RAISE(filter, filter.Bind(*input->output_schema()));
  }

  if (filter.type()->id() != Type::BOOL) {
    return Status::TypeError("Filter expression must evaluate to bool, but ",
                             filter.ToString(), " evaluates to ",
                             filter.type()->ToString());
  }

  return input->plan()->EmplaceNode<FilterNode>(input, std::move(label),
                                                std::move(filter));
}

struct ProjectNode : ExecNode {
  ProjectNode(ExecNode* input, std::string label, std::shared_ptr<Schema> output_schema,
              std::vector<Expression> exprs)
      : ExecNode(input->plan(), std::move(label), {input}, {"target"},
                 /*output_schema=*/std::move(output_schema),
                 /*num_outputs=*/1),
        exprs_(std::move(exprs)) {}

  const char* kind_name() override { return "ProjectNode"; }

  Result<ExecBatch> DoProject(const ExecBatch& target) {
    std::vector<Datum> values{exprs_.size()};
    for (size_t i = 0; i < exprs_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(Expression simplified_expr,
                            SimplifyWithGuarantee(exprs_[i], target.guarantee));

      ARROW_ASSIGN_OR_RAISE(values[i], ExecuteScalarExpression(simplified_expr, target,
                                                               plan()->exec_context()));
    }
    return ExecBatch{std::move(values), target.length};
  }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    auto maybe_projected = DoProject(std::move(batch));
    if (!maybe_projected.ok()) {
      outputs_[0]->ErrorReceived(this, maybe_projected.status());
      return;
    }

    maybe_projected->guarantee = batch.guarantee;
    outputs_[0]->InputReceived(this, seq, maybe_projected.MoveValueUnsafe());
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int seq) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->InputFinished(this, seq);
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override { inputs_[0]->StopProducing(this); }

  Future<> finished() override { return inputs_[0]->finished(); }

 private:
  std::vector<Expression> exprs_;
};

Result<ExecNode*> MakeProjectNode(ExecNode* input, std::string label,
                                  std::vector<Expression> exprs) {
  FieldVector fields(exprs.size());

  int i = 0;
  for (auto& expr : exprs) {
    if (!expr.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(expr, expr.Bind(*input->output_schema()));
    }
    fields[i] = field(expr.ToString(), expr.type());
    ++i;
  }

  return input->plan()->EmplaceNode<ProjectNode>(
      input, std::move(label), schema(std::move(fields)), std::move(exprs));
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

  Status StartProducing() override {
    finished_ = Future<>::Make();
    return Status::OK();
  }

  // sink nodes have no outputs from which to feel backpressure
  [[noreturn]] static void NoOutputs() {
    DCHECK(false) << "no outputs; this should never be called";
    std::abort();
  }
  [[noreturn]] void ResumeProducing(ExecNode* output) override { NoOutputs(); }
  [[noreturn]] void PauseProducing(ExecNode* output) override { NoOutputs(); }
  [[noreturn]] void StopProducing(ExecNode* output) override { NoOutputs(); }

  void StopProducing() override {
    Finish();
    inputs_[0]->StopProducing(this);
  }

  Future<> finished() override { return finished_; }

  void InputReceived(ExecNode* input, int seq_num, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    std::unique_lock<std::mutex> lock(mutex_);
    if (finished_.is_finished()) return;

    ++num_received_;
    if (num_received_ == emit_stop_) {
      lock.unlock();
      Finish();
      lock.lock();
    }

    if (emit_stop_ != -1) {
      DCHECK_LE(seq_num, emit_stop_);
    }
    lock.unlock();

    producer_.Push(std::move(batch));
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    producer_.Push(std::move(error));
    Finish();
    inputs_[0]->StopProducing(this);
  }

  void InputFinished(ExecNode* input, int seq_stop) override {
    std::unique_lock<std::mutex> lock(mutex_);
    emit_stop_ = seq_stop;
    lock.unlock();
    Finish();
  }

 private:
  void Finish() {
    if (producer_.Close()) {
      finished_.MarkFinished();
    }
  }

  std::mutex mutex_;

  int num_received_ = 0;
  int emit_stop_ = -1;
  Future<> finished_ = Future<>::MakeFinished();

  PushGenerator<util::optional<ExecBatch>>::Producer producer_;
};

AsyncGenerator<util::optional<ExecBatch>> MakeSinkNode(ExecNode* input,
                                                       std::string label) {
  AsyncGenerator<util::optional<ExecBatch>> out;
  (void)input->plan()->EmplaceNode<SinkNode>(input, std::move(label), &out);
  return out;
}

struct GroupByNode : ExecNode {
  GroupByNode(ExecNode* input, std::string label, std::shared_ptr<Schema> output_schema,
              ExecContext* ctx, const std::vector<int>&& key_field_ids,
              std::unique_ptr<internal::Grouper>&& grouper,
              const std::vector<int>&& agg_src_field_ids,
              const std::vector<const HashAggregateKernel*>&& agg_kernels,
              std::vector<std::unique_ptr<KernelState>>&& agg_states)
      : ExecNode(input->plan(), std::move(label), {input}, {"groupby"},
                 std::move(output_schema), /*num_outputs=*/1),
        ctx_(ctx),
        key_field_ids_(std::move(key_field_ids)),
        grouper_(std::move(grouper)),
        agg_src_field_ids_(std::move(agg_src_field_ids)),
        agg_kernels_(std::move(agg_kernels)),
        agg_states_(std::move(agg_states)) {}

  const char* kind_name() override { return "GroupByNode"; }

  Status ProcessInputBatch(const ExecBatch& batch) {
    // Create a batch with key columns
    std::vector<Datum> keys(key_field_ids_.size());
    for (size_t i = 0; i < key_field_ids_.size(); ++i) {
      keys[i] = batch.values[key_field_ids_[i]];
    }
    ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

    // Create a batch with group ids
    ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper_->Consume(key_batch));

    // Execute aggregate kernels
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      KernelContext kernel_ctx{ctx_};
      kernel_ctx.SetState(agg_states_[i].get());

      ARROW_ASSIGN_OR_RAISE(
          auto agg_batch,
          ExecBatch::Make({batch.values[agg_src_field_ids_[i]], id_batch}));

      RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, grouper_->num_groups()));
      RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_batch));
    }

    return Status::OK();
  }

  Status OutputResult() {
    // Finalize output
    ArrayDataVector out_data(agg_kernels_.size() + key_field_ids_.size());
    auto it = out_data.begin();

    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      KernelContext batch_ctx{ctx_};
      batch_ctx.SetState(agg_states_[i].get());
      Datum out;
      RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out));
      *it++ = out.array();
    }

    ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, grouper_->GetUniques());
    for (const auto& key : out_keys.values) {
      *it++ = key.array();
    }

    uint32_t num_groups = grouper_->num_groups();
    int num_result_batches = (num_groups + output_batch_size_ - 1) / output_batch_size_;
    outputs_[0]->InputFinished(this, num_result_batches);

    for (int i = 0; i < num_result_batches; ++i) {
      // Check finished flag
      if (finished_) {
        break;
      }

      // Slice arrays
      int64_t batch_start = i * output_batch_size_;
      int64_t batch_length =
          std::min(output_batch_size_, static_cast<int>(num_groups - batch_start));
      std::vector<Datum> output_slices(out_data.size());
      for (size_t out_field_id = 0; out_field_id < out_data.size(); ++out_field_id) {
        output_slices[out_field_id] =
            out_data[out_field_id]->Slice(batch_start, batch_length);
      }

      ARROW_ASSIGN_OR_RAISE(ExecBatch output_batch, ExecBatch::Make(output_slices));
      outputs_[0]->InputReceived(this, i, output_batch);
    }

    return Status::OK();
  }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    std::unique_lock<std::mutex> lock(mutex_);

    Status status = ProcessInputBatch(batch);
    if (!status.ok()) {
      ErrorReceived(input, status);
      return;
    }

    ++num_input_batches_processed_;
    if (num_input_batches_processed_ == num_input_batches_total_) {
      status = OutputResult();
      if (!status.ok()) {
        ErrorReceived(input, status);
        return;
      }
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);

    outputs_[0]->ErrorReceived(this, std::move(error));
    StopProducing();
  }

  void InputFinished(ExecNode* input, int seq) override {
    DCHECK_EQ(input, inputs_[0]);

    std::unique_lock<std::mutex> lock(mutex_);

    num_input_batches_total_ = seq;
    if (num_input_batches_processed_ == num_input_batches_total_) {
      Status status = OutputResult();
      if (!status.ok()) {
        ErrorReceived(input, status);
        return;
      }
    }
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    inputs_[0]->StopProducing(this);

    finished_ = true;
  }

  void StopProducing() override { StopProducing(outputs_[0]); }

 private:
  ExecContext* ctx_;
  std::mutex mutex_;
  bool finished_{false};
  int num_input_batches_processed_{0};
  int num_input_batches_total_{-1};
  int output_batch_size_{32 * 1024};

  const std::vector<int> key_field_ids_;
  std::unique_ptr<internal::Grouper> grouper_;
  const std::vector<int> agg_src_field_ids_;
  const std::vector<const HashAggregateKernel*> agg_kernels_;
  std::vector<std::unique_ptr<KernelState>> agg_states_;
};

namespace internal {
Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<internal::Aggregate>& aggregates,
    const std::vector<ValueDescr>& in_descrs);
Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<internal::Aggregate>& aggregates,
    const std::vector<ValueDescr>& in_descrs);
Result<FieldVector> ResolveKernels(
    const std::vector<internal::Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<ValueDescr>& descrs);
}  // namespace internal

Result<ExecNode*> MakeGroupByNode(ExecNode* input, std::string label,
                                  std::vector<std::string> keys,
                                  std::vector<std::string> agg_srcs,
                                  std::vector<internal::Aggregate> aggs,
                                  ExecContext* ctx) {
  // Get input schema
  auto input_schema = input->output_schema();

  // Find input field indices for key fields
  std::vector<int> key_field_ids(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(keys[i]).FindOne(*input_schema));
    key_field_ids[i] = match[0];
  }

  // Build vector of key field data types
  std::vector<ValueDescr> key_descrs(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    auto key_field_id = key_field_ids[i];
    key_descrs[i] = ValueDescr(input_schema->field(key_field_id)->type());
  }

  // Construct grouper
  ARROW_ASSIGN_OR_RAISE(auto grouper, internal::Grouper::Make(key_descrs, ctx));

  // Find input field indices for aggregates
  std::vector<int> agg_src_field_ids(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(agg_srcs[i]).FindOne(*input_schema));
    agg_src_field_ids[i] = match[0];
  }

  // Build vector of aggregate source field data types
  DCHECK_EQ(agg_srcs.size(), aggs.size());
  std::vector<ValueDescr> agg_src_descrs(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    auto agg_src_field_id = agg_src_field_ids[i];
    agg_src_descrs[i] =
        ValueDescr(input_schema->field(agg_src_field_id)->type(), ValueDescr::ARRAY);
  }

  // Construct aggregates
  ARROW_ASSIGN_OR_RAISE(auto agg_kernels,
                        internal::GetKernels(ctx, aggs, agg_src_descrs));

  ARROW_ASSIGN_OR_RAISE(auto agg_states,
                        internal::InitKernels(agg_kernels, ctx, aggs, agg_src_descrs));

  ARROW_ASSIGN_OR_RAISE(
      FieldVector agg_result_fields,
      internal::ResolveKernels(aggs, agg_kernels, agg_states, ctx, agg_src_descrs));

  // Build field vector for output schema
  FieldVector output_fields{keys.size() + aggs.size()};

  // Aggregate fields come before key fields to match the behavior of GroupBy function
  for (size_t i = 0; i < aggs.size(); ++i) {
    output_fields[i] = agg_result_fields[i];
  }
  size_t base = aggs.size();
  for (size_t i = 0; i < keys.size(); ++i) {
    int key_field_id = key_field_ids[i];
    output_fields[base + i] = input_schema->field(key_field_id);
  }

  return input->plan()->EmplaceNode<GroupByNode>(
      input, std::move(label), schema(std::move(output_fields)), ctx,
      std::move(key_field_ids), std::move(grouper), std::move(agg_src_field_ids),
      std::move(agg_kernels), std::move(agg_states));
}

}  // namespace compute
}  // namespace arrow

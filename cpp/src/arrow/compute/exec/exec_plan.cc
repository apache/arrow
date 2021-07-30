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

#include <unordered_map>
#include <unordered_set>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
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

ExecNode::ExecNode(ExecPlan* plan, NodeVector inputs,
                   std::vector<std::string> input_labels,
                   std::shared_ptr<Schema> output_schema, int num_outputs)
    : plan_(plan),
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

bool ExecNode::ErrorIfNotOk(Status status) {
  if (status.ok()) return false;

  for (auto out : outputs_) {
    out->ErrorReceived(this, out == outputs_.back() ? std::move(status) : status);
  }
  return true;
}

ExecFactoryRegistry::AddOnLoad::AddOnLoad(std::string factory_name, Factory factory)
    : AddOnLoad(std::move(factory_name), std::move(factory),
                default_exec_factory_registry()) {}

ExecFactoryRegistry::AddOnLoad::AddOnLoad(std::string factory_name, Factory factory,
                                          ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(std::move(factory_name), std::move(factory)));
}

std::shared_ptr<RecordBatchReader> MakeGeneratorReader(
    std::shared_ptr<Schema> schema,
    std::function<Future<util::optional<ExecBatch>>()> gen, MemoryPool* pool) {
  struct Impl : RecordBatchReader {
    std::shared_ptr<Schema> schema() const override { return schema_; }

    Status ReadNext(std::shared_ptr<RecordBatch>* record_batch) override {
      ARROW_ASSIGN_OR_RAISE(auto batch, iterator_.Next());
      if (batch) {
        ARROW_ASSIGN_OR_RAISE(*record_batch, batch->ToRecordBatch(schema_, pool_));
      } else {
        *record_batch = IterationEnd<std::shared_ptr<RecordBatch>>();
      }
      return Status::OK();
    }

    MemoryPool* pool_;
    std::shared_ptr<Schema> schema_;
    Iterator<util::optional<ExecBatch>> iterator_;
  };

  auto out = std::make_shared<Impl>();
  out->pool_ = pool;
  out->schema_ = std::move(schema);
  out->iterator_ = MakeGeneratorIterator(std::move(gen));
  return out;
}

Result<ExecNode*> Declaration::AddToPlan(ExecPlan* plan,
                                         ExecFactoryRegistry* registry) const {
  std::vector<ExecNode*> inputs(this->inputs.size());

  size_t i = 0;
  for (const Input& input : this->inputs) {
    if (auto node = util::get_if<ExecNode*>(&input)) {
      inputs[i++] = *node;
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(inputs[i++],
                          util::get<Declaration>(input).AddToPlan(plan, registry));
  }

  ARROW_ASSIGN_OR_RAISE(
      auto node, MakeExecNode(this->factory_name, plan, std::move(inputs), *this->options,
                              registry));
  node->SetLabel(this->label);
  return node;
}

Declaration Declaration::Sequence(std::vector<Declaration> decls) {
  DCHECK(!decls.empty());

  Declaration out = std::move(decls.back());
  decls.pop_back();
  auto receiver = &out;
  while (!decls.empty()) {
    Declaration input = std::move(decls.back());
    decls.pop_back();

    receiver->inputs.emplace_back(std::move(input));
    receiver = &util::get<Declaration>(receiver->inputs.front());
  }
  return out;
}

ExecFactoryRegistry* default_exec_factory_registry() {
  static class : public ExecFactoryRegistry {
   public:
    Result<Factory> GetFactory(const std::string& factory_name) override {
      auto it = factories_.find(factory_name);
      if (it == factories_.end()) {
        return Status::KeyError("ExecNode factory named ", factory_name,
                                " not present in registry.");
      }
      return it->second;
    }

    Status AddFactory(std::string factory_name, Factory factory) override {
      auto it_success = factories_.emplace(std::move(factory_name), std::move(factory));

      if (!it_success.second) {
        const auto& factory_name = it_success.first->first;
        return Status::KeyError("ExecNode factory named ", factory_name,
                                " already registered.");
      }

      return Status::OK();
    }

   private:
    std::unordered_map<std::string, Factory> factories_;
  } instance;

  return &instance;
}

struct HashSemiJoinNode : ExecNode {
  HashSemiJoinNode(ExecNode* build_input, ExecNode* probe_input, std::string label,
                   std::shared_ptr<Schema> output_schema, ExecContext* ctx,
                   const std::vector<int>&& index_field_ids)
      : ExecNode(build_input->plan(), std::move(label), {build_input, probe_input},
                 {"hash_join_build", "hash_join_probe"}, std::move(output_schema),
                 /*num_outputs=*/1),
        ctx_(ctx),
        index_field_ids_(index_field_ids),
        build_side_complete_(false) {}

 private:
  struct ThreadLocalState;

 public:
  const char* kind_name() override { return "HashSemiJoinNode"; }

  Status InitLocalStateIfNeeded(ThreadLocalState* state) {
    // Get input schema
    auto input_schema = inputs_[0]->output_schema();

    if (state->grouper != nullptr) return Status::OK();

    // Build vector of key field data types
    std::vector<ValueDescr> key_descrs(index_field_ids_.size());
    for (size_t i = 0; i < index_field_ids_.size(); ++i) {
      auto idx_field_id = index_field_ids_[i];
      key_descrs[i] = ValueDescr(input_schema->field(idx_field_id)->type());
    }

    // Construct grouper
    ARROW_ASSIGN_OR_RAISE(state->grouper, internal::Grouper::Make(key_descrs, ctx_));

    return Status::OK();
  }

  // merge all other groupers to grouper[0]. nothing needs to be done on the
  // cached_probe_batches, because when probing everyone
  Status BuildSideMerge() {
    ThreadLocalState* state0 = &local_states_[0];
    for (int i = 1; i < local_states_.size(); ++i) {
      ThreadLocalState* state = &local_states_[i];
      ARROW_DCHECK(state);
      ARROW_DCHECK(state->grouper);
      ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, state->grouper->GetUniques());
      ARROW_ASSIGN_OR_RAISE(Datum _, state0->grouper->Consume(other_keys));
      state->grouper.reset();
    }
    return Status::OK();
  }

  // consumes a build batch and increments the build_batches count. if the build batches
  // total reached at the end of consumption, all the local states will be merged, before
  // incrementing the total batches
  Status ConsumeBuildBatch(const size_t thread_index, ExecBatch batch) {
    auto state = &local_states_[thread_index];
    RETURN_NOT_OK(InitLocalStateIfNeeded(state));

    // Create a batch with key columns
    std::vector<Datum> keys(index_field_ids_.size());
    for (size_t i = 0; i < index_field_ids_.size(); ++i) {
      keys[i] = batch.values[index_field_ids_[i]];
    }
    ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

    // Create a batch with group ids
    ARROW_ASSIGN_OR_RAISE(Datum id_batch, state->grouper->Consume(key_batch));

    if (build_counter_.Increment()) {
      // while incrementing, if the total is reached, merge all the groupers to 0'th one
      RETURN_NOT_OK(BuildSideMerge());

      // enable flag that build side is completed
      build_side_complete_.store(true);

      // since the build side is completed, consume cached probe batches
      RETURN_NOT_OK(ConsumeCachedProbeBatches(thread_index));
    }

    return Status::OK();
  }

  Status ConsumeCachedProbeBatches(const size_t thread_index) {
    ThreadLocalState* state = &local_states_[thread_index];

    // TODO (niranda) check if this is the best way to move batches
    for (auto cached : state->cached_probe_batches) {
      RETURN_NOT_OK(ConsumeProbeBatch(cached.first, std::move(cached.second)));
    }
    state->cached_probe_batches.clear();

    return Status::OK();
  }

  // consumes a probe batch and increment probe batches count. Probing would query the
  // grouper[0] which have been merged with all others.
  Status ConsumeProbeBatch(int seq, ExecBatch batch) {
    auto* grouper = local_states_[0].grouper.get();

    // Create a batch with key columns
    std::vector<Datum> keys(index_field_ids_.size());
    for (size_t i = 0; i < index_field_ids_.size(); ++i) {
      keys[i] = batch.values[index_field_ids_[i]];
    }
    ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

    // Query the grouper with key_batch. If no match was found, returning group_ids would
    // have null.
    ARROW_ASSIGN_OR_RAISE(Datum group_ids, grouper->Find(key_batch));
    auto group_ids_data = *group_ids.array();

    if (group_ids_data.MayHaveNulls()) {  // values need to be filtered
      auto filter_arr =
          std::make_shared<BooleanArray>(group_ids_data.length, group_ids_data.buffers[0],
                                         /*null_bitmap=*/nullptr, /*null_count=*/0,
                                         /*offset=*/group_ids_data.offset);
      ARROW_ASSIGN_OR_RAISE(auto rec_batch,
                            batch.ToRecordBatch(output_schema_, ctx_->memory_pool()));
      ARROW_ASSIGN_OR_RAISE(
          auto filtered,
          Filter(rec_batch, filter_arr,
                 /* null_selection = DROP*/ FilterOptions::Defaults(), ctx_));
      auto out_batch = ExecBatch(*filtered.record_batch());
      outputs_[0]->InputReceived(this, seq, std::move(out_batch));
    } else {  // all values are valid for output
      outputs_[0]->InputReceived(this, seq, std::move(batch));
    }

    out_counter_.Increment();
    return Status::OK();
  }

  Status CacheProbeBatch(const size_t thread_index, int seq_num, ExecBatch batch) {
    ThreadLocalState* state = &local_states_[thread_index];
    state->cached_probe_batches.emplace_back(seq_num, std::move(batch));
    return Status::OK();
  }

  inline bool IsBuildInput(ExecNode* input) { return input == inputs_[0]; }

  // If all build side batches received? continue streaming using probing
  // else cache the batches in thread-local state
  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);

    size_t thread_index = get_thread_index_();
    ARROW_DCHECK(thread_index < local_states_.size());

    if (finished_.is_finished()) {
      return;
    }

    if (IsBuildInput(input)) {  // build input batch is received
      // if a build input is received when build side is completed, something's wrong!
      ARROW_DCHECK(!build_side_complete_.load());

      if (ErrorIfNotOk(ConsumeBuildBatch(thread_index, std::move(batch)))) return;
    } else {                              // probe input batch is received
      if (build_side_complete_.load()) {  // build side done, continue with probing
        if (ErrorIfNotOk(ConsumeProbeBatch(seq, std::move(batch)))) return;
      } else {  // build side not completed. Cache this batch!
        if (ErrorIfNotOk(CacheProbeBatch(thread_index, seq, std::move(batch)))) return;
      }
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);

    outputs_[0]->ErrorReceived(this, std::move(error));
    StopProducing();
  }

  void InputFinished(ExecNode* input, int num_total) override {
    // bail if StopProducing was called
    if (finished_.is_finished()) return;

    ARROW_DCHECK(input == inputs_[0] || input == inputs_[1]);

    // set total for build input
    if (IsBuildInput(input) && build_counter_.SetTotal(num_total)) {
      // only build side has completed! so process cached probe batches (of this thread)
      ErrorIfNotOk(ConsumeCachedProbeBatches(get_thread_index_()));
      return;
    }

    // set total for probe input. If it returns that probe side has completed, nothing to
    // do, because probing inputs will be streamed to the output
    // probe_counter_.SetTotal(num_total);

    // output will be streamed from the probe side. So, they will have the same total.
    if (out_counter_.SetTotal(num_total)) {
      // if out_counter has completed, the future is finished!
      finished_.MarkFinished();
    }
  }

  Status StartProducing() override {
    finished_ = Future<>::Make();

    local_states_.resize(ThreadIndexer::Capacity());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);

    if (build_counter_.Cancel() || /*probe_counter_.Cancel() ||*/ out_counter_.Cancel()) {
      finished_.MarkFinished();
    }

    for (auto&& input : inputs_) {
      input->StopProducing(this);
    }
  }

  // TODO(niranda) couldn't there be multiple outputs for a Node?
  void StopProducing() override {
    for (auto&& output : outputs_) {
      StopProducing(output);
    }
  }

  Future<> finished() override { return finished_; }

 private:

  struct ThreadLocalState {
    std::unique_ptr<internal::Grouper> grouper;
    std::vector<std::pair<int, ExecBatch>> cached_probe_batches{};
  };

  ExecContext* ctx_;
  Future<> finished_ = Future<>::MakeFinished();

  ThreadIndexer get_thread_index_;
  const std::vector<int> index_field_ids_;

  AtomicCounter build_counter_, /*probe_counter_,*/ out_counter_;
  std::vector<ThreadLocalState> local_states_;

  // need a separate atomic bool to track if the build side complete. Can't use the flag
  // inside the AtomicCounter, because we need to merge the build groupers once we receive
  // all the build batches. So, while merging, we need to prevent probe batches, being
  // consumed.
  std::atomic<bool> build_side_complete_;
};

}  // namespace compute
}  // namespace arrow

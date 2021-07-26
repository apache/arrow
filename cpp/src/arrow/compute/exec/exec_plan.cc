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

/*struct HashSemiIndexJoinNode : ExecNode {
  HashSemiIndexJoinNode(ExecNode* left_input, ExecNode* right_input, std::string label,
                        std::shared_ptr<Schema> output_schema, ExecContext* ctx,
                        const std::vector<int>&& index_field_ids)
      : ExecNode(left_input->plan(), std::move(label), {left_input, right_input},
                 {"hashsemiindexjoin"}, std::move(output_schema), */
/*num_outputs=*//*1),
ctx_(ctx),
num_build_batches_processed_(0),
num_build_batches_total_(-1),
num_probe_batches_processed_(0),
num_probe_batches_total_(-1),
num_output_batches_processed_(0),
index_field_ids_(std::move(index_field_ids)),
output_started_(false),
build_phase_finished_(false){}

const char* kind_name() override { return "HashSemiIndexJoinNode"; }

private:
struct ThreadLocalState;

public:
Status InitLocalStateIfNeeded(ThreadLocalState* state) {
// Get input schema
auto input_schema = inputs_[0]->output_schema();

if (!state->grouper) {
// Build vector of key field data types
std::vector<ValueDescr> key_descrs(index_field_ids_.size());
for (size_t i = 0; i < index_field_ids_.size(); ++i) {
auto key_field_id = index_field_ids_[i];
key_descrs[i] = ValueDescr(input_schema->field(key_field_id)->type());
}

// Construct grouper
ARROW_ASSIGN_OR_RAISE(state->grouper, internal::Grouper::Make(key_descrs, ctx_));
}

return Status::OK();
}

Status ProcessBuildSideBatch(const ExecBatch& batch) {
SmallUniqueIdHolder id_holder(&local_state_id_assignment_);
int id = id_holder.get();
ThreadLocalState* state = local_states_.get(id);
RETURN_NOT_OK(InitLocalStateIfNeeded(state));

// Create a batch with key columns
std::vector<Datum> keys(key_field_ids_.size());
for (size_t i = 0; i < key_field_ids_.size(); ++i) {
keys[i] = batch.values[key_field_ids_[i]];
}
ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(keys));

// Create a batch with group ids
ARROW_ASSIGN_OR_RAISE(Datum id_batch, state->grouper->Consume(key_batch));

// Execute aggregate kernels
for (size_t i = 0; i < agg_kernels_.size(); ++i) {
KernelContext kernel_ctx{ctx_};
kernel_ctx.SetState(state->agg_states[i].get());

ARROW_ASSIGN_OR_RAISE(
auto agg_batch,
ExecBatch::Make({batch.values[agg_src_field_ids_[i]], id_batch}));

RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, state->grouper->num_groups()));
RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_batch));
}

return Status::OK();
}

// merge all other groupers to grouper[0]. nothing needs to be done on the
// early_probe_batches, because when probing everyone
Status BuildSideMerge() {
int num_local_states = local_state_id_assignment_.num_ids();
ThreadLocalState* state0 = local_states_.get(0);
for (int i = 1; i < num_local_states; ++i) {
ThreadLocalState* state = local_states_.get(i);
ARROW_DCHECK(state);
ARROW_DCHECK(state->grouper);
ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, state->grouper->GetUniques());
ARROW_ASSIGN_OR_RAISE(Datum _, state0->grouper->Consume(other_keys));
state->grouper.reset();
}
return Status::OK();
}

Status Finalize() {
out_data_.resize(agg_kernels_.size() + key_field_ids_.size());
auto it = out_data_.begin();

ThreadLocalState* state = local_states_.get(0);
num_out_groups_ = state->grouper->num_groups();

// Aggregate fields come before key fields to match the behavior of GroupBy function

for (size_t i = 0; i < agg_kernels_.size(); ++i) {
KernelContext batch_ctx{ctx_};
batch_ctx.SetState(state->agg_states[i].get());
Datum out;
RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out));
*it++ = out.array();
state->agg_states[i].reset();
}

ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, state->grouper->GetUniques());
for (const auto& key : out_keys.values) {
*it++ = key.array();
}
state->grouper.reset();

return Status::OK();
}

Status OutputNthBatch(int n) {
ARROW_DCHECK(output_started_.load());

// Check finished flag
if (finished_.is_finished()) {
return Status::OK();
}

// Slice arrays
int64_t batch_size = output_batch_size();
int64_t batch_start = n * batch_size;
int64_t batch_length = std::min(batch_size, num_out_groups_ - batch_start);
std::vector<Datum> output_slices(out_data_.size());
for (size_t out_field_id = 0; out_field_id < out_data_.size(); ++out_field_id) {
output_slices[out_field_id] =
out_data_[out_field_id]->Slice(batch_start, batch_length);
}

ARROW_ASSIGN_OR_RAISE(ExecBatch output_batch, ExecBatch::Make(output_slices));
outputs_[0]->InputReceived(this, n, output_batch);

uint32_t num_output_batches_processed =
1 + num_output_batches_processed_.fetch_add(1);
if (num_output_batches_processed * batch_size >= num_out_groups_) {
finished_.MarkFinished();
}

return Status::OK();
}

Status OutputResult() {
bool expected = false;
if (!output_started_.compare_exchange_strong(expected, true)) {
return Status::OK();
}

RETURN_NOT_OK(BuildSideMerge());
RETURN_NOT_OK(Finalize());

int batch_size = output_batch_size();
int num_result_batches = (num_out_groups_ + batch_size - 1) / batch_size;
outputs_[0]->InputFinished(this, num_result_batches);

auto executor = arrow::internal::GetCpuThreadPool();
for (int i = 0; i < num_result_batches; ++i) {
// Check finished flag
if (finished_.is_finished()) {
break;
}

RETURN_NOT_OK(executor->Spawn([this, i]() {
Status status = OutputNthBatch(i);
if (!status.ok()) {
ErrorReceived(inputs_[0], status);
}
}));
}

return Status::OK();
}

void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
assert(input == inputs_[0] || input == inputs_[1]);

if (finished_.is_finished()) {
return;
}

ARROW_DCHECK(num_build_batches_processed_.load() != num_build_batches_total_.load());

Status status = ProcessBuildSideBatch(batch);
if (!status.ok()) {
ErrorReceived(input, status);
return;
}

num_build_batches_processed_.fetch_add(1);
if (num_build_batches_processed_.load() == num_build_batches_total_.load()) {
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

num_build_batches_total_.store(seq);
if (num_build_batches_processed_.load() == num_build_batches_total_.load()) {
Status status = OutputResult();

if (!status.ok()) {
ErrorReceived(input, status);
}
}
}

Status StartProducing() override {
finished_ = Future<>::Make();
return Status::OK();
}

void PauseProducing(ExecNode* output) override {}

void ResumeProducing(ExecNode* output) override {}

void StopProducing(ExecNode* output) override {
DCHECK_EQ(output, outputs_[0]);
inputs_[0]->StopProducing(this);

finished_.MarkFinished();
}

void StopProducing() override { StopProducing(outputs_[0]); }

Future<> finished() override { return finished_; }

private:
int output_batch_size() const {
int result = static_cast<int>(ctx_->exec_chunksize());
if (result < 0) {
result = 32 * 1024;
}
return result;
}

ExecContext* ctx_;
Future<> finished_ = Future<>::MakeFinished();

std::atomic<int> num_build_batches_processed_;
std::atomic<int> num_build_batches_total_;
std::atomic<int> num_probe_batches_processed_;
std::atomic<int> num_probe_batches_total_;
std::atomic<uint32_t> num_output_batches_processed_;

const std::vector<int> index_field_ids_;

struct ThreadLocalState {
std::unique_ptr<internal::Grouper> grouper;
std::vector<ExecBatch> early_probe_batches{};
};
SharedSequenceOfObjects<ThreadLocalState> local_states_;
SmallUniqueIdAssignment local_state_id_assignment_;
uint32_t num_out_groups_{0};
ArrayDataVector out_data_;
std::atomic<bool> output_started_, build_phase_finished_;
};*/
}  // namespace compute
}  // namespace arrow

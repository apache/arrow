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

#include <optional>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

namespace {

struct ExecPlanImpl : public ExecPlan {
  explicit ExecPlanImpl(ExecContext* exec_context,
                        std::shared_ptr<const KeyValueMetadata> metadata = NULLPTR)
      : ExecPlan(exec_context), metadata_(std::move(metadata)) {}

  ~ExecPlanImpl() override {
    if (started_ && !finished_.is_finished()) {
      ARROW_LOG(WARNING) << "Plan was destroyed before finishing";
      Abort();
      finished().Wait();
    }
  }

  size_t GetThreadIndex() { return thread_indexer_(); }
  size_t max_concurrency() const { return thread_indexer_.Capacity(); }
  const std::vector<std::unique_ptr<ExecNode>>& nodes() const { return nodes_; }

  ExecNode* AddNode(std::unique_ptr<ExecNode> node) {
    if (node->label().empty()) {
      node->SetLabel(std::to_string(auto_label_counter_++));
    }
    nodes_.push_back(std::move(node));
    return nodes_.back().get();
  }

  Result<Future<>> BeginExternalTask() {
    // The task group isn't relevant in synchronous execution mode
    if (!exec_context_->executor()) return Future<>::Make();

    Future<> completion_future = Future<>::Make();
    if (async_scheduler_->AddSimpleTask(
            [completion_future] { return completion_future; })) {
      return completion_future;
    }
    return Future<>{};
  }

  Status ScheduleTask(std::function<Status()> fn) {
    auto executor = exec_context_->executor();
    if (!executor) return fn();
    // Adds a task which submits fn to the executor and tracks its progress.  If we're
    // aborted then the task is ignored and fn is not executed.
    async_scheduler_->AddSimpleTask(
        [this, executor, fn = std::move(fn)]()
        {
            ARROW_ASSIGN_OR_RAISE(Future<> fut,
                                  executor->Submit(stop_source_.token(), std::move(fn)));
            fut.AddCallback([this](const Status& status) {
                if (!status.ok()) {
                    std::lock_guard<std::mutex> guard(abort_mutex_);
                    errors_.emplace_back(std::move(status));
                    AbortUnlocked();
                }
            });
            return Status::OK();
        });
    return Status::OK();
  }

  Status ScheduleTask(std::function<Status(size_t)> fn) {
    std::function<Status()> indexed_fn = [this, fn]() {
      size_t thread_index = GetThreadIndex();
      return fn(thread_index);
    };
    return ScheduleTask(std::move(indexed_fn));
  }

  int RegisterTaskGroup(std::function<Status(size_t, int64_t)> task,
                        std::function<Status(size_t)> on_finished) {
    return task_scheduler_->RegisterTaskGroup(std::move(task), std::move(on_finished));
  }

  Status StartTaskGroup(int task_group_id, int64_t num_tasks) {
    return task_scheduler_->StartTaskGroup(GetThreadIndex(), task_group_id, num_tasks);
  }

  util::AsyncTaskScheduler* async_scheduler() { return async_scheduler_.get(); }

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
    START_COMPUTE_SPAN(span_, "ExecPlan", {{"plan", ToString()}});
#ifdef ARROW_WITH_OPENTELEMETRY
    if (HasMetadata()) {
      auto pairs = metadata().get()->sorted_pairs();
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span =
          ::arrow::internal::tracing::UnwrapSpan(span_.details.get());
      std::for_each(std::begin(pairs), std::end(pairs),
                    [span](std::pair<std::string, std::string> const& pair) {
                      span->SetAttribute(pair.first, pair.second);
                    });
    }
#endif
    if (started_) {
      return Status::Invalid("restarted ExecPlan");
    }

    for (std::unique_ptr<ExecNode>& n : nodes_) RETURN_NOT_OK(n->Init());

    task_scheduler_->RegisterEnd();
    int num_threads = 1;
    bool sync_execution = true;
    if (auto executor = exec_context()->executor()) {
      num_threads = executor->GetCapacity();
      sync_execution = false;
    }
    RETURN_NOT_OK(task_scheduler_->StartScheduling(
        0 /* thread_index */,
        [this](std::function<Status(size_t)> fn) -> Status {
          return this->ScheduleTask(std::move(fn));
        },
        /*concurrent_tasks=*/2 * num_threads, sync_execution));

    started_ = true;
    for (std::unique_ptr<ExecNode>& n : nodes_) {
      Status st = n->StartProducing();
      if (!st.ok()) {
        Abort();
        return st;
      }
    }
    // StartProducing will have added some tasks to the task group.
    // Now we end the task group so that as soon as we run out of tasks,
    // we've finished executing.
    EndTaskGroup();
    return Status::OK();
  }

  void EndTaskGroup() {
    bool expected = false;
    if (group_ended_.compare_exchange_strong(expected, true)) {
       async_scheduler_->End();
       async_scheduler_->OnFinished().AddCallback([this](const Status& st) {
        MARK_SPAN(span_, error_st_ & st);
        END_SPAN(span_);
        if (aborted_) {
          for (std::unique_ptr<ExecNode>& node : nodes_) node->Abort();
        }
        if (!errors_.empty())
          finished_.MarkFinished(errors_[0]);
        else
          finished_.MarkFinished(st);
      });
    }
  }

  void Abort() {
    DCHECK(started_) << "aborted an ExecPlan which never started";
    EVENT(span_, "Abort");
    if (finished_.is_finished()) return;
    std::lock_guard<std::mutex> guard(abort_mutex_);
    AbortUnlocked();
  }

  void AbortUnlocked() {
    if (!aborted_) {
      aborted_ = true;
      stop_source_.RequestStop();
      EndTaskGroup();
      task_scheduler_->Abort([]() {});
    }
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "ExecPlan with " << nodes_.size() << " nodes:" << std::endl;
    for (const std::unique_ptr<ExecNode>& node : nodes_) {
      if (!node->output()) {
        PrintSubtree(node.get(), ss, /*indent=*/0);
      }
    }
    return ss.str();
  }

  void PrintSubtree(ExecNode* node, std::stringstream& ss, int indent) const {
    for (int i = 0; i < indent; i++) ss << "  ";
    ss << node->ToString(indent) << std::endl;
    for (ExecNode* input : node->inputs()) {
      PrintSubtree(input, ss, indent + 1);
    }
  }

  Future<> finished_ = Future<>::Make();
  bool started_ = false;
  std::vector<std::unique_ptr<ExecNode>> nodes_;
  uint32_t auto_label_counter_ = 0;
  util::tracing::Span span_;
  std::shared_ptr<const KeyValueMetadata> metadata_;

  ThreadIndexer thread_indexer_;
  std::atomic<bool> group_ended_{false};
  std::unique_ptr<util::AsyncTaskScheduler> async_scheduler_ =
      util::AsyncTaskScheduler::Make();
  std::unique_ptr<TaskScheduler> task_scheduler_ = TaskScheduler::Make();

  std::mutex abort_mutex_;
  bool aborted_ = false;
  StopSource stop_source_;
  std::vector<Status> errors_;
};

ExecPlanImpl* ToDerived(ExecPlan* ptr) { return checked_cast<ExecPlanImpl*>(ptr); }

const ExecPlanImpl* ToDerived(const ExecPlan* ptr) {
  return checked_cast<const ExecPlanImpl*>(ptr);
}

std::optional<int> GetNodeIndex(const std::vector<ExecNode*>& nodes,
                                const ExecNode* node) {
  for (int i = 0; i < static_cast<int>(nodes.size()); ++i) {
    if (nodes[i] == node) return i;
  }
  return std::nullopt;
}

}  // namespace

const uint32_t ExecPlan::kMaxBatchSize;

Result<std::shared_ptr<ExecPlan>> ExecPlan::Make(
    ExecContext* ctx, std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::shared_ptr<ExecPlan>(new ExecPlanImpl{ctx, metadata});
}

ExecNode* ExecPlan::AddNode(std::unique_ptr<ExecNode> node) {
  return ToDerived(this)->AddNode(std::move(node));
}

size_t ExecPlan::GetThreadIndex() { return ToDerived(this)->GetThreadIndex(); }
size_t ExecPlan::max_concurrency() const { return ToDerived(this)->max_concurrency(); }
const std::vector<std::unique_ptr<ExecNode>>& ExecPlan::nodes() const {
  return ToDerived(this)->nodes();
}

Result<Future<>> ExecPlan::BeginExternalTask() {
  return ToDerived(this)->BeginExternalTask();
}

Status ExecPlan::ScheduleTask(std::function<Status()> fn) {
  return ToDerived(this)->ScheduleTask(std::move(fn));
}
Status ExecPlan::ScheduleTask(std::function<Status(size_t)> fn) {
  return ToDerived(this)->ScheduleTask(std::move(fn));
}
int ExecPlan::RegisterTaskGroup(std::function<Status(size_t, int64_t)> task,
                                std::function<Status(size_t)> on_finished) {
  return ToDerived(this)->RegisterTaskGroup(std::move(task), std::move(on_finished));
}
Status ExecPlan::StartTaskGroup(int task_group_id, int64_t num_tasks) {
  return ToDerived(this)->StartTaskGroup(task_group_id, num_tasks);
}

util::AsyncTaskScheduler* ExecPlan::async_scheduler() {
  return ToDerived(this)->async_scheduler();
}

Status ExecPlan::Validate() { return ToDerived(this)->Validate(); }

Status ExecPlan::StartProducing() { return ToDerived(this)->StartProducing(); }
void ExecPlan::Abort() { ToDerived(this)->Abort(); }

Future<> ExecPlan::finished() { return ToDerived(this)->finished_; }

bool ExecPlan::HasMetadata() const { return !!(ToDerived(this)->metadata_); }

std::shared_ptr<const KeyValueMetadata> ExecPlan::metadata() const {
  return ToDerived(this)->metadata_;
}

std::string ExecPlan::ToString() const { return ToDerived(this)->ToString(); }

ExecNode::ExecNode(ExecPlan* plan, NodeVector inputs,
                   std::vector<std::string> input_labels,
                   std::shared_ptr<Schema> output_schema)
    : plan_(plan),
      inputs_(std::move(inputs)),
      input_labels_(std::move(input_labels)),
      output_schema_(std::move(output_schema)),
      output_(nullptr) {
  for (auto input : inputs_) {
    input->output_ = this;
  }
}

Status ExecNode::Init() {
  START_COMPUTE_SPAN(
      span_, std::string(kind_name()) + ":" + label(),
      {{"node.label", label()}, {"node.detail", ToString()}, {"node.kind", kind_name()}});
  return Status::OK();
}

Status ExecNode::Validate() const {
  if (inputs_.size() != input_labels_.size()) {
    return Status::Invalid("Invalid number of inputs for '", label(), "' (expected ",
                           num_inputs(), ", actual ", input_labels_.size(), ")");
  }

  if (output_) {
    auto input_index = GetNodeIndex(output_->inputs(), this);
    if (!input_index)
      return Status::Invalid("Node '", label(), "' outputs to node '", output_->label(),
                             "' but is not listed as an input.");
  }
  return Status::OK();
}

std::string ExecNode::ToString(int indent) const {
  std::stringstream ss;

  auto PrintLabelAndKind = [&](const ExecNode* node) {
    ss << node->label() << ":" << node->kind_name();
  };

  PrintLabelAndKind(this);
  ss << "{";

  const std::string extra = ToStringExtra(indent);
  if (!extra.empty()) {
    ss << extra;
  }

  ss << '}';
  return ss.str();
}

std::string ExecNode::ToStringExtra(int indent = 0) const { return ""; }

std::shared_ptr<RecordBatchReader> MakeGeneratorReader(
    std::shared_ptr<Schema> schema, std::function<Future<std::optional<ExecBatch>>()> gen,
    MemoryPool* pool) {
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

    Status Close() override {
      // reading from generator until end is reached.
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(ReadNext(&batch));
      while (batch != NULLPTR) {
        RETURN_NOT_OK(ReadNext(&batch));
      }
      return Status::OK();
    }

    MemoryPool* pool_;
    std::shared_ptr<Schema> schema_;
    Iterator<std::optional<ExecBatch>> iterator_;
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
    if (auto node = std::get_if<ExecNode*>(&input)) {
      inputs[i++] = *node;
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(inputs[i++],
                          std::get<Declaration>(input).AddToPlan(plan, registry));
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
    receiver = &std::get<Declaration>(receiver->inputs.front());
  }
  return out;
}

bool Declaration::IsValid(ExecFactoryRegistry* registry) const {
  return !this->factory_name.empty() && this->options != nullptr;
}

Future<std::shared_ptr<Table>> DeclarationToTableAsync(Declaration declaration,
                                                       ExecContext* exec_context) {
  std::shared_ptr<std::shared_ptr<Table>> output_table =
      std::make_shared<std::shared_ptr<Table>>();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> exec_plan,
                        ExecPlan::Make(exec_context));
  Declaration with_sink = Declaration::Sequence(
      {declaration, {"table_sink", TableSinkNodeOptions(output_table.get())}});
  ARROW_RETURN_NOT_OK(with_sink.AddToPlan(exec_plan.get()));
  ARROW_RETURN_NOT_OK(exec_plan->StartProducing());
  return exec_plan->finished().Then([exec_plan, output_table] { return *output_table; });
}

Result<std::shared_ptr<Table>> DeclarationToTable(Declaration declaration,
                                                  ExecContext* exec_context) {
  return DeclarationToTableAsync(std::move(declaration), exec_context).result();
}

Future<std::vector<std::shared_ptr<RecordBatch>>> DeclarationToBatchesAsync(
    Declaration declaration, ExecContext* exec_context) {
  return DeclarationToTableAsync(std::move(declaration), exec_context)
      .Then([](const std::shared_ptr<Table>& table) {
        return TableBatchReader(table).ToRecordBatches();
      });
}

Result<std::vector<std::shared_ptr<RecordBatch>>> DeclarationToBatches(
    Declaration declaration, ExecContext* exec_context) {
  return DeclarationToBatchesAsync(std::move(declaration), exec_context).result();
}

Future<std::vector<ExecBatch>> DeclarationToExecBatchesAsync(Declaration declaration,
                                                             ExecContext* exec_context) {
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> exec_plan,
                        ExecPlan::Make(exec_context));
  Declaration with_sink =
      Declaration::Sequence({declaration, {"sink", SinkNodeOptions(&sink_gen)}});
  ARROW_RETURN_NOT_OK(with_sink.AddToPlan(exec_plan.get()));
  ARROW_RETURN_NOT_OK(exec_plan->StartProducing());
  auto collected_fut = CollectAsyncGenerator(sink_gen);
  return AllComplete({exec_plan->finished(), Future<>(collected_fut)})
      .Then([collected_fut, exec_plan]() -> Result<std::vector<ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        return ::arrow::internal::MapVector(
            [](std::optional<ExecBatch> batch) { return std::move(*batch); },
            std::move(collected));
      });
}

Result<std::vector<ExecBatch>> DeclarationToExecBatches(Declaration declaration,
                                                        ExecContext* exec_context) {
  return DeclarationToExecBatchesAsync(std::move(declaration), exec_context).result();
}

namespace internal {

void RegisterSourceNode(ExecFactoryRegistry*);
void RegisterFilterNode(ExecFactoryRegistry*);
void RegisterProjectNode(ExecFactoryRegistry*);
void RegisterUnionNode(ExecFactoryRegistry*);
void RegisterAggregateNode(ExecFactoryRegistry*);
void RegisterSinkNode(ExecFactoryRegistry*);
void RegisterHashJoinNode(ExecFactoryRegistry*);
void RegisterAsofJoinNode(ExecFactoryRegistry*);

}  // namespace internal

ExecFactoryRegistry* default_exec_factory_registry() {
  class DefaultRegistry : public ExecFactoryRegistry {
   public:
    DefaultRegistry() {
      internal::RegisterSourceNode(this);
      internal::RegisterFilterNode(this);
      internal::RegisterProjectNode(this);
      internal::RegisterUnionNode(this);
      internal::RegisterAggregateNode(this);
      internal::RegisterSinkNode(this);
      internal::RegisterHashJoinNode(this);
      internal::RegisterAsofJoinNode(this);
    }

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
  };

  static DefaultRegistry instance;
  return &instance;
}

Result<std::function<Future<std::optional<ExecBatch>>()>> MakeReaderGenerator(
    std::shared_ptr<RecordBatchReader> reader, ::arrow::internal::Executor* io_executor,
    int max_q, int q_restart) {
  auto batch_it = MakeMapIterator(
      [](std::shared_ptr<RecordBatch> batch) {
        return std::make_optional(ExecBatch(*batch));
      },
      MakeIteratorFromReader(reader));

  return MakeBackgroundGenerator(std::move(batch_it), io_executor, max_q, q_restart);
}

}  // namespace compute
}  // namespace arrow

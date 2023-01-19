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

#include <atomic>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/query_context.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;
using internal::ThreadPool;
using internal::ToChars;

namespace compute {

namespace {

struct ExecPlanImpl : public ExecPlan {
  explicit ExecPlanImpl(QueryOptions options, ExecContext exec_context,
                        std::shared_ptr<const KeyValueMetadata> metadata = nullptr,
                        std::shared_ptr<ThreadPool> owned_thread_pool = nullptr)
      : metadata_(std::move(metadata)),
        query_context_(options, exec_context),
        owned_thread_pool_(std::move(owned_thread_pool)) {}

  ~ExecPlanImpl() override {
    if (started_ && !finished_.is_finished()) {
      ARROW_LOG(WARNING) << "Plan was destroyed before finishing";
      StopProducing();
      finished().Wait();
    }
  }

  ExecNode* AddNode(std::unique_ptr<ExecNode> node) {
    if (node->label().empty()) {
      node->SetLabel(ToChars(auto_label_counter_++));
    }
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
    if (query_context_.exec_context()->executor() == nullptr) {
      return Status::Invalid(
          "An exec plan must have an executor for CPU tasks.  To run without threads use "
          "a SerialExeuctor (the arrow::compute::DeclarationTo... methods should take "
          "care of this for you and are an easier way to execute an ExecPlan.)");
    }
    if (query_context_.io_context()->executor() == nullptr) {
      return Status::Invalid("An exec plan must have an I/O executor for I/O tasks.");
    }

    started_ = true;

    // We call StartProducing on each of the nodes.  The source nodes should generally
    // start scheduling some tasks during this call.
    //
    // If no source node schedules any tasks (e.g. they do all their word synchronously as
    // part of StartProducing) then the plan may be finished before we return from this
    // call.
    Future<> scheduler_finished = util::AsyncTaskScheduler::Make(
        [this](util::AsyncTaskScheduler* async_scheduler) {
          QueryContext* ctx = query_context();
          RETURN_NOT_OK(ctx->Init(ctx->max_concurrency(), async_scheduler));

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
          // TODO(weston) The entire concept of ExecNode::finished() will hopefully go
          // away soon (or at least be replaced by a sub-scheduler to facilitate OT)
          for (auto& n : nodes_) {
            RETURN_NOT_OK(n->Init());
          }
          for (auto& n : nodes_) {
            async_scheduler->AddSimpleTask([&] { return n->finished(); });
          }

          ctx->scheduler()->RegisterEnd();
          int num_threads = 1;
          bool sync_execution = true;
          if (auto executor = query_context()->exec_context()->executor()) {
            num_threads = executor->GetCapacity();
            sync_execution = false;
          }
          RETURN_NOT_OK(ctx->scheduler()->StartScheduling(
              0 /* thread_index */,
              [ctx](std::function<Status(size_t)> fn) -> Status {
                return ctx->ScheduleTask(std::move(fn));
              },
              /*concurrent_tasks=*/2 * num_threads, sync_execution));

          // producers precede consumers
          sorted_nodes_ = TopoSort();

          Status st = Status::OK();

          using rev_it = std::reverse_iterator<NodeVector::iterator>;
          for (rev_it it(sorted_nodes_.end()), end(sorted_nodes_.begin()); it != end;
               ++it) {
            auto node = *it;

            EVENT(span_, "StartProducing:" + node->label(),
                  {{"node.label", node->label()}, {"node.kind_name", node->kind_name()}});
            st = node->StartProducing();
            EVENT(span_, "StartProducing:" + node->label(), {{"status", st.ToString()}});
            if (!st.ok()) {
              // Stop nodes that successfully started, in reverse order
              bool expected = false;
              if (stopped_.compare_exchange_strong(expected, true)) {
                StopProducingImpl(it.base(), sorted_nodes_.end());
                for (NodeVector::iterator fw_it = sorted_nodes_.begin();
                     fw_it != it.base(); ++fw_it) {
                  Future<> fut = (*fw_it)->finished();
                  if (!fut.is_finished()) fut.MarkFinished();
                }
              }
              return st;
            }
          }
          return st;
        },
        [this](const Status& st) { StopProducing(); });
    scheduler_finished.AddCallback(
        [this](const Status& st) { finished_.MarkFinished(st); });
    // TODO(weston) Do we really need to return status here?  Could we change this return
    // to void?
    if (finished_.is_finished()) {
      return finished_.status();
    } else {
      return Status::OK();
    }
  }

  void StopProducing() {
    DCHECK(started_) << "stopped an ExecPlan which never started";
    EVENT(span_, "StopProducing");
    bool expected = false;
    if (stopped_.compare_exchange_strong(expected, true)) {
      query_context()->scheduler()->Abort(
          [this]() { StopProducingImpl(sorted_nodes_.begin(), sorted_nodes_.end()); });
    }
  }

  template <typename It>
  void StopProducingImpl(It begin, It end) {
    for (auto it = begin; it != end; ++it) {
      auto node = *it;
      EVENT(span_, "StopProducing:" + node->label(),
            {{"node.label", node->label()}, {"node.kind_name", node->kind_name()}});
      node->StopProducing();
    }
  }

  NodeVector TopoSort() const {
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

  // This function returns a node vector and a vector of integers with the
  // number of spaces to add as an indentation. The main difference between
  // this function and the TopoSort function is that here we visit the nodes
  // in reverse order and we can have repeated nodes if necessary.
  // For example, in the following plan:
  // s1 --> s3 -
  //   -        -
  //    -        -> s5 --> s6
  //     -      -
  // s2 --> s4 -
  // Toposort node vector: s1 s2 s3 s4 s5 s6
  // OrderedNodes node vector: s6 s5 s3 s1 s4 s2 s1
  std::pair<NodeVector, std::vector<int>> OrderedNodes() const {
    struct Impl {
      const std::vector<std::unique_ptr<ExecNode>>& nodes;
      std::unordered_set<ExecNode*> visited;
      std::unordered_set<ExecNode*> marked;
      NodeVector sorted;
      std::vector<int> indents;

      explicit Impl(const std::vector<std::unique_ptr<ExecNode>>& nodes) : nodes(nodes) {
        visited.reserve(nodes.size());

        for (auto it = nodes.rbegin(); it != nodes.rend(); ++it) {
          if (visited.count(it->get()) != 0) continue;
          Visit(it->get());
        }

        DCHECK_EQ(visited.size(), nodes.size());
      }

      void Visit(ExecNode* node, int indent = 0) {
        marked.insert(node);
        for (auto input : node->inputs()) {
          if (marked.count(input) != 0) continue;
          Visit(input, indent + 1);
        }
        marked.erase(node);

        indents.push_back(indent);
        sorted.push_back(node);
        visited.insert(node);
      }
    };

    auto result = Impl{nodes_};
    return std::make_pair(result.sorted, result.indents);
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "ExecPlan with " << nodes_.size() << " nodes:" << std::endl;
    auto sorted = OrderedNodes();
    for (size_t i = sorted.first.size(); i > 0; --i) {
      for (int j = 0; j < sorted.second[i - 1]; ++j) ss << "  ";
      ss << sorted.first[i - 1]->ToString(sorted.second[i - 1]) << std::endl;
    }
    return ss.str();
  }

  Status error_st_;
  Future<> finished_ = Future<>::Make();
  bool started_ = false;
  std::atomic<bool> stopped_{false};
  std::vector<std::unique_ptr<ExecNode>> nodes_;
  NodeVector sources_, sinks_;
  NodeVector sorted_nodes_;
  uint32_t auto_label_counter_ = 0;
  util::tracing::Span span_;
  std::shared_ptr<const KeyValueMetadata> metadata_;
  QueryContext query_context_;
  // This field only exists for backwards compatibility.  Remove once the deprecated
  // ExecPlan::Make overloads have been removed.
  std::shared_ptr<ThreadPool> owned_thread_pool_;
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
    QueryOptions opts, ExecContext ctx,
    std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::shared_ptr<ExecPlan>(new ExecPlanImpl{opts, ctx, std::move(metadata)});
}

Result<std::shared_ptr<ExecPlan>> ExecPlan::Make(
    ExecContext ctx, std::shared_ptr<const KeyValueMetadata> metadata) {
  return Make(/*opts=*/{}, ctx, std::move(metadata));
}

// Deprecated and left for backwards compatibility.  If the user does not supply a CPU
// executor then we will create a 1 thread pool and tie its lifetime to the plan
Result<std::shared_ptr<ExecPlan>> ExecPlan::Make(
    QueryOptions opts, ExecContext* ctx,
    std::shared_ptr<const KeyValueMetadata> metadata) {
  if (ctx->executor() == nullptr) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> tpool, ThreadPool::Make(1));
    ExecContext actual_ctx(ctx->memory_pool(), tpool.get(), ctx->func_registry());
    return std::shared_ptr<ExecPlan>(
        new ExecPlanImpl{opts, actual_ctx, std::move(metadata), std::move(tpool)});
  }
  return ExecPlan::Make(opts, *ctx, std::move(metadata));
}

// Deprecated
Result<std::shared_ptr<ExecPlan>> ExecPlan::Make(
    ExecContext* ctx, std::shared_ptr<const KeyValueMetadata> metadata) {
  ARROW_SUPPRESS_DEPRECATION_WARNING
  return Make(/*opts=*/{}, ctx, std::move(metadata));
  ARROW_UNSUPPRESS_DEPRECATION_WARNING
}

ExecNode* ExecPlan::AddNode(std::unique_ptr<ExecNode> node) {
  return ToDerived(this)->AddNode(std::move(node));
}

const ExecPlan::NodeVector& ExecPlan::sources() const {
  return ToDerived(this)->sources_;
}

const ExecPlan::NodeVector& ExecPlan::sinks() const { return ToDerived(this)->sinks_; }

QueryContext* ExecPlan::query_context() { return &ToDerived(this)->query_context_; }

Status ExecPlan::Validate() { return ToDerived(this)->Validate(); }

Status ExecPlan::StartProducing() { return ToDerived(this)->StartProducing(); }

void ExecPlan::StopProducing() { ToDerived(this)->StopProducing(); }

Future<> ExecPlan::finished() { return ToDerived(this)->finished_; }

bool ExecPlan::HasMetadata() const { return !!(ToDerived(this)->metadata_); }

std::shared_ptr<const KeyValueMetadata> ExecPlan::metadata() const {
  return ToDerived(this)->metadata_;
}

std::string ExecPlan::ToString() const { return ToDerived(this)->ToString(); }

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

Status ExecNode::Init() { return Status::OK(); }

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

bool ExecNode::ErrorIfNotOk(Status status) {
  if (status.ok()) return false;

  for (auto out : outputs_) {
    out->ErrorReceived(this, out == outputs_.back() ? std::move(status) : status);
  }
  return true;
}

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
      while (batch != nullptr) {
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
                                                       ExecContext exec_context) {
  std::shared_ptr<std::shared_ptr<Table>> output_table =
      std::make_shared<std::shared_ptr<Table>>();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> exec_plan,
                        ExecPlan::Make(exec_context));
  Declaration with_sink = Declaration::Sequence(
      {declaration, {"table_sink", TableSinkNodeOptions(output_table.get())}});
  ARROW_RETURN_NOT_OK(with_sink.AddToPlan(exec_plan.get()));
  ARROW_RETURN_NOT_OK(exec_plan->Validate());
  ARROW_RETURN_NOT_OK(exec_plan->StartProducing());
  return exec_plan->finished().Then([exec_plan, output_table] { return *output_table; });
}

Future<std::shared_ptr<Table>> DeclarationToTableAsync(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  if (use_threads) {
    ExecContext ctx(memory_pool, ::arrow::internal::GetCpuThreadPool(),
                    function_registry);
    return DeclarationToTableAsync(std::move(declaration), ctx);
  } else {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> tpool, ThreadPool::Make(1));
    ExecContext ctx(memory_pool, tpool.get(), function_registry);
    return DeclarationToTableAsync(std::move(declaration), ctx)
        .Then([tpool](const std::shared_ptr<Table>& table) { return table; });
  }
}

Result<std::shared_ptr<Table>> DeclarationToTable(Declaration declaration,
                                                  bool use_threads,
                                                  MemoryPool* memory_pool,
                                                  FunctionRegistry* function_registry) {
  return ::arrow::internal::RunSynchronously<Future<std::shared_ptr<Table>>>(
      [=, declaration = std::move(declaration)](::arrow::internal::Executor* executor) {
        ExecContext ctx(memory_pool, executor, function_registry);
        return DeclarationToTableAsync(std::move(declaration), ctx);
      },
      use_threads);
}

Future<std::vector<std::shared_ptr<RecordBatch>>> DeclarationToBatchesAsync(
    Declaration declaration, ExecContext exec_context) {
  return DeclarationToTableAsync(std::move(declaration), exec_context)
      .Then([](const std::shared_ptr<Table>& table) {
        return TableBatchReader(table).ToRecordBatches();
      });
}

Future<std::vector<std::shared_ptr<RecordBatch>>> DeclarationToBatchesAsync(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  if (use_threads) {
    ExecContext ctx(memory_pool, ::arrow::internal::GetCpuThreadPool(),
                    function_registry);
    return DeclarationToBatchesAsync(std::move(declaration), ctx);
  } else {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> tpool, ThreadPool::Make(1));
    ExecContext ctx(memory_pool, tpool.get(), function_registry);
    return DeclarationToBatchesAsync(std::move(declaration), ctx)
        .Then([tpool](const std::vector<std::shared_ptr<RecordBatch>>& batches) {
          return batches;
        });
  }
}

Result<std::vector<std::shared_ptr<RecordBatch>>> DeclarationToBatches(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  return ::arrow::internal::RunSynchronously<
      Future<std::vector<std::shared_ptr<RecordBatch>>>>(
      [=, declaration = std::move(declaration)](::arrow::internal::Executor* executor) {
        ExecContext ctx(memory_pool, executor, function_registry);
        return DeclarationToBatchesAsync(std::move(declaration), ctx);
      },
      use_threads);
}

Future<BatchesWithCommonSchema> DeclarationToExecBatchesAsync(Declaration declaration,
                                                              ExecContext exec_context) {
  std::shared_ptr<Schema> out_schema;
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> exec_plan,
                        ExecPlan::Make(exec_context));
  Declaration with_sink = Declaration::Sequence(
      {declaration, {"sink", SinkNodeOptions(&sink_gen, &out_schema)}});
  ARROW_RETURN_NOT_OK(with_sink.AddToPlan(exec_plan.get()));
  ARROW_RETURN_NOT_OK(exec_plan->Validate());
  ARROW_RETURN_NOT_OK(exec_plan->StartProducing());
  auto collected_fut = CollectAsyncGenerator(sink_gen);
  return AllFinished({exec_plan->finished(), Future<>(collected_fut)})
      .Then([collected_fut, exec_plan,
             schema = std::move(out_schema)]() -> Result<BatchesWithCommonSchema> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        std::vector<ExecBatch> exec_batches = ::arrow::internal::MapVector(
            [](std::optional<ExecBatch> batch) { return batch.value_or(ExecBatch()); },
            std::move(collected));
        return BatchesWithCommonSchema{std::move(exec_batches), schema};
      });
}

Future<BatchesWithCommonSchema> DeclarationToExecBatchesAsync(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  if (use_threads) {
    ExecContext ctx(memory_pool, ::arrow::internal::GetCpuThreadPool(),
                    function_registry);
    return DeclarationToExecBatchesAsync(std::move(declaration), ctx);
  } else {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> tpool, ThreadPool::Make(1));
    ExecContext ctx(memory_pool, tpool.get(), function_registry);
    return DeclarationToExecBatchesAsync(std::move(declaration), ctx)
        .Then([tpool](const BatchesWithCommonSchema& batches) { return batches; });
  }
}

Result<BatchesWithCommonSchema> DeclarationToExecBatches(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  return ::arrow::internal::RunSynchronously<Future<BatchesWithCommonSchema>>(
      [=, declaration = std::move(declaration)](::arrow::internal::Executor* executor) {
        ExecContext ctx(memory_pool, executor, function_registry);
        return DeclarationToExecBatchesAsync(std::move(declaration), ctx);
      },
      use_threads);
}

Future<> DeclarationToStatusAsync(Declaration declaration, ExecContext exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> exec_plan,
                        ExecPlan::Make(exec_context));
  ARROW_ASSIGN_OR_RAISE(ExecNode * last_node, declaration.AddToPlan(exec_plan.get()));
  for (int i = 0; i < last_node->num_outputs(); i++) {
    ARROW_RETURN_NOT_OK(
        Declaration("consuming_sink", {last_node},
                    ConsumingSinkNodeOptions(NullSinkNodeConsumer::Make()))
            .AddToPlan(exec_plan.get()));
  }
  ARROW_RETURN_NOT_OK(exec_plan->Validate());
  ARROW_RETURN_NOT_OK(exec_plan->StartProducing());
  // Keep the exec_plan alive until it finishes
  return exec_plan->finished().Then([exec_plan]() {});
}

Future<> DeclarationToStatusAsync(Declaration declaration, bool use_threads,
                                  MemoryPool* memory_pool,
                                  FunctionRegistry* function_registry) {
  if (use_threads) {
    ExecContext ctx(memory_pool, ::arrow::internal::GetCpuThreadPool(),
                    function_registry);
    return DeclarationToStatusAsync(std::move(declaration), ctx);
  } else {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> tpool, ThreadPool::Make(1));
    ExecContext ctx(memory_pool, tpool.get(), function_registry);
    return DeclarationToStatusAsync(std::move(declaration), ctx).Then([tpool]() {});
  }
}

Status DeclarationToStatus(Declaration declaration, bool use_threads,
                           MemoryPool* memory_pool, FunctionRegistry* function_registry) {
  return ::arrow::internal::RunSynchronously<Future<>>(
      [=, declaration = std::move(declaration)](::arrow::internal::Executor* executor) {
        ExecContext ctx(memory_pool, executor, function_registry);
        return DeclarationToStatusAsync(std::move(declaration), ctx);
      },
      use_threads);
}

namespace {
struct BatchConverter {
  ~BatchConverter() {
    if (!exec_plan) {
      return;
    }
    if (exec_plan->finished().is_finished()) {
      return;
    }
    exec_plan->StopProducing();
    Status abandoned_status = exec_plan->finished().status();
    if (!abandoned_status.ok()) {
      abandoned_status.Warn();
    }
  }

  Future<std::shared_ptr<RecordBatch>> operator()() {
    return exec_batch_gen().Then(
        [this](const std::optional<ExecBatch>& batch)
            -> Future<std::shared_ptr<RecordBatch>> {
          if (batch) {
            return batch->ToRecordBatch(schema);
          } else {
            return exec_plan->finished().Then(
                []() -> std::shared_ptr<RecordBatch> { return nullptr; });
          }
        },
        [this](const Status& err) {
          return exec_plan->finished().Then(
              [err]() -> Result<std::shared_ptr<RecordBatch>> { return err; });
        });
  }

  AsyncGenerator<std::optional<ExecBatch>> exec_batch_gen;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<ExecPlan> exec_plan;
};

Result<AsyncGenerator<std::shared_ptr<RecordBatch>>> DeclarationToRecordBatchGenerator(
    Declaration declaration, ExecContext exec_ctx, std::shared_ptr<Schema>* out_schema) {
  auto converter = std::make_shared<BatchConverter>();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> plan, ExecPlan::Make(exec_ctx));
  Declaration with_sink = Declaration::Sequence(
      {declaration,
       {"sink", SinkNodeOptions(&converter->exec_batch_gen, &converter->schema)}});
  ARROW_RETURN_NOT_OK(with_sink.AddToPlan(plan.get()));
  ARROW_RETURN_NOT_OK(plan->StartProducing());
  converter->exec_plan = std::move(plan);
  *out_schema = converter->schema;
  return [conv = std::move(converter)] { return (*conv)(); };
}
}  // namespace

Result<std::unique_ptr<RecordBatchReader>> DeclarationToReader(
    Declaration declaration, bool use_threads, MemoryPool* memory_pool,
    FunctionRegistry* function_registry) {
  std::shared_ptr<Schema> schema;
  auto batch_iterator = std::make_unique<Iterator<std::shared_ptr<RecordBatch>>>(
      ::arrow::internal::IterateSynchronously<std::shared_ptr<RecordBatch>>(
          [&](::arrow::internal::Executor* executor)
              -> Result<AsyncGenerator<std::shared_ptr<RecordBatch>>> {
            ExecContext exec_ctx(memory_pool, executor, function_registry);
            return DeclarationToRecordBatchGenerator(declaration, exec_ctx, &schema);
          },
          use_threads));

  struct PlanReader : RecordBatchReader {
    PlanReader(std::shared_ptr<Schema> schema,
               std::unique_ptr<Iterator<std::shared_ptr<RecordBatch>>> iterator)
        : schema_(std::move(schema)), iterator_(std::move(iterator)) {}

    std::shared_ptr<Schema> schema() const override { return schema_; }

    Status ReadNext(std::shared_ptr<RecordBatch>* record_batch) override {
      DCHECK(!!iterator_) << "call to ReadNext on already closed reader";
      return iterator_->Next().Value(record_batch);
    }

    Status Close() override {
      // End plan and read from generator until finished
      std::shared_ptr<RecordBatch> batch;
      do {
        ARROW_RETURN_NOT_OK(ReadNext(&batch));
      } while (batch != nullptr);
      iterator_.reset();
      return Status::OK();
    }

    std::shared_ptr<Schema> schema_;
    std::unique_ptr<Iterator<std::shared_ptr<RecordBatch>>> iterator_;
  };

  return std::make_unique<PlanReader>(std::move(schema), std::move(batch_iterator));
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

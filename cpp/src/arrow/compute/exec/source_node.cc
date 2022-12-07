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

#include <atomic>
#include <mutex>
#include <optional>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/datum.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;
using internal::MapVector;

namespace compute {
namespace {

struct SourceNode : ExecNode {
  SourceNode(ExecPlan* plan, std::shared_ptr<Schema> output_schema,
             AsyncGenerator<std::optional<ExecBatch>> generator)
      : ExecNode(plan, {}, {}, std::move(output_schema),
                 /*num_outputs=*/1),
        generator_(std::move(generator)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "SourceNode"));
    const auto& source_options = checked_cast<const SourceNodeOptions&>(options);
    return plan->EmplaceNode<SourceNode>(plan, source_options.output_schema,
                                         source_options.generator);
  }

  const char* kind_name() const override { return "SourceNode"; }

  [[noreturn]] static void NoInputs() {
    Unreachable("no inputs; this should never be called");
  }
  [[noreturn]] void InputReceived(ExecNode*, ExecBatch) override { NoInputs(); }
  [[noreturn]] void ErrorReceived(ExecNode*, Status) override { NoInputs(); }
  [[noreturn]] void InputFinished(ExecNode*, int) override { NoInputs(); }

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.kind", kind_name()},
                        {"node.label", label()},
                        {"node.output_schema", output_schema()->ToString()},
                        {"node.detail", ToString()}});
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_);
    {
      // If another exec node encountered an error during its StartProducing call
      // it might have already called StopProducing on all of its inputs (including this
      // node).
      //
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_requested_) {
        return Status::OK();
      }
      started_ = true;
    }

    CallbackOptions options;
    auto executor = plan()->exec_context()->executor();
    if (executor) {
      // These options will transfer execution to the desired Executor if necessary.
      // This can happen for in-memory scans where batches didn't require
      // any CPU work to decode. Otherwise, parsing etc should have already
      // been placed us on the desired Executor and no queues will be pushed to.
      options.executor = executor;
      options.should_schedule = ShouldSchedule::IfDifferentExecutor;
    }
    ARROW_ASSIGN_OR_RAISE(Future<> scan_task, plan_->BeginExternalTask());
    if (!scan_task.is_valid()) {
      finished_.MarkFinished();
      // Plan has already been aborted, no need to start scanning
      return Status::OK();
    }
    auto fut = Loop([this, options] {
                 std::unique_lock<std::mutex> lock(mutex_);
                 if (stop_requested_) {
                   return Future<ControlFlow<int>>::MakeFinished(Break(batch_count_));
                 }
                 lock.unlock();

                 return generator_().Then(
                     [this](const std::optional<ExecBatch>& maybe_morsel)
                         -> Future<ControlFlow<int>> {
                       std::unique_lock<std::mutex> lock(mutex_);
                       if (IsIterationEnd(maybe_morsel) || stop_requested_) {
                         return Break(batch_count_);
                       }
                       lock.unlock();
                       bool use_legacy_batching = plan_->UseLegacyBatching();
                       ExecBatch morsel = std::move(*maybe_morsel);
                       int64_t morsel_length = static_cast<int64_t>(morsel.length);
                       if (use_legacy_batching || morsel_length == 0) {
                         // For various reasons (e.g. ARROW-13982) we pass empty batches
                         // through
                         batch_count_++;
                       } else {
                         int num_batches = static_cast<int>(
                             bit_util::CeilDiv(morsel_length, ExecPlan::kMaxBatchSize));
                         batch_count_ += num_batches;
                       }
                       RETURN_NOT_OK(plan_->ScheduleTask(
                           [this, use_legacy_batching, morsel, morsel_length]() {
                             int64_t offset = 0;
                             do {
                               int64_t batch_size = std::min<int64_t>(
                                   morsel_length - offset, ExecPlan::kMaxBatchSize);
                               // In order for the legacy batching model to work we must
                               // not slice batches from the source
                               if (use_legacy_batching) {
                                 batch_size = morsel_length;
                               }
                               ExecBatch batch = morsel.Slice(offset, batch_size);
                               offset += batch_size;
                               outputs_[0]->InputReceived(this, std::move(batch));
                             } while (offset < morsel.length);
                             return Status::OK();
                           }));
                       lock.lock();
                       if (!backpressure_future_.is_finished()) {
                         EVENT(span_, "Source paused due to backpressure");
                         return backpressure_future_.Then(
                             []() -> ControlFlow<int> { return Continue(); });
                       }
                       return Future<ControlFlow<int>>::MakeFinished(Continue());
                     },
                     [this](const Status& error) -> ControlFlow<int> {
                       outputs_[0]->ErrorReceived(this, error);
                       return Break(batch_count_);
                     },
                     options);
               })
                   .Then(
                       [this, scan_task](int total_batches) mutable {
                         outputs_[0]->InputFinished(this, total_batches);
                         scan_task.MarkFinished();
                         finished_.MarkFinished();
                       },
                       {}, options);
    if (!executor && finished_.is_finished()) return finished_.status();
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    std::lock_guard<std::mutex> lg(mutex_);
    if (counter <= backpressure_counter_) {
      return;
    }
    backpressure_counter_ = counter;
    if (!backpressure_future_.is_finished()) {
      // Could happen if we get something like Pause(1) Pause(3) Resume(2)
      return;
    }
    backpressure_future_ = Future<>::Make();
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    Future<> to_finish;
    {
      std::lock_guard<std::mutex> lg(mutex_);
      if (counter <= backpressure_counter_) {
        return;
      }
      backpressure_counter_ = counter;
      if (backpressure_future_.is_finished()) {
        return;
      }
      to_finish = backpressure_future_;
    }
    to_finish.MarkFinished();
  }

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_requested_ = true;
    if (!started_) {
      finished_.MarkFinished();
    }
  }

 private:
  std::mutex mutex_;
  std::atomic<int32_t> backpressure_counter_{0};
  Future<> backpressure_future_ = Future<>::MakeFinished();
  bool stop_requested_{false};
  bool started_ = false;
  int batch_count_{0};
  AsyncGenerator<std::optional<ExecBatch>> generator_;
};

struct TableSourceNode : public SourceNode {
  TableSourceNode(ExecPlan* plan, std::shared_ptr<Table> table, int64_t batch_size)
      : SourceNode(plan, table->schema(), TableGenerator(*table, batch_size)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "TableSourceNode"));
    const auto& table_options = checked_cast<const TableSourceNodeOptions&>(options);
    const auto& table = table_options.table;
    const int64_t batch_size = table_options.max_batch_size;

    RETURN_NOT_OK(ValidateTableSourceNodeInput(table, batch_size));

    return plan->EmplaceNode<TableSourceNode>(plan, table, batch_size);
  }

  const char* kind_name() const override { return "TableSourceNode"; }

  static arrow::Status ValidateTableSourceNodeInput(const std::shared_ptr<Table> table,
                                                    const int64_t batch_size) {
    if (table == nullptr) {
      return Status::Invalid("TableSourceNode requires table which is not null");
    }

    if (batch_size <= 0) {
      return Status::Invalid(
          "TableSourceNode node requires, batch_size > 0 , but got batch size ",
          batch_size);
    }

    return Status::OK();
  }

  static arrow::AsyncGenerator<std::optional<ExecBatch>> TableGenerator(
      const Table& table, const int64_t batch_size) {
    auto batches = ConvertTableToExecBatches(table, batch_size);
    auto opt_batches =
        MapVector([](ExecBatch batch) { return std::make_optional(std::move(batch)); },
                  std::move(batches));
    AsyncGenerator<std::optional<ExecBatch>> gen;
    gen = MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }

  static std::vector<ExecBatch> ConvertTableToExecBatches(const Table& table,
                                                          const int64_t batch_size) {
    std::shared_ptr<TableBatchReader> reader = std::make_shared<TableBatchReader>(table);

    // setting chunksize for the batch reader
    reader->set_chunksize(batch_size);

    std::shared_ptr<RecordBatch> batch;
    std::vector<ExecBatch> exec_batches;
    while (true) {
      auto batch_res = reader->Next();
      if (batch_res.ok()) {
        batch = std::move(batch_res).MoveValueUnsafe();
      }
      if (batch == NULLPTR) {
        break;
      }
      exec_batches.emplace_back(*batch);
    }
    return exec_batches;
  }
};

template <typename This, typename Options>
struct SchemaSourceNode : public SourceNode {
  SchemaSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema,
                   arrow::AsyncGenerator<std::optional<ExecBatch>> generator)
      : SourceNode(plan, schema, generator) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, This::kKindName));
    const auto& cast_options = checked_cast<const Options&>(options);
    auto& it_maker = cast_options.it_maker;
    auto& schema = cast_options.schema;
    auto io_executor = cast_options.io_executor;

    if (io_executor == NULLPTR) {
      io_executor = plan->exec_context()->executor();
    }
    auto it = it_maker();

    if (schema == NULLPTR) {
      return Status::Invalid(This::kKindName, " requires schema which is not null");
    }
    if (io_executor == NULLPTR) {
      io_executor = io::internal::GetIOThreadPool();
    }

    ARROW_ASSIGN_OR_RAISE(auto generator, This::MakeGenerator(it, io_executor, schema));
    return plan->EmplaceNode<This>(plan, schema, generator);
  }
};

struct RecordBatchSourceNode
    : public SchemaSourceNode<RecordBatchSourceNode, RecordBatchSourceNodeOptions> {
  using RecordBatchSchemaSourceNode =
      SchemaSourceNode<RecordBatchSourceNode, RecordBatchSourceNodeOptions>;

  using RecordBatchSchemaSourceNode::RecordBatchSchemaSourceNode;

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    return RecordBatchSchemaSourceNode::Make(plan, inputs, options);
  }

  const char* kind_name() const override { return kKindName; }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      Iterator<std::shared_ptr<RecordBatch>>& batch_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [schema](const std::shared_ptr<RecordBatch>& batch) -> std::optional<ExecBatch> {
      if (batch == NULLPTR || *batch->schema() != *schema) {
        return std::nullopt;
      }
      return std::optional<ExecBatch>(ExecBatch(*batch));
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

  static const char kKindName[];
};

const char RecordBatchSourceNode::kKindName[] = "RecordBatchSourceNode";

struct ExecBatchSourceNode
    : public SchemaSourceNode<ExecBatchSourceNode, ExecBatchSourceNodeOptions> {
  using ExecBatchSchemaSourceNode =
      SchemaSourceNode<ExecBatchSourceNode, ExecBatchSourceNodeOptions>;

  using ExecBatchSchemaSourceNode::ExecBatchSchemaSourceNode;

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    return ExecBatchSchemaSourceNode::Make(plan, inputs, options);
  }

  const char* kind_name() const override { return kKindName; }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      Iterator<std::shared_ptr<ExecBatch>>& batch_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [](const std::shared_ptr<ExecBatch>& batch) -> std::optional<ExecBatch> {
      return batch == NULLPTR ? std::nullopt : std::optional<ExecBatch>(*batch);
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

  static const char kKindName[];
};

const char ExecBatchSourceNode::kKindName[] = "ExecBatchSourceNode";

struct ArrayVectorSourceNode
    : public SchemaSourceNode<ArrayVectorSourceNode, ArrayVectorSourceNodeOptions> {
  using ArrayVectorSchemaSourceNode =
      SchemaSourceNode<ArrayVectorSourceNode, ArrayVectorSourceNodeOptions>;

  using ArrayVectorSchemaSourceNode::ArrayVectorSchemaSourceNode;

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    return ArrayVectorSchemaSourceNode::Make(plan, inputs, options);
  }

  const char* kind_name() const override { return kKindName; }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      Iterator<std::shared_ptr<ArrayVector>>& arrayvec_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [](const std::shared_ptr<ArrayVector>& arrayvec) -> std::optional<ExecBatch> {
      if (arrayvec == NULLPTR || arrayvec->size() == 0) {
        return std::nullopt;
      }
      std::vector<Datum> datumvec;
      for (const auto& array : *arrayvec) {
        datumvec.push_back(Datum(array));
      }
      return std::optional<ExecBatch>(
          ExecBatch(std::move(datumvec), (*arrayvec)[0]->length()));
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(arrayvec_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

  static const char kKindName[];
};

const char ArrayVectorSourceNode::kKindName[] = "ArrayVectorSourceNode";

}  // namespace

namespace internal {

void RegisterSourceNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("source", SourceNode::Make));
  DCHECK_OK(registry->AddFactory("table_source", TableSourceNode::Make));
  DCHECK_OK(registry->AddFactory("record_batch_source", RecordBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("exec_batch_source", ExecBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("array_vector_source", ArrayVectorSourceNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

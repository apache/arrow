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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/io/util_internal.h"
#include "arrow/ipc/util.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/align_util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

using namespace std::string_view_literals;  // NOLINT

namespace arrow {

using arrow::internal::checked_cast;
using arrow::internal::MapVector;

namespace acero {
namespace {

Status HandleUnalignedBuffers(ExecBatch* batch, UnalignedBufferHandling handling) {
  if (handling == UnalignedBufferHandling::kIgnore) {
    return Status::OK();
  }
  for (auto& value : batch->values) {
    if (value.is_array()) {
      switch (handling) {
        case UnalignedBufferHandling::kIgnore:
          // Should be impossible to get here
          return Status::OK();
        case UnalignedBufferHandling::kError:
          if (!arrow::util::CheckAlignment(*value.array(),
                                           arrow::util::kValueAlignment)) {
            return Status::Invalid(
                "An input buffer was poorly aligned and UnalignedBufferHandling is set "
                "to kError");
          }
          break;
        case UnalignedBufferHandling::kWarn:
          if (!arrow::util::CheckAlignment(*value.array(),
                                           arrow::util::kValueAlignment)) {
            ARROW_LOG(WARNING)
                << "An input buffer was poorly aligned.  This could lead to crashes or "
                   "poor performance on some hardware.  Please ensure that all Acero "
                   "sources generate aligned buffers, or change the unaligned buffer "
                   "handling configuration to silence this warning.";
          }
          break;
        case UnalignedBufferHandling::kReallocate: {
          ARROW_ASSIGN_OR_RAISE(value, arrow::util::EnsureAlignment(
                                           value.array(), arrow::util::kValueAlignment,
                                           default_memory_pool()));
          break;
        }
      }
    }
  }
  return Status::OK();
}

struct SourceNode : ExecNode, public TracedNode {
  SourceNode(ExecPlan* plan, std::shared_ptr<Schema> output_schema,
             AsyncGenerator<std::optional<ExecBatch>> generator,
             Ordering ordering = Ordering::Unordered())
      : ExecNode(plan, {}, {}, std::move(output_schema)),
        TracedNode(this),
        generator_(std::move(generator)),
        ordering_(ordering) {}

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
  [[noreturn]] Status InputReceived(ExecNode*, ExecBatch) override { NoInputs(); }
  [[noreturn]] Status InputFinished(ExecNode*, int) override { NoInputs(); }

  void SliceAndDeliverMorsel(const ExecBatch& morsel) {
    bool use_legacy_batching = plan_->query_context()->options().use_legacy_batching;
    int64_t morsel_length = static_cast<int64_t>(morsel.length);
    int initial_batch_index = batch_count_;
    if (use_legacy_batching || morsel_length == 0) {
      // For various reasons (e.g. ARROW-13982) we pass empty batches
      // through
      batch_count_++;
    } else {
      int num_batches =
          static_cast<int>(bit_util::CeilDiv(morsel_length, ExecPlan::kMaxBatchSize));
      batch_count_ += num_batches;
    }
    plan_->query_context()->ScheduleTask(
        [this, morsel_length, use_legacy_batching, initial_batch_index, morsel,
         has_ordering = !ordering_.is_unordered()]() {
          int64_t offset = 0;
          int batch_index = initial_batch_index;
          do {
            int64_t batch_size =
                std::min<int64_t>(morsel_length - offset, ExecPlan::kMaxBatchSize);
            // In order for the legacy batching model to work we must
            // not slice batches from the source
            if (use_legacy_batching) {
              batch_size = morsel_length;
            }
            ExecBatch batch = morsel.Slice(offset, batch_size);
            UnalignedBufferHandling unaligned_buffer_handling =
                plan_->query_context()->options().unaligned_buffer_handling.value_or(
                    GetDefaultUnalignedBufferHandling());
            ARROW_RETURN_NOT_OK(
                HandleUnalignedBuffers(&batch, unaligned_buffer_handling));
            if (has_ordering) {
              batch.index = batch_index;
            }
            offset += batch_size;
            batch_index++;
            ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
          } while (offset < morsel.length);
          return Status::OK();
        },
        "SourceNode::ProcessMorsel");
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
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
    // These options will transfer execution to the desired Executor if necessary.
    // This can happen for in-memory scans where batches didn't require
    // any CPU work to decode. Otherwise, parsing etc should have already
    // been placed us on the desired Executor and no queues will be pushed to.
    options.executor = plan()->query_context()->executor();
    options.should_schedule = ShouldSchedule::IfDifferentExecutor;
    ARROW_ASSIGN_OR_RAISE(Future<> scan_task, plan_->query_context()->BeginExternalTask(
                                                  "SourceNode::DatasetScan"));
    if (!scan_task.is_valid()) {
      // Plan has already been aborted, no need to start scanning
      return Status::OK();
    }
    auto fut = Loop([this, options] {
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_requested_) {
        return Future<ControlFlow<int>>::MakeFinished(Break(batch_count_));
      }
      lock.unlock();

      arrow::util::tracing::Span fetch_batch_span;
      auto fetch_batch_scope =
          START_SCOPED_SPAN(fetch_batch_span, "SourceNode::ReadBatch");
      return generator_().Then(
          [this](
              const std::optional<ExecBatch>& morsel_or_end) -> Future<ControlFlow<int>> {
            std::unique_lock<std::mutex> lock(mutex_);
            if (IsIterationEnd(morsel_or_end) || stop_requested_) {
              return Break(batch_count_);
            }
            lock.unlock();
            SliceAndDeliverMorsel(*morsel_or_end);
            lock.lock();
            if (!backpressure_future_.is_finished()) {
              EVENT_ON_CURRENT_SPAN("SourceNode::BackpressureApplied");
              return backpressure_future_.Then(
                  []() -> ControlFlow<int> { return Continue(); });
            }
            return Future<ControlFlow<int>>::MakeFinished(Continue());
          },
          [](const Status& err) -> Future<ControlFlow<int>> { return err; }, options);
    });
    fut.AddCallback(
        [this, scan_task](Result<int> maybe_total_batches) mutable {
          if (maybe_total_batches.ok()) {
            plan_->query_context()->ScheduleTask(
                [this, total_batches = *maybe_total_batches] {
                  return output_->InputFinished(this, total_batches);
                },
                "SourceNode::InputFinished");
          }
          scan_task.MarkFinished(maybe_total_batches.status());
        },
        options);
    return Status::OK();
  }

  const Ordering& ordering() const override { return ordering_; }

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

  Status StopProducingImpl() override {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_requested_ = true;
    return Status::OK();
  }

 private:
  std::mutex mutex_;
  std::atomic<int32_t> backpressure_counter_{0};
  Future<> backpressure_future_ = Future<>::MakeFinished();
  bool stop_requested_{false};
  bool started_ = false;
  int batch_count_{0};
  const AsyncGenerator<std::optional<ExecBatch>> generator_;
  const Ordering ordering_;
};

struct TableSourceNode : public SourceNode {
  TableSourceNode(ExecPlan* plan, std::shared_ptr<Table> table, int64_t batch_size)
      : SourceNode(plan, table->schema(), TableGenerator(*table, batch_size),
                   Ordering::Implicit()) {}

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
      : SourceNode(plan, schema, generator, Ordering::Implicit()) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, This::kKindName));
    const auto& cast_options = checked_cast<const Options&>(options);
    auto& it_maker = cast_options.it_maker;
    auto& schema = cast_options.schema;
    auto io_executor = cast_options.io_executor;

    auto it = it_maker();

    if (schema == nullptr) {
      return Status::Invalid(This::kKindName, " requires schema which is not null");
    }

    if (cast_options.requires_io) {
      if (io_executor == nullptr) {
        io_executor = io::internal::GetIOThreadPool();
      }
    } else {
      if (io_executor != nullptr) {
        return Status::Invalid(
            This::kKindName,
            " specified with requires_io=false but io_executor was not nullptr");
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto generator, This::MakeGenerator(it, io_executor, schema));
    return plan->EmplaceNode<This>(plan, schema, generator);
  }
};

struct RecordBatchReaderSourceNode : public SourceNode {
  RecordBatchReaderSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema,
                              arrow::AsyncGenerator<std::optional<ExecBatch>> generator)
      : SourceNode(plan, schema, generator) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName));
    const auto& cast_options =
        checked_cast<const RecordBatchReaderSourceNodeOptions&>(options);
    auto& reader = cast_options.reader;
    auto io_executor = cast_options.io_executor;

    if (reader == nullptr) {
      return Status::Invalid(kKindName, " requires a reader which is not null");
    }

    if (io_executor == nullptr) {
      io_executor = io::internal::GetIOThreadPool();
    }

    ARROW_ASSIGN_OR_RAISE(auto generator, MakeGenerator(reader, io_executor));
    return plan->EmplaceNode<RecordBatchReaderSourceNode>(plan, reader->schema(),
                                                          generator);
  }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      const std::shared_ptr<RecordBatchReader>& reader,
      arrow::internal::Executor* io_executor) {
    auto to_exec_batch =
        [](const std::shared_ptr<RecordBatch>& batch) -> std::optional<ExecBatch> {
      if (batch == NULLPTR) {
        return std::nullopt;
      }
      return std::optional<ExecBatch>(ExecBatch(*batch));
    };
    Iterator<std::shared_ptr<RecordBatch>> batch_it = MakeIteratorFromReader(reader);
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    if (io_executor == nullptr) {
      return MakeBlockingGenerator(std::move(exec_batch_it));
    }
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

  static const char kKindName[];
};

const char RecordBatchReaderSourceNode::kKindName[] = "RecordBatchReaderSourceNode";

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
    if (io_executor == nullptr) {
      return MakeBlockingGenerator(std::move(exec_batch_it));
    }
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
    if (io_executor == nullptr) {
      return MakeBlockingGenerator(std::move(exec_batch_it));
    }
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
    if (io_executor == nullptr) {
      return MakeBlockingGenerator(std::move(exec_batch_it));
    }
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

  static const char kKindName[];
};

const char ArrayVectorSourceNode::kKindName[] = "ArrayVectorSourceNode";

Result<ExecNode*> MakeNamedTableNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                     const ExecNodeOptions& options) {
  return Status::Invalid(
      "The named table node is for serialization purposes only and can never be "
      "converted into an exec plan or executed");
}

}  // namespace

namespace internal {

void RegisterSourceNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("source", SourceNode::Make));
  DCHECK_OK(registry->AddFactory("table_source", TableSourceNode::Make));
  DCHECK_OK(registry->AddFactory("record_batch_source", RecordBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("record_batch_reader_source",
                                 RecordBatchReaderSourceNode::Make));
  DCHECK_OK(registry->AddFactory("exec_batch_source", ExecBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("array_vector_source", ArrayVectorSourceNode::Make));
  DCHECK_OK(registry->AddFactory("named_table", MakeNamedTableNode));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow

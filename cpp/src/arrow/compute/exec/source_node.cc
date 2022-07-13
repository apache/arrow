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

#include <mutex>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"
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
             AsyncGenerator<util::optional<ExecBatch>> generator)
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
    {
      // If another exec node encountered an error during its StartProducing call
      // it might have already called StopProducing on all of its inputs (including this
      // node).
      //
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_requested_) {
        return Status::OK();
      }
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
    finished_ = Loop([this, executor, options] {
                  std::unique_lock<std::mutex> lock(mutex_);
                  int total_batches = batch_count_++;
                  if (stop_requested_) {
                    return Future<ControlFlow<int>>::MakeFinished(Break(total_batches));
                  }
                  lock.unlock();

                  return generator_().Then(
                      [=](const util::optional<ExecBatch>& maybe_batch)
                          -> Future<ControlFlow<int>> {
                        std::unique_lock<std::mutex> lock(mutex_);
                        if (IsIterationEnd(maybe_batch) || stop_requested_) {
                          stop_requested_ = true;
                          return Break(total_batches);
                        }
                        lock.unlock();
                        ExecBatch batch = std::move(*maybe_batch);

                        if (executor) {
                          auto status = task_group_.AddTask(
                              [this, executor, batch]() -> Result<Future<>> {
                                return executor->Submit([=]() {
                                  outputs_[0]->InputReceived(this, std::move(batch));
                                  return Status::OK();
                                });
                              });
                          if (!status.ok()) {
                            outputs_[0]->ErrorReceived(this, std::move(status));
                            return Break(total_batches);
                          }
                        } else {
                          outputs_[0]->InputReceived(this, std::move(batch));
                        }
                        lock.lock();
                        if (!backpressure_future_.is_finished()) {
                          EVENT(span_, "Source paused due to backpressure");
                          return backpressure_future_.Then(
                              []() -> ControlFlow<int> { return Continue(); });
                        }
                        return Future<ControlFlow<int>>::MakeFinished(Continue());
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
                        return Break(total_batches);
                      },
                      options);
                }).Then([&](int total_batches) {
      outputs_[0]->InputFinished(this, total_batches);
      return task_group_.End();
    });
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_, this);
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
  }

  Future<> finished() override { return finished_; }

 private:
  std::mutex mutex_;
  int32_t backpressure_counter_{0};
  Future<> backpressure_future_ = Future<>::MakeFinished();
  bool stop_requested_{false};
  int batch_count_{0};
  util::AsyncTaskGroup task_group_;
  AsyncGenerator<util::optional<ExecBatch>> generator_;
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

  static arrow::AsyncGenerator<util::optional<ExecBatch>> TableGenerator(
      const Table& table, const int64_t batch_size) {
    auto batches = ConvertTableToExecBatches(table, batch_size);
    auto opt_batches =
        MapVector([](ExecBatch batch) { return util::make_optional(std::move(batch)); },
                  std::move(batches));
    AsyncGenerator<util::optional<ExecBatch>> gen;
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

struct RecordBatchSourceNode : public SourceNode {
  RecordBatchSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema,
                        arrow::AsyncGenerator<util::optional<ExecBatch>> generator)
      : SourceNode(plan, schema, generator) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName.c_str()));
    const auto& rb_options = checked_cast<const RecordBatchSourceNodeOptions&>(options);
    auto& batch_it_maker = rb_options.batch_it_maker;
    auto& schema = rb_options.schema;

    auto io_executor = plan->exec_context()->executor();
    auto batch_it = batch_it_maker();

    RETURN_NOT_OK(ValidateRecordBatchSourceNodeInput(io_executor, schema));
    ARROW_ASSIGN_OR_RAISE(auto generator,
                          RecordBatchGenerator(batch_it, io_executor, schema));
    return plan->EmplaceNode<RecordBatchSourceNode>(plan, schema, generator);
  }

  const char* kind_name() const override { return kKindName.c_str(); }

  static arrow::Status ValidateRecordBatchSourceNodeInput(
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    if (schema == NULLPTR) {
      return Status::Invalid(kKindName + " requires schema which is not null");
    }
    if (io_executor == NULLPTR) {
      return Status::Invalid(kKindName + " requires IO-context which is not null");
    }

    return Status::OK();
  }

  static Result<arrow::AsyncGenerator<util::optional<ExecBatch>>> RecordBatchGenerator(
      Iterator<std::shared_ptr<RecordBatch>>& batch_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [&schema](const std::shared_ptr<RecordBatch>& batch) -> util::optional<ExecBatch> {
      if (batch == NULLPTR || batch->schema() != schema) {
        return util::nullopt;
      }
      return util::optional<ExecBatch>(ExecBatch(*batch));
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

 private:
  static const std::string kKindName;
};

const std::string RecordBatchSourceNode::kKindName = "RecordBatchSourceNode";

struct ExecBatchSourceNode : public SourceNode {
  ExecBatchSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema,
                      arrow::AsyncGenerator<util::optional<ExecBatch>> generator)
      : SourceNode(plan, schema, generator) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName.c_str()));
    const auto& eb_options = checked_cast<const ExecBatchSourceNodeOptions&>(options);
    auto& batch_it_maker = eb_options.batch_it_maker;
    auto& schema = eb_options.schema;

    auto io_executor = plan->exec_context()->executor();
    auto batch_it = batch_it_maker();

    RETURN_NOT_OK(ValidateExecBatchSourceNodeInput(io_executor, schema));
    ARROW_ASSIGN_OR_RAISE(auto generator,
                          ExecBatchGenerator(batch_it, io_executor, schema));
    return plan->EmplaceNode<ExecBatchSourceNode>(plan, schema, generator);
  }

  const char* kind_name() const override { return kKindName.c_str(); }

  static arrow::Status ValidateExecBatchSourceNodeInput(
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    if (schema == NULLPTR) {
      return Status::Invalid(kKindName + " requires schema which is not null");
    }
    if (io_executor == NULLPTR) {
      return Status::Invalid(kKindName + " requires IO-context which is not null");
    }

    return Status::OK();
  }

  static Result<arrow::AsyncGenerator<util::optional<ExecBatch>>> ExecBatchGenerator(
      Iterator<std::shared_ptr<ExecBatch>>& batch_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [&schema](const std::shared_ptr<ExecBatch>& batch) -> util::optional<ExecBatch> {
      return batch == NULLPTR ? util::nullopt : util::optional<ExecBatch>(*batch);
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

 private:
  static const std::string kKindName;
};

const std::string ExecBatchSourceNode::kKindName = "ExecBatchSourceNode";

struct ArrayVectorSourceNode : public SourceNode {
  ArrayVectorSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema,
                        arrow::AsyncGenerator<util::optional<ExecBatch>> generator)
      : SourceNode(plan, schema, generator) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName.c_str()));
    const auto& av_options = checked_cast<const ArrayVectorSourceNodeOptions&>(options);
    auto& arrayvec_it_maker = av_options.arrayvec_it_maker;
    auto& schema = av_options.schema;

    auto io_executor = plan->exec_context()->executor();
    auto arrayvec_it = arrayvec_it_maker();

    RETURN_NOT_OK(ValidateArrayVectorSourceNodeInput(io_executor, schema));
    ARROW_ASSIGN_OR_RAISE(auto generator,
                          ArrayVectorGenerator(arrayvec_it, io_executor, schema));
    return plan->EmplaceNode<ArrayVectorSourceNode>(plan, schema, generator);
  }

  const char* kind_name() const override { return kKindName.c_str(); }

  static arrow::Status ValidateArrayVectorSourceNodeInput(
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    if (schema == NULLPTR) {
      return Status::Invalid(kKindName + " requires schema which is not null");
    }
    if (io_executor == NULLPTR) {
      return Status::Invalid(kKindName + " requires IO-context which is not null");
    }

    return Status::OK();
  }

  static Result<arrow::AsyncGenerator<util::optional<ExecBatch>>> ArrayVectorGenerator(
      Iterator<std::shared_ptr<ArrayVector>>& arrayvec_it,
      arrow::internal::Executor* io_executor, const std::shared_ptr<Schema>& schema) {
    auto to_exec_batch =
        [&schema](const std::shared_ptr<ArrayVector>& arrayvec) -> util::optional<ExecBatch> {
      if (arrayvec == NULLPTR || arrayvec->size() == 0) {
        return util::nullopt;
      }
      std::vector<Datum> datumvec;
      for (const auto& array : *arrayvec) {
        datumvec.push_back(Datum(array));
      }
      return util::optional<ExecBatch>(ExecBatch(std::move(datumvec),
                                                 (*arrayvec)[0]->length()));
    };
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(arrayvec_it));
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

 private:
  static const std::string kKindName;
};

const std::string ArrayVectorSourceNode::kKindName = "ArrayVectorSourceNode";

Result<std::shared_ptr<RecordBatch>> MakeDatumRecordBatch(std::shared_ptr<Schema> schema,
                                                          Datum& datum) {
  if (!datum.is_array()) {
    return Status::Invalid("datum of non-array kind");
  }
  ArrayData* data = datum.mutable_array();
  if (data->child_data.size() != static_cast<size_t>(schema->num_fields())) {
    return Status::Invalid("data with shape not conforming to schema");
  }
  return RecordBatch::Make(std::move(schema), data->length, std::move(data->child_data));
}

Result<ExecNode*> MakeFunctionSourceNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                         const ExecNodeOptions& options) {
  const auto& fs_options = checked_cast<const FunctionSourceNodeOptions&>(options);
  const std::string function_name = fs_options.function_name;
  const std::string source_node_factory = fs_options.source_node_factory;

  auto func_registry = plan->exec_context()->func_registry();
  ARROW_ASSIGN_OR_RAISE(auto func, func_registry->GetFunction(function_name));
  if (func->kind() != Function::SCALAR) {
    return Status::Invalid("source function of non-scalar kind");
  }
  auto arity = func->arity();
  if (arity.num_args != 0 || arity.is_varargs) {
    return Status::Invalid("source function of non-null arity");
  }
  auto kernels = ::arrow::internal::checked_pointer_cast<ScalarFunction>(func)->kernels();
  if (kernels.size() != 1) {
    return Status::Invalid("source function with non-single kernel");
  }
  const ScalarKernel* kernel = kernels[0];
  auto out_type = kernel->signature->out_type();
  if (out_type.kind() != OutputType::FIXED) {
    return Status::Invalid("source kernel of non-fixed kind");
  }
  auto datatype = out_type.type();
  if (datatype->id() != Type::type::STRUCT) {
    return Status::Invalid("source kernel with non-struct output");
  }
  auto fields = checked_cast<const StructType*>(datatype.get())->fields();
  auto schema = ::arrow::schema(fields);
  if (source_node_factory == "record_source") {
    auto next_func = [function_name, schema] () -> Result<std::shared_ptr<RecordBatch>> {
      std::vector<Datum> args;
      ARROW_ASSIGN_OR_RAISE(auto datum, CallFunction(function_name, args));
      return MakeDatumRecordBatch(std::move(schema), datum);
    };
    RecordBatchSourceNodeOptions source_node_options{
        schema, [next_func] { return MakeFunctionIterator(next_func); }};
    return MakeExecNode(source_node_factory, plan, std::move(inputs),
                        std::move(source_node_options));
  }
  return Status::Invalid("source node factory unknown");
}

}  // namespace

namespace internal {

void RegisterSourceNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("source", SourceNode::Make));
  DCHECK_OK(registry->AddFactory("table_source", TableSourceNode::Make));
  DCHECK_OK(registry->AddFactory("record_source", RecordBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("exec_source", ExecBatchSourceNode::Make));
  DCHECK_OK(registry->AddFactory("array_source", ArrayVectorSourceNode::Make));
  DCHECK_OK(registry->AddFactory("function_source", MakeFunctionSourceNode));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

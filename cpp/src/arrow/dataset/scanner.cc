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

#include "arrow/dataset/scanner.h"

#include <algorithm>
#include <memory>
#include <mutex>

#include "arrow/compute/api_scalar.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace dataset {

std::vector<std::string> ScanOptions::MaterializedFields() const {
  std::vector<std::string> fields;

  for (const Expression* expr : {&filter, &projection}) {
    for (const FieldRef& ref : FieldsInExpression(*expr)) {
      DCHECK(ref.name());
      fields.push_back(*ref.name());
    }
  }

  return fields;
}

using arrow::internal::TaskGroup;

std::shared_ptr<TaskGroup> ScanOptions::TaskGroup() const {
  if (use_threads) {
    auto* thread_pool = arrow::internal::GetCpuThreadPool();
    return TaskGroup::MakeThreaded(thread_pool);
  }
  return TaskGroup::MakeSerial();
}

Result<RecordBatchGenerator> InMemoryScanTask::ExecuteAsync() {
  return MakeVectorGenerator(record_batches_);
}

class Scanner::FilterAndProjectScanTask : public ScanTask {
 public:
  explicit FilterAndProjectScanTask(std::shared_ptr<ScanTask> task, Expression partition)
      : ScanTask(task->options(), task->fragment()),
        task_(std::move(task)),
        partition_(std::move(partition)) {}

  Result<RecordBatchGenerator> ExecuteAsync() override {
    ARROW_ASSIGN_OR_RAISE(auto rbs, task_->ExecuteAsync());
    ARROW_ASSIGN_OR_RAISE(Expression simplified_filter,
                          SimplifyWithGuarantee(options()->filter, partition_));

    ARROW_ASSIGN_OR_RAISE(Expression simplified_projection,
                          SimplifyWithGuarantee(options()->projection, partition_));

    RecordBatchGenerator filtered_rbs =
        AddFiltering(rbs, simplified_filter, options_->pool);

    return AddProjection(std::move(filtered_rbs), simplified_projection, options_->pool);
  }

 private:
  std::shared_ptr<ScanTask> task_;
  Expression partition_;
};

Result<RecordBatchIterator> ScanTask::Execute() {
  ARROW_ASSIGN_OR_RAISE(auto gen, ExecuteAsync());
  return MakeGeneratorIterator(gen);
}

Future<FragmentVector> Scanner::GetFragmentsAsync() {
  if (fragment_ != nullptr) {
    return Future<FragmentVector>::MakeFinished(FragmentVector{fragment_});
  }

  // Transform Datasets in a flat Iterator<Fragment>. This
  // iterator is lazily constructed, i.e. Dataset::GetFragments is
  // not invoked until a Fragment is requested.
  return GetFragmentsFromDatasets({dataset_}, scan_options_->filter);
}

Future<ScanTaskGenerator> Scanner::ScanAsync() {
  auto unordered_generator_fut = ScanUnorderedAsync();
  return unordered_generator_fut.Then([](const PositionedScanTaskGenerator&
                                             unordered_generator) -> ScanTaskGenerator {
    auto cmp = [](const PositionedScanTask& left, const PositionedScanTask& right) {
      if (left.fragment_index > right.fragment_index) {
        return true;
      }
      return left.scan_task_index > right.scan_task_index;
    };
    auto is_next = [](const PositionedScanTask& last,
                      const PositionedScanTask& maybe_next) {
      if (last.scan_task == nullptr) {
        return maybe_next.fragment_index == 0 && maybe_next.scan_task_index == 0;
      }
      if (maybe_next.fragment_index == last.fragment_index) {
        return maybe_next.scan_task_index == last.scan_task_index + 1;
      }
      if (maybe_next.fragment_index == last.fragment_index + 1) {
        return last.last_scan_task;
      }
      return false;
    };
    PositionedScanTaskGenerator ordered_generator = MakeSequencingGenerator(
        std::move(unordered_generator), cmp, is_next, PositionedScanTask::BeforeAny());

    std::function<std::shared_ptr<ScanTask>(const PositionedScanTask&)> unwrap_scan_task =
        [](const PositionedScanTask& positioned_task) {
          return positioned_task.scan_task;
        };
    return MakeMappedGenerator(std::move(ordered_generator), unwrap_scan_task);
  });
}

Future<PositionedScanTaskGenerator> Scanner::ScanUnorderedAsync() {
  // Transforms Iterator<Fragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.

  // FIXME: Should just return ScanTaskGenerator, not Future<ScanTaskGenerator>
  return GetFragmentsAsync().Then(
      [this](const FragmentVector& fragments) -> Result<PositionedScanTaskGenerator> {
        ARROW_ASSIGN_OR_RAISE(auto scan_task_generator,
                              GetUnorderedScanTaskGenerator(fragments, scan_options_));
        return MakeReadaheadGenerator(scan_task_generator,
                                      static_cast<int>(scan_options_->block_readahead));
      });
}

Result<ScanTaskIterator> Scanner::Scan() {
  auto scan_fut = ScanAsync();
  scan_fut.Wait();
  ARROW_ASSIGN_OR_RAISE(auto gen, scan_fut.result());
  return MakeGeneratorIterator(std::move(gen));
}

Result<std::shared_ptr<RecordBatch>> Scanner::FilterRecordBatch(
    const Expression& filter, MemoryPool* pool, const std::shared_ptr<RecordBatch>& in) {
  compute::ExecContext exec_context{pool};
  ARROW_ASSIGN_OR_RAISE(Datum mask,
                        ExecuteScalarExpression(filter, Datum(in), &exec_context));

  if (mask.is_scalar()) {
    const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
    if (mask_scalar.is_valid && mask_scalar.value) {
      return std::move(in);
    }
    return in->Slice(0, 0);
  }

  ARROW_ASSIGN_OR_RAISE(
      Datum filtered,
      compute::Filter(in, mask, compute::FilterOptions::Defaults(), &exec_context));
  return filtered.record_batch();
}

RecordBatchGenerator Scanner::AddFiltering(RecordBatchGenerator rbs, Expression filter,
                                           MemoryPool* pool) {
  auto mapper = [=](const std::shared_ptr<RecordBatch>& in) {
    return FilterRecordBatch(filter, pool, in);
  };
  return MakeMappedGenerator(std::move(rbs), mapper);
}

Result<std::shared_ptr<RecordBatch>> Scanner::ProjectRecordBatch(
    const Expression& projection, MemoryPool* pool,
    const std::shared_ptr<RecordBatch>& in) {
  compute::ExecContext exec_context{pool};
  ARROW_ASSIGN_OR_RAISE(Datum projected,
                        ExecuteScalarExpression(projection, Datum(in), &exec_context));
  DCHECK_EQ(projected.type()->id(), Type::STRUCT);
  if (projected.shape() == ValueDescr::SCALAR) {
    // Only virtual columns are projected. Broadcast to an array
    ARROW_ASSIGN_OR_RAISE(projected,
                          MakeArrayFromScalar(*projected.scalar(), in->num_rows(), pool));
  }

  ARROW_ASSIGN_OR_RAISE(auto out,
                        RecordBatch::FromStructArray(projected.array_as<StructArray>()));

  return out->ReplaceSchemaMetadata(in->schema()->metadata());
}

RecordBatchGenerator Scanner::AddProjection(RecordBatchGenerator rbs,
                                            Expression projection, MemoryPool* pool) {
  auto mapper = [=](const std::shared_ptr<RecordBatch>& in) {
    return ProjectRecordBatch(projection, pool, in);
  };
  return MakeMappedGenerator(std::move(rbs), mapper);
}

namespace {
std::vector<PositionedScanTask> PositionTasks(const ScanTaskVector& scan_tasks,
                                              uint32_t fragment_index) {
  std::vector<PositionedScanTask> positioned_tasks;
  positioned_tasks.reserve(scan_tasks.size());
  for (std::size_t i = 0; i < scan_tasks.size(); i++) {
    positioned_tasks.push_back(PositionedScanTask{scan_tasks[i], fragment_index,
                                                  static_cast<uint32_t>(i),
                                                  i == scan_tasks.size() - 1});
  }
  return positioned_tasks;
}
}  // namespace

Future<PositionedScanTaskGenerator> Scanner::FragmentToPositionedScanTasks(
    std::shared_ptr<ScanOptions> options, const std::shared_ptr<Fragment>& fragment,
    uint32_t fragment_index) {
  return fragment->Scan(options).Then(
      [fragment, fragment_index](const ScanTaskVector& scan_tasks) {
        auto positioned_tasks = PositionTasks(scan_tasks, fragment_index);
        // Apply the filter and/or projection to incoming RecordBatches by
        // wrapping the ScanTask with a FilterAndProjectScanTask
        auto wrap_scan_task =
            [fragment](const PositionedScanTask& task) -> Result<PositionedScanTask> {
          auto partition = fragment->partition_expression();
          auto wrapped_task = std::make_shared<FilterAndProjectScanTask>(
              std::move(task.scan_task), partition);
          return task.ReplaceTask(wrapped_task);
        };

        auto scan_tasks_gen = MakeVectorGenerator(positioned_tasks);
        return MakeMappedGenerator(scan_tasks_gen, wrap_scan_task);
      });
}

std::vector<Future<PositionedScanTaskGenerator>> Scanner::FragmentsToPositionedScanTasks(
    std::shared_ptr<ScanOptions> options, const FragmentVector& fragments) {
  std::vector<Future<PositionedScanTaskGenerator>> positioned_scan_tasks;
  positioned_scan_tasks.reserve(fragments.size());
  for (std::size_t i = 0; i < fragments.size(); i++) {
    positioned_scan_tasks.push_back(
        FragmentToPositionedScanTasks(options, fragments[i], i));
  }
  return positioned_scan_tasks;
}

/// \brief GetScanTaskGenerator transforms FragmentVector->ScanTaskGenerator
Result<PositionedScanTaskGenerator> Scanner::GetUnorderedScanTaskGenerator(
    const FragmentVector& fragments, std::shared_ptr<ScanOptions> options) {
  // Fragments -> ScanTaskGeneratorGenerator
  auto scan_tasks = FragmentsToPositionedScanTasks(options, fragments);
  auto scan_tasks_generator = MakeGeneratorFromFutures(scan_tasks);
  // ScanTaskGeneratorGenerator -> ScanTaskGenerator
  return MakeMergedGenerator(std::move(scan_tasks_generator), options->file_readahead);
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset)
    : ScannerBuilder(std::move(dataset), std::make_shared<ScanOptions>()) {}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanOptions> scan_options)
    : dataset_(std::move(dataset)),
      fragment_(nullptr),
      scan_options_(std::move(scan_options)) {
  scan_options_->dataset_schema = dataset_->schema();
  DCHECK_OK(Filter(literal(true)));
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Schema> schema,
                               std::shared_ptr<Fragment> fragment,
                               std::shared_ptr<ScanOptions> scan_options)
    : dataset_(nullptr),
      fragment_(std::move(fragment)),
      scan_options_(std::move(scan_options)) {
  scan_options_->dataset_schema = std::move(schema);
  DCHECK_OK(Filter(literal(true)));
}

const std::shared_ptr<Schema>& ScannerBuilder::schema() const {
  return scan_options_->dataset_schema;
}

Status ScannerBuilder::Project(std::vector<std::string> columns) {
  return SetProjection(scan_options_.get(), std::move(columns));
}

Status ScannerBuilder::Project(std::vector<Expression> exprs,
                               std::vector<std::string> names) {
  return SetProjection(scan_options_.get(), std::move(exprs), std::move(names));
}

Status ScannerBuilder::Filter(const Expression& filter) {
  return SetFilter(scan_options_.get(), filter);
}

Status ScannerBuilder::UseThreads(bool use_threads) {
  scan_options_->use_threads = use_threads;
  return Status::OK();
}

Status ScannerBuilder::BatchSize(int64_t batch_size) {
  if (batch_size <= 0) {
    return Status::Invalid("BatchSize must be greater than 0, got ", batch_size);
  }
  scan_options_->batch_size = batch_size;
  return Status::OK();
}

Status ScannerBuilder::Pool(MemoryPool* pool) {
  scan_options_->pool = pool;
  return Status::OK();
}

Status ScannerBuilder::FragmentScanOptions(
    std::shared_ptr<dataset::FragmentScanOptions> fragment_scan_options) {
  scan_options_->fragment_scan_options = std::move(fragment_scan_options);
  return Status::OK();
}

Result<std::shared_ptr<Scanner>> ScannerBuilder::Finish() {
  if (!scan_options_->projection.IsBound()) {
    RETURN_NOT_OK(Project(scan_options_->dataset_schema->field_names()));
  }

  if (dataset_ == nullptr) {
    return std::make_shared<Scanner>(fragment_, scan_options_);
  }
  return std::make_shared<Scanner>(dataset_, scan_options_);
}

static inline RecordBatchVector FlattenRecordBatchVector(
    std::vector<RecordBatchVector> nested_batches) {
  RecordBatchVector flattened;

  for (auto& task_batches : nested_batches) {
    for (auto& batch : task_batches) {
      flattened.emplace_back(std::move(batch));
    }
  }

  return flattened;
}

struct TableAssemblyState {
  /// Protecting mutating accesses to batches
  std::mutex mutex{};
  std::vector<RecordBatchVector> batches{};
  int scan_task_id = 0;

  void Emplace(RecordBatchVector b, size_t position) {
    std::lock_guard<std::mutex> lock(mutex);
    if (batches.size() <= position) {
      batches.resize(position + 1);
    }
    batches[position] = std::move(b);
  }
};

struct TaggedRecordBatch {
  std::shared_ptr<RecordBatch> record_batch;
};

Result<std::shared_ptr<Table>> Scanner::ToTable() {
  auto table_fut = ToTableAsync();
  table_fut.Wait();
  ARROW_ASSIGN_OR_RAISE(auto table, table_fut.result());
  return table;
}

Future<std::shared_ptr<Table>> Scanner::ToTableAsync() {
  auto scan_options = scan_options_;
  return ScanAsync().Then([scan_options](const ScanTaskGenerator& scan_task_gen)
                              -> Future<std::shared_ptr<Table>> {
    /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
    /// invalidate concurrently running tasks when Finish() early returns
    /// and the mutex/batches fail out of scope.
    auto state = std::make_shared<TableAssemblyState>();

    // FIXME use auto
    // TODO(ARROW-12023)
    std::function<Future<std::shared_ptr<ScanTask>>(
        const std::shared_ptr<ScanTask>& scan_task)>
        table_building_task = [state](const std::shared_ptr<ScanTask>& scan_task)
        -> Future<std::shared_ptr<ScanTask>> {
      auto id = state->scan_task_id++;
      ARROW_ASSIGN_OR_RAISE(auto batch_gen, scan_task->ExecuteAsync());
      return CollectAsyncGenerator(std::move(batch_gen))
          .Then([state, id,
                 scan_task](const RecordBatchVector& rbs) -> std::shared_ptr<ScanTask> {
            state->Emplace(rbs, id);
            return scan_task;
          });
    };

    auto transferred_generator =
        MakeTransferredGenerator(scan_task_gen, internal::GetCpuThreadPool());

    auto table_building_gen =
        MakeMappedGenerator(transferred_generator, table_building_task);

    return DiscardAllFromAsyncGenerator(table_building_gen)
        .Then([state, scan_options](...) {
          return Table::FromRecordBatches(
              scan_options->projected_schema,
              FlattenRecordBatchVector(std::move(state->batches)));
        });
  });
}

}  // namespace dataset
}  // namespace arrow

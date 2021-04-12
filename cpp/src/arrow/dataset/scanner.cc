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
#include "arrow/util/async_generator.h"
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

using arrow::internal::Executor;
using arrow::internal::SerialExecutor;
using arrow::internal::TaskGroup;

std::shared_ptr<TaskGroup> ScanOptions::TaskGroup() const {
  if (use_threads) {
    auto* thread_pool = arrow::internal::GetCpuThreadPool();
    return TaskGroup::MakeThreaded(thread_pool);
  }
  return TaskGroup::MakeSerial();
}

Result<RecordBatchIterator> InMemoryScanTask::Execute() {
  return MakeVectorIterator(record_batches_);
}

Result<RecordBatchGenerator> ScanTask::ExecuteAsync(internal::Executor*) {
  return Status::NotImplemented("Async is not implemented for this scan task yet");
}

bool ScanTask::supports_async() const { return false; }

Result<FragmentIterator> Scanner::GetFragments() {
  if (fragment_ != nullptr) {
    return MakeVectorIterator(FragmentVector{fragment_});
  }

  // Transform Datasets in a flat Iterator<Fragment>. This
  // iterator is lazily constructed, i.e. Dataset::GetFragments is
  // not invoked until a Fragment is requested.
  return GetFragmentsFromDatasets({dataset_}, scan_options_->filter);
}

Result<ScanTaskIterator> Scanner::Scan() {
  // Transforms Iterator<Fragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.
  ARROW_ASSIGN_OR_RAISE(auto fragment_it, GetFragments());
  return GetScanTaskIterator(std::move(fragment_it), scan_options_);
}

Result<ScanTaskIterator> ScanTaskIteratorFromRecordBatch(
    std::vector<std::shared_ptr<RecordBatch>> batches,
    std::shared_ptr<ScanOptions> options) {
  if (batches.empty()) {
    return MakeVectorIterator(ScanTaskVector());
  }
  auto schema = batches[0]->schema();
  auto fragment =
      std::make_shared<InMemoryFragment>(std::move(schema), std::move(batches));
  return fragment->Scan(std::move(options));
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

  void Emplace(RecordBatchVector b, size_t position) {
    std::lock_guard<std::mutex> lock(mutex);
    if (batches.size() <= position) {
      batches.resize(position + 1);
    }
    batches[position] = std::move(b);
  }
};

Result<std::shared_ptr<Table>> Scanner::ToTable() {
  return internal::RunSynchronously<std::shared_ptr<Table>>(
      [this](Executor* executor) { return ToTableInternal(executor); },
      scan_options_->use_threads);
}

Future<std::shared_ptr<Table>> Scanner::ToTableInternal(
    internal::Executor* cpu_executor) {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, Scan());
  auto task_group = scan_options_->TaskGroup();

  /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
  /// invalidate concurrently running tasks when Finish() early returns
  /// and the mutex/batches fail out of scope.
  auto state = std::make_shared<TableAssemblyState>();

  size_t scan_task_id = 0;
  std::vector<Future<>> scan_futures;
  for (auto maybe_scan_task : scan_task_it) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);

    auto id = scan_task_id++;
    if (scan_task->supports_async()) {
      ARROW_ASSIGN_OR_RAISE(auto scan_gen, scan_task->ExecuteAsync(cpu_executor));
      auto scan_fut = CollectAsyncGenerator(std::move(scan_gen))
                          .Then([state, id](const RecordBatchVector& rbs) {
                            state->Emplace(rbs, id);
                          });
      scan_futures.push_back(std::move(scan_fut));
    } else {
      task_group->Append([state, id, scan_task] {
        ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());
        ARROW_ASSIGN_OR_RAISE(auto local, batch_it.ToVector());
        state->Emplace(std::move(local), id);
        return Status::OK();
      });
    }
  }
  auto scan_options = scan_options_;
  scan_futures.push_back(task_group->FinishAsync());
  // Wait for all tasks to complete, or the first error
  return AllComplete(scan_futures)
      .Then(
          [scan_options, state](const detail::Empty&) -> Result<std::shared_ptr<Table>> {
            return Table::FromRecordBatches(
                scan_options->projected_schema,
                FlattenRecordBatchVector(std::move(state->batches)));
          });
}

}  // namespace dataset
}  // namespace arrow

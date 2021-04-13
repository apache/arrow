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

Result<ScanTaskIterator> Scanner::Scan() {
  // TODO(ARROW-12289) This is overridden in SyncScanner and will never be implemented in
  // AsyncScanner.  It is deprecated and will eventually go away.
  return Status::NotImplemented("This scanner does not support the legacy Scan() method");
}

Result<EnumeratedRecordBatchIterator> Scanner::ScanBatchesUnordered() {
  // If a scanner doesn't support unordered scanning (i.e. SyncScanner) then we just
  // fall back to an ordered scan and assign the appropriate tagging
  ARROW_ASSIGN_OR_RAISE(auto ordered_scan, ScanBatches());
  return AddPositioningToInOrderScan(std::move(ordered_scan));
}

Result<EnumeratedRecordBatchIterator> Scanner::AddPositioningToInOrderScan(
    TaggedRecordBatchIterator scan) {
  ARROW_ASSIGN_OR_RAISE(auto first, scan.Next());
  if (IsIterationEnd(first)) {
    return MakeEmptyIterator<EnumeratedRecordBatch>();
  }
  struct State {
    State(TaggedRecordBatchIterator source, TaggedRecordBatch first)
        : source(std::move(source)),
          batch_index(0),
          fragment_index(0),
          finished(false),
          prev_batch(std::move(first)) {}
    TaggedRecordBatchIterator source;
    int batch_index;
    int fragment_index;
    bool finished;
    TaggedRecordBatch prev_batch;
  };
  struct EnumeratingIterator {
    Result<EnumeratedRecordBatch> Next() {
      if (state->finished) {
        return IterationEnd<EnumeratedRecordBatch>();
      }
      ARROW_ASSIGN_OR_RAISE(auto next, state->source.Next());
      if (IsIterationEnd<TaggedRecordBatch>(next)) {
        state->finished = true;
        return EnumeratedRecordBatch{
            {std::move(state->prev_batch.record_batch), state->batch_index, true},
            {std::move(state->prev_batch.fragment), state->fragment_index, true}};
      }
      auto prev = std::move(state->prev_batch);
      bool prev_is_last_batch = false;
      auto prev_batch_index = state->batch_index;
      auto prev_fragment_index = state->fragment_index;
      // Reference equality here seems risky but a dataset should have a constant set of
      // fragments which should be consistent for the lifetime of a scan
      if (prev.fragment.get() != next.fragment.get()) {
        state->batch_index = 0;
        state->fragment_index++;
        prev_is_last_batch = true;
      } else {
        state->batch_index++;
      }
      state->prev_batch = std::move(next);
      return EnumeratedRecordBatch{
          {std::move(prev.record_batch), prev_batch_index, prev_is_last_batch},
          {std::move(prev.fragment), prev_fragment_index, false}};
    }
    std::shared_ptr<State> state;
  };
  return EnumeratedRecordBatchIterator(
      EnumeratingIterator{std::make_shared<State>(std::move(scan), std::move(first))});
}

Result<TaggedRecordBatchIterator> SyncScanner::ScanBatches() {
  // TODO(ARROW-11797) Provide a better implementation that does readahead.  Also, add
  // unit testing
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, Scan());
  struct BatchIter {
    explicit BatchIter(ScanTaskIterator scan_task_it)
        : scan_task_it(std::move(scan_task_it)) {}

    Result<TaggedRecordBatch> Next() {
      while (true) {
        if (current_task == nullptr) {
          ARROW_ASSIGN_OR_RAISE(current_task, scan_task_it.Next());
          if (IsIterationEnd<std::shared_ptr<ScanTask>>(current_task)) {
            return IterationEnd<TaggedRecordBatch>();
          }
          ARROW_ASSIGN_OR_RAISE(batch_it, current_task->Execute());
        }
        ARROW_ASSIGN_OR_RAISE(auto next, batch_it.Next());
        if (IsIterationEnd<std::shared_ptr<RecordBatch>>(next)) {
          current_task = nullptr;
        } else {
          return TaggedRecordBatch{next, current_task->fragment()};
        }
      }
    }

    ScanTaskIterator scan_task_it;
    RecordBatchIterator batch_it;
    std::shared_ptr<ScanTask> current_task;
  };
  return TaggedRecordBatchIterator(BatchIter(std::move(scan_task_it)));
}

Result<FragmentIterator> SyncScanner::GetFragments() {
  if (fragment_ != nullptr) {
    return MakeVectorIterator(FragmentVector{fragment_});
  }

  // Transform Datasets in a flat Iterator<Fragment>. This
  // iterator is lazily constructed, i.e. Dataset::GetFragments is
  // not invoked until a Fragment is requested.
  return GetFragmentsFromDatasets({dataset_}, scan_options_->filter);
}

Result<ScanTaskIterator> SyncScanner::Scan() {
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
  DCHECK_OK(Filter(scan_options_->filter));
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Schema> schema,
                               std::shared_ptr<Fragment> fragment,
                               std::shared_ptr<ScanOptions> scan_options)
    : dataset_(nullptr),
      fragment_(std::move(fragment)),
      scan_options_(std::move(scan_options)) {
  scan_options_->dataset_schema = std::move(schema);
  DCHECK_OK(Filter(scan_options_->filter));
}

const std::shared_ptr<Schema>& ScannerBuilder::schema() const {
  return scan_options_->dataset_schema;
}

const std::shared_ptr<Schema>& ScannerBuilder::projected_schema() const {
  return scan_options_->projected_schema;
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
    // AsyncScanner does not support this method of running.  It may in the future
    return std::make_shared<SyncScanner>(fragment_, scan_options_);
  }
  if (scan_options_->use_async) {
    // TODO(ARROW-12289)
    return Status::NotImplemented("The asynchronous scanner is not yet available");
  } else {
    return std::make_shared<SyncScanner>(dataset_, scan_options_);
  }
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

Result<std::shared_ptr<Table>> SyncScanner::ToTable() {
  return internal::RunSynchronously<std::shared_ptr<Table>>(
      [this](Executor* executor) { return ToTableInternal(executor); },
      scan_options_->use_threads);
}

Future<std::shared_ptr<Table>> SyncScanner::ToTableInternal(
    internal::Executor* cpu_executor) {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, Scan());
  auto task_group = scan_options_->TaskGroup();

  /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
  /// invalidate concurrently running tasks when Finish() early returns
  /// and the mutex/batches fail out of scope.
  auto state = std::make_shared<TableAssemblyState>();

  // TODO (ARROW-11797) Migrate to using ScanBatches()
  size_t scan_task_id = 0;
  for (auto maybe_scan_task : scan_task_it) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);

    auto id = scan_task_id++;
    task_group->Append([state, id, scan_task] {
      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());
      ARROW_ASSIGN_OR_RAISE(auto local, batch_it.ToVector());
      state->Emplace(std::move(local), id);
      return Status::OK();
    });
  }
  auto scan_options = scan_options_;
  // Wait for all tasks to complete, or the first error
  RETURN_NOT_OK(task_group->Finish());
  return Table::FromRecordBatches(scan_options->projected_schema,
                                  FlattenRecordBatchVector(std::move(state->batches)));
}

}  // namespace dataset
}  // namespace arrow

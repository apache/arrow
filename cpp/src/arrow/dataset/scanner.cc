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
#include <condition_variable>
#include <memory>
#include <mutex>
#include <sstream>

#include "arrow/array/array_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
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

struct ScanBatchesState : public std::enable_shared_from_this<ScanBatchesState> {
  explicit ScanBatchesState(ScanTaskIterator scan_task_it,
                            std::shared_ptr<TaskGroup> task_group_)
      : scan_tasks(std::move(scan_task_it)), task_group(std::move(task_group_)) {}

  void ResizeBatches(size_t task_index) {
    if (task_batches.size() <= task_index) {
      task_batches.resize(task_index + 1);
      task_drained.resize(task_index + 1);
    }
  }

  void Push(TaggedRecordBatch batch, size_t task_index) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      ResizeBatches(task_index);
      task_batches[task_index].push_back(std::move(batch));
    }
    ready.notify_one();
  }

  Status Finish(size_t task_index) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      ResizeBatches(task_index);
      task_drained[task_index] = true;
    }
    ready.notify_one();
    return Status::OK();
  }

  void PushScanTask() {
    if (no_more_tasks) return;
    std::unique_lock<std::mutex> lock(mutex);
    auto maybe_task = scan_tasks.Next();
    if (!maybe_task.ok()) {
      no_more_tasks = true;
      iteration_error = maybe_task.status();
      return;
    }
    auto scan_task = maybe_task.ValueOrDie();
    if (IsIterationEnd(scan_task)) {
      no_more_tasks = true;
      return;
    }
    auto state = shared_from_this();
    auto id = next_scan_task_id++;
    ResizeBatches(id);

    lock.unlock();
    task_group->Append([state, id, scan_task]() {
      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());
      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        state->Push(TaggedRecordBatch{std::move(batch), scan_task->fragment()}, id);
      }
      return state->Finish(id);
    });
  }

  Result<TaggedRecordBatch> Pop() {
    std::unique_lock<std::mutex> lock(mutex);
    ready.wait(lock, [this, &lock] {
      while (pop_cursor < task_batches.size()) {
        // queue for current scan task contains at least one batch, pop that
        if (!task_batches[pop_cursor].empty()) return true;
        // queue is empty but will be appended to eventually, wait for that
        if (!task_drained[pop_cursor]) return false;

        // Finished draining current scan task, enqueue a new one
        ++pop_cursor;
        // Must unlock since serial task group will execute synchronously
        lock.unlock();
        PushScanTask();
        lock.lock();
      }
      DCHECK(no_more_tasks);
      // all scan tasks drained (or getting next task failed), terminate
      return true;
    });

    if (pop_cursor == task_batches.size()) {
      // Don't report an error until we yield up everything we can first
      RETURN_NOT_OK(iteration_error);
      return IterationEnd<TaggedRecordBatch>();
    }

    auto batch = std::move(task_batches[pop_cursor].front());
    task_batches[pop_cursor].pop_front();
    return batch;
  }

  /// Protecting mutating accesses to batches
  std::mutex mutex;
  std::condition_variable ready;
  ScanTaskIterator scan_tasks;
  std::shared_ptr<TaskGroup> task_group;
  int next_scan_task_id = 0;
  bool no_more_tasks = false;
  Status iteration_error;
  std::vector<std::deque<TaggedRecordBatch>> task_batches;
  std::vector<bool> task_drained;
  size_t pop_cursor = 0;
};

Result<TaggedRecordBatchIterator> SyncScanner::ScanBatches() {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, ScanInternal());
  auto task_group = scan_options_->TaskGroup();
  auto state = std::make_shared<ScanBatchesState>(std::move(scan_task_it), task_group);
  for (int i = 0; i < scan_options_->fragment_readahead; i++) {
    state->PushScanTask();
  }
  return MakeFunctionIterator([task_group, state]() -> Result<TaggedRecordBatch> {
    ARROW_ASSIGN_OR_RAISE(auto batch, state->Pop());
    if (!IsIterationEnd(batch)) return batch;
    RETURN_NOT_OK(task_group->Finish());
    return IterationEnd<TaggedRecordBatch>();
  });
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

Result<ScanTaskIterator> SyncScanner::Scan() { return ScanInternal(); }

Status SyncScanner::Scan(std::function<Status(TaggedRecordBatch)> visitor) {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, ScanInternal());

  auto task_group = scan_options_->TaskGroup();

  for (auto maybe_scan_task : scan_task_it) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);
    task_group->Append([scan_task, visitor] {
      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());
      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        RETURN_NOT_OK(
            visitor(TaggedRecordBatch{std::move(batch), scan_task->fragment()}));
      }
      return Status::OK();
    });
  }

  return task_group->Finish();
}

Result<ScanTaskIterator> SyncScanner::ScanInternal() {
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
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, ScanInternal());
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

Result<std::shared_ptr<Table>> Scanner::TakeRows(const Array& indices) {
  if (indices.null_count() != 0) {
    return Status::NotImplemented("null take indices");
  }

  compute::ExecContext ctx(scan_options_->pool);

  const Array* original_indices;
  // If we have to cast, this is the backing reference
  std::shared_ptr<Array> original_indices_ptr;
  if (indices.type_id() != Type::INT64) {
    ARROW_ASSIGN_OR_RAISE(
        original_indices_ptr,
        compute::Cast(indices, int64(), compute::CastOptions::Safe(), &ctx));
    original_indices = original_indices_ptr.get();
  } else {
    original_indices = &indices;
  }

  std::shared_ptr<Array> unsort_indices;
  {
    ARROW_ASSIGN_OR_RAISE(
        auto sort_indices,
        compute::SortIndices(*original_indices, compute::SortOrder::Ascending, &ctx));
    ARROW_ASSIGN_OR_RAISE(original_indices_ptr,
                          compute::Take(*original_indices, *sort_indices,
                                        compute::TakeOptions::Defaults(), &ctx));
    original_indices = original_indices_ptr.get();
    ARROW_ASSIGN_OR_RAISE(
        unsort_indices,
        compute::SortIndices(*sort_indices, compute::SortOrder::Ascending, &ctx));
  }

  RecordBatchVector out_batches;

  auto raw_indices = static_cast<const Int64Array&>(*original_indices).raw_values();
  int64_t offset = 0, row_begin = 0;

  ARROW_ASSIGN_OR_RAISE(auto batch_it, ScanBatches());
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_it.Next());
    if (IsIterationEnd(batch)) break;
    if (offset == original_indices->length()) break;
    DCHECK_LT(offset, original_indices->length());

    int64_t length = 0;
    while (offset + length < original_indices->length()) {
      auto rel_index = raw_indices[offset + length] - row_begin;
      if (rel_index >= batch.record_batch->num_rows()) break;
      ++length;
    }
    DCHECK_LE(offset + length, original_indices->length());
    if (length == 0) {
      row_begin += batch.record_batch->num_rows();
      continue;
    }

    Datum rel_indices = original_indices->Slice(offset, length);
    ARROW_ASSIGN_OR_RAISE(rel_indices,
                          compute::Subtract(rel_indices, Datum(row_begin),
                                            compute::ArithmeticOptions(), &ctx));

    ARROW_ASSIGN_OR_RAISE(Datum out_batch,
                          compute::Take(batch.record_batch, rel_indices,
                                        compute::TakeOptions::Defaults(), &ctx));
    out_batches.push_back(out_batch.record_batch());

    offset += length;
    row_begin += batch.record_batch->num_rows();
  }

  if (offset < original_indices->length()) {
    std::stringstream error;
    const int64_t max_values_shown = 3;
    const int64_t num_remaining = original_indices->length() - offset;
    for (int64_t i = 0; i < std::min<int64_t>(max_values_shown, num_remaining); i++) {
      if (i > 0) error << ", ";
      error << static_cast<const Int64Array*>(original_indices)->Value(offset + i);
    }
    if (num_remaining > max_values_shown) error << ", ...";
    return Status::IndexError("Some indices were out of bounds: ", error.str());
  }
  ARROW_ASSIGN_OR_RAISE(Datum out, Table::FromRecordBatches(options()->projected_schema,
                                                            std::move(out_batches)));
  ARROW_ASSIGN_OR_RAISE(
      out, compute::Take(out, unsort_indices, compute::TakeOptions::Defaults(), &ctx));
  return out.table();
}

Result<std::shared_ptr<Table>> Scanner::Head(int64_t num_rows) {
  if (num_rows == 0) {
    return Table::FromRecordBatches(options()->projected_schema, {});
  }
  ARROW_ASSIGN_OR_RAISE(auto batch_iterator, ScanBatches());
  RecordBatchVector batches;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_iterator.Next());
    if (IsIterationEnd(batch)) break;
    batches.push_back(batch.record_batch->Slice(0, num_rows));
    num_rows -= batch.record_batch->num_rows();
    if (num_rows <= 0) break;
  }
  return Table::FromRecordBatches(options()->projected_schema, batches);
}

}  // namespace dataset
}  // namespace arrow

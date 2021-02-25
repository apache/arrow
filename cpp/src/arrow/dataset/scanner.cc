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

PositionedRecordBatch PositionedRecordBatch::BeforeAny() {
  return PositionedRecordBatch{nullptr, nullptr, -1, -1, false, -1, false};
}

PositionedRecordBatch PositionedRecordBatch::AfterAny() {
  return PositionedRecordBatch{nullptr, nullptr, -1, -1, true, -1, true};
}

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

Result<FragmentIterator> Scanner::GetFragments() {
  auto fut = GetFragmentsAsync();
  fut.Wait();
  ARROW_ASSIGN_OR_RAISE(auto fragments_vec, fut.result());
  return MakeVectorIterator(fragments_vec);
}

PositionedRecordBatchGenerator Scanner::ScanAsync() {
  auto unordered_generator = ScanUnorderedAsync();
  auto cmp = [](const PositionedRecordBatch& left, const PositionedRecordBatch& right) {
    if (left.fragment_index > right.fragment_index) {
      return true;
    }
    if (left.scan_task_index > right.scan_task_index) {
      return true;
    }
    return left.record_batch_index > right.record_batch_index;
  };
  auto is_next = [](const PositionedRecordBatch& last,
                    const PositionedRecordBatch& maybe_next) {
    if (last.record_batch == nullptr) {
      return maybe_next.fragment_index == 0 && maybe_next.scan_task_index == 0 &&
             maybe_next.record_batch_index == 0;
    }
    if (maybe_next.fragment_index > last.fragment_index + 1) {
      return false;
    }
    if (maybe_next.fragment_index == last.fragment_index + 1) {
      return maybe_next.record_batch_index == 0 && maybe_next.scan_task_index == 0 &&
             last.last_scan_task && last.last_record_batch;
    }
    if (maybe_next.scan_task_index > last.scan_task_index + 1) {
      return false;
    }
    if (maybe_next.scan_task_index == last.scan_task_index + 1) {
      return maybe_next.record_batch_index == 0 && last.last_record_batch;
    }
    return maybe_next.record_batch_index == last.record_batch_index + 1;
  };
  return MakeSequencingGenerator(std::move(unordered_generator), cmp, is_next,
                                 PositionedRecordBatch::BeforeAny());
}

Status Scanner::ValidateOptions() {
  if (scan_options_->batch_readahead < 1) {
    return Status::Invalid("ScanOptions::batch_readahead must be >= 1");
  }
  if (scan_options_->file_readahead < 1) {
    return Status::Invalid("ScanOptions::file_readahead must be >= 1");
  }
  return Status::OK();
}

PositionedRecordBatchGenerator Scanner::ScanUnorderedAsync() {
  return MakeFromFuture(GetFragmentsAsync().Then(
      [this](const FragmentVector& fragments) -> Result<PositionedRecordBatchGenerator> {
        RETURN_NOT_OK(ValidateOptions());
        return GetUnorderedRecordBatchGenerator(fragments, scan_options_);
      }));
}

Result<ScanTaskIterator> Scanner::Scan() {
  // This method is kept around for backwards compatibility.  There is no longer any need
  // for the consumer to execute the scan tasks.  Instead we return each record batch
  // wrapped in an InMemoryScanTask.
  auto record_batch_generator = ScanAsync();
  auto scan_options = scan_options_;
  auto wrap_record_batch = [scan_options](const PositionedRecordBatch& positioned_batch)
      -> Result<std::shared_ptr<ScanTask>> {
    return std::make_shared<InMemoryScanTask>(
        RecordBatchVector{positioned_batch.record_batch}, scan_options,
        positioned_batch.fragment);
  };
  auto wrapped_generator =
      MakeMappedGenerator(std::move(record_batch_generator), wrap_record_batch);
  return MakeGeneratorIterator(std::move(wrapped_generator));
}

Result<PositionedRecordBatchIterator> Scanner::ScanBatches() {
  auto record_batch_generator = ScanAsync();
  return MakeGeneratorIterator(std::move(record_batch_generator));
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

struct PositionedScanTask {
  std::shared_ptr<ScanTask> scan_task;
  int fragment_index;
  int scan_task_index;
  bool last_scan_task;

  PositionedScanTask ReplaceScanTask(std::shared_ptr<ScanTask> new_task) const {
    return PositionedScanTask{new_task, fragment_index, scan_task_index, last_scan_task};
  }
};

std::vector<PositionedScanTask> PositionTasks(const ScanTaskVector& scan_tasks,
                                              int fragment_index) {
  std::vector<PositionedScanTask> positioned_tasks;
  positioned_tasks.reserve(scan_tasks.size());
  for (std::size_t i = 0; i < scan_tasks.size(); i++) {
    positioned_tasks.push_back(PositionedScanTask{
        scan_tasks[i], fragment_index, static_cast<int>(i), i == scan_tasks.size() - 1});
  }
  return positioned_tasks;
}

}  // namespace dataset

template <>
struct IterationTraits<dataset::PositionedScanTask> {
  static dataset::PositionedScanTask End() {
    return dataset::PositionedScanTask{nullptr, -1, -1, false};
  }
  static bool IsEnd(const dataset::PositionedScanTask val) {
    return val.scan_task == nullptr;
  }
};

namespace dataset {

PositionedRecordBatchGenerator Scanner::FragmentToPositionedRecordBatches(
    std::shared_ptr<ScanOptions> options, const std::shared_ptr<Fragment>& fragment,
    int fragment_index) {
  return MakeFromFuture(fragment->Scan(options).Then(
      [fragment, fragment_index, options](const ScanTaskVector& scan_tasks) {
        auto positioned_tasks = PositionTasks(scan_tasks, fragment_index);
        // Apply the filter and/or projection to incoming RecordBatches by
        // wrapping the ScanTask with a FilterAndProjectScanTask
        auto wrap_scan_task =
            [fragment](const PositionedScanTask& task) -> Result<PositionedScanTask> {
          auto partition = fragment->partition_expression();
          auto wrapped_task = std::make_shared<FilterAndProjectScanTask>(
              std::move(task.scan_task), partition);
          return task.ReplaceScanTask(wrapped_task);
        };

        auto scan_tasks_gen = MakeVectorGenerator(positioned_tasks);
        auto wrapped_tasks_gen =
            MakeMappedGenerator(std::move(scan_tasks_gen), wrap_scan_task);

        auto execute_task = [fragment, fragment_index](const PositionedScanTask& task)
            -> Result<PositionedRecordBatchGenerator> {
          ARROW_ASSIGN_OR_RAISE(auto record_batch_gen, task.scan_task->ExecuteAsync());
          auto enumerated_rb_gen = MakeEnumeratedGenerator(record_batch_gen);
          auto scan_task_index = task.scan_task_index;
          auto last_scan_task = task.last_scan_task;
          auto to_positioned_batch =
              [fragment, fragment_index, scan_task_index,
               last_scan_task](const Enumerated<std::shared_ptr<RecordBatch>>& batch)
              // FIXME This shouldn't have to be a result, need to fix mapped generator
              // SNIFAE weirdness
              -> Result<PositionedRecordBatch> {
                return PositionedRecordBatch{
                    *batch.value,   fragment,    fragment_index, scan_task_index,
                    last_scan_task, batch.index, batch.last};
              };
          return MakeMappedGenerator(enumerated_rb_gen, to_positioned_batch);
        };

        auto rb_gen_gen = MakeMappedGenerator(std::move(wrapped_tasks_gen), execute_task);
        // This is really "how many scan tasks to run at once" however all readers either
        // do 1 scan task (in which case this is pointless) or in the parquet case scan
        // task per row group which is close enough to per-batch
        return MakeMergedGenerator(rb_gen_gen, options->batch_readahead);
      }));
}

AsyncGenerator<PositionedRecordBatchGenerator>
Scanner::FragmentsToPositionedRecordBatches(std::shared_ptr<ScanOptions> options,
                                            const FragmentVector& fragments) {
  auto fragment_generator = MakeVectorGenerator(fragments);
  auto fragment_counter = std::make_shared<std::atomic<int>>(0);
  std::function<PositionedRecordBatchGenerator(const std::shared_ptr<Fragment>& fragment)>
      fragment_to_scan_tasks =
          [options, fragment_counter](const std::shared_ptr<Fragment>& fragment) {
            auto fragment_index = fragment_counter->fetch_add(1);
            return FragmentToPositionedRecordBatches(options, fragment, fragment_index);
          };
  return MakeMappedGenerator(fragment_generator, fragment_to_scan_tasks);
}

/// \brief GetScanTaskGenerator transforms FragmentVector->ScanTaskGenerator
Result<PositionedRecordBatchGenerator> Scanner::GetUnorderedRecordBatchGenerator(
    const FragmentVector& fragments, std::shared_ptr<ScanOptions> options) {
  // Fragments -> ScanTaskGeneratorGenerator
  auto scan_task_generator_generator =
      FragmentsToPositionedRecordBatches(options, fragments);
  // ScanTaskGeneratorGenerator -> ScanTaskGenerator
  auto merged = MakeMergedGenerator(std::move(scan_task_generator_generator),
                                    options->file_readahead);
  return MakeReadaheadGenerator(merged, options->file_readahead);
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

struct TableAssemblyState {
  /// Protecting mutating accesses to batches
  std::mutex mutex{};
  std::vector<std::vector<RecordBatchVector>> batches{};
  int scan_task_id = 0;

  void Emplace(std::shared_ptr<RecordBatch> batch, size_t fragment_index,
               size_t task_index, size_t record_batch_index) {
    std::lock_guard<std::mutex> lock(mutex);
    if (batches.size() <= fragment_index) {
      batches.resize(fragment_index + 1);
    }
    if (batches[fragment_index].size() <= task_index) {
      batches[fragment_index].resize(task_index + 1);
    }
    if (batches[fragment_index][task_index].size() <= record_batch_index) {
      batches[fragment_index][task_index].resize(record_batch_index + 1);
    }
    batches[fragment_index][task_index][record_batch_index] = std::move(batch);
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
  auto positioned_batch_gen = ScanUnorderedAsync();
  /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
  /// invalidate concurrently running tasks when Finish() early returns
  /// and the mutex/batches fail out of scope.
  auto state = std::make_shared<TableAssemblyState>();

  // TODO(ARROW-12023) Ideally this mapping function would just return Status but that
  // isn't allowed because Status is not iterable
  // FIXME Return PositionedBatch and not Result<PB> when snifae confusion is figured out
  // in async_generator
  auto table_building_task =
      [state](const PositionedRecordBatch& batch) -> Result<PositionedRecordBatch> {
    state->Emplace(batch.record_batch, batch.fragment_index, batch.scan_task_index,
                   batch.record_batch_index);
    return batch;
  };

  auto table_building_gen =
      MakeMappedGeneratorAsync(positioned_batch_gen, table_building_task);

  return DiscardAllFromAsyncGenerator(table_building_gen)
      .Then([state, scan_options](...) {
        return Table::FromRecordBatches(
            scan_options->projected_schema,
            internal::FlattenVectors(internal::FlattenVectors(state->batches)));
      });
}

}  // namespace dataset
}  // namespace arrow

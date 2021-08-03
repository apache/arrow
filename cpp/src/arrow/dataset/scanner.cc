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
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/exec_plan.h"
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

using FragmentGenerator = std::function<Future<std::shared_ptr<Fragment>>()>;

std::vector<std::string> ScanOptions::MaterializedFields() const {
  std::vector<std::string> fields;

  for (const compute::Expression* expr : {&filter, &projection}) {
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

Future<RecordBatchVector> ScanTask::SafeExecute(internal::Executor* executor) {
  // If the ScanTask can't possibly be async then just execute it
  ARROW_ASSIGN_OR_RAISE(auto rb_it, Execute());
  return Future<RecordBatchVector>::MakeFinished(rb_it.ToVector());
}

Future<> ScanTask::SafeVisit(
    internal::Executor* executor,
    std::function<Status(std::shared_ptr<RecordBatch>)> visitor) {
  // If the ScanTask can't possibly be async then just execute it
  ARROW_ASSIGN_OR_RAISE(auto rb_it, Execute());
  return Future<>::MakeFinished(rb_it.Visit(visitor));
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

Result<int64_t> Scanner::CountRows() {
  // Naive base implementation
  ARROW_ASSIGN_OR_RAISE(auto batch_it, ScanBatchesUnordered());
  int64_t count = 0;
  RETURN_NOT_OK(batch_it.Visit([&](EnumeratedRecordBatch batch) {
    count += batch.record_batch.value->num_rows();
    return Status::OK();
  }));
  return count;
}

namespace {
class ScannerRecordBatchReader : public RecordBatchReader {
 public:
  explicit ScannerRecordBatchReader(std::shared_ptr<Schema> schema,
                                    TaggedRecordBatchIterator delegate)
      : schema_(std::move(schema)), delegate_(std::move(delegate)) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }
  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    ARROW_ASSIGN_OR_RAISE(auto next, delegate_.Next());
    if (IsIterationEnd(next)) {
      *batch = nullptr;
    } else {
      *batch = std::move(next.record_batch);
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  TaggedRecordBatchIterator delegate_;
};
}  // namespace

Result<std::shared_ptr<RecordBatchReader>> Scanner::ToRecordBatchReader() {
  ARROW_ASSIGN_OR_RAISE(auto it, ScanBatches());
  return std::make_shared<ScannerRecordBatchReader>(options()->projected_schema,
                                                    std::move(it));
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

  template <typename T>
  Result<T> PushError(Result<T>&& result, size_t task_index) {
    if (!result.ok()) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        task_drained[task_index] = true;
        iteration_error = result.status();
      }
      ready.notify_one();
    }
    return std::move(result);
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
      ARROW_ASSIGN_OR_RAISE(auto batch_it, state->PushError(scan_task->Execute(), id));
      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, state->PushError(std::move(maybe_batch), id));
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

class ARROW_DS_EXPORT SyncScanner : public Scanner {
 public:
  SyncScanner(std::shared_ptr<Dataset> dataset, std::shared_ptr<ScanOptions> scan_options)
      : Scanner(std::move(scan_options)), dataset_(std::move(dataset)) {}

  Result<TaggedRecordBatchIterator> ScanBatches() override;
  Result<ScanTaskIterator> Scan() override;
  Status Scan(std::function<Status(TaggedRecordBatch)> visitor) override;
  Result<std::shared_ptr<Table>> ToTable() override;
  Result<TaggedRecordBatchGenerator> ScanBatchesAsync() override;
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync() override;
  Result<int64_t> CountRows() override;

 protected:
  /// \brief GetFragments returns an iterator over all Fragments in this scan.
  Result<FragmentIterator> GetFragments();
  Result<TaggedRecordBatchIterator> ScanBatches(ScanTaskIterator scan_task_it);
  Future<std::shared_ptr<Table>> ToTableInternal(internal::Executor* cpu_executor);
  Result<ScanTaskIterator> ScanInternal();

  std::shared_ptr<Dataset> dataset_;
};

Result<TaggedRecordBatchIterator> SyncScanner::ScanBatches() {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, ScanInternal());
  return ScanBatches(std::move(scan_task_it));
}

Result<TaggedRecordBatchIterator> SyncScanner::ScanBatches(
    ScanTaskIterator scan_task_it) {
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

Result<TaggedRecordBatchGenerator> SyncScanner::ScanBatchesAsync() {
  return Status::NotImplemented("Asynchronous scanning is not supported by SyncScanner");
}

Result<EnumeratedRecordBatchGenerator> SyncScanner::ScanBatchesUnorderedAsync() {
  return Status::NotImplemented("Asynchronous scanning is not supported by SyncScanner");
}

Result<FragmentIterator> SyncScanner::GetFragments() {
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

class ARROW_DS_EXPORT AsyncScanner : public Scanner,
                                     public std::enable_shared_from_this<AsyncScanner> {
 public:
  AsyncScanner(std::shared_ptr<Dataset> dataset,
               std::shared_ptr<ScanOptions> scan_options)
      : Scanner(std::move(scan_options)), dataset_(std::move(dataset)) {}

  Status Scan(std::function<Status(TaggedRecordBatch)> visitor) override;
  Result<TaggedRecordBatchIterator> ScanBatches() override;
  Result<TaggedRecordBatchGenerator> ScanBatchesAsync() override;
  Result<EnumeratedRecordBatchIterator> ScanBatchesUnordered() override;
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync() override;
  Result<std::shared_ptr<Table>> ToTable() override;
  Result<int64_t> CountRows() override;

 private:
  Result<TaggedRecordBatchGenerator> ScanBatchesAsync(internal::Executor* executor);
  Future<> VisitBatchesAsync(std::function<Status(TaggedRecordBatch)> visitor,
                             internal::Executor* executor);
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync(
      internal::Executor* executor);
  Future<std::shared_ptr<Table>> ToTableAsync(internal::Executor* executor);

  Result<FragmentGenerator> GetFragments() const;

  std::shared_ptr<Dataset> dataset_;
};

namespace {

Result<EnumeratedRecordBatchGenerator> FragmentToBatches(
    const Enumerated<std::shared_ptr<Fragment>>& fragment,
    const std::shared_ptr<ScanOptions>& options) {
  ARROW_ASSIGN_OR_RAISE(auto batch_gen, fragment.value->ScanBatchesAsync(options));
  auto enumerated_batch_gen = MakeEnumeratedGenerator(std::move(batch_gen));

  auto combine_fn =
      [fragment](const Enumerated<std::shared_ptr<RecordBatch>>& record_batch) {
        return EnumeratedRecordBatch{record_batch, fragment};
      };

  return MakeMappedGenerator(enumerated_batch_gen, std::move(combine_fn));
}

Result<AsyncGenerator<EnumeratedRecordBatchGenerator>> FragmentsToBatches(
    FragmentGenerator fragment_gen, const std::shared_ptr<ScanOptions>& options) {
  auto enumerated_fragment_gen = MakeEnumeratedGenerator(std::move(fragment_gen));
  return MakeMappedGenerator(std::move(enumerated_fragment_gen),
                             [=](const Enumerated<std::shared_ptr<Fragment>>& fragment) {
                               return FragmentToBatches(fragment, options);
                             });
}

const FieldVector kAugmentedFields{
    field("__fragment_index", int32()),
    field("__batch_index", int32()),
    field("__last_in_fragment", boolean()),
};

Result<compute::ExecNode*> MakeScanNode(compute::ExecPlan* plan,
                                        FragmentGenerator fragment_gen,
                                        std::shared_ptr<ScanOptions> options) {
  if (!options->use_async) {
    return Status::NotImplemented("ScanNodes without asynchrony");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch_gen_gen,
                        FragmentsToBatches(std::move(fragment_gen), options));

  auto merged_batch_gen =
      MakeMergedGenerator(std::move(batch_gen_gen), options->fragment_readahead);

  auto batch_gen =
      MakeReadaheadGenerator(std::move(merged_batch_gen), options->fragment_readahead);

  auto gen = MakeMappedGenerator(
      std::move(batch_gen),
      [options](const EnumeratedRecordBatch& partial)
          -> Result<util::optional<compute::ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(
            util::optional<compute::ExecBatch> batch,
            compute::MakeExecBatch(*options->dataset_schema, partial.record_batch.value));
        // TODO(ARROW-13263) fragments may be able to attach more guarantees to batches
        // than this, for example parquet's row group stats. Failing to do this leaves
        // perf on the table because row group stats could be used to skip kernel execs in
        // FilterNode.
        //
        // Additionally, if a fragment failed to perform projection pushdown there may be
        // unnecessarily materialized columns in batch. We could drop them now instead of
        // letting them coast through the rest of the plan.
        batch->guarantee = partial.fragment.value->partition_expression();

        // tag rows with fragment- and batch-of-origin
        batch->values.emplace_back(partial.fragment.index);
        batch->values.emplace_back(partial.record_batch.index);
        batch->values.emplace_back(partial.record_batch.last);
        return batch;
      });

  auto fields = options->dataset_schema->fields();
  for (const auto& aug_field : kAugmentedFields) {
    fields.push_back(aug_field);
  }
  return compute::MakeSourceNode(plan, "dataset_scan", schema(std::move(fields)),
                                 std::move(gen));
}

class OneShotScanTask : public ScanTask {
 public:
  OneShotScanTask(RecordBatchIterator batch_it, std::shared_ptr<ScanOptions> options,
                  std::shared_ptr<Fragment> fragment)
      : ScanTask(std::move(options), std::move(fragment)),
        batch_it_(std::move(batch_it)) {}
  Result<RecordBatchIterator> Execute() override {
    if (!batch_it_) return Status::Invalid("OneShotScanTask was already scanned");
    return std::move(batch_it_);
  }

 private:
  RecordBatchIterator batch_it_;
};

class OneShotFragment : public Fragment {
 public:
  OneShotFragment(std::shared_ptr<Schema> schema, RecordBatchIterator batch_it)
      : Fragment(compute::literal(true), std::move(schema)),
        batch_it_(std::move(batch_it)) {
    DCHECK_NE(physical_schema_, nullptr);
  }
  Status CheckConsumed() {
    if (!batch_it_) return Status::Invalid("OneShotFragment was already scanned");
    return Status::OK();
  }
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options) override {
    RETURN_NOT_OK(CheckConsumed());
    ScanTaskVector tasks{std::make_shared<OneShotScanTask>(
        std::move(batch_it_), std::move(options), shared_from_this())};
    return MakeVectorIterator(std::move(tasks));
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    RETURN_NOT_OK(CheckConsumed());
    ARROW_ASSIGN_OR_RAISE(
        auto background_gen,
        MakeBackgroundGenerator(std::move(batch_it_), options->io_context.executor()));
    return MakeTransferredGenerator(std::move(background_gen),
                                    internal::GetCpuThreadPool());
  }
  std::string type_name() const override { return "one-shot"; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  }

  RecordBatchIterator batch_it_;
};
}  // namespace

Result<FragmentGenerator> AsyncScanner::GetFragments() const {
  // TODO(ARROW-8163): Async fragment scanning will return AsyncGenerator<Fragment>
  // here. Current iterator based versions are all fast & sync so we will just ToVector
  // it
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset_->GetFragments(scan_options_->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments_vec, fragments_it.ToVector());
  return MakeVectorGenerator(std::move(fragments_vec));
}

Result<TaggedRecordBatchIterator> AsyncScanner::ScanBatches() {
  ARROW_ASSIGN_OR_RAISE(auto batches_gen, ScanBatchesAsync(internal::GetCpuThreadPool()));
  return MakeGeneratorIterator(std::move(batches_gen));
}

Result<EnumeratedRecordBatchIterator> AsyncScanner::ScanBatchesUnordered() {
  ARROW_ASSIGN_OR_RAISE(auto batches_gen,
                        ScanBatchesUnorderedAsync(internal::GetCpuThreadPool()));
  return MakeGeneratorIterator(std::move(batches_gen));
}

Result<std::shared_ptr<Table>> AsyncScanner::ToTable() {
  auto table_fut = ToTableAsync(internal::GetCpuThreadPool());
  return table_fut.result();
}

Result<EnumeratedRecordBatchGenerator> AsyncScanner::ScanBatchesUnorderedAsync() {
  return ScanBatchesUnorderedAsync(internal::GetCpuThreadPool());
}

namespace {
Result<EnumeratedRecordBatch> ToEnumeratedRecordBatch(
    const util::optional<compute::ExecBatch>& batch, const ScanOptions& options,
    const FragmentVector& fragments) {
  int num_fields = options.projected_schema->num_fields();

  EnumeratedRecordBatch out;
  out.fragment.index = batch->values[num_fields].scalar_as<Int32Scalar>().value;
  out.fragment.last = false;  // ignored during reordering
  out.fragment.value = fragments[out.fragment.index];

  out.record_batch.index = batch->values[num_fields + 1].scalar_as<Int32Scalar>().value;
  out.record_batch.last = batch->values[num_fields + 2].scalar_as<BooleanScalar>().value;
  ARROW_ASSIGN_OR_RAISE(out.record_batch.value,
                        batch->ToRecordBatch(options.projected_schema, options.pool));
  return out;
}
}  // namespace

Result<EnumeratedRecordBatchGenerator> AsyncScanner::ScanBatchesUnorderedAsync(
    internal::Executor* cpu_executor) {
  if (!scan_options_->use_threads) {
    cpu_executor = nullptr;
  }

  auto exec_context =
      std::make_shared<compute::ExecContext>(scan_options_->pool, cpu_executor);

  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(exec_context.get()));

  ARROW_ASSIGN_OR_RAISE(auto scan, MakeScanNode(plan.get(), dataset_, scan_options_));

  ARROW_ASSIGN_OR_RAISE(auto filter,
                        compute::MakeFilterNode(scan, "filter", scan_options_->filter));

  auto exprs = scan_options_->projection.call()->arguments;
  auto names = checked_cast<const compute::MakeStructOptions*>(
                   scan_options_->projection.call()->options.get())
                   ->field_names;
  ARROW_ASSIGN_OR_RAISE(
      auto project,
      MakeAugmentedProjectNode(filter, "project", std::move(exprs), std::move(names)));

  AsyncGenerator<util::optional<compute::ExecBatch>> sink_gen =
      compute::MakeSinkNode(project, "sink");

  RETURN_NOT_OK(plan->StartProducing());

  auto options = scan_options_;
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset_->GetFragments(scan_options_->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments, fragments_it.ToVector());
  auto shared_fragments = std::make_shared<FragmentVector>(std::move(fragments));

  // If the generator is destroyed before being completely drained, inform plan
  std::shared_ptr<void> stop_producing{
      nullptr, [plan, exec_context](...) {
        bool not_finished_yet = plan->finished().TryAddCallback(
            [&plan, &exec_context] { return [plan, exec_context](const Status&) {}; });

        if (not_finished_yet) {
          plan->StopProducing();
        }
      }};

  return MakeMappedGenerator(
      std::move(sink_gen),
      [sink_gen, options, stop_producing,
       shared_fragments](const util::optional<compute::ExecBatch>& batch)
          -> Future<EnumeratedRecordBatch> {
        return ToEnumeratedRecordBatch(batch, *options, *shared_fragments);
      });
}

Result<TaggedRecordBatchGenerator> AsyncScanner::ScanBatchesAsync() {
  return ScanBatchesAsync(internal::GetCpuThreadPool());
}

Result<TaggedRecordBatchGenerator> AsyncScanner::ScanBatchesAsync(
    internal::Executor* cpu_executor) {
  ARROW_ASSIGN_OR_RAISE(auto unordered, ScanBatchesUnorderedAsync(cpu_executor));
  // We need an initial value sentinel, so we use one with fragment.index < 0
  auto is_before_any = [](const EnumeratedRecordBatch& batch) {
    return batch.fragment.index < 0;
  };
  auto left_after_right = [&is_before_any](const EnumeratedRecordBatch& left,
                                           const EnumeratedRecordBatch& right) {
    // Before any comes first
    if (is_before_any(left)) {
      return false;
    }
    if (is_before_any(right)) {
      return true;
    }
    // Compare batches if fragment is the same
    if (left.fragment.index == right.fragment.index) {
      return left.record_batch.index > right.record_batch.index;
    }
    // Otherwise compare fragment
    return left.fragment.index > right.fragment.index;
  };
  auto is_next = [is_before_any](const EnumeratedRecordBatch& prev,
                                 const EnumeratedRecordBatch& next) {
    // Only true if next is the first batch
    if (is_before_any(prev)) {
      return next.fragment.index == 0 && next.record_batch.index == 0;
    }
    // If same fragment, compare batch index
    if (prev.fragment.index == next.fragment.index) {
      return next.record_batch.index == prev.record_batch.index + 1;
    }
    // Else only if next first batch of next fragment and prev is last batch of previous
    return next.fragment.index == prev.fragment.index + 1 && prev.record_batch.last &&
           next.record_batch.index == 0;
  };
  auto before_any = EnumeratedRecordBatch{{nullptr, -1, false}, {nullptr, -1, false}};
  auto sequenced = MakeSequencingGenerator(std::move(unordered), left_after_right,
                                           is_next, before_any);

  auto unenumerate_fn = [](const EnumeratedRecordBatch& enumerated_batch) {
    return TaggedRecordBatch{enumerated_batch.record_batch.value,
                             enumerated_batch.fragment.value};
  };
  return MakeMappedGenerator(std::move(sequenced), unenumerate_fn);
}

struct AsyncTableAssemblyState {
  /// Protecting mutating accesses to batches
  std::mutex mutex{};
  std::vector<RecordBatchVector> batches{};

  void Emplace(const EnumeratedRecordBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    auto fragment_index = batch.fragment.index;
    auto batch_index = batch.record_batch.index;
    if (static_cast<int>(batches.size()) <= fragment_index) {
      batches.resize(fragment_index + 1);
    }
    if (static_cast<int>(batches[fragment_index].size()) <= batch_index) {
      batches[fragment_index].resize(batch_index + 1);
    }
    batches[fragment_index][batch_index] = batch.record_batch.value;
  }

  RecordBatchVector Finish() {
    RecordBatchVector all_batches;
    for (auto& fragment_batches : batches) {
      auto end = std::make_move_iterator(fragment_batches.end());
      for (auto it = std::make_move_iterator(fragment_batches.begin()); it != end; it++) {
        all_batches.push_back(*it);
      }
    }
    return all_batches;
  }
};

Status AsyncScanner::Scan(std::function<Status(TaggedRecordBatch)> visitor) {
  auto top_level_task = [this, &visitor](Executor* executor) {
    return VisitBatchesAsync(visitor, executor);
  };
  return internal::RunSynchronously<Future<>>(top_level_task, scan_options_->use_threads);
}

Future<> AsyncScanner::VisitBatchesAsync(std::function<Status(TaggedRecordBatch)> visitor,
                                         internal::Executor* executor) {
  ARROW_ASSIGN_OR_RAISE(auto batches_gen, ScanBatchesAsync(executor));
  return VisitAsyncGenerator(std::move(batches_gen), visitor);
}

Future<std::shared_ptr<Table>> AsyncScanner::ToTableAsync(
    internal::Executor* cpu_executor) {
  auto scan_options = scan_options_;
  ARROW_ASSIGN_OR_RAISE(auto positioned_batch_gen,
                        ScanBatchesUnorderedAsync(cpu_executor));
  /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
  /// invalidate concurrently running tasks when Finish() early returns
  /// and the mutex/batches fail out of scope.
  auto state = std::make_shared<AsyncTableAssemblyState>();

  auto table_building_task = [state](const EnumeratedRecordBatch& batch) {
    state->Emplace(batch);
    return batch;
  };

  auto table_building_gen =
      MakeMappedGenerator(positioned_batch_gen, table_building_task);

  return DiscardAllFromAsyncGenerator(table_building_gen).Then([state, scan_options]() {
    return Table::FromRecordBatches(scan_options->projected_schema, state->Finish());
  });
}

Result<int64_t> AsyncScanner::CountRows() {
  ARROW_ASSIGN_OR_RAISE(auto fragment_gen, GetFragments());

  auto cpu_executor = scan_options_->use_threads ? internal::GetCpuThreadPool() : nullptr;
  compute::ExecContext exec_context(scan_options_->pool, cpu_executor);

  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(&exec_context));
  // Drop projection since we only need to count rows
  auto options = std::make_shared<ScanOptions>(*scan_options_);
  RETURN_NOT_OK(SetProjection(options.get(), std::vector<std::string>()));

  std::atomic<int64_t> total{0};

  fragment_gen = MakeMappedGenerator(
      std::move(fragment_gen), [&](const std::shared_ptr<Fragment>& fragment) {
        return fragment->CountRows(options->filter, options)
            .Then([&, fragment](util::optional<int64_t> fast_count) mutable
                  -> std::shared_ptr<Fragment> {
              if (fast_count) {
                // fast path: got row count directly; skip scanning this fragment
                total += *fast_count;
                return std::make_shared<OneShotFragment>(
                    options->dataset_schema,
                    MakeEmptyIterator<std::shared_ptr<RecordBatch>>());
              }

              // slow path: actually filter this fragment's batches
              return std::move(fragment);
            });
      });

  ARROW_ASSIGN_OR_RAISE(auto scan,
                        MakeScanNode(plan.get(), std::move(fragment_gen), options));

  ARROW_ASSIGN_OR_RAISE(
      auto get_selection,
      compute::MakeProjectNode(scan, "get_selection", {options->filter}));

  ARROW_ASSIGN_OR_RAISE(
      auto sum_selection,
      compute::MakeScalarAggregateNode(get_selection, "sum_selection",
                                       {compute::internal::Aggregate{"sum", nullptr}}));

  AsyncGenerator<util::optional<compute::ExecBatch>> sink_gen =
      compute::MakeSinkNode(sum_selection, "sink");

  RETURN_NOT_OK(plan->StartProducing());
  auto maybe_slow_count = sink_gen().result();
  plan->finished().Wait();

  ARROW_ASSIGN_OR_RAISE(auto slow_count, maybe_slow_count);
  total += slow_count->values[0].scalar_as<UInt64Scalar>().value;

  return total.load();
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset)
    : ScannerBuilder(std::move(dataset), std::make_shared<ScanOptions>()) {}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanOptions> scan_options)
    : dataset_(std::move(dataset)), scan_options_(std::move(scan_options)) {
  scan_options_->dataset_schema = dataset_->schema();
  DCHECK_OK(Filter(scan_options_->filter));
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Schema> schema,
                               std::shared_ptr<Fragment> fragment,
                               std::shared_ptr<ScanOptions> scan_options)
    : ScannerBuilder(std::make_shared<FragmentDataset>(
                         std::move(schema), FragmentVector{std::move(fragment)}),
                     std::move(scan_options)) {}

std::shared_ptr<ScannerBuilder> ScannerBuilder::FromRecordBatchReader(
    std::shared_ptr<RecordBatchReader> reader) {
  auto batch_it = MakeIteratorFromReader(reader);
  auto fragment =
      std::make_shared<OneShotFragment>(reader->schema(), std::move(batch_it));
  return std::make_shared<ScannerBuilder>(reader->schema(), std::move(fragment),
                                          std::make_shared<ScanOptions>());
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

Status ScannerBuilder::Project(std::vector<compute::Expression> exprs,
                               std::vector<std::string> names) {
  return SetProjection(scan_options_.get(), std::move(exprs), std::move(names));
}

Status ScannerBuilder::Filter(const compute::Expression& filter) {
  return SetFilter(scan_options_.get(), filter);
}

Status ScannerBuilder::UseThreads(bool use_threads) {
  scan_options_->use_threads = use_threads;
  return Status::OK();
}

Status ScannerBuilder::FragmentReadahead(int fragment_readahead) {
  if (fragment_readahead <= 0) {
    return Status::Invalid("FragmentReadahead must be greater than 0, got ",
                           fragment_readahead);
  }
  scan_options_->fragment_readahead = fragment_readahead;
  return Status::OK();
}

Status ScannerBuilder::UseAsync(bool use_async) {
  scan_options_->use_async = use_async;
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

  if (scan_options_->use_async) {
    return std::make_shared<AsyncScanner>(dataset_, scan_options_);
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
      ARROW_ASSIGN_OR_RAISE(
          auto local, internal::SerialExecutor::RunInSerialExecutor<RecordBatchVector>(
                          [&](internal::Executor* executor) {
                            return scan_task->SafeExecute(executor);
                          }));
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

Result<int64_t> SyncScanner::CountRows() {
  // While readers could implement an optimization where they just fabricate empty
  // batches based on metadata when no columns are selected, skipping I/O (and
  // indeed, the Parquet reader does this), counting rows using that optimization is
  // still slower than just hitting metadata directly where possible.
  ARROW_ASSIGN_OR_RAISE(auto fragment_it, GetFragments());
  // Fragment is non-null iff fast path could not be taken.
  std::vector<Future<std::pair<int64_t, std::shared_ptr<Fragment>>>> futures;
  for (auto maybe_fragment : fragment_it) {
    ARROW_ASSIGN_OR_RAISE(auto fragment, maybe_fragment);
    auto count_fut = fragment->CountRows(scan_options_->filter, scan_options_);
    futures.push_back(
        count_fut.Then([fragment](const util::optional<int64_t>& count)
                           -> std::pair<int64_t, std::shared_ptr<Fragment>> {
          if (count.has_value()) {
            return std::make_pair(*count, nullptr);
          }
          return std::make_pair(0, std::move(fragment));
        }));
  }

  int64_t count = 0;
  FragmentVector fragments;
  for (auto& future : futures) {
    ARROW_ASSIGN_OR_RAISE(auto count_result, future.result());
    count += count_result.first;
    if (count_result.second) {
      fragments.push_back(std::move(count_result.second));
    }
  }
  // Now check for any fragments where we couldn't take the fast path
  if (!fragments.empty()) {
    auto options = std::make_shared<ScanOptions>(*scan_options_);
    RETURN_NOT_OK(SetProjection(options.get(), std::vector<std::string>()));
    ARROW_ASSIGN_OR_RAISE(
        auto scan_task_it,
        GetScanTaskIterator(MakeVectorIterator(std::move(fragments)), options));
    ARROW_ASSIGN_OR_RAISE(auto batch_it, ScanBatches(std::move(scan_task_it)));
    RETURN_NOT_OK(batch_it.Visit([&](TaggedRecordBatch batch) {
      count += batch.record_batch->num_rows();
      return Status::OK();
    }));
  }
  return count;
}

Result<compute::ExecNode*> MakeScanNode(compute::ExecPlan* plan,
                                        std::shared_ptr<Dataset> dataset,
                                        std::shared_ptr<ScanOptions> scan_options) {
  if (scan_options->dataset_schema == nullptr) {
    scan_options->dataset_schema = dataset->schema();
  }

  if (!scan_options->filter.IsBound()) {
    ARROW_ASSIGN_OR_RAISE(scan_options->filter,
                          scan_options->filter.Bind(*dataset->schema()));
  }

  if (!scan_options->projection.IsBound()) {
    auto fields = dataset->schema()->fields();
    for (const auto& aug_field : kAugmentedFields) {
      fields.push_back(aug_field);
    }

    ARROW_ASSIGN_OR_RAISE(scan_options->projection,
                          scan_options->projection.Bind(Schema(std::move(fields))));
  }

  // using a generator for speculative forward compatibility with async fragment discovery
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset->GetFragments(scan_options->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments_vec, fragments_it.ToVector());
  auto fragments_gen = MakeVectorGenerator(std::move(fragments_vec));

  return MakeScanNode(plan, std::move(fragments_gen), std::move(scan_options));
}

Result<compute::ExecNode*> MakeAugmentedProjectNode(
    compute::ExecNode* input, std::string label, std::vector<compute::Expression> exprs,
    std::vector<std::string> names) {
  if (names.size() == 0) {
    names.resize(exprs.size());
    for (size_t i = 0; i < exprs.size(); ++i) {
      names[i] = exprs[i].ToString();
    }
  }

  for (const auto& aug_field : kAugmentedFields) {
    exprs.push_back(compute::field_ref(aug_field->name()));
    names.push_back(aug_field->name());
  }
  return compute::MakeProjectNode(input, std::move(label), std::move(exprs),
                                  std::move(names));
}

Result<AsyncGenerator<util::optional<compute::ExecBatch>>> MakeOrderedSinkNode(
    compute::ExecNode* input, std::string label) {
  auto unordered = compute::MakeSinkNode(input, std::move(label));

  const Schema& schema = *input->output_schema();
  ARROW_ASSIGN_OR_RAISE(FieldPath match, FieldRef("__fragment_index").FindOne(schema));
  int i = match[0];
  auto fragment_index = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<Int32Scalar>().value;
  };
  compute::ExecBatch before_any{{}, 0};
  before_any.values.resize(i + 1);
  before_any.values.back() = Datum(-1);

  ARROW_ASSIGN_OR_RAISE(match, FieldRef("__batch_index").FindOne(schema));
  i = match[0];
  auto batch_index = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<Int32Scalar>().value;
  };

  ARROW_ASSIGN_OR_RAISE(match, FieldRef("__last_in_fragment").FindOne(schema));
  i = match[0];
  auto last_in_fragment = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<BooleanScalar>().value;
  };

  auto is_before_any = [=](const compute::ExecBatch& batch) {
    return fragment_index(batch) < 0;
  };

  auto left_after_right = [=](const util::optional<compute::ExecBatch>& left,
                              const util::optional<compute::ExecBatch>& right) {
    // Before any comes first
    if (is_before_any(*left)) {
      return false;
    }
    if (is_before_any(*right)) {
      return true;
    }
    // Compare batches if fragment is the same
    if (fragment_index(*left) == fragment_index(*right)) {
      return batch_index(*left) > batch_index(*right);
    }
    // Otherwise compare fragment
    return fragment_index(*left) > fragment_index(*right);
  };

  auto is_next = [=](const util::optional<compute::ExecBatch>& prev,
                     const util::optional<compute::ExecBatch>& next) {
    // Only true if next is the first batch
    if (is_before_any(*prev)) {
      return fragment_index(*next) == 0 && batch_index(*next) == 0;
    }
    // If same fragment, compare batch index
    if (fragment_index(*next) == fragment_index(*prev)) {
      return batch_index(*next) == batch_index(*prev) + 1;
    }
    // Else only if next first batch of next fragment and prev is last batch of previous
    return fragment_index(*next) == fragment_index(*prev) + 1 &&
           last_in_fragment(*prev) && batch_index(*next) == 0;
  };

  return MakeSequencingGenerator(std::move(unordered), left_after_right, is_next,
                                 util::make_optional(std::move(before_any)));
}

}  // namespace dataset
}  // namespace arrow

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

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace dataset {

ScanOptions::ScanOptions()
    : filter(scalar(true)), evaluator(ExpressionEvaluator::Null()) {}

std::shared_ptr<ScanOptions> ScanOptions::Defaults() {
  return std::shared_ptr<ScanOptions>(new ScanOptions);
}

RecordBatchIterator SimpleScanTask::Scan() { return MakeVectorIterator(record_batches_); }

/// \brief GetScanTaskIterator transforms an Iterator<DataFragment> in a
/// flattened Iterator<ScanTask>.
static ScanTaskIterator GetScanTaskIterator(DataFragmentIterator fragments,
                                            std::shared_ptr<ScanContext> context) {
  // DataFragment -> ScanTaskIterator
  auto fn = [context](std::shared_ptr<DataFragment> fragment,
                      ScanTaskIterator* out) -> Status {
    return fragment->Scan(context, out);
  };

  // Iterator<Iterator<ScanTask>>
  auto maybe_scantask_it = MakeMaybeMapIterator(fn, std::move(fragments));

  // Iterator<ScanTask>
  return MakeFlattenIterator(std::move(maybe_scantask_it));
}

static ScanTaskIterator ProjectAndFilterScanTaskIterator(
    ScanTaskIterator it, std::shared_ptr<Expression> filter,
    std::shared_ptr<ExpressionEvaluator> evaluator,
    std::shared_ptr<RecordBatchProjector> projector) {
  // Wrap the scanner ScanTask with a FilterAndProjectScanTask
  auto wrap_scan_task = [filter, evaluator, projector](
                            std::unique_ptr<ScanTask> task) -> std::unique_ptr<ScanTask> {
    return internal::make_unique<FilterAndProjectScanTask>(std::move(task), filter,
                                                           evaluator, projector);
  };
  return MakeMapIterator(wrap_scan_task, std::move(it));
}

ScanTaskIterator SimpleScanner::Scan() {
  // First, transforms DataSources in a flat Iterator<DataFragment>. This
  // iterator is lazily constructed, i.e. DataSource::GetFragments is never
  // invoked.
  auto fragments_it = GetFragmentsFromSources(sources_, options_);
  // Second, transforms Iterator<DataFragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.
  auto scan_task_it = GetScanTaskIterator(std::move(fragments_it), context_);
  // Third, apply the filter and/or projection to incoming RecordBatches.
  return ProjectAndFilterScanTaskIterator(std::move(scan_task_it), options_->filter,
                                          options_->evaluator, options_->projector);
}

Status ScanTaskIteratorFromRecordBatch(std::vector<std::shared_ptr<RecordBatch>> batches,
                                       ScanTaskIterator* out) {
  std::unique_ptr<ScanTask> scan_task = internal::make_unique<SimpleScanTask>(batches);
  std::vector<std::unique_ptr<ScanTask>> tasks;
  tasks.emplace_back(std::move(scan_task));
  *out = MakeVectorIterator(std::move(tasks));
  return Status::OK();
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanContext> scan_context)
    : dataset_(std::move(dataset)),
      scan_options_(ScanOptions::Defaults()),
      scan_context_(std::move(scan_context)) {}

Status EnsureColumnsInSchema(const std::shared_ptr<Schema>& schema,
                             const std::vector<std::string>& columns) {
  for (const auto& column : columns) {
    if (schema->GetFieldByName(column) == nullptr) {
      return Status::Invalid("Requested column ", column,
                             " not found in dataset's schema.");
    }
  }

  return Status::OK();
}

Status ScannerBuilder::Project(const std::vector<std::string>& columns) {
  RETURN_NOT_OK(EnsureColumnsInSchema(schema(), columns));
  has_projection_ = true;
  project_columns_ = columns;
  return Status::OK();
}

Status ScannerBuilder::Filter(std::shared_ptr<Expression> filter, bool implicit_casts) {
  RETURN_NOT_OK(EnsureColumnsInSchema(schema(), FieldsInExpression(*filter)));
  if (implicit_casts) {
    ARROW_ASSIGN_OR_RAISE(scan_options_->filter, InsertImplicitCasts(*filter, *schema()));
  } else {
    scan_options_->filter = std::move(filter);
  }
  return Status::OK();
}

Status ScannerBuilder::Filter(const Expression& filter, bool implicit_casts) {
  return Filter(filter.Copy(), implicit_casts);
}

Status ScannerBuilder::UseThreads(bool use_threads) {
  scan_options_->use_threads = use_threads;
  return Status::OK();
}

Status ScannerBuilder::Finish(std::unique_ptr<Scanner>* out) const {
  scan_options_->schema = dataset_->schema();
  if (has_projection_ && !project_columns_.empty()) {
    auto projected_schema = SchemaFromColumnNames(schema(), project_columns_);
    scan_options_->schema = projected_schema;
    scan_options_->projector =
        std::make_shared<RecordBatchProjector>(scan_context_->pool, projected_schema);
  }

  if (scan_options_->filter->Equals(true)) {
    scan_options_->evaluator = ExpressionEvaluator::Null();
  } else {
    scan_options_->evaluator = std::make_shared<TreeEvaluator>(scan_context_->pool);
  }

  out->reset(new SimpleScanner(dataset_->sources(), scan_options_, scan_context_));
  return Status::OK();
}

using arrow::internal::TaskGroup;

std::shared_ptr<TaskGroup> Scanner::TaskGroup() const {
  return options_->use_threads ? TaskGroup::MakeThreaded(context_->thread_pool)
                               : TaskGroup::MakeSerial();
}

struct TableAggregator {
  void Append(std::shared_ptr<RecordBatch> batch) {
    std::lock_guard<std::mutex> lock(m);
    batches.emplace_back(std::move(batch));
  }

  Status Finish(const std::shared_ptr<Schema>& schema, std::shared_ptr<Table>* out) {
    return Table::FromRecordBatches(schema, batches, out);
  }

  std::mutex m;
  std::vector<std::shared_ptr<RecordBatch>> batches;
};

struct ScanTaskPromise {
  Status operator()() {
    for (auto maybe_batch : task->Scan()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
      aggregator.Append(std::move(batch));
    }

    return Status::OK();
  }

  TableAggregator& aggregator;
  std::shared_ptr<ScanTask> task;
};

Status Scanner::ToTable(std::shared_ptr<Table>* out) {
  auto task_group = TaskGroup();

  TableAggregator aggregator;
  for (auto maybe_scan_task : Scan()) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, std::move(maybe_scan_task));
    task_group->Append(ScanTaskPromise{aggregator, std::move(scan_task)});
  }

  // Wait for all tasks to complete, or the first error.
  RETURN_NOT_OK(task_group->Finish());

  return aggregator.Finish(options_->schema, out);
}

}  // namespace dataset
}  // namespace arrow

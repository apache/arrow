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

ScanOptions::ScanOptions(std::shared_ptr<Schema> schema)
    : filter(scalar(true)),
      evaluator(ExpressionEvaluator::Null()),
      projector(RecordBatchProjector(std::move(schema))) {}

std::shared_ptr<ScanOptions> ScanOptions::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  auto copy = ScanOptions::Make(std::move(schema));
  copy->use_threads = use_threads;
  copy->filter = filter;
  copy->evaluator = evaluator;
  return copy;
}

std::vector<std::string> ScanOptions::MaterializedFields() const {
  std::vector<std::string> fields;

  for (const auto& f : schema()->fields()) {
    fields.push_back(f->name());
  }

  for (auto&& name : FieldsInExpression(filter)) {
    fields.push_back(std::move(name));
  }

  return fields;
}

Result<RecordBatchIterator> InMemoryScanTask::Execute() {
  return MakeVectorIterator(record_batches_);
}

/// \brief GetScanTaskIterator transforms an Iterator<Fragment> in a
/// flattened Iterator<ScanTask>.
static ScanTaskIterator GetScanTaskIterator(FragmentIterator fragments,
                                            std::shared_ptr<ScanContext> context) {
  // Fragment -> ScanTaskIterator
  auto fn = [context](std::shared_ptr<Fragment> fragment) {
    return fragment->Scan(context);
  };

  // Iterator<Iterator<ScanTask>>
  auto maybe_scantask_it = MakeMaybeMapIterator(fn, std::move(fragments));

  // Iterator<ScanTask>
  return MakeFlattenIterator(std::move(maybe_scantask_it));
}

Result<ScanTaskIterator> Scanner::Scan() {
  // First, transforms Sources in a flat Iterator<Fragment>. This
  // iterator is lazily constructed, i.e. Source::GetFragments is never
  // invoked.
  auto fragments_it = GetFragmentsFromSources(sources_, options_);
  // Second, transforms Iterator<Fragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.
  auto scan_task_it = GetScanTaskIterator(std::move(fragments_it), context_);
  // Third, apply the filter and/or projection to incoming RecordBatches by
  // wrapping the ScanTask with a FilterAndProjectScanTask
  auto wrap_scan_task = [](std::shared_ptr<ScanTask> task) -> std::shared_ptr<ScanTask> {
    return std::make_shared<FilterAndProjectScanTask>(std::move(task));
  };
  return MakeMapIterator(wrap_scan_task, std::move(scan_task_it));
}

Result<ScanTaskIterator> ScanTaskIteratorFromRecordBatch(
    std::vector<std::shared_ptr<RecordBatch>> batches,
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context) {
  ScanTaskVector tasks{std::make_shared<InMemoryScanTask>(batches, std::move(options),
                                                          std::move(context))};
  return MakeVectorIterator(std::move(tasks));
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanContext> context)
    : dataset_(std::move(dataset)),
      options_(ScanOptions::Make(dataset_->schema())),
      context_(std::move(context)) {}

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

Status ScannerBuilder::Filter(std::shared_ptr<Expression> filter) {
  RETURN_NOT_OK(EnsureColumnsInSchema(schema(), FieldsInExpression(*filter)));
  RETURN_NOT_OK(filter->Validate(*schema()).status());
  options_->filter = std::move(filter);
  return Status::OK();
}

Status ScannerBuilder::Filter(const Expression& filter) { return Filter(filter.Copy()); }

Status ScannerBuilder::UseThreads(bool use_threads) {
  options_->use_threads = use_threads;
  return Status::OK();
}

Result<std::shared_ptr<Scanner>> ScannerBuilder::Finish() const {
  std::shared_ptr<ScanOptions> options;
  if (has_projection_ && !project_columns_.empty()) {
    options = options_->ReplaceSchema(SchemaFromColumnNames(schema(), project_columns_));
  } else {
    options = std::make_shared<ScanOptions>(*options_);
  }

  if (!options->filter->Equals(true)) {
    options->evaluator = std::make_shared<TreeEvaluator>();
  }

  return std::make_shared<Scanner>(dataset_->sources(), std::move(options), context_);
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

  Result<std::shared_ptr<Table>> Finish(const std::shared_ptr<Schema>& schema) {
    std::shared_ptr<Table> out;
    RETURN_NOT_OK(Table::FromRecordBatches(schema, batches, &out));
    return out;
  }

  std::mutex m;
  std::vector<std::shared_ptr<RecordBatch>> batches;
};

struct ScanTaskPromise {
  Status operator()() {
    ARROW_ASSIGN_OR_RAISE(auto it, task->Execute());
    for (auto maybe_batch : it) {
      ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
      aggregator.Append(std::move(batch));
    }

    return Status::OK();
  }

  TableAggregator& aggregator;
  std::shared_ptr<ScanTask> task;
};

Result<std::shared_ptr<Table>> Scanner::ToTable() {
  auto task_group = TaskGroup();

  TableAggregator aggregator;
  ARROW_ASSIGN_OR_RAISE(auto it, Scan());
  for (auto maybe_scan_task : it) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, std::move(maybe_scan_task));
    task_group->Append(ScanTaskPromise{aggregator, std::move(scan_task)});
  }

  // Wait for all tasks to complete, or the first error.
  RETURN_NOT_OK(task_group->Finish());

  return aggregator.Finish(options_->schema());
}

}  // namespace dataset
}  // namespace arrow

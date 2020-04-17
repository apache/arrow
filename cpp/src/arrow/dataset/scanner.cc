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
  copy->filter = filter;
  copy->evaluator = evaluator;
  copy->batch_size = batch_size;
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

FragmentIterator Scanner::GetFragments() {
  if (fragment_ != nullptr) {
    return MakeVectorIterator(FragmentVector{fragment_});
  }

  // Transform Datasets in a flat Iterator<Fragment>. This
  // iterator is lazily constructed, i.e. Dataset::GetFragments is
  // not invoked until a Fragment is requested.
  return GetFragmentsFromDatasets({dataset_}, scan_options_);
}

Result<ScanTaskIterator> Scanner::Scan() {
  // Transforms Iterator<Fragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.
  auto scan_task_it = GetScanTaskIterator(GetFragments(), scan_context_);

  // Apply the filter and/or projection to incoming RecordBatches by
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
                               std::shared_ptr<ScanContext> scan_context)
    : dataset_(std::move(dataset)),
      scan_options_(ScanOptions::Make(dataset_->schema())),
      scan_context_(std::move(scan_context)) {}

Status ScannerBuilder::Project(std::vector<std::string> columns) {
  RETURN_NOT_OK(schema()->CanReferenceFieldsByNames(columns));
  has_projection_ = true;
  project_columns_ = std::move(columns);
  return Status::OK();
}

Status ScannerBuilder::Filter(std::shared_ptr<Expression> filter) {
  RETURN_NOT_OK(schema()->CanReferenceFieldsByNames(FieldsInExpression(*filter)));
  RETURN_NOT_OK(filter->Validate(*schema()).status());
  scan_options_->filter = std::move(filter);
  return Status::OK();
}

Status ScannerBuilder::Filter(const Expression& filter) { return Filter(filter.Copy()); }

Status ScannerBuilder::UseThreads(bool use_threads) {
  scan_context_->use_threads = use_threads;
  return Status::OK();
}

Status ScannerBuilder::BatchSize(int64_t batch_size) {
  if (batch_size <= 0) {
    return Status::Invalid("BatchSize must be greater than 0, got ", batch_size);
  }
  scan_options_->batch_size = batch_size;
  return Status::OK();
}

Result<std::shared_ptr<Scanner>> ScannerBuilder::Finish() const {
  std::shared_ptr<ScanOptions> scan_options;
  if (has_projection_ && !project_columns_.empty()) {
    scan_options =
        scan_options_->ReplaceSchema(SchemaFromColumnNames(schema(), project_columns_));
  } else {
    scan_options = std::make_shared<ScanOptions>(*scan_options_);
  }

  if (!scan_options->filter->Equals(true)) {
    scan_options->evaluator = std::make_shared<TreeEvaluator>();
  }

  return std::make_shared<Scanner>(dataset_, std::move(scan_options), scan_context_);
}

std::shared_ptr<internal::TaskGroup> ScanContext::TaskGroup() const {
  if (use_threads) {
    internal::ThreadPool* thread_pool = arrow::internal::GetCpuThreadPool();
    return internal::TaskGroup::MakeThreaded(thread_pool);
  }
  return internal::TaskGroup::MakeSerial();
}

Result<std::shared_ptr<Table>> Scanner::ToTable() {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, Scan());
  std::mutex mutex;
  RecordBatchVector batches;

  auto task_group = scan_context_->TaskGroup();

  for (auto maybe_scan_task : scan_task_it) {
    ARROW_ASSIGN_OR_RAISE(auto scan_task, std::move(maybe_scan_task));

    task_group->Append([&batches, &mutex, scan_task] {
      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());

      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
        std::lock_guard<std::mutex> lock(mutex);
        batches.emplace_back(std::move(batch));
      }

      return Status::OK();
    });
  }

  // Wait for all tasks to complete, or the first error.
  RETURN_NOT_OK(task_group->Finish());

  return Table::FromRecordBatches(scan_options_->schema(), std::move(batches));
}

}  // namespace dataset
}  // namespace arrow

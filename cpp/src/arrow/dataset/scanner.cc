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

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"

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

Status ScannerBuilder::Filter(std::shared_ptr<Expression> filter) {
  RETURN_NOT_OK(EnsureColumnsInSchema(schema(), FieldsInExpression(*filter)));
  scan_options_->filter = std::move(filter);
  return Status::OK();
}

Status ScannerBuilder::Filter(const Expression& filter) { return Filter(filter.Copy()); }

std::shared_ptr<Schema> SchemaFromColumnNames(
    const std::shared_ptr<Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<Field>> columns;
  for (const auto& name : column_names) {
    columns.push_back(input->GetFieldByName(name));
  }

  return std::make_shared<Schema>(columns);
}

Status ScannerBuilder::Finish(std::unique_ptr<Scanner>* out) const {
  if (has_projection_ && !project_columns_.empty()) {
    scan_options_->projector = std::make_shared<RecordBatchProjector>(
        scan_context_->pool, SchemaFromColumnNames(schema(), project_columns_));
  }

  if (scan_options_->filter->Equals(true)) {
    scan_options_->evaluator = ExpressionEvaluator::Null();
  } else {
    scan_options_->evaluator = std::make_shared<TreeEvaluator>(scan_context_->pool);
  }

  out->reset(new SimpleScanner(dataset_->sources(), scan_options_, scan_context_));
  return Status::OK();
}

Status Scanner::ToTable(std::shared_ptr<Table>* out) {
  std::vector<std::shared_ptr<RecordBatch>> batches;

  auto it_scantasks = Scan();
  RETURN_NOT_OK(it_scantasks.Visit([&batches](std::unique_ptr<ScanTask> task) -> Status {
    auto it = task->Scan();
    return it.Visit([&batches](std::shared_ptr<RecordBatch> batch) {
      batches.push_back(batch);
      return Status::OK();
    });
  }));

  return Table::FromRecordBatches(batches, out);
}

}  // namespace dataset
}  // namespace arrow

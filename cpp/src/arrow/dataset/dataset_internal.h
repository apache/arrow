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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace dataset {

/// \brief GetFragmentsFromDatasets transforms a vector<Dataset> into a
/// flattened FragmentIterator.
static inline FragmentIterator GetFragmentsFromDatasets(
    const DatasetVector& datasets, std::shared_ptr<ScanOptions> options) {
  // Iterator<Dataset>
  auto datasets_it = MakeVectorIterator(datasets);

  // Dataset -> Iterator<Fragment>
  auto fn = [options](std::shared_ptr<Dataset> dataset) -> FragmentIterator {
    return dataset->GetFragments(options);
  };

  // Iterator<Iterator<Fragment>>
  auto fragments_it = MakeMapIterator(fn, std::move(datasets_it));

  // Iterator<Fragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

inline std::shared_ptr<Schema> SchemaFromColumnNames(
    const std::shared_ptr<Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<Field>> columns;
  for (FieldRef ref : column_names) {
    auto maybe_field = ref.GetOne(*input);
    if (maybe_field.ok()) {
      columns.push_back(std::move(maybe_field).ValueOrDie());
    }
  }

  return schema(std::move(columns));
}

inline std::shared_ptr<internal::TaskGroup> MakeTaskGroup(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<ScanContext>& context) {
  return options->use_threads ? internal::TaskGroup::MakeThreaded(context->thread_pool)
                              : internal::TaskGroup::MakeSerial();
}

static inline RecordBatchIterator FilterRecordBatch(RecordBatchIterator it,
                                                    const ExpressionEvaluator& evaluator,
                                                    const Expression& filter,
                                                    MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [&filter, &evaluator, pool](std::shared_ptr<RecordBatch> in) {
        return evaluator.Evaluate(filter, *in, pool).Map([&](compute::Datum selection) {
          return evaluator.Filter(selection, in);
        });
      },
      std::move(it));
}

static inline RecordBatchIterator ProjectRecordBatch(RecordBatchIterator it,
                                                     RecordBatchProjector* projector,
                                                     MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [=](std::shared_ptr<RecordBatch> in) { return projector->Project(*in, pool); },
      std::move(it));
}

class FilterAndProjectScanTask : public ScanTask {
 public:
  explicit FilterAndProjectScanTask(std::shared_ptr<ScanTask> task)
      : ScanTask(task->options(), task->context()), task_(std::move(task)) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto it, task_->Execute());
    auto filter_it = FilterRecordBatch(std::move(it), *options_->evaluator,
                                       *options_->filter, context_->pool);
    return ProjectRecordBatch(std::move(filter_it), &task_->options()->projector,
                              context_->pool);
  }

  static ScanTaskIterator Wrap(ScanTaskIterator scan_task_it) {
    auto wrap_scan_task =
        [](std::shared_ptr<ScanTask> task) -> std::shared_ptr<ScanTask> {
      return std::make_shared<FilterAndProjectScanTask>(std::move(task));
    };
    return MakeMapIterator(wrap_scan_task, std::move(scan_task_it));
  }

 private:
  std::shared_ptr<ScanTask> task_;
};

struct TableAggregator {
  void Append(std::shared_ptr<RecordBatch> batch) {
    std::lock_guard<std::mutex> lock(m);
    batches.emplace_back(std::move(batch));
  }

  template <typename... Args>
  Status AppendFrom(ScanTaskIterator scan_task_it, Args&&... args) {
    auto task_group = MakeTaskGroup(std::forward<Args>(args)...);

    for (auto maybe_scan_task : scan_task_it) {
      ARROW_ASSIGN_OR_RAISE(auto scan_task, std::move(maybe_scan_task));
      AppendFrom(std::move(scan_task), task_group.get());
    }

    // Wait for all tasks to complete, or the first error.
    return task_group->Finish();
  }

  void AppendFrom(std::shared_ptr<ScanTask> scan_task, internal::TaskGroup* task_group) {
    task_group->Append([this, scan_task] {
      ARROW_ASSIGN_OR_RAISE(auto batch_it, scan_task->Execute());
      for (auto maybe_batch : batch_it) {
        ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
        Append(std::move(batch));
      }

      return Status::OK();
    });
  }

  Result<std::shared_ptr<Table>> Finish(const std::shared_ptr<Schema>& schema) {
    std::shared_ptr<Table> out;
    RETURN_NOT_OK(Table::FromRecordBatches(schema, batches, &out));
    return out;
  }

  std::mutex m;
  std::vector<std::shared_ptr<RecordBatch>> batches;
};

}  // namespace dataset
}  // namespace arrow

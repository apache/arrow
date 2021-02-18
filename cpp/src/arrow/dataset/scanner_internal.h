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
#include <utility>

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {

inline RecordBatchIterator FilterRecordBatch(RecordBatchIterator it, Expression filter,
                                             MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [=](std::shared_ptr<RecordBatch> in) -> Result<std::shared_ptr<RecordBatch>> {
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
      },
      std::move(it));
}

inline RecordBatchIterator ProjectRecordBatch(RecordBatchIterator it,
                                              Expression projection, MemoryPool* pool) {
  return MakeMaybeMapIterator(
      [=](std::shared_ptr<RecordBatch> in) -> Result<std::shared_ptr<RecordBatch>> {
        compute::ExecContext exec_context{pool};
        ARROW_ASSIGN_OR_RAISE(Datum projected, ExecuteScalarExpression(
                                                   projection, Datum(in), &exec_context));

        DCHECK_EQ(projected.type()->id(), Type::STRUCT);
        if (projected.shape() == ValueDescr::SCALAR) {
          // Only virtual columns are projected. Broadcast to an array
          ARROW_ASSIGN_OR_RAISE(
              projected, MakeArrayFromScalar(*projected.scalar(), in->num_rows(), pool));
        }

        return RecordBatch::FromStructArray(projected.array_as<StructArray>());
      },
      std::move(it));
}

class FilterAndProjectScanTask : public ScanTask {
 public:
  explicit FilterAndProjectScanTask(std::shared_ptr<ScanTask> task, Expression partition)
      : ScanTask(task->options(), task->context()),
        task_(std::move(task)),
        partition_(std::move(partition)),
        filter_(options()->filter),
        projection_(options()->projection) {}

  Result<RecordBatchIterator> Execute() override {
    ARROW_ASSIGN_OR_RAISE(auto it, task_->Execute());

    ARROW_ASSIGN_OR_RAISE(Expression simplified_filter,
                          SimplifyWithGuarantee(filter_, partition_));

    ARROW_ASSIGN_OR_RAISE(Expression simplified_projection,
                          SimplifyWithGuarantee(projection_, partition_));

    RecordBatchIterator filter_it =
        FilterRecordBatch(std::move(it), simplified_filter, context_->pool);

    return ProjectRecordBatch(std::move(filter_it), simplified_projection,
                              context_->pool);
  }

 private:
  std::shared_ptr<ScanTask> task_;
  Expression partition_;
  Expression filter_;
  Expression projection_;
};

/// \brief GetScanTaskIterator transforms an Iterator<Fragment> in a
/// flattened Iterator<ScanTask>.
inline Result<ScanTaskIterator> GetScanTaskIterator(
    FragmentIterator fragments, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) {
  // Fragment -> ScanTaskIterator
  auto fn = [options,
             context](std::shared_ptr<Fragment> fragment) -> Result<ScanTaskIterator> {
    ARROW_ASSIGN_OR_RAISE(auto scan_task_it, fragment->Scan(options, context));

    auto partition = fragment->partition_expression();
    // Apply the filter and/or projection to incoming RecordBatches by
    // wrapping the ScanTask with a FilterAndProjectScanTask
    auto wrap_scan_task =
        [partition](std::shared_ptr<ScanTask> task) -> std::shared_ptr<ScanTask> {
      return std::make_shared<FilterAndProjectScanTask>(std::move(task), partition);
    };

    return MakeMapIterator(wrap_scan_task, std::move(scan_task_it));
  };

  // Iterator<Iterator<ScanTask>>
  auto maybe_scantask_it = MakeMaybeMapIterator(fn, std::move(fragments));

  // Iterator<ScanTask>
  return MakeFlattenIterator(std::move(maybe_scantask_it));
}

// FIXME delete this
inline Status SetProjection(ScanOptions* options, const std::vector<std::string>& names) {
  options->projected_schema = SchemaFromColumnNames(options->dataset_schema, names);

  std::vector<Expression> project_args;
  compute::ProjectOptions project_options{{}};

  for (const auto& field : options->projected_schema->fields()) {
    project_args.push_back(field_ref(field->name()));
    project_options.field_names.push_back(field->name());
    project_options.field_nullability.push_back(field->nullable());
    project_options.field_metadata.push_back(field->metadata());
  }

  ARROW_ASSIGN_OR_RAISE(options->projection, call("project", std::move(project_args),
                                                  std::move(project_options))
                                                 .Bind(*options->dataset_schema));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow

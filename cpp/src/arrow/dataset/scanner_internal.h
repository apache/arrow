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

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"

namespace arrow {
namespace dataset {

static inline RecordBatchIterator FilterRecordBatch(RecordBatchIterator it,
                                                    ScanOptionsPtr options,
                                                    ScanContextPtr context) {
  auto filter_fn = [options, context](std::shared_ptr<RecordBatch> in,
                                      std::shared_ptr<RecordBatch>* out) {
    ARROW_ASSIGN_OR_RAISE(
        auto selection_datum,
        options->evaluator->Evaluate(*options->filter, *in, context->pool));
    return options->evaluator->Filter(selection_datum, in, context->pool).Value(out);
  };

  return MakeMaybeMapIterator(filter_fn, std::move(it));
}

static inline RecordBatchIterator ProjectRecordBatch(RecordBatchIterator it,
                                                     ScanOptionsPtr options,
                                                     ScanContextPtr context) {
  if (options->projector == nullptr) {
    return it;
  }

  auto project = [options, context](std::shared_ptr<RecordBatch> in,
                                    std::shared_ptr<RecordBatch>* out) {
    return options->projector->Project(*in, context->pool).Value(out);
  };
  return MakeMaybeMapIterator(project, std::move(it));
}

class FilterAndProjectScanTask : public ScanTask {
 public:
  explicit FilterAndProjectScanTask(ScanTaskPtr task) : task_(std::move(task)) {}

  Result<RecordBatchIterator> Scan() override {
    ARROW_ASSIGN_OR_RAISE(auto it, task_->Scan());
    auto filter_it = FilterRecordBatch(std::move(it), task_->options(), task_->context());
    return ProjectRecordBatch(std::move(filter_it), task_->options(), task_->context());
  }

 private:
  ScanTaskPtr task_;
};

}  // namespace dataset
}  // namespace arrow

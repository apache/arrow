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
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace dataset {

/// \brief Project a RecordBatch to a given schema.
///
/// Projected record batches will reorder columns from input record batches when possible,
/// otherwise the given schema will be satisfied by augmenting with null or constant
/// columns.
///
/// RecordBatchProjector is most efficient when projecting record batches with a
/// consistent schema (for example batches from a table), but it can project record
/// batches having any schema.
class RecordBatchProjector {
 public:
  /// A column required by the given schema but absent from a record batch will be added
  /// to the projected record batch with all its slots null.
  RecordBatchProjector(MemoryPool* pool, std::shared_ptr<Schema> to)
      : pool_(pool),
        to_(std::move(to)),
        missing_columns_(to_->num_fields(), nullptr),
        column_indices_(to_->num_fields(), kNoMatch),
        scalars_(to_->num_fields(), nullptr) {}

  /// A column required by the given schema but absent from a record batch will be added
  /// to the projected record batch with all its slots equal to the corresponding entry in
  /// scalars or null if the correspondign entry in scalars is nullptr.
  RecordBatchProjector(MemoryPool* pool, std::shared_ptr<Schema> to,
                       std::vector<std::shared_ptr<Scalar>> scalars)
      : pool_(pool),
        to_(std::move(to)),
        missing_columns_(to_->num_fields(), nullptr),
        column_indices_(to_->num_fields(), kNoMatch),
        scalars_(std::move(scalars)) {
    DCHECK_EQ(scalars_.size(), missing_columns_.size());
  }

  Status Project(const RecordBatch& batch, std::shared_ptr<RecordBatch>* out) {
    if (from_ == nullptr || !batch.schema()->Equals(*from_)) {
      RETURN_NOT_OK(SetInputSchema(batch.schema()));
    }

    if (missing_columns_length_ < batch.num_rows()) {
      RETURN_NOT_OK(ResizeMissingColumns(batch.num_rows()));
    }

    std::vector<std::shared_ptr<Array>> columns(to_->num_fields());

    for (int i = 0; i < to_->num_fields(); ++i) {
      int matching_index = column_indices_[i];
      if (matching_index != kNoMatch) {
        columns[i] = batch.column(matching_index);
        continue;
      }

      columns[i] = missing_columns_[i]->Slice(0, batch.num_rows());
    }

    *out = RecordBatch::Make(to_, batch.num_rows(), std::move(columns));
    return Status::OK();
  }

  const std::shared_ptr<Schema>& schema() const { return to_; }

  Status SetInputSchema(std::shared_ptr<Schema> from) {
    from_ = std::move(from);

    for (int i = 0; i < to_->num_fields(); ++i) {
      const auto& field = to_->field(i);
      int matching_index = from_->GetFieldIndex(field->name());

      if (matching_index != kNoMatch) {
        if (!from_->field(matching_index)->Equals(field)) {
          return Status::TypeError("fields had matching names but were not equivalent ",
                                   from_->field(matching_index)->ToString(), " vs ",
                                   field->ToString());
        }

        // Mark column i as not missing by setting missing_columns_[i] to nullptr
        missing_columns_[i] = nullptr;
      } else {
        // Mark column i as missing by setting missing_columns_[i]
        // to a non-null placeholder.
        RETURN_NOT_OK(
            MakeArrayOfNull(pool_, to_->field(i)->type(), 0, &missing_columns_[i]));
      }

      column_indices_[i] = matching_index;
    }
    return Status::OK();
  }

 private:
  static constexpr int kNoMatch = -1;

  Status ResizeMissingColumns(int64_t new_length) {
    // TODO(bkietz) MakeArrayOfNull could use fewer buffers by reusing a single zeroed
    // buffer for every buffer in every column which is null
    for (int i = 0; i < to_->num_fields(); ++i) {
      if (missing_columns_[i] == nullptr) {
        continue;
      }
      if (scalars_[i] == nullptr) {
        RETURN_NOT_OK(MakeArrayOfNull(pool_, missing_columns_[i]->type(), new_length,
                                      &missing_columns_[i]));
        continue;
      }
      RETURN_NOT_OK(
          MakeArrayFromScalar(pool_, *scalars_[i], new_length, &missing_columns_[i]));
    }
    missing_columns_length_ = new_length;
    return Status::OK();
  }

  MemoryPool* pool_;
  std::shared_ptr<Schema> from_, to_;
  int64_t missing_columns_length_ = 0;
  std::vector<std::shared_ptr<Array>> missing_columns_;
  std::vector<int> column_indices_;
  std::vector<std::shared_ptr<Scalar>> scalars_;
};

constexpr int RecordBatchProjector::kNoMatch;

/// Wraps a RecordBatchIterator and projects each yielded batch using the given projector.
///
/// Note that as with RecordBatchProjector, ProjectedRecordBatchReader is most efficient
/// when projecting record batches with a consistent schema (for example batches from a
/// table), but it can project record batches having any schema.
class ProjectedRecordBatchReader : public RecordBatchReader {
 public:
  static Status Make(MemoryPool* pool, RecordBatchProjector projector,
                     std::unique_ptr<RecordBatchIterator> wrapped,
                     std::unique_ptr<RecordBatchIterator>* out) {
    out->reset(
        new ProjectedRecordBatchReader(pool, std::move(projector), std::move(wrapped)));
    return Status::OK();
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    std::shared_ptr<RecordBatch> rb;
    RETURN_NOT_OK(wrapped_->Next(&rb));
    if (rb == nullptr) {
      *out = nullptr;
      return Status::OK();
    }

    return projector_.Project(*rb, out);
  }

  std::shared_ptr<Schema> schema() const override { return projector_.schema(); }

 private:
  ProjectedRecordBatchReader(MemoryPool* pool, RecordBatchProjector projector,
                             std::unique_ptr<RecordBatchIterator> wrapped)
      : projector_(std::move(projector)), wrapped_(std::move(wrapped)) {}

  RecordBatchProjector projector_;
  std::unique_ptr<RecordBatchIterator> wrapped_;
};

}  // namespace dataset
}  // namespace arrow

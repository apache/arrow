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
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {

constexpr int kNoMatch = -1;

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
  explicit RecordBatchProjector(std::shared_ptr<Schema> to)
      : to_(std::move(to)),
        missing_columns_(to_->num_fields(), nullptr),
        column_indices_(to_->num_fields(), kNoMatch),
        scalars_(to_->num_fields(), nullptr) {}

  /// If the named field is absent from a record batch it will be added to the projected
  /// record batch with all its slots equal to the provided scalar (instead of null).
  Status SetDefaultValue(const std::string& name, std::shared_ptr<Scalar> scalar) {
    auto field_index = to_->GetFieldIndex(name);
    if (field_index == -1) {
      return Status::Invalid("no field named ", name, " in schema ", *to_);
    }

    DCHECK_NE(scalar, nullptr);

    auto field_type = to_->field(field_index)->type();
    if (!field_type->Equals(scalar->type)) {
      return Status::TypeError("field ", name, " is ", *field_type,
                               " but provided scalar is ", *scalar->type);
    }

    scalars_[field_index] = std::move(scalar);
    return Status::OK();
  }

  Result<std::shared_ptr<RecordBatch>> Project(
      const RecordBatch& batch, MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT) {
    if (from_ == nullptr || !batch.schema()->Equals(*from_)) {
      RETURN_NOT_OK(SetInputSchema(batch.schema(), pool));
    }

    if (missing_columns_length_ < batch.num_rows()) {
      RETURN_NOT_OK(ResizeMissingColumns(batch.num_rows(), pool));
    }

    std::vector<std::shared_ptr<Array>> columns(to_->num_fields());

    for (int i = 0; i < to_->num_fields(); ++i) {
      if (column_indices_[i] != kNoMatch) {
        columns[i] = batch.column(column_indices_[i]);
      } else {
        columns[i] = missing_columns_[i]->Slice(0, batch.num_rows());
      }
    }

    return RecordBatch::Make(to_, batch.num_rows(), std::move(columns));
  }

  const std::shared_ptr<Schema>& schema() const { return to_; }

  Status SetInputSchema(std::shared_ptr<Schema> from,
                        MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT) {
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
            MakeArrayOfNull(pool, to_->field(i)->type(), 0, &missing_columns_[i]));
      }

      column_indices_[i] = matching_index;
    }
    return Status::OK();
  }

 private:
  Status ResizeMissingColumns(int64_t new_length, MemoryPool* pool) {
    // TODO(bkietz) MakeArrayOfNull could use fewer buffers by reusing a single zeroed
    // buffer for every buffer in every column which is null
    for (int i = 0; i < to_->num_fields(); ++i) {
      if (missing_columns_[i] == nullptr) {
        continue;
      }
      if (scalars_[i] == nullptr) {
        RETURN_NOT_OK(MakeArrayOfNull(pool, missing_columns_[i]->type(), new_length,
                                      &missing_columns_[i]));
        continue;
      }
      RETURN_NOT_OK(
          MakeArrayFromScalar(pool, *scalars_[i], new_length, &missing_columns_[i]));
    }
    missing_columns_length_ = new_length;
    return Status::OK();
  }

  std::shared_ptr<Schema> from_, to_;
  int64_t missing_columns_length_ = 0;
  std::vector<std::shared_ptr<Array>> missing_columns_;
  std::vector<int> column_indices_;
  std::vector<std::shared_ptr<Scalar>> scalars_;
};

/// \brief GetFragmentsFromSources transforms a vector<DataSource> into a
/// flattened DataFragmentIterator.
static inline DataFragmentIterator GetFragmentsFromSources(
    const DataSourceVector& sources, ScanOptionsPtr options) {
  // Iterator<DataSource>
  auto sources_it = MakeVectorIterator(sources);

  // DataSource -> Iterator<DataFragment>
  auto fn = [options](DataSourcePtr source) -> DataFragmentIterator {
    return source->GetFragments(options);
  };

  // Iterator<Iterator<DataFragment>>
  auto fragments_it = MakeMapIterator(fn, std::move(sources_it));

  // Iterator<DataFragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

inline std::shared_ptr<Schema> SchemaFromColumnNames(
    const std::shared_ptr<Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<Field>> columns;
  for (const auto& name : column_names) {
    columns.push_back(input->GetFieldByName(name));
  }

  return std::make_shared<Schema>(columns);
}

}  // namespace dataset
}  // namespace arrow

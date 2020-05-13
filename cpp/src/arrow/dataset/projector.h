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

// This API is EXPERIMENTAL.

#pragma once

#include <memory>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace dataset {

ARROW_DS_EXPORT Status CheckProjectable(const Schema& from, const Schema& to);

/// \brief Project a RecordBatch to a given schema.
///
/// Projected record batches will reorder columns from input record batches when possible,
/// otherwise the given schema will be satisfied by augmenting with null or constant
/// columns.
///
/// RecordBatchProjector is most efficient when projecting record batches with a
/// consistent schema (for example batches from a table), but it can project record
/// batches having any schema.
class ARROW_DS_EXPORT RecordBatchProjector {
 public:
  static constexpr int kNoMatch = -1;

  /// A column required by the given schema but absent from a record batch will be added
  /// to the projected record batch with all its slots null.
  explicit RecordBatchProjector(std::shared_ptr<Schema> to);

  /// If the indexed field is absent from a record batch it will be added to the projected
  /// record batch with all its slots equal to the provided scalar (instead of null).
  Status SetDefaultValue(FieldRef ref, std::shared_ptr<Scalar> scalar);

  Result<std::shared_ptr<RecordBatch>> Project(const RecordBatch& batch,
                                               MemoryPool* pool = default_memory_pool());

  const std::shared_ptr<Schema>& schema() const { return to_; }

  Status SetInputSchema(std::shared_ptr<Schema> from,
                        MemoryPool* pool = default_memory_pool());

 private:
  Status ResizeMissingColumns(int64_t new_length, MemoryPool* pool);

  std::shared_ptr<Schema> from_, to_;
  int64_t missing_columns_length_ = 0;
  // these vectors are indexed parallel to to_->fields()
  std::vector<std::shared_ptr<Array>> missing_columns_;
  std::vector<int> column_indices_;
  std::vector<std::shared_ptr<Scalar>> scalars_;
};

}  // namespace dataset
}  // namespace arrow

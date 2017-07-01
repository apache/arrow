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

#ifndef ARROW_TABLE_BUILDER_H
#define ARROW_TABLE_BUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ArrayBuilder;
class MemoryPool;
class RecordBatch;
class Schema;

/// \class RecordBatchBuilder
/// \brief Helper class for creating record batches iteratively given a known
/// schema
class RecordBatchBuilder {
 public:
  static Status Create(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
      std::unique_ptr<RecordBatchBuilder>* builder);

  static Status Create(const std::shared_ptr<Schema>& schema, MemoryPool* pool,
      int64_t initial_capacity, std::unique_ptr<RecordBatchBuilder>* builder);

  /// Get base pointer to field builder
  ArrayBuilder* GetField(int i) { return raw_field_builders_[i]; }

  /// Get base pointer to field builder
  const ArrayBuilder* GetField(int i) const { return raw_field_builders_[i]; }

  /// Return field builder casted to indicated specific builder type
  template <typename T>
  T* GetFieldAs(int i) {
    return static_cast<T*>(raw_field_builders_[i]);
  }

  /// Return field builder casted to indicated specific builder type
  template <typename T>
  const T* GetFieldAs(int i) const {
    return static_cast<const T*>(raw_field_builders_[i]);
  }

  /// Finish current batch and reset
  Status FlushAndReset(std::shared_ptr<RecordBatch>* batch);

  /// Flush current batch without resetting
  Status Flush(std::shared_ptr<RecordBatch>* batch);

  /// Set the initial capacity for new builders
  void SetInitialCapacity(int64_t capacity);

  int num_fields() const { return schema_->num_fields(); }
  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  RecordBatchBuilder(
      const std::shared_ptr<Schema>& schema, MemoryPool* pool, int64_t initial_capacity);

  Status CreateBuilders();
  Status InitBuilders();

  std::shared_ptr<Schema> schema_;
  int64_t initial_capacity_;
  MemoryPool* pool_;

  std::vector<std::unique_ptr<ArrayBuilder>> field_builders_;
  std::vector<ArrayBuilder*> raw_field_builders_;
};

}  // namespace arrow

#endif  // ARROW_TABLE_BUILDER_H

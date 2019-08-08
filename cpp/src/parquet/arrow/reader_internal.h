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

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

namespace arrow {

class Array;
class ChunkedArray;
class DataType;
class Field;

}  // namespace arrow

using arrow::Status;

namespace parquet {

class ArrowReaderProperties;

namespace arrow {

class ColumnReaderImpl;

// ----------------------------------------------------------------------
// Iteration utilities

// Abstraction to decouple row group iteration details from the ColumnReader,
// so we can read only a single row group if we want
class FileColumnIterator {
 public:
  explicit FileColumnIterator(int column_index, ParquetFileReader* reader,
                              std::vector<int> row_groups)
      : column_index_(column_index),
        reader_(reader),
        schema_(reader->metadata()->schema()),
        row_groups_(row_groups.begin(), row_groups.end()) {}

  virtual ~FileColumnIterator() {}

  std::unique_ptr<::parquet::PageReader> NextChunk() {
    if (row_groups_.empty()) {
      return nullptr;
    }

    auto row_group_reader = reader_->RowGroup(row_groups_.front());
    row_groups_.pop_front();
    return row_group_reader->GetColumnPageReader(column_index_);
  }

  const SchemaDescriptor* schema() const { return schema_; }

  const ColumnDescriptor* descr() const { return schema_->Column(column_index_); }

  std::shared_ptr<FileMetaData> metadata() const { return reader_->metadata(); }

  int column_index() const { return column_index_; }

 protected:
  int column_index_;
  ParquetFileReader* reader_;
  const SchemaDescriptor* schema_;
  std::deque<int> row_groups_;
};

using FileColumnIteratorFactory =
    std::function<FileColumnIterator*(int, ParquetFileReader*)>;

Status TransferColumnData(::parquet::internal::RecordReader* reader,
                          std::shared_ptr<::arrow::DataType> value_type,
                          const ColumnDescriptor* descr, ::arrow::MemoryPool* pool,
                          std::shared_ptr<::arrow::ChunkedArray>* out);

Status ReconstructNestedList(const std::shared_ptr<::arrow::Array>& arr,
                             std::shared_ptr<::arrow::Field> field, int16_t max_def_level,
                             int16_t max_rep_level, const int16_t* def_levels,
                             const int16_t* rep_levels, int64_t total_levels,
                             ::arrow::MemoryPool* pool,
                             std::shared_ptr<::arrow::Array>* out);

struct ReaderContext {
  ParquetFileReader* reader;
  ::arrow::MemoryPool* pool;
  FileColumnIteratorFactory iterator_factory;
  bool filter_leaves;
  std::unordered_set<int> included_leaves;

  bool IncludesLeaf(int leaf_index) const {
    return (!this->filter_leaves ||
            (included_leaves.find(leaf_index) != included_leaves.end()));
  }
};

struct PARQUET_EXPORT SchemaField {
  std::shared_ptr<::arrow::Field> field;
  std::vector<SchemaField> children;

  // Only set for leaf nodes
  int column_index = -1;

  int16_t max_definition_level;
  int16_t max_repetition_level;

  bool is_leaf() const { return column_index != -1; }

  Status GetReader(const ReaderContext& context,
                   std::unique_ptr<ColumnReaderImpl>* out) const;
};

struct SchemaManifest {
  const SchemaDescriptor* descr;
  std::vector<SchemaField> schema_fields;

  std::unordered_map<int, const SchemaField*> column_index_to_field;
  std::unordered_map<const SchemaField*, const SchemaField*> child_to_parent;

  Status GetColumnField(int column_index, const SchemaField** out) const {
    auto it = column_index_to_field.find(column_index);
    if (it == column_index_to_field.end()) {
      return Status::KeyError("Column index ", column_index,
                              " not found in schema manifest, may be malformed");
    }
    *out = it->second;
    return Status::OK();
  }

  const SchemaField* GetParent(const SchemaField* field) const {
    // Returns nullptr also if not found
    auto it = child_to_parent.find(field);
    if (it == child_to_parent.end()) {
      return nullptr;
    }
    return it->second;
  }

  bool GetFieldIndices(const std::vector<int>& column_indices, std::vector<int>* out) {
    // Coalesce a list of schema fields indices which are the roots of the
    // columns referred by a list of column indices
    const schema::GroupNode* group = descr->group_node();
    std::unordered_set<int> already_added;
    out->clear();
    for (auto& column_idx : column_indices) {
      auto field_node = descr->GetColumnRoot(column_idx);
      auto field_idx = group->FieldIndex(*field_node);
      if (field_idx < 0) {
        return false;
      }
      auto insertion = already_added.insert(field_idx);
      if (insertion.second) {
        out->push_back(field_idx);
      }
    }
    return true;
  }
};

PARQUET_EXPORT
Status BuildSchemaManifest(const SchemaDescriptor* schema,
                           const ArrowReaderProperties& properties,
                           SchemaManifest* manifest);

}  // namespace arrow
}  // namespace parquet

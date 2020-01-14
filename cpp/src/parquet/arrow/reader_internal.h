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

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "parquet/arrow/schema.h"
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
class KeyValueMetadata;
class Schema;

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
  std::shared_ptr<std::unordered_set<int>> included_leaves;

  bool IncludesLeaf(int leaf_index) const {
    const auto& to_search = *included_leaves;
    return (!this->filter_leaves || (to_search.find(leaf_index) != to_search.end()));
  }
};

Status GetReader(const SchemaField& field, const std::shared_ptr<ReaderContext>& context,
                 std::unique_ptr<ColumnReaderImpl>* out);

}  // namespace arrow
}  // namespace parquet

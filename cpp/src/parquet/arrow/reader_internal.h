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
#include <unordered_set>
#include <utility>
#include <vector>

#include "parquet/arrow/schema.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/row_ranges.h"

namespace arrow {

class Array;
class ChunkedArray;
class DataType;
class Field;
class KeyValueMetadata;
class Schema;

}  // namespace arrow

using arrow::Status;

using RowRangesOpt = std::optional<std::unordered_map<int, ::parquet::RowRanges>>;

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

  explicit FileColumnIterator(int column_index, ParquetFileReader* reader,
                              std::vector<int> row_groups, RowRangesOpt row_ranges)
      : column_index_(column_index),
        reader_(reader),
        schema_(reader->metadata()->schema()),
        row_groups_(row_groups.begin(), row_groups.end()),
        row_ranges_(std::move(row_ranges)) {}

  virtual ~FileColumnIterator() {}

  std::unique_ptr<::parquet::PageReader> NextChunk() {
    if (row_groups_.empty()) {
      return nullptr;
    }

    const int row_group_ordinal = row_groups_.front();
    auto row_group_reader = reader_->RowGroup(row_group_ordinal);
    row_groups_.pop_front();
    if (row_ranges_.has_value()) {
      auto row_ranges_map = row_ranges_.value();
      auto it = row_ranges_map.find(row_group_ordinal);
      if (it != row_ranges_map.end()) {
        auto index_reader = reader_->GetPageIndexReader()->RowGroup(row_group_ordinal);
        return row_group_reader->GetColumnPageReader(column_index_, it->second,
                                                     index_reader);
      }
    }

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

  RowRangesOpt row_ranges_;
};

using FileColumnIteratorFactory =
    std::function<FileColumnIterator*(int, ParquetFileReader*)>;

Status TransferColumnData(::parquet::internal::RecordReader* reader,
                          const std::shared_ptr<::arrow::Field>& value_field,
                          const ColumnDescriptor* descr, ::arrow::MemoryPool* pool,
                          std::shared_ptr<::arrow::ChunkedArray>* out);

struct ReaderContext {
  ParquetFileReader* reader;
  ::arrow::MemoryPool* pool;
  FileColumnIteratorFactory iterator_factory;
  bool filter_leaves;
  std::shared_ptr<std::unordered_set<int>> included_leaves;

  bool IncludesLeaf(int leaf_index) const {
    if (this->filter_leaves) {
      return this->included_leaves->find(leaf_index) != this->included_leaves->end();
    }
    return true;
  }
};

}  // namespace arrow
}  // namespace parquet

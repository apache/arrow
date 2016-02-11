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

#ifndef PARQUET_FILE_READER_H
#define PARQUET_FILE_READER_H

#include <cstdint>
#include <memory>
#include <string>
#include <stdio.h>
#include <unordered_map>

#include "parquet/thrift/parquet_types.h"

#include "parquet/types.h"
#include "parquet/schema/descriptor.h"
#include "parquet/util/input.h"

namespace parquet_cpp {

class ColumnReader;
class ParquetFileReader;

class RowGroupReader {
 public:
  RowGroupReader(ParquetFileReader* parent, parquet::RowGroup* group) :
      parent_(parent),
      row_group_(group) {}

  // Construct a ColumnReader for the indicated row group-relative
  // column. Ownership is shared with the RowGroupReader.
  std::shared_ptr<ColumnReader> Column(size_t i);

  const parquet::ColumnMetaData* column_metadata(size_t i) const {
    return &row_group_->columns[i].meta_data;
  }

  size_t num_columns() const {
    return row_group_->columns.size();
  }

 private:
  friend class ParquetFileReader;

  ParquetFileReader* parent_;
  parquet::RowGroup* row_group_;

  // Column index -> ColumnReader
  std::unordered_map<int, std::shared_ptr<ColumnReader> > column_readers_;
};


class ParquetFileReader {
 public:
  ParquetFileReader();
  ~ParquetFileReader();

  // This class does _not_ take ownership of the file. You must manage its
  // lifetime separately
  void Open(RandomAccessSource* buffer);

  void Close();

  void ParseMetaData();

  // The RowGroupReader is owned by the FileReader
  RowGroupReader* RowGroup(size_t i);

  size_t num_row_groups() const {
    return metadata_.row_groups.size();
  }

  const ColumnDescriptor* column_descr(size_t i) const {
    return schema_descr_.Column(i);
  }

  size_t num_columns() const {
    return schema_descr_.num_columns();
  }

  const parquet::FileMetaData& metadata() const {
    return metadata_;
  }

  void DebugPrint(std::ostream& stream, bool print_values = true);

 private:
  friend class RowGroupReader;

  parquet::FileMetaData metadata_;
  SchemaDescriptor schema_descr_;

  bool parsed_metadata_;

  // Row group index -> RowGroupReader
  std::unordered_map<int, std::shared_ptr<RowGroupReader> > row_group_readers_;

  RandomAccessSource* buffer_;
};


} // namespace parquet_cpp

#endif // PARQUET_FILE_READER_H

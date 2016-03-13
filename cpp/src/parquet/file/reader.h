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
#include <iosfwd>
#include <memory>
#include <string>

#include "parquet/column/page.h"
#include "parquet/schema/descriptor.h"

namespace parquet_cpp {

class ColumnReader;

struct RowGroupStatistics {
  int64_t num_values;
  int64_t null_count;
  int64_t distinct_count;
};

class RowGroupReader {
 public:
  // Forward declare the PIMPL
  struct Contents {
    virtual int num_columns() const = 0;
    virtual RowGroupStatistics GetColumnStats(int i) = 0;
    virtual std::unique_ptr<PageReader> GetColumnPageReader(int i) = 0;
  };

  RowGroupReader(const SchemaDescriptor* schema, std::unique_ptr<Contents> contents);

  // Construct a ColumnReader for the indicated row group-relative
  // column. Ownership is shared with the RowGroupReader.
  std::shared_ptr<ColumnReader> Column(int i);
  int num_columns() const;

  RowGroupStatistics GetColumnStats(int i) const;

 private:
  // Owned by the parent ParquetFileReader
  const SchemaDescriptor* schema_;

  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;
};


class ParquetFileReader {
 public:
  // Forward declare the PIMPL
  struct Contents {
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;

    virtual std::shared_ptr<RowGroupReader> GetRowGroup(int i) = 0;

    virtual int64_t num_rows() const = 0;
    virtual int num_columns() const = 0;
    virtual int num_row_groups() const = 0;

    // Return const-poitner to make it clear that this object is not to be copied
    const SchemaDescriptor* schema() const {
      return &schema_;
    }
    SchemaDescriptor schema_;
  };

  ParquetFileReader();
  ~ParquetFileReader();

  // API Convenience to open a serialized Parquet file on disk
  static std::unique_ptr<ParquetFileReader> OpenFile(const std::string& path,
      bool memory_map = true);

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  // The RowGroupReader is owned by the FileReader
  std::shared_ptr<RowGroupReader> RowGroup(int i);

  int num_columns() const;
  int64_t num_rows() const;
  int num_row_groups() const;

  // Returns the file schema descriptor
  const SchemaDescriptor* descr() {
    return schema_;
  }

  const ColumnDescriptor* column_schema(int i) const {
    return schema_->Column(i);
  }

  void DebugPrint(std::ostream& stream, bool print_values = true);

 private:
  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  // The SchemaDescriptor is provided by the Contents impl
  const SchemaDescriptor* schema_;
};


} // namespace parquet_cpp

#endif // PARQUET_FILE_READER_H

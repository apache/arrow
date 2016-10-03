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
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/column/properties.h"
#include "parquet/column/statistics.h"
#include "parquet/file/metadata.h"
#include "parquet/schema/descriptor.h"
#include "parquet/util/visibility.h"

namespace parquet {

class ColumnReader;
class RandomAccessSource;

class PARQUET_EXPORT RowGroupReader {
 public:
  // Forward declare a virtual class 'Contents' to aid dependency injection and more
  // easily create test fixtures
  // An implementation of the Contents class is defined in the .cc file
  struct Contents {
    virtual ~Contents() {}
    virtual std::unique_ptr<PageReader> GetColumnPageReader(int i) = 0;
    virtual const RowGroupMetaData* metadata() const = 0;
    virtual const ReaderProperties* properties() const = 0;
  };

  explicit RowGroupReader(std::unique_ptr<Contents> contents);

  // Returns the rowgroup metadata
  const RowGroupMetaData* metadata() const;

  // Construct a ColumnReader for the indicated row group-relative
  // column. Ownership is shared with the RowGroupReader.
  std::shared_ptr<ColumnReader> Column(int i);

 private:
  // Holds a pointer to an instance of Contents implementation
  std::unique_ptr<Contents> contents_;
};

class PARQUET_EXPORT ParquetFileReader {
 public:
  // Forward declare a virtual class 'Contents' to aid dependency injection and more
  // easily create test fixtures
  // An implementation of the Contents class is defined in the .cc file
  struct Contents {
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;
    virtual std::shared_ptr<RowGroupReader> GetRowGroup(int i) = 0;
    virtual const FileMetaData* metadata() const = 0;
  };

  ParquetFileReader();
  ~ParquetFileReader();

  // API Convenience to open a serialized Parquet file on disk
  static std::unique_ptr<ParquetFileReader> OpenFile(const std::string& path,
      bool memory_map = true, ReaderProperties props = default_reader_properties());

  static std::unique_ptr<ParquetFileReader> Open(
      std::unique_ptr<RandomAccessSource> source,
      ReaderProperties props = default_reader_properties());

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  // The RowGroupReader is owned by the FileReader
  std::shared_ptr<RowGroupReader> RowGroup(int i);

  // Returns the file metadata
  const FileMetaData* metadata() const;

  void DebugPrint(
      std::ostream& stream, std::list<int> selected_columns, bool print_values = true);

 private:
  // Holds a pointer to an instance of Contents implementation
  std::unique_ptr<Contents> contents_;
};

}  // namespace parquet

#endif  // PARQUET_FILE_READER_H

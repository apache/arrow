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
#include <memory>

#include "parquet/platform.h"
#include "parquet/properties.h"

namespace arrow {

class Array;
class ChunkedArray;
class Schema;
class Table;

}  // namespace arrow

namespace parquet {

class FileMetaData;
class ParquetFileWriter;

namespace arrow {

/// \brief Iterative FileWriter class
///
/// Start a new RowGroup or Chunk with NewRowGroup.
/// Write column-by-column the whole column chunk.
///
/// If PARQUET:field_id is present as a metadata key on a field, and the corresponding
/// value is a nonnegative integer, then it will be used as the field_id in the parquet
/// file.
class PARQUET_EXPORT FileWriter {
 public:
  static ::arrow::Status Make(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer,
                              std::shared_ptr<::arrow::Schema> schema,
                              std::shared_ptr<ArrowWriterProperties> arrow_properties,
                              std::unique_ptr<FileWriter>* out);

  static ::arrow::Status Open(const ::arrow::Schema& schema, MemoryPool* pool,
                              std::shared_ptr<::arrow::io::OutputStream> sink,
                              std::shared_ptr<WriterProperties> properties,
                              std::unique_ptr<FileWriter>* writer);

  static ::arrow::Status Open(const ::arrow::Schema& schema, MemoryPool* pool,
                              std::shared_ptr<::arrow::io::OutputStream> sink,
                              std::shared_ptr<WriterProperties> properties,
                              std::shared_ptr<ArrowWriterProperties> arrow_properties,
                              std::unique_ptr<FileWriter>* writer);

  virtual std::shared_ptr<::arrow::Schema> schema() const = 0;

  /// \brief Write a Table to Parquet.
  virtual ::arrow::Status WriteTable(const ::arrow::Table& table, int64_t chunk_size) = 0;

  virtual ::arrow::Status NewRowGroup(int64_t chunk_size) = 0;
  virtual ::arrow::Status WriteColumnChunk(const ::arrow::Array& data) = 0;

  /// \brief Write ColumnChunk in row group using slice of a ChunkedArray
  virtual ::arrow::Status WriteColumnChunk(
      const std::shared_ptr<::arrow::ChunkedArray>& data, int64_t offset,
      int64_t size) = 0;

  virtual ::arrow::Status WriteColumnChunk(
      const std::shared_ptr<::arrow::ChunkedArray>& data) = 0;
  virtual ::arrow::Status Close() = 0;
  virtual ~FileWriter();

  virtual MemoryPool* memory_pool() const = 0;
  virtual const std::shared_ptr<FileMetaData> metadata() const = 0;
};

/// \brief Write Parquet file metadata only to indicated Arrow OutputStream
PARQUET_EXPORT
::arrow::Status WriteFileMetaData(const FileMetaData& file_metadata,
                                  ::arrow::io::OutputStream* sink);

/// \brief Write metadata-only Parquet file to indicated Arrow OutputStream
PARQUET_EXPORT
::arrow::Status WriteMetaDataFile(const FileMetaData& file_metadata,
                                  ::arrow::io::OutputStream* sink);

/// \brief Write a Table to Parquet.
::arrow::Status PARQUET_EXPORT
WriteTable(const ::arrow::Table& table, MemoryPool* pool,
           std::shared_ptr<::arrow::io::OutputStream> sink, int64_t chunk_size,
           std::shared_ptr<WriterProperties> properties = default_writer_properties(),
           std::shared_ptr<ArrowWriterProperties> arrow_properties =
               default_arrow_writer_properties());

}  // namespace arrow
}  // namespace parquet

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

#ifndef ARROW_PARQUET_CONVERTER_H
#define ARROW_PARQUET_CONVERTER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"
#include "parquet/properties.h"

namespace jni {

namespace parquet {

namespace adapters {

using MemoryPool = arrow::MemoryPool;
using OutputStream = arrow::io::OutputStream;
using RandomAccessFile = arrow::io::RandomAccessFile;
using RecordBatch = arrow::RecordBatch;
using Schema = arrow::Schema;
using Status = arrow::Status;

/// \class ParquetFileReader
/// \brief Read an Arrow RecordBatch from an PARQUET file.
class ARROW_EXPORT ParquetFileReader {
 public:
  ~ParquetFileReader();

  /// \brief Creates a new PARQUET reader.
  ///
  /// \param[in] file the data source
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[in] properties ArrowReaderProperties
  /// \param[out] reader the returned reader object
  /// \return Status
  static Status Open(std::shared_ptr<RandomAccessFile>& file, MemoryPool* pool,
                     ::parquet::ArrowReaderProperties properties,
                     std::unique_ptr<ParquetFileReader>* reader);

  /// \brief Creates a new PARQUET reader.
  ///
  /// \param[in] file the data source
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[out] reader the returned reader object
  /// \return Status
  static Status Open(std::shared_ptr<RandomAccessFile>& file, MemoryPool* pool,
                     std::unique_ptr<ParquetFileReader>* reader);

  /// \brief Get a record batch iterator with specified row group index and
  //          column indices.
  ///
  /// \param[in] column_indices indexes of columns expected to be read
  /// \param[in] row_group_indices indexes of row_groups expected to be read
  Status InitRecordBatchReader(const std::vector<int>& column_indices,
                               const std::vector<int>& row_group_indices);

  /// \brief Get a record batch iterator with a start and end pos to indicate
  //          row groupand column indices.
  ///
  /// \param[in] column_indices indexes of columns expected to be read
  /// \param[in] start_pos start position of row_groups expected to be read
  /// \param[in] end_pos end position of row_groups expected to be read
  Status InitRecordBatchReader(const std::vector<int>& column_indices, int64_t start_pos,
                               int64_t end_pos);

  /// \brief Return the schema read from the PARQUET file
  ///
  /// \param[out] out the returned Schema object
  Status ReadSchema(std::shared_ptr<Schema>* out);

  /// \brief Read a RecordBatch
  ///
  /// \param[out] out the returned RecordBatch
  Status ReadNext(std::shared_ptr<RecordBatch>* out);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ParquetFileReader();
};

/// \class ParquetFileWriter
/// \brief Write an Arrow RecordBatch to an PARQUET file.
class ARROW_EXPORT ParquetFileWriter {
 public:
  ~ParquetFileWriter();

  /// \brief Creates a new PARQUET writer.
  ///
  /// \param[in] output_stream output stream
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[in] schema Arrow schema for to be written data
  /// \param[out] writer the returned writer object
  /// \return Status
  static Status Open(std::shared_ptr<OutputStream>& output_stream, MemoryPool* pool,
                     std::shared_ptr<Schema> schema,
                     std::unique_ptr<ParquetFileWriter>* writer);

  /// \brief Creates a new PARQUET writer.
  ///
  /// \param[in] output_stream output stream
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[in] schema Arrow schema for to be written data
  /// \param[in] properties ArrowWriterProperties
  /// \param[out] writer the returned writer object
  /// \return Status
  static Status Open(std::shared_ptr<OutputStream>& output_stream, MemoryPool* pool,
                     std::shared_ptr<Schema> schema,
                     std::shared_ptr<::parquet::ArrowWriterProperties> properties,
                     std::unique_ptr<ParquetFileWriter>* writer);

  /// \brief write a RecordBatch to buffer
  ///
  /// \param[in] in record batch data to be written
  Status WriteNext(std::shared_ptr<RecordBatch> in);

  /// \brief flush all record batch in buffer to parquet output stream
  Status Flush();

  /// \brief Return the schema read from the PARQUET file
  ///
  /// \param[out] out the returned Schema object
  Status GetSchema(std::shared_ptr<Schema>* out);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ParquetFileWriter();
};
}  // namespace adapters

}  // namespace parquet

}  // namespace jni

#endif  // ARROW_PARQUET_CONVERTER_H

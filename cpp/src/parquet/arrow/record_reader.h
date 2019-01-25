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

#ifndef PARQUET_RECORD_READER_H
#define PARQUET_RECORD_READER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/memory_pool.h"

#include "parquet/util/memory.h"

namespace arrow {

class Array;

}  // namespace arrow

namespace parquet {

class ColumnDescriptor;
class PageReader;

namespace internal {

/// \brief Stateful column reader that delimits semantic records for both flat
/// and nested columns
///
/// \note API EXPERIMENTAL
/// \since 1.3.0
class RecordReader {
 public:
  // So that we can create subclasses
  class RecordReaderImpl;

  static std::shared_ptr<RecordReader> Make(
      const ColumnDescriptor* descr,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  virtual ~RecordReader();

  /// \brief Decoded definition levels
  const int16_t* def_levels() const;

  /// \brief Decoded repetition levels
  const int16_t* rep_levels() const;

  /// \brief Decoded values, including nulls, if any
  const uint8_t* values() const;

  /// \brief Attempt to read indicated number of records from column chunk
  /// \return number of records read
  int64_t ReadRecords(int64_t num_records);

  /// \brief Pre-allocate space for data. Results in better flat read performance
  void Reserve(int64_t num_values);

  /// \brief Clear consumed values and repetition/definition levels as the
  /// result of calling ReadRecords
  void Reset();

  std::shared_ptr<ResizableBuffer> ReleaseValues();
  std::shared_ptr<ResizableBuffer> ReleaseIsValid();

  /// \brief Number of values written including nulls (if any)
  int64_t values_written() const;

  /// \brief Number of definition / repetition levels (from those that have
  /// been decoded) that have been consumed inside the reader.
  int64_t levels_position() const;

  /// \brief Number of definition / repetition levels that have been written
  /// internally in the reader
  int64_t levels_written() const;

  /// \brief Number of nulls in the leaf
  int64_t null_count() const;

  /// \brief True if the leaf values are nullable
  bool nullable_values() const;

  /// \brief Return true if the record reader has more internal data yet to
  /// process
  bool HasMoreData() const;

  /// \brief Advance record reader to the next row group
  /// \param[in] reader obtained from RowGroupReader::GetColumnPageReader
  void SetPageReader(std::unique_ptr<PageReader> reader);

  void DebugPrintState();

  // For BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY types that may have chunked output
  std::vector<std::shared_ptr<::arrow::Array>> GetBuilderChunks();

 private:
  std::unique_ptr<RecordReaderImpl> impl_;
  explicit RecordReader(RecordReaderImpl* impl);
};

}  // namespace internal
}  // namespace parquet

#endif  // PARQUET_RECORD_READER_H

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

#ifndef PARQUET_ARROW_WRITER_H
#define PARQUET_ARROW_WRITER_H

#include <cstdint>
#include <cstring>
#include <memory>

#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/types.h"

#include "arrow/type.h"

namespace arrow {

class Array;
class ChunkedArray;
class Table;

}  // namespace arrow

namespace parquet {

class FileMetaData;
class ParquetFileWriter;

namespace arrow {

/**
 * Iterative API:
 *  Start a new RowGroup/Chunk with NewRowGroup
 *  Write column-by-column the whole column chunk
 */
class PARQUET_EXPORT FileWriter {
 public:
  static ::arrow::Status Make(
      ::arrow::MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer,
      const std::shared_ptr<::arrow::Schema>& schema,
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
      std::unique_ptr<FileWriter>* out);

  static ::arrow::Status Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                              const std::shared_ptr<::arrow::io::OutputStream>& sink,
                              const std::shared_ptr<WriterProperties>& properties,
                              std::unique_ptr<FileWriter>* writer);

  static ::arrow::Status Open(
      const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
      const std::shared_ptr<::arrow::io::OutputStream>& sink,
      const std::shared_ptr<WriterProperties>& properties,
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
      std::unique_ptr<FileWriter>* writer);

  /// \brief Write a Table to Parquet.
  virtual ::arrow::Status WriteTable(const ::arrow::Table& table, int64_t chunk_size) = 0;

  virtual ::arrow::Status NewRowGroup(int64_t chunk_size) = 0;
  virtual ::arrow::Status WriteColumnChunk(const ::arrow::Array& data) = 0;

  /// \brief Write ColumnChunk in row group using slice of a ChunkedArray
  virtual ::arrow::Status WriteColumnChunk(const std::shared_ptr<::arrow::ChunkedArray>& data,
                                           const int64_t offset, const int64_t size) = 0;

  virtual ::arrow::Status WriteColumnChunk(
      const std::shared_ptr<::arrow::ChunkedArray>& data) = 0;
  virtual ::arrow::Status Close() = 0;
  virtual ~FileWriter();

  virtual ::arrow::MemoryPool* memory_pool() const = 0;
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

/**
 * Write a Table to Parquet.
 *
 * The table shall only consist of columns of primitive type or of primitive lists.
 */
::arrow::Status PARQUET_EXPORT WriteTable(
    const ::arrow::Table& table, ::arrow::MemoryPool* pool,
    const std::shared_ptr<::arrow::io::OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
    const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
        default_arrow_writer_properties());

namespace internal {

/**
 * Timestamp conversion constants
 */
constexpr int64_t kJulianEpochOffsetDays = INT64_C(2440588);

template <int64_t UnitPerDay, int64_t NanosecondsPerUnit>
inline void ArrowTimestampToImpalaTimestamp(const int64_t time, Int96* impala_timestamp) {
  int64_t julian_days = (time / UnitPerDay) + kJulianEpochOffsetDays;
  (*impala_timestamp).value[2] = (uint32_t)julian_days;

  int64_t last_day_units = time % UnitPerDay;
  auto last_day_nanos = last_day_units * NanosecondsPerUnit;
  // impala_timestamp will be unaligned every other entry so do memcpy instead
  // of assign and reinterpret cast to avoid undefined behavior.
  std::memcpy(impala_timestamp, &last_day_nanos, sizeof(int64_t));
}

constexpr int64_t kSecondsInNanos = INT64_C(1000000000);

inline void SecondsToImpalaTimestamp(const int64_t seconds, Int96* impala_timestamp) {
  ArrowTimestampToImpalaTimestamp<kSecondsPerDay, kSecondsInNanos>(seconds,
                                                                   impala_timestamp);
}

constexpr int64_t kMillisecondsInNanos = kSecondsInNanos / INT64_C(1000);

inline void MillisecondsToImpalaTimestamp(const int64_t milliseconds,
                                          Int96* impala_timestamp) {
  ArrowTimestampToImpalaTimestamp<kMillisecondsPerDay, kMillisecondsInNanos>(
      milliseconds, impala_timestamp);
}

constexpr int64_t kMicrosecondsInNanos = kMillisecondsInNanos / INT64_C(1000);

inline void MicrosecondsToImpalaTimestamp(const int64_t microseconds,
                                          Int96* impala_timestamp) {
  ArrowTimestampToImpalaTimestamp<kMicrosecondsPerDay, kMicrosecondsInNanos>(
      microseconds, impala_timestamp);
}

constexpr int64_t kNanosecondsInNanos = INT64_C(1);

inline void NanosecondsToImpalaTimestamp(const int64_t nanoseconds,
                                         Int96* impala_timestamp) {
  ArrowTimestampToImpalaTimestamp<kNanosecondsPerDay, kNanosecondsInNanos>(
      nanoseconds, impala_timestamp);
}

}  // namespace internal

}  // namespace arrow

}  // namespace parquet

#endif  // PARQUET_ARROW_WRITER_H

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
#include <memory>

#include "parquet/properties.h"
#include "parquet/types.h"
#include "parquet/util/visibility.h"

#include "arrow/type.h"

namespace arrow {

class Array;
class ChunkedArray;
class MemoryPool;
class Status;
class Table;

namespace io {

class OutputStream;

}  // namespace io

}  // namespace arrow

namespace parquet {

class FileMetaData;
class OutputStream;
class ParquetFileWriter;

namespace arrow {

class PARQUET_EXPORT ArrowWriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : write_timestamps_as_int96_(false),
          coerce_timestamps_enabled_(false),
          coerce_timestamps_unit_(::arrow::TimeUnit::SECOND),
          truncated_timestamps_allowed_(false) {}
    virtual ~Builder() {}

    Builder* disable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = false;
      return this;
    }

    Builder* enable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = true;
      return this;
    }

    Builder* coerce_timestamps(::arrow::TimeUnit::type unit) {
      coerce_timestamps_enabled_ = true;
      coerce_timestamps_unit_ = unit;
      return this;
    }

    Builder* allow_truncated_timestamps() {
      truncated_timestamps_allowed_ = true;
      return this;
    }

    Builder* disallow_truncated_timestamps() {
      truncated_timestamps_allowed_ = false;
      return this;
    }

    std::shared_ptr<ArrowWriterProperties> build() {
      return std::shared_ptr<ArrowWriterProperties>(new ArrowWriterProperties(
          write_timestamps_as_int96_, coerce_timestamps_enabled_, coerce_timestamps_unit_,
          truncated_timestamps_allowed_));
    }

   private:
    bool write_timestamps_as_int96_;

    bool coerce_timestamps_enabled_;
    ::arrow::TimeUnit::type coerce_timestamps_unit_;
    bool truncated_timestamps_allowed_;
  };

  bool support_deprecated_int96_timestamps() const { return write_timestamps_as_int96_; }

  bool coerce_timestamps_enabled() const { return coerce_timestamps_enabled_; }
  ::arrow::TimeUnit::type coerce_timestamps_unit() const {
    return coerce_timestamps_unit_;
  }

  bool truncated_timestamps_allowed() const { return truncated_timestamps_allowed_; }

 private:
  explicit ArrowWriterProperties(bool write_nanos_as_int96,
                                 bool coerce_timestamps_enabled,
                                 ::arrow::TimeUnit::type coerce_timestamps_unit,
                                 bool truncated_timestamps_allowed)
      : write_timestamps_as_int96_(write_nanos_as_int96),
        coerce_timestamps_enabled_(coerce_timestamps_enabled),
        coerce_timestamps_unit_(coerce_timestamps_unit),
        truncated_timestamps_allowed_(truncated_timestamps_allowed) {}

  const bool write_timestamps_as_int96_;
  const bool coerce_timestamps_enabled_;
  const ::arrow::TimeUnit::type coerce_timestamps_unit_;
  const bool truncated_timestamps_allowed_;
};

std::shared_ptr<ArrowWriterProperties> PARQUET_EXPORT default_arrow_writer_properties();

/**
 * Iterative API:
 *  Start a new RowGroup/Chunk with NewRowGroup
 *  Write column-by-column the whole column chunk
 */
class PARQUET_EXPORT FileWriter {
 public:
  FileWriter(::arrow::MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer,
             const std::shared_ptr<::arrow::Schema>& schema,
             const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
                 default_arrow_writer_properties());

  static ::arrow::Status Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                              const std::shared_ptr<OutputStream>& sink,
                              const std::shared_ptr<WriterProperties>& properties,
                              std::unique_ptr<FileWriter>* writer);

  static ::arrow::Status Open(
      const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
      const std::shared_ptr<OutputStream>& sink,
      const std::shared_ptr<WriterProperties>& properties,
      const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
      std::unique_ptr<FileWriter>* writer);

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
  ::arrow::Status WriteTable(const ::arrow::Table& table, int64_t chunk_size);

  ::arrow::Status NewRowGroup(int64_t chunk_size);
  ::arrow::Status WriteColumnChunk(const ::arrow::Array& data);

  /// \brief Write ColumnChunk in row group using slice of a ChunkedArray
  ::arrow::Status WriteColumnChunk(const std::shared_ptr<::arrow::ChunkedArray>& data,
                                   const int64_t offset, const int64_t size);
  ::arrow::Status WriteColumnChunk(const std::shared_ptr<::arrow::ChunkedArray>& data);
  ::arrow::Status Close();

  virtual ~FileWriter();

  ::arrow::MemoryPool* memory_pool() const;

 private:
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
  std::shared_ptr<::arrow::Schema> schema_;
};

/// \brief Write Parquet file metadata only to indicated OutputStream
PARQUET_EXPORT
::arrow::Status WriteFileMetaData(const FileMetaData& file_metadata, OutputStream* sink);

/// \brief Write Parquet file metadata only to indicated Arrow OutputStream
PARQUET_EXPORT
::arrow::Status WriteFileMetaData(const FileMetaData& file_metadata,
                                  const std::shared_ptr<::arrow::io::OutputStream>& sink);

/**
 * Write a Table to Parquet.
 *
 * The table shall only consist of columns of primitive type or of primitive lists.
 */
::arrow::Status PARQUET_EXPORT WriteTable(
    const ::arrow::Table& table, ::arrow::MemoryPool* pool,
    const std::shared_ptr<OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
    const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
        default_arrow_writer_properties());

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
  int64_t* impala_last_day_nanos = reinterpret_cast<int64_t*>(impala_timestamp);
  *impala_last_day_nanos = last_day_units * NanosecondsPerUnit;
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

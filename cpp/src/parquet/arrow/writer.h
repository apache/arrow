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

#include <memory>

#include "parquet/api/schema.h"
#include "parquet/api/writer.h"

#include "arrow/io/interfaces.h"

namespace arrow {

class Array;
class MemoryPool;
class PrimitiveArray;
class RowBatch;
class Schema;
class Status;
class StringArray;
class Table;
}  // namespace arrow

namespace parquet {
namespace arrow {

class PARQUET_EXPORT ArrowWriterProperties {
 public:
  class Builder {
   public:
    Builder() : write_nanos_as_int96_(false) {}
    virtual ~Builder() {}

    Builder* disable_deprecated_int96_timestamps() {
      write_nanos_as_int96_ = false;
      return this;
    }

    Builder* enable_deprecated_int96_timestamps() {
      write_nanos_as_int96_ = true;
      return this;
    }

    std::shared_ptr<ArrowWriterProperties> build() {
      return std::shared_ptr<ArrowWriterProperties>(
          new ArrowWriterProperties(write_nanos_as_int96_));
    }

   private:
    bool write_nanos_as_int96_;
  };

  bool support_deprecated_int96_timestamps() const { return write_nanos_as_int96_; }

 private:
  explicit ArrowWriterProperties(bool write_nanos_as_int96)
      : write_nanos_as_int96_(write_nanos_as_int96) {}

  const bool write_nanos_as_int96_;
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

  /**
   * Write a Table to Parquet.
   *
   * The table shall only consist of columns of primitive type or of primitive lists.
   */
  ::arrow::Status WriteTable(const ::arrow::Table& table, int64_t chunk_size);

  ::arrow::Status NewRowGroup(int64_t chunk_size);
  ::arrow::Status WriteColumnChunk(const ::arrow::Array& data);
  ::arrow::Status Close();

  virtual ~FileWriter();

  ::arrow::MemoryPool* memory_pool() const;

 private:
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

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
constexpr int64_t kNanosecondsPerDay = INT64_C(86400000000000);

/**
 * Converts nanosecond timestamps to Impala (Int96) format
 */
inline void NanosecondsToImpalaTimestamp(const int64_t nanoseconds,
                                         Int96* impala_timestamp) {
  int64_t julian_days = (nanoseconds / kNanosecondsPerDay) + kJulianEpochOffsetDays;
  (*impala_timestamp).value[2] = (uint32_t)julian_days;

  int64_t last_day_nanos = nanoseconds % kNanosecondsPerDay;
  int64_t* impala_last_day_nanos = reinterpret_cast<int64_t*>(impala_timestamp);
  *impala_last_day_nanos = last_day_nanos;
}

}  // namespace internal

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_WRITER_H

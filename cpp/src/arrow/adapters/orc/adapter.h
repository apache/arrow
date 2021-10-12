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
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace adapters {
namespace orc {

/// \class ORCFileReader
/// \brief Read an Arrow Table or RecordBatch from an ORC file.
class ARROW_EXPORT ORCFileReader {
 public:
  ~ORCFileReader();

  /// \brief Creates a new ORC reader.
  ///
  /// \param[in] file the data source
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[out] reader the returned reader object
  /// \return Status
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool,
                     std::unique_ptr<ORCFileReader>* reader);

  /// \brief Creates a new ORC reader
  ///
  /// \param[in] file the data source
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \return the returned reader object
  static Result<std::unique_ptr<ORCFileReader>> Open(
      const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool);

  /// \brief Return the metadata read from the ORC file
  ///
  /// \return A KeyValueMetadata object containing the ORC metadata
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata();

  /// \brief Return the schema read from the ORC file
  ///
  /// \param[out] out the returned Schema object
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status ReadSchema(std::shared_ptr<Schema>* out);

  /// \brief Return the schema read from the ORC file
  ///
  /// \return the returned Schema object
  Result<std::shared_ptr<Schema>> ReadSchema();

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[out] out the returned Table
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status Read(std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \return the returned Table
  Result<std::shared_ptr<Table>> Read();

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \param[out] out the returned Table
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status Read(const std::shared_ptr<Schema>& schema, std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \return the returned Table
  Result<std::shared_ptr<Table>> Read(const std::shared_ptr<Schema>& schema);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned Table
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status Read(const std::vector<int>& include_indices, std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \return the returned Table
  Result<std::shared_ptr<Table>> Read(const std::vector<int>& include_indices);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] include_names the selected field names to read
  /// \return the returned Table
  Result<std::shared_ptr<Table>> Read(const std::vector<std::string>& include_names);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned Table
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status Read(const std::shared_ptr<Schema>& schema,
              const std::vector<int>& include_indices, std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \param[in] include_indices the selected field indices to read
  /// \return the returned Table
  Result<std::shared_ptr<Table>> Read(const std::shared_ptr<Schema>& schema,
                                      const std::vector<int>& include_indices);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[out] out the returned RecordBatch
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \return the returned RecordBatch
  Result<std::shared_ptr<RecordBatch>> ReadStripe(int64_t stripe);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned RecordBatch
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status ReadStripe(int64_t stripe, const std::vector<int>& include_indices,
                    std::shared_ptr<RecordBatch>* out);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[in] include_indices the selected field indices to read
  /// \return the returned RecordBatch
  Result<std::shared_ptr<RecordBatch>> ReadStripe(
      int64_t stripe, const std::vector<int>& include_indices);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[in] include_names the selected field names to read
  /// \return the returned RecordBatch
  Result<std::shared_ptr<RecordBatch>> ReadStripe(
      int64_t stripe, const std::vector<std::string>& include_names);

  /// \brief Seek to designated row. Invoke NextStripeReader() after seek
  ///        will return stripe reader starting from designated row.
  ///
  /// \param[in] row_number the rows number to seek
  Status Seek(int64_t row_number);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size the number of rows each record batch contains in
  ///            record batch iteration.
  /// \param[out] out the returned stripe reader
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status NextStripeReader(int64_t batch_size, std::shared_ptr<RecordBatchReader>* out);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size the number of rows each record batch contains in
  ///            record batch iteration.
  /// \return the returned stripe reader
  Result<std::shared_ptr<RecordBatchReader>> NextStripeReader(int64_t batch_size);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size Get a stripe level record batch iterator with specified row
  /// count in each record batch.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned stripe reader
  ARROW_DEPRECATED("Deprecated in 6.0.0. Use Result-returning overload instead.")
  Status NextStripeReader(int64_t batch_size, const std::vector<int>& include_indices,
                          std::shared_ptr<RecordBatchReader>* out);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size Get a stripe level record batch iterator with specified row
  /// count in each record batch.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \return the returned stripe reader
  Result<std::shared_ptr<RecordBatchReader>> NextStripeReader(
      int64_t batch_size, const std::vector<int>& include_indices);

  /// \brief The number of stripes in the file
  int64_t NumberOfStripes();

  /// \brief The number of rows in the file
  int64_t NumberOfRows();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ORCFileReader();
};

/// \class ORCFileWriter
/// \brief Write an Arrow Table or RecordBatch to an ORC file.
class ARROW_EXPORT ORCFileWriter {
 public:
  ~ORCFileWriter();
  /// \brief Creates a new ORC writer.
  ///
  /// \param[in] output_stream a pointer to the io::OutputStream to write into
  /// \return the returned writer object
  static Result<std::unique_ptr<ORCFileWriter>> Open(io::OutputStream* output_stream);

  /// \brief Write a table
  ///
  /// \param[in] table the Arrow table from which data is extracted
  /// \return Status
  Status Write(const Table& table);

  /// \brief Close an ORC writer (orc::Writer)
  ///
  /// \return Status
  Status Close();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

 private:
  ORCFileWriter();
};

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

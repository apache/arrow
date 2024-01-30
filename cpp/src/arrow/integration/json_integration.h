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

// Implement Arrow JSON serialization format for integration tests

#pragma once

#include <memory>
#include <string>

#include "arrow/io/type_fwd.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow::internal::integration {

/// \class IntegrationJsonWriter
/// \brief Write the JSON representation of an Arrow record batch file or stream
///
/// This is used for integration testing
class ARROW_EXPORT IntegrationJsonWriter {
 public:
  ~IntegrationJsonWriter();

  /// \brief Create a new JSON writer that writes to memory
  ///
  /// \param[in] schema the schema of record batches
  /// \return the creator writer object
  static Result<std::unique_ptr<IntegrationJsonWriter>> Open(
      const std::shared_ptr<Schema>& schema);

  /// \brief Append a record batch
  Status WriteRecordBatch(const RecordBatch& batch);

  /// \brief Finish the JSON payload and return as a std::string
  ///
  /// \return the JSON payload as a string
  Result<std::string> Finish();

 private:
  explicit IntegrationJsonWriter(const std::shared_ptr<Schema>& schema);

  // Hide RapidJSON details from public API
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \class IntegrationJsonReader
/// \brief Read the JSON representation of an Arrow record batch file or stream
///
/// This is used for integration testing
class ARROW_EXPORT IntegrationJsonReader {
 public:
  ~IntegrationJsonReader();

  /// \brief Create a new JSON reader
  ///
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[in] data a Buffer containing the JSON data
  /// \return the created JSON reader
  static Result<std::unique_ptr<IntegrationJsonReader>> Open(
      MemoryPool* pool, std::shared_ptr<Buffer> data);

  /// \brief Create a new JSON reader that uses the default memory pool
  ///
  /// \param[in] data a Buffer containing the JSON data
  /// \return the created JSON reader
  static Result<std::unique_ptr<IntegrationJsonReader>> Open(
      std::shared_ptr<Buffer> data);

  /// \brief Create a new JSON reader from a file
  ///
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[in] in_file a ReadableFile containing JSON data
  /// \return the created JSON reader
  static Result<std::unique_ptr<IntegrationJsonReader>> Open(
      MemoryPool* pool, const std::shared_ptr<io::ReadableFile>& in_file);

  /// \brief Return the schema read from the JSON
  std::shared_ptr<Schema> schema() const;

  /// \brief Return the number of record batches
  int num_record_batches() const;

  /// \brief Read a particular record batch from the file
  ///
  /// \param[in] i the record batch index, does not boundscheck
  /// \return the record batch read
  Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(int i) const;

 private:
  IntegrationJsonReader(MemoryPool* pool, std::shared_ptr<Buffer> data);

  // Hide RapidJSON details from public API
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace arrow::internal::integration

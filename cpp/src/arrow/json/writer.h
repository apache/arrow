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

#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/ipc/type_fwd.h"
#include "arrow/json/options.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"

namespace arrow {
namespace json {

// Functionality for converting Arrow data to JSON text.
// This library supports all primitive types that can be converted to JSON values.
// Each row in a RecordBatch or Table is converted to a JSON object with field names
// as keys and values as JSON values. Null values are omitted by default.

/// \defgroup json-write-functions High-level functions for writing JSON files
/// @{

/// \brief Convert table to JSON and write the result to output.
/// Experimental
ARROW_EXPORT Status WriteJSON(const Table& table, const WriteOptions& options,
                              arrow::io::OutputStream* output);
/// \brief Convert batch to JSON and write the result to output.
/// Experimental
ARROW_EXPORT Status WriteJSON(const RecordBatch& batch, const WriteOptions& options,
                              arrow::io::OutputStream* output);
/// \brief Convert batches read through a RecordBatchReader
/// to JSON and write the results to output.
/// Experimental
ARROW_EXPORT Status WriteJSON(const std::shared_ptr<RecordBatchReader>& reader,
                              const WriteOptions& options,
                              arrow::io::OutputStream* output);

/// @}

/// \defgroup json-writer-factories Functions for creating an incremental JSON writer
/// @{

/// \brief Create a new JSON writer. User is responsible for closing the
/// actual OutputStream.
///
/// \param[in] sink output stream to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization
/// \return Result<std::shared_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeJSONWriter(
    std::shared_ptr<io::OutputStream> sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options = WriteOptions::Defaults());

/// \brief Create a new JSON writer.
///
/// \param[in] sink output stream to write to (does not take ownership)
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization
/// \return Result<std::shared_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeJSONWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options = WriteOptions::Defaults());

/// @}

}  // namespace json
}  // namespace arrow

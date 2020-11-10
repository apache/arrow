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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace dataset {

/// \brief Convert a 64-bit integer to a buffer.
ARROW_DS_EXPORT Status int64_to_char(char* buffer, int64_t num);

/// \brief Convert a buffer to 64-bit integer.
ARROW_DS_EXPORT Status char_to_int64(char* buffer, int64_t& num);

/// \brief Serialize Expression and Schema to a bufferlist.
///
/// \param[in] filter the filter Expression to apply on a RadosDataset.
/// \param[in] schema the Schema of the projection to apply on a RadosDataset.
/// \param[in] bl a bufferlist to write the sequence of bytes to
/// comprising of the serialized Expression and Schema.
ARROW_DS_EXPORT Status serialize_scan_request_to_bufferlist(
    std::shared_ptr<Expression> filter, std::shared_ptr<Schema> schema,
    librados::bufferlist& bl);

/// \brief Deserialize Expression and Schema from a bufferlist.
///
/// \param[in] filter a pointer to write the deserialized Filter data.
/// \param[in] schema a pointer to write the deserialized projection Schema.
/// \param[in] bl a bufferlist to read the sequence of bytes comprising of the
/// serialized Schema and Expression.
ARROW_DS_EXPORT Status deserialize_scan_request_from_bufferlist(
    std::shared_ptr<Expression>* filter, std::shared_ptr<Schema>* schema,
    librados::bufferlist& bl);

/// \brief Serialize a Table to a bufferlist.
///
/// \param[in] table the Table to serialize to a bufferlist.
/// \param[in] bl the bufferlist to write the Table to.
ARROW_DS_EXPORT Status serialize_table_to_bufferlist(std::shared_ptr<Table>& table,
                                                     librados::bufferlist& bl);

/// \brief Deserialize the Table from a bufferlist.
///
/// \param[in] table a pointer to write the deserialized Table.
/// \param[in] bl the bufferlist to read the Table from.
ARROW_DS_EXPORT Status deserialize_table_from_bufferlist(std::shared_ptr<Table>* table,
                                                         librados::bufferlist& bl);

/// \brief Takes a vector of RecordBatches, wrap them with an InMemoryFragment
/// and applies scan operations on them.
///
/// \param[in] filter the filter Expression to apply to the fragment .
/// \param[in] schema the schema of the projection to apply.
/// \param[in] batches the RecordBatches to scan.
/// \param[in] table the resultant table after the scan.
ARROW_DS_EXPORT Status scan_batches(std::shared_ptr<Expression>& filter,
                                    std::shared_ptr<Schema>& schema,
                                    RecordBatchVector& batches,
                                    std::shared_ptr<Table>* table);

/// \brief Read RecordBatches from a bufferlist.
///
/// \param[in] batches a pointer to a vector to store the read RecordBatches.
/// \param[in] bl the bufferlist to read the Record Batch vector from.
ARROW_DS_EXPORT Status extract_batches_from_bufferlist(RecordBatchVector* batches,
                                                       librados::bufferlist& bl);
}  // namespace dataset
}  // namespace arrow

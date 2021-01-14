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
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

namespace arrow {
namespace dataset {

/// \brief Convert a 64-bit integer to a buffer.
ARROW_DS_EXPORT Status int64_to_char(char* buffer, int64_t num);

/// \brief Convert a buffer to 64-bit integer.
ARROW_DS_EXPORT Status char_to_int64(char* buffer, int64_t& num);

/// \brief Serialize Expression(s) and Schema to a bufferlist.
ARROW_DS_EXPORT Status SerializeScanRequestToBufferlist(
    std::shared_ptr<Expression> filter, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> schema, int64_t format, librados::bufferlist& bl);

/// \brief Deserialize Expression(s) and Schema from a bufferlist.
ARROW_DS_EXPORT Status DeserializeScanRequestFromBufferlist(
    std::shared_ptr<Expression>* filter, std::shared_ptr<Expression>* part_expr,
    std::shared_ptr<Schema>* schema, int64_t* format, librados::bufferlist& bl);

/// \brief Serialize a Table to an Arrow IPC binary buffer.
ARROW_DS_EXPORT Status SerializeTableToIPCStream(std::shared_ptr<Table>& table,
                                                 librados::bufferlist& bl);

/// \brief Serialize a Table to an Parquet binary buffer.
ARROW_DS_EXPORT Status SerializeTableToParquetStream(std::shared_ptr<Table>& table,
                                                     librados::bufferlist& bl);

/// \brief Deserialize the Table from an Arrow IPC binary bufferlist.
ARROW_DS_EXPORT Status DeserializeTableFromBufferlist(std::shared_ptr<Table>* table,
                                                      librados::bufferlist& bl);

}  // namespace dataset
}  // namespace arrow

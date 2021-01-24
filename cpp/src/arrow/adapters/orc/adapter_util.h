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

#include "arrow/array/builder_base.h"
#include "arrow/status.h"
#include "orc/OrcFile.hh"

namespace liborc = orc;

namespace arrow {

namespace adapters {

namespace orc {

Status GetArrowType(const liborc::Type* type, std::shared_ptr<DataType>* out);

Status GetORCType(const DataType& type, ORC_UNIQUE_PTR<liborc::Type>* out);

Status GetORCType(const Schema& schema, ORC_UNIQUE_PTR<liborc::Type>* out);

Status AppendBatch(const liborc::Type* type, liborc::ColumnVectorBatch* batch,
                   int64_t offset, int64_t length, ArrayBuilder* builder);

/* incoming_mask is exclusively used by FillStructBatch. The cause is that ORC is much
 * stricter than Arrow in terms of consistency. In this case if a struct scalar is null
 * all its children must be set to null or ORC is not going to function properly. This is
 * why I added incomingMask to pass on null status from a struct to its children.
 */

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                 int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                 Array* parray, std::vector<bool>* incoming_mask = NULLPTR);

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                 int64_t& arrow_index_offset, int& arrow_chunk_offset, int64_t length,
                 ChunkedArray* chunked_array);

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

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

Result<ORC_UNIQUE_PTR<liborc::Type>> GetOrcType(const Schema& schema);

Status AppendBatch(const liborc::Type* type, liborc::ColumnVectorBatch* batch,
                   int64_t offset, int64_t length, arrow::ArrayBuilder* builder);

/// \brief Write a chunked array to an orc::ColumnVectorBatch
///
/// \param[in] chunked_array the chunked array
/// \param[in] length the orc::ColumnVectorBatch size limit
/// \param[in,out] arrow_chunk_offset The current chunk being processed
/// \param[in,out] arrow_index_offset The index of the arrow_chunk_offset array
/// before or after a process
/// \param[in,out] column_vector_batch the orc::ColumnVectorBatch to be filled
/// \return Status
Status WriteBatch(const ChunkedArray& chunked_array, int64_t length,
                  int* arrow_chunk_offset, int64_t* arrow_index_offset,
                  liborc::ColumnVectorBatch* column_vector_batch);

}  // namespace orc
}  // namespace adapters
}  // namespace arrow

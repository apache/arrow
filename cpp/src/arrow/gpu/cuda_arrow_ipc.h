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

#include "arrow/buffer.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

#include "arrow/gpu/cuda_memory.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

namespace ipc {

class Message;
class DictionaryMemo;

}  // namespace ipc

namespace cuda {

/// \defgroup cuda-ipc-functions Functions for CUDA IPC
///
/// @{

/// \brief Write record batch message to GPU device memory
/// \param[in] batch record batch to write
/// \param[in] ctx CudaContext to allocate device memory from
/// \return CudaBuffer or Status
ARROW_EXPORT
Result<std::shared_ptr<CudaBuffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                         CudaContext* ctx);

/// \brief ReadRecordBatch specialized to handle metadata on CUDA device
/// \param[in] schema the Schema for the record batch
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] buffer a CudaBuffer containing the complete IPC message
/// \param[in] pool a MemoryPool to use for allocating space for the metadata
/// \return RecordBatch or Status
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const std::shared_ptr<Schema>& schema, const ipc::DictionaryMemo* dictionary_memo,
    const std::shared_ptr<CudaBuffer>& buffer, MemoryPool* pool = default_memory_pool());

/// @}

}  // namespace cuda
}  // namespace arrow

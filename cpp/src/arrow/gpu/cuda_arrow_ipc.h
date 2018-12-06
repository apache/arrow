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

#ifndef ARROW_GPU_CUDA_ARROW_IPC_H
#define ARROW_GPU_CUDA_ARROW_IPC_H

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "arrow/gpu/cuda_memory.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

namespace ipc {

class Message;

}  // namespace ipc

namespace cuda {

/// \brief Write record batch message to GPU device memory
/// \param[in] batch record batch to write
/// \param[in] ctx CudaContext to allocate device memory from
/// \param[out] out the returned device buffer which contains the record batch message
/// \return Status
ARROW_EXPORT
Status SerializeRecordBatch(const RecordBatch& batch, CudaContext* ctx,
                            std::shared_ptr<CudaBuffer>* out);

/// \brief Read Arrow IPC message located on GPU device
/// \param[in] reader a CudaBufferReader
/// \param[in] pool a MemoryPool to allocate CPU memory for the metadata
/// \param[out] message the deserialized message, body still on device
///
/// This function reads the message metadata into host memory, but leaves the
/// message body on the device
ARROW_EXPORT
Status ReadMessage(CudaBufferReader* reader, MemoryPool* pool,
                   std::unique_ptr<ipc::Message>* message);

/// \brief ReadRecordBatch specialized to handle metadata on CUDA device
/// \param[in] schema the Schema for the record batch
/// \param[in] buffer a CudaBuffer containing the complete IPC message
/// \param[in] pool a MemoryPool to use for allocating space for the metadata
/// \param[out] out the reconstructed RecordBatch, with device pointers
ARROW_EXPORT
Status ReadRecordBatch(const std::shared_ptr<Schema>& schema,
                       const std::shared_ptr<CudaBuffer>& buffer, MemoryPool* pool,
                       std::shared_ptr<RecordBatch>* out);

}  // namespace cuda
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_ARROW_IPC_H

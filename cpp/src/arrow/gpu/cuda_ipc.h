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

#ifndef ARROW_GPU_CUDA_MEMORY_H
#define ARROW_GPU_CUDA_MEMORY_H

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "arrow/cuda_memory.h"

namespace arrow {
namespace gpu {

/// \brief Write record batch message to GPU device memory
///
///
ARROW_EXPORT
SerializeRecordBatch(const RecordBatch& batch, CudaContext* ctx,
                     std::shared_ptr<CudaBuffer>* out);

/// \brief Write record batch to pre-allocated GPU device memory
///
/// \param[in] batch the record batch to write
/// \param[in] out the CudaBufferWriter to write the output to
/// \return Status
///
/// The CudaBufferWriter must have enough pre-allocated space to accommodate
/// the record batch. You can use arrow::ipc::GetRecordBatchSize to compute
/// this
ARROW_EXPORT
SerializeRecordBatch(const RecordBatch& batch, CudaBufferWriter* out);

}  // namespace gpu
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_MEMORY_H

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

#include "arrow/gpu/cuda_memory.h"

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/gpu/cuda_common.h"

namespace arrow {
namespace gpu {

CudaBuffer::~CudaBuffer() {
  if (own_data_) {
    CUDA_DCHECK(cudaFree(mutable_data_));
  }
}

Status CudaBuffer::CopyHost(uint8_t* out) {
  CUDA_RETURN_NOT_OK(cudaMemcpy(out, data_, size_, cudaMemcpyDeviceToHost));
  return Status::OK();
}

Status AllocateCudaBuffer(int gpu_number, const int64_t size,
                          std::shared_ptr<CudaBuffer>* out) {
  CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number));
  uint8_t* data = nullptr;
  CUDA_RETURN_NOT_OK(
      cudaMalloc(reinterpret_cast<void**>(&data), static_cast<size_t>(size)));
  *out = std::make_shared<CudaBuffer>(data, size, gpu_number, true);
  return Status::OK();
}

CudaHostBuffer::~CudaHostBuffer() { CUDA_DCHECK(cudaFreeHost(mutable_data_)); }

Status AllocateCudaHostBuffer(const int gpu_number, const int64_t size,
                              std::shared_ptr<CudaHostBuffer>* out) {
  uint8_t* data = nullptr;
  CUDA_RETURN_NOT_OK(
      cudaMallocHost(reinterpret_cast<void**>(&data), static_cast<size_t>(size)));
  *out = std::make_shared<CudaHostBuffer>(data, size);
  return Status::OK();
}

}  // namespace gpu
}  // namespace arrow

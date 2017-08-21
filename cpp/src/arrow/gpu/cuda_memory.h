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

namespace arrow {
namespace gpu {

/// \brief An Arrow buffer located on a GPU device
///
/// Be careful using this in any Arrow code which may not be GPU-aware
class ARROW_EXPORT CudaBuffer : public MutableBuffer {
 public:
  CudaBuffer(uint8_t* data, int64_t size, const int gpu_number, bool own_data = false)
      : MutableBuffer(data, size), gpu_number_(gpu_number), own_data_(own_data) {}

  ~CudaBuffer();

  /// \brief Copy memory from GPU device to CPU host
  /// \param[out] out a pre-allocated output buffer
  /// \return Status
  Status CopyHost(uint8_t* out);

  int gpu_number() const { return gpu_number_; }

 private:
  const int gpu_number_;
  bool own_data_;
};

/// \brief Device-accessible CPU memory created using cudaHostAlloc
class ARROW_EXPORT CudaHostBuffer : public MutableBuffer {
 public:
  using MutableBuffer::MutableBuffer;
  ~CudaHostBuffer();
};

/// \brief Allocate CUDA memory on a GPU device
/// \param[in] gpu_number Device number to allocate
/// \param[in] size number of bytes
/// \param[out] out the allocated buffer
/// \return Status
ARROW_EXPORT
Status AllocateCudaBuffer(const int gpu_number, const int64_t size,
                          std::shared_ptr<CudaBuffer>* out);

/// \brief Allocate CUDA-accessible memory on CPU host
/// \param[in] size number of bytes
/// \param[out] out the allocated buffer
/// \return Status
ARROW_EXPORT
Status AllocateCudaHostBuffer(const int64_t size, std::shared_ptr<CudaHostBuffer>* out);

}  // namespace gpu
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_MEMORY_H

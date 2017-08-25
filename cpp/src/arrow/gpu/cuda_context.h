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

#ifndef ARROW_GPU_CUDA_CONTEXT_H
#define ARROW_GPU_CUDA_CONTEXT_H

#include <cstdint>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace gpu {

class CudaBuffer;
class CudaHostBuffer;

// Forward declaration
class CudaContext;

class ARROW_EXPORT CudaDeviceManager {
 public:
  static Status GetInstance(CudaDeviceManager** manager);

  /// \brief Create a CUDA driver context for a particular device
  Status CreateContext(int gpu_number, std::shared_ptr<CudaContext>* ctx);

  Status AllocateHost(int64_t nbytes, std::shared_ptr<CudaHostBuffer>* buffer);
  Status FreeHost(uint8_t* data, int64_t nbytes);

  int num_devices() const;

 private:
  std::unique_ptr<CudaDeviceManager> instance_;

  class CudaDeviceManagerImpl;
  std::unique_ptr<CudaDeviceManagerImpl> impl_;

  friend CudaContext;
};

struct ARROW_EXPORT CudaDeviceInfo {};

/// \class CudaContext
/// \brief Friendlier interface to the CUDA driver API
class ARROW_EXPORT CudaContext {
 public:
  ~CudaContext();

  Status Destroy();

  Status CopyHostToDevice(uint8_t* dst, const uint8_t* src, int64_t nbytes);
  Status CopyDeviceToHost(uint8_t* dst, const uint8_t* src, int64_t nbytes);

  Status Allocate(int64_t nbytes, std::shared_ptr<CudaBuffer>* buffer);
  Status Free(uint8_t* device_ptr, int64_t nbytes);

  int64_t bytes_allocated() const;

 private:
  CudaContext();

  class CudaContextImpl;
  std::unique_ptr<CudaContextImpl> impl_;

  friend CudaDeviceManager::CudaDeviceManagerImpl;
};

}  // namespace gpu
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_CONTEXT_H

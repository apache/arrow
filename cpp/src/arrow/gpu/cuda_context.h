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

#include "arrow/gpu/cuda_memory.h"

namespace arrow {
namespace gpu {

// Forward declaration
class CudaContext;

class ARROW_EXPORT CudaDeviceManager {
 public:
  static Status GetInstance(CudaDeviceManager** manager);

  /// \brief Get the shared CUDA driver context for a particular device
  Status GetContext(int gpu_number, std::shared_ptr<CudaContext>* ctx);

  /// \brief Create a new context for a given device number
  ///
  /// In general code will use GetContext
  Status CreateNewContext(int gpu_number, std::shared_ptr<CudaContext>* ctx);

  Status AllocateHost(int64_t nbytes, std::shared_ptr<CudaHostBuffer>* buffer);

  Status FreeHost(uint8_t* data, int64_t nbytes);

  int num_devices() const;

 private:
  CudaDeviceManager();
  static std::unique_ptr<CudaDeviceManager> instance_;

  class CudaDeviceManagerImpl;
  std::unique_ptr<CudaDeviceManagerImpl> impl_;

  friend CudaContext;
};

struct ARROW_EXPORT CudaDeviceInfo {};

/// \class CudaContext
/// \brief Friendlier interface to the CUDA driver API
class ARROW_EXPORT CudaContext : public std::enable_shared_from_this<CudaContext> {
 public:
  ~CudaContext();

  Status Close();

  /// \brief Allocate CUDA memory on GPU device for this context
  /// \param[in] nbytes number of bytes
  /// \param[out] out the allocated buffer
  /// \return Status
  Status Allocate(int64_t nbytes, std::shared_ptr<CudaBuffer>* out);

  /// \brief Open existing CUDA IPC memory handle
  /// \param[in] ipc_handle opaque pointer to CUipcMemHandle (driver API)
  /// \param[out] buffer a CudaBuffer referencing
  /// \return Status
  Status OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle,
                       std::shared_ptr<CudaBuffer>* buffer);

  int64_t bytes_allocated() const;

 private:
  CudaContext();

  Status ExportIpcBuffer(uint8_t* data, std::unique_ptr<CudaIpcMemHandle>* handle);
  Status CopyHostToDevice(uint8_t* dst, const uint8_t* src, int64_t nbytes);
  Status CopyDeviceToHost(uint8_t* dst, const uint8_t* src, int64_t nbytes);
  Status Free(uint8_t* device_ptr, int64_t nbytes);

  class CudaContextImpl;
  std::unique_ptr<CudaContextImpl> impl_;

  friend CudaBuffer;
  friend CudaBufferReader;
  friend CudaBufferWriter;
  friend CudaDeviceManager::CudaDeviceManagerImpl;
};

}  // namespace gpu
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_CONTEXT_H

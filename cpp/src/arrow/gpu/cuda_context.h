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
namespace cuda {

// Forward declaration
class CudaContext;

class ARROW_EXPORT CudaDeviceManager {
 public:
  static Status GetInstance(CudaDeviceManager** manager);

  /// \brief Get the CUDA driver context for a particular device
  /// \param[in] device_number the CUDA device
  /// \param[out] out cached context
  Status GetContext(int device_number, std::shared_ptr<CudaContext>* out);

  /// \brief Get the shared CUDA driver context for a particular device
  /// \param[in] device_number the CUDA device
  /// \param[in] handle CUDA context handler created by another library
  /// \param[out] out shared context
  Status GetSharedContext(int device_number, void* handle,
                          std::shared_ptr<CudaContext>* out);

  /// \brief Allocate host memory with fast access to given GPU device
  /// \param[in] device_number the CUDA device
  /// \param[in] nbytes number of bytes
  /// \param[out] out the allocated buffer
  Status AllocateHost(int device_number, int64_t nbytes,
                      std::shared_ptr<CudaHostBuffer>* out);

  Status FreeHost(void* data, int64_t nbytes);

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

  /// \brief Create a view of CUDA memory on GPU device of this context
  /// \param[in] data the starting device address
  /// \param[in] nbytes number of bytes
  /// \param[out] out the view buffer
  /// \return Status
  ///
  /// \note The caller is responsible for allocating and freeing the
  /// memory as well as ensuring that the memory belongs to the CUDA
  /// context that this CudaContext instance holds.
  Status View(uint8_t* data, int64_t nbytes, std::shared_ptr<CudaBuffer>* out);

  /// \brief Open existing CUDA IPC memory handle
  /// \param[in] ipc_handle opaque pointer to CUipcMemHandle (driver API)
  /// \param[out] out a CudaBuffer referencing the IPC segment
  /// \return Status
  Status OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle,
                       std::shared_ptr<CudaBuffer>* out);

  /// \brief Close memory mapped with IPC buffer
  /// \param[in] buffer a CudaBuffer referencing
  /// \return Status
  Status CloseIpcBuffer(CudaBuffer* buffer);

  /// \brief Block until the all device tasks are completed.
  Status Synchronize(void);

  int64_t bytes_allocated() const;

  /// \brief Expose CUDA context handle to other libraries
  void* handle() const;

  /// \brief Return device number
  int device_number() const;

 private:
  CudaContext();

  Status ExportIpcBuffer(void* data, int64_t size,
                         std::shared_ptr<CudaIpcMemHandle>* handle);
  Status CopyHostToDevice(void* dst, const void* src, int64_t nbytes);
  Status CopyDeviceToHost(void* dst, const void* src, int64_t nbytes);
  Status CopyDeviceToDevice(void* dst, const void* src, int64_t nbytes);
  Status Free(void* device_ptr, int64_t nbytes);

  class CudaContextImpl;
  std::unique_ptr<CudaContextImpl> impl_;

  friend CudaBuffer;
  friend CudaBufferReader;
  friend CudaBufferWriter;
  friend CudaDeviceManager::CudaDeviceManagerImpl;
};

}  // namespace cuda
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_CONTEXT_H

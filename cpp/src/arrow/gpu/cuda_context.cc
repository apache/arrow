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

#include "arrow/gpu/cuda_context.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <cuda.h>

#include "arrow/gpu/cuda_common.h"
#include "arrow/gpu/cuda_memory.h"

namespace arrow {
namespace gpu {

struct CudaDevice {
  int device_num;
  CUdevice handle;
  int64_t total_memory;
};

class CudaContext::CudaContextImpl {
 public:
  CudaContextImpl() {}

  Status Init(const CudaDevice& device) {
    device_ = device;
    CU_RETURN_NOT_OK(cuCtxCreate(&context_, 0, device_.handle));
    is_open_ = true;
    return Status::OK();
  }

  Status Close() {
    if (is_open_ && own_context_) {
      CU_RETURN_NOT_OK(cuCtxDestroy(context_));
    }
    is_open_ = false;
    return Status::OK();
  }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  Status Allocate(int64_t nbytes, uint8_t** out) {
    CU_RETURN_NOT_OK(cuCtxSetCurrent(context_));

    CUdeviceptr data;
    CU_RETURN_NOT_OK(cuMemAlloc(&data, static_cast<size_t>(nbytes)));
    bytes_allocated_ += nbytes;
    *out = reinterpret_cast<uint8_t*>(data);
    return Status::OK();
  }

  Status CopyHostToDevice(void* dst, const void* src, int64_t nbytes) {
    CU_RETURN_NOT_OK(cuCtxSetCurrent(context_));
    CU_RETURN_NOT_OK(cuMemcpyHtoD(reinterpret_cast<CUdeviceptr>(dst), src,
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToHost(void* dst, const void* src, int64_t nbytes) {
    CU_RETURN_NOT_OK(cuCtxSetCurrent(context_));
    CU_RETURN_NOT_OK(cuMemcpyDtoH(dst, reinterpret_cast<const CUdeviceptr>(src),
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status Free(void* device_ptr, int64_t nbytes) {
    CU_RETURN_NOT_OK(cuMemFree(reinterpret_cast<CUdeviceptr>(device_ptr)));
    bytes_allocated_ -= nbytes;
    return Status::OK();
  }

  Status ExportIpcBuffer(void* data, std::shared_ptr<CudaIpcMemHandle>* handle) {
    CU_RETURN_NOT_OK(cuCtxSetCurrent(context_));
    CUipcMemHandle cu_handle;
    CU_RETURN_NOT_OK(cuIpcGetMemHandle(&cu_handle, reinterpret_cast<CUdeviceptr>(data)));
    *handle = std::shared_ptr<CudaIpcMemHandle>(new CudaIpcMemHandle(&cu_handle));
    return Status::OK();
  }

  Status OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle, uint8_t** out) {
    CU_RETURN_NOT_OK(cuCtxSetCurrent(context_));
    auto handle = reinterpret_cast<const CUipcMemHandle*>(ipc_handle.handle());

    CUdeviceptr data;
    CU_RETURN_NOT_OK(
        cuIpcOpenMemHandle(&data, *handle, CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS));
    *out = reinterpret_cast<uint8_t*>(data);
    return Status::OK();
  }

  const CudaDevice device() const { return device_; }

 private:
  CudaDevice device_;
  CUcontext context_;
  bool is_open_;

  // So that we can utilize a CUcontext that was created outside this library
  bool own_context_;

  std::atomic<int64_t> bytes_allocated_;
};

class CudaDeviceManager::CudaDeviceManagerImpl {
 public:
  CudaDeviceManagerImpl() : host_bytes_allocated_(0) {}

  Status Init() {
    CU_RETURN_NOT_OK(cuInit(0));
    CU_RETURN_NOT_OK(cuDeviceGetCount(&num_devices_));

    devices_.resize(num_devices_);
    for (int i = 0; i < num_devices_; ++i) {
      RETURN_NOT_OK(GetDeviceProperties(i, &devices_[i]));
    }
    return Status::OK();
  }

  Status AllocateHost(int64_t nbytes, uint8_t** out) {
    CU_RETURN_NOT_OK(cuMemHostAlloc(reinterpret_cast<void**>(out),
                                    static_cast<size_t>(nbytes),
                                    CU_MEMHOSTALLOC_PORTABLE));
    host_bytes_allocated_ += nbytes;
    return Status::OK();
  }

  Status FreeHost(void* data, int64_t nbytes) {
    CU_RETURN_NOT_OK(cuMemFreeHost(data));
    host_bytes_allocated_ -= nbytes;
    return Status::OK();
  }

  Status GetDeviceProperties(int device_number, CudaDevice* device) {
    device->device_num = device_number;
    CU_RETURN_NOT_OK(cuDeviceGet(&device->handle, device_number));

    size_t total_memory = 0;
    CU_RETURN_NOT_OK(cuDeviceTotalMem(&total_memory, device->handle));
    device->total_memory = total_memory;
    return Status::OK();
  }

  Status CreateNewContext(int device_number, std::shared_ptr<CudaContext>* out) {
    *out = std::shared_ptr<CudaContext>(new CudaContext());
    return (*out)->impl_->Init(devices_[device_number]);
  }

  Status GetContext(int device_number, std::shared_ptr<CudaContext>* out) {
    auto it = contexts_.find(device_number);
    if (it == contexts_.end()) {
      std::shared_ptr<CudaContext> new_context;
      RETURN_NOT_OK(CreateNewContext(device_number, &new_context));
      contexts_[device_number] = *out = new_context;
    } else {
      *out = it->second;
    }
    return Status::OK();
  }

  int num_devices() const { return num_devices_; }

 private:
  int num_devices_;
  std::vector<CudaDevice> devices_;

  // device_number -> CudaContext
  std::unordered_map<int, std::shared_ptr<CudaContext>> contexts_;

  int64_t host_bytes_allocated_;
};

CudaDeviceManager::CudaDeviceManager() { impl_.reset(new CudaDeviceManagerImpl()); }

std::unique_ptr<CudaDeviceManager> CudaDeviceManager::instance_ = nullptr;

Status CudaDeviceManager::GetInstance(CudaDeviceManager** manager) {
  if (!instance_) {
    instance_.reset(new CudaDeviceManager());
    RETURN_NOT_OK(instance_->impl_->Init());
  }
  *manager = instance_.get();
  return Status::OK();
}

Status CudaDeviceManager::GetContext(int device_number,
                                     std::shared_ptr<CudaContext>* out) {
  return impl_->GetContext(device_number, out);
}

Status CudaDeviceManager::CreateNewContext(int device_number,
                                           std::shared_ptr<CudaContext>* out) {
  return impl_->CreateNewContext(device_number, out);
}

Status CudaDeviceManager::AllocateHost(int64_t nbytes,
                                       std::shared_ptr<CudaHostBuffer>* out) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->AllocateHost(nbytes, &data));
  *out = std::make_shared<CudaHostBuffer>(data, nbytes);
  return Status::OK();
}

Status CudaDeviceManager::FreeHost(void* data, int64_t nbytes) {
  return impl_->FreeHost(data, nbytes);
}

int CudaDeviceManager::num_devices() const { return impl_->num_devices(); }

// ----------------------------------------------------------------------
// CudaContext public API

CudaContext::CudaContext() { impl_.reset(new CudaContextImpl()); }

CudaContext::~CudaContext() {}

Status CudaContext::Allocate(int64_t nbytes, std::shared_ptr<CudaBuffer>* out) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->Allocate(nbytes, &data));
  *out = std::make_shared<CudaBuffer>(data, nbytes, this->shared_from_this(), true);
  return Status::OK();
}

Status CudaContext::ExportIpcBuffer(void* data,
                                    std::shared_ptr<CudaIpcMemHandle>* handle) {
  return impl_->ExportIpcBuffer(data, handle);
}

Status CudaContext::CopyHostToDevice(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyHostToDevice(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToHost(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyDeviceToHost(dst, src, nbytes);
}

Status CudaContext::Close() { return impl_->Close(); }

Status CudaContext::Free(void* device_ptr, int64_t nbytes) {
  return impl_->Free(device_ptr, nbytes);
}

Status CudaContext::OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle,
                                  std::shared_ptr<CudaBuffer>* out) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->OpenIpcBuffer(ipc_handle, &data));

  // Need to ask the device how big the buffer is
  size_t allocation_size = 0;
  CU_RETURN_NOT_OK(cuMemGetAddressRange(nullptr, &allocation_size,
                                        reinterpret_cast<CUdeviceptr>(data)));

  *out = std::make_shared<CudaBuffer>(data, allocation_size, this->shared_from_this(),
                                      true, true);
  return Status::OK();
}

int64_t CudaContext::bytes_allocated() const { return impl_->bytes_allocated(); }

}  // namespace gpu
}  // namespace arrow

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
namespace cuda {

struct CudaDevice {
  int device_num;
  CUdevice handle;
  int64_t total_memory;
};

class ContextSaver {
 public:
  explicit ContextSaver(CUcontext new_context) { cuCtxPushCurrent(new_context); }
  ~ContextSaver() {
    CUcontext unused;
    cuCtxPopCurrent(&unused);
  }
};

class CudaContext::CudaContextImpl {
 public:
  CudaContextImpl() : bytes_allocated_(0) {}

  Status Init(const CudaDevice& device) {
    device_ = device;
    own_context_ = true;
    CU_RETURN_NOT_OK(cuDevicePrimaryCtxRetain(&context_, device_.handle));
    is_open_ = true;
    return Status::OK();
  }

  Status InitShared(const CudaDevice& device, CUcontext ctx) {
    device_ = device;
    own_context_ = false;
    context_ = ctx;
    is_open_ = true;
    return Status::OK();
  }

  Status Close() {
    if (is_open_ && own_context_) {
      CU_RETURN_NOT_OK(cuDevicePrimaryCtxRelease(device_.handle));
    }
    is_open_ = false;
    return Status::OK();
  }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  Status Allocate(int64_t nbytes, uint8_t** out) {
    if (nbytes > 0) {
      ContextSaver set_temporary(context_);
      CUdeviceptr data;
      CU_RETURN_NOT_OK(cuMemAlloc(&data, static_cast<size_t>(nbytes)));
      bytes_allocated_ += nbytes;
      *out = reinterpret_cast<uint8_t*>(data);
    } else {
      *out = nullptr;
    }
    return Status::OK();
  }

  Status CopyHostToDevice(void* dst, const void* src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK(cuMemcpyHtoD(reinterpret_cast<CUdeviceptr>(dst), src,
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToHost(void* dst, const void* src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK(cuMemcpyDtoH(dst, reinterpret_cast<const CUdeviceptr>(src),
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToDevice(void* dst, const void* src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK(cuMemcpyDtoD(reinterpret_cast<CUdeviceptr>(dst),
                                  reinterpret_cast<const CUdeviceptr>(src),
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx, void* dst,
                                   const void* src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK(cuMemcpyPeer(reinterpret_cast<CUdeviceptr>(dst),
                                  reinterpret_cast<CUcontext>(dst_ctx->handle()),
                                  reinterpret_cast<const CUdeviceptr>(src), context_,
                                  static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status Synchronize(void) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK(cuCtxSynchronize());
    return Status::OK();
  }

  Status Free(void* device_ptr, int64_t nbytes) {
    CU_RETURN_NOT_OK(cuMemFree(reinterpret_cast<CUdeviceptr>(device_ptr)));
    bytes_allocated_ -= nbytes;
    return Status::OK();
  }

  Status ExportIpcBuffer(void* data, int64_t size,
                         std::shared_ptr<CudaIpcMemHandle>* handle) {
    CUipcMemHandle cu_handle;
    if (size > 0) {
      ContextSaver set_temporary(context_);
      CU_RETURN_NOT_OK(
          cuIpcGetMemHandle(&cu_handle, reinterpret_cast<CUdeviceptr>(data)));
    }
    *handle = std::shared_ptr<CudaIpcMemHandle>(new CudaIpcMemHandle(size, &cu_handle));
    return Status::OK();
  }

  Status OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle, uint8_t** out) {
    int64_t size = ipc_handle.memory_size();
    if (size > 0) {
      auto handle = reinterpret_cast<const CUipcMemHandle*>(ipc_handle.handle());
      CUdeviceptr data;
      CU_RETURN_NOT_OK(
          cuIpcOpenMemHandle(&data, *handle, CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS));
      *out = reinterpret_cast<uint8_t*>(data);
    } else {
      *out = nullptr;
    }
    return Status::OK();
  }

  const CudaDevice device() const { return device_; }

  void* context_handle() const { return reinterpret_cast<void*>(context_); }

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

  Status AllocateHost(int device_number, int64_t nbytes, uint8_t** out) {
    std::shared_ptr<CudaContext> ctx;
    RETURN_NOT_OK(GetContext(device_number, &ctx));
    ContextSaver set_temporary((CUcontext)(ctx.get()->handle()));
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

  Status GetContext(int device_number, std::shared_ptr<CudaContext>* out) {
    *out = std::shared_ptr<CudaContext>(new CudaContext());
    return (*out)->impl_->Init(devices_[device_number]);
  }

  Status GetSharedContext(int device_number, CUcontext ctx,
                          std::shared_ptr<CudaContext>* out) {
    *out = std::shared_ptr<CudaContext>(new CudaContext());
    return (*out)->impl_->InitShared(devices_[device_number], ctx);
  }

  int num_devices() const { return num_devices_; }

 private:
  int num_devices_;
  std::vector<CudaDevice> devices_;

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

Status CudaDeviceManager::GetSharedContext(int device_number, void* ctx,
                                           std::shared_ptr<CudaContext>* out) {
  return impl_->GetSharedContext(device_number, (CUcontext)ctx, out);
}

Status CudaDeviceManager::AllocateHost(int device_number, int64_t nbytes,
                                       std::shared_ptr<CudaHostBuffer>* out) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->AllocateHost(device_number, nbytes, &data));
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

Status CudaContext::View(uint8_t* data, int64_t nbytes,
                         std::shared_ptr<CudaBuffer>* out) {
  *out = std::make_shared<CudaBuffer>(data, nbytes, this->shared_from_this(), false);
  return Status::OK();
}

Status CudaContext::ExportIpcBuffer(void* data, int64_t size,
                                    std::shared_ptr<CudaIpcMemHandle>* handle) {
  return impl_->ExportIpcBuffer(data, size, handle);
}

Status CudaContext::CopyHostToDevice(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyHostToDevice(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToHost(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyDeviceToHost(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToDevice(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyDeviceToDevice(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx,
                                              void* dst, const void* src,
                                              int64_t nbytes) {
  return impl_->CopyDeviceToAnotherDevice(dst_ctx, dst, src, nbytes);
}

Status CudaContext::Synchronize(void) { return impl_->Synchronize(); }

Status CudaContext::Close() { return impl_->Close(); }

Status CudaContext::Free(void* device_ptr, int64_t nbytes) {
  return impl_->Free(device_ptr, nbytes);
}

Status CudaContext::OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle,
                                  std::shared_ptr<CudaBuffer>* out) {
  if (ipc_handle.memory_size() > 0) {
    ContextSaver set_temporary(reinterpret_cast<CUcontext>(handle()));
    uint8_t* data = nullptr;
    RETURN_NOT_OK(impl_->OpenIpcBuffer(ipc_handle, &data));
    // Need to ask the device how big the buffer is
    size_t allocation_size = 0;
    CU_RETURN_NOT_OK(cuMemGetAddressRange(nullptr, &allocation_size,
                                          reinterpret_cast<CUdeviceptr>(data)));
    *out = std::make_shared<CudaBuffer>(data, allocation_size, this->shared_from_this(),
                                        true, true);
  } else {
    // zero-sized buffer does not own data (which is nullptr), hence
    // CloseIpcBuffer will not be called (see CudaBuffer::Close).
    *out =
        std::make_shared<CudaBuffer>(nullptr, 0, this->shared_from_this(), false, true);
  }
  return Status::OK();
}

Status CudaContext::CloseIpcBuffer(CudaBuffer* buf) {
  ContextSaver set_temporary(reinterpret_cast<CUcontext>(handle()));
  uint8_t* data = buf->mutable_data();
  CU_RETURN_NOT_OK(cuIpcCloseMemHandle(reinterpret_cast<CUdeviceptr>(data)));
  return Status::OK();
}

int64_t CudaContext::bytes_allocated() const { return impl_->bytes_allocated(); }

void* CudaContext::handle() const { return impl_->context_handle(); }

int CudaContext::device_number() const { return impl_->device().device_num; }

Status CudaContext::GetDeviceAddress(uint8_t* addr, uint8_t** devaddr) {
  ContextSaver set_temporary(reinterpret_cast<CUcontext>(handle()));
  CU_RETURN_NOT_OK(cuPointerGetAttribute(devaddr, CU_POINTER_ATTRIBUTE_DEVICE_POINTER,
                                         reinterpret_cast<CUdeviceptr>(addr)));
  return Status::OK();
}

}  // namespace cuda
}  // namespace arrow

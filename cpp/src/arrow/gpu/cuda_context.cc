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
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <cuda.h>

#include "arrow/gpu/cuda_internal.h"
#include "arrow/gpu/cuda_memory.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace cuda {

using internal::ContextSaver;

namespace {

struct DeviceProperties {
  int device_number_;
  CUdevice handle_;
  int64_t total_memory_;
  std::string name_;

  Status Init(int device_number) {
    device_number_ = device_number;
    CU_RETURN_NOT_OK("cuDeviceGet", cuDeviceGet(&handle_, device_number));
    size_t total_memory = 0;
    CU_RETURN_NOT_OK("cuDeviceTotalMem", cuDeviceTotalMem(&total_memory, handle_));
    total_memory_ = total_memory;

    char buf[200];
    CU_RETURN_NOT_OK("cuDeviceGetName", cuDeviceGetName(buf, sizeof(buf), device_number));
    name_.assign(buf);
    return Status::OK();
  }
};

const char kCudaDeviceTypeName[] = "arrow::cuda::CudaDevice";

}  // namespace

struct CudaDevice::Impl {
  DeviceProperties props;
};

// ----------------------------------------------------------------------
// CudaContext implementation

class CudaContext::Impl {
 public:
  Impl() : bytes_allocated_(0) {}

  Status Init(const std::shared_ptr<CudaDevice>& device) {
    mm_ = checked_pointer_cast<CudaMemoryManager>(device->default_memory_manager());
    props_ = &device->impl_->props;
    own_context_ = true;
    CU_RETURN_NOT_OK("cuDevicePrimaryCtxRetain",
                     cuDevicePrimaryCtxRetain(&context_, props_->handle_));
    is_open_ = true;
    return Status::OK();
  }

  Status InitShared(const std::shared_ptr<CudaDevice>& device, CUcontext ctx) {
    mm_ = checked_pointer_cast<CudaMemoryManager>(device->default_memory_manager());
    props_ = &device->impl_->props;
    own_context_ = false;
    context_ = ctx;
    is_open_ = true;
    return Status::OK();
  }

  Status Close() {
    if (is_open_ && own_context_) {
      CU_RETURN_NOT_OK("cuDevicePrimaryCtxRelease",
                       cuDevicePrimaryCtxRelease(props_->handle_));
    }
    is_open_ = false;
    return Status::OK();
  }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  Status Allocate(int64_t nbytes, uint8_t** out) {
    if (nbytes > 0) {
      ContextSaver set_temporary(context_);
      CUdeviceptr data;
      CU_RETURN_NOT_OK("cuMemAlloc", cuMemAlloc(&data, static_cast<size_t>(nbytes)));
      bytes_allocated_ += nbytes;
      *out = reinterpret_cast<uint8_t*>(data);
    } else {
      *out = nullptr;
    }
    return Status::OK();
  }

  Status CopyHostToDevice(uintptr_t dst, const void* src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK("cuMemcpyHtoD", cuMemcpyHtoD(dst, src, static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToHost(void* dst, uintptr_t src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK("cuMemcpyDtoH", cuMemcpyDtoH(dst, src, static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToDevice(uintptr_t dst, uintptr_t src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK("cuMemcpyDtoD", cuMemcpyDtoD(dst, src, static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx,
                                   uintptr_t dst, uintptr_t src, int64_t nbytes) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK("cuMemcpyPeer",
                     cuMemcpyPeer(dst, reinterpret_cast<CUcontext>(dst_ctx->handle()),
                                  src, context_, static_cast<size_t>(nbytes)));
    return Status::OK();
  }

  Status Synchronize(void) {
    ContextSaver set_temporary(context_);
    CU_RETURN_NOT_OK("cuCtxSynchronize", cuCtxSynchronize());
    return Status::OK();
  }

  Status Free(void* device_ptr, int64_t nbytes) {
    CU_RETURN_NOT_OK("cuMemFree", cuMemFree(reinterpret_cast<CUdeviceptr>(device_ptr)));
    bytes_allocated_ -= nbytes;
    return Status::OK();
  }

  Result<std::shared_ptr<CudaIpcMemHandle>> ExportIpcBuffer(const void* data,
                                                            int64_t size) {
    CUipcMemHandle cu_handle;
    if (size > 0) {
      ContextSaver set_temporary(context_);
      CU_RETURN_NOT_OK(
          "cuIpcGetMemHandle",
          cuIpcGetMemHandle(&cu_handle, reinterpret_cast<CUdeviceptr>(data)));
    }
    return std::shared_ptr<CudaIpcMemHandle>(new CudaIpcMemHandle(size, &cu_handle));
  }

  Status OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle, uint8_t** out) {
    int64_t size = ipc_handle.memory_size();
    if (size > 0) {
      auto handle = reinterpret_cast<const CUipcMemHandle*>(ipc_handle.handle());
      CUdeviceptr data;
      CU_RETURN_NOT_OK(
          "cuIpcOpenMemHandle",
          cuIpcOpenMemHandle(&data, *handle, CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS));
      *out = reinterpret_cast<uint8_t*>(data);
    } else {
      *out = nullptr;
    }
    return Status::OK();
  }

  std::shared_ptr<CudaDevice> device() const {
    return checked_pointer_cast<CudaDevice>(mm_->device());
  }

  const std::shared_ptr<CudaMemoryManager>& memory_manager() const { return mm_; }

  void* context_handle() const { return reinterpret_cast<void*>(context_); }

 private:
  std::shared_ptr<CudaMemoryManager> mm_;
  const DeviceProperties* props_;
  CUcontext context_;
  bool is_open_;

  // So that we can utilize a CUcontext that was created outside this library
  bool own_context_;

  std::atomic<int64_t> bytes_allocated_;
};

// ----------------------------------------------------------------------
// CudaDevice implementation

CudaDevice::CudaDevice(Impl impl) : impl_(new Impl(std::move(impl))) {}

const char* CudaDevice::type_name() const { return kCudaDeviceTypeName; }

std::string CudaDevice::ToString() const {
  std::stringstream ss;
  ss << "CudaDevice(device_number=" << device_number() << ", name=\"" << device_name()
     << "\")";
  return ss.str();
}

bool CudaDevice::Equals(const Device& other) const {
  if (!IsCudaDevice(other)) {
    return false;
  }
  return checked_cast<const CudaDevice&>(other).device_number() == device_number();
}

int CudaDevice::device_number() const { return impl_->props.device_number_; }

std::string CudaDevice::device_name() const { return impl_->props.name_; }

int64_t CudaDevice::total_memory() const { return impl_->props.total_memory_; }

int CudaDevice::handle() const { return impl_->props.handle_; }

Result<std::shared_ptr<CudaDevice>> CudaDevice::Make(int device_number) {
  ARROW_ASSIGN_OR_RAISE(auto manager, CudaDeviceManager::Instance());
  return manager->GetDevice(device_number);
}

std::shared_ptr<MemoryManager> CudaDevice::default_memory_manager() {
  return CudaMemoryManager::Make(shared_from_this());
}

Result<std::shared_ptr<CudaContext>> CudaDevice::GetContext() {
  // XXX should we cache a default context in CudaDevice instance?
  auto context = std::shared_ptr<CudaContext>(new CudaContext());
  auto self = checked_pointer_cast<CudaDevice>(shared_from_this());
  RETURN_NOT_OK(context->impl_->Init(self));
  return context;
}

Result<std::shared_ptr<CudaContext>> CudaDevice::GetSharedContext(void* handle) {
  auto context = std::shared_ptr<CudaContext>(new CudaContext());
  auto self = checked_pointer_cast<CudaDevice>(shared_from_this());
  RETURN_NOT_OK(context->impl_->InitShared(self, reinterpret_cast<CUcontext>(handle)));
  return context;
}

Result<std::shared_ptr<CudaHostBuffer>> CudaDevice::AllocateHostBuffer(int64_t size) {
  ARROW_ASSIGN_OR_RAISE(auto context, GetContext());
  ContextSaver set_temporary(*context);
  void* ptr;
  CU_RETURN_NOT_OK("cuMemHostAlloc", cuMemHostAlloc(&ptr, static_cast<size_t>(size),
                                                    CU_MEMHOSTALLOC_PORTABLE));
  return std::make_shared<CudaHostBuffer>(reinterpret_cast<uint8_t*>(ptr), size);
}

bool IsCudaDevice(const Device& device) {
  return device.type_name() == kCudaDeviceTypeName;
}

Result<std::shared_ptr<CudaDevice>> AsCudaDevice(const std::shared_ptr<Device>& device) {
  if (IsCudaDevice(*device)) {
    return checked_pointer_cast<CudaDevice>(device);
  } else {
    return Status::TypeError("Device is not a Cuda device: ", device->ToString());
  }
}

// ----------------------------------------------------------------------
// CudaMemoryManager implementation

std::shared_ptr<CudaMemoryManager> CudaMemoryManager::Make(
    const std::shared_ptr<Device>& device) {
  return std::shared_ptr<CudaMemoryManager>(new CudaMemoryManager(device));
}

std::shared_ptr<CudaDevice> CudaMemoryManager::cuda_device() const {
  return checked_pointer_cast<CudaDevice>(device_);
}

Result<std::shared_ptr<io::RandomAccessFile>> CudaMemoryManager::GetBufferReader(
    std::shared_ptr<Buffer> buf) {
  if (*buf->device() != *device_) {
    return Status::Invalid(
        "CudaMemoryManager::GetBufferReader called on foreign buffer "
        "for device ",
        buf->device()->ToString());
  }
  return std::make_shared<CudaBufferReader>(checked_pointer_cast<CudaBuffer>(buf));
}

Result<std::shared_ptr<io::OutputStream>> CudaMemoryManager::GetBufferWriter(
    std::shared_ptr<Buffer> buf) {
  if (*buf->device() != *device_) {
    return Status::Invalid(
        "CudaMemoryManager::GetBufferReader called on foreign buffer "
        "for device ",
        buf->device()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto cuda_buf, CudaBuffer::FromBuffer(buf));
  auto writer = std::make_shared<CudaBufferWriter>(cuda_buf);
  // Use 8MB buffering, which yields generally good performance
  RETURN_NOT_OK(writer->SetBufferSize(1 << 23));
  return writer;
}

Result<std::unique_ptr<Buffer>> CudaMemoryManager::AllocateBuffer(int64_t size) {
  ARROW_ASSIGN_OR_RAISE(auto context, cuda_device()->GetContext());
  return context->Allocate(size);
}

Result<std::shared_ptr<Buffer>> CudaMemoryManager::CopyBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (to->is_cpu()) {
    // Device-to-CPU copy
    std::shared_ptr<Buffer> dest;
    ARROW_ASSIGN_OR_RAISE(auto from_context, cuda_device()->GetContext());
    ARROW_ASSIGN_OR_RAISE(dest, to->AllocateBuffer(buf->size()));
    RETURN_NOT_OK(from_context->CopyDeviceToHost(dest->mutable_data(), buf->address(),
                                                 buf->size()));
    return dest;
  }
  return nullptr;
}

Result<std::shared_ptr<Buffer>> CudaMemoryManager::CopyBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (from->is_cpu()) {
    // CPU-to-device copy
    ARROW_ASSIGN_OR_RAISE(auto to_context, cuda_device()->GetContext());
    ARROW_ASSIGN_OR_RAISE(auto dest, to_context->Allocate(buf->size()));
    RETURN_NOT_OK(
        to_context->CopyHostToDevice(dest->address(), buf->data(), buf->size()));
    return dest;
  }
  if (IsCudaMemoryManager(*from)) {
    // Device-to-device copy
    ARROW_ASSIGN_OR_RAISE(auto to_context, cuda_device()->GetContext());
    ARROW_ASSIGN_OR_RAISE(
        auto from_context,
        checked_cast<const CudaMemoryManager&>(*from).cuda_device()->GetContext());
    ARROW_ASSIGN_OR_RAISE(auto dest, to_context->Allocate(buf->size()));
    if (to_context->handle() == from_context->handle()) {
      // Same context
      RETURN_NOT_OK(
          to_context->CopyDeviceToDevice(dest->address(), buf->address(), buf->size()));
    } else {
      // Other context
      RETURN_NOT_OK(from_context->CopyDeviceToAnotherDevice(to_context, dest->address(),
                                                            buf->address(), buf->size()));
    }
    return dest;
  }
  return nullptr;
}

Result<std::shared_ptr<Buffer>> CudaMemoryManager::ViewBufferTo(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& to) {
  if (to->is_cpu()) {
    // Device-on-CPU view
    ARROW_ASSIGN_OR_RAISE(auto address, GetHostAddress(buf->address()));
    return std::make_shared<Buffer>(address, buf->size(), to, buf);
  }
  return nullptr;
}

Result<std::shared_ptr<Buffer>> CudaMemoryManager::ViewBufferFrom(
    const std::shared_ptr<Buffer>& buf, const std::shared_ptr<MemoryManager>& from) {
  if (from->is_cpu()) {
    // CPU-on-device view
    ARROW_ASSIGN_OR_RAISE(auto to_context, cuda_device()->GetContext());
    ARROW_ASSIGN_OR_RAISE(auto address, GetDeviceAddress(buf->data(), to_context));
    return std::make_shared<Buffer>(address, buf->size(), shared_from_this(), buf);
  }
  return nullptr;
}

bool IsCudaMemoryManager(const MemoryManager& mm) { return IsCudaDevice(*mm.device()); }

Result<std::shared_ptr<CudaMemoryManager>> AsCudaMemoryManager(
    const std::shared_ptr<MemoryManager>& mm) {
  if (IsCudaMemoryManager(*mm)) {
    return checked_pointer_cast<CudaMemoryManager>(mm);
  } else {
    return Status::TypeError("Device is not a Cuda device: ", mm->device()->ToString());
  }
}

// ----------------------------------------------------------------------
// CudaDeviceManager implementation

class CudaDeviceManager::Impl {
 public:
  Impl() : host_bytes_allocated_(0) {}

  Status Init() {
    CU_RETURN_NOT_OK("cuInit", cuInit(0));
    CU_RETURN_NOT_OK("cuDeviceGetCount", cuDeviceGetCount(&num_devices_));

    devices_.resize(num_devices_);
    for (int i = 0; i < num_devices_; ++i) {
      ARROW_ASSIGN_OR_RAISE(devices_[i], MakeDevice(i));
    }
    return Status::OK();
  }

  Status AllocateHost(int device_number, int64_t nbytes, uint8_t** out) {
    RETURN_NOT_OK(CheckDeviceNum(device_number));
    ARROW_ASSIGN_OR_RAISE(auto ctx, GetContext(device_number));
    ContextSaver set_temporary((CUcontext)(ctx.get()->handle()));
    CU_RETURN_NOT_OK("cuMemHostAlloc", cuMemHostAlloc(reinterpret_cast<void**>(out),
                                                      static_cast<size_t>(nbytes),
                                                      CU_MEMHOSTALLOC_PORTABLE));
    host_bytes_allocated_ += nbytes;
    return Status::OK();
  }

  Status FreeHost(void* data, int64_t nbytes) {
    CU_RETURN_NOT_OK("cuMemFreeHost", cuMemFreeHost(data));
    host_bytes_allocated_ -= nbytes;
    return Status::OK();
  }

  Result<std::shared_ptr<CudaContext>> GetContext(int device_number) {
    RETURN_NOT_OK(CheckDeviceNum(device_number));
    return devices_[device_number]->GetContext();
  }

  Result<std::shared_ptr<CudaContext>> GetSharedContext(int device_number, void* handle) {
    RETURN_NOT_OK(CheckDeviceNum(device_number));
    return devices_[device_number]->GetSharedContext(handle);
  }

  Result<std::shared_ptr<CudaDevice>> GetDevice(int device_number) {
    RETURN_NOT_OK(CheckDeviceNum(device_number));
    return devices_[device_number];
  }

  int num_devices() const { return num_devices_; }

  Status CheckDeviceNum(int device_number) const {
    if (device_number < 0 || device_number >= num_devices_) {
      return Status::Invalid("Invalid Cuda device number ", device_number,
                             " (should be between 0 and ", num_devices_ - 1,
                             ", inclusive)");
    }
    return Status::OK();
  }

 protected:
  Result<std::shared_ptr<CudaDevice>> MakeDevice(int device_number) {
    DeviceProperties props;
    RETURN_NOT_OK(props.Init(device_number));
    return std::shared_ptr<CudaDevice>(new CudaDevice({std::move(props)}));
  }

 private:
  int num_devices_;
  std::vector<std::shared_ptr<CudaDevice>> devices_;

  int64_t host_bytes_allocated_;
};

CudaDeviceManager::CudaDeviceManager() { impl_.reset(new Impl()); }

std::unique_ptr<CudaDeviceManager> CudaDeviceManager::instance_ = nullptr;

Result<CudaDeviceManager*> CudaDeviceManager::Instance() {
  static std::mutex mutex;
  static std::atomic<bool> init_end(false);

  if (!init_end) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!init_end) {
      instance_.reset(new CudaDeviceManager());
      RETURN_NOT_OK(instance_->impl_->Init());
      init_end = true;
    }
  }
  return instance_.get();
}

Result<std::shared_ptr<CudaDevice>> CudaDeviceManager::GetDevice(int device_number) {
  return impl_->GetDevice(device_number);
}

Result<std::shared_ptr<CudaContext>> CudaDeviceManager::GetContext(int device_number) {
  return impl_->GetContext(device_number);
}

Result<std::shared_ptr<CudaContext>> CudaDeviceManager::GetSharedContext(
    int device_number, void* ctx) {
  return impl_->GetSharedContext(device_number, ctx);
}

Result<std::shared_ptr<CudaHostBuffer>> CudaDeviceManager::AllocateHost(int device_number,
                                                                        int64_t nbytes) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->AllocateHost(device_number, nbytes, &data));
  return std::make_shared<CudaHostBuffer>(data, nbytes);
}

Status CudaDeviceManager::FreeHost(void* data, int64_t nbytes) {
  return impl_->FreeHost(data, nbytes);
}

int CudaDeviceManager::num_devices() const { return impl_->num_devices(); }

// ----------------------------------------------------------------------
// CudaContext public API

CudaContext::CudaContext() { impl_.reset(new Impl()); }

CudaContext::~CudaContext() {}

Result<std::unique_ptr<CudaBuffer>> CudaContext::Allocate(int64_t nbytes) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(impl_->Allocate(nbytes, &data));
  return arrow::internal::make_unique<CudaBuffer>(data, nbytes, this->shared_from_this(),
                                                  true);
}

Result<std::shared_ptr<CudaBuffer>> CudaContext::View(uint8_t* data, int64_t nbytes) {
  return std::make_shared<CudaBuffer>(data, nbytes, this->shared_from_this(), false);
}

Result<std::shared_ptr<CudaIpcMemHandle>> CudaContext::ExportIpcBuffer(const void* data,
                                                                       int64_t size) {
  return impl_->ExportIpcBuffer(data, size);
}

Status CudaContext::CopyHostToDevice(uintptr_t dst, const void* src, int64_t nbytes) {
  return impl_->CopyHostToDevice(dst, src, nbytes);
}

Status CudaContext::CopyHostToDevice(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyHostToDevice(reinterpret_cast<uintptr_t>(dst), src, nbytes);
}

Status CudaContext::CopyDeviceToHost(void* dst, uintptr_t src, int64_t nbytes) {
  return impl_->CopyDeviceToHost(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToHost(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyDeviceToHost(dst, reinterpret_cast<uintptr_t>(src), nbytes);
}

Status CudaContext::CopyDeviceToDevice(uintptr_t dst, uintptr_t src, int64_t nbytes) {
  return impl_->CopyDeviceToDevice(dst, src, nbytes);
}

Status CudaContext::CopyDeviceToDevice(void* dst, const void* src, int64_t nbytes) {
  return impl_->CopyDeviceToDevice(reinterpret_cast<uintptr_t>(dst),
                                   reinterpret_cast<uintptr_t>(src), nbytes);
}

Status CudaContext::CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx,
                                              uintptr_t dst, uintptr_t src,
                                              int64_t nbytes) {
  return impl_->CopyDeviceToAnotherDevice(dst_ctx, dst, src, nbytes);
}

Status CudaContext::CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx,
                                              void* dst, const void* src,
                                              int64_t nbytes) {
  return impl_->CopyDeviceToAnotherDevice(dst_ctx, reinterpret_cast<uintptr_t>(dst),
                                          reinterpret_cast<uintptr_t>(src), nbytes);
}

Status CudaContext::Synchronize(void) { return impl_->Synchronize(); }

Status CudaContext::Close() { return impl_->Close(); }

Status CudaContext::Free(void* device_ptr, int64_t nbytes) {
  return impl_->Free(device_ptr, nbytes);
}

Result<std::shared_ptr<CudaBuffer>> CudaContext::OpenIpcBuffer(
    const CudaIpcMemHandle& ipc_handle) {
  if (ipc_handle.memory_size() > 0) {
    ContextSaver set_temporary(*this);
    uint8_t* data = nullptr;
    RETURN_NOT_OK(impl_->OpenIpcBuffer(ipc_handle, &data));
    // Need to ask the device how big the buffer is
    size_t allocation_size = 0;
    CU_RETURN_NOT_OK("cuMemGetAddressRange",
                     cuMemGetAddressRange(nullptr, &allocation_size,
                                          reinterpret_cast<CUdeviceptr>(data)));
    return std::make_shared<CudaBuffer>(data, allocation_size, this->shared_from_this(),
                                        true, true);
  } else {
    // zero-sized buffer does not own data (which is nullptr), hence
    // CloseIpcBuffer will not be called (see CudaBuffer::Close).
    return std::make_shared<CudaBuffer>(nullptr, 0, this->shared_from_this(), false,
                                        true);
  }
}

Status CudaContext::CloseIpcBuffer(CudaBuffer* buf) {
  ContextSaver set_temporary(*this);
  CU_RETURN_NOT_OK("cuIpcCloseMemHandle", cuIpcCloseMemHandle(buf->address()));
  return Status::OK();
}

int64_t CudaContext::bytes_allocated() const { return impl_->bytes_allocated(); }

void* CudaContext::handle() const { return impl_->context_handle(); }

std::shared_ptr<CudaDevice> CudaContext::device() const { return impl_->device(); }

std::shared_ptr<CudaMemoryManager> CudaContext::memory_manager() const {
  return impl_->memory_manager();
}

int CudaContext::device_number() const { return impl_->device()->device_number(); }

Result<uintptr_t> CudaContext::GetDeviceAddress(uintptr_t addr) {
  ContextSaver set_temporary(*this);
  CUdeviceptr ptr;
  CU_RETURN_NOT_OK("cuPointerGetAttribute",
                   cuPointerGetAttribute(&ptr, CU_POINTER_ATTRIBUTE_DEVICE_POINTER,
                                         static_cast<CUdeviceptr>(addr)));
  return static_cast<uintptr_t>(ptr);
}

Result<uintptr_t> CudaContext::GetDeviceAddress(uint8_t* addr) {
  return GetDeviceAddress(reinterpret_cast<uintptr_t>(addr));
}

}  // namespace cuda
}  // namespace arrow

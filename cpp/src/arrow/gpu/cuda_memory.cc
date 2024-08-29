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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <utility>

#include <cuda.h>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/gpu/cuda_context.h"
#include "arrow/gpu/cuda_internal.h"

namespace arrow {
namespace cuda {

using internal::ContextSaver;

// ----------------------------------------------------------------------
// CUDA IPC memory handle

struct CudaIpcMemHandle::CudaIpcMemHandleImpl {
  explicit CudaIpcMemHandleImpl(const uint8_t* handle) {
    memcpy(&memory_size, handle, sizeof(memory_size));
    if (memory_size != 0)
      memcpy(&ipc_handle, handle + sizeof(memory_size), sizeof(CUipcMemHandle));
  }

  explicit CudaIpcMemHandleImpl(int64_t memory_size, const void* cu_handle)
      : memory_size(memory_size) {
    if (memory_size != 0) {
      memcpy(&ipc_handle, cu_handle, sizeof(CUipcMemHandle));
    }
  }

  CUipcMemHandle ipc_handle;  /// initialized only when memory_size != 0
  int64_t memory_size;        /// size of the memory that ipc_handle refers to
};

CudaIpcMemHandle::CudaIpcMemHandle(const void* handle) {
  impl_.reset(new CudaIpcMemHandleImpl(reinterpret_cast<const uint8_t*>(handle)));
}

CudaIpcMemHandle::CudaIpcMemHandle(int64_t memory_size, const void* cu_handle) {
  impl_.reset(new CudaIpcMemHandleImpl(memory_size, cu_handle));
}

CudaIpcMemHandle::~CudaIpcMemHandle() {}

Result<std::shared_ptr<CudaIpcMemHandle>> CudaIpcMemHandle::FromBuffer(
    const void* opaque_handle) {
  return std::shared_ptr<CudaIpcMemHandle>(new CudaIpcMemHandle(opaque_handle));
}

Result<std::shared_ptr<Buffer>> CudaIpcMemHandle::Serialize(MemoryPool* pool) const {
  int64_t size = impl_->memory_size;
  const size_t handle_size =
      (size > 0 ? sizeof(int64_t) + sizeof(CUipcMemHandle) : sizeof(int64_t));

  ARROW_ASSIGN_OR_RAISE(auto buffer,
                        AllocateBuffer(static_cast<int64_t>(handle_size), pool));
  memcpy(buffer->mutable_data(), &impl_->memory_size, sizeof(impl_->memory_size));
  if (size > 0) {
    memcpy(buffer->mutable_data() + sizeof(impl_->memory_size), &impl_->ipc_handle,
           sizeof(impl_->ipc_handle));
  }
  return std::move(buffer);
}

const void* CudaIpcMemHandle::handle() const { return &impl_->ipc_handle; }

int64_t CudaIpcMemHandle::memory_size() const { return impl_->memory_size; }

// ----------------------------------------------------------------------

CudaBuffer::CudaBuffer(uint8_t* data, int64_t size,
                       const std::shared_ptr<CudaContext>& context, bool own_data,
                       bool is_ipc)
    : Buffer(data, size), context_(context), own_data_(own_data), is_ipc_(is_ipc) {
  is_mutable_ = true;
  SetMemoryManager(context_->memory_manager());
}

CudaBuffer::CudaBuffer(uintptr_t address, int64_t size,
                       const std::shared_ptr<CudaContext>& context, bool own_data,
                       bool is_ipc)
    : CudaBuffer(reinterpret_cast<uint8_t*>(address), size, context, own_data, is_ipc) {}

CudaBuffer::~CudaBuffer() { ARROW_CHECK_OK(Close()); }

Status CudaBuffer::Close() {
  if (own_data_) {
    if (is_ipc_) {
      return context_->CloseIpcBuffer(this);
    } else {
      return context_->Free(const_cast<uint8_t*>(data_), size_);
    }
  }
  return Status::OK();
}

CudaBuffer::CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
                       const int64_t size)
    : Buffer(parent, offset, size),
      context_(parent->context()),
      own_data_(false),
      is_ipc_(false) {
  is_mutable_ = parent->is_mutable();
}

Result<std::shared_ptr<CudaBuffer>> CudaBuffer::FromBuffer(
    std::shared_ptr<Buffer> buffer) {
  int64_t offset = 0, size = buffer->size();
  bool is_mutable = buffer->is_mutable();
  std::shared_ptr<CudaBuffer> cuda_buffer;

  // The original CudaBuffer may have been wrapped in another Buffer
  // (for example through slicing).
  // TODO check device instead
  while (!(cuda_buffer = std::dynamic_pointer_cast<CudaBuffer>(buffer))) {
    const std::shared_ptr<Buffer> parent = buffer->parent();
    if (!parent) {
      return Status::TypeError("buffer is not backed by a CudaBuffer");
    }
    offset += buffer->address() - parent->address();
    buffer = parent;
  }
  // Re-slice to represent the same memory area
  if (offset != 0 || cuda_buffer->size() != size || !is_mutable) {
    cuda_buffer = std::make_shared<CudaBuffer>(std::move(cuda_buffer), offset, size);
    cuda_buffer->is_mutable_ = is_mutable;
  }
  return cuda_buffer;
}

Status CudaBuffer::CopyToHost(const int64_t position, const int64_t nbytes,
                              void* out) const {
  return context_->CopyDeviceToHost(out, data_ + position, nbytes);
}

Status CudaBuffer::CopyFromHost(const int64_t position, const void* data,
                                int64_t nbytes) {
  if (nbytes > size_ - position) {
    return Status::Invalid("Copy would overflow buffer");
  }
  return context_->CopyHostToDevice(const_cast<uint8_t*>(data_) + position, data, nbytes);
}

Status CudaBuffer::CopyFromDevice(const int64_t position, const void* data,
                                  int64_t nbytes) {
  if (nbytes > size_ - position) {
    return Status::Invalid("Copy would overflow buffer");
  }
  return context_->CopyDeviceToDevice(const_cast<uint8_t*>(data_) + position, data,
                                      nbytes);
}

Status CudaBuffer::CopyFromAnotherDevice(const std::shared_ptr<CudaContext>& src_ctx,
                                         const int64_t position, const void* data,
                                         int64_t nbytes) {
  if (nbytes > size_ - position) {
    return Status::Invalid("Copy would overflow buffer");
  }
  return src_ctx->CopyDeviceToAnotherDevice(
      context_, const_cast<uint8_t*>(data_) + position, data, nbytes);
}

Result<std::shared_ptr<CudaIpcMemHandle>> CudaBuffer::ExportForIpc() {
  if (is_ipc_) {
    return Status::Invalid("Buffer has already been exported for IPC");
  }
  ARROW_ASSIGN_OR_RAISE(auto handle, context_->ExportIpcBuffer(data_, size_));
  own_data_ = false;
  return handle;
}

CudaHostBuffer::CudaHostBuffer(uint8_t* data, const int64_t size)
    : MutableBuffer(data, size) {
  device_type_ = DeviceAllocationType::kCUDA_HOST;
}

CudaHostBuffer::~CudaHostBuffer() {
  auto maybe_manager = CudaDeviceManager::Instance();
  ARROW_CHECK_OK(maybe_manager.status());
  ARROW_CHECK_OK((*maybe_manager)->FreeHost(const_cast<uint8_t*>(data_), size_));
}

Result<uintptr_t> CudaHostBuffer::GetDeviceAddress(
    const std::shared_ptr<CudaContext>& ctx) {
  return ::arrow::cuda::GetDeviceAddress(data(), ctx);
}

// ----------------------------------------------------------------------
// CudaBufferReader

CudaBufferReader::CudaBufferReader(const std::shared_ptr<Buffer>& buffer)
    : address_(buffer->address()), size_(buffer->size()), position_(0), is_open_(true) {
  auto maybe_buffer = CudaBuffer::FromBuffer(buffer);
  if (ARROW_PREDICT_FALSE(!maybe_buffer.ok())) {
    throw std::bad_cast();
  }
  buffer_ = *std::move(maybe_buffer);
  context_ = buffer_->context();
}

Status CudaBufferReader::DoClose() {
  is_open_ = false;
  return Status::OK();
}

bool CudaBufferReader::closed() const { return !is_open_; }

// XXX Only in a certain sense (not on the CPU)...
bool CudaBufferReader::supports_zero_copy() const { return true; }

Result<int64_t> CudaBufferReader::DoTell() const {
  RETURN_NOT_OK(CheckClosed());
  return position_;
}

Result<int64_t> CudaBufferReader::DoGetSize() {
  RETURN_NOT_OK(CheckClosed());
  return size_;
}

Status CudaBufferReader::DoSeek(int64_t position) {
  RETURN_NOT_OK(CheckClosed());

  if (position < 0 || position > size_) {
    return Status::IOError("Seek out of bounds");
  }

  position_ = position;
  return Status::OK();
}

Result<int64_t> CudaBufferReader::DoReadAt(int64_t position, int64_t nbytes,
                                           void* buffer) {
  RETURN_NOT_OK(CheckClosed());

  nbytes = std::min(nbytes, size_ - position);
  RETURN_NOT_OK(context_->CopyDeviceToHost(buffer, address_ + position, nbytes));
  return nbytes;
}

Result<int64_t> CudaBufferReader::DoRead(int64_t nbytes, void* buffer) {
  RETURN_NOT_OK(CheckClosed());

  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, DoReadAt(position_, nbytes, buffer));
  position_ += bytes_read;
  return bytes_read;
}

Result<std::shared_ptr<Buffer>> CudaBufferReader::DoReadAt(int64_t position,
                                                           int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());

  int64_t size = std::min(nbytes, size_ - position);
  return std::make_shared<CudaBuffer>(buffer_, position, size);
}

Result<std::shared_ptr<Buffer>> CudaBufferReader::DoRead(int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());

  int64_t size = std::min(nbytes, size_ - position_);
  auto buffer = std::make_shared<CudaBuffer>(buffer_, position_, size);
  position_ += size;
  return buffer;
}

// ----------------------------------------------------------------------
// CudaBufferWriter

class CudaBufferWriter::CudaBufferWriterImpl {
 public:
  explicit CudaBufferWriterImpl(const std::shared_ptr<CudaBuffer>& buffer)
      : context_(buffer->context()),
        buffer_(buffer),
        buffer_size_(0),
        buffer_position_(0) {
    buffer_ = buffer;
    ARROW_CHECK(buffer->is_mutable()) << "Must pass mutable buffer";
    address_ = buffer->mutable_address();
    size_ = buffer->size();
    position_ = 0;
    closed_ = false;
  }

#define CHECK_CLOSED()                                              \
  if (closed_) {                                                    \
    return Status::Invalid("Operation on closed CudaBufferWriter"); \
  }

  Status Seek(int64_t position) {
    CHECK_CLOSED();
    if (position < 0 || position >= size_) {
      return Status::IOError("position out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  Status Close() {
    if (!closed_) {
      closed_ = true;
      RETURN_NOT_OK(FlushInternal());
    }
    return Status::OK();
  }

  Status Flush() {
    CHECK_CLOSED();
    return FlushInternal();
  }

  Status FlushInternal() {
    if (buffer_size_ > 0 && buffer_position_ > 0) {
      // Only need to flush when the write has been buffered
      RETURN_NOT_OK(context_->CopyHostToDevice(address_ + position_ - buffer_position_,
                                               host_buffer_data_, buffer_position_));
      buffer_position_ = 0;
    }
    return Status::OK();
  }

  bool closed() const { return closed_; }

  Result<int64_t> Tell() const {
    CHECK_CLOSED();
    return position_;
  }

  Status Write(const void* data, int64_t nbytes) {
    CHECK_CLOSED();
    if (nbytes == 0) {
      return Status::OK();
    }

    if (buffer_size_ > 0) {
      if (nbytes + buffer_position_ >= buffer_size_) {
        // Reach end of buffer, write everything
        RETURN_NOT_OK(Flush());
        RETURN_NOT_OK(context_->CopyHostToDevice(address_ + position_, data, nbytes));
      } else {
        // Write bytes to buffer
        std::memcpy(host_buffer_data_ + buffer_position_, data, nbytes);
        buffer_position_ += nbytes;
      }
    } else {
      // Unbuffered write
      RETURN_NOT_OK(context_->CopyHostToDevice(address_ + position_, data, nbytes));
    }
    position_ += nbytes;
    return Status::OK();
  }

  Status WriteAt(int64_t position, const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    CHECK_CLOSED();
    RETURN_NOT_OK(Seek(position));
    return Write(data, nbytes);
  }

  Status SetBufferSize(const int64_t buffer_size) {
    CHECK_CLOSED();
    if (buffer_position_ > 0) {
      // Flush any buffered data
      RETURN_NOT_OK(Flush());
    }
    ARROW_ASSIGN_OR_RAISE(
        host_buffer_,
        AllocateCudaHostBuffer(context_.get()->device_number(), buffer_size));
    host_buffer_data_ = host_buffer_->mutable_data();
    buffer_size_ = buffer_size;
    return Status::OK();
  }

  int64_t buffer_size() const { return buffer_size_; }

  int64_t buffer_position() const { return buffer_position_; }

#undef CHECK_CLOSED

 private:
  std::shared_ptr<CudaContext> context_;
  std::shared_ptr<CudaBuffer> buffer_;
  std::mutex lock_;
  uintptr_t address_;
  int64_t size_;
  int64_t position_;
  bool closed_;

  // Pinned host buffer for buffering writes on CPU before calling cudaMalloc
  int64_t buffer_size_;
  int64_t buffer_position_;
  std::shared_ptr<CudaHostBuffer> host_buffer_;
  uint8_t* host_buffer_data_;
};

CudaBufferWriter::CudaBufferWriter(const std::shared_ptr<CudaBuffer>& buffer) {
  impl_.reset(new CudaBufferWriterImpl(buffer));
}

CudaBufferWriter::~CudaBufferWriter() {}

Status CudaBufferWriter::Close() { return impl_->Close(); }

bool CudaBufferWriter::closed() const { return impl_->closed(); }

Status CudaBufferWriter::Flush() { return impl_->Flush(); }

Status CudaBufferWriter::Seek(int64_t position) {
  if (impl_->buffer_position() > 0) {
    RETURN_NOT_OK(Flush());
  }
  return impl_->Seek(position);
}

Result<int64_t> CudaBufferWriter::Tell() const { return impl_->Tell(); }

Status CudaBufferWriter::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status CudaBufferWriter::WriteAt(int64_t position, const void* data, int64_t nbytes) {
  return impl_->WriteAt(position, data, nbytes);
}

Status CudaBufferWriter::SetBufferSize(const int64_t buffer_size) {
  return impl_->SetBufferSize(buffer_size);
}

int64_t CudaBufferWriter::buffer_size() const { return impl_->buffer_size(); }

int64_t CudaBufferWriter::num_bytes_buffered() const { return impl_->buffer_position(); }

// ----------------------------------------------------------------------

Result<std::shared_ptr<CudaHostBuffer>> AllocateCudaHostBuffer(int device_number,
                                                               const int64_t size) {
  ARROW_ASSIGN_OR_RAISE(auto manager, CudaDeviceManager::Instance());
  return manager->AllocateHost(device_number, size);
}

Result<uintptr_t> GetDeviceAddress(const uint8_t* cpu_data,
                                   const std::shared_ptr<CudaContext>& ctx) {
  ContextSaver context_saver(*ctx);
  CUdeviceptr ptr;
  // XXX should we use cuPointerGetAttribute(CU_POINTER_ATTRIBUTE_DEVICE_POINTER)
  // instead?
  CU_RETURN_NOT_OK("cuMemHostGetDevicePointer",
                   cuMemHostGetDevicePointer(&ptr, const_cast<uint8_t*>(cpu_data), 0));
  return static_cast<uintptr_t>(ptr);
}

Result<uint8_t*> GetHostAddress(uintptr_t device_ptr) {
  void* ptr;
  CU_RETURN_NOT_OK(
      "cuPointerGetAttribute",
      cuPointerGetAttribute(&ptr, CU_POINTER_ATTRIBUTE_HOST_POINTER, device_ptr));
  return static_cast<uint8_t*>(ptr);
}

Result<std::shared_ptr<MemoryManager>> DefaultMemoryMapper(ArrowDeviceType device_type,
                                                           int64_t device_id) {
  switch (device_type) {
    case ARROW_DEVICE_CPU:
      return default_cpu_memory_manager();
    case ARROW_DEVICE_CUDA:
    case ARROW_DEVICE_CUDA_HOST:
    case ARROW_DEVICE_CUDA_MANAGED: {
      ARROW_ASSIGN_OR_RAISE(auto device, arrow::cuda::CudaDevice::Make(device_id));
      return device->default_memory_manager();
    }
    default:
      return Status::NotImplemented("memory manager not implemented for device");
  }
}

}  // namespace cuda
}  // namespace arrow

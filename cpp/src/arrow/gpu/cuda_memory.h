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

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/c/abi.h"
#include "arrow/io/concurrency.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace cuda {

class CudaContext;
class CudaIpcMemHandle;

/// \class CudaBuffer
/// \brief An Arrow buffer located on a GPU device
///
/// Be careful using this in any Arrow code which may not be GPU-aware
class ARROW_EXPORT CudaBuffer : public Buffer {
 public:
  // XXX deprecate?
  CudaBuffer(uint8_t* data, int64_t size, const std::shared_ptr<CudaContext>& context,
             bool own_data = false, bool is_ipc = false);

  CudaBuffer(uintptr_t address, int64_t size, const std::shared_ptr<CudaContext>& context,
             bool own_data = false, bool is_ipc = false);

  CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
             const int64_t size);

  ~CudaBuffer();

  /// \brief Convert back generic buffer into CudaBuffer
  /// \param[in] buffer buffer to convert
  /// \return CudaBuffer or Status
  ///
  /// \note This function returns an error if the buffer isn't backed
  /// by GPU memory
  static Result<std::shared_ptr<CudaBuffer>> FromBuffer(std::shared_ptr<Buffer> buffer);

  /// \brief Copy memory from GPU device to CPU host
  /// \param[in] position start position inside buffer to copy bytes from
  /// \param[in] nbytes number of bytes to copy
  /// \param[out] out start address of the host memory area to copy to
  /// \return Status
  Status CopyToHost(const int64_t position, const int64_t nbytes, void* out) const;

  /// \brief Copy memory to device at position
  /// \param[in] position start position to copy bytes to
  /// \param[in] data the host data to copy
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  Status CopyFromHost(const int64_t position, const void* data, int64_t nbytes);

  /// \brief Copy memory from device to device at position
  /// \param[in] position start position inside buffer to copy bytes to
  /// \param[in] data start address of the device memory area to copy from
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  ///
  /// \note It is assumed that both source and destination device
  /// memories have been allocated within the same context.
  Status CopyFromDevice(const int64_t position, const void* data, int64_t nbytes);

  /// \brief Copy memory from another device to device at position
  /// \param[in] src_ctx context of the source device memory
  /// \param[in] position start position inside buffer to copy bytes to
  /// \param[in] data start address of the another device memory area to copy from
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  Status CopyFromAnotherDevice(const std::shared_ptr<CudaContext>& src_ctx,
                               const int64_t position, const void* data, int64_t nbytes);

  /// \brief Expose this device buffer as IPC memory which can be used in other processes
  /// \return Handle or Status
  ///
  /// \note After calling this function, this device memory will not be freed
  /// when the CudaBuffer is destructed
  virtual Result<std::shared_ptr<CudaIpcMemHandle>> ExportForIpc();

  const std::shared_ptr<CudaContext>& context() const { return context_; }

 protected:
  std::shared_ptr<CudaContext> context_;
  bool own_data_;
  bool is_ipc_;

  virtual Status Close();
};

/// \class CudaHostBuffer
/// \brief Device-accessible CPU memory created using cudaHostAlloc
class ARROW_EXPORT CudaHostBuffer : public MutableBuffer {
 public:
  CudaHostBuffer(uint8_t* data, const int64_t size);

  ~CudaHostBuffer();

  /// \brief Return a device address the GPU can read this memory from.
  Result<uintptr_t> GetDeviceAddress(const std::shared_ptr<CudaContext>& ctx);
};

/// \class CudaIpcHandle
/// \brief A container for a CUDA IPC handle
class ARROW_EXPORT CudaIpcMemHandle {
 public:
  ~CudaIpcMemHandle();

  /// \brief Create CudaIpcMemHandle from opaque buffer (e.g. from another process)
  /// \param[in] opaque_handle a CUipcMemHandle as a const void*
  /// \return Handle or Status
  static Result<std::shared_ptr<CudaIpcMemHandle>> FromBuffer(const void* opaque_handle);

  /// \brief Write CudaIpcMemHandle to a Buffer
  /// \param[in] pool a MemoryPool to allocate memory from
  /// \return Buffer or Status
  Result<std::shared_ptr<Buffer>> Serialize(
      MemoryPool* pool = default_memory_pool()) const;

 private:
  explicit CudaIpcMemHandle(const void* handle);
  CudaIpcMemHandle(int64_t memory_size, const void* cu_handle);

  struct CudaIpcMemHandleImpl;
  std::unique_ptr<CudaIpcMemHandleImpl> impl_;

  const void* handle() const;
  int64_t memory_size() const;

  friend CudaBuffer;
  friend CudaContext;
};

/// \class CudaBufferReader
/// \brief File interface for zero-copy read from CUDA buffers
///
/// CAUTION: reading to a Buffer returns a Buffer pointing to device memory.
/// It will generally not be compatible with Arrow code expecting a buffer
/// pointing to CPU memory.
/// Reading to a raw pointer, though, copies device memory into the host
/// memory pointed to.
class ARROW_EXPORT CudaBufferReader
    : public ::arrow::io::internal::RandomAccessFileConcurrencyWrapper<CudaBufferReader> {
 public:
  explicit CudaBufferReader(const std::shared_ptr<Buffer>& buffer);

  bool closed() const override;

  bool supports_zero_copy() const override;

  std::shared_ptr<CudaBuffer> buffer() const { return buffer_; }

 protected:
  friend ::arrow::io::internal::RandomAccessFileConcurrencyWrapper<CudaBufferReader>;

  Status DoClose();

  Result<int64_t> DoRead(int64_t nbytes, void* buffer);
  Result<std::shared_ptr<Buffer>> DoRead(int64_t nbytes);
  Result<int64_t> DoReadAt(int64_t position, int64_t nbytes, void* out);
  Result<std::shared_ptr<Buffer>> DoReadAt(int64_t position, int64_t nbytes);

  Result<int64_t> DoTell() const;
  Status DoSeek(int64_t position);
  Result<int64_t> DoGetSize();

  Status CheckClosed() const {
    if (!is_open_) {
      return Status::Invalid("Operation forbidden on closed CudaBufferReader");
    }
    return Status::OK();
  }

  std::shared_ptr<CudaBuffer> buffer_;
  std::shared_ptr<CudaContext> context_;
  const uintptr_t address_;
  int64_t size_;
  int64_t position_;
  bool is_open_;
};

/// \class CudaBufferWriter
/// \brief File interface for writing to CUDA buffers, with optional buffering
class ARROW_EXPORT CudaBufferWriter : public io::WritableFile {
 public:
  explicit CudaBufferWriter(const std::shared_ptr<CudaBuffer>& buffer);
  ~CudaBufferWriter() override;

  /// \brief Close writer and flush buffered bytes to GPU
  Status Close() override;

  bool closed() const override;

  /// \brief Flush buffered bytes to GPU
  Status Flush() override;

  Status Seek(int64_t position) override;

  Status Write(const void* data, int64_t nbytes) override;

  Status WriteAt(int64_t position, const void* data, int64_t nbytes) override;

  Result<int64_t> Tell() const override;

  /// \brief Set CPU buffer size to limit calls to cudaMemcpy
  /// \param[in] buffer_size the size of CPU buffer to allocate
  /// \return Status
  ///
  /// By default writes are unbuffered
  Status SetBufferSize(const int64_t buffer_size);

  /// \brief Returns size of host (CPU) buffer, 0 for unbuffered
  int64_t buffer_size() const;

  /// \brief Returns number of bytes buffered on host
  int64_t num_bytes_buffered() const;

 private:
  class CudaBufferWriterImpl;
  std::unique_ptr<CudaBufferWriterImpl> impl_;
};

/// \brief Allocate CUDA-accessible memory on CPU host
///
/// The GPU will benefit from fast access to this CPU-located buffer,
/// including fast memory copy.
///
/// \param[in] device_number device to expose host memory
/// \param[in] size number of bytes
/// \return Host buffer or Status
ARROW_EXPORT
Result<std::shared_ptr<CudaHostBuffer>> AllocateCudaHostBuffer(int device_number,
                                                               const int64_t size);

/// Low-level: get a device address through which the CPU data be accessed.
ARROW_EXPORT
Result<uintptr_t> GetDeviceAddress(const uint8_t* cpu_data,
                                   const std::shared_ptr<CudaContext>& ctx);

/// Low-level: get a CPU address through which the device data be accessed.
ARROW_EXPORT
Result<uint8_t*> GetHostAddress(uintptr_t device_ptr);

ARROW_EXPORT
Result<std::shared_ptr<MemoryManager>> DefaultMemoryMapper(ArrowDeviceType device_type,
                                                           int64_t device_id);

}  // namespace cuda
}  // namespace arrow

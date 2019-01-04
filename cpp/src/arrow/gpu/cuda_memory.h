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
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"

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
  CudaBuffer(uint8_t* data, int64_t size, const std::shared_ptr<CudaContext>& context,
             bool own_data = false, bool is_ipc = false);

  CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
             const int64_t size);

  ~CudaBuffer();

  /// \brief Convert back generic buffer into CudaBuffer
  /// \param[in] buffer buffer to convert
  /// \param[out] out conversion result
  /// \return Status
  ///
  /// \note This function returns an error if the buffer isn't backed
  /// by GPU memory
  static Status FromBuffer(std::shared_ptr<Buffer> buffer,
                           std::shared_ptr<CudaBuffer>* out);

  /// \brief Copy memory from GPU device to CPU host
  /// \param[out] out a pre-allocated output buffer
  /// \return Status
  Status CopyToHost(const int64_t position, const int64_t nbytes, void* out) const;

  /// \brief Copy memory to device at position
  /// \param[in] position start position to copy bytes
  /// \param[in] data the host data to copy
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  Status CopyFromHost(const int64_t position, const void* data, int64_t nbytes);

  /// \brief Copy memory from device to device at position
  /// \param[in] position start position to copy bytes
  /// \param[in] data the device data to copy
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  ///
  /// \note It is assumed that both source and destination device
  /// memories have been allocated within the same context.
  Status CopyFromDevice(const int64_t position, const void* data, int64_t nbytes);

  /// \brief Expose this device buffer as IPC memory which can be used in other processes
  /// \param[out] handle the exported IPC handle
  /// \return Status
  ///
  /// \note After calling this function, this device memory will not be freed
  /// when the CudaBuffer is destructed
  virtual Status ExportForIpc(std::shared_ptr<CudaIpcMemHandle>* handle);

  std::shared_ptr<CudaContext> context() const { return context_; }

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
  using MutableBuffer::MutableBuffer;
  ~CudaHostBuffer();
};

/// \class CudaIpcHandle
/// \brief A container for a CUDA IPC handle
class ARROW_EXPORT CudaIpcMemHandle {
 public:
  ~CudaIpcMemHandle();

  /// \brief Create CudaIpcMemHandle from opaque buffer (e.g. from another process)
  /// \param[in] opaque_handle a CUipcMemHandle as a const void*
  /// \param[out] handle the CudaIpcMemHandle instance
  /// \return Status
  static Status FromBuffer(const void* opaque_handle,
                           std::shared_ptr<CudaIpcMemHandle>* handle);

  /// \brief Write CudaIpcMemHandle to a Buffer
  /// \param[in] pool a MemoryPool to allocate memory from
  /// \param[out] out the serialized buffer
  /// \return Status
  Status Serialize(MemoryPool* pool, std::shared_ptr<Buffer>* out) const;

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
/// Note: Reads return pointers to device memory. This means you must be
/// careful using this interface with any Arrow code which may expect to be
/// able to do anything other than pointer arithmetic on the returned buffers
class ARROW_EXPORT CudaBufferReader : public io::BufferReader {
 public:
  explicit CudaBufferReader(const std::shared_ptr<Buffer>& buffer);
  ~CudaBufferReader() override;

  /// \brief Read bytes into pre-allocated host memory
  /// \param[in] nbytes number of bytes to read
  /// \param[out] bytes_read actual number of bytes read
  /// \param[out] buffer pre-allocated memory to write into
  Status Read(int64_t nbytes, int64_t* bytes_read, void* buffer) override;

  /// \brief Zero-copy read from device memory
  /// \param[in] nbytes number of bytes to read
  /// \param[out] out a Buffer referencing device memory
  /// \return Status
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

 private:
  std::shared_ptr<CudaBuffer> cuda_buffer_;
  std::shared_ptr<CudaContext> context_;
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

  Status Tell(int64_t* position) const override;

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
/// \param[in] device_number device to expose host memory
/// \param[in] size number of bytes
/// \param[out] out the allocated buffer
/// \return Status
ARROW_EXPORT
Status AllocateCudaHostBuffer(int device_number, const int64_t size,
                              std::shared_ptr<CudaHostBuffer>* out);

}  // namespace cuda
}  // namespace arrow

#endif  // ARROW_GPU_CUDA_MEMORY_H

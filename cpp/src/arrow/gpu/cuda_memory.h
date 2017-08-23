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
namespace gpu {

/// \class CudaBuffer
/// \brief An Arrow buffer located on a GPU device
///
/// Be careful using this in any Arrow code which may not be GPU-aware
class ARROW_EXPORT CudaBuffer : public Buffer {
 public:
  CudaBuffer(uint8_t* data, int64_t size, const int gpu_number, bool own_data = false)
      : Buffer(data, size), gpu_number_(gpu_number), own_data_(own_data) {
    is_mutable_ = true;
    mutable_data_ = data;
  }

  CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
             const int64_t size);

  ~CudaBuffer();

  /// \brief Copy memory from GPU device to CPU host
  /// \param[out] out a pre-allocated output buffer
  /// \return Status
  Status CopyToHost(const int64_t position, const int64_t nbytes, uint8_t* out) const;

  /// \brief Copy memory to device at position
  /// \param[in] position start position to copy bytes
  /// \param[in] data the host data to copy
  /// \param[in] nbytes number of bytes to copy
  /// \return Status
  Status CopyFromHost(const int64_t position, const uint8_t* data, int64_t nbytes);

  int gpu_number() const { return gpu_number_; }

 private:
  const int gpu_number_;
  bool own_data_;
};

/// \class CudaHostBuffer
/// \brief Device-accessible CPU memory created using cudaHostAlloc
class ARROW_EXPORT CudaHostBuffer : public MutableBuffer {
 public:
  using MutableBuffer::MutableBuffer;
  ~CudaHostBuffer();
};

/// \class CudaBufferReader
/// \brief File interface for zero-copy read from CUDA buffers
///
/// Note: Reads return pointers to device memory. This means you must be
/// careful using this interface with any Arrow code which may expect to be
/// able to do anything other than pointer arithmetic on the returned buffers
class ARROW_EXPORT CudaBufferReader : public io::BufferReader {
 public:
  explicit CudaBufferReader(const std::shared_ptr<CudaBuffer>& buffer);
  ~CudaBufferReader();

  /// \brief Read bytes into pre-allocated host memory
  /// \param[in] nbytes number of bytes to read
  /// \param[out] bytes_read actual number of bytes read
  /// \param[out] buffer pre-allocated memory to write into
  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;

  /// \brief Zero-copy read from device memory
  /// \param[in] nbytes number of bytes to read
  /// \param[out] out a Buffer referencing device memory
  /// \return Status
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

 private:
  // In case we need to access anything GPU-specific, like device number
  std::shared_ptr<CudaBuffer> cuda_buffer_;
};

/// \class CudaBufferWriter
/// \brief File interface for writing to CUDA buffers, with optional buffering
class ARROW_EXPORT CudaBufferWriter : public io::FixedSizeBufferWriter {
 public:
  explicit CudaBufferWriter(const std::shared_ptr<CudaBuffer>& buffer);
  ~CudaBufferWriter();

  /// \brief Close writer and flush buffered bytes to GPU
  Status Close() override;

  /// \brief Flush buffered bytes to GPU
  Status Flush() override;

  // Seek requires flushing if any bytes are buffered
  Status Seek(int64_t position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;

  /// \brief Set CPU buffer size to limit calls to cudaMemcpy
  /// \param[in] buffer_size the size of CPU buffer to allocate
  /// \return Status
  ///
  /// By default writes are unbuffered
  Status SetBufferSize(const int64_t buffer_size);

  /// \brief Returns size of host (CPU) buffer, 0 for unbuffered
  int64_t buffer_size() const { return buffer_size_; }

  /// \brief Returns number of bytes buffered on host
  int64_t num_bytes_buffered() const { return buffer_position_; }

 private:
  // Pinned host buffer for buffering writes on CPU before calling cudaMalloc
  int64_t buffer_size_;
  int64_t buffer_position_;
  std::shared_ptr<CudaHostBuffer> host_buffer_;
  uint8_t* host_buffer_data_;
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

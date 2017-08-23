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
#include <memory>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/gpu/cuda_common.h"

namespace arrow {
namespace gpu {

CudaBuffer::~CudaBuffer() {
  if (own_data_) {
    CUDA_DCHECK(cudaFree(mutable_data_));
  }
}

CudaBuffer::CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
                       const int64_t size)
    : Buffer(parent, offset, size), gpu_number_(parent->gpu_number()) {}

Status CudaBuffer::CopyToHost(const int64_t position, const int64_t nbytes,
                              uint8_t* out) const {
  CUDA_RETURN_NOT_OK(cudaMemcpy(out, data_ + position, nbytes, cudaMemcpyDeviceToHost));
  return Status::OK();
}

Status CudaBuffer::CopyFromHost(const int64_t position, const uint8_t* data,
                                int64_t nbytes) {
  DCHECK_LE(nbytes, size_ - position) << "Copy would overflow buffer";
  CUDA_RETURN_NOT_OK(
      cudaMemcpy(mutable_data_ + position, data, nbytes, cudaMemcpyHostToDevice));
  return Status::OK();
}

Status AllocateCudaBuffer(int gpu_number, const int64_t size,
                          std::shared_ptr<CudaBuffer>* out) {
  CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number));
  uint8_t* data = nullptr;
  CUDA_RETURN_NOT_OK(
      cudaMalloc(reinterpret_cast<void**>(&data), static_cast<size_t>(size)));
  *out = std::make_shared<CudaBuffer>(data, size, gpu_number, true);
  return Status::OK();
}

CudaHostBuffer::~CudaHostBuffer() { CUDA_DCHECK(cudaFreeHost(mutable_data_)); }

// ----------------------------------------------------------------------
// CudaBufferReader

CudaBufferReader::CudaBufferReader(const std::shared_ptr<CudaBuffer>& buffer)
    : io::BufferReader(buffer), cuda_buffer_(buffer), gpu_number_(buffer->gpu_number()) {}

CudaBufferReader::~CudaBufferReader() {}

Status CudaBufferReader::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  nbytes = std::min(nbytes, size_ - position_);
  CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number_));
  CUDA_RETURN_NOT_OK(
      cudaMemcpy(buffer, data_ + position_, nbytes, cudaMemcpyDeviceToHost));
  *bytes_read = nbytes;
  position_ += nbytes;
  return Status::OK();
}

Status CudaBufferReader::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  int64_t size = std::min(nbytes, size_ - position_);
  *out = std::make_shared<CudaBuffer>(cuda_buffer_, position_, size);
  position_ += size;
  return Status::OK();
}

// ----------------------------------------------------------------------
// CudaBufferWriter

CudaBufferWriter::CudaBufferWriter(const std::shared_ptr<CudaBuffer>& buffer)
    : io::FixedSizeBufferWriter(buffer),
      gpu_number_(buffer->gpu_number()),
      buffer_size_(0),
      buffer_position_(0) {}

CudaBufferWriter::~CudaBufferWriter() {}

Status CudaBufferWriter::Close() { return Flush(); }

Status CudaBufferWriter::Flush() {
  if (buffer_size_ > 0 && buffer_position_ > 0) {
    // Only need to flush when the write has been buffered
    CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number_));
    CUDA_RETURN_NOT_OK(cudaMemcpy(mutable_data_ + position_ - buffer_position_,
                                  host_buffer_data_, buffer_position_,
                                  cudaMemcpyHostToDevice));
    buffer_position_ = 0;
  }
  return Status::OK();
}

Status CudaBufferWriter::Seek(int64_t position) {
  if (buffer_position_ > 0) {
    RETURN_NOT_OK(Flush());
  }
  return io::FixedSizeBufferWriter::Seek(position);
}

Status CudaBufferWriter::Write(const uint8_t* data, int64_t nbytes) {
  if (memcopy_num_threads_ > 1) {
    return Status::Invalid("parallel CUDA memcpy not supported");
  }

  if (nbytes == 0) {
    return Status::OK();
  }

  if (buffer_size_ > 0) {
    if (nbytes + buffer_position_ >= buffer_size_) {
      // Reach end of buffer, write everything
      RETURN_NOT_OK(Flush());
      CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number_));
      CUDA_RETURN_NOT_OK(
          cudaMemcpy(mutable_data_ + position_, data, nbytes, cudaMemcpyHostToDevice));
    } else {
      // Write bytes to buffer
      std::memcpy(host_buffer_data_ + buffer_position_, data, nbytes);
      buffer_position_ += nbytes;
    }
  } else {
    // Unbuffered write
    CUDA_RETURN_NOT_OK(cudaSetDevice(gpu_number_));
    CUDA_RETURN_NOT_OK(
        cudaMemcpy(mutable_data_ + position_, data, nbytes, cudaMemcpyHostToDevice));
  }
  position_ += nbytes;
  return Status::OK();
}

Status CudaBufferWriter::SetBufferSize(const int64_t buffer_size) {
  if (buffer_position_ > 0) {
    // Flush any buffered data
    RETURN_NOT_OK(Flush());
  }
  RETURN_NOT_OK(AllocateCudaHostBuffer(buffer_size, &host_buffer_));
  host_buffer_data_ = host_buffer_->mutable_data();
  buffer_size_ = buffer_size;
  return Status::OK();
}

// ----------------------------------------------------------------------

Status AllocateCudaHostBuffer(const int64_t size, std::shared_ptr<CudaHostBuffer>* out) {
  uint8_t* data = nullptr;
  CUDA_RETURN_NOT_OK(
      cudaMallocHost(reinterpret_cast<void**>(&data), static_cast<size_t>(size)));
  *out = std::make_shared<CudaHostBuffer>(data, size);
  return Status::OK();
}

}  // namespace gpu
}  // namespace arrow

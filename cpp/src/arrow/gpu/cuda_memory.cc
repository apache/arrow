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
#include "arrow/gpu/cuda_context.h"

namespace arrow {
namespace gpu {

CudaBuffer::~CudaBuffer() {
  if (own_data_) {
    DCHECK(context_->Free(mutable_data_, size_).ok());
  }
}

CudaBuffer::CudaBuffer(const std::shared_ptr<CudaBuffer>& parent, const int64_t offset,
                       const int64_t size)
    : Buffer(parent, offset, size),
      context_(parent->context()) {}

Status CudaBuffer::CopyToHost(const int64_t position, const int64_t nbytes,
                              uint8_t* out) const {
  return context_->CopyDeviceToHost(out, data_ + position, nbytes);
}

Status CudaBuffer::CopyFromHost(const int64_t position, const uint8_t* data,
                                int64_t nbytes) {
  DCHECK_LE(nbytes, size_ - position) << "Copy would overflow buffer";
  return context_->CopyHostToDevice(mutable_data_ + position, data, nbytes);
}

Status AllocateCudaBuffer(const int64_t size,
                          const std::shared_ptr<CudaContext>& context,
                          std::shared_ptr<CudaBuffer>* out) {
  uint8_t* data = nullptr;
  RETURN_NOT_OK(context->Allocate(size, &data));
  *out = std::make_shared<CudaBuffer>(data, size, context);
  return Status::OK();
}

CudaHostBuffer::~CudaHostBuffer() {
  CudaDeviceManager* manager = nullptr;
  DCHECK(CudaDeviceManager::GetInstance(&manager).ok());
  DCHECK(manager->FreeHost(mutable_data_, size_).ok());
}

// ----------------------------------------------------------------------
// CudaBufferReader

CudaBufferReader::CudaBufferReader(const std::shared_ptr<CudaBuffer>& buffer)
    : io::BufferReader(buffer),
      cuda_buffer_(buffer),
      context_(buffer->context()) {}

CudaBufferReader::~CudaBufferReader() {}

Status CudaBufferReader::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  nbytes = std::min(nbytes, size_ - position_);
  *bytes_read = nbytes;
  RETURN_NOT_OK(context_->CopyDeviceToHost(buffer, data_ + position_, nbytes));
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
      context_(buffer->context()),
      buffer_size_(0),
      buffer_position_(0) {}

CudaBufferWriter::~CudaBufferWriter() {}

Status CudaBufferWriter::Close() { return Flush(); }

Status CudaBufferWriter::Flush() {
  if (buffer_size_ > 0 && buffer_position_ > 0) {
    // Only need to flush when the write has been buffered
    RETURN_NOT_OK(context_->CopyHostToDevice(mutable_data_ + position_ -
                                             buffer_position_,
                                             host_buffer_data_,
                                             buffer_position_));
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
      RETURN_NOT_OK(context_->CopyHostToDevice(mutable_data_ + position_,
                                               data, nbytes));
    } else {
      // Write bytes to buffer
      std::memcpy(host_buffer_data_ + buffer_position_, data, nbytes);
      buffer_position_ += nbytes;
    }
  } else {
    // Unbuffered write
    RETURN_NOT_OK(context_->CopyHostToDevice(mutable_data_ + position_,
                                             data, nbytes));
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
  CudaDeviceManager* manager = nullptr;
  RETURN_NOT_OK(CudaDeviceManager::GetInstance(&manager));
  return manager->AllocateHost(size, out);
}

}  // namespace gpu
}  // namespace arrow

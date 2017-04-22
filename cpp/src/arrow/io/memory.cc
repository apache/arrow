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

#include "arrow/io/memory.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

void SerialMemcopy::memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) {
  std::memcpy(dst, src, nbytes);
}

void ParallelMemcopy::memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) {
  if (nbytes >= BYTES_IN_MB) {
    memcopy_aligned(dst, src, nbytes, block_size_);
  } else {
    memcpy(dst, src, nbytes);
  }
}

void ParallelMemcopy::memcopy_aligned(
    uint8_t* dst, const uint8_t* src, uint64_t nbytes, uint64_t block_size) {
  uint64_t num_threads = threadpool_.size();
  uint64_t src_address = reinterpret_cast<uint64_t>(src);
  uint64_t left_address = (src_address + block_size - 1) & ~(block_size - 1);
  uint64_t right_address = (src_address + nbytes) & ~(block_size - 1);
  uint64_t num_blocks = (right_address - left_address) / block_size;
  // Update right address
  right_address = right_address - (num_blocks % num_threads) * block_size;
  // Now we divide these blocks between available threads. The remainder is
  // handled on the main thread.

  uint64_t chunk_size = (right_address - left_address) / num_threads;
  uint64_t prefix = left_address - src_address;
  uint64_t suffix = src_address + nbytes - right_address;
  // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
  // We have chunk_size = k * block_size, therefore the data layout is
  // | prefix | num_threads * chunk_size | suffix |.
  // Each thread gets a "chunk" of k blocks.

  // Start all threads first and handle leftovers while threads run.
  for (uint64_t i = 0; i < num_threads; i++) {
    threadpool_[i] = std::thread(memcpy, dst + prefix + i * chunk_size,
        reinterpret_cast<uint8_t*>(left_address) + i * chunk_size, chunk_size);
  }

  memcpy(dst, src, prefix);
  memcpy(dst + prefix + num_threads * chunk_size,
      reinterpret_cast<uint8_t*>(right_address), suffix);
  for (auto& t : threadpool_) {
    if (t.joinable()) { t.join(); }
  }
}

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

static constexpr int64_t kBufferMinimumSize = 256;

BufferOutputStream::BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer, std::unique_ptr<Memcopy> memcopy)
    : buffer_(buffer),
      capacity_(buffer->size()),
      position_(0),
      mutable_data_(buffer->mutable_data()),
      memcopy_(std::move(memcopy)) {
    if (!memcopy_) {
      memcopy_ = std::unique_ptr<SerialMemcopy>(new SerialMemcopy());
    }
  }

Status BufferOutputStream::Create(int64_t initial_capacity, MemoryPool* pool,
    std::shared_ptr<BufferOutputStream>* out) {
  std::shared_ptr<ResizableBuffer> buffer;
  RETURN_NOT_OK(AllocateResizableBuffer(pool, initial_capacity, &buffer));
  *out = std::make_shared<BufferOutputStream>(buffer);
  return Status::OK();
}

BufferOutputStream::~BufferOutputStream() {
  // This can fail, better to explicitly call close
  if (buffer_) { Close(); }
}

Status BufferOutputStream::Close() {
  if (position_ < capacity_) {
    return buffer_->Resize(position_);
  } else {
    return Status::OK();
  }
}

Status BufferOutputStream::Finish(std::shared_ptr<Buffer>* result) {
  RETURN_NOT_OK(Close());
  *result = buffer_;
  buffer_ = nullptr;
  return Status::OK();
}

Status BufferOutputStream::Tell(int64_t* position) {
  *position = position_;
  return Status::OK();
}

Status BufferOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  DCHECK(buffer_);
  RETURN_NOT_OK(Reserve(nbytes));
  memcopy_->memcopy(mutable_data_ + position_, data, nbytes);
  position_ += nbytes;
  return Status::OK();
}

Status BufferOutputStream::Reserve(int64_t nbytes) {
  int64_t new_capacity = capacity_;
  while (position_ + nbytes > new_capacity) {
    new_capacity = std::max(kBufferMinimumSize, new_capacity * 2);
  }
  if (new_capacity > capacity_) {
    RETURN_NOT_OK(buffer_->Resize(new_capacity));
    capacity_ = new_capacity;
  }
  mutable_data_ = buffer_->mutable_data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// In-memory buffer writer

/// Input buffer must be mutable, will abort if not
FixedSizeBufferWriter::FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer, std::unique_ptr<Memcopy> memcopy) : memcopy_(std::move(memcopy)) {
  buffer_ = buffer;
  DCHECK(buffer->is_mutable()) << "Must pass mutable buffer";
  mutable_data_ = buffer->mutable_data();
  size_ = buffer->size();
  position_ = 0;
  if (!memcopy_) {
    memcopy_ = std::unique_ptr<SerialMemcopy>(new SerialMemcopy());
  }
}

FixedSizeBufferWriter::~FixedSizeBufferWriter() {}

Status FixedSizeBufferWriter::Close() {
  // No-op
  return Status::OK();
}

Status FixedSizeBufferWriter::Seek(int64_t position) {
  if (position < 0 || position >= size_) {
    return Status::IOError("position out of bounds");
  }
  position_ = position;
  return Status::OK();
}

Status FixedSizeBufferWriter::Tell(int64_t* position) {
  *position = position_;
  return Status::OK();
}

Status FixedSizeBufferWriter::Write(const uint8_t* data, int64_t nbytes) {
  memcopy_->memcopy(mutable_data_ + position_, data, nbytes);
  position_ += nbytes;
  return Status::OK();
}

Status FixedSizeBufferWriter::WriteAt(
    int64_t position, const uint8_t* data, int64_t nbytes) {
  std::lock_guard<std::mutex> guard(lock_);
  RETURN_NOT_OK(Seek(position));
  return Write(data, nbytes);
}

// ----------------------------------------------------------------------
// In-memory buffer reader

BufferReader::BufferReader(const std::shared_ptr<Buffer>& buffer)
    : buffer_(buffer), data_(buffer->data()), size_(buffer->size()), position_(0) {}

BufferReader::BufferReader(const uint8_t* data, int64_t size)
    : buffer_(nullptr), data_(data), size_(size), position_(0) {}

BufferReader::~BufferReader() {}

Status BufferReader::Close() {
  // no-op
  return Status::OK();
}

Status BufferReader::Tell(int64_t* position) {
  *position = position_;
  return Status::OK();
}

bool BufferReader::supports_zero_copy() const {
  return true;
}

Status BufferReader::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  memcpy(buffer, data_ + position_, nbytes);
  *bytes_read = std::min(nbytes, size_ - position_);
  position_ += *bytes_read;
  return Status::OK();
}

Status BufferReader::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  int64_t size = std::min(nbytes, size_ - position_);

  if (size > 0 && buffer_ != nullptr) {
    *out = SliceBuffer(buffer_, position_, size);
  } else {
    *out = std::make_shared<Buffer>(data_ + position_, size);
  }

  position_ += size;
  return Status::OK();
}

Status BufferReader::GetSize(int64_t* size) {
  *size = size_;
  return Status::OK();
}

Status BufferReader::Seek(int64_t position) {
  if (position < 0 || position >= size_) {
    return Status::IOError("position out of bounds");
  }

  position_ = position;
  return Status::OK();
}

}  // namespace io
}  // namespace arrow

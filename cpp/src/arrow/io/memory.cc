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
#include <cstdint>
#include <cstring>
#include <mutex>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/memory.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

static constexpr int64_t kBufferMinimumSize = 256;

BufferOutputStream::BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer)
    : buffer_(buffer),
      is_open_(true),
      capacity_(buffer->size()),
      position_(0),
      mutable_data_(buffer->mutable_data()) {}

Status BufferOutputStream::Create(int64_t initial_capacity, MemoryPool* pool,
                                  std::shared_ptr<BufferOutputStream>* out) {
  std::shared_ptr<ResizableBuffer> buffer;
  RETURN_NOT_OK(AllocateResizableBuffer(pool, initial_capacity, &buffer));
  *out = std::make_shared<BufferOutputStream>(buffer);
  return Status::OK();
}

BufferOutputStream::~BufferOutputStream() {
  // This can fail, better to explicitly call close
  if (buffer_) {
    DCHECK(Close().ok());
  }
}

Status BufferOutputStream::Close() {
  if (position_ < capacity_) {
    return buffer_->Resize(position_, false);
  } else {
    return Status::OK();
  }
}

Status BufferOutputStream::Finish(std::shared_ptr<Buffer>* result) {
  RETURN_NOT_OK(Close());
  *result = buffer_;
  buffer_ = nullptr;
  is_open_ = false;
  return Status::OK();
}

Status BufferOutputStream::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

Status BufferOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  if (ARROW_PREDICT_FALSE(!is_open_)) {
    return Status::IOError("OutputStream is closed");
  }
  DCHECK(buffer_);
  RETURN_NOT_OK(Reserve(nbytes));
  memcpy(mutable_data_ + position_, data, nbytes);
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
// OutputStream that doesn't write anything

Status MockOutputStream::Close() {
  // no-op
  return Status::OK();
}

Status MockOutputStream::Tell(int64_t* position) const {
  *position = extent_bytes_written_;
  return Status::OK();
}

Status MockOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  extent_bytes_written_ += nbytes;
  return Status::OK();
}

// ----------------------------------------------------------------------
// In-memory buffer writer

static constexpr int kMemcopyDefaultNumThreads = 1;
static constexpr int64_t kMemcopyDefaultBlocksize = 64;
static constexpr int64_t kMemcopyDefaultThreshold = 1024 * 1024;

class FixedSizeBufferWriter::FixedSizeBufferWriterImpl {
 public:
  /// Input buffer must be mutable, will abort if not

  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriterImpl(const std::shared_ptr<Buffer>& buffer)
      : memcopy_num_threads_(kMemcopyDefaultNumThreads),
        memcopy_blocksize_(kMemcopyDefaultBlocksize),
        memcopy_threshold_(kMemcopyDefaultThreshold) {
    buffer_ = buffer;
    DCHECK(buffer->is_mutable()) << "Must pass mutable buffer";
    mutable_data_ = buffer->mutable_data();
    size_ = buffer->size();
    position_ = 0;
  }

  ~FixedSizeBufferWriterImpl() {}

  Status Close() {
    // No-op
    return Status::OK();
  }

  Status Seek(int64_t position) {
    if (position < 0 || position >= size_) {
      return Status::IOError("position out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  Status Tell(int64_t* position) {
    *position = position_;
    return Status::OK();
  }

  Status Write(const uint8_t* data, int64_t nbytes) {
    if (nbytes > memcopy_threshold_ && memcopy_num_threads_ > 1) {
      internal::parallel_memcopy(mutable_data_ + position_, data, nbytes,
                                 memcopy_blocksize_, memcopy_num_threads_);
    } else {
      memcpy(mutable_data_ + position_, data, nbytes);
    }
    position_ += nbytes;
    return Status::OK();
  }

  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(Seek(position));
    return Write(data, nbytes);
  }

  void set_memcopy_threads(int num_threads) { memcopy_num_threads_ = num_threads; }

  void set_memcopy_blocksize(int64_t blocksize) { memcopy_blocksize_ = blocksize; }

  void set_memcopy_threshold(int64_t threshold) { memcopy_threshold_ = threshold; }

 private:
  std::mutex lock_;
  std::shared_ptr<Buffer> buffer_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t position_;

  int memcopy_num_threads_;
  int64_t memcopy_blocksize_;
  int64_t memcopy_threshold_;
};

FixedSizeBufferWriter::~FixedSizeBufferWriter() {}

FixedSizeBufferWriter::FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer)
    : impl_(new FixedSizeBufferWriterImpl(buffer)) {}

Status FixedSizeBufferWriter::Close() { return impl_->Close(); }

Status FixedSizeBufferWriter::Seek(int64_t position) { return impl_->Seek(position); }

Status FixedSizeBufferWriter::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status FixedSizeBufferWriter::Write(const uint8_t* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status FixedSizeBufferWriter::WriteAt(int64_t position, const uint8_t* data,
                                      int64_t nbytes) {
  return impl_->WriteAt(position, data, nbytes);
}

void FixedSizeBufferWriter::set_memcopy_threads(int num_threads) {
  impl_->set_memcopy_threads(num_threads);
}

void FixedSizeBufferWriter::set_memcopy_blocksize(int64_t blocksize) {
  impl_->set_memcopy_blocksize(blocksize);
}

void FixedSizeBufferWriter::set_memcopy_threshold(int64_t threshold) {
  impl_->set_memcopy_threshold(threshold);
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

Status BufferReader::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

bool BufferReader::supports_zero_copy() const { return true; }

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

Status BufferReader::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                            uint8_t* out) {
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status BufferReader::ReadAt(int64_t position, int64_t nbytes,
                            std::shared_ptr<Buffer>* out) {
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, out);
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

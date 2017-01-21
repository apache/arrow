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

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

static constexpr int64_t kBufferMinimumSize = 256;

BufferOutputStream::BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer)
    : buffer_(buffer),
      capacity_(buffer->size()),
      position_(0),
      mutable_data_(buffer->mutable_data()) {}

BufferOutputStream::~BufferOutputStream() {
  // This can fail, better to explicitly call close
  Close();
}

Status BufferOutputStream::Close() {
  if (position_ < capacity_) {
    return buffer_->Resize(position_);
  } else {
    return Status::OK();
  }
}

Status BufferOutputStream::Tell(int64_t* position) {
  *position = position_;
  return Status::OK();
}

Status BufferOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(Reserve(nbytes));
  std::memcpy(mutable_data_ + position_, data, nbytes);
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

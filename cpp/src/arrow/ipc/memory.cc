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

#include "arrow/ipc/memory.h"

#include <sys/mman.h>  // For memory-mapping

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>

#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

MemorySource::MemorySource(AccessMode access_mode) : access_mode_(access_mode) {}

MemorySource::~MemorySource() {}

// Implement MemoryMappedSource

class MemoryMappedSource::Impl {
 public:
  Impl() : file_(nullptr), is_open_(false), is_writable_(false), data_(nullptr) {}

  ~Impl() {
    if (is_open_) {
      munmap(data_, size_);
      fclose(file_);
    }
  }

  Status Open(const std::string& path, MemorySource::AccessMode mode) {
    if (is_open_) { return Status::IOError("A file is already open"); }

    int prot_flags = PROT_READ;

    if (mode == MemorySource::READ_WRITE) {
      file_ = fopen(path.c_str(), "r+b");
      prot_flags |= PROT_WRITE;
      is_writable_ = true;
    } else {
      file_ = fopen(path.c_str(), "rb");
    }
    if (file_ == nullptr) {
      std::stringstream ss;
      ss << "Unable to open file, errno: " << errno;
      return Status::IOError(ss.str());
    }

    fseek(file_, 0L, SEEK_END);
    if (ferror(file_)) { return Status::IOError("Unable to seek to end of file"); }
    size_ = ftell(file_);

    fseek(file_, 0L, SEEK_SET);
    is_open_ = true;

    void* result = mmap(nullptr, size_, prot_flags, MAP_SHARED, fileno(file_), 0);
    if (result == MAP_FAILED) {
      std::stringstream ss;
      ss << "Memory mapping file failed, errno: " << errno;
      return Status::IOError(ss.str());
    }
    data_ = reinterpret_cast<uint8_t*>(result);

    return Status::OK();
  }

  int64_t size() const { return size_; }

  uint8_t* data() { return data_; }

  bool writable() { return is_writable_; }

  bool opened() { return is_open_; }

 private:
  FILE* file_;
  int64_t size_;
  bool is_open_;
  bool is_writable_;

  // The memory map
  uint8_t* data_;
};

MemoryMappedSource::MemoryMappedSource(AccessMode access_mode)
    : MemorySource(access_mode) {}

Status MemoryMappedSource::Open(const std::string& path, AccessMode access_mode,
    std::shared_ptr<MemoryMappedSource>* out) {
  std::shared_ptr<MemoryMappedSource> result(new MemoryMappedSource(access_mode));

  result->impl_.reset(new Impl());
  RETURN_NOT_OK(result->impl_->Open(path, access_mode));

  *out = result;
  return Status::OK();
}

int64_t MemoryMappedSource::Size() const {
  return impl_->size();
}

Status MemoryMappedSource::Close() {
  // munmap handled in ::Impl dtor
  return Status::OK();
}

Status MemoryMappedSource::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  if (position < 0 || position >= impl_->size()) {
    return Status::Invalid("position is out of bounds");
  }

  nbytes = std::min(nbytes, impl_->size() - position);
  *out = std::make_shared<Buffer>(impl_->data() + position, nbytes);
  return Status::OK();
}

Status MemoryMappedSource::Write(int64_t position, const uint8_t* data, int64_t nbytes) {
  if (!impl_->opened() || !impl_->writable()) {
    return Status::IOError("Unable to write");
  }
  if (position < 0 || position >= impl_->size()) {
    return Status::Invalid("position is out of bounds");
  }

  // TODO(wesm): verify we are not writing past the end of the buffer
  uint8_t* dst = impl_->data() + position;
  memcpy(dst, data, nbytes);

  return Status::OK();
}

MockMemorySource::MockMemorySource(int64_t size)
    : size_(size), extent_bytes_written_(0) {}

Status MockMemorySource::Close() {
  return Status::OK();
}

Status MockMemorySource::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return Status::OK();
}

Status MockMemorySource::Write(int64_t position, const uint8_t* data, int64_t nbytes) {
  extent_bytes_written_ = std::max(extent_bytes_written_, position + nbytes);
  return Status::OK();
}

int64_t MockMemorySource::Size() const {
  return size_;
}

int64_t MockMemorySource::GetExtentBytesWritten() const {
  return extent_bytes_written_;
}

}  // namespace ipc
}  // namespace arrow

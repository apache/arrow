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

#include <sys/mman.h>  // For memory-mapping

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>

#include "arrow/io/interfaces.h"

#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {
namespace io {

// Implement MemoryMappedFile

class MemoryMappedFile::MemoryMappedFileImpl {
 public:
  MemoryMappedFileImpl()
      : file_(nullptr), is_open_(false), is_writable_(false), data_(nullptr) {}

  ~MemoryMappedFileImpl() {
    if (is_open_) {
      munmap(data_, size_);
      fclose(file_);
    }
  }

  Status Open(const std::string& path, FileMode::type mode) {
    if (is_open_) { return Status::IOError("A file is already open"); }

    int prot_flags = PROT_READ;

    if (mode == FileMode::READWRITE) {
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
    position_ = 0;

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

  Status Seek(int64_t position) {
    if (position < 0 || position >= size_) {
      return Status::Invalid("position is out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  int64_t position() { return position_; }

  void advance(int64_t nbytes) { position_ = std::min(size_, position_ + nbytes); }

  uint8_t* data() { return data_; }

  uint8_t* head() { return data_ + position_; }

  bool writable() { return is_writable_; }

  bool opened() { return is_open_; }

 private:
  FILE* file_;
  int64_t position_;
  int64_t size_;
  bool is_open_;
  bool is_writable_;

  // The memory map
  uint8_t* data_;
};

MemoryMappedFile::MemoryMappedFile(FileMode::type mode) {
  ReadableFileInterface::set_mode(mode);
}

Status MemoryMappedFile::Open(const std::string& path, FileMode::type mode,
    std::shared_ptr<MemoryMappedFile>* out) {
  std::shared_ptr<MemoryMappedFile> result(new MemoryMappedFile(mode));

  result->impl_.reset(new MemoryMappedFileImpl());
  RETURN_NOT_OK(result->impl_->Open(path, mode));

  *out = result;
  return Status::OK();
}

Status MemoryMappedFile::GetSize(int64_t* size) {
  *size = impl_->size();
  return Status::OK();
}

Status MemoryMappedFile::Tell(int64_t* position) {
  *position = impl_->position();
  return Status::OK();
}

Status MemoryMappedFile::Seek(int64_t position) {
  return impl_->Seek(position);
}

Status MemoryMappedFile::Close() {
  // munmap handled in pimpl dtor
  return Status::OK();
}

Status MemoryMappedFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  nbytes = std::min(nbytes, impl_->size() - impl_->position());
  std::memcpy(out, impl_->head(), nbytes);
  *bytes_read = nbytes;
  impl_->advance(nbytes);
  return Status::OK();
}

Status MemoryMappedFile::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  RETURN_NOT_OK(impl_->Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status MemoryMappedFile::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  nbytes = std::min(nbytes, impl_->size() - position);
  RETURN_NOT_OK(impl_->Seek(position));
  *out = std::make_shared<Buffer>(impl_->head(), nbytes);
  impl_->advance(nbytes);
  return Status::OK();
}

bool MemoryMappedFile::supports_zero_copy() const {
  return true;
}

Status MemoryMappedFile::WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) {
  if (!impl_->opened() || !impl_->writable()) {
    return Status::IOError("Unable to write");
  }

  RETURN_NOT_OK(impl_->Seek(position));
  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::Write(const uint8_t* data, int64_t nbytes) {
  if (!impl_->opened() || !impl_->writable()) {
    return Status::IOError("Unable to write");
  }
  if (nbytes + impl_->position() > impl_->size()) {
    return Status::Invalid("Cannot write past end of memory map");
  }

  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::WriteInternal(const uint8_t* data, int64_t nbytes) {
  memcpy(impl_->head(), data, nbytes);
  impl_->advance(nbytes);
  return Status::OK();
}

// ----------------------------------------------------------------------
// In-memory buffer reader

Status BufferReader::Close() {
  // no-op
  return Status::OK();
}

Status BufferReader::Tell(int64_t* position) {
  *position = position_;
  return Status::OK();
}

Status BufferReader::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, buffer);
}

Status BufferReader::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  int64_t size = std::min(nbytes, buffer_size_ - position_);
  *out = std::make_shared<Buffer>(buffer_ + position, size);
  position_ += nbytes;
  return Status::OK();
}

bool BufferReader::supports_zero_copy() const {
  return true;
}

Status BufferReader::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  memcpy(buffer, buffer_ + position_, nbytes);
  *bytes_read = std::min(nbytes, buffer_size_ - position_);
  position_ += *bytes_read;
  return Status::OK();
}

Status BufferReader::GetSize(int64_t* size) {
  *size = buffer_size_;
  return Status::OK();
}

Status BufferReader::Seek(int64_t position) {
  if (position < 0 || position >= buffer_size_) {
    return Status::IOError("position out of bounds");
  }

  position_ = position;
  return Status::OK();
}

}  // namespace io
}  // namespace arrow

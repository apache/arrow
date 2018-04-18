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

#include "arrow/io/windows_compatibility.h"

// sys/mman.h not present in Visual Studio or Cygwin
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#else
#include <sys/mman.h>
#endif

#include <string.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <sstream>

// ----------------------------------------------------------------------
// Other Arrow includes

#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

class OSFile {
 public:
  OSFile() : fd_(-1), is_open_(false), size_(-1) {}

  ~OSFile() {}

  // Note: only one of the Open* methods below may be called on a given instance

  Status OpenWriteable(const std::string& path, bool append, bool write_only) {
    RETURN_NOT_OK(SetFileName(path));

    RETURN_NOT_OK(internal::FileOpenWriteable(file_name_, write_only, !append, &fd_));
    is_open_ = true;
    mode_ = write_only ? FileMode::WRITE : FileMode::READWRITE;

    if (append) {
      RETURN_NOT_OK(internal::FileGetSize(fd_, &size_));
    } else {
      size_ = 0;
    }
    return Status::OK();
  }

  // This is different from OpenWriteable(string, ...) in that it doesn't
  // truncate nor mandate a seekable file
  Status OpenWriteable(int fd) {
    if (!internal::FileGetSize(fd, &size_).ok()) {
      // Non-seekable file
      size_ = -1;
    }
    RETURN_NOT_OK(SetFileName(fd));
    is_open_ = true;
    mode_ = FileMode::WRITE;
    fd_ = fd;
    return Status::OK();
  }

  Status OpenReadable(const std::string& path) {
    RETURN_NOT_OK(SetFileName(path));

    RETURN_NOT_OK(internal::FileOpenReadable(file_name_, &fd_));
    RETURN_NOT_OK(internal::FileGetSize(fd_, &size_));

    is_open_ = true;
    mode_ = FileMode::READ;
    return Status::OK();
  }

  Status OpenReadable(int fd) {
    RETURN_NOT_OK(internal::FileGetSize(fd, &size_));
    RETURN_NOT_OK(SetFileName(fd));
    is_open_ = true;
    mode_ = FileMode::READ;
    fd_ = fd;
    return Status::OK();
  }

  Status Close() {
    if (is_open_) {
      // Even if closing fails, the fd will likely be closed (perhaps it's
      // already closed).
      is_open_ = false;
      RETURN_NOT_OK(internal::FileClose(fd_));
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) {
    return internal::FileRead(fd_, reinterpret_cast<uint8_t*>(out), nbytes, bytes_read);
  }

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(Seek(position));
    return Read(nbytes, bytes_read, out);
  }

  Status Seek(int64_t pos) {
    if (pos < 0) {
      return Status::Invalid("Invalid position");
    }
    return internal::FileSeek(fd_, pos);
  }

  Status Tell(int64_t* pos) const { return internal::FileTell(fd_, pos); }

  Status Write(const void* data, int64_t length) {
    std::lock_guard<std::mutex> guard(lock_);
    if (length < 0) {
      return Status::IOError("Length must be non-negative");
    }
    return internal::FileWrite(fd_, reinterpret_cast<const uint8_t*>(data), length);
  }

  int fd() const { return fd_; }

  bool is_open() const { return is_open_; }

  int64_t size() const { return size_; }

  FileMode::type mode() const { return mode_; }

  std::mutex& lock() { return lock_; }

 protected:
  Status SetFileName(const std::string& file_name) {
    return internal::FileNameFromString(file_name, &file_name_);
  }
  Status SetFileName(int fd) {
    std::stringstream ss;
    ss << "<fd " << fd << ">";
    return SetFileName(ss.str());
  }

  internal::PlatformFilename file_name_;

  std::mutex lock_;

  // File descriptor
  int fd_;

  FileMode::type mode_;

  bool is_open_;
  int64_t size_;
};

// ----------------------------------------------------------------------
// ReadableFile implementation

class ReadableFile::ReadableFileImpl : public OSFile {
 public:
  explicit ReadableFileImpl(MemoryPool* pool) : OSFile(), pool_(pool) {}

  Status Open(const std::string& path) { return OpenReadable(path); }
  Status Open(int fd) { return OpenReadable(fd); }

  Status ReadBuffer(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> buffer;
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
    }
    *out = buffer;
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
};

ReadableFile::ReadableFile(MemoryPool* pool) { impl_.reset(new ReadableFileImpl(pool)); }

ReadableFile::~ReadableFile() { DCHECK(impl_->Close().ok()); }

Status ReadableFile::Open(const std::string& path, std::shared_ptr<ReadableFile>* file) {
  return Open(path, default_memory_pool(), file);
}

Status ReadableFile::Open(const std::string& path, MemoryPool* memory_pool,
                          std::shared_ptr<ReadableFile>* file) {
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(memory_pool));
  return (*file)->impl_->Open(path);
}

Status ReadableFile::Open(int fd, MemoryPool* memory_pool,
                          std::shared_ptr<ReadableFile>* file) {
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(memory_pool));
  return (*file)->impl_->Open(fd);
}

Status ReadableFile::Open(int fd, std::shared_ptr<ReadableFile>* file) {
  return Open(fd, default_memory_pool(), file);
}

Status ReadableFile::Close() { return impl_->Close(); }

Status ReadableFile::Tell(int64_t* pos) const { return impl_->Tell(pos); }

Status ReadableFile::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  std::lock_guard<std::mutex> guard(impl_->lock());
  return impl_->Read(nbytes, bytes_read, out);
}

Status ReadableFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                            void* out) {
  return impl_->ReadAt(position, nbytes, bytes_read, out);
}

Status ReadableFile::ReadAt(int64_t position, int64_t nbytes,
                            std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> guard(impl_->lock());
  RETURN_NOT_OK(Seek(position));
  return impl_->ReadBuffer(nbytes, out);
}

Status ReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> guard(impl_->lock());
  return impl_->ReadBuffer(nbytes, out);
}

Status ReadableFile::GetSize(int64_t* size) {
  *size = impl_->size();
  return Status::OK();
}

Status ReadableFile::Seek(int64_t pos) { return impl_->Seek(pos); }

bool ReadableFile::supports_zero_copy() const { return false; }

int ReadableFile::file_descriptor() const { return impl_->fd(); }

// ----------------------------------------------------------------------
// FileOutputStream

class FileOutputStream::FileOutputStreamImpl : public OSFile {
 public:
  Status Open(const std::string& path, bool append) {
    return OpenWriteable(path, append, true /* write_only */);
  }
  Status Open(int fd) { return OpenWriteable(fd); }
};

FileOutputStream::FileOutputStream() { impl_.reset(new FileOutputStreamImpl()); }

FileOutputStream::~FileOutputStream() {
  // This can fail; better to explicitly call close
  DCHECK(impl_->Close().ok());
}

Status FileOutputStream::Open(const std::string& path,
                              std::shared_ptr<OutputStream>* file) {
  return Open(path, false, file);
}

Status FileOutputStream::Open(const std::string& path, bool append,
                              std::shared_ptr<OutputStream>* out) {
  *out = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return std::static_pointer_cast<FileOutputStream>(*out)->impl_->Open(path, append);
}

Status FileOutputStream::Open(int fd, std::shared_ptr<OutputStream>* out) {
  *out = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return std::static_pointer_cast<FileOutputStream>(*out)->impl_->Open(fd);
}

Status FileOutputStream::Open(const std::string& path,
                              std::shared_ptr<FileOutputStream>* file) {
  return Open(path, false, file);
}

Status FileOutputStream::Open(const std::string& path, bool append,
                              std::shared_ptr<FileOutputStream>* file) {
  // private ctor
  *file = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return (*file)->impl_->Open(path, append);
}

Status FileOutputStream::Open(int fd, std::shared_ptr<FileOutputStream>* file) {
  *file = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return (*file)->impl_->Open(fd);
}

Status FileOutputStream::Close() { return impl_->Close(); }

Status FileOutputStream::Tell(int64_t* pos) const { return impl_->Tell(pos); }

Status FileOutputStream::Write(const void* data, int64_t length) {
  return impl_->Write(data, length);
}

int FileOutputStream::file_descriptor() const { return impl_->fd(); }

// ----------------------------------------------------------------------
// Implement MemoryMappedFile

class MemoryMappedFile::MemoryMap : public MutableBuffer {
 public:
  MemoryMap() : MutableBuffer(nullptr, 0) {}

  ~MemoryMap() {
    if (file_->is_open()) {
      munmap(mutable_data_, static_cast<size_t>(size_));
      DCHECK(file_->Close().ok());
    }
  }

  Status Open(const std::string& path, FileMode::type mode) {
    int prot_flags;
    int map_mode;

    file_.reset(new OSFile());

    if (mode != FileMode::READ) {
      // Memory mapping has permission failures if PROT_READ not set
      prot_flags = PROT_READ | PROT_WRITE;
      map_mode = MAP_SHARED;
      constexpr bool append = true;
      constexpr bool write_only = false;
      RETURN_NOT_OK(file_->OpenWriteable(path, append, write_only));

      is_mutable_ = true;
    } else {
      prot_flags = PROT_READ;
      map_mode = MAP_PRIVATE;  // Changes are not to be committed back to the file
      RETURN_NOT_OK(file_->OpenReadable(path));

      is_mutable_ = false;
    }

    size_ = file_->size();

    void* result = nullptr;

    // Memory mapping fails when file size is 0
    if (size_ > 0) {
      result =
          mmap(nullptr, static_cast<size_t>(size_), prot_flags, map_mode, file_->fd(), 0);
      if (result == MAP_FAILED) {
        std::stringstream ss;
        ss << "Memory mapping file failed: " << std::strerror(errno);
        return Status::IOError(ss.str());
      }
    }

    data_ = mutable_data_ = reinterpret_cast<uint8_t*>(result);

    position_ = 0;

    return Status::OK();
  }

  int64_t size() const { return size_; }

  Status Seek(int64_t position) {
    if (position < 0) {
      return Status::Invalid("position is out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  int64_t position() { return position_; }

  void advance(int64_t nbytes) { position_ = position_ + nbytes; }

  uint8_t* head() { return mutable_data_ + position_; }

  bool writable() { return file_->mode() != FileMode::READ; }

  bool opened() { return file_->is_open(); }

  int fd() const { return file_->fd(); }

  std::mutex& lock() { return file_->lock(); }

 private:
  std::unique_ptr<OSFile> file_;
  int64_t position_;
};

MemoryMappedFile::MemoryMappedFile() {}
MemoryMappedFile::~MemoryMappedFile() {}

Status MemoryMappedFile::Create(const std::string& path, int64_t size,
                                std::shared_ptr<MemoryMappedFile>* out) {
  std::shared_ptr<FileOutputStream> file;
  RETURN_NOT_OK(FileOutputStream::Open(path, &file));

  RETURN_NOT_OK(internal::FileTruncate(file->file_descriptor(), size));

  RETURN_NOT_OK(file->Close());
  return MemoryMappedFile::Open(path, FileMode::READWRITE, out);
}

Status MemoryMappedFile::Open(const std::string& path, FileMode::type mode,
                              std::shared_ptr<MemoryMappedFile>* out) {
  std::shared_ptr<MemoryMappedFile> result(new MemoryMappedFile());

  result->memory_map_.reset(new MemoryMap());
  RETURN_NOT_OK(result->memory_map_->Open(path, mode));

  *out = result;
  return Status::OK();
}

Status MemoryMappedFile::GetSize(int64_t* size) {
  *size = memory_map_->size();
  return Status::OK();
}

Status MemoryMappedFile::Tell(int64_t* position) const {
  *position = memory_map_->position();
  return Status::OK();
}

Status MemoryMappedFile::Seek(int64_t position) { return memory_map_->Seek(position); }

Status MemoryMappedFile::Close() {
  // munmap handled in pimpl dtor
  return Status::OK();
}

Status MemoryMappedFile::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  nbytes = std::max<int64_t>(
      0, std::min(nbytes, memory_map_->size() - memory_map_->position()));
  if (nbytes > 0) {
    std::memcpy(out, memory_map_->head(), static_cast<size_t>(nbytes));
  }
  *bytes_read = nbytes;
  memory_map_->advance(nbytes);
  return Status::OK();
}

Status MemoryMappedFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  nbytes = std::max<int64_t>(
      0, std::min(nbytes, memory_map_->size() - memory_map_->position()));

  if (nbytes > 0) {
    *out = SliceBuffer(memory_map_, memory_map_->position(), nbytes);
  } else {
    *out = std::make_shared<Buffer>(nullptr, 0);
  }
  memory_map_->advance(nbytes);
  return Status::OK();
}

Status MemoryMappedFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                void* out) {
  std::lock_guard<std::mutex> guard(memory_map_->lock());
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status MemoryMappedFile::ReadAt(int64_t position, int64_t nbytes,
                                std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> guard(memory_map_->lock());
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, out);
}

bool MemoryMappedFile::supports_zero_copy() const { return true; }

Status MemoryMappedFile::WriteAt(int64_t position, const void* data, int64_t nbytes) {
  std::lock_guard<std::mutex> guard(memory_map_->lock());

  if (!memory_map_->opened() || !memory_map_->writable()) {
    return Status::IOError("Unable to write");
  }

  RETURN_NOT_OK(memory_map_->Seek(position));
  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::Write(const void* data, int64_t nbytes) {
  std::lock_guard<std::mutex> guard(memory_map_->lock());

  if (!memory_map_->opened() || !memory_map_->writable()) {
    return Status::IOError("Unable to write");
  }
  if (nbytes + memory_map_->position() > memory_map_->size()) {
    return Status::Invalid("Cannot write past end of memory map");
  }
  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::WriteInternal(const void* data, int64_t nbytes) {
  memcpy(memory_map_->head(), data, static_cast<size_t>(nbytes));
  memory_map_->advance(nbytes);
  return Status::OK();
}

int MemoryMappedFile::file_descriptor() const { return memory_map_->fd(); }

}  // namespace io
}  // namespace arrow

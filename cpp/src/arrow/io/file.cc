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

// Ensure 64-bit off_t for platforms where it matters
#ifdef _FILE_OFFSET_BITS
#undef _FILE_OFFSET_BITS
#endif

#define _FILE_OFFSET_BITS 64

// define max read/write count
#if defined(_MSC_VER)
#define ARROW_MAX_IO_CHUNKSIZE INT32_MAX
#else

#ifdef __APPLE__
// due to macOS bug, we need to set read/write max
#define ARROW_MAX_IO_CHUNKSIZE INT32_MAX
#else
// see notes on Linux read/write manpage
#define ARROW_MAX_IO_CHUNKSIZE 0x7ffff000
#endif

#endif

#include "arrow/io/file.h"

#if _WIN32 || _WIN64
#if _WIN64
#define ENVIRONMENT64
#else
#define ENVIRONMENT32
#endif
#endif

// sys/mman.h not present in Visual Studio or Cygwin
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#include <windows.h>
#else
#include <sys/mman.h>
#endif

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <sstream>  // IWYU pragma: keep

#if defined(_MSC_VER)
#include <codecvt>
#include <locale>
#endif

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>  // IWYU pragma: keep

#ifndef _MSC_VER  // POSIX-like platforms

#include <unistd.h>

// Not available on some platforms
#ifndef errno_t
#define errno_t int
#endif

#endif  // _MSC_VER

// defines that don't exist in MinGW
#if defined(__MINGW32__)
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR
#elif defined(_MSC_VER)  // Visual Studio

#else  // gcc / clang on POSIX platforms
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
#endif

// ----------------------------------------------------------------------
// file compatibility stuff

#if defined(__MINGW32__)  // MinGW
// nothing
#elif defined(_MSC_VER)  // Visual Studio
#include <io.h>
#else  // POSIX / Linux
// nothing
#endif

// POSIX systems do not have this
#ifndef O_BINARY
#define O_BINARY 0
#endif

// ----------------------------------------------------------------------
// Other Arrow includes

#include "arrow/io/interfaces.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#if defined(_MSC_VER)
#include <boost/filesystem.hpp>           // NOLINT
#include <boost/system/system_error.hpp>  // NOLINT
namespace fs = boost::filesystem;
#define PlatformFilename fs::path

namespace arrow {
namespace io {

#else
namespace arrow {
namespace io {

struct PlatformFilename {
  PlatformFilename() {}
  explicit PlatformFilename(const std::string& path) { utf8_path = path; }

  const char* c_str() const { return utf8_path.c_str(); }

  const std::string& string() const { return utf8_path; }

  size_t length() const { return utf8_path.size(); }

  std::string utf8_path;
};
#endif

static inline Status CheckFileOpResult(int ret, int errno_actual,
                                       const PlatformFilename& file_name,
                                       const std::string& opname) {
  if (ret == -1) {
    std::stringstream ss;
    ss << "Failed to " << opname << " file: " << file_name.string();
    ss << " , error: " << std::strerror(errno_actual);
    return Status::IOError(ss.str());
  }
  return Status::OK();
}

#define CHECK_LSEEK(retval) \
  if ((retval) == -1) return Status::IOError("lseek failed");

static inline int64_t lseek64_compat(int fd, int64_t pos, int whence) {
#if defined(_MSC_VER)
  return _lseeki64(fd, pos, whence);
#else
  return lseek(fd, pos, whence);
#endif
}

static inline Status FileOpenReadable(const PlatformFilename& file_name, int* fd) {
  int ret;
  errno_t errno_actual = 0;
#if defined(_MSC_VER)
  errno_actual = _wsopen_s(fd, file_name.wstring().c_str(), _O_RDONLY | _O_BINARY,
                           _SH_DENYNO, _S_IREAD);
  ret = *fd;
#else
  ret = *fd = open(file_name.c_str(), O_RDONLY | O_BINARY);
  errno_actual = errno;
#endif

  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

static inline Status FileOpenWriteable(const PlatformFilename& file_name, bool write_only,
                                       bool truncate, int* fd) {
  int ret;
  errno_t errno_actual = 0;

#if defined(_MSC_VER)
  int oflag = _O_CREAT | _O_BINARY;
  int pmode = _S_IWRITE;
  if (!write_only) {
    pmode |= _S_IREAD;
  }

  if (truncate) {
    oflag |= _O_TRUNC;
  }

  if (write_only) {
    oflag |= _O_WRONLY;
  } else {
    oflag |= _O_RDWR;
  }

  errno_actual = _wsopen_s(fd, file_name.wstring().c_str(), oflag, _SH_DENYNO, pmode);
  ret = *fd;

#else
  int oflag = O_CREAT | O_BINARY;

  if (truncate) {
    oflag |= O_TRUNC;
  }

  if (write_only) {
    oflag |= O_WRONLY;
  } else {
    oflag |= O_RDWR;
  }

  ret = *fd = open(file_name.c_str(), oflag, ARROW_WRITE_SHMODE);
#endif
  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

static inline Status FileTell(int fd, int64_t* pos) {
  int64_t current_pos;

#if defined(_MSC_VER)
  current_pos = _telli64(fd);
  if (current_pos == -1) {
    return Status::IOError("_telli64 failed");
  }
#else
  current_pos = lseek64_compat(fd, 0, SEEK_CUR);
  CHECK_LSEEK(current_pos);
#endif

  *pos = current_pos;
  return Status::OK();
}

static inline Status FileSeek(int fd, int64_t pos) {
  int64_t ret = lseek64_compat(fd, pos, SEEK_SET);
  CHECK_LSEEK(ret);
  return Status::OK();
}

static inline Status FileRead(const int fd, uint8_t* buffer, const int64_t nbytes,
                              int64_t* bytes_read) {
  *bytes_read = 0;

  while (*bytes_read != -1 && *bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - *bytes_read);
#if defined(_MSC_VER)
    int64_t ret = static_cast<int64_t>(
        _read(fd, buffer + *bytes_read, static_cast<uint32_t>(chunksize)));
#else
    int64_t ret = static_cast<int64_t>(
        read(fd, buffer + *bytes_read, static_cast<size_t>(chunksize)));
#endif

    if (ret != -1) {
      *bytes_read += ret;
      if (ret < chunksize) {
        // EOF
        break;
      }
    } else {
      *bytes_read = ret;
    }
  }

  if (*bytes_read == -1) {
    return Status::IOError(std::string("Error reading bytes from file: ") +
                           std::string(strerror(errno)));
  }

  return Status::OK();
}

static inline Status FileWrite(const int fd, const uint8_t* buffer,
                               const int64_t nbytes) {
  int ret = 0;
  int64_t bytes_written = 0;

  while (ret != -1 && bytes_written < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - bytes_written);
#if defined(_MSC_VER)
    ret = static_cast<int>(
        _write(fd, buffer + bytes_written, static_cast<uint32_t>(chunksize)));
#else
    ret = static_cast<int>(
        write(fd, buffer + bytes_written, static_cast<size_t>(chunksize)));
#endif

    if (ret != -1) {
      bytes_written += ret;
    }
  }

  if (ret == -1) {
    return Status::IOError(std::string("Error writing bytes from file: ") +
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

static inline Status FileGetSize(int fd, int64_t* size) {
  int64_t ret;

  // Save current position
  int64_t current_position = lseek64_compat(fd, 0, SEEK_CUR);
  CHECK_LSEEK(current_position);

  // move to end of the file
  ret = lseek64_compat(fd, 0, SEEK_END);
  CHECK_LSEEK(ret);

  // Get file length
  ret = lseek64_compat(fd, 0, SEEK_CUR);
  CHECK_LSEEK(ret);

  *size = ret;

  // Restore file position
  ret = lseek64_compat(fd, current_position, SEEK_SET);
  CHECK_LSEEK(ret);

  return Status::OK();
}

static inline Status FileClose(int fd) {
  int ret;

#if defined(_MSC_VER)
  ret = static_cast<int>(_close(fd));
#else
  ret = static_cast<int>(close(fd));
#endif

  if (ret == -1) {
    return Status::IOError("error closing file");
  }
  return Status::OK();
}

class OSFile {
 public:
  OSFile() : fd_(-1), is_open_(false), size_(-1) {}

  ~OSFile() {}

  Status OpenWriteable(const std::string& path, bool append, bool write_only) {
    RETURN_NOT_OK(SetFileName(path));

    RETURN_NOT_OK(FileOpenWriteable(file_name_, write_only, !append, &fd_));
    is_open_ = true;
    mode_ = write_only ? FileMode::WRITE : FileMode::READWRITE;

    if (append) {
      RETURN_NOT_OK(FileGetSize(fd_, &size_));
    } else {
      size_ = 0;
    }
    return Status::OK();
  }

  Status OpenReadable(const std::string& path) {
    RETURN_NOT_OK(SetFileName(path));

    RETURN_NOT_OK(FileOpenReadable(file_name_, &fd_));
    RETURN_NOT_OK(FileGetSize(fd_, &size_));

    is_open_ = true;
    mode_ = FileMode::READ;
    return Status::OK();
  }

  Status Close() {
    if (is_open_) {
      RETURN_NOT_OK(FileClose(fd_));
      is_open_ = false;
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) {
    return FileRead(fd_, reinterpret_cast<uint8_t*>(out), nbytes, bytes_read);
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
    return FileSeek(fd_, pos);
  }

  Status Tell(int64_t* pos) const { return FileTell(fd_, pos); }

  Status Write(const void* data, int64_t length) {
    std::lock_guard<std::mutex> guard(lock_);
    if (length < 0) {
      return Status::IOError("Length must be non-negative");
    }
    return FileWrite(fd_, reinterpret_cast<const uint8_t*>(data), length);
  }

  int fd() const { return fd_; }

  bool is_open() const { return is_open_; }

  int64_t size() const { return size_; }

  FileMode::type mode() const { return mode_; }

  std::mutex& lock() { return lock_; }

 protected:
  Status SetFileName(const std::string& file_name) {
#if defined(_MSC_VER)
    try {
      std::codecvt_utf8_utf16<wchar_t> utf16_converter;
      file_name_.assign(file_name, utf16_converter);
    } catch (boost::system::system_error& e) {
      return Status::Invalid(e.what());
    }
#else
    file_name_ = PlatformFilename(file_name);
#endif
    return Status::OK();
  }

  PlatformFilename file_name_;

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
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(default_memory_pool()));
  return (*file)->impl_->Open(path);
}

Status ReadableFile::Open(const std::string& path, MemoryPool* memory_pool,
                          std::shared_ptr<ReadableFile>* file) {
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(memory_pool));
  return (*file)->impl_->Open(path);
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
    return OpenWriteable(path, append, true);
  }
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
  int ret;
  errno_t errno_actual;
  std::shared_ptr<FileOutputStream> file;
  RETURN_NOT_OK(FileOutputStream::Open(path, &file));

#ifdef _MSC_VER
  errno_actual = _chsize_s(file->file_descriptor(), static_cast<size_t>(size));
  ret = errno_actual == 0 ? 0 : -1;
#else
  ret = ftruncate(file->file_descriptor(), static_cast<size_t>(size));
  errno_actual = errno;
#endif

  RETURN_NOT_OK(CheckFileOpResult(ret, errno_actual, PlatformFilename(path), "truncate"));

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

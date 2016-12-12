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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifndef _MSC_VER  // POSIX-like platforms

#include <unistd.h>

// Not available on some platforms
#ifndef errno_t
#define errno_t int
#endif

#endif  // _MSC_VER

// defines that
#if defined(__MINGW32__)
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR
#elif defined(_MSC_VER)  // Visual Studio

#else  // gcc / clang on POSIX platforms
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
#endif

// ----------------------------------------------------------------------
// C++ standard library

#include <algorithm>
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>
#include <vector>

#if defined(_MSC_VER)
#include <codecvt>
#include <locale>
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

#include <cstdio>

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

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// Cross-platform file compatability layer

static inline Status CheckOpenResult(
    int ret, int errno_actual, const char* filename, size_t filename_length) {
  if (ret == -1) {
    // TODO: errno codes to strings
    std::stringstream ss;
    ss << "Failed to open file: ";
#if defined(_MSC_VER)
    // using wchar_t

    // this requires c++11
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
    std::wstring wide_string(
        reinterpret_cast<const wchar_t*>(filename), filename_length / sizeof(wchar_t));
    std::string byte_string = converter.to_bytes(wide_string);
    ss << byte_string;
#else
    ss << filename;
#endif
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

static inline Status FileOpenReadable(const std::string& filename, int* fd) {
  int ret;
  errno_t errno_actual = 0;
#if defined(_MSC_VER)
  // https://msdn.microsoft.com/en-us/library/w64k0ytk.aspx

  // See GH #209. Here we are assuming that the filename has been encoded in
  // utf-16le so that unicode filenames can be supported
  const int nwchars = static_cast<int>(filename.size()) / sizeof(wchar_t);
  std::vector<wchar_t> wpath(nwchars + 1);
  memcpy(wpath.data(), filename.data(), filename.size());
  memcpy(wpath.data() + nwchars, L"\0", sizeof(wchar_t));

  errno_actual = _wsopen_s(fd, wpath.data(), _O_RDONLY | _O_BINARY, _SH_DENYNO, _S_IREAD);
  ret = *fd;
#else
  ret = *fd = open(filename.c_str(), O_RDONLY | O_BINARY);
  errno_actual = errno;
#endif

  return CheckOpenResult(ret, errno_actual, filename.c_str(), filename.size());
}

static inline Status FileOpenWriteable(const std::string& filename, int* fd) {
  int ret;
  errno_t errno_actual = 0;

#if defined(_MSC_VER)
  // https://msdn.microsoft.com/en-us/library/w64k0ytk.aspx
  // Same story with wchar_t as above
  const int nwchars = static_cast<int>(filename.size()) / sizeof(wchar_t);
  std::vector<wchar_t> wpath(nwchars + 1);
  memcpy(wpath.data(), filename.data(), filename.size());
  memcpy(wpath.data() + nwchars, L"\0", sizeof(wchar_t));

  errno_actual = _wsopen_s(fd, wpath.data(), _O_WRONLY | _O_CREAT | _O_BINARY | _O_TRUNC,
      _SH_DENYNO, _S_IWRITE);
  ret = *fd;

#else
  ret = *fd =
      open(filename.c_str(), O_WRONLY | O_CREAT | O_BINARY | O_TRUNC, ARROW_WRITE_SHMODE);
#endif
  return CheckOpenResult(ret, errno_actual, filename.c_str(), filename.size());
}

static inline Status FileTell(int fd, int64_t* pos) {
  int64_t current_pos;

#if defined(_MSC_VER)
  current_pos = _telli64(fd);
  if (current_pos == -1) { return Status::IOError("_telli64 failed"); }
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

static inline Status FileRead(
    int fd, uint8_t* buffer, int64_t nbytes, int64_t* bytes_read) {
#if defined(_MSC_VER)
  if (nbytes > INT32_MAX) { return Status::IOError("Unable to read > 2GB blocks yet"); }
  *bytes_read = _read(fd, buffer, static_cast<unsigned int>(nbytes));
#else
  *bytes_read = read(fd, buffer, nbytes);
#endif

  if (*bytes_read == -1) {
    // TODO(wesm): errno to string
    return Status::IOError("Error reading bytes from file");
  }

  return Status::OK();
}

static inline Status FileWrite(int fd, const uint8_t* buffer, int64_t nbytes) {
  int ret;
#if defined(_MSC_VER)
  if (nbytes > INT32_MAX) {
    return Status::IOError("Unable to write > 2GB blocks to file yet");
  }
  ret = _write(fd, buffer, static_cast<unsigned int>(nbytes));
#else
  ret = write(fd, buffer, nbytes);
#endif

  if (ret == -1) {
    // TODO(wesm): errno to string
    return Status::IOError("Error writing bytes to file");
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
  ret = _close(fd);
#else
  ret = close(fd);
#endif

  if (ret == -1) { return Status::IOError("error closing file"); }
  return Status::OK();
}

class OSFile {
 public:
  OSFile() : fd_(-1), is_open_(false), size_(-1) {}

  ~OSFile() {}

  Status OpenWritable(const std::string& path) {
    RETURN_NOT_OK(FileOpenWriteable(path, &fd_));
    path_ = path;
    is_open_ = true;
    return Status::OK();
  }

  Status OpenReadable(const std::string& path) {
    RETURN_NOT_OK(FileOpenReadable(path, &fd_));
    RETURN_NOT_OK(FileGetSize(fd_, &size_));

    // The position should be 0 after GetSize
    // RETURN_NOT_OK(Seek(0));

    path_ = path;
    is_open_ = true;
    return Status::OK();
  }

  Status Close() {
    if (is_open_) {
      RETURN_NOT_OK(FileClose(fd_));
      is_open_ = false;
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
    return FileRead(fd_, out, nbytes, bytes_read);
  }

  Status Seek(int64_t pos) {
    if (pos > size_) { pos = size_; }
    return FileSeek(fd_, pos);
  }

  Status Tell(int64_t* pos) const { return FileTell(fd_, pos); }

  Status Write(const uint8_t* data, int64_t length) {
    if (length < 0) { return Status::IOError("Length must be non-negative"); }
    return FileWrite(fd_, data, length);
  }

  int fd() const { return fd_; }

  bool is_open() const { return is_open_; }
  const std::string& path() const { return path_; }

  int64_t size() const { return size_; }

 private:
  std::string path_;

  // File descriptor
  int fd_;

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
    auto buffer = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(buffer->Resize(nbytes));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));

    // XXX: heuristic
    if (bytes_read < nbytes / 2) { RETURN_NOT_OK(buffer->Resize(bytes_read)); }

    *out = buffer;
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
};

ReadableFile::ReadableFile(MemoryPool* pool) {
  impl_.reset(new ReadableFileImpl(pool));
}

ReadableFile::~ReadableFile() {
  impl_->Close();
}

Status ReadableFile::Open(const std::string& path, std::shared_ptr<ReadableFile>* file) {
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(default_memory_pool()));
  return (*file)->impl_->Open(path);
}

Status ReadableFile::Open(const std::string& path, MemoryPool* memory_pool,
    std::shared_ptr<ReadableFile>* file) {
  *file = std::shared_ptr<ReadableFile>(new ReadableFile(memory_pool));
  return (*file)->impl_->Open(path);
}

Status ReadableFile::Close() {
  return impl_->Close();
}

Status ReadableFile::Tell(int64_t* pos) {
  return impl_->Tell(pos);
}

Status ReadableFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  return impl_->Read(nbytes, bytes_read, out);
}

Status ReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return impl_->ReadBuffer(nbytes, out);
}

Status ReadableFile::GetSize(int64_t* size) {
  *size = impl_->size();
  return Status::OK();
}

Status ReadableFile::Seek(int64_t pos) {
  return impl_->Seek(pos);
}

bool ReadableFile::supports_zero_copy() const {
  return false;
}

int ReadableFile::file_descriptor() const {
  return impl_->fd();
}

// ----------------------------------------------------------------------
// FileOutputStream

class FileOutputStream::FileOutputStreamImpl : public OSFile {
 public:
  Status Open(const std::string& path) { return OpenWritable(path); }
};

FileOutputStream::FileOutputStream() {
  impl_.reset(new FileOutputStreamImpl());
}

FileOutputStream::~FileOutputStream() {
  impl_->Close();
}

Status FileOutputStream::Open(
    const std::string& path, std::shared_ptr<FileOutputStream>* file) {
  // private ctor
  *file = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return (*file)->impl_->Open(path);
}

Status FileOutputStream::Close() {
  return impl_->Close();
}

Status FileOutputStream::Tell(int64_t* pos) {
  return impl_->Tell(pos);
}

Status FileOutputStream::Write(const uint8_t* data, int64_t length) {
  return impl_->Write(data, length);
}

int FileOutputStream::file_descriptor() const {
  return impl_->fd();
}

}  // namespace io
}  // namespace arrow

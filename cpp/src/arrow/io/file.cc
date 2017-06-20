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

// defines that don't exist in MinGW
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
#include <mutex>
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
#if defined(_MSC_VER)
constexpr const char* kRangeExceptionError =
    "Range exception during wide-char string conversion";
#endif

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
    try {
      std::string byte_string = converter.to_bytes(wide_string);
      ss << byte_string;
    } catch (const std::range_error&) { ss << kRangeExceptionError; }
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

#if defined(_MSC_VER)
static inline Status ConvertToUtf16(const std::string& input, std::wstring* result) {
  if (result == nullptr) { return Status::Invalid("Pointer to result is not valid"); }

  if (input.empty()) {
    *result = std::wstring();
    return Status::OK();
  }

  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> utf16_converter;
  try {
    *result = utf16_converter.from_bytes(input);
  } catch (const std::range_error&) { return Status::Invalid(kRangeExceptionError); }
  return Status::OK();
}
#endif

static inline Status FileOpenReadable(const std::string& filename, int* fd) {
  int ret;
  errno_t errno_actual = 0;
#if defined(_MSC_VER)
  std::wstring wide_filename;
  RETURN_NOT_OK(ConvertToUtf16(filename, &wide_filename));

  errno_actual =
      _wsopen_s(fd, wide_filename.c_str(), _O_RDONLY | _O_BINARY, _SH_DENYNO, _S_IREAD);
  ret = *fd;
#else
  ret = *fd = open(filename.c_str(), O_RDONLY | O_BINARY);
  errno_actual = errno;
#endif

  return CheckOpenResult(ret, errno_actual, filename.c_str(), filename.size());
}

static inline Status FileOpenWriteable(
    const std::string& filename, bool write_only, bool truncate, int* fd) {
  int ret;
  errno_t errno_actual = 0;

#if defined(_MSC_VER)
  std::wstring wide_filename;
  RETURN_NOT_OK(ConvertToUtf16(filename, &wide_filename));

  int oflag = _O_CREAT | _O_BINARY;
  int pmode = _S_IWRITE;
  if (!write_only) { pmode |= _S_IREAD; }

  if (truncate) { oflag |= _O_TRUNC; }

  if (write_only) {
    oflag |= _O_WRONLY;
  } else {
    oflag |= _O_RDWR;
  }

  errno_actual = _wsopen_s(fd, wide_filename.c_str(), oflag, _SH_DENYNO, pmode);
  ret = *fd;

#else
  int oflag = O_CREAT | O_BINARY;

  if (truncate) { oflag |= O_TRUNC; }

  if (write_only) {
    oflag |= O_WRONLY;
  } else {
    oflag |= O_RDWR;
  }

  ret = *fd = open(filename.c_str(), oflag, ARROW_WRITE_SHMODE);
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
  *bytes_read = static_cast<int64_t>(_read(fd, buffer, static_cast<uint32_t>(nbytes)));
#else
  *bytes_read = static_cast<int64_t>(read(fd, buffer, static_cast<size_t>(nbytes)));
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
  ret = static_cast<int>(_write(fd, buffer, static_cast<uint32_t>(nbytes)));
#else
  ret = static_cast<int>(write(fd, buffer, static_cast<size_t>(nbytes)));
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
  ret = static_cast<int>(_close(fd));
#else
  ret = static_cast<int>(close(fd));
#endif

  if (ret == -1) { return Status::IOError("error closing file"); }
  return Status::OK();
}

class OSFile {
 public:
  OSFile() : fd_(-1), is_open_(false), size_(-1) {}

  ~OSFile() {}

  Status OpenWriteable(const std::string& path, bool append, bool write_only) {
    RETURN_NOT_OK(FileOpenWriteable(path, write_only, !append, &fd_));
    path_ = path;
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
    RETURN_NOT_OK(FileOpenReadable(path, &fd_));
    RETURN_NOT_OK(FileGetSize(fd_, &size_));

    path_ = path;
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

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
    std::lock_guard<std::mutex> guard(lock_);
    return FileRead(fd_, out, nbytes, bytes_read);
  }

  Status Seek(int64_t pos) {
    if (pos < 0) { return Status::Invalid("Invalid position"); }
    return FileSeek(fd_, pos);
  }

  Status Tell(int64_t* pos) const { return FileTell(fd_, pos); }

  Status Write(const uint8_t* data, int64_t length) {
    std::lock_guard<std::mutex> guard(lock_);
    if (length < 0) { return Status::IOError("Length must be non-negative"); }
    return FileWrite(fd_, data, length);
  }

  int fd() const { return fd_; }

  bool is_open() const { return is_open_; }
  const std::string& path() const { return path_; }

  int64_t size() const { return size_; }

  FileMode::type mode() const { return mode_; }

 protected:
  std::string path_;

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
    if (bytes_read < nbytes) { RETURN_NOT_OK(buffer->Resize(bytes_read)); }
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
  Status Open(const std::string& path, bool append) {
    return OpenWriteable(path, append, true);
  }
};

FileOutputStream::FileOutputStream() {
  impl_.reset(new FileOutputStreamImpl());
}

FileOutputStream::~FileOutputStream() {
  // This can fail; better to explicitly call close
  impl_->Close();
}

Status FileOutputStream::Open(
    const std::string& path, std::shared_ptr<FileOutputStream>* file) {
  return Open(path, false, file);
}

Status FileOutputStream::Open(
    const std::string& path, bool append, std::shared_ptr<FileOutputStream>* file) {
  // private ctor
  *file = std::shared_ptr<FileOutputStream>(new FileOutputStream());
  return (*file)->impl_->Open(path, append);
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

// ----------------------------------------------------------------------
// Implement MemoryMappedFile

class MemoryMappedFile::MemoryMap : public MutableBuffer {
 public:
  MemoryMap() : MutableBuffer(nullptr, 0) {}

  ~MemoryMap() {
    if (file_->is_open()) {
      munmap(mutable_data_, static_cast<size_t>(size_));
      file_->Close();
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

    void* result = mmap(nullptr, static_cast<size_t>(file_->size()), prot_flags, map_mode,
        file_->fd(), 0);
    if (result == MAP_FAILED) {
      std::stringstream ss;
      ss << "Memory mapping file failed, errno: " << errno;
      return Status::IOError(ss.str());
    }

    data_ = mutable_data_ = reinterpret_cast<uint8_t*>(result);
    size_ = file_->size();

    position_ = 0;

    return Status::OK();
  }

  int64_t size() const { return size_; }

  Status Seek(int64_t position) {
    if (position < 0) { return Status::Invalid("position is out of bounds"); }
    position_ = position;
    return Status::OK();
  }

  int64_t position() { return position_; }

  void advance(int64_t nbytes) { position_ = position_ + nbytes; }

  uint8_t* head() { return mutable_data_ + position_; }

  bool writable() { return file_->mode() != FileMode::READ; }

  bool opened() { return file_->is_open(); }

  int fd() const { return file_->fd(); }

 private:
  std::unique_ptr<OSFile> file_;
  int64_t position_;
};

MemoryMappedFile::MemoryMappedFile() {}
MemoryMappedFile::~MemoryMappedFile() {}

Status MemoryMappedFile::Create(
    const std::string& path, int64_t size, std::shared_ptr<MemoryMappedFile>* out) {
  std::shared_ptr<FileOutputStream> file;
  RETURN_NOT_OK(FileOutputStream::Open(path, &file));
#ifdef _MSC_VER
  _chsize_s(file->file_descriptor(), static_cast<size_t>(size));
#else
  ftruncate(file->file_descriptor(), static_cast<size_t>(size));
#endif
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

Status MemoryMappedFile::Tell(int64_t* position) {
  *position = memory_map_->position();
  return Status::OK();
}

Status MemoryMappedFile::Seek(int64_t position) {
  return memory_map_->Seek(position);
}

Status MemoryMappedFile::Close() {
  // munmap handled in pimpl dtor
  return Status::OK();
}

Status MemoryMappedFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  nbytes = std::max<int64_t>(
      0, std::min(nbytes, memory_map_->size() - memory_map_->position()));
  if (nbytes > 0) { std::memcpy(out, memory_map_->head(), static_cast<size_t>(nbytes)); }
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

bool MemoryMappedFile::supports_zero_copy() const {
  return true;
}

Status MemoryMappedFile::WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) {
  std::lock_guard<std::mutex> guard(lock_);

  if (!memory_map_->opened() || !memory_map_->writable()) {
    return Status::IOError("Unable to write");
  }

  RETURN_NOT_OK(memory_map_->Seek(position));
  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::Write(const uint8_t* data, int64_t nbytes) {
  std::lock_guard<std::mutex> guard(lock_);

  if (!memory_map_->opened() || !memory_map_->writable()) {
    return Status::IOError("Unable to write");
  }
  if (nbytes + memory_map_->position() > memory_map_->size()) {
    return Status::Invalid("Cannot write past end of memory map");
  }
  return WriteInternal(data, nbytes);
}

Status MemoryMappedFile::WriteInternal(const uint8_t* data, int64_t nbytes) {
  memcpy(memory_map_->head(), data, static_cast<size_t>(nbytes));
  memory_map_->advance(nbytes);
  return Status::OK();
}

int MemoryMappedFile::file_descriptor() const {
  return memory_map_->fd();
}

}  // namespace io
}  // namespace arrow

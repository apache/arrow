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

#include "arrow/io/windows_compatibility.h"

#include <algorithm>
#include <cerrno>
#include <sstream>

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>  // IWYU pragma: keep

// Defines that don't exist in MinGW
#if defined(__MINGW32__)
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR
#elif defined(_MSC_VER)  // Visual Studio

#else  // gcc / clang on POSIX platforms
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
#endif

// For filename conversion
#if defined(_MSC_VER)
#include <boost/system/system_error.hpp>  // NOLINT
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

#ifndef _MSC_VER  // POSIX-like platforms
#include <unistd.h>
#endif  // _MSC_VER

// POSIX systems do not have this
#ifndef O_BINARY
#define O_BINARY 0
#endif

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

#include "arrow/util/io-util.h"

namespace arrow {
namespace internal {

#define CHECK_LSEEK(retval) \
  if ((retval) == -1) return Status::IOError("lseek failed");

static inline int64_t lseek64_compat(int fd, int64_t pos, int whence) {
#if defined(_MSC_VER)
  return _lseeki64(fd, pos, whence);
#else
  return lseek(fd, pos, whence);
#endif
}

static inline Status CheckFileOpResult(int ret, int errno_actual,
                                       const PlatformFilename& file_name,
                                       const char* opname) {
  if (ret == -1) {
    std::stringstream ss;
    ss << "Failed to " << opname << " file: " << file_name.string();
    ss << " , error: " << std::strerror(errno_actual);
    return Status::IOError(ss.str());
  }
  return Status::OK();
}

//
// File name handling
//

Status FileNameFromString(const std::string& file_name, PlatformFilename* out) {
#if defined(_MSC_VER)
  try {
    std::codecvt_utf8_utf16<wchar_t> utf16_converter;
    out->assign(file_name, utf16_converter);
  } catch (boost::system::system_error& e) {
    return Status::Invalid(e.what());
  }
#else
  *out = internal::PlatformFilename(file_name);
#endif
  return Status::OK();
}

//
// Functions for creating file descriptors
//

Status FileOpenReadable(const PlatformFilename& file_name, int* fd) {
  int ret, errno_actual;
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

Status FileOpenWriteable(const PlatformFilename& file_name, bool write_only,
                         bool truncate, int* fd) {
  int ret, errno_actual;

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
  errno_actual = errno;
#endif
  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

Status FileTell(int fd, int64_t* pos) {
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

Status CreatePipe(int fd[2]) {
  int ret;
#if defined(_MSC_VER)
  ret = _pipe(fd, 4096, _O_BINARY);
#else
  ret = pipe(fd);
#endif

  if (ret == -1) {
    return Status::IOError(std::string("Error creating pipe: ") +
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

//
// Closing files
//

Status FileClose(int fd) {
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

//
// Seeking and telling
//

Status FileSeek(int fd, int64_t pos, int whence) {
  int64_t ret = lseek64_compat(fd, pos, whence);
  CHECK_LSEEK(ret);
  return Status::OK();
}

Status FileSeek(int fd, int64_t pos) { return FileSeek(fd, pos, SEEK_SET); }

Status FileGetSize(int fd, int64_t* size) {
  int64_t ret;

  // XXX Should use fstat() instead, but this function also ensures the
  // file is seekable

  // Save current position
  int64_t current_position = lseek64_compat(fd, 0, SEEK_CUR);
  CHECK_LSEEK(current_position);

  // Move to end of the file, which returns the file length
  ret = lseek64_compat(fd, 0, SEEK_END);
  CHECK_LSEEK(ret);

  *size = ret;

  // Restore file position
  ret = lseek64_compat(fd, current_position, SEEK_SET);
  CHECK_LSEEK(ret);

  return Status::OK();
}

//
// Reading data
//

static inline int64_t pread_compat(int fd, void* buf, int64_t nbytes, int64_t pos) {
#if defined(_MSC_VER)
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  DWORD dwBytesRead = 0;
  OVERLAPPED overlapped = {0};
  overlapped.Offset = static_cast<uint32_t>(pos);
  overlapped.OffsetHigh = static_cast<uint32_t>(pos >> 32);

  // Note: ReadFile() will update the file position
  BOOL bRet =
      ReadFile(handle, buf, static_cast<uint32_t>(nbytes), &dwBytesRead, &overlapped);
  if (bRet || GetLastError() == ERROR_HANDLE_EOF) {
    return dwBytesRead;
  } else {
    return -1;
  }
#else
  return static_cast<int64_t>(
      pread(fd, buf, static_cast<size_t>(nbytes), static_cast<off_t>(pos)));
#endif
}

Status FileRead(int fd, uint8_t* buffer, int64_t nbytes, int64_t* bytes_read) {
  *bytes_read = 0;

  while (*bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - *bytes_read);
#if defined(_MSC_VER)
    int64_t ret =
        static_cast<int64_t>(_read(fd, buffer, static_cast<uint32_t>(chunksize)));
#else
    int64_t ret = static_cast<int64_t>(read(fd, buffer, static_cast<size_t>(chunksize)));
#endif

    if (ret == -1) {
      *bytes_read = ret;
      break;
    }
    if (ret == 0) {
      // EOF
      break;
    }
    buffer += ret;
    *bytes_read += ret;
  }

  if (*bytes_read == -1) {
    return Status::IOError(std::string("Error reading bytes from file: ") +
                           std::string(strerror(errno)));
  }

  return Status::OK();
}

Status FileReadAt(int fd, uint8_t* buffer, int64_t position, int64_t nbytes,
                  int64_t* bytes_read) {
  *bytes_read = 0;

  while (*bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - *bytes_read);
    int64_t ret = pread_compat(fd, buffer, chunksize, position);

    if (ret == -1) {
      *bytes_read = ret;
      break;
    }
    if (ret == 0) {
      // EOF
      break;
    }
    buffer += ret;
    position += ret;
    *bytes_read += ret;
  }

  if (*bytes_read == -1) {
    return Status::IOError(std::string("Error reading bytes from file: ") +
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

//
// Writing data
//

Status FileWrite(int fd, const uint8_t* buffer, const int64_t nbytes) {
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

Status FileTruncate(int fd, const int64_t size) {
  int ret, errno_actual;

#ifdef _MSC_VER
  errno_actual = _chsize_s(fd, static_cast<size_t>(size));
  ret = errno_actual == 0 ? 0 : -1;
#else
  ret = ftruncate(fd, static_cast<size_t>(size));
  errno_actual = errno;
#endif

  if (ret == -1) {
    return Status::IOError(std::string("Error truncating file: ") +
                           std::string(strerror(errno_actual)));
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow

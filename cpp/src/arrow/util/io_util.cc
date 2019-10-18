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

#include "arrow/util/windows_compatibility.h"  // IWYU pragma: keep

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>  // IWYU pragma: keep

// Defines that don't exist in MinGW
#if defined(__MINGW32__)
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR
#elif defined(_MSC_VER)  // Visual Studio

#else  // gcc / clang on POSIX platforms
#define ARROW_WRITE_SHMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
#endif

#ifdef ARROW_WITH_BOOST_FILESYSTEM
#include <boost/filesystem.hpp>
#endif

// ----------------------------------------------------------------------
// file compatibility stuff

#if defined(_WIN32)
#include <io.h>
#include <share.h>
#endif

#ifdef _WIN32  // Windows
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#else  // POSIX-like platforms
#include <sys/mman.h>
#include <unistd.h>
#endif

// define max read/write count
#if defined(_WIN32)
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

#include "arrow/buffer.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

// For filename conversion
#if defined(_WIN32)
#include "arrow/util/utf8.h"
#endif

namespace arrow {
namespace io {

//
// StdoutStream implementation
//

StdoutStream::StdoutStream() : pos_(0) { set_mode(FileMode::WRITE); }

Status StdoutStream::Close() { return Status::OK(); }

bool StdoutStream::closed() const { return false; }

Status StdoutStream::Tell(int64_t* position) const {
  *position = pos_;
  return Status::OK();
}

Status StdoutStream::Write(const void* data, int64_t nbytes) {
  pos_ += nbytes;
  std::cout.write(reinterpret_cast<const char*>(data), nbytes);
  return Status::OK();
}

//
// StderrStream implementation
//

StderrStream::StderrStream() : pos_(0) { set_mode(FileMode::WRITE); }

Status StderrStream::Close() { return Status::OK(); }

bool StderrStream::closed() const { return false; }

Status StderrStream::Tell(int64_t* position) const {
  *position = pos_;
  return Status::OK();
}

Status StderrStream::Write(const void* data, int64_t nbytes) {
  pos_ += nbytes;
  std::cerr.write(reinterpret_cast<const char*>(data), nbytes);
  return Status::OK();
}

//
// StdinStream implementation
//

StdinStream::StdinStream() : pos_(0) { set_mode(FileMode::READ); }

Status StdinStream::Close() { return Status::OK(); }

bool StdinStream::closed() const { return false; }

Status StdinStream::Tell(int64_t* position) const {
  *position = pos_;
  return Status::OK();
}

Status StdinStream::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  std::cin.read(reinterpret_cast<char*>(out), nbytes);
  if (std::cin) {
    *bytes_read = nbytes;
    pos_ += nbytes;
  } else {
    *bytes_read = 0;
  }
  return Status::OK();
}

Status StdinStream::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  std::shared_ptr<ResizableBuffer> buffer;
  ARROW_RETURN_NOT_OK(AllocateResizableBuffer(nbytes, &buffer));
  int64_t bytes_read;
  ARROW_RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
  ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
  buffer->ZeroPadding();
  *out = buffer;
  return Status::OK();
}

}  // namespace io

namespace internal {

#ifdef ARROW_WITH_BOOST_FILESYSTEM
namespace bfs = ::boost::filesystem;
#endif

namespace {

template <typename CharT>
std::basic_string<CharT> ReplaceChars(std::basic_string<CharT> s, CharT find, CharT rep) {
  if (find != rep) {
    for (size_t i = 0; i < s.length(); ++i) {
      if (s[i] == find) {
        s[i] = rep;
      }
    }
  }
  return s;
}

Status StringToNative(const std::string& s, NativePathString* out) {
#if _WIN32
  std::wstring ws;
  RETURN_NOT_OK(::arrow::util::UTF8ToWideString(s, &ws));
  *out = std::move(ws);
#else
  *out = s;
#endif
  return Status::OK();
}

#if _WIN32
Status NativeToString(const NativePathString& ws, std::string* out) {
  std::string s;
  RETURN_NOT_OK(::arrow::util::WideStringToUTF8(ws, &s));
  *out = std::move(s);
  return Status::OK();
}
#endif

#if _WIN32
const wchar_t kNativeSep = L'\\';
const wchar_t kGenericSep = L'/';
const wchar_t* kAllSeps = L"\\/";
#else
const char kNativeSep = '/';
const char kGenericSep = '/';
const char* kAllSeps = "/";
#endif

NativePathString NativeSlashes(NativePathString s) {
  return ReplaceChars(std::move(s), kGenericSep, kNativeSep);
}

NativePathString GenericSlashes(NativePathString s) {
  return ReplaceChars(std::move(s), kNativeSep, kGenericSep);
}

NativePathString NativeParent(const NativePathString& s) {
  auto last_sep = s.find_last_of(kAllSeps);
  if (last_sep == s.length() - 1) {
    // Last separator is a trailing separator, skip all trailing separators
    // and try again
    auto before_last_seps = s.find_last_not_of(kAllSeps);
    if (before_last_seps == NativePathString::npos) {
      // Only separators in path
      return s;
    }
    last_sep = s.find_last_of(kAllSeps, before_last_seps);
  }
  if (last_sep == NativePathString::npos) {
    // No (other) separator in path
    return s;
  }
  // There may be multiple contiguous separators, skip all of them
  auto before_last_seps = s.find_last_not_of(kAllSeps, last_sep);
  if (before_last_seps == NativePathString::npos) {
    // All separators are at start of string, keep them all
    return s.substr(0, last_sep + 1);
  } else {
    return s.substr(0, before_last_seps + 1);
  }
}

Status ValidatePath(const std::string& s) {
  if (s.find_first_of('\0') != std::string::npos) {
    return Status::Invalid("Embedded NUL char in path: '", s, "'");
  }
  return Status::OK();
}

}  // namespace

#ifdef ARROW_WITH_BOOST_FILESYSTEM

// NOTE: catching filesystem_error gives more context than system::error_code
// (it includes the file path(s) in the error message)

#define BOOST_FILESYSTEM_TRY try {
#define BOOST_FILESYSTEM_CATCH           \
  }                                      \
  catch (bfs::filesystem_error & _err) { \
    return ToStatus(_err);               \
  }

static Status ToStatus(const bfs::filesystem_error& err) {
  return Status::IOError(err.what());
}

#endif  // ARROW_WITH_BOOST_FILESYSTEM

std::string ErrnoMessage(int errnum) { return std::strerror(errnum); }

#if _WIN32
std::string WinErrorMessage(int errnum) {
  char buf[1024];
  auto nchars = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                               NULL, errnum, 0, buf, sizeof(buf), NULL);
  if (nchars == 0) {
    // Fallback
    std::stringstream ss;
    ss << "Windows error #" << errnum;
    return ss.str();
  }
  return std::string(buf, nchars);
}
#endif

//
// PlatformFilename implementation
//

struct PlatformFilename::Impl {
  Impl() = default;
  explicit Impl(NativePathString p) : native_(NativeSlashes(std::move(p))) {}

  NativePathString native_;

  // '/'-separated
  NativePathString generic() const { return GenericSlashes(native_); }

#ifdef ARROW_WITH_BOOST_FILESYSTEM
  bfs::path boost_path() const { return bfs::path(native_); }
#endif
};

PlatformFilename::PlatformFilename() : impl_(new Impl{}) {}

PlatformFilename::~PlatformFilename() {}

PlatformFilename::PlatformFilename(Impl impl) : impl_(new Impl(std::move(impl))) {}

PlatformFilename::PlatformFilename(const PlatformFilename& other)
    : PlatformFilename(Impl{other.impl_->native_}) {}

PlatformFilename::PlatformFilename(PlatformFilename&& other)
    : impl_(std::move(other.impl_)) {}

PlatformFilename& PlatformFilename::operator=(const PlatformFilename& other) {
  this->impl_.reset(new Impl{other.impl_->native_});
  return *this;
}

PlatformFilename& PlatformFilename::operator=(PlatformFilename&& other) {
  this->impl_ = std::move(other.impl_);
  return *this;
}

PlatformFilename::PlatformFilename(const NativePathString& path)
    : PlatformFilename(Impl{path}) {}

bool PlatformFilename::operator==(const PlatformFilename& other) const {
  return impl_->native_ == other.impl_->native_;
}

bool PlatformFilename::operator!=(const PlatformFilename& other) const {
  return impl_->native_ != other.impl_->native_;
}

const NativePathString& PlatformFilename::ToNative() const { return impl_->native_; }

std::string PlatformFilename::ToString() const {
#if _WIN32
  std::string s;
  Status st = NativeToString(impl_->generic(), &s);
  if (!st.ok()) {
    std::stringstream ss;
    ss << "<Unrepresentable filename: " << st.ToString() << ">";
    return ss.str();
  }
  return s;
#else
  return impl_->generic();
#endif
}

PlatformFilename PlatformFilename::Parent() const {
  return PlatformFilename(NativeParent(ToNative()));
}

Status PlatformFilename::FromString(const std::string& file_name, PlatformFilename* out) {
  RETURN_NOT_OK(ValidatePath(file_name));
  NativePathString ns;
  RETURN_NOT_OK(StringToNative(file_name, &ns));
  *out = PlatformFilename(std::move(ns));
  return Status::OK();
}

Status PlatformFilename::Join(const std::string& child_name,
                              PlatformFilename* out) const {
  PlatformFilename child;
  RETURN_NOT_OK(PlatformFilename::FromString(child_name, &child));
  if (impl_->native_.empty() || impl_->native_.back() == kNativeSep) {
    *out = PlatformFilename(Impl{impl_->native_ + child.impl_->native_});
    return Status::OK();
  } else {
    *out = PlatformFilename(Impl{impl_->native_ + kNativeSep + child.impl_->native_});
    return Status::OK();
  }
}

Status FileNameFromString(const std::string& file_name, PlatformFilename* out) {
  return PlatformFilename::FromString(file_name, out);
}

//
// Filesystem access routines
//

namespace {

Status DoCreateDir(const PlatformFilename& dir_path, bool create_parents, bool* created) {
#ifdef _WIN32
  if (CreateDirectoryW(dir_path.ToNative().c_str(), nullptr)) {
    *created = true;
    return Status::OK();
  }
  int errnum = GetLastError();
  if (errnum == ERROR_ALREADY_EXISTS) {
    *created = false;
    return Status::OK();
  }
  if (create_parents && errnum == ERROR_PATH_NOT_FOUND) {
    auto parent_path = dir_path.Parent();
    if (parent_path != dir_path) {
      RETURN_NOT_OK(DoCreateDir(parent_path, create_parents, created));
      return DoCreateDir(dir_path, false, created);  // Retry
    }
  }
  return Status::IOError("Cannot create directory '", dir_path.ToString(),
                         "': ", WinErrorMessage(errnum));
#else
  if (mkdir(dir_path.ToNative().c_str(), S_IRWXU | S_IRWXG | S_IRWXO) == 0) {
    *created = true;
    return Status::OK();
  }
  if (errno == EEXIST) {
    *created = false;
    return Status::OK();
  }
  if (create_parents && errno == ENOENT) {
    auto parent_path = dir_path.Parent();
    if (parent_path != dir_path) {
      RETURN_NOT_OK(DoCreateDir(parent_path, create_parents, created));
      return DoCreateDir(dir_path, false, created);  // Retry
    }
  }
  return Status::IOError("Cannot create directory '", dir_path.ToString(),
                         "': ", ErrnoMessage(errno));
#endif
}

}  // namespace

Status CreateDir(const PlatformFilename& dir_path, bool* created) {
  bool did_create = false;
  RETURN_NOT_OK(DoCreateDir(dir_path, false, &did_create));
  if (created) {
    *created = did_create;
  }
  return Status::OK();
}

Status CreateDirTree(const PlatformFilename& dir_path, bool* created) {
  bool did_create = false;
  RETURN_NOT_OK(DoCreateDir(dir_path, true, &did_create));
  if (created) {
    *created = did_create;
  }
  return Status::OK();
}

#ifdef ARROW_WITH_BOOST_FILESYSTEM

Status DeleteDirTree(const PlatformFilename& dir_path, bool* deleted) {
  BOOST_FILESYSTEM_TRY
  const auto& path = dir_path.impl()->boost_path();
  // XXX There is a race here.
  auto st = bfs::symlink_status(path);
  if (st.type() != bfs::file_not_found && st.type() != bfs::directory_file) {
    return Status::IOError("Cannot delete non-directory '", path.string(), "'");
  }
  auto n_removed = bfs::remove_all(path);
  if (deleted) {
    *deleted = n_removed != 0;
  }
  BOOST_FILESYSTEM_CATCH
  return Status::OK();
}

Status DeleteDirContents(const PlatformFilename& dir_path, bool* deleted) {
  BOOST_FILESYSTEM_TRY
  const auto& path = dir_path.impl()->boost_path();
  // XXX There is a race here.
  auto st = bfs::symlink_status(path);
  if (st.type() == bfs::file_not_found) {
    if (deleted) {
      *deleted = false;
    }
    return Status::OK();
  }
  if (st.type() != bfs::directory_file) {
    return Status::IOError("Cannot delete contents of non-directory '", path.string(),
                           "'");
  }
  // Delete children one by one
  for (const auto& child : bfs::directory_iterator(path)) {
    bfs::remove_all(child.path());
  }
  BOOST_FILESYSTEM_CATCH
  if (deleted) {
    *deleted = true;
  }
  return Status::OK();
}

#else  // ARROW_WITH_BOOST_FILESYSTEM

Status DeleteDirTree(const PlatformFilename& dir_path, bool* deleted) {
  return Status::NotImplemented("DeleteDirTree not available in this Arrow build");
}

Status DeleteDirContents(const PlatformFilename& dir_path, bool* deleted) {
  return Status::NotImplemented("DeleteDirContents not available in this Arrow build");
}

#endif

Status DeleteFile(const PlatformFilename& file_path, bool* deleted) {
  bool did_delete = false;
#ifdef _WIN32
  if (DeleteFileW(file_path.ToNative().c_str())) {
    did_delete = true;
  } else {
    int errnum = GetLastError();
    if (errnum != ERROR_FILE_NOT_FOUND) {
      return Status::IOError("Cannot delete file '", file_path.ToString(),
                             "': ", WinErrorMessage(errnum));
    }
  }
#else
  if (unlink(file_path.ToNative().c_str()) == 0) {
    did_delete = true;
  } else {
    if (errno != ENOENT) {
      return Status::IOError("Cannot delete file '", file_path.ToString(),
                             "': ", ErrnoMessage(errno));
    }
  }
#endif
  if (deleted) {
    *deleted = did_delete;
  }
  return Status::OK();
}

Status FileExists(const PlatformFilename& path, bool* out) {
#ifdef _WIN32
  if (GetFileAttributesW(path.ToNative().c_str()) != INVALID_FILE_ATTRIBUTES) {
    *out = true;
  } else {
    int errnum = GetLastError();
    if (errnum != ERROR_PATH_NOT_FOUND && errnum != ERROR_FILE_NOT_FOUND) {
      return Status::IOError("Failed getting information for path '", path.ToString(),
                             "': ", WinErrorMessage(errnum));
    }
    *out = false;
  }
#else
  struct stat st;
  if (stat(path.ToNative().c_str(), &st) == 0) {
    *out = true;
  } else {
    if (errno != ENOENT && errno != ENOTDIR) {
      return Status::IOError("Failed getting information for path '", path.ToString(),
                             "': ", ErrnoMessage(errno));
    }
    *out = false;
  }
#endif
  return Status::OK();
}

//
// Functions for creating file descriptors
//

#define CHECK_LSEEK(retval) \
  if ((retval) == -1) return Status::IOError("lseek failed");

static inline int64_t lseek64_compat(int fd, int64_t pos, int whence) {
#if defined(_WIN32)
  return _lseeki64(fd, pos, whence);
#else
  return lseek(fd, pos, whence);
#endif
}

static inline Status CheckFileOpResult(int ret, int errno_actual,
                                       const PlatformFilename& file_name,
                                       const char* opname) {
  if (ret == -1) {
#ifdef _WIN32
    int winerr = GetLastError();
    if (winerr != ERROR_SUCCESS) {
      return Status::IOError("Failed to ", opname, " file '", file_name.ToString(),
                             "', error: ", WinErrorMessage(winerr));
    }
#endif
    return Status::IOError("Failed to ", opname, " file '", file_name.ToString(),
                           "', error: ", ErrnoMessage(errno_actual));
  }
  return Status::OK();
}

Status FileOpenReadable(const PlatformFilename& file_name, int* fd) {
  int ret, errno_actual;
#if defined(_WIN32)
  SetLastError(0);
  errno_actual = _wsopen_s(fd, file_name.ToNative().c_str(),
                           _O_RDONLY | _O_BINARY | _O_NOINHERIT, _SH_DENYNO, _S_IREAD);
  ret = *fd;
#else
  ret = *fd = open(file_name.ToNative().c_str(), O_RDONLY);
  errno_actual = errno;

  if (ret >= 0) {
    // open(O_RDONLY) succeeds on directories, check for it
    struct stat st;
    ret = fstat(*fd, &st);
    if (ret == -1) {
      ARROW_UNUSED(FileClose(*fd));
      // Will propagate error below
    } else if (S_ISDIR(st.st_mode)) {
      ARROW_UNUSED(FileClose(*fd));
      return Status::IOError("Cannot open for reading: path '", file_name.ToString(),
                             "' is a directory");
    }
  }
#endif

  return CheckFileOpResult(ret, errno_actual, file_name, "open local");
}

Status FileOpenWritable(const PlatformFilename& file_name, bool write_only, bool truncate,
                        bool append, int* fd) {
  int ret, errno_actual;

#if defined(_WIN32)
  SetLastError(0);
  int oflag = _O_CREAT | _O_BINARY | _O_NOINHERIT;
  int pmode = _S_IREAD | _S_IWRITE;

  if (truncate) {
    oflag |= _O_TRUNC;
  }
  if (append) {
    oflag |= _O_APPEND;
  }

  if (write_only) {
    oflag |= _O_WRONLY;
  } else {
    oflag |= _O_RDWR;
  }

  errno_actual = _wsopen_s(fd, file_name.ToNative().c_str(), oflag, _SH_DENYNO, pmode);
  ret = *fd;

#else
  int oflag = O_CREAT;

  if (truncate) {
    oflag |= O_TRUNC;
  }
  if (append) {
    oflag |= O_APPEND;
  }

  if (write_only) {
    oflag |= O_WRONLY;
  } else {
    oflag |= O_RDWR;
  }

  ret = *fd = open(file_name.ToNative().c_str(), oflag, ARROW_WRITE_SHMODE);
  errno_actual = errno;
#endif
  RETURN_NOT_OK(CheckFileOpResult(ret, errno_actual, file_name, "open local"));
  if (append) {
    // Seek to end, as O_APPEND does not necessarily do it
    auto ret = lseek64_compat(*fd, 0, SEEK_END);
    if (ret == -1) {
      ARROW_UNUSED(FileClose(*fd));
      return Status::IOError("lseek failed");
    }
  }
  return Status::OK();
}

Status FileTell(int fd, int64_t* pos) {
  int64_t current_pos;

#if defined(_WIN32)
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
#if defined(_WIN32)
  ret = _pipe(fd, 4096, _O_BINARY);
#else
  ret = pipe(fd);
#endif

  if (ret == -1) {
    return Status::IOError("Error creating pipe: ", ErrnoMessage(errno));
  }
  return Status::OK();
}

static Status StatusFromErrno(const char* prefix) {
#ifdef _WIN32
  errno = __map_mman_error(GetLastError(), EPERM);
#endif
  return Status::IOError(prefix, ErrnoMessage(errno));
}

//
// Compatible way to remap a memory map
//

Status MemoryMapRemap(void* addr, size_t old_size, size_t new_size, int fildes,
                      void** new_addr) {
  // should only be called with writable files
  *new_addr = MAP_FAILED;
#ifdef _WIN32
  // flags are ignored on windows
  HANDLE fm, h;

  if (!UnmapViewOfFile(addr)) {
    return StatusFromErrno("UnmapViewOfFile failed: ");
  }

  h = reinterpret_cast<HANDLE>(_get_osfhandle(fildes));
  if (h == INVALID_HANDLE_VALUE) {
    return StatusFromErrno("Cannot get file handle: ");
  }

  uint64_t new_size64 = new_size;
  LONG new_size_low = static_cast<LONG>(new_size64 & 0xFFFFFFFFUL);
  LONG new_size_high = static_cast<LONG>((new_size64 >> 32) & 0xFFFFFFFFUL);

  SetFilePointer(h, new_size_low, &new_size_high, FILE_BEGIN);
  SetEndOfFile(h);
  fm = CreateFileMapping(h, NULL, PAGE_READWRITE, 0, 0, "");
  if (fm == NULL) {
    return StatusFromErrno("CreateFileMapping failed: ");
  }
  *new_addr = MapViewOfFile(fm, FILE_MAP_WRITE, 0, 0, new_size);
  CloseHandle(fm);
  if (new_addr == NULL) {
    return StatusFromErrno("MapViewOfFile failed: ");
  }
  return Status::OK();
#else
#ifdef __APPLE__
  // we have to close the mmap first, truncate the file to the new size
  // and recreate the mmap
  if (munmap(addr, old_size) == -1) {
    return StatusFromErrno("munmap failed: ");
  }
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromErrno("ftruncate failed: ");
  }
  // we set READ / WRITE flags on the new map, since we could only have
  // unlarged a RW map in the first place
  *new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fildes, 0);
  if (*new_addr == MAP_FAILED) {
    return StatusFromErrno("mmap failed: ");
  }
  return Status::OK();
#else
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromErrno("ftruncate failed: ");
  }
  *new_addr = mremap(addr, old_size, new_size, MREMAP_MAYMOVE);
  if (*new_addr == MAP_FAILED) {
    return StatusFromErrno("mremap failed: ");
  }
  return Status::OK();
#endif
#endif
}

//
// Closing files
//

Status FileClose(int fd) {
  int ret;

#if defined(_WIN32)
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
#if defined(_WIN32)
  struct __stat64 st;
#else
  struct stat st;
#endif
  st.st_size = -1;

#if defined(_WIN32)
  int ret = _fstat64(fd, &st);
#else
  int ret = fstat(fd, &st);
#endif

  if (ret == -1) {
    return Status::IOError("error stat()ing file");
  }
  if (st.st_size == 0) {
    // Maybe the file doesn't support getting its size, double-check by
    // trying to tell() (seekable files usually have a size, while
    // non-seekable files don't)
    int64_t position;
    RETURN_NOT_OK(FileTell(fd, &position));
  } else if (st.st_size < 0) {
    return Status::IOError("error getting file size");
  }
  *size = st.st_size;
  return Status::OK();
}

//
// Reading data
//

static inline int64_t pread_compat(int fd, void* buf, int64_t nbytes, int64_t pos) {
#if defined(_WIN32)
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
#if defined(_WIN32)
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
    return Status::IOError("Error reading bytes from file: ", ErrnoMessage(errno));
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
    return Status::IOError("Error reading bytes from file: ", ErrnoMessage(errno));
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
#if defined(_WIN32)
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
    return Status::IOError("Error writing bytes to file: ", ErrnoMessage(errno));
  }
  return Status::OK();
}

Status FileTruncate(int fd, const int64_t size) {
  int ret, errno_actual;

#ifdef _WIN32
  errno_actual = _chsize_s(fd, static_cast<size_t>(size));
  ret = errno_actual == 0 ? 0 : -1;
#else
  ret = ftruncate(fd, static_cast<size_t>(size));
  errno_actual = errno;
#endif

  if (ret == -1) {
    return Status::IOError("Error writing bytes to file: ", ErrnoMessage(errno_actual));
  }
  return Status::OK();
}

//
// Environment variables
//

Status GetEnvVar(const char* name, std::string* out) {
#ifdef _WIN32
  // On Windows, getenv() reads an early copy of the process' environment
  // which doesn't get updated when SetEnvironmentVariable() is called.
  constexpr int32_t bufsize = 2000;
  char c_str[bufsize];
  auto res = GetEnvironmentVariableA(name, c_str, bufsize);
  if (res >= bufsize) {
    return Status::CapacityError("environment variable value too long");
  } else if (res == 0) {
    return Status::KeyError("environment variable undefined");
  }
  *out = std::string(c_str);
  return Status::OK();
#else
  char* c_str = getenv(name);
  if (c_str == nullptr) {
    return Status::KeyError("environment variable undefined");
  }
  *out = std::string(c_str);
  return Status::OK();
#endif
}

Status GetEnvVar(const std::string& name, std::string* out) {
  return GetEnvVar(name.c_str(), out);
}

#ifdef _WIN32
Status GetEnvVar(const std::string& name, NativePathString* out) {
  NativePathString w_name;
  constexpr int32_t bufsize = 2000;
  wchar_t w_str[bufsize];

  RETURN_NOT_OK(StringToNative(name, &w_name));
  auto res = GetEnvironmentVariableW(w_name.c_str(), w_str, bufsize);
  if (res >= bufsize) {
    return Status::CapacityError("environment variable value too long");
  } else if (res == 0) {
    return Status::KeyError("environment variable undefined");
  }
  *out = NativePathString(w_str);
  return Status::OK();
}

Status GetEnvVar(const char* name, NativePathString* out) {
  return GetEnvVar(std::string(name), out);
}
#endif

Status SetEnvVar(const char* name, const char* value) {
#ifdef _WIN32
  if (SetEnvironmentVariableA(name, value)) {
    return Status::OK();
  } else {
    return Status::Invalid("failed setting environment variable");
  }
#else
  if (setenv(name, value, 1) == 0) {
    return Status::OK();
  } else {
    return Status::Invalid("failed setting environment variable");
  }
#endif
}

Status SetEnvVar(const std::string& name, const std::string& value) {
  return SetEnvVar(name.c_str(), value.c_str());
}

Status DelEnvVar(const char* name) {
#ifdef _WIN32
  if (SetEnvironmentVariableA(name, nullptr)) {
    return Status::OK();
  } else {
    return Status::Invalid("failed deleting environment variable");
  }
#else
  if (unsetenv(name) == 0) {
    return Status::OK();
  } else {
    return Status::Invalid("failed deleting environment variable");
  }
#endif
}

Status DelEnvVar(const std::string& name) { return DelEnvVar(name.c_str()); }

//
// Temporary directories
//

#ifdef ARROW_WITH_BOOST_FILESYSTEM

namespace {

#if _WIN32
NativePathString GetWindowsDirectoryPath() {
  auto size = GetWindowsDirectoryW(nullptr, 0);
  ARROW_CHECK_GT(size, 0) << "GetWindowsDirectoryW failed";
  std::vector<wchar_t> w_str(size);
  size = GetWindowsDirectoryW(w_str.data(), size);
  ARROW_CHECK_GT(size, 0) << "GetWindowsDirectoryW failed";
  return {w_str.data(), size};
}
#endif

// Return a list of preferred locations for temporary files
std::vector<NativePathString> GetPlatformTemporaryDirs() {
  struct TempDirSelector {
    std::string env_var;
    NativePathString path_append;
  };

  std::vector<TempDirSelector> selectors;
  NativePathString fallback_tmp;

#if _WIN32
  selectors = {
      {"TMP", L""}, {"TEMP", L""}, {"LOCALAPPDATA", L"Temp"}, {"USERPROFILE", L"Temp"}};
  fallback_tmp = GetWindowsDirectoryPath();

#else
  selectors = {{"TMPDIR", ""}, {"TMP", ""}, {"TEMP", ""}, {"TEMPDIR", ""}};
#ifdef __ANDROID__
  fallback_tmp = "/data/local/tmp";
#else
  fallback_tmp = "/tmp";
#endif
#endif

  std::vector<NativePathString> temp_dirs;
  for (const auto& sel : selectors) {
    NativePathString p;
    Status st = GetEnvVar(sel.env_var, &p);
    if (st.IsKeyError()) {
      // Environment variable absent, skip
      continue;
    }
    if (!st.ok()) {
      ARROW_LOG(WARNING) << "Failed getting env var '" << sel.env_var
                         << "': " << st.ToString();
      continue;
    }
    if (p.empty()) {
      // Environment variable set to empty string, skip
      continue;
    }
    if (sel.path_append.empty()) {
      temp_dirs.push_back(p);
    } else {
      temp_dirs.push_back(p + kNativeSep + sel.path_append);
    }
  }
  temp_dirs.push_back(fallback_tmp);
  return temp_dirs;
}

std::string MakeRandomName(int num_chars) {
  static const std::string chars = "0123456789abcdefghijklmnopqrstuvwxyz";
  std::random_device gen;
  std::uniform_int_distribution<int> dist(0, static_cast<int>(chars.length() - 1));

  std::string s;
  s.reserve(num_chars);
  for (int i = 0; i < num_chars; ++i) {
    s += chars[dist(gen)];
  }
  return s;
}
}  // namespace

Status TemporaryDir::Make(const std::string& prefix, std::unique_ptr<TemporaryDir>* out) {
  std::string suffix = MakeRandomName(8);
  NativePathString base_name;
  RETURN_NOT_OK(StringToNative(prefix + suffix, &base_name));

  auto base_dirs = GetPlatformTemporaryDirs();
  DCHECK_NE(base_dirs.size(), 0);

  auto st = Status::OK();
  for (const auto& p : base_dirs) {
    PlatformFilename fn(p + kNativeSep + base_name + kNativeSep);
    bool created = false;
    st = CreateDir(fn, &created);
    if (!st.ok()) {
      continue;
    }
    if (!created) {
      // XXX Should we retry with another random name?
      return Status::IOError("Path already exists: '", fn.ToString(), "'");
    } else {
      out->reset(new TemporaryDir(std::move(fn)));
      return Status::OK();
    }
  }

  DCHECK(!st.ok());
  return st;
}

#else  // ARROW_WITH_BOOST_FILESYSTEM

Status TemporaryDir::Make(const std::string& prefix, std::unique_ptr<TemporaryDir>* out) {
  return Status::NotImplemented("TemporaryDir not available in this Arrow build");
}

#endif

TemporaryDir::TemporaryDir(PlatformFilename&& path) : path_(std::move(path)) {}

TemporaryDir::~TemporaryDir() {
  Status st = DeleteDirTree(path_);
  if (!st.ok()) {
    ARROW_LOG(WARNING) << "When trying to delete temporary directory: " << st;
  }
}

SignalHandler::SignalHandler() : SignalHandler(static_cast<Callback>(nullptr)) {}

SignalHandler::SignalHandler(Callback cb) {
#if ARROW_HAVE_SIGACTION
  sa_.sa_handler = cb;
  sa_.sa_flags = 0;
  sigemptyset(&sa_.sa_mask);
#else
  cb_ = cb;
#endif
}

#if ARROW_HAVE_SIGACTION
SignalHandler::SignalHandler(const struct sigaction& sa) {
  memcpy(&sa_, &sa, sizeof(sa));
}
#endif

SignalHandler::Callback SignalHandler::callback() const {
#if ARROW_HAVE_SIGACTION
  return sa_.sa_handler;
#else
  return cb_;
#endif
}

#if ARROW_HAVE_SIGACTION
const struct sigaction& SignalHandler::action() const { return sa_; }
#endif

Status GetSignalHandler(int signum, SignalHandler* out) {
#if ARROW_HAVE_SIGACTION
  struct sigaction sa;
  int ret = sigaction(signum, nullptr, &sa);
  if (ret != 0) {
    // TODO more detailed message using errno
    return Status::IOError("sigaction call failed");
  }
  *out = SignalHandler(sa);
#else
  // To read the old handler, set the signal handler to something else temporarily
  SignalHandler::Callback cb = signal(signum, SIG_IGN);
  if (cb == SIG_ERR || signal(signum, cb) == SIG_ERR) {
    // TODO more detailed message using errno
    return Status::IOError("signal call failed");
  }
  *out = SignalHandler(cb);
#endif
  return Status::OK();
}

Status SetSignalHandler(int signum, const SignalHandler& handler,
                        SignalHandler* old_handler) {
#if ARROW_HAVE_SIGACTION
  struct sigaction old_sa;
  int ret = sigaction(signum, &handler.action(), &old_sa);
  if (ret != 0) {
    // TODO more detailed message using errno
    return Status::IOError("sigaction call failed");
  }
  if (old_handler != nullptr) {
    *old_handler = SignalHandler(old_sa);
  }
#else
  SignalHandler::Callback cb = signal(signum, handler.callback());
  if (cb == SIG_ERR) {
    // TODO more detailed message using errno
    return Status::IOError("signal call failed");
  }
  if (old_handler != nullptr) {
    *old_handler = SignalHandler(cb);
  }
#endif
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow

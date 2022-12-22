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

#if defined(sun) || defined(__sun)
// According to https://bugs.python.org/issue1759169#msg82201, __EXTENSIONS__
// is the best way to enable modern POSIX APIs, such as posix_madvise(), on Solaris.
// (see also
// https://github.com/illumos/illumos-gate/blob/master/usr/src/uts/common/sys/mman.h)
#undef __EXTENSIONS__
#define __EXTENSIONS__
#endif

#include "arrow/util/windows_compatibility.h"  // IWYU pragma: keep

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>  // IWYU pragma: keep

// ----------------------------------------------------------------------
// file compatibility stuff

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <share.h>
#else  // POSIX-like platforms
#include <dirent.h>
#endif

#ifdef _WIN32
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#else  // POSIX-like platforms
#include <sys/mman.h>
#include <unistd.h>
#endif

// define max read/write count
#ifdef _WIN32
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
#include "arrow/result.h"
#include "arrow/util/atfork_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/mutex.h"

// For filename conversion
#if defined(_WIN32)
#include "arrow/util/utf8.h"
#endif

#ifdef _WIN32
#include <psapi.h>

#elif __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>

#elif __linux__
#include <sys/sysinfo.h>
#include <fstream>
#endif

namespace arrow {

using internal::checked_cast;

namespace internal {

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

Result<NativePathString> StringToNative(std::string_view s) {
#if _WIN32
  return ::arrow::util::UTF8ToWideString(s);
#else
  return std::string(s);
#endif
}

#if _WIN32
Result<std::string> NativeToString(const NativePathString& ws) {
  return ::arrow::util::WideStringToUTF8(ws);
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

Status ValidatePath(std::string_view s) {
  if (s.find_first_of('\0') != std::string::npos) {
    return Status::Invalid("Embedded NUL char in path: '", s, "'");
  }
  return Status::OK();
}

}  // namespace

std::string ErrnoMessage(int errnum) { return std::strerror(errnum); }

#if _WIN32
std::string WinErrorMessage(int errnum) {
  constexpr DWORD max_n_chars = 1024;
  WCHAR utf16_message[max_n_chars];
  auto n_utf16_chars =
      FormatMessageW(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
                     errnum, 0, utf16_message, max_n_chars, NULL);
  if (n_utf16_chars == 0) {
    // Fallback
    std::stringstream ss;
    ss << "Windows error #" << errnum;
    return ss.str();
  }
  auto utf8_message_result =
      arrow::util::WideStringToUTF8(std::wstring(utf16_message, n_utf16_chars));
  if (!utf8_message_result.ok()) {
    std::stringstream ss;
    ss << "Windows error #" << errnum;
    ss << "; failed to convert error message to UTF-8: " << utf8_message_result.status();
    return ss.str();
  }
  return *utf8_message_result;
}
#endif

namespace {

const char kErrnoDetailTypeId[] = "arrow::ErrnoDetail";

class ErrnoDetail : public StatusDetail {
 public:
  explicit ErrnoDetail(int errnum) : errnum_(errnum) {}

  const char* type_id() const override { return kErrnoDetailTypeId; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "[errno " << errnum_ << "] " << ErrnoMessage(errnum_);
    return ss.str();
  }

  int errnum() const { return errnum_; }

 protected:
  int errnum_;
};

#if _WIN32
const char kWinErrorDetailTypeId[] = "arrow::WinErrorDetail";

// Map from a Windows error code to a `errno` value
//
// Most code in this function is taken from CPython's `PC/errmap.h`.
// Unlike CPython however, we return 0 for unknown / unsupported values.
int WinErrorToErrno(int winerror) {
  // Unwrap FACILITY_WIN32 HRESULT errors.
  if ((winerror & 0xFFFF0000) == 0x80070000) {
    winerror &= 0x0000FFFF;
  }

  // Winsock error codes (10000-11999) are errno values.
  if (winerror >= 10000 && winerror < 12000) {
    switch (winerror) {
      case WSAEINTR:
      case WSAEBADF:
      case WSAEACCES:
      case WSAEFAULT:
      case WSAEINVAL:
      case WSAEMFILE:
        // Winsock definitions of errno values. See WinSock2.h
        return winerror - 10000;
      default:
        return winerror;
    }
  }

  switch (winerror) {
    case ERROR_FILE_NOT_FOUND:        //    2
    case ERROR_PATH_NOT_FOUND:        //    3
    case ERROR_INVALID_DRIVE:         //   15
    case ERROR_NO_MORE_FILES:         //   18
    case ERROR_BAD_NETPATH:           //   53
    case ERROR_BAD_NET_NAME:          //   67
    case ERROR_BAD_PATHNAME:          //  161
    case ERROR_FILENAME_EXCED_RANGE:  //  206
      return ENOENT;

    case ERROR_BAD_ENVIRONMENT:  //   10
      return E2BIG;

    case ERROR_BAD_FORMAT:                 //   11
    case ERROR_INVALID_STARTING_CODESEG:   //  188
    case ERROR_INVALID_STACKSEG:           //  189
    case ERROR_INVALID_MODULETYPE:         //  190
    case ERROR_INVALID_EXE_SIGNATURE:      //  191
    case ERROR_EXE_MARKED_INVALID:         //  192
    case ERROR_BAD_EXE_FORMAT:             //  193
    case ERROR_ITERATED_DATA_EXCEEDS_64k:  //  194
    case ERROR_INVALID_MINALLOCSIZE:       //  195
    case ERROR_DYNLINK_FROM_INVALID_RING:  //  196
    case ERROR_IOPL_NOT_ENABLED:           //  197
    case ERROR_INVALID_SEGDPL:             //  198
    case ERROR_AUTODATASEG_EXCEEDS_64k:    //  199
    case ERROR_RING2SEG_MUST_BE_MOVABLE:   //  200
    case ERROR_RELOC_CHAIN_XEEDS_SEGLIM:   //  201
    case ERROR_INFLOOP_IN_RELOC_CHAIN:     //  202
      return ENOEXEC;

    case ERROR_INVALID_HANDLE:         //    6
    case ERROR_INVALID_TARGET_HANDLE:  //  114
    case ERROR_DIRECT_ACCESS_HANDLE:   //  130
      return EBADF;

    case ERROR_WAIT_NO_CHILDREN:    //  128
    case ERROR_CHILD_NOT_COMPLETE:  //  129
      return ECHILD;

    case ERROR_NO_PROC_SLOTS:        //   89
    case ERROR_MAX_THRDS_REACHED:    //  164
    case ERROR_NESTING_NOT_ALLOWED:  //  215
      return EAGAIN;

    case ERROR_ARENA_TRASHED:      //    7
    case ERROR_NOT_ENOUGH_MEMORY:  //    8
    case ERROR_INVALID_BLOCK:      //    9
    case ERROR_NOT_ENOUGH_QUOTA:   // 1816
      return ENOMEM;

    case ERROR_ACCESS_DENIED:            //    5
    case ERROR_CURRENT_DIRECTORY:        //   16
    case ERROR_WRITE_PROTECT:            //   19
    case ERROR_BAD_UNIT:                 //   20
    case ERROR_NOT_READY:                //   21
    case ERROR_BAD_COMMAND:              //   22
    case ERROR_CRC:                      //   23
    case ERROR_BAD_LENGTH:               //   24
    case ERROR_SEEK:                     //   25
    case ERROR_NOT_DOS_DISK:             //   26
    case ERROR_SECTOR_NOT_FOUND:         //   27
    case ERROR_OUT_OF_PAPER:             //   28
    case ERROR_WRITE_FAULT:              //   29
    case ERROR_READ_FAULT:               //   30
    case ERROR_GEN_FAILURE:              //   31
    case ERROR_SHARING_VIOLATION:        //   32
    case ERROR_LOCK_VIOLATION:           //   33
    case ERROR_WRONG_DISK:               //   34
    case ERROR_SHARING_BUFFER_EXCEEDED:  //   36
    case ERROR_NETWORK_ACCESS_DENIED:    //   65
    case ERROR_CANNOT_MAKE:              //   82
    case ERROR_FAIL_I24:                 //   83
    case ERROR_DRIVE_LOCKED:             //  108
    case ERROR_SEEK_ON_DEVICE:           //  132
    case ERROR_NOT_LOCKED:               //  158
    case ERROR_LOCK_FAILED:              //  167
    case 35:                             //   35 (undefined)
      return EACCES;

    case ERROR_FILE_EXISTS:     //   80
    case ERROR_ALREADY_EXISTS:  //  183
      return EEXIST;

    case ERROR_NOT_SAME_DEVICE:  //   17
      return EXDEV;

    case ERROR_DIRECTORY:  //  267 (bpo-12802)
      return ENOTDIR;

    case ERROR_TOO_MANY_OPEN_FILES:  //    4
      return EMFILE;

    case ERROR_DISK_FULL:  //  112
      return ENOSPC;

    case ERROR_BROKEN_PIPE:  //  109
    case ERROR_NO_DATA:      //  232 (bpo-13063)
      return EPIPE;

    case ERROR_DIR_NOT_EMPTY:  //  145
      return ENOTEMPTY;

    case ERROR_NO_UNICODE_TRANSLATION:  // 1113
      return EILSEQ;

    case ERROR_INVALID_FUNCTION:   //    1
    case ERROR_INVALID_ACCESS:     //   12
    case ERROR_INVALID_DATA:       //   13
    case ERROR_INVALID_PARAMETER:  //   87
    case ERROR_NEGATIVE_SEEK:      //  131
      return EINVAL;
    default:
      return 0;
  }
}

class WinErrorDetail : public StatusDetail {
 public:
  explicit WinErrorDetail(int errnum) : errnum_(errnum) {}

  const char* type_id() const override { return kWinErrorDetailTypeId; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "[Windows error " << errnum_ << "] " << WinErrorMessage(errnum_);
    return ss.str();
  }

  int errnum() const { return errnum_; }

  int equivalent_errno() const { return WinErrorToErrno(errnum_); }

 protected:
  int errnum_;
};
#endif

const char kSignalDetailTypeId[] = "arrow::SignalDetail";

class SignalDetail : public StatusDetail {
 public:
  explicit SignalDetail(int signum) : signum_(signum) {}

  const char* type_id() const override { return kSignalDetailTypeId; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "received signal " << signum_;
    return ss.str();
  }

  int signum() const { return signum_; }

 protected:
  int signum_;
};

}  // namespace

std::shared_ptr<StatusDetail> StatusDetailFromErrno(int errnum) {
  if (!errnum) {
    return nullptr;
  }
  return std::make_shared<ErrnoDetail>(errnum);
}

#if _WIN32
std::shared_ptr<StatusDetail> StatusDetailFromWinError(int errnum) {
  if (!errnum) {
    return nullptr;
  }
  return std::make_shared<WinErrorDetail>(errnum);
}
#endif

std::shared_ptr<StatusDetail> StatusDetailFromSignal(int signum) {
  return std::make_shared<SignalDetail>(signum);
}

int ErrnoFromStatus(const Status& status) {
  const auto detail = status.detail();
  if (detail != nullptr) {
    if (detail->type_id() == kErrnoDetailTypeId) {
      return checked_cast<const ErrnoDetail&>(*detail).errnum();
    }
#if _WIN32
    if (detail->type_id() == kWinErrorDetailTypeId) {
      return checked_cast<const WinErrorDetail&>(*detail).equivalent_errno();
    }
#endif
  }
  return 0;
}

int WinErrorFromStatus(const Status& status) {
#if _WIN32
  const auto detail = status.detail();
  if (detail != nullptr && detail->type_id() == kWinErrorDetailTypeId) {
    return checked_cast<const WinErrorDetail&>(*detail).errnum();
  }
#endif
  return 0;
}

int SignalFromStatus(const Status& status) {
  const auto detail = status.detail();
  if (detail != nullptr && detail->type_id() == kSignalDetailTypeId) {
    return checked_cast<const SignalDetail&>(*detail).signum();
  }
  return 0;
}

namespace {

Result<NativePathString> NativeReal(const NativePathString& path) {
#if _WIN32
  std::array<wchar_t, _MAX_PATH> resolved = {};
  if (_wfullpath(const_cast<wchar_t*>(path.c_str()), resolved.data(), resolved.size()) ==
      nullptr) {
    return IOErrorFromWinError(errno, "Failed to resolve real path");
  }
#else
  std::array<char, PATH_MAX + 1> resolved;
  if (realpath(path.c_str(), resolved.data()) == nullptr) {
    return IOErrorFromErrno(errno, "Failed to resolve real path");
  }
#endif
  return NativePathString{resolved.data()};
}

}  // namespace

//
// PlatformFilename implementation
//

struct PlatformFilename::Impl {
  Impl() = default;
  explicit Impl(NativePathString p) : native_(NativeSlashes(std::move(p))) {}

  NativePathString native_;

  // '/'-separated
  NativePathString generic() const { return GenericSlashes(native_); }
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

PlatformFilename::PlatformFilename(NativePathString path)
    : PlatformFilename(Impl{std::move(path)}) {}

PlatformFilename::PlatformFilename(const NativePathString::value_type* path)
    : PlatformFilename(NativePathString(path)) {}

bool PlatformFilename::operator==(const PlatformFilename& other) const {
  return impl_->native_ == other.impl_->native_;
}

bool PlatformFilename::operator!=(const PlatformFilename& other) const {
  return impl_->native_ != other.impl_->native_;
}

const NativePathString& PlatformFilename::ToNative() const { return impl_->native_; }

std::string PlatformFilename::ToString() const {
#if _WIN32
  auto result = NativeToString(impl_->generic());
  if (!result.ok()) {
    std::stringstream ss;
    ss << "<Unrepresentable filename: " << result.status().ToString() << ">";
    return ss.str();
  }
  return *std::move(result);
#else
  return impl_->generic();
#endif
}

PlatformFilename PlatformFilename::Parent() const {
  return PlatformFilename(NativeParent(ToNative()));
}

Result<PlatformFilename> PlatformFilename::Real() const {
  ARROW_ASSIGN_OR_RAISE(auto real, NativeReal(ToNative()));
  return PlatformFilename(std::move(real));
}

Result<PlatformFilename> PlatformFilename::FromString(std::string_view file_name) {
  RETURN_NOT_OK(ValidatePath(file_name));
  ARROW_ASSIGN_OR_RAISE(auto ns, StringToNative(file_name));
  return PlatformFilename(std::move(ns));
}

PlatformFilename PlatformFilename::Join(const PlatformFilename& child) const {
  if (impl_->native_.empty() || impl_->native_.back() == kNativeSep) {
    return PlatformFilename(Impl{impl_->native_ + child.impl_->native_});
  } else {
    return PlatformFilename(Impl{impl_->native_ + kNativeSep + child.impl_->native_});
  }
}

Result<PlatformFilename> PlatformFilename::Join(std::string_view child_name) const {
  ARROW_ASSIGN_OR_RAISE(auto child,
                        PlatformFilename::FromString(std::string(child_name)));
  return Join(child);
}

//
// Filesystem access routines
//

namespace {

Result<bool> DoCreateDir(const PlatformFilename& dir_path, bool create_parents) {
#ifdef _WIN32
  const auto s = dir_path.ToNative().c_str();
  if (CreateDirectoryW(s, nullptr)) {
    return true;
  }
  int errnum = GetLastError();
  if (errnum == ERROR_ALREADY_EXISTS) {
    const auto attrs = GetFileAttributesW(s);
    if (attrs == INVALID_FILE_ATTRIBUTES || !(attrs & FILE_ATTRIBUTE_DIRECTORY)) {
      // Note we propagate the original error, not the GetFileAttributesW() error
      return IOErrorFromWinError(ERROR_ALREADY_EXISTS, "Cannot create directory '",
                                 dir_path.ToString(), "': non-directory entry exists");
    }
    return false;
  }
  if (create_parents && errnum == ERROR_PATH_NOT_FOUND) {
    auto parent_path = dir_path.Parent();
    if (parent_path != dir_path) {
      RETURN_NOT_OK(DoCreateDir(parent_path, create_parents));
      return DoCreateDir(dir_path, false);  // Retry
    }
  }
  return IOErrorFromWinError(GetLastError(), "Cannot create directory '",
                             dir_path.ToString(), "'");
#else
  const auto s = dir_path.ToNative().c_str();
  if (mkdir(s, S_IRWXU | S_IRWXG | S_IRWXO) == 0) {
    return true;
  }
  if (errno == EEXIST) {
    struct stat st;
    if (stat(s, &st) || !S_ISDIR(st.st_mode)) {
      // Note we propagate the original errno, not the stat() errno
      return IOErrorFromErrno(EEXIST, "Cannot create directory '", dir_path.ToString(),
                              "': non-directory entry exists");
    }
    return false;
  }
  if (create_parents && errno == ENOENT) {
    auto parent_path = dir_path.Parent();
    if (parent_path != dir_path) {
      RETURN_NOT_OK(DoCreateDir(parent_path, create_parents));
      return DoCreateDir(dir_path, false);  // Retry
    }
  }
  return IOErrorFromErrno(errno, "Cannot create directory '", dir_path.ToString(), "'");
#endif
}

}  // namespace

Result<bool> CreateDir(const PlatformFilename& dir_path) {
  return DoCreateDir(dir_path, false);
}

Result<bool> CreateDirTree(const PlatformFilename& dir_path) {
  return DoCreateDir(dir_path, true);
}

#ifdef _WIN32

namespace {

void FindHandleDeleter(HANDLE* handle) {
  if (!FindClose(*handle)) {
    ARROW_LOG(WARNING) << "Cannot close directory handle: "
                       << WinErrorMessage(GetLastError());
  }
}

std::wstring PathWithoutTrailingSlash(const PlatformFilename& fn) {
  std::wstring path = fn.ToNative();
  while (!path.empty() && path.back() == kNativeSep) {
    path.pop_back();
  }
  return path;
}

Result<std::vector<WIN32_FIND_DATAW>> ListDirInternal(const PlatformFilename& dir_path) {
  WIN32_FIND_DATAW find_data;
  std::wstring pattern = PathWithoutTrailingSlash(dir_path) + L"\\*.*";
  HANDLE handle = FindFirstFileW(pattern.c_str(), &find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    return IOErrorFromWinError(GetLastError(), "Cannot list directory '",
                               dir_path.ToString(), "'");
  }

  std::unique_ptr<HANDLE, decltype(&FindHandleDeleter)> handle_guard(&handle,
                                                                     FindHandleDeleter);

  std::vector<WIN32_FIND_DATAW> results;
  do {
    // Skip "." and ".."
    if (find_data.cFileName[0] == L'.') {
      if (find_data.cFileName[1] == L'\0' ||
          (find_data.cFileName[1] == L'.' && find_data.cFileName[2] == L'\0')) {
        continue;
      }
    }
    results.push_back(find_data);
  } while (FindNextFileW(handle, &find_data));

  int errnum = GetLastError();
  if (errnum != ERROR_NO_MORE_FILES) {
    return IOErrorFromWinError(GetLastError(), "Cannot list directory '",
                               dir_path.ToString(), "'");
  }
  return results;
}

Status FindOneFile(const PlatformFilename& fn, WIN32_FIND_DATAW* find_data,
                   bool* exists = nullptr) {
  HANDLE handle = FindFirstFileW(PathWithoutTrailingSlash(fn).c_str(), find_data);
  if (handle == INVALID_HANDLE_VALUE) {
    int errnum = GetLastError();
    if (exists == nullptr ||
        (errnum != ERROR_PATH_NOT_FOUND && errnum != ERROR_FILE_NOT_FOUND)) {
      return IOErrorFromWinError(GetLastError(), "Cannot get information for path '",
                                 fn.ToString(), "'");
    }
    *exists = false;
  } else {
    if (exists != nullptr) {
      *exists = true;
    }
    FindHandleDeleter(&handle);
  }
  return Status::OK();
}

}  // namespace

Result<std::vector<PlatformFilename>> ListDir(const PlatformFilename& dir_path) {
  ARROW_ASSIGN_OR_RAISE(auto entries, ListDirInternal(dir_path));

  std::vector<PlatformFilename> results;
  results.reserve(entries.size());
  for (const auto& entry : entries) {
    results.emplace_back(std::wstring(entry.cFileName));
  }
  return results;
}

#else

Result<std::vector<PlatformFilename>> ListDir(const PlatformFilename& dir_path) {
  DIR* dir = opendir(dir_path.ToNative().c_str());
  if (dir == nullptr) {
    return IOErrorFromErrno(errno, "Cannot list directory '", dir_path.ToString(), "'");
  }

  auto dir_deleter = [](DIR* dir) -> void {
    if (closedir(dir) != 0) {
      ARROW_LOG(WARNING) << "Cannot close directory handle: " << ErrnoMessage(errno);
    }
  };
  std::unique_ptr<DIR, decltype(dir_deleter)> dir_guard(dir, dir_deleter);

  std::vector<PlatformFilename> results;
  errno = 0;
  struct dirent* entry = readdir(dir);
  while (entry != nullptr) {
    std::string path = entry->d_name;
    if (path != "." && path != "..") {
      results.emplace_back(std::move(path));
    }
    entry = readdir(dir);
  }
  if (errno != 0) {
    return IOErrorFromErrno(errno, "Cannot list directory '", dir_path.ToString(), "'");
  }
  return results;
}

#endif

namespace {

#ifdef _WIN32

Status DeleteDirTreeInternal(const PlatformFilename& dir_path);

// Remove a directory entry that's always a directory
Status DeleteDirEntryDir(const PlatformFilename& path, const WIN32_FIND_DATAW& entry,
                         bool remove_top_dir = true) {
  if ((entry.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0) {
    // It's a directory that doesn't have a reparse point => recurse
    RETURN_NOT_OK(DeleteDirTreeInternal(path));
  }
  if (remove_top_dir) {
    // Remove now empty directory or reparse point (e.g. symlink to dir)
    if (!RemoveDirectoryW(path.ToNative().c_str())) {
      return IOErrorFromWinError(GetLastError(), "Cannot delete directory entry '",
                                 path.ToString(), "': ");
    }
  }
  return Status::OK();
}

Status DeleteDirEntry(const PlatformFilename& path, const WIN32_FIND_DATAW& entry) {
  if ((entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0) {
    return DeleteDirEntryDir(path, entry);
  }
  // It's a non-directory entry, most likely a regular file
  if (!DeleteFileW(path.ToNative().c_str())) {
    return IOErrorFromWinError(GetLastError(), "Cannot delete file '", path.ToString(),
                               "': ");
  }
  return Status::OK();
}

Status DeleteDirTreeInternal(const PlatformFilename& dir_path) {
  ARROW_ASSIGN_OR_RAISE(auto entries, ListDirInternal(dir_path));
  for (const auto& entry : entries) {
    PlatformFilename path = dir_path.Join(PlatformFilename(entry.cFileName));
    RETURN_NOT_OK(DeleteDirEntry(path, entry));
  }
  return Status::OK();
}

Result<bool> DeleteDirContents(const PlatformFilename& dir_path, bool allow_not_found,
                               bool remove_top_dir) {
  bool exists = true;
  WIN32_FIND_DATAW entry;
  if (allow_not_found) {
    RETURN_NOT_OK(FindOneFile(dir_path, &entry, &exists));
  } else {
    // Will raise if dir_path does not exist
    RETURN_NOT_OK(FindOneFile(dir_path, &entry));
  }
  if (exists) {
    RETURN_NOT_OK(DeleteDirEntryDir(dir_path, entry, remove_top_dir));
  }
  return exists;
}

#else  // POSIX

Status LinkStat(const PlatformFilename& path, struct stat* lst, bool* exists = nullptr) {
  if (lstat(path.ToNative().c_str(), lst) != 0) {
    if (exists == nullptr || (errno != ENOENT && errno != ENOTDIR && errno != ELOOP)) {
      return IOErrorFromErrno(errno, "Cannot get information for path '", path.ToString(),
                              "'");
    }
    *exists = false;
  } else if (exists != nullptr) {
    *exists = true;
  }
  return Status::OK();
}

Status DeleteDirTreeInternal(const PlatformFilename& dir_path);

Status DeleteDirEntryDir(const PlatformFilename& path, const struct stat& lst,
                         bool remove_top_dir = true) {
  if (!S_ISLNK(lst.st_mode)) {
    // Not a symlink => delete contents recursively
    DCHECK(S_ISDIR(lst.st_mode));
    RETURN_NOT_OK(DeleteDirTreeInternal(path));
    if (remove_top_dir && rmdir(path.ToNative().c_str()) != 0) {
      return IOErrorFromErrno(errno, "Cannot delete directory entry '", path.ToString(),
                              "'");
    }
  } else {
    // Remove symlink
    if (remove_top_dir && unlink(path.ToNative().c_str()) != 0) {
      return IOErrorFromErrno(errno, "Cannot delete directory entry '", path.ToString(),
                              "'");
    }
  }
  return Status::OK();
}

Status DeleteDirEntry(const PlatformFilename& path, const struct stat& lst) {
  if (S_ISDIR(lst.st_mode)) {
    return DeleteDirEntryDir(path, lst);
  }
  if (unlink(path.ToNative().c_str()) != 0) {
    return IOErrorFromErrno(errno, "Cannot delete directory entry '", path.ToString(),
                            "'");
  }
  return Status::OK();
}

Status DeleteDirTreeInternal(const PlatformFilename& dir_path) {
  ARROW_ASSIGN_OR_RAISE(auto children, ListDir(dir_path));
  for (const auto& child : children) {
    struct stat lst;
    PlatformFilename full_path = dir_path.Join(child);
    RETURN_NOT_OK(LinkStat(full_path, &lst));
    RETURN_NOT_OK(DeleteDirEntry(full_path, lst));
  }
  return Status::OK();
}

Result<bool> DeleteDirContents(const PlatformFilename& dir_path, bool allow_not_found,
                               bool remove_top_dir) {
  bool exists = true;
  struct stat lst;
  if (allow_not_found) {
    RETURN_NOT_OK(LinkStat(dir_path, &lst, &exists));
  } else {
    // Will raise if dir_path does not exist
    RETURN_NOT_OK(LinkStat(dir_path, &lst));
  }
  if (exists) {
    if (!S_ISDIR(lst.st_mode) && !S_ISLNK(lst.st_mode)) {
      return Status::IOError("Cannot delete directory '", dir_path.ToString(),
                             "': not a directory");
    }
    RETURN_NOT_OK(DeleteDirEntryDir(dir_path, lst, remove_top_dir));
  }
  return exists;
}

#endif

}  // namespace

Result<bool> DeleteDirContents(const PlatformFilename& dir_path, bool allow_not_found) {
  return DeleteDirContents(dir_path, allow_not_found, /*remove_top_dir=*/false);
}

Result<bool> DeleteDirTree(const PlatformFilename& dir_path, bool allow_not_found) {
  return DeleteDirContents(dir_path, allow_not_found, /*remove_top_dir=*/true);
}

Result<bool> DeleteFile(const PlatformFilename& file_path, bool allow_not_found) {
#ifdef _WIN32
  if (DeleteFileW(file_path.ToNative().c_str())) {
    return true;
  } else {
    int errnum = GetLastError();
    if (!allow_not_found || errnum != ERROR_FILE_NOT_FOUND) {
      return IOErrorFromWinError(GetLastError(), "Cannot delete file '",
                                 file_path.ToString(), "'");
    }
  }
#else
  if (unlink(file_path.ToNative().c_str()) == 0) {
    return true;
  } else {
    if (!allow_not_found || errno != ENOENT) {
      return IOErrorFromErrno(errno, "Cannot delete file '", file_path.ToString(), "'");
    }
  }
#endif
  return false;
}

Result<bool> FileExists(const PlatformFilename& path) {
#ifdef _WIN32
  if (GetFileAttributesW(path.ToNative().c_str()) != INVALID_FILE_ATTRIBUTES) {
    return true;
  } else {
    int errnum = GetLastError();
    if (errnum != ERROR_PATH_NOT_FOUND && errnum != ERROR_FILE_NOT_FOUND) {
      return IOErrorFromWinError(GetLastError(), "Failed getting information for path '",
                                 path.ToString(), "'");
    }
    return false;
  }
#else
  struct stat st;
  if (stat(path.ToNative().c_str(), &st) == 0) {
    return true;
  } else {
    if (errno != ENOENT && errno != ENOTDIR) {
      return IOErrorFromErrno(errno, "Failed getting information for path '",
                              path.ToString(), "'");
    }
    return false;
  }
#endif
}

//
// Creating and destroying file descriptors
//

FileDescriptor::FileDescriptor(FileDescriptor&& other) : fd_(other.fd_.exchange(-1)) {}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) {
  int old_fd = fd_.exchange(other.fd_.exchange(-1));
  if (old_fd != -1) {
    CloseFromDestructor(old_fd);
  }
  return *this;
}

void FileDescriptor::CloseFromDestructor(int fd) {
  ARROW_WARN_NOT_OK(FileClose(fd), "Failed to close file descriptor");
}

FileDescriptor::~FileDescriptor() {
  int fd = fd_.load();
  if (fd != -1) {
    CloseFromDestructor(fd);
  }
}

Status FileDescriptor::Close() {
  int fd = fd_.exchange(-1);
  if (fd != -1) {
    return FileClose(fd);
  }
  return Status::OK();
}

int FileDescriptor::Detach() { return fd_.exchange(-1); }

static Result<int64_t> lseek64_compat(int fd, int64_t pos, int whence) {
#if defined(_WIN32)
  int64_t ret = _lseeki64(fd, pos, whence);
#else
  int64_t ret = lseek(fd, pos, whence);
#endif
  if (ret == -1) {
    return Status::IOError("lseek failed");
  }
  return ret;
}

Result<FileDescriptor> FileOpenReadable(const PlatformFilename& file_name) {
  FileDescriptor fd;
#if defined(_WIN32)
  HANDLE file_handle = CreateFileW(file_name.ToNative().c_str(), GENERIC_READ,
                                   FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                                   OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_handle == INVALID_HANDLE_VALUE) {
    return IOErrorFromWinError(GetLastError(), "Failed to open local file '",
                               file_name.ToString(), "'");
  }
  int ret = _open_osfhandle(reinterpret_cast<intptr_t>(file_handle),
                            _O_RDONLY | _O_BINARY | _O_NOINHERIT);
  if (ret == -1) {
    CloseHandle(file_handle);
    return IOErrorFromErrno(errno, "Failed to open local file '", file_name.ToString(),
                            "'");
  }
  fd = FileDescriptor(ret);
#else
  int ret = open(file_name.ToNative().c_str(), O_RDONLY);
  if (ret < 0) {
    return IOErrorFromErrno(errno, "Failed to open local file '", file_name.ToString(),
                            "'");
  }
  // open(O_RDONLY) succeeds on directories, check for it
  fd = FileDescriptor(ret);
  struct stat st;
  ret = fstat(fd.fd(), &st);
  if (ret == 0 && S_ISDIR(st.st_mode)) {
    return Status::IOError("Cannot open for reading: path '", file_name.ToString(),
                           "' is a directory");
  }
#endif

  return std::move(fd);
}

Result<FileDescriptor> FileOpenWritable(const PlatformFilename& file_name,
                                        bool write_only, bool truncate, bool append) {
  FileDescriptor fd;

#if defined(_WIN32)
  DWORD desired_access = GENERIC_WRITE;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  DWORD creation_disposition = OPEN_ALWAYS;

  if (truncate) {
    creation_disposition = CREATE_ALWAYS;
  }

  if (!write_only) {
    desired_access |= GENERIC_READ;
  }

  HANDLE file_handle =
      CreateFileW(file_name.ToNative().c_str(), desired_access, share_mode, NULL,
                  creation_disposition, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_handle == INVALID_HANDLE_VALUE) {
    return IOErrorFromWinError(GetLastError(), "Failed to open local file '",
                               file_name.ToString(), "'");
  }

  int ret = _open_osfhandle(reinterpret_cast<intptr_t>(file_handle),
                            _O_RDONLY | _O_BINARY | _O_NOINHERIT);
  if (ret == -1) {
    CloseHandle(file_handle);
    return IOErrorFromErrno(errno, "Failed to open local file '", file_name.ToString(),
                            "'");
  }
  fd = FileDescriptor(ret);
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

  int ret = open(file_name.ToNative().c_str(), oflag, 0666);
  if (ret == -1) {
    return IOErrorFromErrno(errno, "Failed to open local file '", file_name.ToString(),
                            "'");
  }
  fd = FileDescriptor(ret);
#endif

  if (append) {
    // Seek to end, as O_APPEND does not necessarily do it
    RETURN_NOT_OK(lseek64_compat(fd.fd(), 0, SEEK_END));
  }
  return std::move(fd);
}

Result<int64_t> FileTell(int fd) {
#if defined(_WIN32)
  int64_t current_pos = _telli64(fd);
  if (current_pos == -1) {
    return Status::IOError("_telli64 failed");
  }
  return current_pos;
#else
  return lseek64_compat(fd, 0, SEEK_CUR);
#endif
}

Result<Pipe> CreatePipe() {
  bool ok;
  int fds[2];
  Pipe pipe;

#if defined(_WIN32)
  ok = _pipe(fds, 4096, _O_BINARY) >= 0;
  if (ok) {
    pipe = {FileDescriptor(fds[0]), FileDescriptor(fds[1])};
  }
#elif defined(__linux__) && defined(__GLIBC__)
  // On Unix, we don't want the file descriptors to survive after an exec() call
  ok = pipe2(fds, O_CLOEXEC) >= 0;
  if (ok) {
    pipe = {FileDescriptor(fds[0]), FileDescriptor(fds[1])};
  }
#else
  auto set_cloexec = [](int fd) -> bool {
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0) {
      flags = fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
    }
    return flags >= 0;
  };

  ok = ::pipe(fds) >= 0;
  if (ok) {
    pipe = {FileDescriptor(fds[0]), FileDescriptor(fds[1])};
    ok &= set_cloexec(fds[0]);
    if (ok) {
      ok &= set_cloexec(fds[1]);
    }
  }
#endif
  if (!ok) {
    return IOErrorFromErrno(errno, "Error creating pipe");
  }

  return pipe;
}

Status SetPipeFileDescriptorNonBlocking(int fd) {
#if defined(_WIN32)
  const auto handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  DWORD mode = PIPE_NOWAIT;
  if (!SetNamedPipeHandleState(handle, &mode, nullptr, nullptr)) {
    return IOErrorFromWinError(GetLastError(), "Error making pipe non-blocking");
  }
#else
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    return IOErrorFromErrno(errno, "Error making pipe non-blocking");
  }
#endif
  return Status::OK();
}

namespace {

#ifdef WIN32
#define PIPE_WRITE _write
#define PIPE_READ _read
#else
#define PIPE_WRITE write
#define PIPE_READ read
#endif

class SelfPipeImpl : public SelfPipe, public std::enable_shared_from_this<SelfPipeImpl> {
  static constexpr uint64_t kEofPayload = 5804561806345822987ULL;

 public:
  explicit SelfPipeImpl(bool signal_safe) : signal_safe_(signal_safe) {}

  Status Init() {
    ARROW_ASSIGN_OR_RAISE(pipe_, CreatePipe());
    if (signal_safe_) {
      if (!please_shutdown_.is_lock_free()) {
        return Status::IOError("Cannot use non-lock-free atomic in a signal handler");
      }
      // We cannot afford blocking writes in a signal handler
      RETURN_NOT_OK(SetPipeFileDescriptorNonBlocking(pipe_.wfd.fd()));
    }

    atfork_handler_ = std::make_shared<AtForkHandler>(
        /*before=*/
        [weak_self = std::weak_ptr<SelfPipeImpl>(shared_from_this())] {
          auto self = weak_self.lock();
          if (self) {
            self->BeforeFork();
          }
          return self;
        },
        /*parent_after=*/
        [](std::any token) {
          auto self = std::any_cast<std::shared_ptr<SelfPipeImpl>>(std::move(token));
          self->ParentAfterFork();
        },
        /*child_after=*/
        [](std::any token) {
          auto self = std::any_cast<std::shared_ptr<SelfPipeImpl>>(std::move(token));
          self->ChildAfterFork();
        });
    RegisterAtFork(atfork_handler_);

    return Status::OK();
  }

  Result<uint64_t> Wait() override {
    if (pipe_.rfd.closed()) {
      // Already closed
      return ClosedPipe();
    }
    uint64_t payload = 0;
    char* buf = reinterpret_cast<char*>(&payload);
    auto buf_size = static_cast<int64_t>(sizeof(payload));
    while (buf_size > 0) {
      int64_t n_read = PIPE_READ(pipe_.rfd.fd(), buf, static_cast<uint32_t>(buf_size));
      if (n_read < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (pipe_.rfd.closed()) {
          return ClosedPipe();
        }
        return IOErrorFromErrno(errno, "Failed reading from self-pipe");
      }
      buf += n_read;
      buf_size -= n_read;
    }
    if (payload == kEofPayload && please_shutdown_.load()) {
      RETURN_NOT_OK(pipe_.rfd.Close());
      return ClosedPipe();
    }
    return payload;
  }

  // XXX return StatusCode from here?
  void Send(uint64_t payload) override {
    if (signal_safe_) {
      int saved_errno = errno;
      DoSend(payload);
      errno = saved_errno;
    } else {
      DoSend(payload);
    }
  }

  Status Shutdown() override {
    please_shutdown_.store(true);
    errno = 0;
    if (!DoSend(kEofPayload)) {
      if (errno) {
        return IOErrorFromErrno(errno, "Could not shutdown self-pipe");
      } else if (!pipe_.wfd.closed()) {
        return Status::UnknownError("Could not shutdown self-pipe");
      }
    }
    return pipe_.wfd.Close();
  }

  ~SelfPipeImpl() { ARROW_WARN_NOT_OK(Shutdown(), "On self-pipe destruction"); }

 protected:
  void BeforeFork() {}

  void ParentAfterFork() {}

  void ChildAfterFork() {
    // Close and recreate pipe, to avoid interfering with parent.
    const bool was_closed = pipe_.rfd.closed() || pipe_.wfd.closed();
    ARROW_CHECK_OK(pipe_.Close());
    if (!was_closed) {
      ARROW_CHECK_OK(CreatePipe().Value(&pipe_));
    }
  }

  Status ClosedPipe() const { return Status::Invalid("Self-pipe closed"); }

  bool DoSend(uint64_t payload) {
    // This needs to be async-signal safe as it's called from Send()
    if (pipe_.wfd.closed()) {
      // Already closed
      return false;
    }
    const char* buf = reinterpret_cast<const char*>(&payload);
    auto buf_size = static_cast<int64_t>(sizeof(payload));
    while (buf_size > 0) {
      int64_t n_written =
          PIPE_WRITE(pipe_.wfd.fd(), buf, static_cast<uint32_t>(buf_size));
      if (n_written < 0) {
        if (errno == EINTR) {
          continue;
        } else {
          // Perhaps EAGAIN if non-blocking, or EBADF if closed in the meantime?
          // In any case, we can't do anything more here.
          break;
        }
      }
      buf += n_written;
      buf_size -= n_written;
    }
    return buf_size == 0;
  }

  const bool signal_safe_;
  Pipe pipe_;
  std::atomic<bool> please_shutdown_{false};

  std::shared_ptr<AtForkHandler> atfork_handler_;
};

#undef PIPE_WRITE
#undef PIPE_READ

}  // namespace

Result<std::shared_ptr<SelfPipe>> SelfPipe::Make(bool signal_safe) {
  auto ptr = std::make_shared<SelfPipeImpl>(signal_safe);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

SelfPipe::~SelfPipe() = default;

namespace {

Status StatusFromMmapErrno(const char* prefix) {
#ifdef _WIN32
  errno = __map_mman_error(GetLastError(), EPERM);
#endif
  return IOErrorFromErrno(errno, prefix);
}

int64_t GetPageSizeInternal() {
#if defined(__APPLE__)
  return getpagesize();
#elif defined(_WIN32)
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return si.dwPageSize;
#else
  errno = 0;
  const auto ret = sysconf(_SC_PAGESIZE);
  if (ret == -1) {
    ARROW_LOG(FATAL) << "sysconf(_SC_PAGESIZE) failed: " << ErrnoMessage(errno);
  }
  return static_cast<int64_t>(ret);
#endif
}

}  // namespace

int64_t GetPageSize() {
  static const int64_t kPageSize = GetPageSizeInternal();  // cache it
  return kPageSize;
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
    return StatusFromMmapErrno("UnmapViewOfFile failed");
  }

  h = reinterpret_cast<HANDLE>(_get_osfhandle(fildes));
  if (h == INVALID_HANDLE_VALUE) {
    return StatusFromMmapErrno("Cannot get file handle");
  }

  uint64_t new_size64 = new_size;
  LONG new_size_low = static_cast<LONG>(new_size64 & 0xFFFFFFFFUL);
  LONG new_size_high = static_cast<LONG>((new_size64 >> 32) & 0xFFFFFFFFUL);

  SetFilePointer(h, new_size_low, &new_size_high, FILE_BEGIN);
  SetEndOfFile(h);
  fm = CreateFileMapping(h, NULL, PAGE_READWRITE, 0, 0, "");
  if (fm == NULL) {
    return StatusFromMmapErrno("CreateFileMapping failed");
  }
  *new_addr = MapViewOfFile(fm, FILE_MAP_WRITE, 0, 0, new_size);
  CloseHandle(fm);
  if (new_addr == NULL) {
    return StatusFromMmapErrno("MapViewOfFile failed");
  }
  return Status::OK();
#elif defined(__linux__)
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromMmapErrno("ftruncate failed");
  }
  *new_addr = mremap(addr, old_size, new_size, MREMAP_MAYMOVE);
  if (*new_addr == MAP_FAILED) {
    return StatusFromMmapErrno("mremap failed");
  }
  return Status::OK();
#else
  // we have to close the mmap first, truncate the file to the new size
  // and recreate the mmap
  if (munmap(addr, old_size) == -1) {
    return StatusFromMmapErrno("munmap failed");
  }
  if (ftruncate(fildes, new_size) == -1) {
    return StatusFromMmapErrno("ftruncate failed");
  }
  // we set READ / WRITE flags on the new map, since we could only have
  // unlarged a RW map in the first place
  *new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fildes, 0);
  if (*new_addr == MAP_FAILED) {
    return StatusFromMmapErrno("mmap failed");
  }
  return Status::OK();
#endif
}

Status MemoryAdviseWillNeed(const std::vector<MemoryRegion>& regions) {
  const auto page_size = static_cast<size_t>(GetPageSize());
  DCHECK_GT(page_size, 0);
  const size_t page_mask = ~(page_size - 1);
  DCHECK_EQ(page_mask & page_size, page_size);

  auto align_region = [=](const MemoryRegion& region) -> MemoryRegion {
    const auto addr = reinterpret_cast<uintptr_t>(region.addr);
    const auto aligned_addr = addr & page_mask;
    DCHECK_LT(addr - aligned_addr, page_size);
    return {reinterpret_cast<void*>(aligned_addr),
            region.size + static_cast<size_t>(addr - aligned_addr)};
  };

#ifdef _WIN32
  // PrefetchVirtualMemory() is available on Windows 8 or later
  struct PrefetchEntry {  // Like WIN32_MEMORY_RANGE_ENTRY
    void* VirtualAddress;
    size_t NumberOfBytes;

    PrefetchEntry(const MemoryRegion& region)  // NOLINT runtime/explicit
        : VirtualAddress(region.addr), NumberOfBytes(region.size) {}
  };
  using PrefetchVirtualMemoryFunc = BOOL (*)(HANDLE, ULONG_PTR, PrefetchEntry*, ULONG);
  static const auto prefetch_virtual_memory = reinterpret_cast<PrefetchVirtualMemoryFunc>(
      GetProcAddress(GetModuleHandleW(L"kernel32.dll"), "PrefetchVirtualMemory"));
  if (prefetch_virtual_memory != nullptr) {
    std::vector<PrefetchEntry> entries;
    entries.reserve(regions.size());
    for (const auto& region : regions) {
      if (region.size != 0) {
        entries.emplace_back(align_region(region));
      }
    }
    if (!entries.empty() &&
        !prefetch_virtual_memory(GetCurrentProcess(),
                                 static_cast<ULONG_PTR>(entries.size()), entries.data(),
                                 0)) {
      return IOErrorFromWinError(GetLastError(), "PrefetchVirtualMemory failed");
    }
  }
  return Status::OK();
#elif defined(POSIX_MADV_WILLNEED)
  for (const auto& region : regions) {
    if (region.size != 0) {
      const auto aligned = align_region(region);
      int err = posix_madvise(aligned.addr, aligned.size, POSIX_MADV_WILLNEED);
      // EBADF can be returned on Linux in the following cases:
      // - the kernel version is older than 3.9
      // - the kernel was compiled with CONFIG_SWAP disabled (ARROW-9577)
      if (err != 0 && err != EBADF) {
        return IOErrorFromErrno(err, "posix_madvise failed");
      }
    }
  }
  return Status::OK();
#else
  return Status::OK();
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
  return lseek64_compat(fd, pos, whence).status();
}

Status FileSeek(int fd, int64_t pos) { return FileSeek(fd, pos, SEEK_SET); }

Result<int64_t> FileGetSize(int fd) {
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
    RETURN_NOT_OK(FileTell(fd));
  } else if (st.st_size < 0) {
    return Status::IOError("error getting file size");
  }
  return st.st_size;
}

//
// Reading data
//

static inline int64_t pread_compat(int fd, void* buf, int64_t nbytes, int64_t pos) {
#if defined(_WIN32)
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  DWORD dwBytesRead = 0;
  OVERLAPPED overlapped = {};
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
  int64_t ret;
  do {
    ret = static_cast<int64_t>(
        pread(fd, buf, static_cast<size_t>(nbytes), static_cast<off_t>(pos)));
  } while (ret == -1 && errno == EINTR);
  return ret;
#endif
}

Result<int64_t> FileRead(int fd, uint8_t* buffer, int64_t nbytes) {
#if defined(_WIN32)
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
#endif
  int64_t total_bytes_read = 0;

  while (total_bytes_read < nbytes) {
    const int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - total_bytes_read);
    int64_t bytes_read = 0;
#if defined(_WIN32)
    DWORD dwBytesRead = 0;
    if (!ReadFile(handle, buffer, static_cast<uint32_t>(chunksize), &dwBytesRead,
                  nullptr)) {
      auto errnum = GetLastError();
      // Return a normal EOF when the write end of a pipe was closed
      if (errnum != ERROR_HANDLE_EOF && errnum != ERROR_BROKEN_PIPE) {
        return IOErrorFromWinError(GetLastError(), "Error reading bytes from file");
      }
    }
    bytes_read = dwBytesRead;
#else
    bytes_read = static_cast<int64_t>(read(fd, buffer, static_cast<size_t>(chunksize)));
    if (bytes_read == -1) {
      if (errno == EINTR) {
        continue;
      }
      return IOErrorFromErrno(errno, "Error reading bytes from file");
    }
#endif

    if (bytes_read == 0) {
      // EOF
      break;
    }
    buffer += bytes_read;
    total_bytes_read += bytes_read;
  }
  return total_bytes_read;
}

Result<int64_t> FileReadAt(int fd, uint8_t* buffer, int64_t position, int64_t nbytes) {
  int64_t bytes_read = 0;

  while (bytes_read < nbytes) {
    int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - bytes_read);
    int64_t ret = pread_compat(fd, buffer, chunksize, position);

    if (ret == -1) {
      return IOErrorFromErrno(errno, "Error reading bytes from file");
    }
    if (ret == 0) {
      // EOF
      break;
    }
    buffer += ret;
    position += ret;
    bytes_read += ret;
  }
  return bytes_read;
}

//
// Writing data
//

Status FileWrite(int fd, const uint8_t* buffer, const int64_t nbytes) {
  int64_t bytes_written = 0;

  while (bytes_written < nbytes) {
    const int64_t chunksize =
        std::min(static_cast<int64_t>(ARROW_MAX_IO_CHUNKSIZE), nbytes - bytes_written);
#if defined(_WIN32)
    int64_t ret = static_cast<int64_t>(
        _write(fd, buffer + bytes_written, static_cast<uint32_t>(chunksize)));
#else
    int64_t ret = static_cast<int64_t>(
        write(fd, buffer + bytes_written, static_cast<size_t>(chunksize)));
    if (ret == -1 && errno == EINTR) {
      continue;
    }
#endif

    if (ret == -1) {
      return IOErrorFromErrno(errno, "Error writing bytes to file");
    }
    bytes_written += ret;
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
    return IOErrorFromErrno(errno_actual, "Error writing bytes to file");
  }
  return Status::OK();
}

//
// Environment variables
//

Result<std::string> GetEnvVar(const char* name) {
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
  return std::string(c_str);
#else
  char* c_str = getenv(name);
  if (c_str == nullptr) {
    return Status::KeyError("environment variable undefined");
  }
  return std::string(c_str);
#endif
}

Result<std::string> GetEnvVar(const std::string& name) { return GetEnvVar(name.c_str()); }

#ifdef _WIN32
Result<NativePathString> GetEnvVarNative(const std::string& name) {
  NativePathString w_name;
  constexpr int32_t bufsize = 2000;
  wchar_t w_str[bufsize];

  ARROW_ASSIGN_OR_RAISE(w_name, StringToNative(name));
  auto res = GetEnvironmentVariableW(w_name.c_str(), w_str, bufsize);
  if (res >= bufsize) {
    return Status::CapacityError("environment variable value too long");
  } else if (res == 0) {
    return Status::KeyError("environment variable undefined");
  }
  return NativePathString(w_str);
}

Result<NativePathString> GetEnvVarNative(const char* name) {
  return GetEnvVarNative(std::string(name));
}

#else

Result<NativePathString> GetEnvVarNative(const std::string& name) {
  return GetEnvVar(name);
}

Result<NativePathString> GetEnvVarNative(const char* name) { return GetEnvVar(name); }
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
    auto result = GetEnvVarNative(sel.env_var);
    if (result.status().IsKeyError()) {
      // Environment variable absent, skip
      continue;
    }
    if (!result.ok()) {
      ARROW_LOG(WARNING) << "Failed getting env var '" << sel.env_var
                         << "': " << result.status().ToString();
      continue;
    }
    NativePathString p = *std::move(result);
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
  std::default_random_engine gen(
      static_cast<std::default_random_engine::result_type>(GetRandomSeed()));
  std::uniform_int_distribution<int> dist(0, static_cast<int>(chars.length() - 1));

  std::string s;
  s.reserve(num_chars);
  for (int i = 0; i < num_chars; ++i) {
    s += chars[dist(gen)];
  }
  return s;
}

}  // namespace

Result<std::unique_ptr<TemporaryDir>> TemporaryDir::Make(const std::string& prefix) {
  const int kNumChars = 8;

  NativePathString base_name;

  auto MakeBaseName = [&]() {
    std::string suffix = MakeRandomName(kNumChars);
    return StringToNative(prefix + suffix);
  };

  auto TryCreatingDirectory =
      [&](const NativePathString& base_dir) -> Result<std::unique_ptr<TemporaryDir>> {
    Status st;
    for (int attempt = 0; attempt < 3; ++attempt) {
      PlatformFilename fn_base_dir(base_dir);
      PlatformFilename fn_base_name(base_name + kNativeSep);
      PlatformFilename fn = fn_base_dir.Join(fn_base_name);
      auto result = CreateDir(fn);
      if (!result.ok()) {
        // Probably a permissions error or a non-existing base_dir
        return nullptr;
      }
      if (*result) {
        return std::unique_ptr<TemporaryDir>(new TemporaryDir(std::move(fn)));
      }
      // The random name already exists in base_dir, try with another name
      st = Status::IOError("Path already exists: '", fn.ToString(), "'");
      ARROW_ASSIGN_OR_RAISE(base_name, MakeBaseName());
    }
    return st;
  };

  ARROW_ASSIGN_OR_RAISE(base_name, MakeBaseName());

  auto base_dirs = GetPlatformTemporaryDirs();
  DCHECK_NE(base_dirs.size(), 0);

  for (const auto& base_dir : base_dirs) {
    ARROW_ASSIGN_OR_RAISE(auto ptr, TryCreatingDirectory(base_dir));
    if (ptr) {
      return std::move(ptr);
    }
    // Cannot create in this directory, try the next one
  }

  return Status::IOError(
      "Cannot create temporary subdirectory in any "
      "of the platform temporary directories");
}

TemporaryDir::TemporaryDir(PlatformFilename&& path) : path_(std::move(path)) {}

TemporaryDir::~TemporaryDir() {
  ARROW_WARN_NOT_OK(DeleteDirTree(path_).status(),
                    "When trying to delete temporary directory");
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

Result<SignalHandler> GetSignalHandler(int signum) {
#if ARROW_HAVE_SIGACTION
  struct sigaction sa;
  int ret = sigaction(signum, nullptr, &sa);
  if (ret != 0) {
    // TODO more detailed message using errno
    return Status::IOError("sigaction call failed");
  }
  return SignalHandler(sa);
#else
  // To read the old handler, set the signal handler to something else temporarily
  SignalHandler::Callback cb = signal(signum, SIG_IGN);
  if (cb == SIG_ERR || signal(signum, cb) == SIG_ERR) {
    // TODO more detailed message using errno
    return Status::IOError("signal call failed");
  }
  return SignalHandler(cb);
#endif
}

Result<SignalHandler> SetSignalHandler(int signum, const SignalHandler& handler) {
#if ARROW_HAVE_SIGACTION
  struct sigaction old_sa;
  int ret = sigaction(signum, &handler.action(), &old_sa);
  if (ret != 0) {
    // TODO more detailed message using errno
    return Status::IOError("sigaction call failed");
  }
  return SignalHandler(old_sa);
#else
  SignalHandler::Callback cb = signal(signum, handler.callback());
  if (cb == SIG_ERR) {
    // TODO more detailed message using errno
    return Status::IOError("signal call failed");
  }
  return SignalHandler(cb);
#endif
  return Status::OK();
}

void ReinstateSignalHandler(int signum, SignalHandler::Callback handler) {
#if !ARROW_HAVE_SIGACTION
  // Cannot report any errors from signal() (but there shouldn't be any)
  signal(signum, handler);
#endif
}

Status SendSignal(int signum) {
  if (raise(signum) == 0) {
    return Status::OK();
  }
  if (errno == EINVAL) {
    return Status::Invalid("Invalid signal number ", signum);
  }
  return IOErrorFromErrno(errno, "Failed to raise signal");
}

Status SendSignalToThread(int signum, uint64_t thread_id) {
#ifdef _WIN32
  return Status::NotImplemented("Cannot send signal to specific thread on Windows");
#else
  // Have to use a C-style cast because pthread_t can be a pointer *or* integer type
  int r = pthread_kill((pthread_t)thread_id, signum);  // NOLINT readability-casting
  if (r == 0) {
    return Status::OK();
  }
  if (r == EINVAL) {
    return Status::Invalid("Invalid signal number ", signum);
  }
  return IOErrorFromErrno(r, "Failed to raise signal");
#endif
}

namespace {

int64_t GetPid() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

std::mt19937_64 GetSeedGenerator() {
  // Initialize Mersenne Twister PRNG with a true random seed.
  // Make sure to mix in process id to minimize risks of clashes when parallel testing.
#ifdef ARROW_VALGRIND
  // Valgrind can crash, hang or enter an infinite loop on std::random_device,
  // use a crude initializer instead.
  const uint8_t dummy = 0;
  ARROW_UNUSED(dummy);
  std::mt19937_64 seed_gen(reinterpret_cast<uintptr_t>(&dummy) ^
                           static_cast<uintptr_t>(GetPid()));
#else
  std::random_device true_random;
  std::mt19937_64 seed_gen(static_cast<uint64_t>(true_random()) ^
                           (static_cast<uint64_t>(true_random()) << 32) ^
                           static_cast<uint64_t>(GetPid()));
#endif
  return seed_gen;
}

}  // namespace

int64_t GetRandomSeed() {
  // The process-global seed generator to aims to avoid calling std::random_device
  // unless truly necessary (it can block on some systems, see ARROW-10287).
  static auto seed_gen = GetSeedGenerator();
  static std::mutex seed_gen_mutex;

  std::lock_guard<std::mutex> lock(seed_gen_mutex);
  return static_cast<int64_t>(seed_gen());
}

uint64_t GetThreadId() {
  uint64_t equiv{0};
  // std::thread::id is trivially copyable as per C++ spec,
  // so type punning as a uint64_t should work
  static_assert(sizeof(std::thread::id) <= sizeof(uint64_t),
                "std::thread::id can't fit into uint64_t");
  const auto tid = std::this_thread::get_id();
  memcpy(&equiv, reinterpret_cast<const void*>(&tid), sizeof(tid));
  return equiv;
}

uint64_t GetOptionalThreadId() {
  auto tid = GetThreadId();
  return (tid == 0) ? tid - 1 : tid;
}

// Returns the current resident set size (physical memory use) measured
// in bytes, or zero if the value cannot be determined on this OS.
int64_t GetCurrentRSS() {
#if defined(_WIN32)
  // Windows --------------------------------------------------
  PROCESS_MEMORY_COUNTERS info;
  GetProcessMemoryInfo(GetCurrentProcess(), &info, sizeof(info));
  return static_cast<int64_t>(info.WorkingSetSize);

#elif defined(__APPLE__)
  // OSX ------------------------------------------------------
  struct mach_task_basic_info info;
  mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) !=
      KERN_SUCCESS) {
    ARROW_LOG(WARNING) << "Can't resolve RSS value";
    return 0;
  }
  return static_cast<int64_t>(info.resident_size);

#elif defined(__linux__)
  // Linux ----------------------------------------------------
  int64_t rss = 0L;

  std::ifstream fp("/proc/self/statm");
  if (fp) {
    fp >> rss;
    return rss * sysconf(_SC_PAGESIZE);
  } else {
    ARROW_LOG(WARNING) << "Can't resolve RSS value from /proc/self/statm";
    return 0;
  }

#else
  // AIX, BSD, Solaris, and Unknown OS ------------------------
  return 0;  // Unsupported.
#endif
}

int64_t GetTotalMemoryBytes() {
#if defined(_WIN32)
  ULONGLONG result_kb;
  if (!GetPhysicallyInstalledSystemMemory(&result_kb)) {
    ARROW_LOG(WARNING) << "Failed to resolve total RAM size: "
                       << std::strerror(GetLastError());
    return -1;
  }
  return static_cast<int64_t>(result_kb * 1024);
#elif defined(__APPLE__)
  int64_t result;
  size_t size = sizeof(result);
  if (sysctlbyname("hw.memsize", &result, &size, nullptr, 0) == -1) {
    ARROW_LOG(WARNING) << "Failed to resolve total RAM size";
    return -1;
  }
  return result;
#elif defined(__linux__)
  struct sysinfo info;
  if (sysinfo(&info) == -1) {
    ARROW_LOG(WARNING) << "Failed to resolve total RAM size: " << std::strerror(errno);
    return -1;
  }
  return static_cast<int64_t>(info.totalram * info.mem_unit);
#else
  return 0;
#endif
}

}  // namespace internal
}  // namespace arrow

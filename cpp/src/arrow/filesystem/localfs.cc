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

#include <chrono>
#include <cstring>
#include <sstream>
#include <utility>

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#else
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#endif

#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/file.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {
namespace fs {

using ::arrow::internal::IOErrorFromErrno;
#ifdef _WIN32
using ::arrow::internal::IOErrorFromWinError;
#endif
using ::arrow::internal::NativePathString;
using ::arrow::internal::PlatformFilename;

namespace internal {

#ifdef _WIN32
static bool IsDriveLetter(char c) {
  // Can't use locale-dependent functions from the C/C++ stdlib
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}
#endif

bool DetectAbsolutePath(const std::string& s) {
  // Is it a /-prefixed local path?
  if (s.length() >= 1 && s[0] == '/') {
    return true;
  }
#ifdef _WIN32
  // Is it a \-prefixed local path?
  if (s.length() >= 1 && s[0] == '\\') {
    return true;
  }
  // Does it start with a drive letter in addition to being /- or \-prefixed,
  // e.g. "C:\..."?
  if (s.length() >= 3 && s[1] == ':' && (s[2] == '/' || s[2] == '\\') &&
      IsDriveLetter(s[0])) {
    return true;
  }
#endif
  return false;
}

}  // namespace internal

namespace {

#ifdef _WIN32

std::string NativeToString(const NativePathString& ns) {
  PlatformFilename fn(ns);
  return fn.ToString();
}

TimePoint ToTimePoint(FILETIME ft) {
  // Hundreds of nanoseconds between January 1, 1601 (UTC) and the Unix epoch.
  static constexpr int64_t kFileTimeEpoch = 11644473600LL * 10000000;

  int64_t hundreds = (static_cast<int64_t>(ft.dwHighDateTime) << 32) + ft.dwLowDateTime -
                     kFileTimeEpoch;  // hundreds of ns since Unix epoch
  std::chrono::nanoseconds ns_count(100 * hundreds);
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileInfo FileInformationToFileInfo(const BY_HANDLE_FILE_INFORMATION& information) {
  FileInfo info;
  if (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
    info.set_type(FileType::Directory);
    info.set_size(kNoSize);
  } else {
    // Regular file
    info.set_type(FileType::File);
    info.set_size((static_cast<int64_t>(information.nFileSizeHigh) << 32) +
                  information.nFileSizeLow);
  }
  info.set_mtime(ToTimePoint(information.ftLastWriteTime));
  return info;
}

Result<FileInfo> StatFile(const std::wstring& path) {
  HANDLE h;
  std::string bytes_path = NativeToString(path);
  FileInfo info;

  /* Inspired by CPython, see Modules/posixmodule.c */
  h = CreateFileW(path.c_str(), FILE_READ_ATTRIBUTES, /* desired access */
                  0,                                  /* share mode */
                  NULL,                               /* security attributes */
                  OPEN_EXISTING,
                  /* FILE_FLAG_BACKUP_SEMANTICS is required to open a directory */
                  FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS, NULL);

  if (h == INVALID_HANDLE_VALUE) {
    DWORD err = GetLastError();
    if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
      info.set_path(bytes_path);
      info.set_type(FileType::NotFound);
      info.set_mtime(kNoTime);
      info.set_size(kNoSize);
      return info;
    } else {
      return IOErrorFromWinError(GetLastError(), "Failed querying information for path '",
                                 bytes_path, "'");
    }
  }
  BY_HANDLE_FILE_INFORMATION information;
  if (!GetFileInformationByHandle(h, &information)) {
    CloseHandle(h);
    return IOErrorFromWinError(GetLastError(), "Failed querying information for path '",
                               bytes_path, "'");
  }
  CloseHandle(h);
  info = FileInformationToFileInfo(information);
  info.set_path(bytes_path);
  return info;
}

#else  // POSIX systems

TimePoint ToTimePoint(const struct timespec& s) {
  std::chrono::nanoseconds ns_count(static_cast<int64_t>(s.tv_sec) * 1000000000 +
                                    static_cast<int64_t>(s.tv_nsec));
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileInfo StatToFileInfo(const struct stat& s) {
  FileInfo info;
  if (S_ISREG(s.st_mode)) {
    info.set_type(FileType::File);
    info.set_size(static_cast<int64_t>(s.st_size));
  } else if (S_ISDIR(s.st_mode)) {
    info.set_type(FileType::Directory);
    info.set_size(kNoSize);
  } else {
    info.set_type(FileType::Unknown);
    info.set_size(kNoSize);
  }
#ifdef __APPLE__
  // macOS doesn't use the POSIX-compliant spelling
  info.set_mtime(ToTimePoint(s.st_mtimespec));
#else
  info.set_mtime(ToTimePoint(s.st_mtim));
#endif
  return info;
}

Result<FileInfo> StatFile(const std::string& path) {
  FileInfo info;
  struct stat s;
  int r = stat(path.c_str(), &s);
  if (r == -1) {
    if (errno == ENOENT || errno == ENOTDIR || errno == ELOOP) {
      info.set_type(FileType::NotFound);
      info.set_mtime(kNoTime);
      info.set_size(kNoSize);
    } else {
      return IOErrorFromErrno(errno, "Failed stat()ing path '", path, "'");
    }
  } else {
    info = StatToFileInfo(s);
  }
  info.set_path(path);
  return info;
}

#endif

Status StatSelector(const PlatformFilename& dir_fn, const FileSelector& select,
                    int32_t nesting_depth, std::vector<FileInfo>* out) {
  auto result = ListDir(dir_fn);
  if (!result.ok()) {
    auto status = result.status();
    if (select.allow_not_found && status.IsIOError()) {
      ARROW_ASSIGN_OR_RAISE(bool exists, FileExists(dir_fn));
      if (!exists) {
        return Status::OK();
      }
    }
    return status;
  }

  for (const auto& child_fn : *result) {
    PlatformFilename full_fn = dir_fn.Join(child_fn);
    ARROW_ASSIGN_OR_RAISE(FileInfo info, StatFile(full_fn.ToNative()));
    if (info.type() != FileType::NotFound) {
      out->push_back(std::move(info));
    }
    if (nesting_depth < select.max_recursion && select.recursive &&
        info.type() == FileType::Directory) {
      RETURN_NOT_OK(StatSelector(full_fn, select, nesting_depth + 1, out));
    }
  }
  return Status::OK();
}

}  // namespace

LocalFileSystemOptions LocalFileSystemOptions::Defaults() {
  return LocalFileSystemOptions();
}

bool LocalFileSystemOptions::Equals(const LocalFileSystemOptions& other) const {
  return use_mmap == other.use_mmap;
}

Result<LocalFileSystemOptions> LocalFileSystemOptions::FromUri(
    const ::arrow::internal::Uri& uri, std::string* out_path) {
  if (!uri.username().empty() || !uri.password().empty()) {
    return Status::Invalid("Unsupported username or password in local URI: '",
                           uri.ToString(), "'");
  }
  std::string path;
  const auto host = uri.host();
  if (!host.empty()) {
#ifdef _WIN32
    std::stringstream ss;
    ss << "//" << host << "/" << internal::RemoveLeadingSlash(uri.path());
    *out_path = ss.str();
#else
    return Status::Invalid("Unsupported hostname in non-Windows local URI: '",
                           uri.ToString(), "'");
#endif
  } else {
    *out_path = uri.path();
  }

  // TODO handle use_mmap option
  return LocalFileSystemOptions();
}

LocalFileSystem::LocalFileSystem(const io::IOContext& io_context)
    : FileSystem(io_context), options_(LocalFileSystemOptions::Defaults()) {}

LocalFileSystem::LocalFileSystem(const LocalFileSystemOptions& options,
                                 const io::IOContext& io_context)
    : FileSystem(io_context), options_(options) {}

LocalFileSystem::~LocalFileSystem() {}

Result<std::string> LocalFileSystem::NormalizePath(std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return fn.ToString();
}

bool LocalFileSystem::Equals(const FileSystem& other) const {
  if (other.type_name() != type_name()) {
    return false;
  } else {
    const auto& localfs = ::arrow::internal::checked_cast<const LocalFileSystem&>(other);
    return options_.Equals(localfs.options());
  }
}

Result<FileInfo> LocalFileSystem::GetFileInfo(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return StatFile(fn.ToNative());
}

Result<std::vector<FileInfo>> LocalFileSystem::GetFileInfo(const FileSelector& select) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(select.base_dir));
  std::vector<FileInfo> results;
  RETURN_NOT_OK(StatSelector(fn, select, 0, &results));
  return results;
}

Status LocalFileSystem::CreateDir(const std::string& path, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  if (recursive) {
    return ::arrow::internal::CreateDirTree(fn).status();
  } else {
    return ::arrow::internal::CreateDir(fn).status();
  }
}

Status LocalFileSystem::DeleteDir(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  auto st = ::arrow::internal::DeleteDirTree(fn, /*allow_not_found=*/false).status();
  if (!st.ok()) {
    // TODO Status::WithPrefix()?
    std::stringstream ss;
    ss << "Cannot delete directory '" << path << "': " << st.message();
    return st.WithMessage(ss.str());
  }
  return Status::OK();
}

Status LocalFileSystem::DeleteDirContents(const std::string& path) {
  if (internal::IsEmptyPath(path)) {
    return internal::InvalidDeleteDirContents(path);
  }
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  auto st = ::arrow::internal::DeleteDirContents(fn, /*allow_not_found=*/false).status();
  if (!st.ok()) {
    std::stringstream ss;
    ss << "Cannot delete directory contents in '" << path << "': " << st.message();
    return st.WithMessage(ss.str());
  }
  return Status::OK();
}

Status LocalFileSystem::DeleteRootDirContents() {
  return Status::Invalid("LocalFileSystem::DeleteRootDirContents is strictly forbidden");
}

Status LocalFileSystem::DeleteFile(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return ::arrow::internal::DeleteFile(fn, /*allow_not_found=*/false).status();
}

Status LocalFileSystem::Move(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto sfn, PlatformFilename::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dfn, PlatformFilename::FromString(dest));

#ifdef _WIN32
  if (!MoveFileExW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                   MOVEFILE_REPLACE_EXISTING)) {
    return IOErrorFromWinError(GetLastError(), "Failed renaming '", sfn.ToString(),
                               "' to '", dfn.ToString(), "'");
  }
#else
  if (rename(sfn.ToNative().c_str(), dfn.ToNative().c_str()) == -1) {
    return IOErrorFromErrno(errno, "Failed renaming '", sfn.ToString(), "' to '",
                            dfn.ToString(), "'");
  }
#endif
  return Status::OK();
}

Status LocalFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto sfn, PlatformFilename::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dfn, PlatformFilename::FromString(dest));
  // XXX should we use fstat() to compare inodes?
  if (sfn.ToNative() == dfn.ToNative()) {
    return Status::OK();
  }

#ifdef _WIN32
  if (!CopyFileW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                 FALSE /* bFailIfExists */)) {
    return IOErrorFromWinError(GetLastError(), "Failed copying '", sfn.ToString(),
                               "' to '", dfn.ToString(), "'");
  }
  return Status::OK();
#else
  ARROW_ASSIGN_OR_RAISE(auto is, OpenInputStream(src));
  ARROW_ASSIGN_OR_RAISE(auto os, OpenOutputStream(dest));
  RETURN_NOT_OK(internal::CopyStream(is, os, 1024 * 1024 /* chunk_size */, io_context()));
  RETURN_NOT_OK(os->Close());
  return is->Close();
#endif
}

namespace {

template <typename InputStreamType>
Result<std::shared_ptr<InputStreamType>> OpenInputStreamGeneric(
    const std::string& path, const LocalFileSystemOptions& options,
    const io::IOContext& io_context) {
  if (options.use_mmap) {
    return io::MemoryMappedFile::Open(path, io::FileMode::READ);
  } else {
    return io::ReadableFile::Open(path, io_context.pool());
  }
}

}  // namespace

Result<std::shared_ptr<io::InputStream>> LocalFileSystem::OpenInputStream(
    const std::string& path) {
  return OpenInputStreamGeneric<io::InputStream>(path, options_, io_context());
}

Result<std::shared_ptr<io::RandomAccessFile>> LocalFileSystem::OpenInputFile(
    const std::string& path) {
  return OpenInputStreamGeneric<io::RandomAccessFile>(path, options_, io_context());
}

namespace {

Result<std::shared_ptr<io::OutputStream>> OpenOutputStreamGeneric(const std::string& path,
                                                                  bool truncate,
                                                                  bool append) {
  int fd;
  bool write_only = true;
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  ARROW_ASSIGN_OR_RAISE(
      fd, ::arrow::internal::FileOpenWritable(fn, write_only, truncate, append));
  auto maybe_stream = io::FileOutputStream::Open(fd);
  if (!maybe_stream.ok()) {
    ARROW_UNUSED(::arrow::internal::FileClose(fd));
  }
  return maybe_stream;
}

}  // namespace

Result<std::shared_ptr<io::OutputStream>> LocalFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  bool truncate = true;
  bool append = false;
  return OpenOutputStreamGeneric(path, truncate, append);
}

Result<std::shared_ptr<io::OutputStream>> LocalFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  bool truncate = false;
  bool append = true;
  return OpenOutputStreamGeneric(path, truncate, append);
}

}  // namespace fs
}  // namespace arrow

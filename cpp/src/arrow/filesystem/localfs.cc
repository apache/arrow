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
#include <utility>

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#else
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#endif

#include <boost/filesystem.hpp>

#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/file.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

namespace bfs = ::boost::filesystem;

using ::arrow::internal::NativePathString;
using ::arrow::internal::PlatformFilename;

namespace {

#define BOOST_FILESYSTEM_TRY try {
#define BOOST_FILESYSTEM_CATCH           \
  }                                      \
  catch (bfs::filesystem_error & _err) { \
    return ToStatus(_err);               \
  }

// NOTE: catching filesystem_error gives more context than system::error_code
// (it includes the file path(s) in the error message)

Status ToStatus(const bfs::filesystem_error& err) { return Status::IOError(err.what()); }

template <typename... Args>
Status ErrnoToStatus(Args&&... args) {
  auto err_string = ::arrow::internal::ErrnoMessage(errno);
  return Status::IOError(std::forward<Args>(args)..., err_string);
}

#ifdef _WIN32

std::string NativeToString(const NativePathString& ns) {
  PlatformFilename fn(ns);
  return fn.ToString();
}

template <typename... Args>
Status WinErrorToStatus(Args&&... args) {
  auto err_string = ::arrow::internal::WinErrorMessage(GetLastError());
  return Status::IOError(std::forward<Args>(args)..., err_string);
}

TimePoint ToTimePoint(FILETIME ft) {
  // Hundreds of nanoseconds between January 1, 1601 (UTC) and the Unix epoch.
  static constexpr int64_t kFileTimeEpoch = 11644473600LL * 10000000;

  int64_t hundreds = (static_cast<int64_t>(ft.dwHighDateTime) << 32) + ft.dwLowDateTime -
                     kFileTimeEpoch;  // hundreds of ns since Unix epoch
  std::chrono::nanoseconds ns_count(100 * hundreds);
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileStats FileInformationToFileStat(const BY_HANDLE_FILE_INFORMATION& info) {
  FileStats st;
  if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
    st.set_type(FileType::Directory);
    st.set_size(kNoSize);
  } else {
    // Regular file
    st.set_type(FileType::File);
    st.set_size((static_cast<int64_t>(info.nFileSizeHigh) << 32) + info.nFileSizeLow);
  }
  st.set_mtime(ToTimePoint(info.ftLastWriteTime));
  return st;
}

Status StatFile(const std::wstring& path, FileStats* out) {
  HANDLE h;
  std::string bytes_path = NativeToString(path);

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
      out->set_path(bytes_path);
      out->set_type(FileType::NonExistent);
      out->set_mtime(kNoTime);
      out->set_size(kNoSize);
      return Status::OK();
    } else {
      return WinErrorToStatus("Failed querying information for path '", bytes_path, "'");
    }
  }
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(h, &info)) {
    Status st =
        WinErrorToStatus("Failed querying information for path '", bytes_path, "'");
    CloseHandle(h);
    return st;
  }
  CloseHandle(h);
  *out = FileInformationToFileStat(info);
  out->set_path(bytes_path);
  return Status::OK();
}

#else  // POSIX systems

TimePoint ToTimePoint(const struct timespec& s) {
  std::chrono::nanoseconds ns_count(static_cast<int64_t>(s.tv_sec) * 1000000000 +
                                    static_cast<int64_t>(s.tv_nsec));
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileStats StatToFileStat(const struct stat& s) {
  FileStats st;
  if (S_ISREG(s.st_mode)) {
    st.set_type(FileType::File);
    st.set_size(static_cast<int64_t>(s.st_size));
  } else if (S_ISDIR(s.st_mode)) {
    st.set_type(FileType::Directory);
    st.set_size(kNoSize);
  } else {
    st.set_type(FileType::Unknown);
    st.set_size(kNoSize);
  }
#ifdef __APPLE__
  // macOS doesn't use the POSIX-compliant spelling
  st.set_mtime(ToTimePoint(s.st_mtimespec));
#else
  st.set_mtime(ToTimePoint(s.st_mtim));
#endif
  return st;
}

Status StatFile(const std::string& path, FileStats* out) {
  struct stat s;
  int r = stat(path.c_str(), &s);
  if (r == -1) {
    if (errno == ENOENT || errno == ENOTDIR || errno == ELOOP) {
      out->set_type(FileType::NonExistent);
      out->set_mtime(kNoTime);
      out->set_size(kNoSize);
    } else {
      return ErrnoToStatus("Failed stat()ing path '", path, "'");
    }
  } else {
    *out = StatToFileStat(s);
  }
  out->set_path(path);
  return Status::OK();
}

#endif

Status StatSelector(const NativePathString& path, const Selector& select,
                    std::vector<FileStats>* out) {
  bfs::path p(path);

  if (select.allow_non_existent) {
    bfs::file_status st;
    BOOST_FILESYSTEM_TRY
    st = bfs::status(p);
    BOOST_FILESYSTEM_CATCH
    if (st.type() == bfs::file_not_found) {
      return Status::OK();
    }
  }

  BOOST_FILESYSTEM_TRY
  for (const auto& entry : bfs::directory_iterator(p)) {
    FileStats st;
    NativePathString ns = entry.path().native();
    RETURN_NOT_OK(StatFile(ns, &st));
    if (st.type() != FileType::NonExistent) {
      out->push_back(std::move(st));
    }
    if (select.recursive && st.type() == FileType::Directory) {
      RETURN_NOT_OK(StatSelector(ns, select, out));
    }
  }
  BOOST_FILESYSTEM_CATCH

  return Status::OK();
}

}  // namespace

LocalFileSystem::LocalFileSystem() {}

LocalFileSystem::~LocalFileSystem() {}

Status LocalFileSystem::GetTargetStats(const std::string& path, FileStats* out) {
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  return StatFile(fn.ToNative(), out);
}

Status LocalFileSystem::GetTargetStats(const Selector& select,
                                       std::vector<FileStats>* out) {
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(select.base_dir, &fn));
  out->clear();
  return StatSelector(fn.ToNative(), select, out);
}

Status LocalFileSystem::CreateDir(const std::string& path, bool recursive) {
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  if (recursive) {
    return ::arrow::internal::CreateDirTree(fn);
  } else {
    return ::arrow::internal::CreateDir(fn);
  }
}

Status LocalFileSystem::DeleteDir(const std::string& path) {
  bool deleted = false;
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  RETURN_NOT_OK(::arrow::internal::DeleteDirTree(fn, &deleted));
  if (deleted) {
    return Status::OK();
  } else {
    return Status::IOError("Directory does not exist: '", path, "'");
  }
}

Status LocalFileSystem::DeleteDirContents(const std::string& path) {
  bool deleted = false;
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  RETURN_NOT_OK(::arrow::internal::DeleteDirContents(fn, &deleted));
  if (deleted) {
    return Status::OK();
  } else {
    return Status::IOError("Directory does not exist: '", path, "'");
  }
}

Status LocalFileSystem::DeleteFile(const std::string& path) {
  bool deleted = false;
  PlatformFilename fn;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  RETURN_NOT_OK(::arrow::internal::DeleteFile(fn, &deleted));
  if (deleted) {
    return Status::OK();
  } else {
    return Status::IOError("File does not exist: '", path, "'");
  }
}

Status LocalFileSystem::Move(const std::string& src, const std::string& dest) {
  PlatformFilename sfn, dfn;
  RETURN_NOT_OK(PlatformFilename::FromString(src, &sfn));
  RETURN_NOT_OK(PlatformFilename::FromString(dest, &dfn));

#ifdef _WIN32
  if (!MoveFileExW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                   MOVEFILE_REPLACE_EXISTING)) {
    return WinErrorToStatus("Failed renaming '", sfn.ToString(), "' to '", dfn.ToString(),
                            "': ");
  }
#else
  if (rename(sfn.ToNative().c_str(), dfn.ToNative().c_str()) == -1) {
    return ErrnoToStatus("Failed renaming '", sfn.ToString(), "' to '", dfn.ToString(),
                         "': ");
  }
#endif
  return Status::OK();
}

Status LocalFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  PlatformFilename sfn, dfn;
  RETURN_NOT_OK(PlatformFilename::FromString(src, &sfn));
  RETURN_NOT_OK(PlatformFilename::FromString(dest, &dfn));
  // XXX should we use fstat() to compare inodes?
  if (sfn.ToNative() == dfn.ToNative()) {
    return Status::OK();
  }

#ifdef _WIN32
  if (!CopyFileW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                 FALSE /* bFailIfExists */)) {
    return WinErrorToStatus("Failed copying '", sfn.ToString(), "' to '", dfn.ToString(),
                            "': ");
  }
  return Status::OK();
#else
  std::shared_ptr<io::InputStream> is;
  std::shared_ptr<io::OutputStream> os;
  RETURN_NOT_OK(OpenInputStream(src, &is));
  RETURN_NOT_OK(OpenOutputStream(dest, &os));
  RETURN_NOT_OK(internal::CopyStream(is, os, 1024 * 1024 /* chunk_size */));
  RETURN_NOT_OK(os->Close());
  return is->Close();
#endif
}

Status LocalFileSystem::OpenInputStream(const std::string& path,
                                        std::shared_ptr<io::InputStream>* out) {
  std::shared_ptr<io::ReadableFile> file;
  RETURN_NOT_OK(io::ReadableFile::Open(path, &file));
  *out = std::move(file);
  return Status::OK();
}

Status LocalFileSystem::OpenInputFile(const std::string& path,
                                      std::shared_ptr<io::RandomAccessFile>* out) {
  std::shared_ptr<io::ReadableFile> file;
  RETURN_NOT_OK(io::ReadableFile::Open(path, &file));
  *out = std::move(file);
  return Status::OK();
}

namespace {

Status OpenOutputStreamGeneric(const std::string& path, bool truncate, bool append,
                               std::shared_ptr<io::OutputStream>* out) {
  PlatformFilename fn;
  int fd;
  bool write_only = true;
  RETURN_NOT_OK(PlatformFilename::FromString(path, &fn));
  RETURN_NOT_OK(
      ::arrow::internal::FileOpenWritable(fn, write_only, truncate, append, &fd));
  Status st = io::FileOutputStream::Open(fd, out);
  if (!st.ok()) {
    ARROW_UNUSED(::arrow::internal::FileClose(fd));
  }
  return st;
}

}  // namespace

Status LocalFileSystem::OpenOutputStream(const std::string& path,
                                         std::shared_ptr<io::OutputStream>* out) {
  bool truncate = true;
  bool append = false;
  return OpenOutputStreamGeneric(path, truncate, append, out);
}

Status LocalFileSystem::OpenAppendStream(const std::string& path,
                                         std::shared_ptr<io::OutputStream>* out) {
  bool truncate = false;
  bool append = true;
  return OpenOutputStreamGeneric(path, truncate, append, out);
}

}  // namespace fs
}  // namespace arrow

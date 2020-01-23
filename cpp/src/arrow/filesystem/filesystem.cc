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

#include <sstream>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#ifdef ARROW_HDFS
#include "arrow/filesystem/hdfs.h"
#endif
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/slow.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::Uri;

namespace fs {

using internal::ConcatAbstractPath;
using internal::EnsureTrailingSlash;
using internal::GetAbstractPathParent;
using internal::kSep;
using internal::RemoveLeadingSlash;
using internal::RemoveTrailingSlash;

std::string ToString(FileType ftype) {
  switch (ftype) {
    case FileType::NonExistent:
      return "non-existent";
    case FileType::Unknown:
      return "unknown";
    case FileType::File:
      return "file";
    case FileType::Directory:
      return "directory";
    default:
      ARROW_LOG(FATAL) << "Invalid FileType value: " << static_cast<int>(ftype);
      return "???";
  }
}

// For googletest
ARROW_EXPORT std::ostream& operator<<(std::ostream& os, FileType ftype) {
#define FILE_TYPE_CASE(value_name)                  \
  case FileType::value_name:                        \
    os << "FileType::" ARROW_STRINGIFY(value_name); \
    break;

  switch (ftype) {
    FILE_TYPE_CASE(NonExistent)
    FILE_TYPE_CASE(Unknown)
    FILE_TYPE_CASE(File)
    FILE_TYPE_CASE(Directory)
    default:
      ARROW_LOG(FATAL) << "Invalid FileType value: " << static_cast<int>(ftype);
  }

#undef FILE_TYPE_CASE
  return os;
}

std::string FileStats::base_name() const {
  return internal::GetAbstractPathParent(path_).second;
}

std::string FileStats::dir_name() const {
  return internal::GetAbstractPathParent(path_).first;
}

// Debug helper
std::string FileStats::ToString() const {
  std::stringstream os;
  os << *this;
  return os.str();
}

std::ostream& operator<<(std::ostream& os, const FileStats& stats) {
  return os << "FileStats(" << stats.type() << ", " << stats.path() << ")";
}

std::string FileStats::extension() const {
  return internal::GetAbstractPathExtension(path_);
}

//////////////////////////////////////////////////////////////////////////
// FileSystem default method implementations

FileSystem::~FileSystem() {}

Result<std::vector<FileStats>> FileSystem::GetTargetStats(
    const std::vector<std::string>& paths) {
  std::vector<FileStats> res;
  res.reserve(paths.size());
  for (const auto& path : paths) {
    ARROW_ASSIGN_OR_RAISE(FileStats st, GetTargetStats(path));
    res.push_back(std::move(st));
  }
  return res;
}

Status FileSystem::DeleteFiles(const std::vector<std::string>& paths) {
  Status st = Status::OK();
  for (const auto& path : paths) {
    st &= DeleteFile(path);
  }
  return st;
}

//////////////////////////////////////////////////////////////////////////
// SubTreeFileSystem implementation

// FIXME EnsureTrailingSlash works on abstract paths... but we will be
// passing a concrete path, e.g. "C:" on Windows.

SubTreeFileSystem::SubTreeFileSystem(const std::string& base_path,
                                     std::shared_ptr<FileSystem> base_fs)
    : base_path_(EnsureTrailingSlash(base_path)), base_fs_(base_fs) {}

SubTreeFileSystem::~SubTreeFileSystem() {}

std::string SubTreeFileSystem::PrependBase(const std::string& s) const {
  if (s.empty()) {
    return base_path_;
  } else {
    return ConcatAbstractPath(base_path_, s);
  }
}

Status SubTreeFileSystem::PrependBaseNonEmpty(std::string* s) const {
  if (s->empty()) {
    return Status::IOError("Empty path");
  } else {
    *s = ConcatAbstractPath(base_path_, *s);
    return Status::OK();
  }
}

Status SubTreeFileSystem::StripBase(const std::string& s, std::string* out) const {
  auto len = base_path_.length();
  // Note base_path_ ends with a slash (if not empty)
  if (s.length() >= len && s.substr(0, len) == base_path_) {
    *out = s.substr(len);
    return Status::OK();
  } else {
    return Status::UnknownError("Underlying filesystem returned path '", s,
                                "', which is not a subpath of '", base_path_, "'");
  }
}

Status SubTreeFileSystem::FixStats(FileStats* st) const {
  std::string fixed_path;
  RETURN_NOT_OK(StripBase(st->path(), &fixed_path));
  st->set_path(fixed_path);
  return Status::OK();
}

Result<FileStats> SubTreeFileSystem::GetTargetStats(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(FileStats st, base_fs_->GetTargetStats(PrependBase(path)));
  RETURN_NOT_OK(FixStats(&st));
  return st;
}

Result<std::vector<FileStats>> SubTreeFileSystem::GetTargetStats(
    const FileSelector& select) {
  auto selector = select;
  selector.base_dir = PrependBase(selector.base_dir);
  ARROW_ASSIGN_OR_RAISE(auto stats, base_fs_->GetTargetStats(selector));
  for (auto& st : stats) {
    RETURN_NOT_OK(FixStats(&st));
  }
  return stats;
}

Status SubTreeFileSystem::CreateDir(const std::string& path, bool recursive) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->CreateDir(s, recursive);
}

Status SubTreeFileSystem::DeleteDir(const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->DeleteDir(s);
}

Status SubTreeFileSystem::DeleteDirContents(const std::string& path) {
  auto s = PrependBase(path);
  return base_fs_->DeleteDirContents(s);
}

Status SubTreeFileSystem::DeleteFile(const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->DeleteFile(s);
}

Status SubTreeFileSystem::Move(const std::string& src, const std::string& dest) {
  auto s = src;
  auto d = dest;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  RETURN_NOT_OK(PrependBaseNonEmpty(&d));
  return base_fs_->Move(s, d);
}

Status SubTreeFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  auto s = src;
  auto d = dest;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  RETURN_NOT_OK(PrependBaseNonEmpty(&d));
  return base_fs_->CopyFile(s, d);
}

Result<std::shared_ptr<io::InputStream>> SubTreeFileSystem::OpenInputStream(
    const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenInputStream(s);
}

Result<std::shared_ptr<io::RandomAccessFile>> SubTreeFileSystem::OpenInputFile(
    const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenInputFile(s);
}

Result<std::shared_ptr<io::OutputStream>> SubTreeFileSystem::OpenOutputStream(
    const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenOutputStream(s);
}

Result<std::shared_ptr<io::OutputStream>> SubTreeFileSystem::OpenAppendStream(
    const std::string& path) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenAppendStream(s);
}

//////////////////////////////////////////////////////////////////////////
// SlowFileSystem implementation

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               std::shared_ptr<io::LatencyGenerator> latencies)
    : base_fs_(base_fs), latencies_(latencies) {}

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               double average_latency)
    : base_fs_(base_fs), latencies_(io::LatencyGenerator::Make(average_latency)) {}

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               double average_latency, int32_t seed)
    : base_fs_(base_fs), latencies_(io::LatencyGenerator::Make(average_latency, seed)) {}

Result<FileStats> SlowFileSystem::GetTargetStats(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->GetTargetStats(path);
}

Result<std::vector<FileStats>> SlowFileSystem::GetTargetStats(
    const FileSelector& selector) {
  latencies_->Sleep();
  return base_fs_->GetTargetStats(selector);
}

Status SlowFileSystem::CreateDir(const std::string& path, bool recursive) {
  latencies_->Sleep();
  return base_fs_->CreateDir(path, recursive);
}

Status SlowFileSystem::DeleteDir(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->DeleteDir(path);
}

Status SlowFileSystem::DeleteDirContents(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->DeleteDirContents(path);
}

Status SlowFileSystem::DeleteFile(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->DeleteFile(path);
}

Status SlowFileSystem::Move(const std::string& src, const std::string& dest) {
  latencies_->Sleep();
  return base_fs_->Move(src, dest);
}

Status SlowFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  latencies_->Sleep();
  return base_fs_->CopyFile(src, dest);
}

Result<std::shared_ptr<io::InputStream>> SlowFileSystem::OpenInputStream(
    const std::string& path) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto stream, base_fs_->OpenInputStream(path));
  return std::make_shared<io::SlowInputStream>(stream, latencies_);
}

Result<std::shared_ptr<io::RandomAccessFile>> SlowFileSystem::OpenInputFile(
    const std::string& path) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto file, base_fs_->OpenInputFile(path));
  return std::make_shared<io::SlowRandomAccessFile>(file, latencies_);
}

Result<std::shared_ptr<io::OutputStream>> SlowFileSystem::OpenOutputStream(
    const std::string& path) {
  latencies_->Sleep();
  // XXX Should we have a SlowOutputStream that waits on Flush() and Close()?
  return base_fs_->OpenOutputStream(path);
}

Result<std::shared_ptr<io::OutputStream>> SlowFileSystem::OpenAppendStream(
    const std::string& path) {
  latencies_->Sleep();
  return base_fs_->OpenAppendStream(path);
}

namespace {

struct FileSystemUri {
  Uri uri;
  std::string scheme;
  std::string path;
  bool is_local = false;
};

Result<FileSystemUri> ParseFileSystemUri(const std::string& uri_string) {
  FileSystemUri fsuri;
  RETURN_NOT_OK(fsuri.uri.Parse(uri_string));
  fsuri.scheme = fsuri.uri.scheme();
  fsuri.path = fsuri.uri.path();
#ifdef _WIN32
  if (fsuri.scheme.size() == 1) {
    // Assuming a plain local path starting with a drive letter, e.g "C:/..."
    fsuri.is_local = true;
    fsuri.path = fsuri.scheme + ':' + fsuri.path;
  }
#endif
  if (fsuri.scheme == "" || fsuri.scheme == "file") {
    fsuri.is_local = true;
  }
  return std::move(fsuri);
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriReal(const FileSystemUri& fsuri,
                                                          const std::string& uri_string,
                                                          std::string* out_path) {
  if (out_path != nullptr) {
    *out_path = fsuri.path;
  }
  if (fsuri.is_local) {
    return std::make_shared<LocalFileSystem>();
  }
  if (fsuri.scheme == "hdfs" || fsuri.scheme == "viewfs") {
#ifdef ARROW_HDFS
    ARROW_ASSIGN_OR_RAISE(auto options, HdfsOptions::FromUri(fsuri.uri));
    ARROW_ASSIGN_OR_RAISE(auto hdfs, HadoopFileSystem::Make(options));
    return hdfs;
#else
    return Status::NotImplemented("Arrow compiled without HDFS support");
#endif
  }

  // Other filesystems below do not have an absolute / relative path distinction,
  // normalize path by removing leading slash.
  // XXX perhaps each filesystem should have a path normalization method?
  if (out_path != nullptr) {
    *out_path = std::string(RemoveLeadingSlash(*out_path));
  }
  if (fsuri.scheme == "mock") {
    return std::make_shared<internal::MockFileSystem>(internal::CurrentTimePoint());
  }

  // TODO add support for S3 URIs
  return Status::Invalid("Unrecognized filesystem type in URI: ", uri_string);
}

}  // namespace

Result<std::shared_ptr<FileSystem>> FileSystemFromUri(const std::string& uri_string,
                                                      std::string* out_path) {
  Status st;
  FileSystemUri fsuri;
  auto maybe_fsuri = ParseFileSystemUri(uri_string);
  if (maybe_fsuri.ok()) {
    fsuri = *std::move(maybe_fsuri);
  } else {
    st = maybe_fsuri.status();
#ifdef _WIN32
    // The user may be passing a local path with backslashes, which fails
    // URI parsing.  Try again with forward slashes, but make sure the
    // resulting URI points to a local path.
    maybe_fsuri = ParseFileSystemUri(internal::ToSlashes(uri_string));
    if (!maybe_fsuri.ok()) {
      return st;
    }
    fsuri = *std::move(maybe_fsuri);
    if (!fsuri.is_local) {
      return st;
    }
#else
    return st;
#endif
  }
  return FileSystemFromUriReal(fsuri, uri_string, out_path);
}

Status FileSystemFromUri(const std::string& uri, std::shared_ptr<FileSystem>* out_fs,
                         std::string* out_path) {
  return FileSystemFromUri(uri, out_path).Value(out_fs);
}

}  // namespace fs
}  // namespace arrow

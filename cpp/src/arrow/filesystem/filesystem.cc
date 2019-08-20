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

#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

using internal::ConcatAbstractPath;
using internal::EnsureTrailingSlash;
using internal::GetAbstractPathParent;
using internal::kSep;

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

std::string FileStats::base_name() const {
  return internal::GetAbstractPathParent(path_).second;
}

//////////////////////////////////////////////////////////////////////////
// FileSystem default method implementations

FileSystem::~FileSystem() {}

Status FileSystem::GetTargetStats(const std::vector<std::string>& paths,
                                  std::vector<FileStats>* out) {
  std::vector<FileStats> res;
  res.reserve(paths.size());
  for (const auto& path : paths) {
    FileStats st;
    RETURN_NOT_OK(GetTargetStats(path, &st));
    res.push_back(std::move(st));
  }
  *out = std::move(res);
  return Status::OK();
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

Status SubTreeFileSystem::GetTargetStats(const std::string& path, FileStats* out) {
  RETURN_NOT_OK(base_fs_->GetTargetStats(PrependBase(path), out));
  return FixStats(out);
}

Status SubTreeFileSystem::GetTargetStats(const Selector& select,
                                         std::vector<FileStats>* out) {
  auto selector = select;
  selector.base_dir = PrependBase(selector.base_dir);
  RETURN_NOT_OK(base_fs_->GetTargetStats(selector, out));
  for (auto& st : *out) {
    RETURN_NOT_OK(FixStats(&st));
  }
  return Status::OK();
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

Status SubTreeFileSystem::OpenInputStream(const std::string& path,
                                          std::shared_ptr<io::InputStream>* out) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenInputStream(s, out);
}

Status SubTreeFileSystem::OpenInputFile(const std::string& path,
                                        std::shared_ptr<io::RandomAccessFile>* out) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenInputFile(s, out);
}

Status SubTreeFileSystem::OpenOutputStream(const std::string& path,
                                           std::shared_ptr<io::OutputStream>* out) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenOutputStream(s, out);
}

Status SubTreeFileSystem::OpenAppendStream(const std::string& path,
                                           std::shared_ptr<io::OutputStream>* out) {
  auto s = path;
  RETURN_NOT_OK(PrependBaseNonEmpty(&s));
  return base_fs_->OpenAppendStream(s, out);
}

}  // namespace fs
}  // namespace arrow

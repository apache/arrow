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
#include "arrow/filesystem/path-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

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

}  // namespace fs
}  // namespace arrow

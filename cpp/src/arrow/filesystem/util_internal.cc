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

#include "arrow/filesystem/util_internal.h"

#include <cerrno>

#include "arrow/buffer.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"

namespace arrow {

using internal::StatusDetailFromErrno;

namespace fs {
namespace internal {

TimePoint CurrentTimePoint() {
  auto now = std::chrono::system_clock::now();
  return TimePoint(
      std::chrono::duration_cast<TimePoint::duration>(now.time_since_epoch()));
}

Status CopyStream(const std::shared_ptr<io::InputStream>& src,
                  const std::shared_ptr<io::OutputStream>& dest, int64_t chunk_size,
                  const io::IOContext& io_context) {
  ARROW_ASSIGN_OR_RAISE(auto chunk, AllocateBuffer(chunk_size, io_context.pool()));

  while (true) {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          src->Read(chunk_size, chunk->mutable_data()));
    if (bytes_read == 0) {
      // EOF
      break;
    }
    RETURN_NOT_OK(dest->Write(chunk->data(), bytes_read));
  }

  return Status::OK();
}

Status PathNotFound(const std::string& path) {
  return Status::IOError("Path does not exist '", path, "'")
      .WithDetail(StatusDetailFromErrno(ENOENT));
}

Status NotADir(const std::string& path) {
  return Status::IOError("Not a directory: '", path, "'")
      .WithDetail(StatusDetailFromErrno(ENOTDIR));
}

Status NotAFile(const std::string& path) {
  return Status::IOError("Not a regular file: '", path, "'");
}

Status InvalidDeleteDirContents(const std::string& path) {
  return Status::Invalid(
      "DeleteDirContents called on invalid path '", path, "'. ",
      "If you wish to delete the root directory's contents, call DeleteRootDirContents.");
}

Result<FileInfoVector> GetGlobFiles(const std::shared_ptr<FileSystem>& filesystem,
                                    const std::string& glob) {
  FileInfoVector results, temp;
  FileSelector selector;
  std::string cur;

  auto split_path = SplitAbstractPath(glob, '/');
  ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo("/"));
  results.push_back(std::move(file));

  for (size_t i = 0; i < split_path.size(); i++) {
    if (split_path[i].find_first_of("*?") == std::string::npos) {
      if (cur.empty())
        cur = split_path[i];
      else
        cur = ConcatAbstractPath(cur, split_path[i]);
      continue;
    } else {
      for (auto res : results) {
        if (res.type() != FileType::Directory) continue;
        if (cur.empty())
          selector.base_dir = res.path();
        else
          selector.base_dir = ConcatAbstractPath(res.path(), cur);
        ARROW_ASSIGN_OR_RAISE(auto entries, filesystem->GetFileInfo(selector));
        Globber globber(ConcatAbstractPath(selector.base_dir, split_path[i]));
        for (auto entry : entries) {
          if (globber.Matches(entry.path())) {
            temp.push_back(std::move(entry));
          }
        }
      }
      results = std::move(temp);
      cur.clear();
    }
  }

  if (!cur.empty()) {
    std::vector<std::string> paths;
    for (size_t i = 0; i < results.size(); i++) {
      paths.push_back(ConcatAbstractPath(results[i].path(), cur));
    }
    ARROW_ASSIGN_OR_RAISE(results, filesystem->GetFileInfo(paths));
  }

  std::vector<FileInfo> out;
  for (const auto& file : results) {
    if (file.type() != FileType::NotFound) {
      out.push_back(std::move(file));
    }
  }

  return out;
}

FileSystemGlobalOptions global_options;

}  // namespace internal
}  // namespace fs
}  // namespace arrow

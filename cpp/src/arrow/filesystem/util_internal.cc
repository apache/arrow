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

Status PathNotFound(std::string_view path) {
  return Status::IOError("Path does not exist '", path, "'")
      .WithDetail(StatusDetailFromErrno(ENOENT));
}

Status NotADir(std::string_view path) {
  return Status::IOError("Not a directory: '", path, "'")
      .WithDetail(StatusDetailFromErrno(ENOTDIR));
}

Status NotAFile(std::string_view path) {
  return Status::IOError("Not a regular file: '", path, "'");
}

Status InvalidDeleteDirContents(std::string_view path) {
  return Status::Invalid(
      "DeleteDirContents called on invalid path '", path, "'. ",
      "If you wish to delete the root directory's contents, call DeleteRootDirContents.");
}

Result<FileInfoVector> GlobFiles(const std::shared_ptr<FileSystem>& filesystem,
                                 const std::string& glob) {
  // TODO: ARROW-17640
  // The candidate entries at the current depth level.
  // We start with the filesystem root.
  FileInfoVector results{FileInfo("", FileType::Directory)};
  // The exact tail that will later require matching with candidate entries
  std::string current_tail;
  auto is_leading_slash = HasLeadingSlash(glob);
  auto split_glob = SplitAbstractPath(glob, '/');

  // Process one depth level at once, from root to leaf
  for (const auto& glob_component : split_glob) {
    if (glob_component.find_first_of("*?") == std::string::npos) {
      // If there are no wildcards at the current level, just append
      // the exact glob path component.
      current_tail = ConcatAbstractPath(current_tail, glob_component);
      continue;
    } else {
      FileInfoVector children;
      for (const auto& res : results) {
        if (res.type() != FileType::Directory) {
          continue;
        }
        FileSelector selector;
        selector.base_dir = current_tail.empty()
                                ? res.path()
                                : ConcatAbstractPath(res.path(), current_tail);
        if (is_leading_slash) {
          selector.base_dir = EnsureLeadingSlash(selector.base_dir);
        }
        ARROW_ASSIGN_OR_RAISE(auto entries, filesystem->GetFileInfo(selector));
        Globber globber(ConcatAbstractPath(selector.base_dir, glob_component));
        for (auto&& entry : entries) {
          if (globber.Matches(entry.path())) {
            children.push_back(std::move(entry));
          }
        }
      }
      results = std::move(children);
      current_tail.clear();
    }
  }

  if (!current_tail.empty()) {
    std::vector<std::string> paths;
    paths.reserve(results.size());
    for (const auto& file : results) {
      paths.push_back(ConcatAbstractPath(file.path(), current_tail));
    }
    ARROW_ASSIGN_OR_RAISE(results, filesystem->GetFileInfo(paths));
  }

  std::vector<FileInfo> out;
  for (auto&& file : results) {
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

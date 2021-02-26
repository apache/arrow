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
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
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
  return Status::IOError("Path does not exist '", path, "'");
}

Status NotADir(const std::string& path) {
  return Status::IOError("Not a directory: '", path, "'");
}

Status NotAFile(const std::string& path) {
  return Status::IOError("Not a regular file: '", path, "'");
}

Status InvalidDeleteDirContents(const std::string& path) {
  return Status::Invalid(
      "DeleteDirContents called on invalid path '", path, "'. ",
      "If you wish to delete the root directory's contents, call DeleteRootDirContents.");
}

FileSystemGlobalOptions global_options;

}  // namespace internal
}  // namespace fs
}  // namespace arrow

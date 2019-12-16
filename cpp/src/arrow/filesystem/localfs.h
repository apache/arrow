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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"

namespace arrow {
namespace fs {

/// Options for the LocalFileSystem implementation.
struct ARROW_EXPORT LocalFileSystemOptions {
  /// Whether OpenInputStream and OpenInputFile return a mmap'ed file,
  /// or a regular one.
  bool use_mmap = false;

  /// \brief Initialize with defaults
  static LocalFileSystemOptions Defaults();
};

/// \brief A FileSystem implementation accessing files on the local machine.
///
/// This class handles only `/`-separated paths.  If desired, conversion
/// from Windows backslash-separated paths should be done by the caller.
/// Details such as symlinks are abstracted away (symlinks are always
/// followed, except when deleting an entry).
class ARROW_EXPORT LocalFileSystem : public FileSystem {
 public:
  LocalFileSystem();
  explicit LocalFileSystem(const LocalFileSystemOptions&);
  ~LocalFileSystem() override;

  std::string type_name() const override { return "local"; }

  /// \cond FALSE
  using FileSystem::GetTargetStats;
  /// \endcond
  Result<FileStats> GetTargetStats(const std::string& path) override;
  Result<std::vector<FileStats>> GetTargetStats(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path) override;

 protected:
  LocalFileSystemOptions options_;
};

}  // namespace fs
}  // namespace arrow

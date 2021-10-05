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
class GCSFileSystem;
struct GCSOptions;
namespace internal {
// TODO(ARROW-1231) - during development only tests should create a GCSFileSystem.
//     Remove before declaring the feature complete.
std::shared_ptr<GCSFileSystem> MakeGCSFileSystemForTest(const GCSOptions& options);
}  // namespace internal

struct ARROW_EXPORT GCSOptions {
  std::string endpoint_override;
  std::string scheme;

  bool Equals(const GCSOptions& other) const;

  /// \brief Initialize with default credentials provider chain
  ///
  /// This is recommended if you use the standard AWS environment variables
  /// and/or configuration file.
  static GCSOptions Defaults();

  /// \brief Initialize with anonymous credentials.
  ///
  /// This will only let you access public buckets.
  static GCSOptions Anonymous();
};

class ARROW_EXPORT GCSFileSystem : public FileSystem {
 public:
  ~GCSFileSystem() override = default;

  std::string type_name() const override;

  bool Equals(const FileSystem& other) const override;

  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<FileInfoVector> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;

  Status DeleteDirContents(const std::string& path) override;

  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) override;

  ARROW_DEPRECATED(
      "Deprecated. "
      "OpenAppendStream is unsupported on the GCS FileSystem.")
  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) override;

 private:
  /// Create a GCSFileSystem instance from the given options.
  friend std::shared_ptr<GCSFileSystem> internal::MakeGCSFileSystemForTest(
      const GCSOptions& options);

  explicit GCSFileSystem(const GCSOptions& options, const io::IOContext& io_context);

  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow

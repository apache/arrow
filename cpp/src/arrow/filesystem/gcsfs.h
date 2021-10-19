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
class GcsFileSystem;
struct GcsOptions;
namespace internal {
// TODO(ARROW-1231) - remove, and provide a public API (static GcsFileSystem::Make()).
std::shared_ptr<GcsFileSystem> MakeGcsFileSystemForTest(const GcsOptions& options);
}  // namespace internal

/// Options for the GcsFileSystem implementation.
struct ARROW_EXPORT GcsOptions {
  std::string endpoint_override;
  std::string scheme;

  bool Equals(const GcsOptions& other) const;
};

/// \brief GCS-backed FileSystem implementation.
///
/// Some implementation notes:
/// - TODO(ARROW-1231) - review all the notes once completed.
/// - buckets are treated as top-level directories on a "root".
/// - GCS buckets are in a global namespace, only one bucket
///   named `foo` exists in Google Cloud.
/// - Creating new top-level directories is implemented by creating
///   a bucket, this may be a slower operation than usual.
/// - A principal (service account, user, etc) can only list the
///   buckets for a single project, but can access the buckets
///   for many projects. It is possible that listing "all"
///   the buckets returns fewer buckets than you have access to.
/// - GCS does not have directories, they are emulated in this
///   library by listing objects with a common prefix.
/// - In general, GCS has much higher latency than local filesystems.
///   The throughput of GCS is comparable to the throughput of
///   a local file system.
class ARROW_EXPORT GcsFileSystem : public FileSystem {
 public:
  ~GcsFileSystem() override = default;

  std::string type_name() const override;

  bool Equals(const FileSystem& other) const override;

  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<FileInfoVector> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive) override;

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
  /// Create a GcsFileSystem instance from the given options.
  friend std::shared_ptr<GcsFileSystem> internal::MakeGcsFileSystemForTest(
      const GcsOptions& options);

  explicit GcsFileSystem(const GcsOptions& options, const io::IOContext& io_context);

  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow

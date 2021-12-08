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

// - TODO(ARROW-1231) - review this documentation before closing the bug.
/// \brief GCS-backed FileSystem implementation.
///
/// GCS (Google Cloud Storage - https://cloud.google.com/storage) is a scalable object
/// storage system for any amount of data. The main abstractions in GCS are buckets and
/// objects. A bucket is a namespace for objects, buckets can store any number of objects,
/// tens of millions and even billions is not uncommon.  Each object contains a single
/// blob of data, up to 5TiB in size.  Buckets are typically configured to keep a single
/// version of each object, but versioning can be enabled. Versioning is important because
/// objects are immutable, once created one cannot append data to the object or modify the
/// object data in any way.
///
/// GCS buckets are in a global namespace, if a Google Cloud customer creates a bucket
/// named `foo` no other customer can create a bucket with the same name. Note that a
/// principal (a user or service account) may only list the buckets they are entitled to,
/// and then only within a project. It is not possible to list "all" the buckets.
///
/// Within each bucket objects are in flat namespace. GCS does not have folders or
/// directories. However, following some conventions it is possible to emulate
/// directories. To this end, this class:
///
/// - All buckets are treated as directories at the "root"
/// - Creating a root directory results in a new bucket being created, this may be slower
///   than most GCS operations.
/// - Any object with a name ending with a slash (`/`) character is treated as a
///   directory.
/// - The class creates marker objects for a directory, using a trailing slash in the
///   marker names. For debugging purposes, the metadata of these marker objects indicate
///   that they are markers created by this class. The class does not rely on this
///   annotation.
/// - GCS can list all the objects with a given prefix, this is used to emulate listing
///   of directories.
/// - In object lists GCS can summarize all the objects with a common prefix as a single
///   entry, this is used to emulate non-recursive lists. Note that GCS list time is
///   proportional to the number of objects in the prefix. Listing recursively takes
///   almost the same time as non-recursive lists.
///
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

  /// This is not implemented in GcsFileSystem, as it would be too dangerous.
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

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

#include "arrow/filesystem/gcsfs.h"

#include <google/cloud/storage/client.h>

#include <sstream>

#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace fs {

namespace gcs = google::cloud::storage;

google::cloud::Options AsGoogleCloudOptions(GCSOptions const& o) {
  auto options = google::cloud::Options{};
  if (!o.endpoint_override.empty()) {
    auto scheme = o.scheme;
    if (scheme.empty()) scheme = "https";
    options.set<gcs::RestEndpointOption>(scheme + "://" + o.endpoint_override);
  }
  return options;
}

class GCSFileSystem::Impl {
 public:
  explicit Impl(GCSOptions const& o) : client_(AsGoogleCloudOptions(o)) {}

 private:
  gcs::Client client_;
};

std::string GCSFileSystem::type_name() const { return "gcs"; }

bool GCSFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& fs = ::arrow::internal::checked_cast<const GCSFileSystem&>(other);
  return impl_ == fs.impl_;
}

Result<FileInfo> GCSFileSystem::GetFileInfo(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<FileInfoVector> GCSFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::DeleteDirContents(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::DeleteFile(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GCSFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> GCSFileSystem::OpenInputStream(
    const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> GCSFileSystem::OpenInputStream(
    const FileInfo& info) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> GCSFileSystem::OpenInputFile(
    const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> GCSFileSystem::OpenInputFile(
    const FileInfo& info) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> GCSFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> GCSFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("Append is not supported in GCS");
}

GCSFileSystem::GCSFileSystem(const GCSOptions& options, const io::IOContext& context)
    : FileSystem(context), impl_(std::make_shared<Impl>(options)) {}

namespace internal {

std::shared_ptr<GCSFileSystem> MakeGCSFileSystemForTest(const GCSOptions& options) {
  return std::shared_ptr<GCSFileSystem>(
      new GCSFileSystem(options, io::default_io_context()));
}

}  // namespace internal

}  // namespace fs
}  // namespace arrow

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

#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace fs {
namespace {

auto constexpr kSep = '/';

struct GcsPath {
  std::string full_path;
  std::string bucket;
  std::string object;

  static Result<GcsPath> FromString(const std::string& s) {
    const auto src = internal::RemoveTrailingSlash(s);
    auto const first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return GcsPath{std::string(src), std::string(src), ""};
    }
    GcsPath path;
    path.full_path = std::string(src);
    path.bucket = std::string(src.substr(0, first_sep));
    path.object = std::string(src.substr(first_sep + 1));
    return path;
  }

  bool empty() const { return bucket.empty() && object.empty(); }

  bool operator==(const GcsPath& other) const {
    return bucket == other.bucket && object == other.object;
  }
};

}  // namespace

namespace gcs = google::cloud::storage;

google::cloud::Options AsGoogleCloudOptions(const GcsOptions& o) {
  auto options = google::cloud::Options{};
  if (!o.endpoint_override.empty()) {
    std::string scheme = o.scheme;
    if (scheme.empty()) scheme = "https";
    if (scheme == "https") {
      options.set<google::cloud::UnifiedCredentialsOption>(
          google::cloud::MakeGoogleDefaultCredentials());
    } else {
      options.set<google::cloud::UnifiedCredentialsOption>(
          google::cloud::MakeInsecureCredentials());
    }
    options.set<gcs::RestEndpointOption>(scheme + "://" + o.endpoint_override);
  }
  return options;
}

class GcsFileSystem::Impl {
 public:
  explicit Impl(GcsOptions o)
      : options_(std::move(o)), client_(AsGoogleCloudOptions(options_)) {}

  const GcsOptions& options() const { return options_; }

  Result<FileInfo> GetFileInfo(const GcsPath& path) {
    if (!path.object.empty()) {
      auto meta = client_.GetObjectMetadata(path.bucket, path.object);
      return GetFileInfoImpl(path, std::move(meta).status(), FileType::File);
    }
    auto meta = client_.GetBucketMetadata(path.bucket);
    return GetFileInfoImpl(path, std::move(meta).status(), FileType::Directory);
  }

 private:
  static Result<FileInfo> GetFileInfoImpl(const GcsPath& path,
                                          const google::cloud::Status& status,
                                          FileType type) {
    if (status.ok()) {
      return FileInfo(path.full_path, type);
    }
    using ::google::cloud::StatusCode;
    if (status.code() == StatusCode::kNotFound) {
      return FileInfo(path.full_path, FileType::NotFound);
    }
    return internal::ToArrowStatus(status);
  }

  GcsOptions options_;
  gcs::Client client_;
};

bool GcsOptions::Equals(const GcsOptions& other) const {
  return endpoint_override == other.endpoint_override && scheme == other.scheme;
}

std::string GcsFileSystem::type_name() const { return "gcs"; }

bool GcsFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& fs = ::arrow::internal::checked_cast<const GcsFileSystem&>(other);
  return impl_->options().Equals(fs.impl_->options());
}

Result<FileInfo> GcsFileSystem::GetFileInfo(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->GetFileInfo(p);
}

Result<FileInfoVector> GcsFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteDirContents(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteFile(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const FileInfo& info) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const FileInfo& info) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> GcsFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> GcsFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("Append is not supported in GCS");
}

GcsFileSystem::GcsFileSystem(const GcsOptions& options, const io::IOContext& context)
    : FileSystem(context), impl_(std::make_shared<Impl>(options)) {}

namespace internal {

std::shared_ptr<GcsFileSystem> MakeGcsFileSystemForTest(const GcsOptions& options) {
  // Cannot use `std::make_shared<>` as the constructor is private.
  return std::shared_ptr<GcsFileSystem>(
      new GcsFileSystem(options, io::default_io_context()));
}

}  // namespace internal

}  // namespace fs
}  // namespace arrow

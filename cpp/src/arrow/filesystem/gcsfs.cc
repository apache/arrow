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

#include "arrow/buffer.h"
#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

#define ARROW_GCS_RETURN_NOT_OK(expr) \
  if (!expr.ok()) return internal::ToArrowStatus(expr)

namespace arrow {
namespace fs {
namespace {

namespace gcs = google::cloud::storage;

auto constexpr kSep = '/';
// Change the default upload buffer size. In general, sending larger buffers is more
// efficient with GCS, as each buffer requires a roundtrip to the service. With formatted
// output (when using `operator<<`), keeping a larger buffer in memory before uploading
// makes sense.  With unformatted output (the only choice given gcs::io::OutputStream's
// API) it is better to let the caller provide as large a buffer as they want. The GCS C++
// client library will upload this buffer with zero copies if possible.
auto constexpr kUploadBufferSize = 256 * 1024;

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

class GcsInputStream : public arrow::io::InputStream {
 public:
  explicit GcsInputStream(gcs::ObjectReadStream stream, std::string bucket_name,
                          std::string object_name, gcs::Generation generation,
                          gcs::Client client)
      : stream_(std::move(stream)),
        bucket_name_(std::move(bucket_name)),
        object_name_(std::move(object_name)),
        generation_(generation),
        client_(std::move(client)) {}

  ~GcsInputStream() override = default;

  //@{
  // @name FileInterface
  Status Close() override {
    stream_.Close();
    return Status::OK();
  }

  Result<int64_t> Tell() const override {
    if (!stream_) {
      return Status::IOError("invalid stream");
    }
    return stream_.tellg();
  }

  bool closed() const override { return !stream_.IsOpen(); }
  //@}

  //@{
  // @name Readable
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    stream_.read(static_cast<char*>(out), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    return stream_.gcount();
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    stream_.read(reinterpret_cast<char*>(buffer->mutable_data()), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    RETURN_NOT_OK(buffer->Resize(stream_.gcount(), true));
    return buffer;
  }
  //@}

  //@{
  // @name InputStream
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    auto metadata = client_.GetObjectMetadata(bucket_name_, object_name_, generation_);
    ARROW_GCS_RETURN_NOT_OK(metadata.status());
    return internal::FromObjectMetadata(*metadata);
  }
  //@}

 private:
  mutable gcs::ObjectReadStream stream_;
  std::string bucket_name_;
  std::string object_name_;
  gcs::Generation generation_;
  gcs::Client client_;
};

class GcsOutputStream : public arrow::io::OutputStream {
 public:
  explicit GcsOutputStream(gcs::ObjectWriteStream stream) : stream_(std::move(stream)) {}
  ~GcsOutputStream() override = default;

  Status Close() override {
    stream_.Close();
    return internal::ToArrowStatus(stream_.last_status());
  }

  Result<int64_t> Tell() const override {
    if (!stream_) {
      return Status::IOError("invalid stream");
    }
    return tell_;
  }

  bool closed() const override { return !stream_.IsOpen(); }

  Status Write(const void* data, int64_t nbytes) override {
    if (stream_.write(reinterpret_cast<const char*>(data), nbytes)) {
      tell_ += nbytes;
      return Status::OK();
    }
    return internal::ToArrowStatus(stream_.last_status());
  }

  Status Flush() override {
    stream_.flush();
    return Status::OK();
  }

 private:
  gcs::ObjectWriteStream stream_;
  int64_t tell_ = 0;
};

}  // namespace

google::cloud::Options AsGoogleCloudOptions(const GcsOptions& o) {
  auto options = google::cloud::Options{};
  std::string scheme = o.scheme;
  if (scheme.empty()) scheme = "https";
  if (scheme == "https") {
    options.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeGoogleDefaultCredentials());
  } else {
    options.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeInsecureCredentials());
  }
  options.set<gcs::UploadBufferSizeOption>(kUploadBufferSize);
  if (!o.endpoint_override.empty()) {
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

  Status CopyFile(const GcsPath& src, const GcsPath& dest) {
    auto metadata =
        client_.RewriteObjectBlocking(src.bucket, src.object, dest.bucket, dest.object);
    return internal::ToArrowStatus(metadata.status());
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const GcsPath& path) {
    auto stream = client_.ReadObject(path.bucket, path.object);
    ARROW_GCS_RETURN_NOT_OK(stream.status());
    return std::make_shared<GcsInputStream>(std::move(stream), path.bucket, path.object,
                                            gcs::Generation(), client_);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const GcsPath& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
    gcs::EncryptionKey encryption_key;
    ARROW_ASSIGN_OR_RAISE(encryption_key, internal::ToEncryptionKey(metadata));
    gcs::PredefinedAcl predefined_acl;
    ARROW_ASSIGN_OR_RAISE(predefined_acl, internal::ToPredefinedAcl(metadata));
    gcs::KmsKeyName kms_key_name;
    ARROW_ASSIGN_OR_RAISE(kms_key_name, internal::ToKmsKeyName(metadata));
    gcs::WithObjectMetadata with_object_metadata;
    ARROW_ASSIGN_OR_RAISE(with_object_metadata, internal::ToObjectMetadata(metadata));

    auto stream = client_.WriteObject(path.bucket, path.object, encryption_key,
                                      predefined_acl, kms_key_name, with_object_metadata);
    ARROW_GCS_RETURN_NOT_OK(stream.last_status());
    return std::make_shared<GcsOutputStream>(std::move(stream));
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
  ARROW_ASSIGN_OR_RAISE(auto s, GcsPath::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto d, GcsPath::FromString(dest));
  return impl_->CopyFile(s, d);
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->OpenInputStream(p);
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const FileInfo& info) {
  if (!info.IsFile()) {
    return Status::IOError("Only files can be opened as input streams");
  }
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  return impl_->OpenInputStream(p);
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
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->OpenOutputStream(p, metadata);
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

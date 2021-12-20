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
#include <algorithm>

#include "arrow/buffer.h"
#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/thread_pool.h"

#define ARROW_GCS_RETURN_NOT_OK(expr) \
  if (!expr.ok()) return internal::ToArrowStatus(expr)

namespace arrow {
namespace fs {
struct GcsCredentials {
  explicit GcsCredentials(std::shared_ptr<google::cloud::Credentials> c)
      : credentials(std::move(c)) {}

  std::shared_ptr<google::cloud::Credentials> credentials;
};

namespace {

namespace gcs = google::cloud::storage;

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
    auto const first_sep = s.find_first_of(internal::kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return GcsPath{s, internal::RemoveTrailingSlash(s).to_string(), ""};
    }
    GcsPath path;
    path.full_path = s;
    path.bucket = s.substr(0, first_sep);
    path.object = s.substr(first_sep + 1);
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
                          gcs::ReadFromOffset offset, gcs::Client client)
      : stream_(std::move(stream)),
        bucket_name_(std::move(bucket_name)),
        object_name_(std::move(object_name)),
        generation_(generation),
        offset_(offset.value_or(0)),
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
    return stream_.tellg() + offset_;
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
  std::int64_t offset_;
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

using InputStreamFactory = std::function<Result<std::shared_ptr<io::InputStream>>(
    const std::string&, const std::string&, gcs::Generation, gcs::ReadFromOffset)>;

class GcsRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  GcsRandomAccessFile(InputStreamFactory factory, gcs::ObjectMetadata metadata,
                      std::shared_ptr<io::InputStream> stream)
      : factory_(std::move(factory)),
        metadata_(std::move(metadata)),
        stream_(std::move(stream)) {}
  ~GcsRandomAccessFile() override = default;

  //@{
  // @name FileInterface
  Status Close() override { return stream_->Close(); }
  Status Abort() override { return stream_->Abort(); }
  Result<int64_t> Tell() const override { return stream_->Tell(); }
  bool closed() const override { return stream_->closed(); }
  //@}

  //@{
  // @name Readable
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    return stream_->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    return stream_->Read(nbytes);
  }
  const arrow::io::IOContext& io_context() const override {
    return stream_->io_context();
  }
  //@}

  //@{
  // @name InputStream
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return internal::FromObjectMetadata(metadata_);
  }
  //@}

  //@{
  // @name RandomAccessFile
  Result<int64_t> GetSize() override { return metadata_.size(); }
  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(metadata_.bucket(), metadata_.name(),
                                           gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(metadata_.bucket(), metadata_.name(),
                                           gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes);
  }
  //@}

  // from Seekable
  Status Seek(int64_t position) override {
    ARROW_ASSIGN_OR_RAISE(stream_, factory_(metadata_.bucket(), metadata_.name(),
                                            gcs::Generation(metadata_.generation()),
                                            gcs::ReadFromOffset(position)));
    return Status::OK();
  }

 private:
  InputStreamFactory factory_;
  gcs::ObjectMetadata metadata_;
  std::shared_ptr<io::InputStream> stream_;
};

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
  if (o.credentials && o.credentials->credentials) {
    options.set<google::cloud::UnifiedCredentialsOption>(o.credentials->credentials);
  }
  return options;
}

}  // namespace

class GcsFileSystem::Impl {
 public:
  explicit Impl(GcsOptions o)
      : options_(std::move(o)), client_(AsGoogleCloudOptions(options_)) {}

  const GcsOptions& options() const { return options_; }

  Result<FileInfo> GetFileInfo(const GcsPath& path) {
    if (path.object.empty()) {
      auto meta = client_.GetBucketMetadata(path.bucket);
      return GetFileInfoDirectory(path, std::move(meta).status());
    }
    auto meta = client_.GetObjectMetadata(path.bucket, path.object);
    if (path.object.back() == '/') {
      return GetFileInfoDirectory(path, std::move(meta).status());
    }
    return GetFileInfoFile(path, meta);
  }

  Result<FileInfoVector> GetFileInfo(const FileSelector& select) {
    ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(select.base_dir));
    const auto canonical = internal::EnsureTrailingSlash(p.object);
    const auto max_depth = internal::Depth(canonical) + select.max_recursion;
    auto prefix = p.object.empty() ? gcs::Prefix() : gcs::Prefix(canonical);
    auto delimiter = select.recursive ? gcs::Delimiter() : gcs::Delimiter("/");
    bool found_directory = false;
    FileInfoVector result;
    for (auto const& o : client_.ListObjects(p.bucket, prefix, delimiter)) {
      if (!o.ok()) {
        if (select.allow_not_found &&
            o.status().code() == google::cloud::StatusCode::kNotFound) {
          continue;
        }
        return internal::ToArrowStatus(o.status());
      }
      found_directory = true;
      // Skip the directory itself from the results, and any result that is "too deep"
      // into the recursion.
      if (o->name() == p.object || internal::Depth(o->name()) > max_depth) {
        continue;
      }
      auto path = internal::ConcatAbstractPath(o->bucket(), o->name());
      if (o->name().back() == '/') {
        result.push_back(
            FileInfo(internal::EnsureTrailingSlash(path), FileType::Directory));
        continue;
      }
      result.push_back(ToFileInfo(path, *o));
    }
    if (!found_directory && !select.allow_not_found) {
      return Status::IOError("No such file or directory '", select.base_dir, "'");
    }
    return result;
  }

  // GCS does not have directories or folders. But folders can be emulated (with some
  // limitations) using marker objects.  That and listing with prefixes creates the
  // illusion of folders.
  google::cloud::Status CreateDirMarker(const std::string& bucket,
                                        util::string_view name) {
    // Make the name canonical.
    const auto canonical = internal::EnsureTrailingSlash(name);
    return client_
        .InsertObject(bucket, canonical, std::string(),
                      gcs::WithObjectMetadata(gcs::ObjectMetadata().upsert_metadata(
                          "arrow/gcsfs", "directory")))
        .status();
  }

  google::cloud::Status CreateDirMarkerRecursive(const std::string& bucket,
                                                 const std::string& object) {
    using GcsCode = google::cloud::StatusCode;
    auto get_parent = [](std::string const& path) {
      return std::move(internal::GetAbstractPathParent(path).first);
    };
    // Maybe counterintuitively we create the markers from the most nested and up. Because
    // GCS does not have directories creating `a/b/c` will succeed, even if `a/` or `a/b/`
    // does not exist.  In the common case, where `a/b/` may already exist, it is more
    // efficient to just create `a/b/c/` and then find out that `a/b/` was already there.
    // In the case where none exists, it does not matter which order we follow.
    auto status = CreateDirMarker(bucket, object);
    if (status.code() == GcsCode::kAlreadyExists) return {};
    if (status.code() == GcsCode::kNotFound) {
      // Missing bucket, create it first ...
      status = client_.CreateBucket(bucket, gcs::BucketMetadata()).status();
      if (status.code() != GcsCode::kOk && status.code() != GcsCode::kAlreadyExists) {
        return status;
      }
    }

    for (auto parent = get_parent(object); !parent.empty(); parent = get_parent(parent)) {
      status = CreateDirMarker(bucket, parent);
      if (status.code() == GcsCode::kAlreadyExists) {
        break;
      }
      if (!status.ok()) {
        return status;
      }
    }
    return {};
  }

  Status CreateDir(const GcsPath& p) {
    if (p.object.empty()) {
      return internal::ToArrowStatus(
          client_.CreateBucket(p.bucket, gcs::BucketMetadata()).status());
    }
    return internal::ToArrowStatus(CreateDirMarker(p.bucket, p.object));
  }

  Status CreateDirRecursive(const GcsPath& p) {
    return internal::ToArrowStatus(CreateDirMarkerRecursive(p.bucket, p.object));
  }

  Status DeleteDir(const GcsPath& p, const io::IOContext& io_context) {
    RETURN_NOT_OK(DeleteDirContents(p, io_context));
    if (!p.object.empty()) {
      return internal::ToArrowStatus(client_.DeleteObject(p.bucket, p.object));
    }
    return internal::ToArrowStatus(client_.DeleteBucket(p.bucket));
  }

  Status DeleteDirContents(const GcsPath& p, const io::IOContext& io_context) {
    // Deleting large directories can be fairly slow, we need to parallelize the
    // operation.
    auto async_delete =
        [&p, this](const google::cloud::StatusOr<gcs::ObjectMetadata>& o) -> Status {
      if (!o) return internal::ToArrowStatus(o.status());
      // The list includes the directory, skip it. DeleteDir() takes care of it.
      if (o->bucket() == p.bucket && o->name() == p.object) return {};
      return internal::ToArrowStatus(
          client_.DeleteObject(o->bucket(), o->name(), gcs::Generation(o->generation())));
    };

    std::vector<Future<>> submitted;
    // This iterates over all the objects, and schedules parallel deletes.
    auto prefix = p.object.empty() ? gcs::Prefix() : gcs::Prefix(p.object);
    for (const auto& o : client_.ListObjects(p.bucket, prefix)) {
      submitted.push_back(DeferNotOk(io_context.executor()->Submit(async_delete, o)));
    }

    return AllFinished(submitted).status();
  }

  Status DeleteFile(const GcsPath& p) {
    if (!p.object.empty() && p.object.back() == '/') {
      return Status::IOError("The given path (" + p.full_path +
                             ") is a directory, use DeleteDir");
    }
    return internal::ToArrowStatus(client_.DeleteObject(p.bucket, p.object));
  }

  Status Move(const GcsPath& src, const GcsPath& dest) {
    if (src.full_path.empty() || src.object.empty() ||
        src.object.back() == internal::kSep) {
      return Status::IOError(
          "Moving directories or buckets cannot be implemented in GCS. You provided (" +
          src.full_path + ") as a source for Move()");
    }
    ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(dest));
    if (info.IsDirectory()) {
      return Status::IOError("Attempting to Move() to an existing directory");
    }
    RETURN_NOT_OK(CopyFile(src, dest));
    return DeleteFile(src);
  }

  Status CopyFile(const GcsPath& src, const GcsPath& dest) {
    auto metadata =
        client_.RewriteObjectBlocking(src.bucket, src.object, dest.bucket, dest.object);
    return internal::ToArrowStatus(metadata.status());
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const std::string& bucket_name,
                                                           const std::string& object_name,
                                                           gcs::Generation generation,
                                                           gcs::ReadFromOffset offset) {
    auto stream = client_.ReadObject(bucket_name, object_name, generation, offset);
    ARROW_GCS_RETURN_NOT_OK(stream.status());
    return std::make_shared<GcsInputStream>(std::move(stream), bucket_name, object_name,
                                            gcs::Generation(), offset, client_);
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

  google::cloud::StatusOr<gcs::ObjectMetadata> GetObjectMetadata(const GcsPath& path) {
    return client_.GetObjectMetadata(path.bucket, path.object);
  }

 private:
  static Result<FileInfo> GetFileInfoDirectory(const GcsPath& path,
                                               const google::cloud::Status& status) {
    using ::google::cloud::StatusCode;
    auto canonical = internal::EnsureTrailingSlash(path.full_path);
    if (status.ok()) {
      return FileInfo(canonical, FileType::Directory);
    }
    if (status.code() == StatusCode::kNotFound) {
      return FileInfo(canonical, FileType::NotFound);
    }
    return internal::ToArrowStatus(status);
  }

  static Result<FileInfo> GetFileInfoFile(
      const GcsPath& path, const google::cloud::StatusOr<gcs::ObjectMetadata>& meta) {
    if (meta.ok()) {
      return ToFileInfo(path.full_path, *meta);
    }
    using ::google::cloud::StatusCode;
    if (meta.status().code() == StatusCode::kNotFound) {
      return FileInfo(path.full_path, FileType::NotFound);
    }
    return internal::ToArrowStatus(meta.status());
  }

  static FileInfo ToFileInfo(const std::string& full_path,
                             const gcs::ObjectMetadata& meta) {
    auto info = FileInfo(full_path, FileType::File);
    info.set_size(static_cast<int64_t>(meta.size()));
    // An object has multiple "time" attributes, including the time when its data was
    // created, and the time when its metadata was last updated. We use the object
    // creation time because the data for an object cannot be changed once created.
    info.set_mtime(meta.time_created());
    return info;
  }

  GcsOptions options_;
  gcs::Client client_;
};

bool GcsOptions::Equals(const GcsOptions& other) const {
  return credentials == other.credentials &&
         endpoint_override == other.endpoint_override && scheme == other.scheme;
}

GcsOptions GcsOptions::Defaults() {
  return GcsOptions{
      std::make_shared<GcsCredentials>(google::cloud::MakeGoogleDefaultCredentials()),
      {},
      "https"};
}

GcsOptions GcsOptions::Anonymous() {
  return GcsOptions{
      std::make_shared<GcsCredentials>(google::cloud::MakeInsecureCredentials()),
      {},
      "http"};
}

GcsOptions GcsOptions::FromAccessToken(const std::string& access_token,
                                       std::chrono::system_clock::time_point expiration) {
  return GcsOptions{
      std::make_shared<GcsCredentials>(
          google::cloud::MakeAccessTokenCredentials(access_token, expiration)),
      {},
      "https"};
}

GcsOptions GcsOptions::FromImpersonatedServiceAccount(
    const GcsCredentials& base_credentials, const std::string& target_service_account) {
  return GcsOptions{std::make_shared<GcsCredentials>(
                        google::cloud::MakeImpersonateServiceAccountCredentials(
                            base_credentials.credentials, target_service_account)),
                    {},
                    "https"};
}

GcsOptions GcsOptions::FromServiceAccountCredentials(const std::string& json_object) {
  return GcsOptions{std::make_shared<GcsCredentials>(
                        google::cloud::MakeServiceAccountCredentials(json_object)),
                    {},
                    "https"};
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
  return impl_->GetFileInfo(select);
}

Status GcsFileSystem::CreateDir(const std::string& path, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  if (!recursive) return impl_->CreateDir(p);
  return impl_->CreateDirRecursive(p);
}

Status GcsFileSystem::DeleteDir(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->DeleteDir(p, io_context());
}

Status GcsFileSystem::DeleteDirContents(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->DeleteDirContents(p, io_context());
}

Status GcsFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented(
      std::string(__func__) +
      " is not implemented as it is too dangerous to delete all the buckets");
}

Status GcsFileSystem::DeleteFile(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->DeleteFile(p);
}

Status GcsFileSystem::Move(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto s, GcsPath::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto d, GcsPath::FromString(dest));
  return impl_->Move(s, d);
}

Status GcsFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto s, GcsPath::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto d, GcsPath::FromString(dest));
  return impl_->CopyFile(s, d);
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(),
                                gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const FileInfo& info) {
  if (!info.IsFile()) {
    return Status::IOError("Only files can be opened as input streams");
  }
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  return impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(),
                                gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl](const std::string& b, const std::string& o, gcs::Generation g,
                            gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(b, o, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(
      auto stream,
      impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(metadata->generation()),
                             gcs::ReadFromOffset()));

  return std::make_shared<GcsRandomAccessFile>(std::move(open_stream),
                                               *std::move(metadata), std::move(stream));
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl](const std::string& b, const std::string& o, gcs::Generation g,
                            gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(b, o, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(
      auto stream,
      impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(metadata->generation()),
                             gcs::ReadFromOffset()));

  return std::make_shared<GcsRandomAccessFile>(std::move(open_stream),
                                               *std::move(metadata), std::move(stream));
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

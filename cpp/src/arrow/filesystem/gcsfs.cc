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
#include "arrow/filesystem/util_internal.h"
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
using GcsCode = google::cloud::StatusCode;
using GcsStatus = google::cloud::Status;

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
    if (internal::IsLikelyUri(s)) {
      return Status::Invalid(
          "Expected a GCS object path of the form 'bucket/key...', got a URI: '", s, "'");
    }
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

  GcsPath parent() const {
    auto object_parent = internal::GetAbstractPathParent(object).first;
    if (object_parent.empty()) return GcsPath{bucket, bucket, ""};
    return GcsPath{internal::ConcatAbstractPath(bucket, object_parent), bucket,
                   object_parent};
  }

  bool empty() const { return bucket.empty() && object.empty(); }

  bool operator==(const GcsPath& other) const {
    return bucket == other.bucket && object == other.object;
  }
};

class GcsInputStream : public arrow::io::InputStream {
 public:
  explicit GcsInputStream(gcs::ObjectReadStream stream, GcsPath path,
                          gcs::Generation generation, gcs::ReadFromOffset offset,
                          gcs::Client client)
      : stream_(std::move(stream)),
        path_(std::move(path)),
        generation_(generation),
        offset_(offset.value_or(0)),
        client_(std::move(client)) {}

  ~GcsInputStream() override = default;

  //@{
  // @name FileInterface
  Status Close() override {
    stream_.Close();
    closed_ = true;
    return Status::OK();
  }

  Result<int64_t> Tell() const override {
    if (closed()) return Status::Invalid("Cannot use Tell() on a closed stream");
    return stream_.tellg() + offset_;
  }

  // A gcs::ObjectReadStream can be "born closed".  For small objects the stream returns
  // `IsOpen() == false` as soon as it is created, but the application can still read from
  // it.
  bool closed() const override { return closed_ && !stream_.IsOpen(); }
  //@}

  //@{
  // @name Readable
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (closed()) return Status::Invalid("Cannot read from a closed stream");
    stream_.read(static_cast<char*>(out), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    return stream_.gcount();
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    if (closed()) return Status::Invalid("Cannot read from a closed stream");
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    stream_.read(reinterpret_cast<char*>(buffer->mutable_data()), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    RETURN_NOT_OK(buffer->Resize(stream_.gcount(), true));
    return std::shared_ptr<Buffer>(std::move(buffer));
  }
  //@}

  //@{
  // @name InputStream
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    auto metadata = client_.GetObjectMetadata(path_.bucket, path_.object, generation_);
    ARROW_GCS_RETURN_NOT_OK(metadata.status());
    return internal::FromObjectMetadata(*metadata);
  }
  //@}

 private:
  mutable gcs::ObjectReadStream stream_;
  GcsPath path_;
  gcs::Generation generation_;
  std::int64_t offset_;
  gcs::Client client_;
  bool closed_ = false;
};

class GcsOutputStream : public arrow::io::OutputStream {
 public:
  explicit GcsOutputStream(gcs::ObjectWriteStream stream) : stream_(std::move(stream)) {}
  ~GcsOutputStream() override = default;

  Status Close() override {
    stream_.Close();
    closed_ = true;
    return internal::ToArrowStatus(stream_.last_status());
  }

  Result<int64_t> Tell() const override {
    if (closed()) return Status::Invalid("Cannot use Tell() on a closed stream");
    return tell_;
  }

  // gcs::ObjectWriteStream can be "closed" without an explicit Close() call. At this time
  // this class does not use any of the mechanisms [*] that trigger such behavior.
  // Nevertheless, we defensively prepare for them by checking either condition.
  //
  // [*]: These mechanisms include:
  // - resumable uploads that are "resumed" after the upload completed are born
  //   "closed",
  // - uploads that prescribe their total size using the `x-upload-content-length` header
  //   are completed and "closed" as soon as the upload reaches that size.
  bool closed() const override { return closed_ || !stream_.IsOpen(); }

  Status Write(const void* data, int64_t nbytes) override {
    if (closed()) return Status::Invalid("Cannot write to a closed stream");
    if (stream_.write(reinterpret_cast<const char*>(data), nbytes)) {
      tell_ += nbytes;
      return Status::OK();
    }
    return internal::ToArrowStatus(stream_.last_status());
  }

  Status Flush() override {
    if (closed()) return Status::Invalid("Cannot flush a closed stream");
    stream_.flush();
    return Status::OK();
  }

 private:
  gcs::ObjectWriteStream stream_;
  int64_t tell_ = 0;
  bool closed_ = false;
};

using InputStreamFactory = std::function<Result<std::shared_ptr<io::InputStream>>(
    gcs::Generation, gcs::ReadFromOffset)>;

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
    if (closed()) return Status::Invalid("Cannot read from closed file");
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    if (closed()) return Status::Invalid("Cannot read from closed file");
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes);
  }
  //@}

  // from Seekable
  Status Seek(int64_t position) override {
    if (closed()) return Status::Invalid("Cannot seek in a closed file");
    ARROW_ASSIGN_OR_RAISE(stream_, factory_(gcs::Generation(metadata_.generation()),
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
      return GetFileInfoBucket(path, std::move(meta).status());
    }
    auto meta = client_.GetObjectMetadata(path.bucket, path.object);
    return GetFileInfoObject(path, meta);
  }

  Result<FileInfoVector> GetFileInfo(const FileSelector& select) {
    ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(select.base_dir));
    // Adding the trailing '/' avoids problems with files named 'a', 'ab', 'ac'  where GCS
    // would return all of them if the prefix is 'a'.
    const auto canonical = internal::EnsureTrailingSlash(p.object);
    const auto max_depth = internal::Depth(canonical) + select.max_recursion;
    auto prefix = p.object.empty() ? gcs::Prefix() : gcs::Prefix(canonical);
    auto delimiter = select.recursive ? gcs::Delimiter() : gcs::Delimiter("/");
    FileInfoVector result;
    for (auto const& o : client_.ListObjects(p.bucket, prefix, delimiter)) {
      if (!o.ok()) {
        if (select.allow_not_found &&
            o.status().code() == google::cloud::StatusCode::kNotFound) {
          return result;
        }
        return internal::ToArrowStatus(o.status());
      }
      // Skip the directory itself from the results, and any result that is "too deep"
      // into the recursion.
      if (o->name() == p.object || internal::Depth(o->name()) > max_depth) {
        continue;
      }
      auto path = internal::ConcatAbstractPath(o->bucket(), o->name());
      result.push_back(ToFileInfo(path, *o));
    }
    // Finding any elements indicates the directory was found.
    if (!result.empty() || select.allow_not_found) {
      return result;
    }
    // To find out if the directory exists we need to perform an additional query.
    ARROW_ASSIGN_OR_RAISE(auto directory, GetFileInfo(p));
    if (directory.IsDirectory()) return result;
    if (directory.IsFile()) {
      return Status::IOError("Cannot use file '", select.base_dir, "' as a directory");
    }
    return Status::IOError("No such file or directory '", select.base_dir, "'");
  }

  // GCS does not have directories or folders. But folders can be emulated (with some
  // limitations) using marker objects.  That and listing with prefixes creates the
  // illusion of folders.
  google::cloud::StatusOr<gcs::ObjectMetadata> CreateDirMarker(const std::string& bucket,
                                                               util::string_view name) {
    // Make the name canonical.
    const auto canonical = internal::RemoveTrailingSlash(name).to_string();
    google::cloud::StatusOr<gcs::ObjectMetadata> object = client_.InsertObject(
        bucket, canonical, std::string(),
        gcs::WithObjectMetadata(
            gcs::ObjectMetadata().upsert_metadata("arrow/gcsfs", "directory")),
        gcs::IfGenerationMatch(0));
    if (object) return object;
    if (object.status().code() == GcsCode::kFailedPrecondition) {
      // The marker already exists, find out if it is a directory or a file.
      return client_.GetObjectMetadata(bucket, canonical);
    }
    return object;
  }

  static Status NotDirectoryError(const gcs::ObjectMetadata& o) {
    return Status::IOError(
        "Cannot create directory, it conflicts with an existing file '",
        internal::ConcatAbstractPath(o.bucket(), o.name()), "'");
  }

  Status CreateDirMarkerRecursive(const std::string& bucket, const std::string& name) {
    auto get_parent = [](std::string const& path) {
      return std::move(internal::GetAbstractPathParent(path).first);
    };
    // Find the list of missing parents. In the process we discover if any elements in
    // the path are files, this is unavoidable as GCS does not really have directories.
    std::vector<std::string> missing_parents;
    auto dir = name;
    for (; !dir.empty(); dir = get_parent(dir)) {
      auto o = client_.GetObjectMetadata(bucket, dir);
      if (o) {
        if (IsDirectory(*o)) break;
        return NotDirectoryError(*o);
      }
      missing_parents.push_back(dir);
    }
    if (dir.empty()) {
      // We could not find any of the parent directories in the bucket, the last step is
      // to find out if the bucket exists, and if necessary, create it
      google::cloud::StatusOr<gcs::BucketMetadata> b = client_.GetBucketMetadata(bucket);
      if (!b) {
        if (b.status().code() == GcsCode::kNotFound) {
          b = client_.CreateBucket(bucket, gcs::BucketMetadata().set_location(
                                               options_.default_bucket_location));
        }
        if (!b) return internal::ToArrowStatus(b.status());
      }
    }

    // Note that the list of parents are sorted from deepest to most shallow, this is
    // convenient because as soon as we find a directory we can stop the iteration.
    for (auto const& d : missing_parents) {
      auto o = CreateDirMarker(bucket, d);
      if (o) {
        if (IsDirectory(*o)) continue;
        // This is probably a race condition, something created a file before we managed
        // to create the directories.
        return NotDirectoryError(*o);
      }
    }
    return Status::OK();
  }

  Status CreateDir(const GcsPath& p) {
    if (p.object.empty()) {
      return internal::ToArrowStatus(
          client_
              .CreateBucket(p.bucket, gcs::BucketMetadata().set_location(
                                          options_.default_bucket_location))
              .status());
    }
    auto parent = p.parent();
    if (!parent.object.empty()) {
      auto o = client_.GetObjectMetadata(p.bucket, parent.object);
      if (!IsDirectory(*o)) return NotDirectoryError(*o);
    }
    return internal::ToArrowStatus(CreateDirMarker(p.bucket, p.object).status());
  }

  Status CreateDirRecursive(const GcsPath& p) {
    return CreateDirMarkerRecursive(p.bucket, p.object);
  }

  Status DeleteDir(const GcsPath& p, const io::IOContext& io_context) {
    RETURN_NOT_OK(DeleteDirContents(p, /*missing_dir_ok=*/false, io_context));
    if (!p.object.empty()) {
      return internal::ToArrowStatus(client_.DeleteObject(p.bucket, p.object));
    }
    return internal::ToArrowStatus(client_.DeleteBucket(p.bucket));
  }

  Status DeleteDirContents(const GcsPath& p, bool missing_dir_ok,
                           const io::IOContext& io_context) {
    // If the directory marker exists, it better be a directory.
    auto dir = client_.GetObjectMetadata(p.bucket, p.object);
    if (dir && !IsDirectory(*dir)) return NotDirectoryError(*dir);

    // Deleting large directories can be fairly slow, we need to parallelize the
    // operation.
    const auto& canonical =
        p.object.empty() ? p.object : internal::EnsureTrailingSlash(p.object);
    auto async_delete =
        [&, this](const google::cloud::StatusOr<gcs::ObjectMetadata>& o) -> Status {
      if (!o) return internal::ToArrowStatus(o.status());
      // The list includes the directory, skip it. DeleteDir() takes care of it.
      if (o->bucket() == p.bucket && o->name() == canonical) return Status::OK();
      return internal::ToArrowStatus(
          client_.DeleteObject(o->bucket(), o->name(), gcs::Generation(o->generation())));
    };

    std::vector<Future<>> submitted;
    // This iterates over all the objects, and schedules parallel deletes.
    auto prefix = p.object.empty() ? gcs::Prefix() : gcs::Prefix(canonical);
    bool at_least_one_obj = false;
    for (const auto& o : client_.ListObjects(p.bucket, prefix)) {
      at_least_one_obj = true;
      submitted.push_back(DeferNotOk(io_context.executor()->Submit(async_delete, o)));
    }

    if (!missing_dir_ok && !at_least_one_obj && !dir) {
      // No files were found and no directory marker exists
      return Status::IOError("No such directory: ", p.full_path);
    }

    return AllFinished(submitted).status();
  }

  Status DeleteFile(const GcsPath& p) {
    if (!p.object.empty()) {
      auto stat = client_.GetObjectMetadata(p.bucket, p.object);
      if (!stat) return internal::ToArrowStatus(stat.status());
      if (IsDirectory(*stat)) {
        return Status::IOError("The given path '", p.full_path,
                               "' is a directory, use DeleteDir");
      }
    }
    return internal::ToArrowStatus(client_.DeleteObject(p.bucket, p.object));
  }

  Status Move(const GcsPath& src, const GcsPath& dest) {
    if (src == dest) return Status::OK();
    if (src.object.empty()) {
      return Status::IOError(
          "Moving directories or buckets cannot be implemented in GCS. You provided (",
          src.full_path, ") as a source for Move()");
    }
    ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(dest));
    if (info.IsDirectory()) {
      return Status::IOError("Attempting to Move() '", info.path(),
                             "' to an existing directory");
    }
    ARROW_ASSIGN_OR_RAISE(auto src_info, GetFileInfo(src));
    if (!src_info.IsFile()) {
      return Status::IOError("Cannot move source '", src.full_path,
                             "' the object does not exist or does not represent a file");
    }
    RETURN_NOT_OK(CopyFile(src, dest));
    return DeleteFile(src);
  }

  Status CopyFile(const GcsPath& src, const GcsPath& dest) {
    auto parent = dest.parent();
    if (!parent.object.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto parent_info, GetFileInfo(parent));
      if (parent_info.IsFile()) {
        return Status::IOError("Cannot use file '", parent.full_path,
                               "' as a destination directory");
      }
    }
    auto metadata =
        client_.RewriteObjectBlocking(src.bucket, src.object, dest.bucket, dest.object);
    return internal::ToArrowStatus(metadata.status());
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const GcsPath& path,
                                                           gcs::Generation generation,
                                                           gcs::ReadFromOffset offset) {
    auto stream = client_.ReadObject(path.bucket, path.object, generation, offset);
    ARROW_GCS_RETURN_NOT_OK(stream.status());
    return std::make_shared<GcsInputStream>(std::move(stream), path, gcs::Generation(),
                                            offset, client_);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const GcsPath& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
    std::shared_ptr<const KeyValueMetadata> resolved_metadata = metadata;
    if (resolved_metadata == nullptr && options_.default_metadata != nullptr) {
      resolved_metadata = options_.default_metadata;
    }
    gcs::EncryptionKey encryption_key;
    ARROW_ASSIGN_OR_RAISE(encryption_key, internal::ToEncryptionKey(resolved_metadata));
    gcs::PredefinedAcl predefined_acl;
    ARROW_ASSIGN_OR_RAISE(predefined_acl, internal::ToPredefinedAcl(resolved_metadata));
    gcs::KmsKeyName kms_key_name;
    ARROW_ASSIGN_OR_RAISE(kms_key_name, internal::ToKmsKeyName(resolved_metadata));
    gcs::WithObjectMetadata with_object_metadata;
    ARROW_ASSIGN_OR_RAISE(with_object_metadata,
                          internal::ToObjectMetadata(resolved_metadata));

    auto stream = client_.WriteObject(path.bucket, path.object, encryption_key,
                                      predefined_acl, kms_key_name, with_object_metadata);
    ARROW_GCS_RETURN_NOT_OK(stream.last_status());
    return std::make_shared<GcsOutputStream>(std::move(stream));
  }

  google::cloud::StatusOr<gcs::ObjectMetadata> GetObjectMetadata(const GcsPath& path) {
    return client_.GetObjectMetadata(path.bucket, path.object);
  }

 private:
  static bool IsDirectory(const gcs::ObjectMetadata& o) {
    return o.has_metadata("arrow/gcsfs") && o.metadata("arrow/gcsfs") == "directory";
  }

  static Result<FileInfo> GetFileInfoBucket(const GcsPath& path,
                                            const google::cloud::Status& status) {
    if (status.ok()) {
      return FileInfo(path.bucket, FileType::Directory);
    }
    using ::google::cloud::StatusCode;
    if (status.code() == StatusCode::kNotFound) {
      return FileInfo(path.bucket, FileType::NotFound);
    }
    return internal::ToArrowStatus(status);
  }

  static Result<FileInfo> GetFileInfoObject(
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
    if (IsDirectory(meta)) {
      return FileInfo(full_path, FileType::Directory);
    }
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
         endpoint_override == other.endpoint_override && scheme == other.scheme &&
         default_bucket_location == other.default_bucket_location;
}

GcsOptions GcsOptions::Defaults() {
  GcsOptions options{};
  options.credentials =
      std::make_shared<GcsCredentials>(google::cloud::MakeGoogleDefaultCredentials());
  options.scheme = "https";
  return options;
}

GcsOptions GcsOptions::Anonymous() {
  GcsOptions options{};
  options.credentials =
      std::make_shared<GcsCredentials>(google::cloud::MakeInsecureCredentials());
  options.scheme = "http";
  return options;
}

GcsOptions GcsOptions::FromAccessToken(const std::string& access_token,
                                       std::chrono::system_clock::time_point expiration) {
  GcsOptions options{};
  options.credentials = std::make_shared<GcsCredentials>(
      google::cloud::MakeAccessTokenCredentials(access_token, expiration));
  options.scheme = "https";
  return options;
}

GcsOptions GcsOptions::FromImpersonatedServiceAccount(
    const GcsCredentials& base_credentials, const std::string& target_service_account) {
  GcsOptions options{};
  options.credentials = std::make_shared<GcsCredentials>(
      google::cloud::MakeImpersonateServiceAccountCredentials(
          base_credentials.credentials, target_service_account));
  options.scheme = "https";
  return options;
}

GcsOptions GcsOptions::FromServiceAccountCredentials(const std::string& json_object) {
  GcsOptions options{};
  options.credentials = std::make_shared<GcsCredentials>(
      google::cloud::MakeServiceAccountCredentials(json_object));
  options.scheme = "https";
  return options;
}

Result<GcsOptions> GcsOptions::FromUri(const arrow::internal::Uri& uri,
                                       std::string* out_path) {
  const auto bucket = uri.host();
  auto path = uri.path();
  if (bucket.empty()) {
    if (!path.empty()) {
      return Status::Invalid("Missing bucket name in GCS URI");
    }
  } else {
    if (path.empty()) {
      path = bucket;
    } else {
      if (path[0] != '/') {
        return Status::Invalid("GCS URI should be absolute, not relative");
      }
      path = bucket + path;
    }
  }
  if (out_path != nullptr) {
    *out_path = std::string(internal::RemoveTrailingSlash(path));
  }

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  if (!uri.password().empty() || !uri.username().empty()) {
    return Status::Invalid("GCS does not accept username or password.");
  }

  auto options = GcsOptions::Defaults();
  for (const auto& kv : options_map) {
    if (kv.first == "location") {
      options.default_bucket_location = kv.second;
    } else if (kv.first == "scheme") {
      options.scheme = kv.second;
    } else if (kv.first == "endpoint_override") {
      options.endpoint_override = kv.second;
    } else {
      return Status::Invalid("Unexpected query parameter in GCS URI: '", kv.first, "'");
    }
  }

  return options;
}

Result<GcsOptions> GcsOptions::FromUri(const std::string& uri_string,
                                       std::string* out_path) {
  arrow::internal::Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
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

Status GcsFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->DeleteDirContents(p, missing_dir_ok, io_context());
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
  return impl_->OpenInputStream(p, gcs::Generation(), gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const FileInfo& info) {
  if (info.IsDirectory()) {
    return Status::IOError("Cannot open directory '", info.path(),
                           "' as an input stream");
  }
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  return impl_->OpenInputStream(p, gcs::Generation(), gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl, p](gcs::Generation g, gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(p, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(auto stream,
                        impl_->OpenInputStream(p, gcs::Generation(metadata->generation()),
                                               gcs::ReadFromOffset()));

  return std::make_shared<GcsRandomAccessFile>(std::move(open_stream),
                                               *std::move(metadata), std::move(stream));
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const FileInfo& info) {
  if (info.IsDirectory()) {
    return Status::IOError("Cannot open directory '", info.path(),
                           "' as an input stream");
  }
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl, p](gcs::Generation g, gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(p, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(auto stream,
                        impl_->OpenInputStream(p, gcs::Generation(metadata->generation()),
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

std::shared_ptr<GcsFileSystem> GcsFileSystem::Make(const GcsOptions& options,
                                                   const io::IOContext& context) {
  // Cannot use `std::make_shared<>` as the constructor is private.
  return std::shared_ptr<GcsFileSystem>(new GcsFileSystem(options, context));
}

GcsFileSystem::GcsFileSystem(const GcsOptions& options, const io::IOContext& context)
    : FileSystem(context), impl_(std::make_shared<Impl>(options)) {}

}  // namespace fs
}  // namespace arrow

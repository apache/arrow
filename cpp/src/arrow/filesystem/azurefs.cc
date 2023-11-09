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

#include "arrow/filesystem/azurefs.h"
#include "arrow/filesystem/azurefs_internal.h"

#include <azure/storage/blobs.hpp>
#include <azure/storage/files/datalake.hpp>

#include "arrow/buffer.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace fs {

// -----------------------------------------------------------------------
// AzureOptions Implementation

AzureOptions::AzureOptions() {}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return (account_dfs_url == other.account_dfs_url &&
          account_blob_url == other.account_blob_url &&
          credentials_kind == other.credentials_kind);
}

Status AzureOptions::ConfigureAccountKeyCredentials(const std::string& account_name,
                                                    const std::string& account_key) {
  if (this->backend == AzureBackend::Azurite) {
    account_blob_url = "http://127.0.0.1:10000/" + account_name + "/";
    account_dfs_url = "http://127.0.0.1:10000/" + account_name + "/";
  } else {
    account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
    account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  }
  storage_credentials_provider =
      std::make_shared<Azure::Storage::StorageSharedKeyCredential>(account_name,
                                                                   account_key);
  credentials_kind = AzureCredentialsKind::StorageCredentials;
  return Status::OK();
}

namespace {

// An AzureFileSystem represents a single Azure storage account. AzurePath describes a
// container and path within that storage account.
struct AzurePath {
  std::string full_path;
  std::string container;
  std::string path_to_file;
  std::vector<std::string> path_to_file_parts;

  static Result<AzurePath> FromString(const std::string& s) {
    // Example expected string format: testcontainer/testdir/testfile.txt
    // container = testcontainer
    // path_to_file = testdir/testfile.txt
    // path_to_file_parts = [testdir, testfile.txt]
    if (internal::IsLikelyUri(s)) {
      return Status::Invalid(
          "Expected an Azure object path of the form 'container/path...', got a URI: '",
          s, "'");
    }
    auto first_sep = s.find_first_of(internal::kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return AzurePath{std::string(s), std::string(s), "", {}};
    }
    AzurePath path;
    path.full_path = std::string(s);
    path.container = std::string(s.substr(0, first_sep));
    path.path_to_file = std::string(s.substr(first_sep + 1));
    path.path_to_file_parts = internal::SplitAbstractPath(path.path_to_file);
    RETURN_NOT_OK(Validate(path));
    return path;
  }

  static Status Validate(const AzurePath& path) {
    auto status = internal::ValidateAbstractPathParts(path.path_to_file_parts);
    if (!status.ok()) {
      return Status::Invalid(status.message(), " in path ", path.full_path);
    } else {
      return status;
    }
  }

  AzurePath parent() const {
    DCHECK(has_parent());
    auto parent = AzurePath{"", container, "", path_to_file_parts};
    parent.path_to_file_parts.pop_back();
    parent.path_to_file = internal::JoinAbstractPath(parent.path_to_file_parts);
    if (parent.path_to_file.empty()) {
      parent.full_path = parent.container;
    } else {
      parent.full_path = parent.container + internal::kSep + parent.path_to_file;
    }
    return parent;
  }

  bool has_parent() const { return !path_to_file.empty(); }

  bool empty() const { return container.empty() && path_to_file.empty(); }

  bool operator==(const AzurePath& other) const {
    return container == other.container && path_to_file == other.path_to_file;
  }
};

Status PathNotFound(const AzurePath& path) {
  return ::arrow::fs::internal::PathNotFound(path.full_path);
}

Status NotAFile(const AzurePath& path) {
  return ::arrow::fs::internal::NotAFile(path.full_path);
}

Status ValidateFilePath(const AzurePath& path) {
  if (path.container.empty()) {
    return PathNotFound(path);
  }

  if (path.path_to_file.empty()) {
    return NotAFile(path);
  }
  return Status::OK();
}

template <typename ArrowType>
std::string FormatValue(typename TypeTraits<ArrowType>::CType value) {
  struct StringAppender {
    std::string string;
    Status operator()(std::string_view view) {
      string.append(view.data(), view.size());
      return Status::OK();
    }
  } appender;
  arrow::internal::StringFormatter<ArrowType> formatter;
  ARROW_UNUSED(formatter(value, appender));
  return appender.string;
}

std::shared_ptr<const KeyValueMetadata> PropertiesToMetadata(
    const Azure::Storage::Blobs::Models::BlobProperties& properties) {
  auto metadata = std::make_shared<KeyValueMetadata>();
  // Not supported yet:
  // * properties.ObjectReplicationSourceProperties
  // * properties.Metadata
  //
  // They may have the same key defined in the following
  // metadata->Append() list. If we have duplicated key in metadata,
  // the first value may be only used by users because
  // KeyValueMetadata::Get() returns the first found value. Note that
  // users can use all values by using KeyValueMetadata::keys() and
  // KeyValueMetadata::values().
  if (properties.ImmutabilityPolicy.HasValue()) {
    metadata->Append("Immutability-Policy-Expires-On",
                     properties.ImmutabilityPolicy.Value().ExpiresOn.ToString());
    metadata->Append("Immutability-Policy-Mode",
                     properties.ImmutabilityPolicy.Value().PolicyMode.ToString());
  }
  metadata->Append("Content-Type", properties.HttpHeaders.ContentType);
  metadata->Append("Content-Encoding", properties.HttpHeaders.ContentEncoding);
  metadata->Append("Content-Language", properties.HttpHeaders.ContentLanguage);
  const auto& content_hash = properties.HttpHeaders.ContentHash.Value;
  metadata->Append("Content-Hash", HexEncode(content_hash.data(), content_hash.size()));
  metadata->Append("Content-Disposition", properties.HttpHeaders.ContentDisposition);
  metadata->Append("Cache-Control", properties.HttpHeaders.CacheControl);
  metadata->Append("Last-Modified", properties.LastModified.ToString());
  metadata->Append("Created-On", properties.CreatedOn.ToString());
  if (properties.ObjectReplicationDestinationPolicyId.HasValue()) {
    metadata->Append("Object-Replication-Destination-Policy-Id",
                     properties.ObjectReplicationDestinationPolicyId.Value());
  }
  metadata->Append("Blob-Type", properties.BlobType.ToString());
  if (properties.CopyCompletedOn.HasValue()) {
    metadata->Append("Copy-Completed-On", properties.CopyCompletedOn.Value().ToString());
  }
  if (properties.CopyStatusDescription.HasValue()) {
    metadata->Append("Copy-Status-Description", properties.CopyStatusDescription.Value());
  }
  if (properties.CopyId.HasValue()) {
    metadata->Append("Copy-Id", properties.CopyId.Value());
  }
  if (properties.CopyProgress.HasValue()) {
    metadata->Append("Copy-Progress", properties.CopyProgress.Value());
  }
  if (properties.CopySource.HasValue()) {
    metadata->Append("Copy-Source", properties.CopySource.Value());
  }
  if (properties.CopyStatus.HasValue()) {
    metadata->Append("Copy-Status", properties.CopyStatus.Value().ToString());
  }
  if (properties.IsIncrementalCopy.HasValue()) {
    metadata->Append("Is-Incremental-Copy",
                     FormatValue<BooleanType>(properties.IsIncrementalCopy.Value()));
  }
  if (properties.IncrementalCopyDestinationSnapshot.HasValue()) {
    metadata->Append("Incremental-Copy-Destination-Snapshot",
                     properties.IncrementalCopyDestinationSnapshot.Value());
  }
  if (properties.LeaseDuration.HasValue()) {
    metadata->Append("Lease-Duration", properties.LeaseDuration.Value().ToString());
  }
  if (properties.LeaseState.HasValue()) {
    metadata->Append("Lease-State", properties.LeaseState.Value().ToString());
  }
  if (properties.LeaseStatus.HasValue()) {
    metadata->Append("Lease-Status", properties.LeaseStatus.Value().ToString());
  }
  metadata->Append("Content-Length", FormatValue<Int64Type>(properties.BlobSize));
  if (properties.ETag.HasValue()) {
    metadata->Append("ETag", properties.ETag.ToString());
  }
  if (properties.SequenceNumber.HasValue()) {
    metadata->Append("Sequence-Number",
                     FormatValue<Int64Type>(properties.SequenceNumber.Value()));
  }
  if (properties.CommittedBlockCount.HasValue()) {
    metadata->Append("Committed-Block-Count",
                     FormatValue<Int32Type>(properties.CommittedBlockCount.Value()));
  }
  metadata->Append("IsServerEncrypted",
                   FormatValue<BooleanType>(properties.IsServerEncrypted));
  if (properties.EncryptionKeySha256.HasValue()) {
    const auto& sha256 = properties.EncryptionKeySha256.Value();
    metadata->Append("Encryption-Key-Sha-256", HexEncode(sha256.data(), sha256.size()));
  }
  if (properties.EncryptionScope.HasValue()) {
    metadata->Append("Encryption-Scope", properties.EncryptionScope.Value());
  }
  if (properties.AccessTier.HasValue()) {
    metadata->Append("Access-Tier", properties.AccessTier.Value().ToString());
  }
  if (properties.IsAccessTierInferred.HasValue()) {
    metadata->Append("Is-Access-Tier-Inferred",
                     FormatValue<BooleanType>(properties.IsAccessTierInferred.Value()));
  }
  if (properties.ArchiveStatus.HasValue()) {
    metadata->Append("Archive-Status", properties.ArchiveStatus.Value().ToString());
  }
  if (properties.AccessTierChangedOn.HasValue()) {
    metadata->Append("Access-Tier-Changed-On",
                     properties.AccessTierChangedOn.Value().ToString());
  }
  if (properties.VersionId.HasValue()) {
    metadata->Append("Version-Id", properties.VersionId.Value());
  }
  if (properties.IsCurrentVersion.HasValue()) {
    metadata->Append("Is-Current-Version",
                     FormatValue<BooleanType>(properties.IsCurrentVersion.Value()));
  }
  if (properties.TagCount.HasValue()) {
    metadata->Append("Tag-Count", FormatValue<Int32Type>(properties.TagCount.Value()));
  }
  if (properties.ExpiresOn.HasValue()) {
    metadata->Append("Expires-On", properties.ExpiresOn.Value().ToString());
  }
  if (properties.IsSealed.HasValue()) {
    metadata->Append("Is-Sealed", FormatValue<BooleanType>(properties.IsSealed.Value()));
  }
  if (properties.RehydratePriority.HasValue()) {
    metadata->Append("Rehydrate-Priority",
                     properties.RehydratePriority.Value().ToString());
  }
  if (properties.LastAccessedOn.HasValue()) {
    metadata->Append("Last-Accessed-On", properties.LastAccessedOn.Value().ToString());
  }
  metadata->Append("Has-Legal-Hold", FormatValue<BooleanType>(properties.HasLegalHold));
  return metadata;
}

class ObjectInputFile final : public io::RandomAccessFile {
 public:
  ObjectInputFile(std::shared_ptr<Azure::Storage::Blobs::BlobClient> blob_client,
                  const io::IOContext& io_context, AzurePath path, int64_t size = kNoSize)
      : blob_client_(std::move(blob_client)),
        io_context_(io_context),
        path_(std::move(path)),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = blob_client_->GetProperties();
      content_length_ = properties.Value.BlobSize;
      metadata_ = PropertiesToMetadata(properties.Value);
      return Status::OK();
    } catch (const Azure::Storage::StorageException& exception) {
      if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
        return PathNotFound(path_);
      }
      return internal::ExceptionToStatus(
          "GetProperties failed for '" + blob_client_->GetUrl() +
              "' with an unexpected Azure error. Can not initialise an ObjectInputFile "
              "without knowing the file size.",
          exception);
    }
  }

  Status CheckClosed(const char* action) const {
    if (closed_) {
      return Status::Invalid("Cannot ", action, " on closed file.");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    DCHECK_GE(content_length_, 0);
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return metadata_;
  }

  Future<std::shared_ptr<const KeyValueMetadata>> ReadMetadataAsync(
      const io::IOContext& io_context) override {
    return metadata_;
  }

  Status Close() override {
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed("tell"));
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed("size"));
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed("seek"));
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed("read"));
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    Azure::Core::Http::HttpRange range{position, nbytes};
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;
    download_options.Range = range;
    try {
      return blob_client_
          ->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, download_options)
          .Value.ContentRange.Length.Value();
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus("DownloadTo from '" + blob_client_->GetUrl() +
                                             "' at position " + std::to_string(position) +
                                             " for " + std::to_string(nbytes) +
                                             " bytes failed with an Azure error. ReadAt "
                                             "failed to read the required byte range.",
                                         exception);
    }
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed("read"));
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          AllocateResizableBuffer(nbytes, io_context_.pool()));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buffer->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buffer->Resize(bytes_read));
    }
    return std::move(buffer);
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

 private:
  std::shared_ptr<Azure::Storage::Blobs::BlobClient> blob_client_;
  const io::IOContext io_context_;
  AzurePath path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};
}  // namespace

// -----------------------------------------------------------------------
// AzureFilesystem Implementation

class AzureFileSystem::Impl {
 public:
  io::IOContext io_context_;
  std::unique_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
      datalake_service_client_;
  std::unique_ptr<Azure::Storage::Blobs::BlobServiceClient> blob_service_client_;
  AzureOptions options_;
  internal::HierarchicalNamespaceDetector hierarchical_namespace_;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    blob_service_client_ = std::make_unique<Azure::Storage::Blobs::BlobServiceClient>(
        options_.account_blob_url, options_.storage_credentials_provider);
    datalake_service_client_ =
        std::make_unique<Azure::Storage::Files::DataLake::DataLakeServiceClient>(
            options_.account_dfs_url, options_.storage_credentials_provider);
    RETURN_NOT_OK(hierarchical_namespace_.Init(datalake_service_client_.get()));
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

 public:
  Result<FileInfo> GetFileInfo(const AzurePath& path) {
    FileInfo info;
    info.set_path(path.full_path);

    if (path.container.empty()) {
      DCHECK(path.path_to_file.empty());  // The path is invalid if the container is empty
                                          // but not path_to_file.
      // path must refer to the root of the Azure storage account. This is a directory,
      // and there isn't any extra metadata to fetch.
      info.set_type(FileType::Directory);
      return info;
    }
    if (path.path_to_file.empty()) {
      // path refers to a container. This is a directory if it exists.
      auto container_client =
          blob_service_client_->GetBlobContainerClient(path.container);
      try {
        auto properties = container_client.GetProperties();
        info.set_type(FileType::Directory);
        info.set_mtime(
            std::chrono::system_clock::time_point(properties.Value.LastModified));
        return info;
      } catch (const Azure::Storage::StorageException& exception) {
        if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
          info.set_type(FileType::NotFound);
          return info;
        }
        return internal::ExceptionToStatus(
            "GetProperties for '" + container_client.GetUrl() +
                "' failed with an unexpected Azure error. GetFileInfo is unable to "
                "determine whether the container exists.",
            exception);
      }
    }
    auto file_client = datalake_service_client_->GetFileSystemClient(path.container)
                           .GetFileClient(path.path_to_file);
    try {
      auto properties = file_client.GetProperties();
      if (properties.Value.IsDirectory) {
        info.set_type(FileType::Directory);
      } else if (internal::HasTrailingSlash(path.path_to_file)) {
        // For a path with a trailing slash a hierarchical namespace may return a blob
        // with that trailing slash removed. For consistency with flat namespace and
        // other filesystems we chose to return NotFound.
        info.set_type(FileType::NotFound);
        return info;
      } else {
        info.set_type(FileType::File);
        info.set_size(properties.Value.FileSize);
      }
      info.set_mtime(
          std::chrono::system_clock::time_point(properties.Value.LastModified));
      return info;
    } catch (const Azure::Storage::StorageException& exception) {
      if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
        ARROW_ASSIGN_OR_RAISE(auto hierarchical_namespace_enabled,
                              hierarchical_namespace_.Enabled(path.container));
        if (hierarchical_namespace_enabled) {
          // If the hierarchical namespace is enabled, then the storage account will have
          // explicit directories. Neither a file nor a directory was found.
          info.set_type(FileType::NotFound);
          return info;
        }
        // On flat namespace accounts there are no real directories. Directories are only
        // implied by using `/` in the blob name.
        Azure::Storage::Blobs::ListBlobsOptions list_blob_options;

        // If listing the prefix `path.path_to_file` with trailing slash returns at least
        // one result then `path` refers to an implied directory.
        auto prefix = internal::EnsureTrailingSlash(path.path_to_file);
        list_blob_options.Prefix = prefix;
        // We only need to know if there is at least one result, so minimise page size
        // for efficiency.
        list_blob_options.PageSizeHint = 1;

        try {
          auto paged_list_result =
              blob_service_client_->GetBlobContainerClient(path.container)
                  .ListBlobs(list_blob_options);
          if (paged_list_result.Blobs.size() > 0) {
            info.set_type(FileType::Directory);
          } else {
            info.set_type(FileType::NotFound);
          }
          return info;
        } catch (const Azure::Storage::StorageException& exception) {
          return internal::ExceptionToStatus(
              "ListBlobs for '" + prefix +
                  "' failed with an unexpected Azure error. GetFileInfo is unable to "
                  "determine whether the path should be considered an implied directory.",
              exception);
        }
      }
      return internal::ExceptionToStatus(
          "GetProperties for '" + file_client.GetUrl() +
              "' failed with an unexpected "
              "Azure error. GetFileInfo is unable to determine whether the path exists.",
          exception);
    }
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const std::string& s,
                                                         AzureFileSystem* fs) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(s));
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
    RETURN_NOT_OK(ValidateFilePath(path));
    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        blob_service_client_->GetBlobContainerClient(path.container)
            .GetBlobClient(path.path_to_file));

    auto ptr =
        std::make_shared<ObjectInputFile>(blob_client, fs->io_context(), std::move(path));
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const FileInfo& info,
                                                         AzureFileSystem* fs) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(info.path()));
    if (info.type() == FileType::NotFound) {
      return ::arrow::fs::internal::PathNotFound(info.path());
    }
    if (info.type() != FileType::File && info.type() != FileType::Unknown) {
      return ::arrow::fs::internal::NotAFile(info.path());
    }
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(info.path()));
    RETURN_NOT_OK(ValidateFilePath(path));
    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        blob_service_client_->GetBlobContainerClient(path.container)
            .GetBlobClient(path.path_to_file));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(),
                                                 std::move(path), info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }
};

const AzureOptions& AzureFileSystem::options() const { return impl_->options(); }

bool AzureFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& azure_fs = ::arrow::internal::checked_cast<const AzureFileSystem&>(other);
  return options().Equals(azure_fs.options());
}

Result<FileInfo> AzureFileSystem::GetFileInfo(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, AzurePath::FromString(path));
  return impl_->GetFileInfo(p);
}

Result<FileInfoVector> AzureFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteFile(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const std::string& path) {
  return impl_->OpenInputFile(path, this);
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const std::string& path) {
  return impl_->OpenInputFile(path, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> AzureFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<AzureFileSystem>> AzureFileSystem::Make(
    const AzureOptions& options, const io::IOContext& io_context) {
  std::shared_ptr<AzureFileSystem> ptr(new AzureFileSystem(options, io_context));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

AzureFileSystem::AzureFileSystem(const AzureOptions& options,
                                 const io::IOContext& io_context)
    : FileSystem(io_context), impl_(std::make_unique<Impl>(options, io_context)) {
  default_async_is_sync_ = false;
}

}  // namespace fs
}  // namespace arrow

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
#include "arrow/io/interfaces.h"
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

// An AzureFileSystem represents a single Azure storage
// account. AzureLocation describes a container and path within
// that storage account.
struct AzureLocation {
  std::string all;
  std::string container;
  std::string path;
  std::vector<std::string> path_parts;

  static Result<AzureLocation> FromString(const std::string& string) {
    // Example expected string format: testcontainer/testdir/testfile.txt
    // container = testcontainer
    // path = testdir/testfile.txt
    // path_parts = [testdir, testfile.txt]
    if (internal::IsLikelyUri(string)) {
      return Status::Invalid(
          "Expected an Azure object location of the form 'container/path...', got a URI: "
          "'",
          string, "'");
    }
    auto first_sep = string.find_first_of(internal::kSep);
    if (first_sep == 0) {
      return Status::Invalid("Location cannot start with a separator ('", string, "')");
    }
    if (first_sep == std::string::npos) {
      return AzureLocation{string, string, "", {}};
    }
    AzureLocation location;
    location.all = string;
    location.container = string.substr(0, first_sep);
    location.path = string.substr(first_sep + 1);
    location.path_parts = internal::SplitAbstractPath(location.path);
    RETURN_NOT_OK(location.Validate());
    return location;
  }

  AzureLocation parent() const {
    DCHECK(has_parent());
    AzureLocation parent{"", container, "", path_parts};
    parent.path_parts.pop_back();
    parent.path = internal::JoinAbstractPath(parent.path_parts);
    if (parent.path.empty()) {
      parent.all = parent.container;
    } else {
      parent.all = parent.container + internal::kSep + parent.path;
    }
    return parent;
  }

  bool has_parent() const { return !path.empty(); }

  bool empty() const { return container.empty() && path.empty(); }

  bool operator==(const AzureLocation& other) const {
    return container == other.container && path == other.path;
  }

 private:
  Status Validate() {
    auto status = internal::ValidateAbstractPathParts(path_parts);
    if (!status.ok()) {
      return Status::Invalid(status.message(), " in location ", all);
    } else {
      return status;
    }
  }
};

Status PathNotFound(const AzureLocation& location) {
  return ::arrow::fs::internal::PathNotFound(location.all);
}

Status NotAFile(const AzureLocation& location) {
  return ::arrow::fs::internal::NotAFile(location.all);
}

Status ValidateFileLocation(const AzureLocation& location) {
  if (location.container.empty()) {
    return PathNotFound(location);
  }
  if (location.path.empty()) {
    return NotAFile(location);
  }
  return Status::OK();
}

Status StatusFromErrorResponse(const std::string& url,
                               Azure::Core::Http::RawResponse* raw_response,
                               const std::string& context) {
  const auto& body = raw_response->GetBody();
  // There isn't an Azure specification that response body on error
  // doesn't contain any binary data but we assume it. We hope that
  // error response body has useful information for the error.
  std::string_view body_text(reinterpret_cast<const char*>(body.data()), body.size());
  return Status::IOError(context, ": ", url, ": ", raw_response->GetReasonPhrase(), " (",
                         static_cast<int>(raw_response->GetStatusCode()),
                         "): ", body_text);
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
                  const io::IOContext& io_context, AzureLocation location,
                  int64_t size = kNoSize)
      : blob_client_(std::move(blob_client)),
        io_context_(io_context),
        location_(std::move(location)),
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
        return PathNotFound(location_);
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
  AzureLocation location_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

class ObjectAppendStream final : public io::OutputStream {
 public:
  ObjectAppendStream(std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client,
                     const bool is_hierarchical_namespace_enabled,
                     const io::IOContext& io_context, const AzurePath& path,
                     const std::shared_ptr<const KeyValueMetadata>& metadata)
      : blob_client_(std::move(blob_client)), io_context_(io_context), path_(path) {}

  ~ObjectAppendStream() override {
    // For compliance with the rest of the IO stack, Close rather than Abort,
    // even though it may be more expensive.
    io::internal::CloseFromDestructor(this);
  }

  Status Init() {
    closed_ = false;
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = blob_client_->GetProperties();
      // TODO: Consider adding a check for whether its a directory.
      content_length_ = properties.Value.BlobSize;
      pos_ = content_length_;
    } catch (const Azure::Storage::StorageException& exception) {
      // new file

      std::string s = "";
      try {
        blob_client_->UploadFrom(
            const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(s.data())), s.size());
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
      content_length_ = 0;
    }
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return pos_;
  }

  Status Write(const std::shared_ptr<Buffer>& buffer) override {
    return DoAppend(buffer->data(), buffer->size(), buffer);
  }

  Status Write(const void* data, int64_t nbytes) override {
    return DoAppend(data, nbytes);
  }

  Status DoAppend(const void* data, int64_t nbytes,
                  std::shared_ptr<Buffer> owned_buffer = nullptr) {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    try {
      auto append_data = static_cast<uint8_t*>((void*)data);
      auto res = blob_client_->GetBlockList().Value;
      auto size = res.CommittedBlocks.size();
      std::string block_id;
      {
        block_id = std::to_string(size + 1);
        size_t n = 8;
        int precision = n - std::min(n, block_id.size());
        block_id.insert(0, precision, '0');
      }
      block_id = Azure::Core::Convert::Base64Encode(
          std::vector<uint8_t>(block_id.begin(), block_id.end()));
      auto block_content = Azure::Core::IO::MemoryBodyStream(
          append_data, strlen(reinterpret_cast<char*>(append_data)));
      if (block_content.Length() == 0) {
        return Status::OK();
      }
      blob_client_->StageBlock(block_id, block_content);
      std::vector<std::string> block_ids;
      for (auto block : res.CommittedBlocks) {
        block_ids.push_back(block.Name);
      }
      block_ids.push_back(block_id);
      blob_client_->CommitBlockList(block_ids);
      pos_ += nbytes;
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    content_length_ += nbytes;
    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    try {
      auto res = blob_client_->GetBlockList().Value;
      std::vector<std::string> block_ids;
      for (auto block : res.UncommittedBlocks) {
        block_ids.push_back(block.Name);
      }
      blob_client_->CommitBlockList(block_ids);
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

 protected:
  std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client_;
  const io::IOContext io_context_;
  const AzurePath path_;

  bool closed_ = true;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
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
  Result<FileInfo> GetFileInfo(const AzureLocation& location) {
    FileInfo info;
    info.set_path(location.all);

    if (location.container.empty()) {
      // The location is invalid if the container is empty but not
      // path.
      DCHECK(location.path.empty());
      // The location must refer to the root of the Azure storage
      // account. This is a directory, and there isn't any extra
      // metadata to fetch.
      info.set_type(FileType::Directory);
      return info;
    }
    if (location.path.empty()) {
      // The location refers to a container. This is a directory if it exists.
      auto container_client =
          blob_service_client_->GetBlobContainerClient(location.container);
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
    auto file_client = datalake_service_client_->GetFileSystemClient(location.container)
                           .GetFileClient(location.path);
    try {
      auto properties = file_client.GetProperties();
      if (properties.Value.IsDirectory) {
        info.set_type(FileType::Directory);
      } else if (internal::HasTrailingSlash(location.path)) {
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
                              hierarchical_namespace_.Enabled(location.container));
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
        auto prefix = internal::EnsureTrailingSlash(location.path);
        list_blob_options.Prefix = prefix;
        // We only need to know if there is at least one result, so minimise page size
        // for efficiency.
        list_blob_options.PageSizeHint = 1;

        try {
          auto paged_list_result =
              blob_service_client_->GetBlobContainerClient(location.container)
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

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const AzureLocation& location,
                                                         AzureFileSystem* fs) {
    RETURN_NOT_OK(ValidateFileLocation(location));
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(location.path));
    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        blob_service_client_->GetBlobContainerClient(location.container)
            .GetBlobClient(location.path));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(),
                                                 std::move(location));
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
    ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(info.path()));
    RETURN_NOT_OK(ValidateFileLocation(location));
    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        blob_service_client_->GetBlobContainerClient(location.container)
            .GetBlobClient(location.path));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(),
                                                 std::move(location), info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Status CreateDir(const AzureLocation& location) {
    if (location.container.empty()) {
      return Status::Invalid("Cannot create an empty container");
    }

    if (location.path.empty()) {
      auto container_client =
          blob_service_client_->GetBlobContainerClient(location.container);
      try {
        auto response = container_client.Create();
        if (response.Value.Created) {
          return Status::OK();
        } else {
          return StatusFromErrorResponse(
              container_client.GetUrl(), response.RawResponse.get(),
              "Failed to create a container: " + location.container);
        }
      } catch (const Azure::Storage::StorageException& exception) {
        return internal::ExceptionToStatus(
            "Failed to create a container: " + location.container + ": " +
                container_client.GetUrl(),
            exception);
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto hierarchical_namespace_enabled,
                          hierarchical_namespace_.Enabled(location.container));
    if (!hierarchical_namespace_enabled) {
      // Without hierarchical namespace enabled Azure blob storage has no directories.
      // Therefore we can't, and don't need to create one. Simply creating a blob with `/`
      // in the name implies directories.
      return Status::OK();
    }

    auto directory_client =
        datalake_service_client_->GetFileSystemClient(location.container)
            .GetDirectoryClient(location.path);
    try {
      auto response = directory_client.Create();
      if (response.Value.Created) {
        return Status::OK();
      } else {
        return StatusFromErrorResponse(directory_client.GetUrl(),
                                       response.RawResponse.get(),
                                       "Failed to create a directory: " + location.path);
      }
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus(
          "Failed to create a directory: " + location.path + ": " +
              directory_client.GetUrl(),
          exception);
    }
  }

  Status CreateDirRecursive(const AzureLocation& location) {
    if (location.container.empty()) {
      return Status::Invalid("Cannot create an empty container");
    }

    auto container_client =
        blob_service_client_->GetBlobContainerClient(location.container);
    try {
      container_client.CreateIfNotExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus(
          "Failed to create a container: " + location.container + " (" +
              container_client.GetUrl() + ")",
          exception);
    }

    ARROW_ASSIGN_OR_RAISE(auto hierarchical_namespace_enabled,
                          hierarchical_namespace_.Enabled(location.container));
    if (!hierarchical_namespace_enabled) {
      // Without hierarchical namespace enabled Azure blob storage has no directories.
      // Therefore we can't, and don't need to create one. Simply creating a blob with `/`
      // in the name implies directories.
      return Status::OK();
    }

    if (!location.path.empty()) {
      auto directory_client =
          datalake_service_client_->GetFileSystemClient(location.container)
              .GetDirectoryClient(location.path);
      try {
        directory_client.CreateIfNotExists();
      } catch (const Azure::Storage::StorageException& exception) {
        return internal::ExceptionToStatus(
            "Failed to create a directory: " + location.path + " (" +
                directory_client.GetUrl() + ")",
            exception);
      }
    }

    return Status::OK();
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
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->GetFileInfo(location);
}

Result<FileInfoVector> AzureFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::CreateDir(const std::string& path, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  if (recursive) {
    return impl_->CreateDirRecursive(location);
  } else {
    return impl_->CreateDir(location);
  }
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
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->OpenInputFile(location, this);
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->OpenInputFile(location, this);
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

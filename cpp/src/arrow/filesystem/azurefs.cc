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
#include "arrow/io/util_internal.h"
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
          credentials_kind == other.credentials_kind &&
          default_metadata == other.default_metadata);
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

  Result<AzureLocation> join(const std::string& stem) const {
    return FromString(internal::ConcatAbstractPath(all, stem));
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
  ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(location.path));
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
              "' with an unexpected Azure error. Cannot initialise an ObjectInputFile "
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

Status CreateEmptyBlockBlob(
    std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> block_blob_client) {
  try {
    block_blob_client->UploadFrom(nullptr, 0);
  } catch (const Azure::Storage::StorageException& exception) {
    return internal::ExceptionToStatus(
        "UploadFrom failed for '" + block_blob_client->GetUrl() +
            "' with an unexpected Azure error. There is no existing blob at this "
            "location or the existing blob must be replaced so ObjectAppendStream must "
            "create a new empty block blob.",
        exception);
  }
  return Status::OK();
}

Result<Azure::Storage::Blobs::Models::GetBlockListResult> GetBlockList(
    std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> block_blob_client) {
  try {
    return block_blob_client->GetBlockList().Value;
  } catch (Azure::Storage::StorageException& exception) {
    return internal::ExceptionToStatus(
        "GetBlockList failed for '" + block_blob_client->GetUrl() +
            "' with an unexpected Azure error. Cannot write to a file without first "
            "fetching the existing block list.",
        exception);
  }
}

Azure::Storage::Metadata ArrowMetadataToAzureMetadata(
    const std::shared_ptr<const KeyValueMetadata>& arrow_metadata) {
  Azure::Storage::Metadata azure_metadata;
  for (auto key_value : arrow_metadata->sorted_pairs()) {
    azure_metadata[key_value.first] = key_value.second;
  }
  return azure_metadata;
}

Status CommitBlockList(
    std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> block_blob_client,
    const std::vector<std::string>& block_ids, const Azure::Storage::Metadata& metadata) {
  Azure::Storage::Blobs::CommitBlockListOptions options;
  options.Metadata = metadata;
  try {
    // CommitBlockList puts all block_ids in the latest element. That means in the case of
    // overlapping block_ids the newly staged block ids will always replace the
    // previously committed blocks.
    // https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list?tabs=microsoft-entra-id#request-body
    block_blob_client->CommitBlockList(block_ids, options);
  } catch (const Azure::Storage::StorageException& exception) {
    return internal::ExceptionToStatus(
        "CommitBlockList failed for '" + block_blob_client->GetUrl() +
            "' with an unexpected Azure error. Committing is required to flush an "
            "output/append stream.",
        exception);
  }
  return Status::OK();
}

class ObjectAppendStream final : public io::OutputStream {
 public:
  ObjectAppendStream(
      std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> block_blob_client,
      const io::IOContext& io_context, const AzureLocation& location,
      const std::shared_ptr<const KeyValueMetadata>& metadata,
      const AzureOptions& options, int64_t size = kNoSize)
      : block_blob_client_(std::move(block_blob_client)),
        io_context_(io_context),
        location_(location),
        content_length_(size) {
    if (metadata && metadata->size() != 0) {
      metadata_ = ArrowMetadataToAzureMetadata(metadata);
    } else if (options.default_metadata && options.default_metadata->size() != 0) {
      metadata_ = ArrowMetadataToAzureMetadata(options.default_metadata);
    }
  }

  ~ObjectAppendStream() override {
    // For compliance with the rest of the IO stack, Close rather than Abort,
    // even though it may be more expensive.
    io::internal::CloseFromDestructor(this);
  }

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      pos_ = content_length_;
    } else {
      try {
        auto properties = block_blob_client_->GetProperties();
        content_length_ = properties.Value.BlobSize;
        pos_ = content_length_;
      } catch (const Azure::Storage::StorageException& exception) {
        if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
          RETURN_NOT_OK(CreateEmptyBlockBlob(block_blob_client_));
        } else {
          return internal::ExceptionToStatus(
              "GetProperties failed for '" + block_blob_client_->GetUrl() +
                  "' with an unexpected Azure error. Cannot initialise an "
                  "ObjectAppendStream without knowing whether a file already exists at "
                  "this path, and if it exists, its size.",
              exception);
        }
        content_length_ = 0;
      }
    }
    if (content_length_ > 0) {
      ARROW_ASSIGN_OR_RAISE(auto block_list, GetBlockList(block_blob_client_));
      for (auto block : block_list.CommittedBlocks) {
        block_ids_.push_back(block.Name);
      }
    }
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    block_blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    RETURN_NOT_OK(Flush());
    block_blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Status CheckClosed(const char* action) const {
    if (closed_) {
      return Status::Invalid("Cannot ", action, " on closed stream.");
    }
    return Status::OK();
  }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed("tell"));
    return pos_;
  }

  Status Write(const std::shared_ptr<Buffer>& buffer) override {
    return DoAppend(buffer->data(), buffer->size(), buffer);
  }

  Status Write(const void* data, int64_t nbytes) override {
    return DoAppend(data, nbytes);
  }

  Status Flush() override {
    RETURN_NOT_OK(CheckClosed("flush"));
    return CommitBlockList(block_blob_client_, block_ids_, metadata_);
  }

 private:
  Status DoAppend(const void* data, int64_t nbytes,
                  std::shared_ptr<Buffer> owned_buffer = nullptr) {
    RETURN_NOT_OK(CheckClosed("append"));
    auto append_data = reinterpret_cast<const uint8_t*>(data);
    Azure::Core::IO::MemoryBodyStream block_content(append_data, nbytes);
    if (block_content.Length() == 0) {
      return Status::OK();
    }

    const auto n_block_ids = block_ids_.size();

    // New block ID must always be distinct from the existing block IDs. Otherwise we
    // will accidentally replace the content of existing blocks, causing corruption.
    // We will use monotonically increasing integers.
    auto new_block_id = std::to_string(n_block_ids);

    // Pad to 5 digits, because Azure allows a maximum of 50,000 blocks.
    const size_t target_number_of_digits = 5;
    const auto required_padding_digits =
        target_number_of_digits - std::min(target_number_of_digits, new_block_id.size());
    new_block_id.insert(0, required_padding_digits, '0');
    // There is a small risk when appending to a blob created by another client that
    // `new_block_id` may overlapping with an existing block id. Adding the `-arrow`
    // suffix significantly reduces the risk, but does not 100% eliminate it. For example
    // if the blob was previously created with one block, with id `00001-arrow` then the
    // next block we append will conflict with that, and cause corruption.
    new_block_id += "-arrow";
    new_block_id = Azure::Core::Convert::Base64Encode(
        std::vector<uint8_t>(new_block_id.begin(), new_block_id.end()));

    try {
      block_blob_client_->StageBlock(new_block_id, block_content);
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus(
          "StageBlock failed for '" + block_blob_client_->GetUrl() + "' new_block_id: '" +
              new_block_id +
              "' with an unexpected Azure error. Staging new blocks is fundamental to "
              "streaming writes to blob storage.",
          exception);
    }
    block_ids_.push_back(new_block_id);
    pos_ += nbytes;
    content_length_ += nbytes;
    return Status::OK();
  }

  std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> block_blob_client_;
  const io::IOContext io_context_;
  const AzureLocation location_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::vector<std::string> block_ids_;
  Azure::Storage::Metadata metadata_;
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

 private:
  template <typename OnContainer>
  Status VisitContainers(const Azure::Core::Context& context,
                         OnContainer&& on_container) const {
    Azure::Storage::Blobs::ListBlobContainersOptions options;
    try {
      auto container_list_response =
          blob_service_client_->ListBlobContainers(options, context);
      for (; container_list_response.HasPage();
           container_list_response.MoveToNextPage(context)) {
        for (const auto& container : container_list_response.BlobContainers) {
          RETURN_NOT_OK(on_container(container));
        }
      }
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus("Failed to list account containers.", exception);
    }
    return Status::OK();
  }

  static FileInfo FileInfoFromBlob(const std::string& container,
                                   const Azure::Storage::Blobs::Models::BlobItem& blob) {
    auto path = internal::ConcatAbstractPath(container, blob.Name);
    if (blob.Name.back() == internal::kSep) {
      return DirectoryFileInfoFromPath(path);
    }
    FileInfo info{std::move(path), FileType::File};
    info.set_size(blob.BlobSize);
    info.set_mtime(std::chrono::system_clock::time_point{blob.Details.LastModified});
    return info;
  }

  static FileInfo DirectoryFileInfoFromPath(const std::string& path) {
    return FileInfo{std::string{internal::RemoveTrailingSlash(path)},
                    FileType::Directory};
  }

  static std::string_view BasenameView(std::string_view s) {
    DCHECK(!internal::HasTrailingSlash(s));
    auto offset = s.find_last_of(internal::kSep);
    auto result = (offset == std::string_view::npos) ? s : s.substr(offset);
    DCHECK(!result.empty() && result.back() != internal::kSep);
    return result;
  }

  /// \brief List the blobs at the root of a container or some dir in a container.
  ///
  /// \pre container_client is the client for the container named like the first
  /// segment of select.base_dir.
  Status GetFileInfoWithSelectorFromContainer(
      const Azure::Storage::Blobs::BlobContainerClient& container_client,
      const Azure::Core::Context& context, Azure::Nullable<int32_t> page_size_hint,
      const FileSelector& select, FileInfoVector* acc_results) {
    ARROW_ASSIGN_OR_RAISE(auto base_location, AzureLocation::FromString(select.base_dir));

    bool found = false;
    Azure::Storage::Blobs::ListBlobsOptions options;
    if (internal::GetAbstractPathDepth(base_location.path) == 0) {
      // If the base_dir is the root of the container, then we want to list all blobs in
      // the container and the Prefix should be empty and not even include the trailing
      // slash because the container itself represents the `<container>/` directory.
      options.Prefix = {};
      found = true;  // Unless the container itself is not found later!
    } else {
      options.Prefix = internal::EnsureTrailingSlash(base_location.path);
    }
    options.PageSizeHint = page_size_hint;
    options.Include = Azure::Storage::Blobs::Models::ListBlobsIncludeFlags::Metadata;

    // When Prefix.Value() contains a trailing slash and we find a blob that
    // matches it completely, it is an empty directory marker blob for the
    // directory we're listing from, and we should skip it.
    auto is_empty_dir_marker =
        [&options](const Azure::Storage::Blobs::Models::BlobItem& blob) noexcept -> bool {
      return options.Prefix.HasValue() && blob.Name == options.Prefix.Value();
    };

    auto recurse = [&](const std::string& blob_prefix) noexcept -> Status {
      if (select.recursive && select.max_recursion > 0) {
        FileSelector sub_select;
        sub_select.base_dir = base_location.container;
        sub_select.base_dir += internal::kSep;
        sub_select.base_dir += internal::RemoveTrailingSlash(blob_prefix);
        sub_select.allow_not_found = true;
        sub_select.recursive = true;
        sub_select.max_recursion = select.max_recursion - 1;
        return GetFileInfoWithSelectorFromContainer(
            container_client, context, page_size_hint, sub_select, acc_results);
      }
      return Status::OK();
    };

    // (*acc_results)[*last_dir_reported] is the last FileType::Directory in the results
    // produced through this loop over the response pages.
    std::optional<size_t> last_dir_reported{};
    auto matches_last_dir_reported = [&last_dir_reported,
                                      acc_results](const FileInfo& info) noexcept {
      if (!last_dir_reported.has_value() || info.type() != FileType::Directory) {
        return false;
      }
      const auto& last_dir = (*acc_results)[*last_dir_reported];
      return BasenameView(info.path()) == BasenameView(last_dir.path());
    };

    auto process_blob =
        [&](const Azure::Storage::Blobs::Models::BlobItem& blob) noexcept {
          if (!is_empty_dir_marker(blob)) {
            const auto& info = acc_results->emplace_back(
                FileInfoFromBlob(base_location.container, blob));
            if (info.type() == FileType::Directory) {
              last_dir_reported = acc_results->size() - 1;
            }
          }
        };
    auto process_prefix = [&](const std::string& prefix) noexcept -> Status {
      const auto path = internal::ConcatAbstractPath(base_location.container, prefix);
      const auto& info = acc_results->emplace_back(DirectoryFileInfoFromPath(path));
      if (ARROW_PREDICT_FALSE(matches_last_dir_reported(info))) {
        acc_results->pop_back();
      } else {
        last_dir_reported = acc_results->size() - 1;
        return recurse(prefix);
      }
      return Status::OK();
    };

    try {
      auto list_response =
          container_client.ListBlobsByHierarchy(/*delimiter=*/"/", options, context);
      for (; list_response.HasPage(); list_response.MoveToNextPage(context)) {
        if (list_response.Blobs.empty() && list_response.BlobPrefixes.empty()) {
          continue;
        }
        found = true;
        // Blob and BlobPrefixes are sorted by name, so we can merge-iterate
        // them to ensure returned results are all sorted.
        size_t blob_index = 0;
        size_t blob_prefix_index = 0;
        while (blob_index < list_response.Blobs.size() &&
               blob_prefix_index < list_response.BlobPrefixes.size()) {
          const auto& blob = list_response.Blobs[blob_index];
          const auto& prefix = list_response.BlobPrefixes[blob_prefix_index];
          const int cmp = blob.Name.compare(prefix);
          if (cmp < 0) {
            process_blob(blob);
            blob_index += 1;
          } else if (cmp > 0) {
            RETURN_NOT_OK(process_prefix(prefix));
            blob_prefix_index += 1;
          } else {  // there is a blob (empty dir marker) and a prefix with the same name
            DCHECK_EQ(blob.Name, prefix);
            RETURN_NOT_OK(process_prefix(prefix));
            blob_index += 1;
            blob_prefix_index += 1;
          }
        }
        for (; blob_index < list_response.Blobs.size(); blob_index++) {
          process_blob(list_response.Blobs[blob_index]);
        }
        for (; blob_prefix_index < list_response.BlobPrefixes.size();
             blob_prefix_index++) {
          RETURN_NOT_OK(process_prefix(list_response.BlobPrefixes[blob_prefix_index]));
        }
      }
    } catch (const Azure::Storage::StorageException& exception) {
      if (exception.ErrorCode == "ContainerNotFound") {
        found = false;
      } else {
        return internal::ExceptionToStatus(
            "Failed to list blobs in a directory: " + select.base_dir + ": " +
                container_client.GetUrl(),
            exception);
      }
    }

    return found || select.allow_not_found
               ? Status::OK()
               : ::arrow::fs::internal::PathNotFound(select.base_dir);
  }

 public:
  Status GetFileInfoWithSelector(const Azure::Core::Context& context,
                                 Azure::Nullable<int32_t> page_size_hint,
                                 const FileSelector& select,
                                 FileInfoVector* acc_results) {
    ARROW_ASSIGN_OR_RAISE(auto base_location, AzureLocation::FromString(select.base_dir));

    if (base_location.container.empty()) {
      // Without a container, the base_location is equivalent to the filesystem
      // root -- `/`. FileSelector::allow_not_found doesn't matter in this case
      // because the root always exists.
      auto on_container =
          [&](const Azure::Storage::Blobs::Models::BlobContainerItem& container) {
            // Deleted containers are not listed by ListContainers.
            DCHECK(!container.IsDeleted);

            // Every container is considered a directory.
            FileInfo info{container.Name, FileType::Directory};
            info.set_mtime(
                std::chrono::system_clock::time_point{container.Details.LastModified});
            acc_results->push_back(std::move(info));

            // Recurse into containers (subdirectories) if requested.
            if (select.recursive && select.max_recursion > 0) {
              FileSelector sub_select;
              sub_select.base_dir = container.Name;
              sub_select.allow_not_found = true;
              sub_select.recursive = true;
              sub_select.max_recursion = select.max_recursion - 1;
              ARROW_RETURN_NOT_OK(GetFileInfoWithSelector(context, page_size_hint,
                                                          sub_select, acc_results));
            }
            return Status::OK();
          };
      return VisitContainers(context, std::move(on_container));
    }

    auto container_client =
        blob_service_client_->GetBlobContainerClient(base_location.container);
    return GetFileInfoWithSelectorFromContainer(container_client, context, page_size_hint,
                                                select, acc_results);
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const AzureLocation& location,
                                                         AzureFileSystem* fs) {
    RETURN_NOT_OK(ValidateFileLocation(location));
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

  Result<std::shared_ptr<ObjectAppendStream>> OpenAppendStream(
      const AzureLocation& location,
      const std::shared_ptr<const KeyValueMetadata>& metadata, const bool truncate,
      AzureFileSystem* fs) {
    RETURN_NOT_OK(ValidateFileLocation(location));

    auto block_blob_client = std::make_shared<Azure::Storage::Blobs::BlockBlobClient>(
        blob_service_client_->GetBlobContainerClient(location.container)
            .GetBlockBlobClient(location.path));

    std::shared_ptr<ObjectAppendStream> stream;
    if (truncate) {
      RETURN_NOT_OK(CreateEmptyBlockBlob(block_blob_client));
      stream = std::make_shared<ObjectAppendStream>(block_blob_client, fs->io_context(),
                                                    location, metadata, options_, 0);
    } else {
      stream = std::make_shared<ObjectAppendStream>(block_blob_client, fs->io_context(),
                                                    location, metadata, options_);
    }
    RETURN_NOT_OK(stream->Init());
    return stream;
  }

 private:
  Status DeleteDirContentsWithoutHierarchicalNamespace(const AzureLocation& location,
                                                       bool missing_dir_ok) {
    auto container_client =
        blob_service_client_->GetBlobContainerClient(location.container);
    Azure::Storage::Blobs::ListBlobsOptions options;
    if (!location.path.empty()) {
      options.Prefix = internal::EnsureTrailingSlash(location.path);
    }
    // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch#remarks
    //
    // Only supports up to 256 subrequests in a single batch. The
    // size of the body for a batch request can't exceed 4 MB.
    const int32_t kNumMaxRequestsInBatch = 256;
    options.PageSizeHint = kNumMaxRequestsInBatch;
    try {
      auto list_response = container_client.ListBlobs(options);
      if (!missing_dir_ok && list_response.Blobs.empty()) {
        return PathNotFound(location);
      }
      for (; list_response.HasPage(); list_response.MoveToNextPage()) {
        if (list_response.Blobs.empty()) {
          continue;
        }
        auto batch = container_client.CreateBatch();
        std::vector<Azure::Storage::DeferredResponse<
            Azure::Storage::Blobs::Models::DeleteBlobResult>>
            deferred_responses;
        for (const auto& blob_item : list_response.Blobs) {
          deferred_responses.push_back(batch.DeleteBlob(blob_item.Name));
        }
        try {
          container_client.SubmitBatch(batch);
        } catch (const Azure::Storage::StorageException& exception) {
          return internal::ExceptionToStatus(
              "Failed to delete blobs in a directory: " + location.path + ": " +
                  container_client.GetUrl(),
              exception);
        }
        std::vector<std::string> failed_blob_names;
        for (size_t i = 0; i < deferred_responses.size(); ++i) {
          const auto& deferred_response = deferred_responses[i];
          bool success = true;
          try {
            auto delete_result = deferred_response.GetResponse();
            success = delete_result.Value.Deleted;
          } catch (const Azure::Storage::StorageException& exception) {
            success = false;
          }
          if (!success) {
            const auto& blob_item = list_response.Blobs[i];
            failed_blob_names.push_back(blob_item.Name);
          }
        }
        if (!failed_blob_names.empty()) {
          if (failed_blob_names.size() == 1) {
            return Status::IOError("Failed to delete a blob: ", failed_blob_names[0],
                                   ": " + container_client.GetUrl());
          } else {
            return Status::IOError("Failed to delete blobs: [",
                                   arrow::internal::JoinStrings(failed_blob_names, ", "),
                                   "]: " + container_client.GetUrl());
          }
        }
      }
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus(
          "Failed to list blobs in a directory: " + location.path + ": " +
              container_client.GetUrl(),
          exception);
    }
    return Status::OK();
  }

 public:
  Status DeleteDir(const AzureLocation& location) {
    if (location.container.empty()) {
      return Status::Invalid("Cannot delete an empty container");
    }

    if (location.path.empty()) {
      auto container_client =
          blob_service_client_->GetBlobContainerClient(location.container);
      try {
        auto response = container_client.Delete();
        if (response.Value.Deleted) {
          return Status::OK();
        } else {
          return StatusFromErrorResponse(
              container_client.GetUrl(), response.RawResponse.get(),
              "Failed to delete a container: " + location.container);
        }
      } catch (const Azure::Storage::StorageException& exception) {
        return internal::ExceptionToStatus(
            "Failed to delete a container: " + location.container + ": " +
                container_client.GetUrl(),
            exception);
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto hierarchical_namespace_enabled,
                          hierarchical_namespace_.Enabled(location.container));
    if (hierarchical_namespace_enabled) {
      auto directory_client =
          datalake_service_client_->GetFileSystemClient(location.container)
              .GetDirectoryClient(location.path);
      try {
        auto response = directory_client.DeleteRecursive();
        if (response.Value.Deleted) {
          return Status::OK();
        } else {
          return StatusFromErrorResponse(
              directory_client.GetUrl(), response.RawResponse.get(),
              "Failed to delete a directory: " + location.path);
        }
      } catch (const Azure::Storage::StorageException& exception) {
        return internal::ExceptionToStatus(
            "Failed to delete a directory: " + location.path + ": " +
                directory_client.GetUrl(),
            exception);
      }
    } else {
      return DeleteDirContentsWithoutHierarchicalNamespace(location,
                                                           /*missing_dir_ok=*/true);
    }
  }

  Status DeleteDirContents(const AzureLocation& location, bool missing_dir_ok) {
    if (location.container.empty()) {
      return internal::InvalidDeleteDirContents(location.all);
    }

    ARROW_ASSIGN_OR_RAISE(auto hierarchical_namespace_enabled,
                          hierarchical_namespace_.Enabled(location.container));
    if (hierarchical_namespace_enabled) {
      auto file_system_client =
          datalake_service_client_->GetFileSystemClient(location.container);
      auto directory_client = file_system_client.GetDirectoryClient(location.path);
      try {
        auto list_response = directory_client.ListPaths(false);
        for (; list_response.HasPage(); list_response.MoveToNextPage()) {
          for (const auto& path : list_response.Paths) {
            if (path.IsDirectory) {
              auto sub_directory_client =
                  file_system_client.GetDirectoryClient(path.Name);
              try {
                sub_directory_client.DeleteRecursive();
              } catch (const Azure::Storage::StorageException& exception) {
                return internal::ExceptionToStatus(
                    "Failed to delete a sub directory: " + location.container +
                        internal::kSep + path.Name + ": " + sub_directory_client.GetUrl(),
                    exception);
              }
            } else {
              auto sub_file_client = file_system_client.GetFileClient(path.Name);
              try {
                sub_file_client.Delete();
              } catch (const Azure::Storage::StorageException& exception) {
                return internal::ExceptionToStatus(
                    "Failed to delete a sub file: " + location.container +
                        internal::kSep + path.Name + ": " + sub_file_client.GetUrl(),
                    exception);
              }
            }
          }
        }
      } catch (const Azure::Storage::StorageException& exception) {
        if (missing_dir_ok &&
            exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
          return Status::OK();
        } else {
          return internal::ExceptionToStatus(
              "Failed to delete directory contents: " + location.path + ": " +
                  directory_client.GetUrl(),
              exception);
        }
      }
      return Status::OK();
    } else {
      return DeleteDirContentsWithoutHierarchicalNamespace(location, missing_dir_ok);
    }
  }

  Status CopyFile(const AzureLocation& src, const AzureLocation& dest) {
    RETURN_NOT_OK(ValidateFileLocation(src));
    RETURN_NOT_OK(ValidateFileLocation(dest));
    if (src == dest) {
      return Status::OK();
    }
    auto dest_blob_client = blob_service_client_->GetBlobContainerClient(dest.container)
                                .GetBlobClient(dest.path);
    auto src_url = blob_service_client_->GetBlobContainerClient(src.container)
                       .GetBlobClient(src.path)
                       .GetUrl();
    try {
      dest_blob_client.CopyFromUri(src_url);
    } catch (const Azure::Storage::StorageException& exception) {
      return internal::ExceptionToStatus(
          "Failed to copy a blob. (" + src_url + " -> " + dest_blob_client.GetUrl() + ")",
          exception);
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
  Azure::Core::Context context;
  Azure::Nullable<int32_t> page_size_hint;  // unspecified
  FileInfoVector results;
  RETURN_NOT_OK(
      impl_->GetFileInfoWithSelector(context, page_size_hint, select, &results));
  return {std::move(results)};
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
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->DeleteDir(location);
}

Status AzureFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->DeleteDirContents(location, missing_dir_ok);
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
  ARROW_ASSIGN_OR_RAISE(auto src_location, AzureLocation::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dest_location, AzureLocation::FromString(dest));
  return impl_->CopyFile(src_location, dest_location);
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
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->OpenAppendStream(location, metadata, true, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  return impl_->OpenAppendStream(location, metadata, false, this);
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

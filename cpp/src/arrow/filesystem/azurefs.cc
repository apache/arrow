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

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>

#include "arrow/filesystem/azurefs.h"
#include "arrow/filesystem/azurefs_internal.h"
#include "arrow/io/memory.h"

// idenfity.hpp triggers -Wattributes warnings cause -Werror builds to fail,
// so disable it for this file with pragmas.
#if defined(__GNUC__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <azure/identity.hpp>
#if defined(__GNUC__)
#  pragma GCC diagnostic pop
#endif
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

namespace arrow::fs {

namespace Blobs = Azure::Storage::Blobs;
namespace Core = Azure::Core;
namespace DataLake = Azure::Storage::Files::DataLake;
namespace Http = Azure::Core::Http;
namespace Storage = Azure::Storage;

using HNSSupport = internal::HierarchicalNamespaceSupport;

// -----------------------------------------------------------------------
// AzureOptions Implementation

AzureOptions::AzureOptions() = default;

AzureOptions::~AzureOptions() = default;

void AzureOptions::ExtractFromUriSchemeAndHierPart(const Uri& uri,
                                                   std::string* out_path) {
  const auto host = uri.host();
  std::string path;
  if (arrow::internal::EndsWith(host, blob_storage_authority)) {
    account_name = host.substr(0, host.size() - blob_storage_authority.size());
    path = internal::RemoveLeadingSlash(uri.path());
  } else if (arrow::internal::EndsWith(host, dfs_storage_authority)) {
    account_name = host.substr(0, host.size() - dfs_storage_authority.size());
    path = internal::ConcatAbstractPath(uri.username(), uri.path());
  } else {
    account_name = uri.username();
    const auto port_text = uri.port_text();
    if (host.find(".") == std::string::npos && port_text.empty()) {
      // abfs://container/dir/file
      path = internal::ConcatAbstractPath(host, uri.path());
    } else {
      // abfs://host.domain/container/dir/file
      // abfs://host.domain:port/container/dir/file
      // abfs://host:port/container/dir/file
      std::string host_port = host;
      if (!port_text.empty()) {
        host_port += ":" + port_text;
      }
      blob_storage_authority = host_port;
      dfs_storage_authority = host_port;
      path = internal::RemoveLeadingSlash(uri.path());
    }
  }
  if (out_path != nullptr) {
    *out_path = path;
  }
}

Status AzureOptions::ExtractFromUriQuery(const Uri& uri) {
  std::optional<CredentialKind> credential_kind;
  std::optional<std::string> credential_kind_value;
  std::string tenant_id;
  std::string client_id;
  std::string client_secret;

  // These query parameters are the union of the following docs:
  // https://learn.microsoft.com/en-us/rest/api/storageservices/create-account-sas#specify-the-account-sas-parameters
  // https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#construct-a-service-sas
  // (excluding parameters for table storage only)
  // https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#construct-a-user-delegation-sas
  static const std::set<std::string> sas_token_query_parameters = {
      "sv",    "ss",    "sr",  "st",  "se",   "sp",   "si",   "sip",   "spr",
      "skoid", "sktid", "srt", "skt", "ske",  "skv",  "sks",  "saoid", "suoid",
      "scid",  "sdd",   "ses", "sig", "rscc", "rscd", "rsce", "rscl",  "rsct",
  };

  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    if (kv.first == "blob_storage_authority") {
      blob_storage_authority = kv.second;
    } else if (kv.first == "dfs_storage_authority") {
      dfs_storage_authority = kv.second;
    } else if (kv.first == "credential_kind") {
      if (kv.second == "default") {
        credential_kind = CredentialKind::kDefault;
      } else if (kv.second == "anonymous") {
        credential_kind = CredentialKind::kAnonymous;
      } else if (kv.second == "cli") {
        credential_kind = CredentialKind::kCLI;
      } else if (kv.second == "workload_identity") {
        credential_kind = CredentialKind::kWorkloadIdentity;
      } else if (kv.second == "environment") {
        credential_kind = CredentialKind::kEnvironment;
      } else {
        // Other credential kinds should be inferred from the given
        // parameters automatically.
        return Status::Invalid("Unexpected credential_kind: '", kv.second, "'");
      }
      credential_kind_value = kv.second;
    } else if (kv.first == "tenant_id") {
      tenant_id = kv.second;
    } else if (kv.first == "client_id") {
      client_id = kv.second;
    } else if (kv.first == "client_secret") {
      client_secret = kv.second;
    } else if (kv.first == "enable_tls") {
      ARROW_ASSIGN_OR_RAISE(auto enable_tls, ::arrow::internal::ParseBoolean(kv.second));
      if (enable_tls) {
        blob_storage_scheme = "https";
        dfs_storage_scheme = "https";
      } else {
        blob_storage_scheme = "http";
        dfs_storage_scheme = "http";
      }
    } else if (kv.first == "background_writes") {
      ARROW_ASSIGN_OR_RAISE(background_writes,
                            ::arrow::internal::ParseBoolean(kv.second));
    } else if (sas_token_query_parameters.find(kv.first) !=
               sas_token_query_parameters.end()) {
      credential_kind = CredentialKind::kSASToken;
    } else {
      return Status::Invalid(
          "Unexpected query parameter in Azure Blob File System URI: '", kv.first, "'");
    }
  }

  if (credential_kind) {
    if (!tenant_id.empty()) {
      return Status::Invalid("tenant_id must not be specified with credential_kind=",
                             *credential_kind_value);
    }
    if (!client_id.empty()) {
      return Status::Invalid("client_id must not be specified with credential_kind=",
                             *credential_kind_value);
    }
    if (!client_secret.empty()) {
      return Status::Invalid("client_secret must not be specified with credential_kind=",
                             *credential_kind_value);
    }

    switch (*credential_kind) {
      case CredentialKind::kAnonymous:
        RETURN_NOT_OK(ConfigureAnonymousCredential());
        break;
      case CredentialKind::kCLI:
        RETURN_NOT_OK(ConfigureCLICredential());
        break;
      case CredentialKind::kWorkloadIdentity:
        RETURN_NOT_OK(ConfigureWorkloadIdentityCredential());
        break;
      case CredentialKind::kEnvironment:
        RETURN_NOT_OK(ConfigureEnvironmentCredential());
        break;
      case CredentialKind::kSASToken:
        // Reconstructing the SAS token without the other URI query parameters is awkward
        // because some parts are URI escaped and some parts are not. Instead we just
        // pass through the entire query string and Azure ignores the extra query
        // parameters.
        RETURN_NOT_OK(ConfigureSASCredential("?" + uri.query_string()));
        break;
      default:
        // Default credential
        break;
    }
  } else {
    if (tenant_id.empty() && client_id.empty() && client_secret.empty()) {
      // No related parameters
      if (account_name.empty()) {
        RETURN_NOT_OK(ConfigureAnonymousCredential());
      } else {
        // Default credential
      }
    } else {
      // One or more tenant_id, client_id or client_secret are specified
      if (client_id.empty()) {
        return Status::Invalid("client_id must be specified");
      }
      if (tenant_id.empty() && client_secret.empty()) {
        RETURN_NOT_OK(ConfigureManagedIdentityCredential(client_id));
      } else if (!tenant_id.empty() && !client_secret.empty()) {
        RETURN_NOT_OK(
            ConfigureClientSecretCredential(tenant_id, client_id, client_secret));
      } else {
        return Status::Invalid("Both of tenant_id and client_secret must be specified");
      }
    }
  }
  return Status::OK();
}

Result<AzureOptions> AzureOptions::FromUri(const Uri& uri, std::string* out_path) {
  AzureOptions options;
  options.ExtractFromUriSchemeAndHierPart(uri, out_path);
  RETURN_NOT_OK(options.ExtractFromUriQuery(uri));
  return options;
}

Result<AzureOptions> AzureOptions::FromUri(const std::string& uri_string,
                                           std::string* out_path) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
}

bool AzureOptions::Equals(const AzureOptions& other) const {
  const bool equals = blob_storage_authority == other.blob_storage_authority &&
                      dfs_storage_authority == other.dfs_storage_authority &&
                      blob_storage_scheme == other.blob_storage_scheme &&
                      dfs_storage_scheme == other.dfs_storage_scheme &&
                      default_metadata == other.default_metadata &&
                      account_name == other.account_name &&
                      credential_kind_ == other.credential_kind_;
  if (!equals) {
    return false;
  }
  switch (credential_kind_) {
    case CredentialKind::kDefault:
    case CredentialKind::kAnonymous:
      return true;
    case CredentialKind::kStorageSharedKey:
      return storage_shared_key_credential_->AccountName ==
             other.storage_shared_key_credential_->AccountName;
    case CredentialKind::kSASToken:
      return sas_token_ == other.sas_token_;
    case CredentialKind::kClientSecret:
    case CredentialKind::kCLI:
    case CredentialKind::kManagedIdentity:
    case CredentialKind::kWorkloadIdentity:
    case CredentialKind::kEnvironment:
      return token_credential_->GetCredentialName() ==
             other.token_credential_->GetCredentialName();
  }
  DCHECK(false);
  return false;
}

namespace {
std::string BuildBaseUrl(const std::string& scheme, const std::string& authority,
                         const std::string& account_name) {
  std::string url;
  url += scheme + "://";
  if (!authority.empty()) {
    if (authority[0] == '.') {
      url += account_name;
      url += authority;
    } else {
      url += authority;
      url += "/";
      url += account_name;
    }
  }
  url += "/";
  return url;
}

template <typename... PrefixArgs>
Status ExceptionToStatus(const Azure::Core::RequestFailedException& exception,
                         PrefixArgs&&... prefix_args) {
  return Status::IOError(std::forward<PrefixArgs>(prefix_args)..., " Azure Error: [",
                         exception.ErrorCode, "] ", exception.what());
}
}  // namespace

std::string AzureOptions::AccountBlobUrl(const std::string& account_name) const {
  return BuildBaseUrl(blob_storage_scheme, blob_storage_authority, account_name);
}

std::string AzureOptions::AccountDfsUrl(const std::string& account_name) const {
  return BuildBaseUrl(dfs_storage_scheme, dfs_storage_authority, account_name);
}

Status AzureOptions::ConfigureDefaultCredential() {
  credential_kind_ = CredentialKind::kDefault;
  token_credential_ = std::make_shared<Azure::Identity::DefaultAzureCredential>();
  return Status::OK();
}

Status AzureOptions::ConfigureAnonymousCredential() {
  credential_kind_ = CredentialKind::kAnonymous;
  return Status::OK();
}

Status AzureOptions::ConfigureAccountKeyCredential(const std::string& account_key) {
  credential_kind_ = CredentialKind::kStorageSharedKey;
  if (account_name.empty()) {
    return Status::Invalid("AzureOptions doesn't contain a valid account name");
  }
  storage_shared_key_credential_ =
      std::make_shared<Storage::StorageSharedKeyCredential>(account_name, account_key);
  return Status::OK();
}

Status AzureOptions::ConfigureSASCredential(const std::string& sas_token) {
  credential_kind_ = CredentialKind::kSASToken;
  if (account_name.empty()) {
    return Status::Invalid("AzureOptions doesn't contain a valid account name");
  }
  sas_token_ = sas_token;
  return Status::OK();
}

Status AzureOptions::ConfigureClientSecretCredential(const std::string& tenant_id,
                                                     const std::string& client_id,
                                                     const std::string& client_secret) {
  credential_kind_ = CredentialKind::kClientSecret;
  token_credential_ = std::make_shared<Azure::Identity::ClientSecretCredential>(
      tenant_id, client_id, client_secret);
  return Status::OK();
}

Status AzureOptions::ConfigureManagedIdentityCredential(const std::string& client_id) {
  credential_kind_ = CredentialKind::kManagedIdentity;
  token_credential_ =
      std::make_shared<Azure::Identity::ManagedIdentityCredential>(client_id);
  return Status::OK();
}

Status AzureOptions::ConfigureCLICredential() {
  credential_kind_ = CredentialKind::kCLI;
  token_credential_ = std::make_shared<Azure::Identity::AzureCliCredential>();
  return Status::OK();
}

Status AzureOptions::ConfigureWorkloadIdentityCredential() {
  credential_kind_ = CredentialKind::kWorkloadIdentity;
  token_credential_ = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
  return Status::OK();
}

Status AzureOptions::ConfigureEnvironmentCredential() {
  credential_kind_ = CredentialKind::kEnvironment;
  token_credential_ = std::make_shared<Azure::Identity::EnvironmentCredential>();
  return Status::OK();
}

Result<std::unique_ptr<Blobs::BlobServiceClient>> AzureOptions::MakeBlobServiceClient()
    const {
  if (account_name.empty()) {
    return Status::Invalid("AzureOptions doesn't contain a valid account name");
  }
  if (!(blob_storage_scheme == "http" || blob_storage_scheme == "https")) {
    return Status::Invalid("AzureOptions::blob_storage_scheme must be http or https: ",
                           blob_storage_scheme);
  }
  switch (credential_kind_) {
    case CredentialKind::kAnonymous:
      return std::make_unique<Blobs::BlobServiceClient>(AccountBlobUrl(account_name));
    case CredentialKind::kDefault:
      if (!token_credential_) {
        token_credential_ = std::make_shared<Azure::Identity::DefaultAzureCredential>();
      }
      [[fallthrough]];
    case CredentialKind::kClientSecret:
    case CredentialKind::kManagedIdentity:
    case CredentialKind::kCLI:
    case CredentialKind::kWorkloadIdentity:
    case CredentialKind::kEnvironment:
      return std::make_unique<Blobs::BlobServiceClient>(AccountBlobUrl(account_name),
                                                        token_credential_);
    case CredentialKind::kStorageSharedKey:
      return std::make_unique<Blobs::BlobServiceClient>(AccountBlobUrl(account_name),
                                                        storage_shared_key_credential_);
    case CredentialKind::kSASToken:
      return std::make_unique<Blobs::BlobServiceClient>(AccountBlobUrl(account_name) +
                                                        sas_token_);
  }
  return Status::Invalid("AzureOptions doesn't contain a valid auth configuration");
}

Result<std::unique_ptr<DataLake::DataLakeServiceClient>>
AzureOptions::MakeDataLakeServiceClient() const {
  if (account_name.empty()) {
    return Status::Invalid("AzureOptions doesn't contain a valid account name");
  }
  if (!(dfs_storage_scheme == "http" || dfs_storage_scheme == "https")) {
    return Status::Invalid("AzureOptions::dfs_storage_scheme must be http or https: ",
                           dfs_storage_scheme);
  }
  switch (credential_kind_) {
    case CredentialKind::kAnonymous:
      return std::make_unique<DataLake::DataLakeServiceClient>(
          AccountDfsUrl(account_name));
    case CredentialKind::kDefault:
      if (!token_credential_) {
        token_credential_ = std::make_shared<Azure::Identity::DefaultAzureCredential>();
      }
      [[fallthrough]];
    case CredentialKind::kClientSecret:
    case CredentialKind::kManagedIdentity:
    case CredentialKind::kCLI:
    case CredentialKind::kWorkloadIdentity:
    case CredentialKind::kEnvironment:
      return std::make_unique<DataLake::DataLakeServiceClient>(
          AccountDfsUrl(account_name), token_credential_);
    case CredentialKind::kStorageSharedKey:
      return std::make_unique<DataLake::DataLakeServiceClient>(
          AccountDfsUrl(account_name), storage_shared_key_credential_);
    case CredentialKind::kSASToken:
      return std::make_unique<DataLake::DataLakeServiceClient>(
          AccountBlobUrl(account_name) + sas_token_);
  }
  return Status::Invalid("AzureOptions doesn't contain a valid auth configuration");
}

namespace {

// An AzureFileSystem represents an Azure storage account. An AzureLocation describes a
// container in that storage account and a path within that container.
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
          "Expected an Azure object location of the form 'container/path...',"
          " got a URI: '",
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
    return status.ok() ? status : Status::Invalid(status.message(), " in location ", all);
  }
};

Status PathNotFound(const AzureLocation& location) {
  return ::arrow::fs::internal::PathNotFound(location.all);
}

Status NotADir(const AzureLocation& location) {
  return ::arrow::fs::internal::NotADir(location.all);
}

Status NotAFile(const AzureLocation& location) {
  return ::arrow::fs::internal::NotAFile(location.all);
}

Status NotEmpty(const AzureLocation& location) {
  return ::arrow::fs::internal::NotEmpty(location.all);
}

Status ValidateFileLocation(const AzureLocation& location) {
  if (location.container.empty()) {
    return PathNotFound(location);
  }
  if (location.path.empty()) {
    return NotAFile(location);
  }
  return internal::AssertNoTrailingSlash(location.path);
}

Status InvalidDirMoveToSubdir(const AzureLocation& src, const AzureLocation& dest) {
  return Status::Invalid("Cannot Move to '", dest.all, "' and make '", src.all,
                         "' a sub-directory of itself.");
}

Status DestinationParentPathNotFound(const AzureLocation& dest) {
  return Status::IOError("The parent directory of the destination path '", dest.all,
                         "' does not exist.");
}

Status CrossContainerMoveNotImplemented(const AzureLocation& src,
                                        const AzureLocation& dest) {
  return Status::NotImplemented(
      "Move of '", src.all, "' to '", dest.all,
      "' requires moving data between containers, which is not implemented.");
}

bool IsContainerNotFound(const Storage::StorageException& e) {
  // In some situations, only the ReasonPhrase is set and the
  // ErrorCode is empty, so we check both.
  if (e.ErrorCode == "ContainerNotFound" ||
      e.ReasonPhrase == "The specified container does not exist." ||
      e.ReasonPhrase == "The specified filesystem does not exist.") {
    DCHECK_EQ(e.StatusCode, Http::HttpStatusCode::NotFound);
    return true;
  }
  return false;
}

const auto kHierarchicalNamespaceIsDirectoryMetadataKey = "hdi_isFolder";
const auto kFlatNamespaceIsDirectoryMetadataKey = "is_directory";

bool MetadataIndicatesIsDirectory(const Storage::Metadata& metadata) {
  // Inspired by
  // https://github.com/Azure/azure-sdk-for-cpp/blob/12407e8bfcb9bc1aa43b253c1d0ec93bf795ae3b/sdk/storage/azure-storage-files-datalake/src/datalake_utilities.cpp#L86-L91
  auto hierarchical_directory_metadata =
      metadata.find(kHierarchicalNamespaceIsDirectoryMetadataKey);
  if (hierarchical_directory_metadata != metadata.end()) {
    return hierarchical_directory_metadata->second == "true";
  }
  auto flat_directory_metadata = metadata.find(kFlatNamespaceIsDirectoryMetadataKey);
  return flat_directory_metadata != metadata.end() &&
         flat_directory_metadata->second == "true";
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
    const Blobs::Models::BlobProperties& properties) {
  auto metadata = std::make_shared<KeyValueMetadata>();
  // Not supported yet:
  // * properties.ObjectReplicationSourceProperties
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
  for (const auto& [key, value] : properties.Metadata) {
    metadata->Append(key, value);
  }
  return metadata;
}

void ArrowMetadataToCommitBlockListOptions(
    const std::shared_ptr<const KeyValueMetadata>& arrow_metadata,
    Blobs::CommitBlockListOptions& options) {
  using ::arrow::internal::AsciiEqualsCaseInsensitive;
  for (auto& [key, value] : arrow_metadata->sorted_pairs()) {
    if (AsciiEqualsCaseInsensitive(key, "Content-Type")) {
      options.HttpHeaders.ContentType = value;
    } else if (AsciiEqualsCaseInsensitive(key, "Content-Encoding")) {
      options.HttpHeaders.ContentEncoding = value;
    } else if (AsciiEqualsCaseInsensitive(key, "Content-Language")) {
      options.HttpHeaders.ContentLanguage = value;
    } else if (AsciiEqualsCaseInsensitive(key, "Content-Hash")) {
      // Ignore: auto-generated value
    } else if (AsciiEqualsCaseInsensitive(key, "Content-Disposition")) {
      options.HttpHeaders.ContentDisposition = value;
    } else if (AsciiEqualsCaseInsensitive(key, "Cache-Control")) {
      options.HttpHeaders.CacheControl = value;
    } else {
      options.Metadata[key] = value;
    }
  }
}

class ObjectInputFile final : public io::RandomAccessFile {
 public:
  ObjectInputFile(std::shared_ptr<Blobs::BlobClient> blob_client,
                  const io::IOContext& io_context, AzureLocation location,
                  int64_t size = kNoSize)
      : blob_client_(std::move(blob_client)),
        io_context_(io_context),
        location_(std::move(location)),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      // When the user provides the file size we don't validate that its a file. This is
      // only a read so its not a big deal if the user makes a mistake.
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      // To open an ObjectInputFile the Blob must exist and it must not represent
      // a directory. Additionally we need to know the file size.
      auto properties = blob_client_->GetProperties();
      if (MetadataIndicatesIsDirectory(properties.Value.Metadata)) {
        return NotAFile(location_);
      }
      content_length_ = properties.Value.BlobSize;
      metadata_ = PropertiesToMetadata(properties.Value);
      return Status::OK();
    } catch (const Storage::StorageException& exception) {
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        return PathNotFound(location_);
      }
      return ExceptionToStatus(
          exception, "GetProperties failed for '", blob_client_->GetUrl(),
          "'. Cannot initialise an ObjectInputFile without knowing the file size.");
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
    Http::HttpRange range{position, nbytes};
    Storage::Blobs::DownloadBlobToOptions download_options;
    download_options.Range = range;
    try {
      return blob_client_
          ->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, download_options)
          .Value.ContentRange.Length.Value();
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(
          exception, "DownloadTo from '", blob_client_->GetUrl(), "' at position ",
          position, " for ", nbytes,
          " bytes failed. ReadAt failed to read the required byte range.");
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
    return buffer;
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return buffer;
  }

 private:
  std::shared_ptr<Blobs::BlobClient> blob_client_;
  const io::IOContext io_context_;
  AzureLocation location_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

Status CreateEmptyBlockBlob(const Blobs::BlockBlobClient& block_blob_client) {
  try {
    block_blob_client.UploadFrom(nullptr, 0);
  } catch (const Storage::StorageException& exception) {
    return ExceptionToStatus(
        exception, "UploadFrom failed for '", block_blob_client.GetUrl(),
        "'. There is no existing blob at this location or the existing blob must be "
        "replaced so ObjectAppendStream must create a new empty block blob.");
  }
  return Status::OK();
}

Result<Blobs::Models::GetBlockListResult> GetBlockList(
    std::shared_ptr<Blobs::BlockBlobClient> block_blob_client) {
  try {
    return block_blob_client->GetBlockList().Value;
  } catch (Storage::StorageException& exception) {
    return ExceptionToStatus(
        exception, "GetBlockList failed for '", block_blob_client->GetUrl(),
        "'. Cannot write to a file without first fetching the existing block list.");
  }
}

Status CommitBlockList(std::shared_ptr<Storage::Blobs::BlockBlobClient> block_blob_client,
                       const std::vector<std::string>& block_ids,
                       const Blobs::CommitBlockListOptions& options) {
  try {
    // CommitBlockList puts all block_ids in the latest element. That means in the case
    // of overlapping block_ids the newly staged block ids will always replace the
    // previously committed blocks.
    // https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list?tabs=microsoft-entra-id#request-body
    block_blob_client->CommitBlockList(block_ids, options);
  } catch (const Storage::StorageException& exception) {
    return ExceptionToStatus(
        exception, "CommitBlockList failed for '", block_blob_client->GetUrl(),
        "'. Committing is required to flush an output/append stream.");
  }
  return Status::OK();
}

Status StageBlock(Blobs::BlockBlobClient* block_blob_client, const std::string& id,
                  Core::IO::MemoryBodyStream& content) {
  try {
    block_blob_client->StageBlock(id, content);
  } catch (const Storage::StorageException& exception) {
    return ExceptionToStatus(
        exception, "StageBlock failed for '", block_blob_client->GetUrl(),
        "' new_block_id: '", id,
        "'. Staging new blocks is fundamental to streaming writes to blob storage.");
  }

  return Status::OK();
}

/// Writes will be buffered up to this size (in bytes) before actually uploading them.
static constexpr int64_t kBlockUploadSizeBytes = 10 * 1024 * 1024;
/// The maximum size of a block in Azure Blob (as per docs).
static constexpr int64_t kMaxBlockSizeBytes = 4UL * 1024 * 1024 * 1024;

/// This output stream, similar to other arrow OutputStreams, is not thread-safe.
class ObjectAppendStream final : public io::OutputStream {
 private:
  struct UploadState;

  std::shared_ptr<ObjectAppendStream> Self() {
    return std::dynamic_pointer_cast<ObjectAppendStream>(shared_from_this());
  }

 public:
  ObjectAppendStream(std::shared_ptr<Blobs::BlockBlobClient> block_blob_client,
                     const io::IOContext& io_context, const AzureLocation& location,
                     const std::shared_ptr<const KeyValueMetadata>& metadata,
                     const AzureOptions& options)
      : block_blob_client_(std::move(block_blob_client)),
        io_context_(io_context),
        location_(location),
        background_writes_(options.background_writes) {
    if (metadata && metadata->size() != 0) {
      ArrowMetadataToCommitBlockListOptions(metadata, commit_block_list_options_);
    } else if (options.default_metadata && options.default_metadata->size() != 0) {
      ArrowMetadataToCommitBlockListOptions(options.default_metadata,
                                            commit_block_list_options_);
    }
  }

  ~ObjectAppendStream() override {
    // For compliance with the rest of the IO stack, Close rather than Abort,
    // even though it may be more expensive.
    io::internal::CloseFromDestructor(this);
  }

  Status Init(const bool truncate,
              std::function<Status()> ensure_not_flat_namespace_directory) {
    if (truncate) {
      content_length_ = 0;
      pos_ = 0;
      // We need to create an empty file overwriting any existing file, but
      // fail if there is an existing directory.
      RETURN_NOT_OK(ensure_not_flat_namespace_directory());
      // On hierarchical namespace CreateEmptyBlockBlob will fail if there is an existing
      // directory so we don't need to check like we do on flat namespace.
      RETURN_NOT_OK(CreateEmptyBlockBlob(*block_blob_client_));
    } else {
      try {
        auto properties = block_blob_client_->GetProperties();
        if (MetadataIndicatesIsDirectory(properties.Value.Metadata)) {
          return NotAFile(location_);
        }
        content_length_ = properties.Value.BlobSize;
        pos_ = content_length_;
      } catch (const Storage::StorageException& exception) {
        if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
          // No file exists but on flat namespace its possible there is a directory
          // marker or an implied directory. Ensure there is no directory before starting
          // a new empty file.
          RETURN_NOT_OK(ensure_not_flat_namespace_directory());
          RETURN_NOT_OK(CreateEmptyBlockBlob(*block_blob_client_));
        } else {
          return ExceptionToStatus(
              exception, "GetProperties failed for '", block_blob_client_->GetUrl(),
              "'. Cannot initialise an ObjectAppendStream without knowing whether a "
              "file already exists at this path, and if it exists, its size.");
        }
        content_length_ = 0;
      }
    }

    upload_state_ = std::make_shared<UploadState>();

    if (content_length_ > 0) {
      ARROW_ASSIGN_OR_RAISE(auto block_list, GetBlockList(block_blob_client_));
      for (auto block : block_list.CommittedBlocks) {
        upload_state_->block_ids.push_back(block.Name);
      }
    }
    initialised_ = true;
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

    if (current_block_) {
      // Upload remaining buffer
      RETURN_NOT_OK(AppendCurrentBlock());
    }

    RETURN_NOT_OK(Flush());
    block_blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  Future<> CloseAsync() override {
    if (closed_) {
      return Status::OK();
    }

    if (current_block_) {
      // Upload remaining buffer
      RETURN_NOT_OK(AppendCurrentBlock());
    }

    return FlushAsync().Then([self = Self()]() {
      self->block_blob_client_ = nullptr;
      self->closed_ = true;
    });
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
    return DoWrite(buffer->data(), buffer->size(), buffer);
  }

  Status Write(const void* data, int64_t nbytes) override {
    return DoWrite(data, nbytes);
  }

  Status Flush() override {
    RETURN_NOT_OK(CheckClosed("flush"));
    if (!initialised_) {
      // If the stream has not been successfully initialized then there is nothing to
      // flush. This also avoids some unhandled errors when flushing in the destructor.
      return Status::OK();
    }

    Future<> pending_blocks_completed;
    {
      std::unique_lock<std::mutex> lock(upload_state_->mutex);
      pending_blocks_completed = upload_state_->pending_blocks_completed;
    }

    RETURN_NOT_OK(pending_blocks_completed.status());
    std::unique_lock<std::mutex> lock(upload_state_->mutex);
    return CommitBlockList(block_blob_client_, upload_state_->block_ids,
                           commit_block_list_options_);
  }

  Future<> FlushAsync() {
    RETURN_NOT_OK(CheckClosed("flush async"));
    if (!initialised_) {
      // If the stream has not been successfully initialized then there is nothing to
      // flush. This also avoids some unhandled errors when flushing in the destructor.
      return Status::OK();
    }

    Future<> pending_blocks_completed;
    {
      std::unique_lock<std::mutex> lock(upload_state_->mutex);
      pending_blocks_completed = upload_state_->pending_blocks_completed;
    }

    return pending_blocks_completed.Then([self = Self()] {
      std::unique_lock<std::mutex> lock(self->upload_state_->mutex);
      return CommitBlockList(self->block_blob_client_, self->upload_state_->block_ids,
                             self->commit_block_list_options_);
    });
  }

 private:
  Status AppendCurrentBlock() {
    ARROW_ASSIGN_OR_RAISE(auto buf, current_block_->Finish());
    current_block_.reset();
    current_block_size_ = 0;
    return AppendBlock(buf);
  }

  Status DoWrite(const void* data, int64_t nbytes,
                 std::shared_ptr<Buffer> owned_buffer = nullptr) {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }

    const auto* data_ptr = reinterpret_cast<const int8_t*>(data);
    auto advance_ptr = [this, &data_ptr, &nbytes](const int64_t offset) {
      data_ptr += offset;
      nbytes -= offset;
      pos_ += offset;
      content_length_ += offset;
    };

    // Handle case where we have some bytes buffered from prior calls.
    if (current_block_size_ > 0) {
      // Try to fill current buffer
      const int64_t to_copy =
          std::min(nbytes, kBlockUploadSizeBytes - current_block_size_);
      RETURN_NOT_OK(current_block_->Write(data_ptr, to_copy));
      current_block_size_ += to_copy;
      advance_ptr(to_copy);

      // If buffer isn't full, break
      if (current_block_size_ < kBlockUploadSizeBytes) {
        return Status::OK();
      }

      // Upload current buffer
      RETURN_NOT_OK(AppendCurrentBlock());
    }

    // We can upload chunks without copying them into a buffer
    while (nbytes >= kBlockUploadSizeBytes) {
      const auto upload_size = std::min(nbytes, kMaxBlockSizeBytes);
      RETURN_NOT_OK(AppendBlock(data_ptr, upload_size));
      advance_ptr(upload_size);
    }

    // Buffer remaining bytes
    if (nbytes > 0) {
      current_block_size_ = nbytes;

      if (current_block_ == nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            current_block_,
            io::BufferOutputStream::Create(kBlockUploadSizeBytes, io_context_.pool()));
      } else {
        // Re-use the allocation from before.
        RETURN_NOT_OK(current_block_->Reset(kBlockUploadSizeBytes, io_context_.pool()));
      }

      RETURN_NOT_OK(current_block_->Write(data_ptr, current_block_size_));
      pos_ += current_block_size_;
      content_length_ += current_block_size_;
    }

    return Status::OK();
  }

  std::string CreateBlock() {
    std::unique_lock<std::mutex> lock(upload_state_->mutex);
    const auto n_block_ids = upload_state_->block_ids.size();

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
    // suffix significantly reduces the risk, but does not 100% eliminate it. For
    // example if the blob was previously created with one block, with id `00001-arrow`
    // then the next block we append will conflict with that, and cause corruption.
    new_block_id += "-arrow";
    new_block_id = Core::Convert::Base64Encode(
        std::vector<uint8_t>(new_block_id.begin(), new_block_id.end()));

    upload_state_->block_ids.push_back(new_block_id);

    // We only use the future if we have background writes enabled. Without background
    // writes the future is initialized as finished and not mutated any more.
    if (background_writes_ && upload_state_->blocks_in_progress++ == 0) {
      upload_state_->pending_blocks_completed = Future<>::Make();
    }

    return new_block_id;
  }

  Status AppendBlock(const void* data, int64_t nbytes,
                     std::shared_ptr<Buffer> owned_buffer = nullptr) {
    RETURN_NOT_OK(CheckClosed("append"));

    if (nbytes == 0) {
      return Status::OK();
    }

    const auto block_id = CreateBlock();

    if (background_writes_) {
      if (owned_buffer == nullptr) {
        ARROW_ASSIGN_OR_RAISE(owned_buffer, AllocateBuffer(nbytes, io_context_.pool()));
        memcpy(owned_buffer->mutable_data(), data, nbytes);
      } else {
        DCHECK_EQ(data, owned_buffer->data());
        DCHECK_EQ(nbytes, owned_buffer->size());
      }

      // The closure keeps the buffer and the upload state alive
      auto deferred = [owned_buffer, block_id, block_blob_client = block_blob_client_,
                       state = upload_state_]() mutable -> Status {
        Core::IO::MemoryBodyStream block_content(owned_buffer->data(),
                                                 owned_buffer->size());

        auto status = StageBlock(block_blob_client.get(), block_id, block_content);
        HandleUploadOutcome(state, status);
        return Status::OK();
      };
      RETURN_NOT_OK(io::internal::SubmitIO(io_context_, std::move(deferred)));
    } else {
      auto append_data = reinterpret_cast<const uint8_t*>(data);
      Core::IO::MemoryBodyStream block_content(append_data, nbytes);

      RETURN_NOT_OK(StageBlock(block_blob_client_.get(), block_id, block_content));
    }

    return Status::OK();
  }

  Status AppendBlock(std::shared_ptr<Buffer> buffer) {
    return AppendBlock(buffer->data(), buffer->size(), buffer);
  }

  static void HandleUploadOutcome(const std::shared_ptr<UploadState>& state,
                                  const Status& status) {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!status.ok()) {
      state->status &= status;
    }
    // Notify completion
    if (--state->blocks_in_progress == 0) {
      auto fut = state->pending_blocks_completed;
      lock.unlock();
      fut.MarkFinished(state->status);
    }
  }

  std::shared_ptr<Blobs::BlockBlobClient> block_blob_client_;
  const io::IOContext io_context_;
  const AzureLocation location_;
  const bool background_writes_;
  int64_t content_length_ = kNoSize;

  std::shared_ptr<io::BufferOutputStream> current_block_;
  int64_t current_block_size_ = 0;

  bool closed_ = false;
  bool initialised_ = false;
  int64_t pos_ = 0;

  // This struct is kept alive through background writes to avoid problems
  // in the completion handler.
  struct UploadState {
    std::mutex mutex;
    std::vector<std::string> block_ids;
    int64_t blocks_in_progress = 0;
    Status status;
    Future<> pending_blocks_completed = Future<>::MakeFinished(Status::OK());
  };
  std::shared_ptr<UploadState> upload_state_;

  Blobs::CommitBlockListOptions commit_block_list_options_;
};

bool IsDfsEmulator(const AzureOptions& options) {
  return options.dfs_storage_authority != ".dfs.core.windows.net";
}

}  // namespace

// -----------------------------------------------------------------------
// internal implementation

namespace internal {

Result<HNSSupport> CheckIfHierarchicalNamespaceIsEnabled(
    const DataLake::DataLakeFileSystemClient& adlfs_client, const AzureOptions& options) {
  try {
    auto directory_client = adlfs_client.GetDirectoryClient("");
    // GetAccessControlList will fail on storage accounts
    // without hierarchical namespace enabled.
    directory_client.GetAccessControlList();
    return HNSSupport::kEnabled;
  } catch (std::out_of_range& exception) {
    // Azurite issue detected.
    DCHECK(IsDfsEmulator(options));
    return HNSSupport::kDisabled;
  } catch (const Storage::StorageException& exception) {
    // Flat namespace storage accounts with "soft delete" enabled return
    //
    //   "Conflict - This endpoint does not support BlobStorageEvents
    //   or SoftDelete. [...]" [1],
    //
    // otherwise it returns:
    //
    //   "BadRequest - This operation is only supported on a hierarchical namespace
    //   account."
    //
    // [1]:
    // https://learn.microsoft.com/en-us/answers/questions/1069779/this-endpoint-does-not-support-blobstorageevents-o
    switch (exception.StatusCode) {
      case Http::HttpStatusCode::BadRequest:
      case Http::HttpStatusCode::Conflict:
        return HNSSupport::kDisabled;
      case Http::HttpStatusCode::NotFound:
        if (IsDfsEmulator(options)) {
          return HNSSupport::kDisabled;
        }
        // Did we get an error because of the container not existing?
        if (IsContainerNotFound(exception)) {
          return HNSSupport::kContainerNotFound;
        }
        [[fallthrough]];
      default:
        if (exception.ErrorCode == "HierarchicalNamespaceNotEnabled") {
          return HNSSupport::kDisabled;
        }
        return ExceptionToStatus(exception,
                                 "Check for Hierarchical Namespace support on '",
                                 adlfs_client.GetUrl(), "' failed.");
    }
  } catch (const Azure::Core::Http::TransportException& exception) {
    return ExceptionToStatus(exception, "Check for Hierarchical Namespace support on '",
                             adlfs_client.GetUrl(), "' failed.");
  } catch (const std::exception& exception) {
    return Status::UnknownError(
        "Check for Hierarchical Namespace support on '", adlfs_client.GetUrl(),
        "' failed: ", typeid(exception).name(), ": ", exception.what());
  }
}

}  // namespace internal

// -----------------------------------------------------------------------
// AzureFilesystem Implementation

namespace {

// In Azure Storage terminology, a "container" and a "filesystem" are the same
// kind of object, but it can be accessed using different APIs. The Blob Storage
// API calls it a "container", the Data Lake Storage Gen 2 API calls it a
// "filesystem". Creating a container using the Blob Storage API will make it
// accessible using the Data Lake Storage Gen 2 API and vice versa.

const char kDelimiter[] = {internal::kSep, '\0'};

/// \pre location.container is not empty.
template <class ContainerClient>
Result<FileInfo> GetContainerPropsAsFileInfo(const AzureLocation& location,
                                             const ContainerClient& container_client) {
  DCHECK(!location.container.empty());
  FileInfo info{location.path.empty() ? location.all : location.container};
  try {
    auto properties = container_client.GetProperties();
    info.set_type(FileType::Directory);
    info.set_mtime(std::chrono::system_clock::time_point{properties.Value.LastModified});
    return info;
  } catch (const Storage::StorageException& exception) {
    if (IsContainerNotFound(exception)) {
      info.set_type(FileType::NotFound);
      return info;
    }
    return ExceptionToStatus(exception, "GetProperties for '", container_client.GetUrl(),
                             "' failed.");
  }
}

template <class ContainerClient>
Status CreateContainerIfNotExists(const std::string& container_name,
                                  const ContainerClient& container_client) {
  try {
    container_client.CreateIfNotExists();
    return Status::OK();
  } catch (const Storage::StorageException& exception) {
    return ExceptionToStatus(exception, "Failed to create a container: ", container_name,
                             ": ", container_client.GetUrl());
  }
}

FileInfo FileInfoFromPath(std::string_view container,
                          const DataLake::Models::PathItem& path) {
  FileInfo info{internal::ConcatAbstractPath(container, path.Name),
                path.IsDirectory ? FileType::Directory : FileType::File};
  info.set_size(path.FileSize);
  info.set_mtime(std::chrono::system_clock::time_point{path.LastModified});
  return info;
}

FileInfo DirectoryFileInfoFromPath(std::string_view path) {
  return FileInfo{std::string{internal::RemoveTrailingSlash(path)}, FileType::Directory};
}

FileInfo FileInfoFromBlob(std::string_view container,
                          const Blobs::Models::BlobItem& blob) {
  auto path = internal::ConcatAbstractPath(container, blob.Name);
  if (internal::HasTrailingSlash(blob.Name)) {
    return DirectoryFileInfoFromPath(path);
  }
  FileInfo info{std::move(path), FileType::File};
  info.set_size(blob.BlobSize);
  info.set_mtime(std::chrono::system_clock::time_point{blob.Details.LastModified});
  return info;
}

/// \brief RAII-style guard for releasing a lease on a blob or container.
///
/// The guard should be constructed right after a successful BlobLeaseClient::Acquire()
/// call. Use std::optional<LeaseGuard> to declare a guard in outer scope and construct it
/// later with std::optional::emplace(...).
///
/// Leases expire automatically, but explicit release means concurrent clients or
/// ourselves when trying new operations on the same blob or container don't have
/// to wait for the lease to expire by itself.
///
/// Learn more about leases at
/// https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob
class LeaseGuard {
 public:
  using SteadyClock = std::chrono::steady_clock;

 private:
  /// \brief The time when the lease expires or is broken.
  ///
  /// The lease is not guaranteed to be valid until this time, but it is guaranteed to
  /// be expired after this time. In other words, this is an overestimation of
  /// the true time_point.
  SteadyClock::time_point break_or_expires_at_;
  const std::unique_ptr<Blobs::BlobLeaseClient> lease_client_;
  bool release_attempt_pending_ = true;

  /// \brief The latest known expiry time of a lease guarded by this class
  /// that failed to be released or was forgotten by calling Forget().
  static std::atomic<SteadyClock::time_point> latest_known_expiry_time_;

  /// \brief The maximum lease duration supported by Azure Storage.
  static constexpr std::chrono::seconds kMaxLeaseDuration{60};

 public:
  LeaseGuard(std::unique_ptr<Blobs::BlobLeaseClient> lease_client,
             std::chrono::seconds lease_duration)
      : break_or_expires_at_(SteadyClock::now() +
                             std::min(kMaxLeaseDuration, lease_duration)),
        lease_client_(std::move(lease_client)) {
    DCHECK(lease_duration <= kMaxLeaseDuration);
    DCHECK(this->lease_client_);
  }

  ARROW_DISALLOW_COPY_AND_ASSIGN(LeaseGuard);

  ~LeaseGuard() {
    // No point in trying any error handling here other than the debug checking. The lease
    // will eventually expire on the backend without any intervention from us (just much
    // later than if we released it).
    [[maybe_unused]] auto status = Release();
    ARROW_LOG(DEBUG) << status;
  }

  bool PendingRelease() const {
    return release_attempt_pending_ && SteadyClock::now() <= break_or_expires_at_;
  }

 private:
  Status DoRelease() {
    DCHECK(release_attempt_pending_);
    try {
      lease_client_->Release();
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(exception, "Failed to release the ",
                               lease_client_->GetLeaseId(), " lease");
    }
    return Status::OK();
  }

 public:
  std::string LeaseId() const { return lease_client_->GetLeaseId(); }

  bool StillValidFor(SteadyClock::duration expected_time_left) const {
    return SteadyClock::now() + expected_time_left < break_or_expires_at_;
  }

  /// \brief Break the lease.
  ///
  /// The lease will stay in the "Breaking" state for break_period seconds or
  /// less if the lease is expiring before that.
  ///
  /// https://learn.microsoft.com/en-us/rest/api/storageservices/lease-container#outcomes-of-use-attempts-on-containers-by-lease-state
  /// https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-use-attempts-on-blobs-by-lease-state
  Status Break(Azure::Nullable<std::chrono::seconds> break_period = {}) {
    auto remaining_time_ms = [this]() {
      const auto remaining_time = break_or_expires_at_ - SteadyClock::now();
      return std::chrono::duration_cast<std::chrono::milliseconds>(remaining_time);
    };
#ifndef NDEBUG
    if (break_period.HasValue() && !StillValidFor(*break_period)) {
      ARROW_LOG(WARNING)
          << "Azure Storage: requested break_period ("
          << break_period.ValueOr(std::chrono::seconds{0}).count()
          << "s) is too long or lease duration is too short for all the operations "
             "performed so far (lease expires in "
          << remaining_time_ms().count() << "ms)";
    }
#endif
    Blobs::BreakLeaseOptions options;
    options.BreakPeriod = break_period;
    try {
      lease_client_->Break(options);
      break_or_expires_at_ =
          std::min(break_or_expires_at_,
                   SteadyClock::now() + break_period.ValueOr(std::chrono::seconds{0}));
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(exception, "Failed to break the ",
                               lease_client_->GetLeaseId(), " lease expiring in ",
                               remaining_time_ms().count(), "ms");
    }
    return Status::OK();
  }

  /// \brief Break the lease before deleting or renaming the resource via the
  /// DataLakeFileSystemClient API.
  ///
  /// NOTE: When using the Blobs API, this is not necessary -- you can release a
  /// lease on a path after it's deleted with a lease on it.
  ///
  /// Calling this is recommended when the resource for which the lease was acquired is
  /// about to be deleted as there is no way of releasing the lease after that, we can
  /// only forget about it. The break_period should be a conservative estimate of the time
  /// it takes to delete/rename the resource.
  ///
  /// If break_period is too small, the delete/rename will fail with a lease conflict,
  /// and if it's too large the only consequence is that a lease on a non-existent
  /// resource will remain in the "Breaking" state for a while blocking others
  /// from recreating the resource.
  void BreakBeforeDeletion(std::chrono::seconds break_period) {
    ARROW_CHECK_OK(Break(break_period));
  }

  // These functions are marked ARROW_NOINLINE because they are called from
  // multiple locations, but are not performance-critical.

  ARROW_NOINLINE Status Release() {
    if (!PendingRelease()) {
      return Status::OK();
    }
    auto status = DoRelease();
    if (!status.ok()) {
      Forget();
      return status;
    }
    release_attempt_pending_ = false;
    return Status::OK();
  }

  /// \brief Prevent any release attempts in the destructor.
  ///
  /// When it's known they would certainly fail.
  /// \see LeaseGuard::BreakBeforeDeletion()
  ARROW_NOINLINE void Forget() {
    if (!PendingRelease()) {
      release_attempt_pending_ = false;
      return;
    }
    release_attempt_pending_ = false;
    // Remember the latest known expiry time so we can gracefully handle lease
    // acquisition failures by waiting until the latest forgotten lease.
    auto latest = latest_known_expiry_time_.load(std::memory_order_relaxed);
    while (
        latest < break_or_expires_at_ &&
        !latest_known_expiry_time_.compare_exchange_weak(latest, break_or_expires_at_)) {
    }
    DCHECK_GE(latest_known_expiry_time_.load(), break_or_expires_at_);
  }

  ARROW_NOINLINE static void WaitUntilLatestKnownExpiryTime() {
    auto remaining_time = latest_known_expiry_time_.load() - SteadyClock::now();
#ifndef NDEBUG
    int64_t remaining_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(remaining_time).count();
    ARROW_LOG(WARNING) << "LeaseGuard::WaitUntilLatestKnownExpiryTime for "
                       << remaining_time_ms << "ms...";
#endif
    DCHECK(remaining_time <= kMaxLeaseDuration);
    if (remaining_time > SteadyClock::duration::zero()) {
      std::this_thread::sleep_for(remaining_time);
    }
  }
};

}  // namespace

class AzureFileSystem::Impl {
 private:
  io::IOContext io_context_;
  AzureOptions options_;

  std::unique_ptr<DataLake::DataLakeServiceClient> datalake_service_client_;
  std::unique_ptr<Blobs::BlobServiceClient> blob_service_client_;
  HNSSupport cached_hns_support_ = HNSSupport::kUnknown;

  Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(std::move(io_context)), options_(std::move(options)) {}

 public:
  static Result<std::unique_ptr<AzureFileSystem::Impl>> Make(AzureOptions options,
                                                             io::IOContext io_context) {
    auto self = std::unique_ptr<AzureFileSystem::Impl>(
        new AzureFileSystem::Impl(std::move(options), std::move(io_context)));
    ARROW_ASSIGN_OR_RAISE(self->blob_service_client_,
                          self->options_.MakeBlobServiceClient());
    ARROW_ASSIGN_OR_RAISE(self->datalake_service_client_,
                          self->options_.MakeDataLakeServiceClient());
    return self;
  }

  io::IOContext& io_context() { return io_context_; }
  const AzureOptions& options() const { return options_; }

  Blobs::BlobContainerClient GetBlobContainerClient(const std::string& container_name) {
    return blob_service_client_->GetBlobContainerClient(container_name);
  }

  Blobs::BlobClient GetBlobClient(const std::string& container_name,
                                  const std::string& blob_name) {
    return GetBlobContainerClient(container_name).GetBlobClient(blob_name);
  }

  /// \param container_name Also known as "filesystem" in the ADLS Gen2 API.
  DataLake::DataLakeFileSystemClient GetFileSystemClient(
      const std::string& container_name) {
    return datalake_service_client_->GetFileSystemClient(container_name);
  }

  /// \brief Memoized version of CheckIfHierarchicalNamespaceIsEnabled.
  ///
  /// \return kEnabled/kDisabled/kContainerNotFound (kUnknown is never returned).
  Result<HNSSupport> HierarchicalNamespaceSupport(
      const DataLake::DataLakeFileSystemClient& adlfs_client) {
    switch (cached_hns_support_) {
      case HNSSupport::kEnabled:
      case HNSSupport::kDisabled:
        return cached_hns_support_;
      case HNSSupport::kUnknown:
      case HNSSupport::kContainerNotFound:
        // Try the check again because the support is still unknown or the container
        // that didn't exist before may exist now.
        break;
    }
    ARROW_ASSIGN_OR_RAISE(
        auto hns_support,
        internal::CheckIfHierarchicalNamespaceIsEnabled(adlfs_client, options_));
    DCHECK_NE(hns_support, HNSSupport::kUnknown);
    if (hns_support == HNSSupport::kContainerNotFound) {
      // Caller should handle kContainerNotFound case appropriately as it knows the
      // container this refers to, but the cached value in that case should remain
      // kUnknown before we get a CheckIfHierarchicalNamespaceIsEnabled result that
      // is not kContainerNotFound.
      cached_hns_support_ = HNSSupport::kUnknown;
    } else {
      cached_hns_support_ = hns_support;
    }
    return hns_support;
  }

  /// This is used from unit tests to ensure we perform operations on all the
  /// possible states of cached_hns_support_.
  void ForceCachedHierarchicalNamespaceSupport(int support) {
    auto hns_support = static_cast<HNSSupport>(support);
    switch (hns_support) {
      case HNSSupport::kUnknown:
      case HNSSupport::kContainerNotFound:
      case HNSSupport::kDisabled:
      case HNSSupport::kEnabled:
        cached_hns_support_ = hns_support;
        return;
    }
    // This is reachable if an invalid int is cast to enum class HNSSupport.
    DCHECK(false) << "Invalid enum HierarchicalNamespaceSupport value.";
  }

  /// \pre location.path is not empty.
  Result<FileInfo> GetFileInfo(const DataLake::DataLakeFileSystemClient& adlfs_client,
                               const AzureLocation& location,
                               Azure::Nullable<std::string> lease_id = {}) {
    auto file_client = adlfs_client.GetFileClient(location.path);
    DataLake::GetPathPropertiesOptions options;
    options.AccessConditions.LeaseId = std::move(lease_id);
    try {
      FileInfo info{location.all};
      auto properties = file_client.GetProperties(options);
      if (properties.Value.IsDirectory) {
        info.set_type(FileType::Directory);
      } else if (internal::HasTrailingSlash(location.path)) {
        // For a path with a trailing slash, a Hierarchical Namespace storage account
        // may recognize a file (path with trailing slash removed). For consistency
        // with other arrow::FileSystem implementations we chose to return NotFound
        // because the trailing slash means the user was looking for a directory.
        info.set_type(FileType::NotFound);
        return info;
      } else {
        info.set_type(FileType::File);
        info.set_size(properties.Value.FileSize);
      }
      info.set_mtime(
          std::chrono::system_clock::time_point{properties.Value.LastModified});
      return info;
    } catch (const Storage::StorageException& exception) {
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        return FileInfo{location.all, FileType::NotFound};
      }
      return ExceptionToStatus(
          exception, "GetProperties for '", file_client.GetUrl(),
          "' failed. GetFileInfo is unable to determine whether the path exists.");
    }
  }

  /// On flat namespace accounts there are no real directories. Directories are
  /// implied by empty directory marker blobs with names ending in "/" or there
  /// being blobs with names starting with the directory path.
  ///
  /// \pre location.path is not empty.
  Result<FileInfo> GetFileInfo(const Blobs::BlobContainerClient& container_client,
                               const AzureLocation& location) {
    DCHECK(!location.path.empty());
    Blobs::ListBlobsOptions options;
    options.Prefix = internal::RemoveTrailingSlash(location.path);
    options.PageSizeHint = 1;

    try {
      FileInfo info{location.all};
      auto list_response = container_client.ListBlobsByHierarchy(kDelimiter, options);
      // Since PageSizeHint=1, we expect at most one entry in either Blobs or
      // BlobPrefixes. A BlobPrefix always ends with kDelimiter ("/"), so we can
      // distinguish between a directory and a file by checking if we received a
      // prefix or a blob.
      // This strategy allows us to implement GetFileInfo with just 1 blob storage
      // operation in almost every case.
      if (!list_response.BlobPrefixes.empty()) {
        // Ensure the returned BlobPrefixes[0] string doesn't contain more characters than
        // the requested Prefix. For instance, if we request with Prefix="dir/abra" and
        // the container contains "dir/abracadabra/" but not "dir/abra/", we will get back
        // "dir/abracadabra/" in the BlobPrefixes list. If "dir/abra/" existed,
        // it would be returned instead because it comes before "dir/abracadabra/" in the
        // lexicographic order guaranteed by ListBlobsByHierarchy.
        const auto& blob_prefix = list_response.BlobPrefixes[0];
        if (blob_prefix == internal::EnsureTrailingSlash(location.path)) {
          info.set_type(FileType::Directory);
          return info;
        }
      }
      if (!list_response.Blobs.empty()) {
        const auto& blob = list_response.Blobs[0];
        if (blob.Name == location.path) {
          info.set_type(FileType::File);
          info.set_size(blob.BlobSize);
          info.set_mtime(
              std::chrono::system_clock::time_point{blob.Details.LastModified});
          return info;
        } else if (blob.Name[options.Prefix.Value().length()] < internal::kSep) {
          // First list result did not indicate a directory and there is definitely no
          // exactly matching blob. However, there may still be a directory that we
          // initially missed because the first list result came before
          // `options.Prefix + internal::kSep` lexigraphically.
          // For example the flat namespace storage account has the following blobs:
          // - container/dir.txt
          // - container/dir/file.txt
          // GetFileInfo(container/dir) should return FileType::Directory but in this
          // edge case `blob = "dir.txt"`, so without further checks we would incorrectly
          // return FileType::NotFound.
          // Therefore we make an extra list operation with the trailing slash to confirm
          // whether the path is a directory.
          options.Prefix = internal::EnsureTrailingSlash(location.path);
          auto list_with_trailing_slash_response = container_client.ListBlobs(options);
          if (!list_with_trailing_slash_response.Blobs.empty()) {
            info.set_type(FileType::Directory);
            return info;
          }
        }
      }
      info.set_type(FileType::NotFound);
      return info;
    } catch (const Storage::StorageException& exception) {
      if (IsContainerNotFound(exception)) {
        return FileInfo{location.all, FileType::NotFound};
      }
      return ExceptionToStatus(
          exception, "ListBlobsByHierarchy failed for prefix='", *options.Prefix,
          "'. GetFileInfo is unable to determine whether the path exists.");
    }
  }

  Result<FileInfo> GetFileInfoOfPathWithinContainer(const AzureLocation& location) {
    DCHECK(!location.container.empty() && !location.path.empty());
    // There is a path to search within the container. Check HNS support to proceed.
    auto adlfs_client = GetFileSystemClient(location.container);
    ARROW_ASSIGN_OR_RAISE(auto hns_support, HierarchicalNamespaceSupport(adlfs_client));
    if (hns_support == HNSSupport::kContainerNotFound) {
      return FileInfo{location.all, FileType::NotFound};
    }
    if (hns_support == HNSSupport::kEnabled) {
      return GetFileInfo(adlfs_client, location);
    }
    DCHECK_EQ(hns_support, HNSSupport::kDisabled);
    auto container_client = GetBlobContainerClient(location.container);
    return GetFileInfo(container_client, location);
  }

 private:
  /// \pre location.container is not empty.
  template <typename ContainerClient>
  Status CheckDirExists(const ContainerClient& container_client,
                        const AzureLocation& location) {
    DCHECK(!location.container.empty());
    FileInfo info;
    if (location.path.empty()) {
      ARROW_ASSIGN_OR_RAISE(info,
                            GetContainerPropsAsFileInfo(location, container_client));
    } else {
      ARROW_ASSIGN_OR_RAISE(info, GetFileInfo(container_client, location));
    }
    if (info.type() == FileType::NotFound) {
      return PathNotFound(location);
    }
    if (info.type() != FileType::Directory) {
      return NotADir(location);
    }
    return Status::OK();
  }

  template <typename OnContainer>
  Status VisitContainers(const Core::Context& context, OnContainer&& on_container) const {
    Blobs::ListBlobContainersOptions options;
    try {
      auto container_list_response =
          blob_service_client_->ListBlobContainers(options, context);
      for (; container_list_response.HasPage();
           container_list_response.MoveToNextPage(context)) {
        for (const auto& container : container_list_response.BlobContainers) {
          RETURN_NOT_OK(on_container(container));
        }
      }
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(exception, "Failed to list account containers.");
    }
    return Status::OK();
  }

  static std::string_view BasenameView(std::string_view s) {
    DCHECK(!internal::HasTrailingSlash(s));
    auto offset = s.find_last_of(internal::kSep);
    auto result = (offset == std::string_view::npos) ? s : s.substr(offset);
    DCHECK(!result.empty() && result.back() != internal::kSep);
    return result;
  }

  /// \brief List the paths at the root of a filesystem or some dir in a filesystem.
  ///
  /// \pre adlfs_client is the client for the filesystem named like the first
  /// segment of select.base_dir. The filesystem is know to exist.
  Status GetFileInfoWithSelectorFromFileSystem(
      const DataLake::DataLakeFileSystemClient& adlfs_client,
      const Core::Context& context, Azure::Nullable<int32_t> page_size_hint,
      const FileSelector& select, FileInfoVector* acc_results) {
    ARROW_ASSIGN_OR_RAISE(auto base_location, AzureLocation::FromString(select.base_dir));

    // The filesystem a.k.a. the container is known to exist so if the path is empty then
    // we have already found the base_location, so initialize found to true.
    bool found = base_location.path.empty();

    auto directory_client = adlfs_client.GetDirectoryClient(base_location.path);
    DataLake::ListPathsOptions options;
    options.PageSizeHint = page_size_hint;

    auto base_path_depth = internal::GetAbstractPathDepth(base_location.path);
    try {
      auto list_response = directory_client.ListPaths(select.recursive, options, context);
      for (; list_response.HasPage(); list_response.MoveToNextPage(context)) {
        if (list_response.Paths.empty()) {
          continue;
        }
        found = true;
        for (const auto& path : list_response.Paths) {
          if (path.Name == base_location.path && !path.IsDirectory) {
            return NotADir(base_location);
          }
          // Subtract 1 because with `max_recursion=0` we want to list the base path,
          // which will produce results with depth 1 greater that the base path's depth.
          // NOTE: `select.max_recursion` + anything will cause integer overflows because
          // `select.max_recursion` defaults to `INT32_MAX`. Therefore, options to
          // rewrite this condition in a more readable way are limited.
          if (internal::GetAbstractPathDepth(path.Name) - base_path_depth - 1 <=
              select.max_recursion) {
            acc_results->push_back(FileInfoFromPath(base_location.container, path));
          }
        }
      }
    } catch (const Storage::StorageException& exception) {
      if (IsContainerNotFound(exception) || exception.ErrorCode == "PathNotFound") {
        found = false;
      } else {
        return ExceptionToStatus(exception,
                                 "Failed to list paths in a directory: ", select.base_dir,
                                 ": ", directory_client.GetUrl());
      }
    }

    return found || select.allow_not_found
               ? Status::OK()
               : ::arrow::fs::internal::PathNotFound(select.base_dir);
  }

  /// \brief List the blobs at the root of a container or some dir in a container.
  ///
  /// \pre container_client is the client for the container named like the first
  /// segment of select.base_dir.
  Status GetFileInfoWithSelectorFromContainer(
      const Blobs::BlobContainerClient& container_client, const Core::Context& context,
      Azure::Nullable<int32_t> page_size_hint, const FileSelector& select,
      FileInfoVector* acc_results) {
    ARROW_ASSIGN_OR_RAISE(auto base_location, AzureLocation::FromString(select.base_dir));

    bool found = false;
    Blobs::ListBlobsOptions options;
    if (internal::IsEmptyPath(base_location.path)) {
      // If the base_dir is the root of the container, then we want to list all blobs in
      // the container and the Prefix should be empty and not even include the trailing
      // slash because the container itself represents the `<container>/` directory.
      options.Prefix = {};
      found = true;  // Unless the container itself is not found later!
    } else {
      options.Prefix = internal::EnsureTrailingSlash(base_location.path);
    }
    options.PageSizeHint = page_size_hint;
    options.Include = Blobs::Models::ListBlobsIncludeFlags::Metadata;

    auto recurse = [&](const std::string& blob_prefix) noexcept -> Status {
      if (select.recursive && select.max_recursion > 0) {
        FileSelector sub_select;
        sub_select.base_dir = internal::ConcatAbstractPath(
            base_location.container, internal::RemoveTrailingSlash(blob_prefix));
        sub_select.allow_not_found = true;
        sub_select.recursive = true;
        sub_select.max_recursion = select.max_recursion - 1;
        return GetFileInfoWithSelectorFromContainer(
            container_client, context, page_size_hint, sub_select, acc_results);
      }
      return Status::OK();
    };

    auto process_blob = [&](const Blobs::Models::BlobItem& blob) noexcept {
      // blob.Name has trailing slash only when Prefix is an empty
      // directory marker blob for the directory we're listing
      // from, and we should skip it.
      if (!internal::HasTrailingSlash(blob.Name)) {
        acc_results->push_back(FileInfoFromBlob(base_location.container, blob));
      }
    };
    auto process_prefix = [&](const std::string& prefix) noexcept -> Status {
      const auto path = internal::ConcatAbstractPath(base_location.container, prefix);
      acc_results->push_back(DirectoryFileInfoFromPath(path));
      return recurse(prefix);
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
          } else {
            DCHECK_EQ(blob.Name, prefix);
            RETURN_NOT_OK(process_prefix(prefix));
            blob_index += 1;
            blob_prefix_index += 1;
            // If the container has an empty dir marker blob and another blob starting
            // with this blob name as a prefix, the blob doesn't appear in the listing
            // that also contains the prefix, so AFAICT this branch in unreachable. The
            // code above is kept just in case, but if this DCHECK(false) is ever reached,
            // we should refactor this loop to ensure no duplicate entries are ever
            // reported.
            DCHECK(false)
                << "Unexpected blob/prefix name collision on the same listing request";
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
    } catch (const Storage::StorageException& exception) {
      if (IsContainerNotFound(exception)) {
        found = false;
      } else {
        return ExceptionToStatus(exception,
                                 "Failed to list blobs in a directory: ", select.base_dir,
                                 ": ", container_client.GetUrl());
      }
    }

    return found || select.allow_not_found
               ? Status::OK()
               : ::arrow::fs::internal::PathNotFound(select.base_dir);
  }

 public:
  Status GetFileInfoWithSelector(const Core::Context& context,
                                 Azure::Nullable<int32_t> page_size_hint,
                                 const FileSelector& select,
                                 FileInfoVector* acc_results) {
    ARROW_ASSIGN_OR_RAISE(auto base_location, AzureLocation::FromString(select.base_dir));

    if (base_location.container.empty()) {
      // Without a container, the base_location is equivalent to the filesystem
      // root -- `/`. FileSelector::allow_not_found doesn't matter in this case
      // because the root always exists.
      auto on_container = [&](const Blobs::Models::BlobContainerItem& container) {
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
          ARROW_RETURN_NOT_OK(
              GetFileInfoWithSelector(context, page_size_hint, sub_select, acc_results));
        }
        return Status::OK();
      };
      return VisitContainers(context, std::move(on_container));
    }

    auto adlfs_client = GetFileSystemClient(base_location.container);
    ARROW_ASSIGN_OR_RAISE(auto hns_support, HierarchicalNamespaceSupport(adlfs_client));
    if (hns_support == HNSSupport::kContainerNotFound) {
      if (select.allow_not_found) {
        return Status::OK();
      } else {
        return ::arrow::fs::internal::PathNotFound(select.base_dir);
      }
    }
    if (hns_support == HNSSupport::kEnabled) {
      return GetFileInfoWithSelectorFromFileSystem(adlfs_client, context, page_size_hint,
                                                   select, acc_results);
    }
    DCHECK_EQ(hns_support, HNSSupport::kDisabled);
    auto container_client =
        blob_service_client_->GetBlobContainerClient(base_location.container);
    return GetFileInfoWithSelectorFromContainer(container_client, context, page_size_hint,
                                                select, acc_results);
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const AzureLocation& location,
                                                         AzureFileSystem* fs) {
    RETURN_NOT_OK(ValidateFileLocation(location));
    auto blob_client = std::make_shared<Blobs::BlobClient>(
        GetBlobClient(location.container, location.path));

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
    auto blob_client = std::make_shared<Blobs::BlobClient>(
        GetBlobClient(location.container, location.path));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(),
                                                 std::move(location), info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

 private:
  /// This function cannot assume the filesystem/container already exists.
  ///
  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  template <class ContainerClient, class CreateDirIfNotExists>
  Status CreateDirTemplate(const ContainerClient& container_client,
                           CreateDirIfNotExists&& create_if_not_exists,
                           const AzureLocation& location, bool recursive) {
    DCHECK(!location.container.empty());
    DCHECK(!location.path.empty());
    if (recursive) {
      // Recursive CreateDir calls require that all path segments be
      // either a directory or not found.

      // Check each path segment is a directory or not
      // found. Nonexistent segments are collected to
      // nonexistent_locations. We'll create directories for
      // nonexistent segments later.
      std::vector<AzureLocation> nonexistent_locations;
      for (auto prefix = location; !prefix.path.empty(); prefix = prefix.parent()) {
        ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(container_client, prefix));
        if (info.type() == FileType::File) {
          return NotADir(prefix);
        }
        if (info.type() == FileType::NotFound) {
          nonexistent_locations.push_back(prefix);
        }
      }
      // Ensure container exists
      ARROW_ASSIGN_OR_RAISE(auto container,
                            AzureLocation::FromString(location.container));
      ARROW_ASSIGN_OR_RAISE(auto container_info,
                            GetContainerPropsAsFileInfo(container, container_client));
      if (container_info.type() == FileType::NotFound) {
        try {
          container_client.CreateIfNotExists();
        } catch (const Storage::StorageException& exception) {
          return ExceptionToStatus(exception, "Failed to create directory '",
                                   location.all, "': ", container_client.GetUrl());
        }
      }
      // Create nonexistent directories from shorter to longer:
      //
      // Example:
      //
      // * location: /container/a/b/c/d/
      // * Nonexistent path segments:
      //   * /container/a/
      //   * /container/a/c/
      //   * /container/a/c/d/
      // * target_locations:
      //   1. /container/a/c/d/
      //   2. /container/a/c/
      //   3. /container/a/
      //
      // Create order:
      //   1. /container/a/
      //   2. /container/a/c/
      //   3. /container/a/c/d/
      for (size_t i = nonexistent_locations.size(); i > 0; --i) {
        const auto& nonexistent_location = nonexistent_locations[i - 1];
        try {
          create_if_not_exists(container_client, nonexistent_location);
        } catch (const Storage::StorageException& exception) {
          return ExceptionToStatus(exception, "Failed to create directory '",
                                   location.all, "': ", container_client.GetUrl());
        }
      }
      return Status::OK();
    } else {
      // Non-recursive CreateDir calls require the parent directory to exist.
      auto parent = location.parent();
      if (!parent.path.empty()) {
        RETURN_NOT_OK(CheckDirExists(container_client, parent));
      }
      // If the parent location is just the container, we don't need to check if it
      // exists because the operation we perform below will fail if the container
      // doesn't exist and we can handle that error according to the recursive flag.
      try {
        create_if_not_exists(container_client, location);
        return Status::OK();
      } catch (const Storage::StorageException& exception) {
        if (IsContainerNotFound(exception)) {
          auto parent = location.parent();
          return PathNotFound(parent);
        }
        return ExceptionToStatus(exception, "Failed to create directory '", location.all,
                                 "': ", container_client.GetUrl());
      }
    }
  }

 public:
  /// This function cannot assume the filesystem already exists.
  ///
  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  Status CreateDirOnFileSystem(const DataLake::DataLakeFileSystemClient& adlfs_client,
                               const AzureLocation& location, bool recursive) {
    return CreateDirTemplate(
        adlfs_client,
        [](const auto& adlfs_client, const auto& location) {
          auto directory_client = adlfs_client.GetDirectoryClient(
              std::string(internal::RemoveTrailingSlash(location.path)));
          directory_client.CreateIfNotExists();
        },
        location, recursive);
  }

  /// This function cannot assume the container already exists.
  ///
  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  Status CreateDirOnContainer(const Blobs::BlobContainerClient& container_client,
                              const AzureLocation& location, bool recursive) {
    return CreateDirTemplate(
        container_client,
        [this](const auto& container_client, const auto& location) {
          EnsureEmptyDirExistsImplThatThrows(container_client, location.path);
        },
        location, recursive);
  }

  Result<std::shared_ptr<ObjectAppendStream>> OpenAppendStream(
      const AzureLocation& location,
      const std::shared_ptr<const KeyValueMetadata>& metadata, const bool truncate,
      AzureFileSystem* fs) {
    RETURN_NOT_OK(ValidateFileLocation(location));

    const auto blob_container_client = GetBlobContainerClient(location.container);
    auto block_blob_client = std::make_shared<Blobs::BlockBlobClient>(
        blob_container_client.GetBlockBlobClient(location.path));

    auto ensure_not_flat_namespace_directory = [this, location,
                                                blob_container_client]() -> Status {
      ARROW_ASSIGN_OR_RAISE(
          auto hns_support,
          HierarchicalNamespaceSupport(GetFileSystemClient(location.container)));
      if (hns_support == HNSSupport::kDisabled) {
        // Flat namespace so we need to GetFileInfo in-case its a directory.
        ARROW_ASSIGN_OR_RAISE(auto status, GetFileInfo(blob_container_client, location))
        if (status.type() == FileType::Directory) {
          return NotAFile(location);
        }
      }
      // kContainerNotFound - it doesn't exist, so no need to check if its a directory.
      // kEnabled - hierarchical namespace so Azure APIs will fail if its a directory. We
      // don't need to explicitly check.
      return Status::OK();
    };

    std::shared_ptr<ObjectAppendStream> stream;
    stream = std::make_shared<ObjectAppendStream>(block_blob_client, fs->io_context(),
                                                  location, metadata, options_);
    RETURN_NOT_OK(stream->Init(truncate, ensure_not_flat_namespace_directory));
    return stream;
  }

 private:
  void EnsureEmptyDirExistsImplThatThrows(
      const Blobs::BlobContainerClient& container_client,
      const std::string& path_within_container) {
    auto dir_marker_blob_path = internal::EnsureTrailingSlash(path_within_container);
    auto block_blob_client =
        container_client.GetBlobClient(dir_marker_blob_path).AsBlockBlobClient();
    // Attach metadata that other filesystem implementations expect to be present
    // on directory marker blobs.
    // https://github.com/fsspec/adlfs/blob/32132c4094350fca2680155a5c236f2e9f991ba5/adlfs/spec.py#L855-L870
    Blobs::UploadBlockBlobFromOptions blob_options;
    blob_options.Metadata.emplace(kFlatNamespaceIsDirectoryMetadataKey, "true");
    block_blob_client.UploadFrom(nullptr, 0, blob_options);
  }

 public:
  /// This function assumes the container already exists. So it can only be
  /// called after that has been verified.
  ///
  /// \pre location.container is not empty.
  /// \pre The location.container container already exists.
  Status EnsureEmptyDirExists(const Blobs::BlobContainerClient& container_client,
                              const AzureLocation& location, const char* operation_name) {
    DCHECK(!location.container.empty());
    if (location.path.empty()) {
      // Nothing to do. The container already exists per the preconditions.
      return Status::OK();
    }
    try {
      EnsureEmptyDirExistsImplThatThrows(container_client, location.path);
      return Status::OK();
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(
          exception, operation_name, " failed to ensure empty directory marker '",
          location.path, "' exists in container: ", container_client.GetUrl());
    }
  }

  /// \pre location.container is not empty.
  /// \pre location.path is empty.
  Status DeleteContainer(const Blobs::BlobContainerClient& container_client,
                         const AzureLocation& location) {
    DCHECK(!location.container.empty());
    DCHECK(location.path.empty());
    try {
      auto response = container_client.Delete();
      // Only the "*IfExists" functions ever set Deleted to false.
      // All the others either succeed or throw an exception.
      DCHECK(response.Value.Deleted);
    } catch (const Storage::StorageException& exception) {
      if (IsContainerNotFound(exception)) {
        return PathNotFound(location);
      }
      return ExceptionToStatus(exception,
                               "Failed to delete a container: ", location.container, ": ",
                               container_client.GetUrl());
    }
    return Status::OK();
  }

  /// Deletes contents of a directory and possibly the directory itself
  /// depending on the value of preserve_dir_marker_blob.
  ///
  /// \pre location.container is not empty.
  /// \pre preserve_dir_marker_blob=false implies location.path is not empty
  /// because we can't *not preserve* the root directory of a container.
  ///
  /// \param require_dir_to_exist Require the directory to exist *before* this
  /// operation, otherwise return PathNotFound.
  /// \param preserve_dir_marker_blob Ensure the empty directory marker blob
  /// is preserved (not deleted) or created (before the contents are deleted) if it
  /// doesn't exist explicitly but is implied by the existence of blobs with names
  /// starting with the directory path.
  /// \param operation_name Used in error messages to accurately describe the operation
  Status DeleteDirContentsOnContainer(const Blobs::BlobContainerClient& container_client,
                                      const AzureLocation& location,
                                      bool require_dir_to_exist,
                                      bool preserve_dir_marker_blob,
                                      const char* operation_name) {
    using DeleteBlobResponse = Storage::DeferredResponse<Blobs::Models::DeleteBlobResult>;
    DCHECK(!location.container.empty());
    DCHECK(preserve_dir_marker_blob || !location.path.empty())
        << "Must pass preserve_dir_marker_blob=true when location.path is empty "
           "(i.e. deleting the contents of a container).";
    Blobs::ListBlobsOptions options;
    if (!location.path.empty()) {
      options.Prefix = internal::EnsureTrailingSlash(location.path);
    }
    // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch#remarks
    //
    // Only supports up to 256 subrequests in a single batch. The
    // size of the body for a batch request can't exceed 4 MB.
    const int32_t kNumMaxRequestsInBatch = 256;
    options.PageSizeHint = kNumMaxRequestsInBatch;
    // trusted only if preserve_dir_marker_blob is true.
    bool found_dir_marker_blob = false;
    try {
      auto list_response = container_client.ListBlobs(options);
      if (list_response.Blobs.empty()) {
        if (require_dir_to_exist) {
          return PathNotFound(location);
        } else {
          ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(container_client, location));
          if (info.type() == FileType::File) {
            return NotADir(location);
          }
        }
      }
      for (; list_response.HasPage(); list_response.MoveToNextPage()) {
        if (list_response.Blobs.empty()) {
          continue;
        }
        auto batch = container_client.CreateBatch();
        std::vector<std::pair<std::string_view, DeleteBlobResponse>> deferred_responses;
        for (const auto& blob_item : list_response.Blobs) {
          if (preserve_dir_marker_blob && !found_dir_marker_blob) {
            const bool is_dir_marker_blob =
                options.Prefix.HasValue() && blob_item.Name == *options.Prefix;
            if (is_dir_marker_blob) {
              // Skip deletion of the existing directory marker blob,
              // but take note that it exists.
              found_dir_marker_blob = true;
              continue;
            }
          }
          deferred_responses.emplace_back(blob_item.Name,
                                          batch.DeleteBlob(blob_item.Name));
        }
        try {
          // Before submitting the batch deleting directory contents, ensure
          // the empty directory marker blob exists. Doing this first, means that
          // directory doesn't "stop existing" during the duration of the batch delete
          // operation.
          if (preserve_dir_marker_blob && !found_dir_marker_blob) {
            // Only create an empty directory marker blob if the directory's
            // existence is implied by the existence of blobs with names
            // starting with the directory path.
            if (!deferred_responses.empty()) {
              RETURN_NOT_OK(
                  EnsureEmptyDirExists(container_client, location, operation_name));
            }
          }
          if (!deferred_responses.empty()) {
            container_client.SubmitBatch(batch);
          }
        } catch (const Storage::StorageException& exception) {
          return ExceptionToStatus(exception, "Failed to delete blobs in a directory: ",
                                   location.path, ": ", container_client.GetUrl());
        }
        std::vector<std::string> failed_blob_names;
        for (auto& [blob_name_view, deferred_response] : deferred_responses) {
          bool success = true;
          try {
            auto delete_result = deferred_response.GetResponse();
            success = delete_result.Value.Deleted;
          } catch (const Storage::StorageException& exception) {
            success = false;
          }
          if (!success) {
            failed_blob_names.emplace_back(blob_name_view);
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
      return Status::OK();
    } catch (const Storage::StorageException& exception) {
      return ExceptionToStatus(exception,
                               "Failed to list blobs in a directory: ", location.path,
                               ": ", container_client.GetUrl());
    }
  }

  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  Status DeleteDirOnFileSystem(const DataLake::DataLakeFileSystemClient& adlfs_client,
                               const AzureLocation& location, bool recursive,
                               bool require_dir_to_exist,
                               Azure::Nullable<std::string> lease_id = {}) {
    DCHECK(!location.container.empty());
    DCHECK(!location.path.empty());
    ARROW_ASSIGN_OR_RAISE(auto file_info, GetFileInfo(adlfs_client, location, lease_id));
    if (file_info.type() == FileType::NotFound) {
      if (require_dir_to_exist) {
        return PathNotFound(location);
      } else {
        return Status::OK();
      }
    }
    if (file_info.type() != FileType::Directory) {
      return NotADir(location);
    }
    auto directory_client = adlfs_client.GetDirectoryClient(
        std::string(internal::RemoveTrailingSlash(location.path)));
    DataLake::DeleteDirectoryOptions options;
    options.AccessConditions.LeaseId = std::move(lease_id);
    try {
      auto response = recursive ? directory_client.DeleteRecursive(options)
                                : directory_client.DeleteEmpty(options);
      // Only the "*IfExists" functions ever set Deleted to false.
      // All the others either succeed or throw an exception.
      DCHECK(response.Value.Deleted);
    } catch (const Storage::StorageException& exception) {
      if (exception.ErrorCode == "FilesystemNotFound" ||
          exception.ErrorCode == "PathNotFound") {
        if (require_dir_to_exist) {
          return PathNotFound(location);
        }
        return Status::OK();
      }
      return ExceptionToStatus(exception, "Failed to delete a directory: ", location.path,
                               ": ", directory_client.GetUrl());
    }
    return Status::OK();
  }

  /// \pre location.container is not empty.
  Status DeleteDirContentsOnFileSystem(
      const DataLake::DataLakeFileSystemClient& adlfs_client,
      const AzureLocation& location, bool missing_dir_ok) {
    auto directory_client = adlfs_client.GetDirectoryClient(location.path);
    try {
      auto list_response = directory_client.ListPaths(false);
      for (; list_response.HasPage(); list_response.MoveToNextPage()) {
        for (const auto& path : list_response.Paths) {
          if (path.IsDirectory) {
            auto sub_directory_client = adlfs_client.GetDirectoryClient(path.Name);
            try {
              sub_directory_client.DeleteRecursive();
            } catch (const Storage::StorageException& exception) {
              return ExceptionToStatus(
                  exception, "Failed to delete a sub directory: ", location.container,
                  kDelimiter, path.Name, ": ", sub_directory_client.GetUrl());
            }
          } else {
            if (path.Name == location.path) {
              return NotADir(location);
            }
            auto sub_file_client = adlfs_client.GetFileClient(path.Name);
            try {
              sub_file_client.Delete();
            } catch (const Storage::StorageException& exception) {
              return ExceptionToStatus(
                  exception, "Failed to delete a sub file: ", location.container,
                  kDelimiter, path.Name, ": ", sub_file_client.GetUrl());
            }
          }
        }
      }
      return Status::OK();
    } catch (const Storage::StorageException& exception) {
      if (missing_dir_ok && exception.StatusCode == Http::HttpStatusCode::NotFound) {
        return Status::OK();
      }
      return ExceptionToStatus(exception,
                               "Failed to delete directory contents: ", location.path,
                               ": ", directory_client.GetUrl());
    }
  }

 private:
  /// \brief Create a BlobLeaseClient and acquire a lease on the container.
  ///
  /// \param allow_missing_container if true, a nullptr may be returned when the container
  /// doesn't exist, otherwise a PathNotFound(location) error is produced right away
  /// \return A BlobLeaseClient is wrapped as a unique_ptr so it's moveable and
  /// optional (nullptr denotes container not found)
  Result<std::unique_ptr<Blobs::BlobLeaseClient>> AcquireContainerLease(
      const AzureLocation& location, std::chrono::seconds lease_duration,
      bool allow_missing_container = false, bool retry_allowed = true) {
    DCHECK(!location.container.empty());
    auto container_client = GetBlobContainerClient(location.container);
    auto lease_id = Blobs::BlobLeaseClient::CreateUniqueLeaseId();
    auto container_url = container_client.GetUrl();
    auto lease_client = std::make_unique<Blobs::BlobLeaseClient>(
        std::move(container_client), std::move(lease_id));
    try {
      [[maybe_unused]] auto result = lease_client->Acquire(lease_duration);
      DCHECK_EQ(result.Value.LeaseId, lease_client->GetLeaseId());
    } catch (const Storage::StorageException& exception) {
      if (IsContainerNotFound(exception)) {
        if (allow_missing_container) {
          return nullptr;
        }
        return PathNotFound(location);
      } else if (exception.StatusCode == Http::HttpStatusCode::Conflict &&
                 exception.ErrorCode == "LeaseAlreadyPresent") {
        if (retry_allowed) {
          LeaseGuard::WaitUntilLatestKnownExpiryTime();
          return AcquireContainerLease(location, lease_duration, allow_missing_container,
                                       /*retry_allowed=*/false);
        }
      }
      return ExceptionToStatus(exception, "Failed to acquire a lease on container '",
                               location.container, "': ", container_url);
    }
    return lease_client;
  }

  /// \brief Create a BlobLeaseClient and acquire a lease on a blob/file (or
  /// directory if Hierarchical Namespace is supported).
  ///
  /// \param allow_missing if true, a nullptr may be returned when the blob
  /// doesn't exist, otherwise a PathNotFound(location) error is produced right away
  /// \return A BlobLeaseClient is wrapped as a unique_ptr so it's moveable and
  /// optional (nullptr denotes blob not found)
  Result<std::unique_ptr<Blobs::BlobLeaseClient>> AcquireBlobLease(
      const AzureLocation& location, std::chrono::seconds lease_duration,
      bool allow_missing, bool retry_allowed = true) {
    DCHECK(!location.container.empty() && !location.path.empty());
    auto path = std::string{internal::RemoveTrailingSlash(location.path)};
    auto blob_client = GetBlobClient(location.container, std::move(path));
    auto lease_id = Blobs::BlobLeaseClient::CreateUniqueLeaseId();
    auto blob_url = blob_client.GetUrl();
    auto lease_client = std::make_unique<Blobs::BlobLeaseClient>(std::move(blob_client),
                                                                 std::move(lease_id));
    try {
      [[maybe_unused]] auto result = lease_client->Acquire(lease_duration);
      DCHECK_EQ(result.Value.LeaseId, lease_client->GetLeaseId());
    } catch (const Storage::StorageException& exception) {
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        if (allow_missing) {
          return nullptr;
        }
        return PathNotFound(location);
      } else if (exception.StatusCode == Http::HttpStatusCode::Conflict &&
                 exception.ErrorCode == "LeaseAlreadyPresent") {
        if (retry_allowed) {
          LeaseGuard::WaitUntilLatestKnownExpiryTime();
          return AcquireBlobLease(location, lease_duration, allow_missing,
                                  /*retry_allowed=*/false);
        }
      }
      return ExceptionToStatus(exception, "Failed to acquire a lease on file '",
                               location.all, "': ", blob_url);
    }
    return lease_client;
  }

  /// \brief The default lease duration used for acquiring a lease on a container or blob.
  ///
  /// Azure Storage leases can be acquired for a duration of 15 to 60 seconds.
  ///
  /// Operations consisting of an unpredictable number of sub-operations should
  /// renew the lease periodically (heartbeat pattern) instead of acquiring an
  /// infinite lease (very bad idea for a library like Arrow).
  static constexpr auto kLeaseDuration = std::chrono::seconds{15};

  // These are conservative estimates of how long it takes for the client
  // request to reach the server counting from the moment the Azure SDK function
  // issuing the request is called. See their usage for more context.
  //
  // If the client connection to the server is unpredictably slow, operations
  // may fail, but due to the use of leases, the entire arrow::FileSystem
  // operation can be retried without risk of data loss. Thus, unexpected network
  // slow downs can be fixed with retries (either by some system using Arrow or
  // an user interactively retrying a failed operation).
  //
  // If a network is permanently slow, the lease time and these numbers should be
  // increased but not so much so that the client gives up an operation because the
  // values say it takes more time to reach the server than the remaining lease
  // time on the resources.
  //
  // NOTE: The initial constant values were chosen conservatively. If we learn,
  // from experience, that they are causing issues, we can increase them. And if
  // broadly applicable values aren't possible, we can make them configurable.
  static constexpr auto kTimeNeededForContainerDeletion = std::chrono::seconds{3};
  static constexpr auto kTimeNeededForContainerRename = std::chrono::seconds{3};
  static constexpr auto kTimeNeededForFileOrDirectoryRename = std::chrono::seconds{3};

 public:
  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  Status DeleteFileOnFileSystem(const DataLake::DataLakeFileSystemClient& adlfs_client,
                                const AzureLocation& location,
                                bool require_file_to_exist) {
    DCHECK(!location.container.empty());
    DCHECK(!location.path.empty());
    auto path_no_trailing_slash =
        std::string{internal::RemoveTrailingSlash(location.path)};
    auto file_client = adlfs_client.GetFileClient(path_no_trailing_slash);
    try {
      // This is necessary to avoid deletion of directories via DeleteFile.
      auto properties = file_client.GetProperties();
      if (properties.Value.IsDirectory) {
        return internal::NotAFile(location.all);
      }
      if (internal::HasTrailingSlash(location.path)) {
        return internal::NotADir(location.all);
      }
      auto response = file_client.Delete();
      // Only the "*IfExists" functions ever set Deleted to false.
      // All the others either succeed or throw an exception.
      DCHECK(response.Value.Deleted);
    } catch (const Storage::StorageException& exception) {
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        // ErrorCode can be "FilesystemNotFound", "PathNotFound"...
        if (require_file_to_exist) {
          return PathNotFound(location);
        }
        return Status::OK();
      }
      return ExceptionToStatus(exception, "Failed to delete a file: ", location.path,
                               ": ", file_client.GetUrl());
    }
    return Status::OK();
  }

  /// \pre location.container is not empty.
  /// \pre location.path is not empty.
  Status DeleteFileOnContainer(const Blobs::BlobContainerClient& container_client,
                               const AzureLocation& location, bool require_file_to_exist,
                               const char* operation) {
    DCHECK(!location.container.empty());
    DCHECK(!location.path.empty());
    constexpr auto kFileBlobLeaseTime = std::chrono::seconds{15};

    // When it's known that the blob doesn't exist as a file, check if it exists as a
    // directory to generate the appropriate error message.
    auto check_if_location_exists_as_dir = [&]() -> Status {
      auto no_trailing_slash = location;
      no_trailing_slash.path = internal::RemoveTrailingSlash(location.path);
      no_trailing_slash.all = internal::RemoveTrailingSlash(location.all);
      ARROW_ASSIGN_OR_RAISE(auto file_info,
                            GetFileInfo(container_client, no_trailing_slash));
      if (file_info.type() == FileType::NotFound) {
        return require_file_to_exist ? PathNotFound(location) : Status::OK();
      }
      if (file_info.type() == FileType::Directory) {
        return internal::NotAFile(location.all);
      }
      return internal::HasTrailingSlash(location.path) ? internal::NotADir(location.all)
                                                       : internal::NotAFile(location.all);
    };

    // Paths ending with trailing slashes are never leading to a deletion,
    // but the correct error message requires a check of the path.
    if (internal::HasTrailingSlash(location.path)) {
      return check_if_location_exists_as_dir();
    }

    // If the parent directory of a file is not the container itself, there is a
    // risk that deleting the file also deletes the *implied directory* -- the
    // directory that is implied by the existence of the file path.
    //
    // In this case, we must ensure that the deletion is not semantically
    // equivalent to also deleting the directory. This is done by ensuring the
    // directory marker blob exists before the file is deleted.
    std::optional<LeaseGuard> file_blob_lease_guard;
    const auto parent = location.parent();
    if (!parent.path.empty()) {
      // We have to check the existence of the file before checking the
      // existence of the parent directory marker, so we acquire a lease on the
      // file first.
      ARROW_ASSIGN_OR_RAISE(auto file_blob_lease_client,
                            AcquireBlobLease(location, kFileBlobLeaseTime,
                                             /*allow_missing=*/true));
      if (file_blob_lease_client) {
        file_blob_lease_guard.emplace(std::move(file_blob_lease_client),
                                      kFileBlobLeaseTime);
        // Ensure the empty directory marker blob of the parent exists before the file is
        // deleted.
        //
        // There is not need to hold a lease on the directory marker because if
        // a concurrent client deletes the directory marker right after we
        // create it, the file deletion itself won't be the cause of the directory
        // deletion. Additionally, the fact that a lease is held on the blob path
        // semantically preserves the directory -- its existence is implied
        // until the blob representing the file is deleted -- even if another
        // client deletes the directory marker.
        RETURN_NOT_OK(EnsureEmptyDirExists(container_client, parent, operation));
      } else {
        return check_if_location_exists_as_dir();
      }
    }

    auto blob_client = container_client.GetBlobClient(location.path);
    Blobs::DeleteBlobOptions options;
    if (file_blob_lease_guard) {
      options.AccessConditions.LeaseId = file_blob_lease_guard->LeaseId();
    }
    try {
      auto response = blob_client.Delete(options);
      // Only the "*IfExists" functions ever set Deleted to false.
      // All the others either succeed or throw an exception.
      DCHECK(response.Value.Deleted);
    } catch (const Storage::StorageException& exception) {
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        return check_if_location_exists_as_dir();
      }
      return ExceptionToStatus(exception, "Failed to delete a file: ", location.all, ": ",
                               blob_client.GetUrl());
    }
    return Status::OK();
  }

  /// The conditions for a successful container rename are derived from the
  /// conditions for a successful `Move("/$src.container", "/$dest.container")`.
  /// The numbers here match the list in `Move`.
  ///
  /// 1. `src.container` must exist.
  /// 2. If `src.container` and `dest.container` are the same, do nothing and
  ///    return OK.
  /// 3. N/A.
  /// 4. N/A.
  /// 5. `dest.container` doesn't exist or is empty.
  ///    NOTE: one exception related to container Move is that when the
  ///    destination is empty we also require the source container to be empty,
  ///    because the only way to perform the "Move" is by deleting the source
  ///    instead of deleting the destination and renaming the source.
  Status RenameContainer(const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && src.path.empty());
    DCHECK(!dest.container.empty() && dest.path.empty());
    auto src_container_client = GetBlobContainerClient(src.container);

    // If src and dest are the same, we only have to check src exists.
    if (src.container == dest.container) {
      ARROW_ASSIGN_OR_RAISE(auto info,
                            GetContainerPropsAsFileInfo(src, src_container_client));
      if (info.type() == FileType::NotFound) {
        return PathNotFound(src);
      }
      DCHECK(info.type() == FileType::Directory);
      return Status::OK();
    }

    // Acquire a lease on the source container because (1) we need the lease
    // before rename and (2) it works as a way of checking the container exists.
    ARROW_ASSIGN_OR_RAISE(auto src_lease_client,
                          AcquireContainerLease(src, kLeaseDuration));
    LeaseGuard src_lease_guard{std::move(src_lease_client), kLeaseDuration};
    // Check dest.container doesn't exist or is empty.
    auto dest_container_client = GetBlobContainerClient(dest.container);
    std::optional<LeaseGuard> dest_lease_guard;
    bool dest_exists = false;
    bool dest_is_empty = false;
    ARROW_ASSIGN_OR_RAISE(
        auto dest_lease_client,
        AcquireContainerLease(dest, kLeaseDuration, /*allow_missing_container*/ true));
    if (dest_lease_client) {
      dest_lease_guard.emplace(std::move(dest_lease_client), kLeaseDuration);
      dest_exists = true;
      // Emptiness check after successful acquisition of the lease.
      Blobs::ListBlobsOptions list_blobs_options;
      list_blobs_options.PageSizeHint = 1;
      try {
        auto dest_list_response = dest_container_client.ListBlobs(list_blobs_options);
        dest_is_empty = dest_list_response.Blobs.empty();
        if (!dest_is_empty) {
          return NotEmpty(dest);
        }
      } catch (const Storage::StorageException& exception) {
        return ExceptionToStatus(exception, "Failed to check that '", dest.container,
                                 "' is empty: ", dest_container_client.GetUrl());
      }
    }
    DCHECK(!dest_exists || dest_is_empty);

    if (!dest_exists) {
      // Rename the source container.
      Blobs::RenameBlobContainerOptions options;
      options.SourceAccessConditions.LeaseId = src_lease_guard.LeaseId();
      try {
        src_lease_guard.BreakBeforeDeletion(kTimeNeededForContainerRename);
        blob_service_client_->RenameBlobContainer(src.container, dest.container, options);
        src_lease_guard.Forget();
      } catch (const Storage::StorageException& exception) {
        if (exception.StatusCode == Http::HttpStatusCode::BadRequest &&
            exception.ErrorCode == "InvalidQueryParameterValue") {
          auto param_name = exception.AdditionalInformation.find("QueryParameterName");
          if (param_name != exception.AdditionalInformation.end() &&
              param_name->second == "comp") {
            return ExceptionToStatus(
                exception, "The 'rename' operation is not supported on containers. ",
                "Attempting a rename of '", src.container, "' to '", dest.container,
                "': ", blob_service_client_->GetUrl());
          }
        }
        return ExceptionToStatus(exception, "Failed to rename container '", src.container,
                                 "' to '", dest.container,
                                 "': ", blob_service_client_->GetUrl());
      }
    } else if (dest_is_empty) {
      // Even if we deleted the empty dest.container, RenameBlobContainer() would still
      // fail because containers are not immediately deleted by the service -- they are
      // deleted asynchronously based on retention policies and non-deterministic behavior
      // of the garbage collection process.
      //
      // One way to still match POSIX rename semantics is to delete the src.container if
      // and only if it's empty because the final state would be equivalent to replacing
      // the dest.container with the src.container and its contents (which happens
      // to also be empty).
      Blobs::ListBlobsOptions list_blobs_options;
      list_blobs_options.PageSizeHint = 1;
      try {
        auto src_list_response = src_container_client.ListBlobs(list_blobs_options);
        if (!src_list_response.Blobs.empty()) {
          // Reminder: dest is used here because we're semantically replacing dest
          // with src. By deleting src if it's empty just like dest.
          return Status::IOError("Unable to replace empty container: '", dest.all, "'");
        }
        // Delete the source container now that we know it's empty.
        Blobs::DeleteBlobContainerOptions options;
        options.AccessConditions.LeaseId = src_lease_guard.LeaseId();
        DCHECK(dest_lease_guard.has_value());
        // Make sure lease on dest is still valid before deleting src. This is to ensure
        // the destination container is not deleted by another process/client before
        // Move() returns.
        if (!dest_lease_guard->StillValidFor(kTimeNeededForContainerDeletion)) {
          return Status::IOError("Unable to replace empty container: '", dest.all, "'. ",
                                 "Preparation for the operation took too long and a "
                                 "container lease expired.");
        }
        try {
          src_lease_guard.BreakBeforeDeletion(kTimeNeededForContainerDeletion);
          src_container_client.Delete(options);
          src_lease_guard.Forget();
        } catch (const Storage::StorageException& exception) {
          return ExceptionToStatus(exception, "Failed to delete empty container: '",
                                   src.container, "': ", src_container_client.GetUrl());
        }
      } catch (const Storage::StorageException& exception) {
        return ExceptionToStatus(exception, "Unable to replace empty container: '",
                                 dest.all, "': ", dest_container_client.GetUrl());
      }
    }
    return Status::OK();
  }

  Status MoveContainerToPath(const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && src.path.empty());
    DCHECK(!dest.container.empty() && !dest.path.empty());
    // Check Move pre-condition 1 -- `src` must exist.
    auto container_client = GetBlobContainerClient(src.container);
    ARROW_ASSIGN_OR_RAISE(auto src_info,
                          GetContainerPropsAsFileInfo(src, container_client));
    if (src_info.type() == FileType::NotFound) {
      return PathNotFound(src);
    }
    // Check Move pre-condition 2.
    if (src.container == dest.container) {
      return InvalidDirMoveToSubdir(src, dest);
    }
    // Instead of checking more pre-conditions, we just abort with a
    // NotImplemented status.
    return CrossContainerMoveNotImplemented(src, dest);
  }

  Status CreateContainerFromPath(const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && !src.path.empty());
    DCHECK(!dest.empty() && dest.path.empty());
    ARROW_ASSIGN_OR_RAISE(auto src_file_info, GetFileInfoOfPathWithinContainer(src));
    switch (src_file_info.type()) {
      case FileType::Unknown:
      case FileType::NotFound:
        return PathNotFound(src);
      case FileType::File:
        return Status::Invalid(
            "Creating files at '/' is not possible, only directories.");
      case FileType::Directory:
        break;
    }
    if (src.container == dest.container) {
      return InvalidDirMoveToSubdir(src, dest);
    }
    return CrossContainerMoveNotImplemented(src, dest);
  }

  Status MovePathWithDataLakeAPI(
      const DataLake::DataLakeFileSystemClient& src_adlfs_client,
      const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && !src.path.empty());
    DCHECK(!dest.container.empty() && !dest.path.empty());
    const auto src_path = std::string{internal::RemoveTrailingSlash(src.path)};
    const auto dest_path = std::string{internal::RemoveTrailingSlash(dest.path)};

    // Ensure that src exists and, if path has a trailing slash, that it's a directory.
    ARROW_ASSIGN_OR_RAISE(auto src_lease_client,
                          AcquireBlobLease(src, kLeaseDuration, /*allow_missing=*/false));
    LeaseGuard src_lease_guard{std::move(src_lease_client), kLeaseDuration};
    // It might be necessary to check src is a directory 0-3 times in this function,
    // so we use a lazy evaluation function to avoid redundant calls to GetFileInfo().
    std::optional<bool> src_is_dir_opt{};
    auto src_is_dir_lazy = [&]() -> Result<bool> {
      if (src_is_dir_opt.has_value()) {
        return *src_is_dir_opt;
      }
      ARROW_ASSIGN_OR_RAISE(
          auto src_info, GetFileInfo(src_adlfs_client, src, src_lease_guard.LeaseId()));
      src_is_dir_opt = src_info.type() == FileType::Directory;
      return *src_is_dir_opt;
    };
    // src must be a directory if it has a trailing slash.
    if (internal::HasTrailingSlash(src.path)) {
      ARROW_ASSIGN_OR_RAISE(auto src_is_dir, src_is_dir_lazy());
      if (!src_is_dir) {
        return NotADir(src);
      }
    }
    // The Azure SDK and the backend don't perform many important checks, so we have to
    // do them ourselves. Additionally, based on many tests on a default-configuration
    // storage account, if the destination is an empty directory, the rename operation
    // will most likely fail due to a timeout. Providing both leases -- to source and
    // destination -- seems to have made things work.
    ARROW_ASSIGN_OR_RAISE(auto dest_lease_client,
                          AcquireBlobLease(dest, kLeaseDuration, /*allow_missing=*/true));
    std::optional<LeaseGuard> dest_lease_guard;
    if (dest_lease_client) {
      dest_lease_guard.emplace(std::move(dest_lease_client), kLeaseDuration);
      // Perform all the checks on dest (and src) before proceeding with the rename.
      auto dest_adlfs_client = GetFileSystemClient(dest.container);
      ARROW_ASSIGN_OR_RAISE(auto dest_info, GetFileInfo(dest_adlfs_client, dest,
                                                        dest_lease_guard->LeaseId()));
      if (dest_info.type() == FileType::Directory) {
        ARROW_ASSIGN_OR_RAISE(auto src_is_dir, src_is_dir_lazy());
        if (!src_is_dir) {
          // If src is a regular file, complain that dest is a directory
          // like POSIX rename() does.
          return internal::IsADir(dest.all);
        }
      } else {
        if (internal::HasTrailingSlash(dest.path)) {
          return NotADir(dest);
        }
      }
    } else {
      // If the destination has trailing slash, we would have to check that it's a
      // directory, but since it doesn't exist we must return PathNotFound...
      if (internal::HasTrailingSlash(dest.path)) {
        // ...unless the src is a directory, in which case we can proceed with the rename.
        ARROW_ASSIGN_OR_RAISE(auto src_is_dir, src_is_dir_lazy());
        if (!src_is_dir) {
          return PathNotFound(dest);
        }
      }
    }

    // Now that src and dest are validated, make sure they are on the same filesystem.
    if (src.container != dest.container) {
      return CrossContainerMoveNotImplemented(src, dest);
    }

    try {
      // NOTE: The Azure SDK provides a RenameDirectory() function, but the
      // implementation is the same as RenameFile() with the only difference being
      // the type of the returned object (DataLakeDirectoryClient vs DataLakeFileClient).
      //
      // If we call RenameDirectory() and the source is a file, no error would
      // be returned and the file would be renamed just fine (!).
      //
      // Since we don't use the returned object, we can just use RenameFile() for both
      // files and directories. Ideally, the SDK would simply expose the PathClient
      // that it uses internally for both files and directories.
      DataLake::RenameFileOptions options;
      options.DestinationFileSystem = dest.container;
      options.SourceAccessConditions.LeaseId = src_lease_guard.LeaseId();
      if (dest_lease_guard.has_value()) {
        options.AccessConditions.LeaseId = dest_lease_guard->LeaseId();
      }
      src_lease_guard.BreakBeforeDeletion(kTimeNeededForFileOrDirectoryRename);
      src_adlfs_client.RenameFile(src_path, dest_path, options);
      src_lease_guard.Forget();
    } catch (const Storage::StorageException& exception) {
      // https://learn.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/create
      if (exception.StatusCode == Http::HttpStatusCode::NotFound) {
        if (exception.ErrorCode == "PathNotFound") {
          return PathNotFound(src);
        }
        // "FilesystemNotFound" could be triggered by the source or destination filesystem
        // not existing, but since we already checked the source filesystem exists (and
        // hold a lease to it), we can assume the destination filesystem is the one the
        // doesn't exist.
        if (exception.ErrorCode == "FilesystemNotFound" ||
            exception.ErrorCode == "RenameDestinationParentPathNotFound") {
          return DestinationParentPathNotFound(dest);
        }
      } else if (exception.StatusCode == Http::HttpStatusCode::Conflict &&
                 exception.ErrorCode == "PathAlreadyExists") {
        // "PathAlreadyExists" is only produced when the destination exists and is a
        // non-empty directory, so we produce the appropriate error.
        return NotEmpty(dest);
      }
      return ExceptionToStatus(exception, "Failed to rename '", src.all, "' to '",
                               dest.all, "': ", src_adlfs_client.GetUrl());
    }
    return Status::OK();
  }

  Status MovePathUsingBlobsAPI(const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && !src.path.empty());
    DCHECK(!dest.container.empty() && !dest.path.empty());
    if (src.container != dest.container) {
      ARROW_ASSIGN_OR_RAISE(auto src_file_info, GetFileInfoOfPathWithinContainer(src));
      if (src_file_info.type() == FileType::NotFound) {
        return PathNotFound(src);
      }
      return CrossContainerMoveNotImplemented(src, dest);
    }
    return Status::NotImplemented(
        "FileSystem::Move() is not implemented for Azure Storage accounts "
        "without Hierarchical Namespace support (see arrow/issues/40405).");
  }

  Status MovePath(const AzureLocation& src, const AzureLocation& dest) {
    DCHECK(!src.container.empty() && !src.path.empty());
    DCHECK(!dest.container.empty() && !dest.path.empty());
    auto src_adlfs_client = GetFileSystemClient(src.container);
    ARROW_ASSIGN_OR_RAISE(auto hns_support,
                          HierarchicalNamespaceSupport(src_adlfs_client));
    if (hns_support == HNSSupport::kContainerNotFound) {
      return PathNotFound(src);
    }
    if (hns_support == HNSSupport::kEnabled) {
      return MovePathWithDataLakeAPI(src_adlfs_client, src, dest);
    }
    DCHECK_EQ(hns_support, HNSSupport::kDisabled);
    return MovePathUsingBlobsAPI(src, dest);
  }

  Status CopyFile(const AzureLocation& src, const AzureLocation& dest) {
    RETURN_NOT_OK(ValidateFileLocation(src));
    RETURN_NOT_OK(ValidateFileLocation(dest));
    if (src == dest) {
      return Status::OK();
    }
    auto src_url = GetBlobClient(src.container, src.path).GetUrl();
    auto dest_blob_client = GetBlobClient(dest.container, dest.path);
    if (!dest.path.empty()) {
      auto dest_parent = dest.parent();
      if (!dest_parent.path.empty()) {
        auto dest_container_client = GetBlobContainerClient(dest_parent.container);
        ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(dest_container_client, dest_parent));
        if (info.type() == FileType::File) {
          return NotADir(dest_parent);
        }
      }
    }
    try {
      // We use StartCopyFromUri instead of CopyFromUri because it supports blobs larger
      // than 256 MiB and it doesn't require generating a SAS token to authenticate
      // reading a source blob in the same storage account.
      auto copy_operation = dest_blob_client.StartCopyFromUri(src_url);
      // For large blobs, the copy operation may be slow so we need to poll until it
      // completes. We use a polling interval of 1 second.
      copy_operation.PollUntilDone(std::chrono::milliseconds(1000));
    } catch (const Storage::StorageException& exception) {
      // StartCopyFromUri failed or a GetProperties call inside PollUntilDone failed.
      return ExceptionToStatus(
          exception, "Failed to start blob copy or poll status of ongoing copy. (",
          src_url, " -> ", dest_blob_client.GetUrl(), ")");
    } catch (const Azure::Core::RequestFailedException& exception) {
      // A GetProperties call inside PollUntilDone returned a failed CopyStatus.
      return ExceptionToStatus(exception, "Failed to copy blob. (", src_url, " -> ",
                               dest_blob_client.GetUrl(), ")");
    }
    return Status::OK();
  }
};

std::atomic<LeaseGuard::SteadyClock::time_point> LeaseGuard::latest_known_expiry_time_ =
    SteadyClock::time_point{SteadyClock::duration::zero()};

AzureFileSystem::AzureFileSystem(std::unique_ptr<Impl>&& impl)
    : FileSystem(impl->io_context()), impl_(std::move(impl)) {
  default_async_is_sync_ = false;
}

void AzureFileSystem::ForceCachedHierarchicalNamespaceSupport(int hns_support) {
  impl_->ForceCachedHierarchicalNamespaceSupport(hns_support);
}

Result<std::shared_ptr<AzureFileSystem>> AzureFileSystem::Make(
    const AzureOptions& options, const io::IOContext& io_context) {
  ARROW_ASSIGN_OR_RAISE(auto impl, AzureFileSystem::Impl::Make(options, io_context));
  return std::shared_ptr<AzureFileSystem>(new AzureFileSystem(std::move(impl)));
}

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
  if (location.container.empty()) {
    DCHECK(location.path.empty());
    // Root directory of the storage account.
    return FileInfo{"", FileType::Directory};
  }
  if (location.path.empty()) {
    // We have a container, but no path within the container.
    // The container itself represents a directory.
    auto container_client = impl_->GetBlobContainerClient(location.container);
    return GetContainerPropsAsFileInfo(location, container_client);
  }
  return impl_->GetFileInfoOfPathWithinContainer(location);
}

Result<FileInfoVector> AzureFileSystem::GetFileInfo(const FileSelector& select) {
  Core::Context context;
  Azure::Nullable<int32_t> page_size_hint;  // unspecified
  FileInfoVector results;
  RETURN_NOT_OK(
      impl_->GetFileInfoWithSelector(context, page_size_hint, select, &results));
  return {std::move(results)};
}

Status AzureFileSystem::CreateDir(const std::string& path, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  if (location.container.empty()) {
    return Status::Invalid("CreateDir requires a non-empty path.");
  }

  auto container_client = impl_->GetBlobContainerClient(location.container);
  if (location.path.empty()) {
    // If the path is just the container, the parent (root) trivially exists,
    // and the CreateDir operation comes down to just creating the container.
    return CreateContainerIfNotExists(location.container, container_client);
  }

  auto adlfs_client = impl_->GetFileSystemClient(location.container);
  ARROW_ASSIGN_OR_RAISE(auto hns_support,
                        impl_->HierarchicalNamespaceSupport(adlfs_client));
  if (hns_support == HNSSupport::kContainerNotFound) {
    if (!recursive) {
      auto parent = location.parent();
      return PathNotFound(parent);
    }
    RETURN_NOT_OK(CreateContainerIfNotExists(location.container, container_client));
    // Perform a second check for HNS support after creating the container.
    ARROW_ASSIGN_OR_RAISE(hns_support, impl_->HierarchicalNamespaceSupport(adlfs_client));
    if (hns_support == HNSSupport::kContainerNotFound) {
      // We only get kContainerNotFound if we are unable to read the properties of the
      // container we just created. This is very unlikely, but theoretically possible in
      // a concurrent system, so the error is handled to avoid infinite recursion.
      return Status::IOError("Unable to read properties of a newly created container: ",
                             location.container, ": " + container_client.GetUrl());
    }
  }
  // CreateDirOnFileSystem and CreateDirOnContainer can handle the container
  // not existing which is useful and necessary here since the only reason
  // a container was created above was to check for HNS support when it wasn't
  // cached yet.
  if (hns_support == HNSSupport::kEnabled) {
    return impl_->CreateDirOnFileSystem(adlfs_client, location, recursive);
  }
  DCHECK_EQ(hns_support, HNSSupport::kDisabled);
  return impl_->CreateDirOnContainer(container_client, location, recursive);
}

Status AzureFileSystem::DeleteDir(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  if (location.container.empty()) {
    return Status::Invalid("DeleteDir requires a non-empty path.");
  }
  if (location.path.empty()) {
    auto container_client = impl_->GetBlobContainerClient(location.container);
    return impl_->DeleteContainer(container_client, location);
  }

  auto adlfs_client = impl_->GetFileSystemClient(location.container);
  ARROW_ASSIGN_OR_RAISE(auto hns_support,
                        impl_->HierarchicalNamespaceSupport(adlfs_client));
  if (hns_support == HNSSupport::kContainerNotFound) {
    return PathNotFound(location);
  }
  if (hns_support == HNSSupport::kEnabled) {
    return impl_->DeleteDirOnFileSystem(adlfs_client, location, /*recursive=*/true,
                                        /*require_dir_to_exist=*/true);
  }
  DCHECK_EQ(hns_support, HNSSupport::kDisabled);
  auto container_client = impl_->GetBlobContainerClient(location.container);
  return impl_->DeleteDirContentsOnContainer(container_client, location,
                                             /*require_dir_to_exist=*/true,
                                             /*preserve_dir_marker_blob=*/false,
                                             "DeleteDir");
}

Status AzureFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  if (location.container.empty()) {
    return internal::InvalidDeleteDirContents(location.all);
  }

  auto adlfs_client = impl_->GetFileSystemClient(location.container);
  ARROW_ASSIGN_OR_RAISE(auto hns_support,
                        impl_->HierarchicalNamespaceSupport(adlfs_client));
  if (hns_support == HNSSupport::kContainerNotFound) {
    return missing_dir_ok ? Status::OK() : PathNotFound(location);
  }

  if (hns_support == HNSSupport::kEnabled) {
    return impl_->DeleteDirContentsOnFileSystem(adlfs_client, location, missing_dir_ok);
  }
  auto container_client = impl_->GetBlobContainerClient(location.container);
  return impl_->DeleteDirContentsOnContainer(container_client, location,
                                             /*require_dir_to_exist=*/!missing_dir_ok,
                                             /*preserve_dir_marker_blob=*/true,
                                             "DeleteDirContents");
}

Status AzureFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("Cannot delete all Azure Blob Storage containers");
}

Status AzureFileSystem::DeleteFile(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto location, AzureLocation::FromString(path));
  if (location.container.empty()) {
    return Status::Invalid("DeleteFile requires a non-empty path.");
  }
  auto container_client = impl_->GetBlobContainerClient(location.container);
  if (location.path.empty()) {
    // Container paths (locations w/o path) are either not found or represent directories.
    ARROW_ASSIGN_OR_RAISE(auto container_info,
                          GetContainerPropsAsFileInfo(location, container_client));
    return container_info.IsDirectory() ? NotAFile(location) : PathNotFound(location);
  }
  auto adlfs_client = impl_->GetFileSystemClient(location.container);
  ARROW_ASSIGN_OR_RAISE(auto hns_support,
                        impl_->HierarchicalNamespaceSupport(adlfs_client));
  if (hns_support == HNSSupport::kContainerNotFound) {
    return PathNotFound(location);
  }
  if (hns_support == HNSSupport::kEnabled) {
    return impl_->DeleteFileOnFileSystem(adlfs_client, location,
                                         /*require_file_to_exist=*/true);
  }
  return impl_->DeleteFileOnContainer(container_client, location,
                                      /*require_file_to_exist=*/true,
                                      /*operation=*/"DeleteFile");
}

Status AzureFileSystem::Move(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto src_location, AzureLocation::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dest_location, AzureLocation::FromString(dest));
  if (src_location.container.empty()) {
    return Status::Invalid("Move requires a non-empty source path.");
  }
  if (dest_location.container.empty()) {
    return Status::Invalid("Move requires a non-empty destination path.");
  }
  if (src_location.path.empty()) {
    if (dest_location.path.empty()) {
      return impl_->RenameContainer(src_location, dest_location);
    }
    return impl_->MoveContainerToPath(src_location, dest_location);
  }
  if (dest_location.path.empty()) {
    return impl_->CreateContainerFromPath(src_location, dest_location);
  }
  return impl_->MovePath(src_location, dest_location);
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

Result<std::string> AzureFileSystem::PathFromUri(const std::string& uri_string) const {
  /// We can not use `internal::PathFromUriHelper` here because for Azure we have to
  /// support different URI schemes where the authority is handled differently.
  /// Example (both should yield the same path `container/some/path`):
  ///   - (1) abfss://storageacc.blob.core.windows.net/container/some/path
  ///   - (2) abfss://acc:pw@container/some/path
  /// The authority handling is different with these two URIs. (1) requires no prepending
  /// of the authority to the path, while (2) requires to preprend the authority to the
  /// path.
  std::string path;
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  RETURN_NOT_OK(AzureOptions::FromUri(uri, &path));

  std::vector<std::string> supported_schemes = {"abfs", "abfss"};
  const auto scheme = uri.scheme();
  if (std::find(supported_schemes.begin(), supported_schemes.end(), scheme) ==
      supported_schemes.end()) {
    std::string expected_schemes =
        ::arrow::internal::JoinStrings(supported_schemes, ", ");
    return Status::Invalid("The filesystem expected a URI with one of the schemes (",
                           expected_schemes, ") but received ", uri_string);
  }

  return path;
}

}  // namespace arrow::fs

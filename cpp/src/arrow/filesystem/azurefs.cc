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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

#include <azure/core/credentials/credentials.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/datalake.hpp>

#include "arrow/util/windows_fixup.h"

#include "arrow/buffer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::Uri;

namespace fs {

static const char kSep = '/';

// -----------------------------------------------------------------------
// AzureOptions implementation

AzureOptions::AzureOptions() {}

Result<std::string> AzureOptions::GetAccountNameFromConnectionString(
    const std::string& connection_string) {
  std::string text = "AccountName=";
  auto pos_text = connection_string.find(text);
  if (pos_text == std::string::npos) {
    return Status::Invalid(
        "Cannot find account name in Azure Blob Storage connection string: '",
        connection_string, "'");
  }
  auto pos_semicolon = connection_string.find(';');
  pos_semicolon = connection_string.find(';', pos_semicolon + 1);
  if (pos_semicolon == std::string::npos) {
    return Status::Invalid("Invalid Azure Blob Storage connection string: '",
                           connection_string, "' passed");
  }
  std::string account_name =
      connection_string.substr(pos_text + text.size(), pos_semicolon);
  return account_name;
}

Status AzureOptions::ConfigureAnonymousCredentials(const std::string& account_name) {
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  credentials_kind = AzureCredentialsKind::Anonymous;
  return Status::OK();
}

Status AzureOptions::ConfigureAccountKeyCredentials(const std::string& account_name,
                                                    const std::string& account_key) {
  if (this->is_azurite) {
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

Status AzureOptions::ConfigureConnectionStringCredentials(
    const std::string& connection_string_key) {
  ARROW_ASSIGN_OR_RAISE(auto account_name,
                        GetAccountNameFromConnectionString(connection_string_key));
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  connection_string = connection_string_key;
  credentials_kind = AzureCredentialsKind::ConnectionString;
  return Status::OK();
}

Status AzureOptions::ConfigureServicePrincipleCredentials(
    const std::string& account_name, const std::string& tenant_id,
    const std::string& client_id, const std::string& client_secret) {
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  service_principle_credentials_provider =
      std::make_shared<Azure::Identity::ClientSecretCredential>(tenant_id, client_id,
                                                                client_secret);
  credentials_kind = AzureCredentialsKind::ServicePrincipleCredentials;
  return Status::OK();
}

Status AzureOptions::ConfigureSasCredentials(const std::string& uri) {
  Uri url;
  RETURN_NOT_OK(url.Parse(uri));
  sas_token = "?" + url.query_string();
  account_blob_url = url.scheme() + "://" + url.host() + kSep;
  account_dfs_url = std::regex_replace(account_blob_url, std::regex("[.]blob"), ".dfs");
  credentials_kind = AzureCredentialsKind::Sas;
  return Status::OK();
}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return (scheme == other.scheme && account_dfs_url == other.account_dfs_url &&
          account_blob_url == other.account_blob_url &&
          credentials_kind == other.credentials_kind);
}

Result<AzureOptions> AzureOptions::FromAnonymous(const std::string& account_name) {
  AzureOptions options;
  RETURN_NOT_OK(options.ConfigureAnonymousCredentials(account_name));
  return options;
}

Result<AzureOptions> AzureOptions::FromAccountKey(const std::string& account_name,
                                                  const std::string& account_key) {
  AzureOptions options;
  RETURN_NOT_OK(options.ConfigureAccountKeyCredentials(account_name, account_key));
  return options;
}

Result<AzureOptions> AzureOptions::FromConnectionString(
    const std::string& connection_string) {
  AzureOptions options;
  RETURN_NOT_OK(options.ConfigureConnectionStringCredentials(connection_string));
  return options;
}

Result<AzureOptions> AzureOptions::FromServicePrincipleCredential(
    const std::string& account_name, const std::string& tenant_id,
    const std::string& client_id, const std::string& client_secret) {
  AzureOptions options;
  RETURN_NOT_OK(options.ConfigureServicePrincipleCredentials(account_name, tenant_id,
                                                             client_id, client_secret));
  return options;
}

Result<AzureOptions> AzureOptions::FromSas(const std::string& uri) {
  AzureOptions options;
  RETURN_NOT_OK(options.ConfigureSasCredentials(uri));
  return options;
}

Result<AzureOptions> AzureOptions::FromUri(const std::string& uri_string,
                                           std::string* out_path) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
}

Result<AzureOptions> AzureOptions::FromUri(const Uri& uri, std::string* out_path) {
  // uri =
  // https://accountName.dfs.core.windows.net/pathToBlob/?sas_token_key=sas_token_value
  AzureOptions options;
  // host = accountName.dfs.core.windows.net
  const auto host = uri.host();
  // path_to_blob = /pathToBlob/
  const auto path_to_blob = uri.path();
  std::string account_name;
  if (host.empty()) {
    return Status::IOError("Missing container in Azure Blob Storage URI: '",
                           uri.ToString(), "'");
  }
  auto pos = host.find('.');
  if (pos == std::string::npos) {
    return Status::IOError("Missing container in Azure Blob Storage URI: '",
                           uri.ToString(), "'");
  }
  std::string full_path = path_to_blob;
  // account_name = accountName
  account_name = host.substr(0, pos);
  if (full_path.empty()) {
    full_path = account_name;
  } else {
    if (full_path[0] != '/') {
      return Status::IOError("Azure Blob Storage URI should be absolute, not relative");
    }
    // full_path = accountName/pathToBlob/
    full_path = account_name + path_to_blob;
  }
  if (out_path != nullptr) {
    *out_path = std::string(internal::RemoveTrailingSlash(full_path));
  }
  // scheme = https
  options.scheme = uri.scheme();
  // query_string = sas_token_key=sas_token_value
  const auto query_string = uri.query_string();
  if (!query_string.empty()) {
    // Accepted Uri =
    // https://accountName.dfs.core.windows.net/pathToBlob/?sas_token_key=sas_token_value
    RETURN_NOT_OK(options.ConfigureSasCredentials(uri.scheme() + "://" + host +
                                                  path_to_blob + "?" + query_string));
  } else {
    RETURN_NOT_OK(options.ConfigureAnonymousCredentials(account_name));
  }
  return options;
}

namespace {

struct AzurePath {
  std::string full_path;
  std::string container;
  std::string path_to_file;
  std::vector<std::string> path_to_file_parts;

  static Result<AzurePath> FromString(const std::string& s) {
    // https://synapsemladlsgen2.dfs.core.windows.net/synapsemlfs/testdir/testfile.txt
    // container = synapsemlfs
    // account_name = synapsemladlsgen2
    // path_to_file = testdir/testfile.txt
    // path_to_file_parts = [testdir, testfile.txt]

    // Expected input here => s = synapsemlfs/testdir/testfile.txt,
    // http://127.0.0.1/accountName/pathToBlob
    auto src = internal::RemoveTrailingSlash(s);
    if (arrow::internal::StartsWith(src, "https://127.0.0.1") || arrow::internal::StartsWith(src, "http://127.0.0.1")) {
      RETURN_NOT_OK(FromLocalHostString(&src));
    }
    auto input_path = std::string(src.data());
    if (internal::IsLikelyUri(input_path)) {
      RETURN_NOT_OK(ExtractBlobPath(&input_path));
      src = std::string_view(input_path);
    }
    src = internal::RemoveLeadingSlash(src);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::IOError("Path cannot start with a separator ('", input_path, "')");
    }
    if (first_sep == std::string::npos) {
      return AzurePath{std::string(src), std::string(src), "", {}};
    }
    AzurePath path;
    path.full_path = std::string(src);
    path.container = std::string(src.substr(0, first_sep));
    path.path_to_file = std::string(src.substr(first_sep + 1));
    path.path_to_file_parts = internal::SplitAbstractPath(path.path_to_file);
    RETURN_NOT_OK(Validate(&path));
    return path;
  }

  static Status FromLocalHostString(std::string_view* src) {
    // src = http://127.0.0.1:10000/accountName/pathToBlob
    Uri uri;
    RETURN_NOT_OK(uri.Parse(src->data()));
    *src = internal::RemoveLeadingSlash(uri.path());
    if (src->empty()) {
      return Status::IOError("Missing account name in Azure Blob Storage URI");
    }
    auto first_sep = src->find_first_of(kSep);
    if (first_sep != std::string::npos) {
      *src = src->substr(first_sep + 1);
    } else {
      *src = "";
    }
    return Status::OK();
  }

  // Removes scheme, host and port from the uri
  static Status ExtractBlobPath(std::string* src) {
    Uri uri;
    RETURN_NOT_OK(uri.Parse(*src));
    *src = uri.path();
    return Status::OK();
  }

  static Status Validate(const AzurePath* path) {
    auto status = internal::ValidateAbstractPathParts(path->path_to_file_parts);
    if (!status.ok()) {
      return Status::Invalid(status.message(), " in path ", path->full_path);
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
      parent.full_path = parent.container + kSep + parent.path_to_file;
    }
    return parent;
  }

  bool has_parent() const { return !path_to_file.empty(); }

  bool empty() const { return container.empty() && path_to_file.empty(); }

  bool operator==(const AzurePath& other) const {
    return container == other.container && path_to_file == other.path_to_file;
  }
};

template <typename ObjectResult>
std::shared_ptr<const KeyValueMetadata> GetObjectMetadata(const ObjectResult& result) {
  auto md = std::make_shared<KeyValueMetadata>();
  for (auto prop : result) {
    md->Append(prop.first, prop.second);
  }
  return md;
}

template <typename T>
Result<std::shared_ptr<T>> InitServiceClient(const AzureOptions& options,
                                             const std::string& url) {
  std::shared_ptr<T> client;
  if (options.credentials_kind == AzureCredentialsKind::StorageCredentials) {
    client = std::make_shared<T>(url, options.storage_credentials_provider);
  } else if (options.credentials_kind ==
             AzureCredentialsKind::ServicePrincipleCredentials) {
    client = std::make_shared<T>(url, options.service_principle_credentials_provider);
  } else if (options.credentials_kind == AzureCredentialsKind::ConnectionString) {
    client =
        std::make_shared<T>(T::CreateFromConnectionString(options.connection_string));
  } else if (options.credentials_kind == AzureCredentialsKind::Sas) {
    client = std::make_shared<T>(url + options.sas_token);
  } else {
    client = std::make_shared<T>(url);
  }
  return client;
}

template <typename T>
Result<std::shared_ptr<T>> InitPathClient(const AzureOptions& options,
                                          const std::string& path,
                                          const std::string& container,
                                          const std::string& path_to_file) {
  std::shared_ptr<T> client;
  if (options.credentials_kind == AzureCredentialsKind::StorageCredentials) {
    client = std::make_shared<T>(path, options.storage_credentials_provider);
  } else if (options.credentials_kind ==
             AzureCredentialsKind::ServicePrincipleCredentials) {
    client = std::make_shared<T>(path, options.service_principle_credentials_provider);
  } else if (options.credentials_kind == AzureCredentialsKind::ConnectionString) {
    client = std::make_shared<T>(T::CreateFromConnectionString(options.connection_string,
                                                               container, path_to_file));
  } else if (options.credentials_kind == AzureCredentialsKind::Sas) {
    auto src = internal::RemoveLeadingSlash(path);
    auto first_sep = src.find("dfs.core.windows.net");
    std::string p;
    if (first_sep != std::string::npos) {
      p = std::string(src.substr(0, first_sep)) + "blob" +
          std::string(src.substr(first_sep + 3));
      client = std::make_shared<T>(p + options.sas_token);
    } else {
      client = std::make_shared<T>(path);
    }
  } else {
    client = std::make_shared<T>(path);
  }
  return client;
}

class ObjectInputFile final : public io::RandomAccessFile {
 public:
  ObjectInputFile(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>& path_client,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient>& file_client,
      const io::IOContext& io_context, const AzurePath& path, int64_t size = kNoSize)
      : path_client_(std::move(path_client)),
        file_client_(std::move(file_client)),
        io_context_(io_context),
        path_(path),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = path_client_->GetProperties();
      if (properties.Value.IsDirectory) {
        return ::arrow::fs::internal::NotAFile(path_.full_path);
      }
      content_length_ = properties.Value.FileSize;
      metadata_ = GetObjectMetadata(properties.Value.Metadata);
      return Status::OK();
    } catch (const Azure::Storage::StorageException& exception) {
      return ::arrow::fs::internal::PathNotFound(path_.full_path);
    }
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
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
    path_client_ = nullptr;
    file_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;
    Azure::Core::Http::HttpRange range;
    range.Offset = position;
    range.Length = nbytes;
    download_options.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    try {
      auto result =
          file_client_
              ->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, download_options)
              .Value;
      return result.ContentRange.Length.Value();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes, io_context_.pool()));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return std::move(buf);
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

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client_;
  const io::IOContext io_context_;
  AzurePath path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

class ObjectOutputStream final : public io::OutputStream {
 public:
  ObjectOutputStream(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient>& file_client,
      std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient>& blob_client,
      const bool is_hierarchical_namespace_enabled, const io::IOContext& io_context,
      const AzurePath& path, const std::shared_ptr<const KeyValueMetadata>& metadata)
      : file_client_(std::move(file_client)),
        blob_client_(std::move(blob_client)),
        is_hierarchical_namespace_enabled_(is_hierarchical_namespace_enabled),
        io_context_(io_context),
        path_(path) {}

  ~ObjectOutputStream() override {
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
    if (is_hierarchical_namespace_enabled_) {
      try {
        file_client_->DeleteIfExists();
        file_client_->CreateIfNotExists();
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
      try {
        std::string s = "";
        file_client_->UploadFrom(
            const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(s.data())), s.size());
        content_length_ = 0;
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    }
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    file_client_ = nullptr;
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    file_client_ = nullptr;
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
    return DoWrite(buffer->data(), buffer->size(), buffer);
  }

  Status Write(const void* data, int64_t nbytes) override {
    return DoWrite(data, nbytes);
  }

  Status DoWrite(const void* data, int64_t nbytes,
                 std::shared_ptr<Buffer> owned_buffer = nullptr) {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    if (is_hierarchical_namespace_enabled_) {
      try {
        auto buffer_stream = std::make_unique<Azure::Core::IO::MemoryBodyStream>(
            Azure::Core::IO::MemoryBodyStream(
                const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(data)), nbytes));
        if (buffer_stream->Length() == 0) {
          return Status::OK();
        }
        auto result = file_client_->Append(*buffer_stream, pos_);
        pos_ += nbytes;
        file_client_->Flush(pos_);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
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
    }
    content_length_ += nbytes;
    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    if (is_hierarchical_namespace_enabled_) {
      try {
        file_client_->Flush(content_length_);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
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
    }
    return Status::OK();
  }

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client_;
  std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client_;
  const bool is_hierarchical_namespace_enabled_;
  const io::IOContext io_context_;
  const AzurePath path_;

  bool closed_ = true;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
};

class ObjectAppendStream final : public io::OutputStream {
 public:
  ObjectAppendStream(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>& path_client,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient>& file_client,
      std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient>& blob_client,
      const bool is_hierarchical_namespace_enabled, const io::IOContext& io_context,
      const AzurePath& path, const std::shared_ptr<const KeyValueMetadata>& metadata)
      : path_client_(std::move(path_client)),
        file_client_(std::move(file_client)),
        blob_client_(std::move(blob_client)),
        is_hierarchical_namespace_enabled_(is_hierarchical_namespace_enabled),
        io_context_(io_context),
        path_(path) {}

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
      auto properties = path_client_->GetProperties();
      if (properties.Value.IsDirectory) {
        return ::arrow::fs::internal::NotAFile(path_.full_path);
      }
      content_length_ = properties.Value.FileSize;
      pos_ = content_length_;
    } catch (const Azure::Storage::StorageException& exception) {
      // new file
      if (is_hierarchical_namespace_enabled_) {
        try {
          file_client_->CreateIfNotExists();
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
      } else {
        std::string s = "";
        try {
          file_client_->UploadFrom(
              const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(s.data())), s.size());
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
      }
      content_length_ = 0;
    }
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    path_client_ = nullptr;
    file_client_ = nullptr;
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    path_client_ = nullptr;
    file_client_ = nullptr;
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
    if (is_hierarchical_namespace_enabled_) {
      try {
        auto buffer_stream = std::make_unique<Azure::Core::IO::MemoryBodyStream>(
            Azure::Core::IO::MemoryBodyStream(
                const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(data)), nbytes));
        if (buffer_stream->Length() == 0) {
          return Status::OK();
        }
        auto result = file_client_->Append(*buffer_stream, pos_);
        pos_ += nbytes;
        file_client_->Flush(pos_);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
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
    }
    content_length_ += nbytes;
    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    if (is_hierarchical_namespace_enabled_) {
      try {
        file_client_->Flush(content_length_);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
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
    }
    return Status::OK();
  }

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client_;
  std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client_;
  const bool is_hierarchical_namespace_enabled_;
  const io::IOContext io_context_;
  const AzurePath path_;

  bool closed_ = true;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
};

TimePoint ToTimePoint(const Azure::DateTime& dt) {
  return std::chrono::time_point_cast<std::chrono::nanoseconds>(
      dt.operator std::chrono::system_clock::time_point());
}

void FileObjectToInfo(
    const Azure::Storage::Files::DataLake::Models::PathProperties& properties,
    FileInfo* info) {
  info->set_type(FileType::File);
  info->set_size(static_cast<int64_t>(properties.FileSize));
  info->set_mtime(ToTimePoint(properties.LastModified));
}

void PathInfoToFileInfo(const std::string& path, const FileType& type, const int64_t size,
                        const Azure::DateTime dt, FileInfo* info) {
  info->set_type(type);
  info->set_size(size);
  info->set_path(path);
  info->set_mtime(ToTimePoint(dt));
}

}  // namespace

// -----------------------------------------------------------------------
// Azure filesystem implementation

class AzureBlobFileSystem::Impl
    : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {
 public:
  io::IOContext io_context_;
  std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> gen1_client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2_client_;
  std::string dfs_endpoint_url_;
  std::string blob_endpoint_url_;
  bool is_hierarchical_namespace_enabled_;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    dfs_endpoint_url_ = options_.account_dfs_url;
    blob_endpoint_url_ = options_.account_blob_url;
    ARROW_ASSIGN_OR_RAISE(gen1_client_,
                          InitServiceClient<Azure::Storage::Blobs::BlobServiceClient>(
                              options_, blob_endpoint_url_));
    ARROW_ASSIGN_OR_RAISE(
        gen2_client_,
        InitServiceClient<Azure::Storage::Files::DataLake::DataLakeServiceClient>(
            options_, dfs_endpoint_url_));
    if (options_.is_azurite) {
      // gen1Client_->GetAccountInfo().Value.IsHierarchicalNamespaceEnabled throws error
      // in azurite
      is_hierarchical_namespace_enabled_ = false;
    } else {
      try {
        auto response = gen1_client_->GetAccountInfo();
        is_hierarchical_namespace_enabled_ =
            response.Value.IsHierarchicalNamespaceEnabled;
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    }
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

  // Create a container. Successful if container already exists.
  Status CreateContainer(const std::string& container) {
    auto file_system_client = gen2_client_->GetFileSystemClient(container);
    try {
      file_system_client.CreateIfNotExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  // Tests to see if a container exists
  Result<bool> ContainerExists(const std::string& container) {
    auto file_system_client = gen2_client_->GetFileSystemClient(container);
    try {
      auto properties = file_system_client.GetProperties();
      return true;
    } catch (const Azure::Storage::StorageException& exception) {
      return false;
    }
  }

  Result<bool> DirExists(const std::string& s) {
    std::string uri = s;
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(uri));
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                         options_, uri, path.container, path.path_to_file));
    try {
      auto properties = path_client->GetProperties();
      return properties.Value.IsDirectory;
    } catch (const Azure::Storage::StorageException& exception) {
      return false;
    }
  }

  Result<bool> FileExists(const std::string& s) {
    std::string uri = s;
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(uri));
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                         options_, uri, path.container, path.path_to_file));
    try {
      auto properties = path_client->GetProperties();
      return !properties.Value.IsDirectory;
    } catch (const Azure::Storage::StorageException& exception) {
      return false;
    }
  }

  Status CreateEmptyDir(const std::string& container,
                        const std::vector<std::string>& path) {
    if (path.empty()) {
      return CreateContainer(container);
    }
    auto directory_client =
        gen2_client_->GetFileSystemClient(container).GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directory_client = directory_client.GetSubdirectoryClient(*it);
      ++it;
    }
    try {
      directory_client.CreateIfNotExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status DeleteContainer(const std::string& container) {
    auto file_system_client = gen2_client_->GetFileSystemClient(container);
    try {
      file_system_client.DeleteIfExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status DeleteDir(const std::string& container, const std::vector<std::string>& path) {
    if (path.empty()) {
      return DeleteContainer(container);
    }
    auto file_system_client = gen2_client_->GetFileSystemClient(container);
    auto directory_client = file_system_client.GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directory_client = directory_client.GetSubdirectoryClient(*it);
      ++it;
    }
    ARROW_ASSIGN_OR_RAISE(auto response, FileExists(directory_client.GetUrl()));
    if (response) {
      return ::arrow::fs::internal::NotADir(directory_client.GetUrl());
    }
    ARROW_ASSIGN_OR_RAISE(response, DirExists(directory_client.GetUrl()));
    if (!response) {
      return ::arrow::fs::internal::NotADir(directory_client.GetUrl());
    }
    try {
      directory_client.DeleteRecursiveIfExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status DeleteFile(const std::string& container, const std::vector<std::string>& path) {
    if (path.empty()) {
      return Status::IOError("Cannot delete file, Invalid Azure Blob Storage file path");
    }
    if (!is_hierarchical_namespace_enabled_) {
      if (path.size() > 1) {
        return Status::IOError(
            "Cannot delete file, Invalid Azure Blob Storage file path,"
            " hierarchical namespace not enabled in storage account");
      }
      auto blob_client =
          gen1_client_->GetBlobContainerClient(container).GetBlobClient(path.front());
      ARROW_ASSIGN_OR_RAISE(auto response, FileExists(blob_client.GetUrl()));
      if (!response) {
        return ::arrow::fs::internal::NotAFile(blob_client.GetUrl());
      }
      try {
        blob_client.DeleteIfExists();
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
      return Status::OK();
    }
    auto file_system_client = gen2_client_->GetFileSystemClient(container);
    if (path.size() == 1) {
      auto file_client = file_system_client.GetFileClient(path.front());
      ARROW_ASSIGN_OR_RAISE(auto response, DirExists(file_client.GetUrl()));
      if (response) {
        return ::arrow::fs::internal::NotAFile(file_client.GetUrl());
      }
      ARROW_ASSIGN_OR_RAISE(response, FileExists(file_client.GetUrl()));
      if (!response) {
        return ::arrow::fs::internal::NotAFile(file_client.GetUrl());
      }
      try {
        file_client.DeleteIfExists();
      } catch (const Azure::Storage::StorageException& exception) {
        // Azurite throws an exception
        if (!options_.is_azurite) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
      }
      return Status::OK();
    }
    std::string file_name = path.back();
    auto directory_client = file_system_client.GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != (path.end() - 1)) {
      directory_client = directory_client.GetSubdirectoryClient(*it);
      ++it;
    }
    auto file_client = directory_client.GetFileClient(file_name);
    ARROW_ASSIGN_OR_RAISE(auto response, DirExists(file_client.GetUrl()));
    if (response) {
      return ::arrow::fs::internal::NotAFile(file_client.GetUrl());
    }
    ARROW_ASSIGN_OR_RAISE(response, FileExists(file_client.GetUrl()));
    if (!response) {
      return ::arrow::fs::internal::NotAFile(file_client.GetUrl());
    }
    try {
      file_client.DeleteIfExists();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status Move(const std::string& src, const std::string& dest) {
    ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
    ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));

    if (!is_hierarchical_namespace_enabled_) {
      return Status::IOError(
          "Cannot perform move operation, Hierarchical namespace not enabled in storage "
          "account");
    }
    if (src_path.empty() || src_path.path_to_file.empty()) {
      return ::arrow::fs::internal::PathNotFound(src_path.full_path);
    }
    if (dest_path.empty() || dest_path.path_to_file.empty()) {
      return ::arrow::fs::internal::PathNotFound(dest_path.full_path);
    }
    if (src_path == dest_path) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto file_exists,
                          FileExists(dfs_endpoint_url_ + src_path.full_path));
    ARROW_ASSIGN_OR_RAISE(auto dir_exists,
                          DirExists(dfs_endpoint_url_ + src_path.full_path));
    if (file_exists) {
      auto file_system_client = gen2_client_->GetFileSystemClient(src_path.container);
      auto path = src_path.path_to_file_parts;
      if (path.size() == 1) {
        try {
          file_system_client.RenameFile(path.front(), dest_path.path_to_file);
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
        return Status::OK();
      }
      auto directory_client = file_system_client.GetDirectoryClient(path.front());
      std::vector<std::string>::const_iterator it = path.begin();
      std::advance(it, 1);
      while (it != path.end()) {
        if ((it + 1) == path.end()) {
          break;
        }
        directory_client = directory_client.GetSubdirectoryClient(*it);
        ++it;
      }
      try {
        directory_client.RenameFile(it->data(), dest_path.path_to_file);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else if (dir_exists) {
      auto file_system_client = gen2_client_->GetFileSystemClient(src_path.container);
      auto path = src_path.path_to_file_parts;
      if (path.size() == 1) {
        try {
          file_system_client.RenameDirectory(path.front(), dest_path.path_to_file);
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
        return Status::OK();
      }
      auto directory_client = file_system_client.GetDirectoryClient(path.front());
      std::vector<std::string>::const_iterator it = path.begin();
      std::advance(it, 1);
      while (it != path.end()) {
        if ((it + 1) == path.end()) {
          break;
        }
        directory_client = directory_client.GetSubdirectoryClient(*it);
        ++it;
      }
      try {
        directory_client.RenameSubdirectory(it->data(), dest_path.path_to_file);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
      return ::arrow::fs::internal::PathNotFound(src_path.full_path);
    }
    return Status::OK();
  }

  Status CopyFile(const std::string& src, const std::string& dest) {
    ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
    ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));

    if (src_path.empty() || src_path.path_to_file.empty()) {
      return ::arrow::fs::internal::NotAFile(src_path.full_path);
    }
    if (dest_path.empty() || dest_path.path_to_file.empty()) {
      return ::arrow::fs::internal::PathNotFound(dest_path.full_path);
    }

    ARROW_ASSIGN_OR_RAISE(auto response,
                          FileExists(dfs_endpoint_url_ + src_path.full_path));
    if (!response) {
      return ::arrow::fs::internal::NotAFile(src_path.full_path);
    }

    ARROW_ASSIGN_OR_RAISE(response, DirExists(dfs_endpoint_url_ + dest_path.full_path));
    if (response) {
      return Status::IOError(
          "Cannot copy file, Invalid Azure Blob Storage destination path");
    }

    if (!is_hierarchical_namespace_enabled_) {
      if (src_path.path_to_file_parts.size() > 1 ||
          dest_path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid Azure Blob Storage path provided, "
            "hierarchical namespace not enabled in storage account");
      }
      if (dest_path.empty() || dest_path.path_to_file_parts.empty()) {
        return ::arrow::fs::internal::PathNotFound(dest_path.full_path);
      }
      if (src_path == dest_path) {
        return Status::OK();
      }
      auto container_client = gen1_client_->GetBlobContainerClient(dest_path.container);
      auto file_client = container_client.GetBlobClient(dest_path.path_to_file);
      try {
        file_client.StartCopyFromUri(blob_endpoint_url_ + src);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
      return Status::OK();
    }

    if (dest_path.has_parent()) {
      AzurePath parent_path = dest_path.parent();
      if (parent_path.path_to_file.empty()) {
        ARROW_ASSIGN_OR_RAISE(response, ContainerExists(parent_path.container));
        if (!response) {
          return Status::IOError("Cannot copy file '", src_path.full_path,
                                 "': parent directory of destination does not exist");
        }
      } else {
        ARROW_ASSIGN_OR_RAISE(response,
                              DirExists(dfs_endpoint_url_ + parent_path.full_path));
        if (!response) {
          return Status::IOError("Cannot copy file '", src_path.full_path,
                                 "': parent directory of destination does not exist");
        }
      }
    }
    if (src_path == dest_path) {
      return Status::OK();
    }
    auto container_client = gen1_client_->GetBlobContainerClient(dest_path.container);
    auto file_client = container_client.GetBlobClient(dest_path.path_to_file);
    try {
      if (options_.credentials_kind == AzureCredentialsKind::Sas) {
        file_client.StartCopyFromUri(blob_endpoint_url_ + src + options_.sas_token);
      } else {
        file_client.StartCopyFromUri(blob_endpoint_url_ + src);
      }
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status ListPaths(const std::string& container, const std::string& path,
                   std::vector<std::string>* children_dirs,
                   std::vector<std::string>* children_files,
                   const bool allow_not_found = false) {
    if (!is_hierarchical_namespace_enabled_) {
      try {
        auto paths = gen1_client_->GetBlobContainerClient(container).ListBlobs();
        for (auto p : paths.Blobs) {
          std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>
              path_client;
          ARROW_ASSIGN_OR_RAISE(
              path_client,
              InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                  options_, dfs_endpoint_url_ + container + "/" + p.Name, container,
                  p.Name));
          children_files->push_back(container + "/" + p.Name);
        }
      } catch (const Azure::Storage::StorageException& exception) {
        if (!allow_not_found) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
      }
      return Status::OK();
    }
    if (path.empty()) {
      try {
        auto paths = gen2_client_->GetFileSystemClient(container).ListPaths(false);
        for (auto p : paths.Paths) {
          std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>
              path_client;
          ARROW_ASSIGN_OR_RAISE(
              path_client,
              InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                  options_, dfs_endpoint_url_ + container + "/" + p.Name, container,
                  p.Name));
          if (path_client->GetProperties().Value.IsDirectory) {
            children_dirs->push_back(container + "/" + p.Name);
          } else {
            children_files->push_back(container + "/" + p.Name);
          }
        }
      } catch (const Azure::Storage::StorageException& exception) {
        if (!allow_not_found) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
      }
      return Status::OK();
    }
    std::vector<std::string> dirs = internal::SplitAbstractPath(path);
    try {
      Azure::Storage::Files::DataLake::DataLakeDirectoryClient dir_client =
          gen2_client_->GetFileSystemClient(container).GetDirectoryClient(dirs.front());
      for (auto dir = dirs.begin() + 1; dir < dirs.end(); ++dir) {
        dir_client = dir_client.GetSubdirectoryClient(*dir);
      }
      auto paths = dir_client.ListPaths(false);
      for (auto p : paths.Paths) {
        std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
        ARROW_ASSIGN_OR_RAISE(
            path_client,
            InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                options_, dfs_endpoint_url_ + container + "/" + p.Name, container,
                p.Name));
        if (path_client->GetProperties().Value.IsDirectory) {
          children_dirs->push_back(container + "/" + p.Name);
        } else {
          children_files->push_back(container + "/" + p.Name);
        }
      }
    } catch (const Azure::Storage::StorageException& exception) {
      if (!allow_not_found) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    }
    return Status::OK();
  }

  Status Walk(const FileSelector& select, const std::string& container,
              const std::string& path, int nesting_depth, std::vector<FileInfo>* out) {
    std::vector<std::string> children_dirs;
    std::vector<std::string> children_files;

    RETURN_NOT_OK(ListPaths(container, path, &children_dirs, &children_files,
                            select.allow_not_found));

    for (const auto& child_file : children_files) {
      FileInfo info;
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      RETURN_NOT_OK(GetProperties(dfs_endpoint_url_ + child_file, &properties));
      PathInfoToFileInfo(child_file, FileType::File, properties.FileSize,
                         properties.LastModified, &info);
      out->push_back(std::move(info));
    }
    for (const auto& child_dir : children_dirs) {
      FileInfo info;
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      RETURN_NOT_OK(GetProperties(dfs_endpoint_url_ + child_dir, &properties));
      PathInfoToFileInfo(child_dir, FileType::Directory, -1, properties.LastModified,
                         &info);
      out->push_back(std::move(info));
      if (select.recursive && nesting_depth < select.max_recursion) {
        const auto src = internal::RemoveTrailingSlash(child_dir);
        auto first_sep = src.find_first_of("/");
        std::string s = std::string(src.substr(first_sep + 1));
        RETURN_NOT_OK(Walk(select, container, s, nesting_depth + 1, out));
      }
    }
    return Status::OK();
  }

  Status GetProperties(
      const std::string& s,
      Azure::Storage::Files::DataLake::Models::PathProperties* properties) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                         options_, s, path.container, path.path_to_file));
    if (path.path_to_file.empty()) {
      auto file_system_client = gen2_client_->GetFileSystemClient(path.container);
      try {
        auto props = file_system_client.GetProperties().Value;
        properties->LastModified = props.LastModified;
        properties->Metadata = props.Metadata;
        properties->ETag = props.ETag;
        properties->FileSize = -1;
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
      return Status::OK();
    }
    try {
      auto props = path_client->GetProperties().Value;
      properties->FileSize = props.FileSize;
      properties->LastModified = props.LastModified;
      properties->Metadata = props.Metadata;
      properties->ETag = props.ETag;
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& container, const std::string& path,
                           const std::vector<std::string>& path_to_file_parts) {
    std::vector<std::string> children_dirs;
    std::vector<std::string> children_files;

    RETURN_NOT_OK(ListPaths(container, path, &children_dirs, &children_files));

    for (const auto& child_file : children_files) {
      ARROW_ASSIGN_OR_RAISE(auto file_path, AzurePath::FromString(child_file));
      RETURN_NOT_OK(DeleteFile(file_path.container, file_path.path_to_file_parts));
    }
    for (const auto& child_dir : children_dirs) {
      ARROW_ASSIGN_OR_RAISE(auto dir_path, AzurePath::FromString(child_dir));
      RETURN_NOT_OK(DeleteDir(dir_path.container, dir_path.path_to_file_parts));
    }
    return Status::OK();
  }

  Result<std::vector<std::string>> ListContainers() {
    try {
      auto outcome = gen2_client_->ListFileSystems();
      std::vector<std::string> containers;
      for (auto container : outcome.FileSystems) {
        containers.push_back(container.Name);
      }
      return containers;
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const std::string& s,
                                                         AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (path.empty()) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    if (!is_hierarchical_namespace_enabled_) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid Azure Blob Storage path provided,"
            " hierarchical namespace not enabled in storage account");
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto response, FileExists(dfs_endpoint_url_ + path.full_path));
    if (!response) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                         options_, dfs_endpoint_url_ + path.full_path, path.container,
                         path.path_to_file));

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client;
    ARROW_ASSIGN_OR_RAISE(
        file_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
                         options_, dfs_endpoint_url_ + path.full_path, path.container,
                         path.path_to_file));

    auto ptr = std::make_shared<ObjectInputFile>(path_client, file_client,
                                                 fs->io_context(), path);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectOutputStream>> OpenOutputStream(
      const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata,
      AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (path.empty() || path.path_to_file.empty()) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    std::string endpoint_url = dfs_endpoint_url_;
    if (!is_hierarchical_namespace_enabled_) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided,"
            " hierarchical namespace not enabled");
      }
      endpoint_url = blob_endpoint_url_;
    }
    ARROW_ASSIGN_OR_RAISE(auto response, DirExists(dfs_endpoint_url_ + path.full_path));
    if (response) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client;
    ARROW_ASSIGN_OR_RAISE(
        file_client,
        InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            options_, endpoint_url + path.full_path, path.container, path.path_to_file));

    std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client;
    ARROW_ASSIGN_OR_RAISE(
        blob_client,
        InitPathClient<Azure::Storage::Blobs::BlockBlobClient>(
            options_, endpoint_url + path.full_path, path.container, path.path_to_file));

    if (path.has_parent()) {
      AzurePath parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        ARROW_ASSIGN_OR_RAISE(response, ContainerExists(parent_path.container));
        if (!response) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        ARROW_ASSIGN_OR_RAISE(response,
                              DirExists(dfs_endpoint_url_ + parent_path.full_path));
        if (!response) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      }
    }
    auto ptr = std::make_shared<ObjectOutputStream>(file_client, blob_client,
                                                    is_hierarchical_namespace_enabled_,
                                                    fs->io_context(), path, metadata);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectAppendStream>> OpenAppendStream(
      const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata,
      AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (path.empty() || path.path_to_file.empty()) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    std::string endpoint_url = dfs_endpoint_url_;
    if (!is_hierarchical_namespace_enabled_) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid Azure Blob Storage path provided,"
            " hierarchical namespace not enabled in storage account");
      }
      endpoint_url = blob_endpoint_url_;
    }
    ARROW_ASSIGN_OR_RAISE(auto response, DirExists(dfs_endpoint_url_ + path.full_path));
    if (response) {
      return ::arrow::fs::internal::PathNotFound(path.full_path);
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client,
        InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
            options_, endpoint_url + path.full_path, path.container, path.path_to_file));

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client;
    ARROW_ASSIGN_OR_RAISE(
        file_client,
        InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            options_, endpoint_url + path.full_path, path.container, path.path_to_file));

    std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> blob_client;
    ARROW_ASSIGN_OR_RAISE(
        blob_client,
        InitPathClient<Azure::Storage::Blobs::BlockBlobClient>(
            options_, endpoint_url + path.full_path, path.container, path.path_to_file));

    if (path.has_parent()) {
      AzurePath parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        ARROW_ASSIGN_OR_RAISE(response, ContainerExists(parent_path.container));
        if (!response) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        ARROW_ASSIGN_OR_RAISE(response,
                              DirExists(dfs_endpoint_url_ + parent_path.full_path));
        if (!response) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      }
    }
    auto ptr = std::make_shared<ObjectAppendStream>(path_client, file_client, blob_client,
                                                    is_hierarchical_namespace_enabled_,
                                                    fs->io_context(), path, metadata);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const FileInfo& info,
                                                         AzureBlobFileSystem* fs) {
    if (info.type() == FileType::NotFound) {
      return ::arrow::fs::internal::PathNotFound(info.path());
    }
    if (info.type() != FileType::File && info.type() != FileType::Unknown) {
      return ::arrow::fs::internal::NotAFile(info.path());
    }

    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(info.path()));

    if (!is_hierarchical_namespace_enabled_) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid Azure Blob Storage path provided, hierarchical namespace"
            " not enabled in storage account");
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto response, FileExists(dfs_endpoint_url_ + info.path()));
    if (!response) {
      return ::arrow::fs::internal::PathNotFound(info.path());
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client;
    ARROW_ASSIGN_OR_RAISE(
        path_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
                         options_, dfs_endpoint_url_ + info.path(), path.container,
                         path.path_to_file));

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client;
    ARROW_ASSIGN_OR_RAISE(
        file_client, InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
                         options_, dfs_endpoint_url_ + info.path(), path.container,
                         path.path_to_file));

    auto ptr = std::make_shared<ObjectInputFile>(path_client, file_client,
                                                 fs->io_context(), path, info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

 protected:
  AzureOptions options_;
};

AzureBlobFileSystem::AzureBlobFileSystem(const AzureOptions& options,
                                         const io::IOContext& io_context)
    : FileSystem(io_context), impl_(std::make_shared<Impl>(options, io_context)) {
  default_async_is_sync_ = false;
}

AzureBlobFileSystem::~AzureBlobFileSystem() {}

Result<std::shared_ptr<AzureBlobFileSystem>> AzureBlobFileSystem::Make(
    const AzureOptions& options, const io::IOContext& io_context) {
  std::shared_ptr<AzureBlobFileSystem> ptr(new AzureBlobFileSystem(options, io_context));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

bool AzureBlobFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& azure_fs =
      ::arrow::internal::checked_cast<const AzureBlobFileSystem&>(other);
  return options().Equals(azure_fs.options());
}

AzureOptions AzureBlobFileSystem::options() const { return impl_->options(); }

Result<FileInfo> AzureBlobFileSystem::GetFileInfo(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  FileInfo info;
  info.set_path(s);

  if (!impl_->is_hierarchical_namespace_enabled_) {
    if (path.path_to_file_parts.size() > 1) {
      info.set_type(FileType::NotFound);
      return info;
    }
  }

  if (path.empty()) {
    // It's the root path ""
    info.set_type(FileType::Directory);
    return info;
  } else if (path.path_to_file.empty()) {
    // It's a container
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      info.set_type(FileType::NotFound);
      return info;
    }
    info.set_type(FileType::Directory);
    return info;
  } else {
    // It's an object
    ARROW_ASSIGN_OR_RAISE(bool file_exists,
                          impl_->FileExists(impl_->dfs_endpoint_url_ + path.full_path));
    if (file_exists) {
      // "File" object found
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      RETURN_NOT_OK(
          impl_->GetProperties(impl_->dfs_endpoint_url_ + path.full_path, &properties));
      FileObjectToInfo(properties, &info);
      return info;
    }
    // Not found => perhaps it's a "directory"
    ARROW_ASSIGN_OR_RAISE(auto is_dir,
                          impl_->DirExists(impl_->dfs_endpoint_url_ + path.full_path));
    if (is_dir) {
      info.set_type(FileType::Directory);
    } else {
      info.set_type(FileType::NotFound);
    }
    return info;
  }
}

Result<FileInfoVector> AzureBlobFileSystem::GetFileInfo(const FileSelector& select) {
  ARROW_ASSIGN_OR_RAISE(auto base_path, AzurePath::FromString(select.base_dir));

  FileInfoVector results;

  if (base_path.empty()) {
    // List all containers
    ARROW_ASSIGN_OR_RAISE(auto containers, impl_->ListContainers());
    for (const auto& container : containers) {
      FileInfo info;
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      RETURN_NOT_OK(
          impl_->GetProperties(impl_->dfs_endpoint_url_ + container, &properties));
      PathInfoToFileInfo(container, FileType::Directory, -1, properties.LastModified,
                         &info);
      results.push_back(std::move(info));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, container, "", 0, &results));
      }
    }
    return results;
  }

  if (!impl_->is_hierarchical_namespace_enabled_) {
    if (base_path.path_to_file_parts.size() > 1) {
      if (!select.allow_not_found) {
        return Status::IOError(
            "Invalid Azure Blob Storage path provided, hierarchical namespace not"
            " enabled in storage account");
      }
      return results;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto response, impl_->ContainerExists(base_path.container));
  if (base_path.path_to_file.empty() && !response) {
    if (!select.allow_not_found) {
      return ::arrow::fs::internal::PathNotFound(base_path.container);
    }
    return results;
  }

  ARROW_ASSIGN_OR_RAISE(
      response, impl_->FileExists(impl_->dfs_endpoint_url_ + base_path.full_path));
  if (response) {
    return ::arrow::fs::internal::PathNotFound(base_path.full_path);
  }

  ARROW_ASSIGN_OR_RAISE(response,
                        impl_->DirExists(impl_->dfs_endpoint_url_ + base_path.full_path));
  if (!(base_path.path_to_file.empty()) && !response) {
    if (!select.allow_not_found) {
      return ::arrow::fs::internal::PathNotFound(base_path.full_path);
    }
    return results;
  }

  // Nominal case -> walk a single container
  RETURN_NOT_OK(
      impl_->Walk(select, base_path.container, base_path.path_to_file, 0, &results));
  return results;
}

Status AzureBlobFileSystem::CreateDir(const std::string& s, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty()) {
    return Status::IOError("Cannot create directory, root path given");
  }
  ARROW_ASSIGN_OR_RAISE(auto response,
                        impl_->FileExists(impl_->dfs_endpoint_url_ + path.full_path));
  if (response) {
    return Status::IOError("Cannot create directory, file exists at the specified path");
  }
  if (path.path_to_file.empty()) {
    // Create container
    return impl_->CreateContainer(path.container);
  }
  // Hierarchical namespace not enabled type storage accounts
  if (!impl_->is_hierarchical_namespace_enabled_) {
    if (!path.path_to_file.empty()) {
      return Status::IOError(
          "Cannot create directory, "
          "storage account doesn't have hierarchical namespace enabled");
    }
  }
  if (recursive) {
    // Ensure container exists
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      RETURN_NOT_OK(impl_->CreateContainer(path.container));
    }
    std::vector<std::string> parent_path_to_file;

    for (const auto& part : path.path_to_file_parts) {
      parent_path_to_file.push_back(part);
      RETURN_NOT_OK(impl_->CreateEmptyDir(path.container, parent_path_to_file));
    }
    return Status::OK();
  } else {
    // Check parent dir exists
    if (path.has_parent()) {
      auto parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        ARROW_ASSIGN_OR_RAISE(auto exists, impl_->ContainerExists(parent_path.container));
        if (!exists) {
          return Status::IOError("Cannot create directory '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        ARROW_ASSIGN_OR_RAISE(auto exists, impl_->DirExists(impl_->dfs_endpoint_url_ +
                                                            parent_path.full_path));
        if (!exists) {
          return Status::IOError("Cannot create directory '", path.full_path,
                                 "': parent directory does not exist");
        }
      }
    }
    return impl_->CreateEmptyDir(path.container, path.path_to_file_parts);
  }
}

Status AzureBlobFileSystem::DeleteDir(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all Azure Containers");
  }
  if (path.path_to_file.empty()) {
    return impl_->DeleteContainer(path.container);
  }
  ARROW_ASSIGN_OR_RAISE(auto response,
                        impl_->FileExists(impl_->dfs_endpoint_url_ + path.full_path));
  if (response) {
    return Status::IOError("Cannot delete directory, file exists at the specified path");
  }

  // Hierarchical namespace not enabled type storage accounts
  if (!impl_->is_hierarchical_namespace_enabled_) {
    if (!path.path_to_file.empty()) {
      return Status::IOError(
          "Cannot delete directory, storage"
          "account doesn't have hierarchical namespace enabled");
    }
  }
  return impl_->DeleteDir(path.container, path.path_to_file_parts);
}

Status AzureBlobFileSystem::DeleteDirContents(const std::string& s, bool missing_dir_ok) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty()) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return ::arrow::fs::internal::PathNotFound(path.full_path);
  }

  ARROW_ASSIGN_OR_RAISE(auto response, impl_->ContainerExists(path.container));
  if (path.path_to_file.empty() && !response) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return ::arrow::fs::internal::PathNotFound(path.container);
  }

  ARROW_ASSIGN_OR_RAISE(response,
                        impl_->FileExists(impl_->dfs_endpoint_url_ + path.full_path));
  if (response) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return ::arrow::fs::internal::PathNotFound(path.full_path);
  }

  ARROW_ASSIGN_OR_RAISE(response,
                        impl_->DirExists(impl_->dfs_endpoint_url_ + path.full_path));
  if (!(path.path_to_file.empty()) && !response) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return ::arrow::fs::internal::PathNotFound(path.full_path);
  }

  return impl_->DeleteDirContents(path.container, path.path_to_file,
                                  path.path_to_file_parts);
}

Status AzureBlobFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("Cannot delete all Azure Containers");
}

Status AzureBlobFileSystem::DeleteFile(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  return impl_->DeleteFile(path.container, path.path_to_file_parts);
}

Status AzureBlobFileSystem::Move(const std::string& src, const std::string& dest) {
  return impl_->Move(src, dest);
}

Status AzureBlobFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return impl_->CopyFile(src, dest);
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const std::string& s) {
  return impl_->OpenInputFile(s, this);
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const std::string& s) {
  return impl_->OpenInputFile(s, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return impl_->OpenOutputStream(path, metadata, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return impl_->OpenAppendStream(path, metadata, this);
}
}  // namespace fs
}  // namespace arrow

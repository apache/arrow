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
#include <azure/core/credentials/credentials.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/datalake.hpp>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

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
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::Uri;

namespace fs {

static const char kSep = '/';

// -----------------------------------------------------------------------
// AzureOptions implementation

AzureOptions::AzureOptions() {}

std::string AzureOptions::GetAccountNameFromConnectionString(
    const std::string& connectionString) {
  std::map<std::string, std::string> connectionStringMap;
  std::string::const_iterator cur = connectionString.begin();

  while (cur != connectionString.end()) {
    auto key_begin = cur;
    auto key_end = std::find(cur, connectionString.end(), '=');
    std::string key = std::string(key_begin, key_end);
    cur = key_end;
    if (cur != connectionString.end()) {
      ++cur;
    }
    auto value_begin = cur;
    auto value_end = std::find(cur, connectionString.end(), ';');
    std::string value = std::string(value_begin, value_end);
    cur = value_end;
    if (cur != connectionString.end()) {
      ++cur;
    }
    if (!key.empty() || !value.empty()) {
      connectionStringMap[std::move(key)] = std::move(value);
    }
  }

  auto getWithDefault = [](const std::map<std::string, std::string>& m,
                           const std::string& key,
                           const std::string& defaultValue = std::string()) {
    auto ite = m.find(key);
    return ite == m.end() ? defaultValue : ite->second;
  };

  std::string accountName = getWithDefault(connectionStringMap, "AccountName");
  if (accountName.empty()) {
    throw std::runtime_error("Cannot find account name in connection string.");
  }
  return accountName;
}

void AzureOptions::ConfigureAnonymousCredentials(const std::string& account_name) {
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  credentials_kind = AzureCredentialsKind::Anonymous;
}

void AzureOptions::ConfigureAccountKeyCredentials(const std::string& account_name,
                                                  const std::string& account_key) {
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  storage_credentials_provider =
      std::make_shared<Azure::Storage::StorageSharedKeyCredential>(account_name,
                                                                   account_key);
  credentials_kind = AzureCredentialsKind::StorageCredentials;
}

void AzureOptions::ConfigureConnectionStringCredentials(
    const std::string& connection_string_uri) {
  auto account_name = GetAccountNameFromConnectionString(connection_string_uri);
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  connection_string = connection_string_uri;
  credentials_kind = AzureCredentialsKind::ConnectionString;
}

void AzureOptions::ConfigureServicePrincipleCredentials(
    const std::string& account_name, const std::string& tenant_id,
    const std::string& client_id, const std::string& client_secret) {
  account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
  account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  service_principle_credentials_provider =
      std::make_shared<Azure::Identity::ClientSecretCredential>(tenant_id, client_id,
                                                                client_secret);
  credentials_kind = AzureCredentialsKind::ServicePrincipleCredentials;
}

void AzureOptions::ConfigureSasCredentials(const std::string& uri) {
  auto src = internal::RemoveTrailingSlash(uri);
  auto first_sep = src.find_first_of("?");
  sas_token = std::string(src.substr(first_sep));
  account_blob_url = std::string(src.substr(0, first_sep));
  src = internal::RemoveTrailingSlash(account_blob_url);
  first_sep = src.find("blob.core.windows.net");
  account_dfs_url = std::string(src.substr(0, first_sep)) + "dfs" +
                    std::string(src.substr(first_sep + 4)) + "/";
  credentials_kind = AzureCredentialsKind::Sas;
}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return (scheme == other.scheme && account_dfs_url == other.account_dfs_url &&
          account_blob_url == other.account_blob_url &&
          credentials_kind == other.credentials_kind);
}

AzureOptions AzureOptions::FromAnonymous(const std::string account_name) {
  AzureOptions options;
  options.ConfigureAnonymousCredentials(account_name);
  return options;
}

AzureOptions AzureOptions::FromAccountKey(const std::string& account_name,
                                          const std::string& account_key) {
  AzureOptions options;
  options.ConfigureAccountKeyCredentials(account_name, account_key);
  return options;
}

AzureOptions AzureOptions::FromConnectionString(const std::string& connection_string) {
  AzureOptions options;
  options.ConfigureConnectionStringCredentials(connection_string);
  return options;
}

AzureOptions AzureOptions::FromServicePrincipleCredential(
    const std::string& account_name, const std::string& tenant_id,
    const std::string& client_id, const std::string& client_secret) {
  AzureOptions options;
  options.ConfigureServicePrincipleCredentials(account_name, tenant_id, client_id,
                                               client_secret);
  return options;
}

AzureOptions AzureOptions::FromSas(const std::string& uri) {
  AzureOptions options;
  options.ConfigureSasCredentials(uri);
  return options;
}

Result<AzureOptions> AzureOptions::FromUri(const std::string& uri_string,
                                           std::string* out_path) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
}

Result<AzureOptions> AzureOptions::FromUri(const Uri& uri, std::string* out_path) {
  AzureOptions options;
  const auto container = uri.host();
  auto path = uri.path();
  if (container.empty()) {
    if (!path.empty()) {
      return Status::Invalid("Missing container in URI");
    }
  } else {
    if (path.empty()) {
      path = container.substr(0, container.find('.'));
    } else {
      if (path[0] != '/') {
        return Status::Invalid("URI should absolute, not relative");
      }
      path = container + path;
    }
  }
  if (out_path != nullptr) {
    *out_path = std::string(internal::RemoveTrailingSlash(path));
  }

  options.scheme = uri.scheme();
  AZURE_ASSERT(container.find('.') != std::string::npos);
  std::string accountName = container.substr(0, container.find('.'));
  const auto query_string = uri.query_string();
  if (!query_string.empty()) {
    options.ConfigureSasCredentials(uri.scheme() + "://" + path + "?" + query_string);
  } else {
    options.ConfigureAnonymousCredentials(accountName);
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

    // Expected input here => s = /synapsemlfs/testdir/testfile.txt
    auto src = internal::RemoveTrailingSlash(s);
    if (src.starts_with("https:") || src.starts_with("http::")) {
      RemoveSchemeFromUri(src);
    }
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
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

  static void RemoveSchemeFromUri(nonstd::sv_lite::string_view& s) {
    auto first = s.find(".core.windows.net");
    s = s.substr(first + 18, s.length());
  }

  static Status Validate(const AzurePath* path) {
    auto result = internal::ValidateAbstractPathParts(path->path_to_file_parts);
    if (!result.ok()) {
      return Status::Invalid(result.message(), " in path ", path->full_path);
    } else {
      return result;
    }
  }

  AzurePath parent() const {
    DCHECK(!path_to_file_parts.empty());
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
  auto push = [&](std::string k, const std::string v) {
    if (!v.empty()) {
      md->Append(std::move(k), v);
    }
  };
  for (auto prop : result) {
    push(prop.first, prop.second);
  }
  return md;
}

template <typename T>
Status InitServiceClient(std::shared_ptr<T>& client, const AzureOptions options,
                         const std::string url) {
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
  return Status::OK();
}

template <typename T>
Status InitPathClient(std::shared_ptr<T>& client, const AzureOptions options,
                      const std::string path, const std::string container,
                      const std::string path_to_file) {
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
  return Status::OK();
}

class ObjectInputFile final : public io::RandomAccessFile {
 public:
  ObjectInputFile(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient,
      const io::IOContext& io_context, const AzurePath& path, int64_t size = kNoSize)
      : pathClient_(std::move(pathClient)),
        fileClient_(std::move(fileClient)),
        io_context_(io_context),
        path_(path),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = pathClient_->GetProperties();
      if (properties.Value.IsDirectory) {
        return Status::IOError("Invalid file path given");
      }
      content_length_ = properties.Value.FileSize;
      DCHECK_GE(content_length_, 0);
      metadata_ = GetObjectMetadata(properties.Value.Metadata);
      return Status::OK();
    } catch (std::exception const& e) {
      return Status::IOError("Invalid file path given");
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
    pathClient_ = nullptr;
    fileClient_ = nullptr;
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
    Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
    Azure::Core::Http::HttpRange range;
    range.Offset = position;
    range.Length = nbytes;
    downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto result =
        fileClient_->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, downloadOptions)
            .Value;
    AZURE_ASSERT(result.ContentRange.Length.HasValue());
    return result.ContentRange.Length.Value();
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
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
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
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient,
      const io::IOContext& io_context, const AzurePath& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata)
      : pathClient_(std::move(pathClient)),
        fileClient_(std::move(fileClient)),
        io_context_(io_context),
        path_(path),
        metadata_(metadata) {}

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
    try {
      auto properties = pathClient_->GetProperties();
      if (properties.Value.IsDirectory) {
        return Status::IOError("Invalid file path given");
      }
      content_length_ = properties.Value.FileSize;
      DCHECK_GE(content_length_, 0);
    } catch (std::exception const& e) {
      // new file
      std::string s = "";
      fileClient_->UploadFrom(
          const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&s[0])), s.size());
      content_length_ = 0;
    }
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    pathClient_ = nullptr;
    fileClient_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    pathClient_ = nullptr;
    fileClient_ = nullptr;
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
    auto result =
        fileClient_
            ->UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(data)),
                         nbytes)
            .Value;
    pos_ += nbytes;
    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    fileClient_->Flush(content_length_);
    return Status::OK();
  }

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
  const io::IOContext io_context_;
  const AzurePath path_;

  bool closed_ = true;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

class ObjectAppendStream final : public io::OutputStream {
 public:
  ObjectAppendStream(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient,
      const io::IOContext& io_context, const AzurePath& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata)
      : pathClient_(std::move(pathClient)),
        fileClient_(std::move(fileClient)),
        io_context_(io_context),
        path_(path),
        metadata_(metadata) {}

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
      auto properties = pathClient_->GetProperties();
      if (properties.Value.IsDirectory) {
        return Status::IOError("Invalid file path given");
      }
      content_length_ = properties.Value.FileSize;
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    } catch (std::exception const& e) {
      return Status::IOError("Invalid file path given");
    }
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }
    pathClient_ = nullptr;
    fileClient_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }
    pathClient_ = nullptr;
    fileClient_ = nullptr;
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
    auto content = Azure::Core::IO::MemoryBodyStream(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(data)), nbytes);
    auto result = fileClient_->Append(content, 0);
    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    fileClient_->Flush(content_length_);
    return Status::OK();
  }

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
  const io::IOContext io_context_;
  const AzurePath path_;

  bool closed_ = true;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  const std::shared_ptr<const KeyValueMetadata> metadata_;
};

TimePoint ToTimePoint(int secs) {
  std::chrono::nanoseconds ns_count(static_cast<int64_t>(secs) * 1000000000);
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

void FileObjectToInfo(
    const Azure::Storage::Files::DataLake::Models::PathProperties& properties,
    FileInfo* info) {
  info->set_type(FileType::File);
  info->set_size(static_cast<int64_t>(properties.FileSize));
  info->set_mtime(ToTimePoint(std::chrono::duration_cast<std::chrono::seconds>(
                                  properties.LastModified - Azure::DateTime(1970))
                                  .count()));
}

void PathInfoToFileInfo(const std::string path, const FileType type, const int64_t size,
                        const Azure::DateTime dt, FileInfo* info) {
  info->set_type(type);
  info->set_size(size);
  info->set_path(path);
  info->set_mtime(ToTimePoint(
      std::chrono::duration_cast<std::chrono::seconds>(dt - Azure::DateTime(1970))
          .count()));
}

}  // namespace

// -----------------------------------------------------------------------
// Azure filesystem implementation

class AzureBlobFileSystem::Impl
    : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {
 public:
  io::IOContext io_context_;
  std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> gen1Client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2Client_;
  std::string dfs_endpoint_url;
  std::string blob_endpoint_url;
  bool isHierarchicalNamespaceEnabled;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    dfs_endpoint_url = options_.account_dfs_url;
    blob_endpoint_url = options_.account_blob_url;
    InitServiceClient(gen1Client_, options_, blob_endpoint_url);
    InitServiceClient(gen2Client_, options_, dfs_endpoint_url);
    isHierarchicalNamespaceEnabled =
        gen1Client_->GetAccountInfo().Value.IsHierarchicalNamespaceEnabled;
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

  // Create a container. Successful if container already exists.
  Status CreateContainer(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    fileSystemClient.CreateIfNotExists();
    return Status::OK();
  }

  // Tests to see if a container exists
  Result<bool> ContainerExists(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    try {
      auto properties = fileSystemClient.GetProperties();
      return true;
    } catch (std::exception const& e) {
      return false;
    }
  }

  Result<bool> DirExists(const std::string& s) {
    std::string uri = s;
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(uri));
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, uri, path.container, path.path_to_file);
    try {
      auto properties = pathClient_->GetProperties();
      return properties.Value.IsDirectory;
    } catch (std::exception const& e) {
      return false;
    }
  }

  Result<bool> FileExists(const std::string& s) {
    std::string uri = s;
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(uri));
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, uri, path.container, path.path_to_file);
    try {
      auto properties = pathClient_->GetProperties();
      return !properties.Value.IsDirectory;
    } catch (std::exception const& e) {
      return false;
    }
  }

  Status CreateEmptyDir(const std::string& container,
                        const std::vector<std::string>& path) {
    auto directoryClient =
        gen2Client_->GetFileSystemClient(container).GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    directoryClient.CreateIfNotExists();
    return Status::OK();
  }

  Status DeleteContainer(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    fileSystemClient.DeleteIfExists();
    return Status::OK();
  }

  Status DeleteDir(const std::string& container, const std::vector<std::string>& path) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    if (FileExists(directoryClient.GetUrl()).ValueOrDie()) {
      return Status::IOError("Cannot delete directory, Invalid Directory Path");
    }
    if (!DirExists(directoryClient.GetUrl()).ValueOrDie()) {
      return Status::IOError("Cannot delete directory, Invalid Directory Path");
    }
    directoryClient.DeleteRecursiveIfExists();
    return Status::OK();
  }

  Status DeleteFile(const std::string& container, const std::vector<std::string>& path) {
    if (path.empty()) {
      return Status::IOError("Cannot delete File, Invalid File Path");
    }
    if (!isHierarchicalNamespaceEnabled) {
      if (path.size() > 1) {
        return Status::IOError(
            "Cannot delete File, Invalid File Path,"
            " hierarchical namespace not enabled");
      }
      auto blobClient =
          gen1Client_->GetBlobContainerClient(container).GetBlobClient(path.front());
      if (!FileExists(blobClient.GetUrl()).ValueOrDie()) {
        return Status::IOError("Cannot delete File, Invalid File Path");
      }
      blobClient.DeleteIfExists();
      return Status::OK();
    }
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    if (path.size() == 1) {
      auto fileClient = fileSystemClient.GetFileClient(path.front());
      if (DirExists(fileClient.GetUrl()).ValueOrDie()) {
        return Status::IOError("Cannot delete File, Invalid File Path");
      }
      if (!FileExists(fileClient.GetUrl()).ValueOrDie()) {
        return Status::IOError("Cannot delete File, Invalid File Path");
      }
      fileClient.DeleteIfExists();
      return Status::OK();
    }
    std::string file_name = path.back();
    auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
    std::vector<std::string>::const_iterator it = path.begin();
    std::advance(it, 1);
    while (it != (path.end() - 1)) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    auto fileClient = directoryClient.GetFileClient(file_name);
    if (DirExists(fileClient.GetUrl()).ValueOrDie()) {
      return Status::IOError("Cannot delete File, Invalid File Path");
    }
    if (!FileExists(fileClient.GetUrl()).ValueOrDie()) {
      return Status::IOError("Cannot delete File, Invalid File Path");
    }
    fileClient.DeleteIfExists();
    return Status::OK();
  }

  Status Move(const std::string& src, const std::string& dest) {
    ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
    ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));

    if (!isHierarchicalNamespaceEnabled) {
      return Status::IOError("Cannot move, Hierarchical namespace not enabled");
    }
    if (src_path.empty() || dest_path.empty() || src_path.path_to_file.empty() ||
        dest_path.path_to_file.empty()) {
      return Status::IOError("Invalid path provided");
    }
    if (src_path == dest_path) {
      return Status::OK();
    }
    if (FileExists(dfs_endpoint_url + src_path.full_path).ValueOrDie()) {
      auto fileSystemClient = gen2Client_->GetFileSystemClient(src_path.container);
      auto path = src_path.path_to_file_parts;
      if (path.size() == 1) {
        try {
          fileSystemClient.RenameFile(path.front(), dest_path.path_to_file);
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
        return Status::OK();
      }
      auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
      std::vector<std::string>::const_iterator it = path.begin();
      std::advance(it, 1);
      while (it != path.end()) {
        if ((it + 1) == path.end()) {
          break;
        }
        directoryClient = directoryClient.GetSubdirectoryClient(*it);
        ++it;
      }
      try {
        directoryClient.RenameFile(it->data(), dest_path.path_to_file);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else if (DirExists(dfs_endpoint_url + src_path.full_path).ValueOrDie()) {
      auto fileSystemClient = gen2Client_->GetFileSystemClient(src_path.container);
      auto path = src_path.path_to_file_parts;
      if (path.size() == 1) {
        try {
          fileSystemClient.RenameDirectory(path.front(), dest_path.path_to_file);
        } catch (const Azure::Storage::StorageException& exception) {
          return Status::IOError(exception.RawResponse->GetReasonPhrase());
        }
        return Status::OK();
      }
      auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
      std::vector<std::string>::const_iterator it = path.begin();
      std::advance(it, 1);
      while (it != path.end()) {
        if ((it + 1) == path.end()) {
          break;
        }
        directoryClient = directoryClient.GetSubdirectoryClient(*it);
        ++it;
      }
      try {
        directoryClient.RenameSubdirectory(it->data(), dest_path.path_to_file);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
    } else {
      return Status::IOError("Invalid path provided");
    }
    return Status::OK();
  }

  Status CopyFile(const std::string& src, const std::string& dest) {
    ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
    ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));

    if (src_path.empty() || dest_path.empty() || src_path.path_to_file.empty() ||
        dest_path.path_to_file.empty()) {
      return Status::IOError("Cannot copy file, file doesn't exist at src");
    }

    if (!(FileExists(dfs_endpoint_url + src_path.full_path)).ValueOrDie()) {
      return Status::IOError("Cannot copy file, file doesn't exist at src");
    }

    if (DirExists(dfs_endpoint_url + dest_path.full_path).ValueOrDie()) {
      return Status::IOError("Cannot copy file, Invalid destination path");
    }

    if (!isHierarchicalNamespaceEnabled) {
      if (src_path.path_to_file_parts.size() > 1 ||
          dest_path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided, "
            "hierarchical namespace not enabled");
      }
      if (dest_path.empty() || dest_path.path_to_file_parts.empty()) {
        return Status::IOError("Invalid path provided at destination");
      }
      if (src_path == dest_path) {
        return Status::OK();
      }
      auto containerClient = gen1Client_->GetBlobContainerClient(dest_path.container);
      auto fileClient = containerClient.GetBlobClient(dest_path.path_to_file);
      try {
        auto response = fileClient.StartCopyFromUri(blob_endpoint_url + src);
      } catch (const Azure::Storage::StorageException& exception) {
        return Status::IOError(exception.RawResponse->GetReasonPhrase());
      }
      return Status::OK();
    }

    if (dest_path.has_parent()) {
      AzurePath parent_path = dest_path.parent();
      if (parent_path.path_to_file.empty()) {
        if (!ContainerExists(parent_path.container).ValueOrDie()) {
          return Status::IOError("Cannot copy file '", src_path.full_path,
                                 "': parent directory of destination does not exist");
        }
      } else {
        auto exists = DirExists(dfs_endpoint_url + parent_path.full_path);
        if (!(exists.ValueOrDie())) {
          return Status::IOError("Cannot copy file '", src_path.full_path,
                                 "': parent directory of destination does not exist");
        }
      }
    }
    if (src_path == dest_path) {
      return Status::OK();
    }
    auto containerClient = gen1Client_->GetBlobContainerClient(dest_path.container);
    auto fileClient = containerClient.GetBlobClient(dest_path.path_to_file);
    try {
      if (options_.credentials_kind == AzureCredentialsKind::Sas) {
        fileClient.StartCopyFromUri(blob_endpoint_url + src + options_.sas_token);
      } else {
        fileClient.StartCopyFromUri(blob_endpoint_url + src);
      }
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
    return Status::OK();
  }

  Status ListPaths(const std::string& container, const std::string& path,
                   std::vector<std::string>* childrenDirs,
                   std::vector<std::string>* childrenFiles,
                   const bool allow_not_found = false) {
    if (!isHierarchicalNamespaceEnabled) {
      try {
        auto paths = gen1Client_->GetBlobContainerClient(container).ListBlobs();
        for (auto p : paths.Blobs) {
          std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>
              pathClient_;
          InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
              pathClient_, options_, dfs_endpoint_url + container + "/" + p.Name,
              container, p.Name);
          childrenFiles->push_back(container + "/" + p.Name);
        }
      } catch (std::exception const& e) {
        if (!allow_not_found) {
          return Status::IOError("Path does not exists");
        }
      }
      return Status::OK();
    }
    if (path.empty()) {
      try {
        auto paths = gen2Client_->GetFileSystemClient(container).ListPaths(false);
        for (auto p : paths.Paths) {
          std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>
              pathClient_;
          InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
              pathClient_, options_, dfs_endpoint_url + container + "/" + p.Name,
              container, p.Name);
          if (pathClient_->GetProperties().Value.IsDirectory) {
            childrenDirs->push_back(container + "/" + p.Name);
          } else {
            childrenFiles->push_back(container + "/" + p.Name);
          }
        }
      } catch (std::exception const& e) {
        if (!allow_not_found) {
          return Status::IOError("Path does not exists");
        }
      }
      return Status::OK();
    }
    std::vector<std::string> dirs = internal::SplitAbstractPath(path);
    try {
      Azure::Storage::Files::DataLake::DataLakeDirectoryClient dirClient =
          gen2Client_->GetFileSystemClient(container).GetDirectoryClient(dirs.front());
      for (auto dir = dirs.begin() + 1; dir < dirs.end(); ++dir) {
        dirClient = dirClient.GetSubdirectoryClient(*dir);
      }
      auto paths = dirClient.ListPaths(false);
      for (auto p : paths.Paths) {
        std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
        InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
            pathClient_, options_, dfs_endpoint_url + container + "/" + p.Name, container,
            p.Name);
        if (pathClient_->GetProperties().Value.IsDirectory) {
          childrenDirs->push_back(container + "/" + p.Name);
        } else {
          childrenFiles->push_back(container + "/" + p.Name);
        }
      }
    } catch (std::exception const& e) {
      if (!allow_not_found) {
        return Status::IOError("Path does not exists");
      }
    }
    return Status::OK();
  }

  Status Walk(const FileSelector& select, const std::string& container,
              const std::string& path, int nesting_depth, std::vector<FileInfo>* out) {
    std::vector<std::string> childrenDirs;
    std::vector<std::string> childrenFiles;

    Status st =
        ListPaths(container, path, &childrenDirs, &childrenFiles, select.allow_not_found);
    if (!st.ok()) {
      return st;
    }

    for (const auto& childFile : childrenFiles) {
      FileInfo info;
      // std::string url = gen2Client_->GetUrl();
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      GetProperties(dfs_endpoint_url + childFile, &properties);
      PathInfoToFileInfo(childFile, FileType::File, properties.FileSize,
                         properties.LastModified, &info);
      out->push_back(std::move(info));
    }
    for (const auto& childDir : childrenDirs) {
      FileInfo info;
      // std::string url = gen2Client_->GetUrl();
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      GetProperties(dfs_endpoint_url + childDir, &properties);
      PathInfoToFileInfo(childDir, FileType::Directory, -1, properties.LastModified,
                         &info);
      out->push_back(std::move(info));
      if (select.recursive && nesting_depth < select.max_recursion) {
        const auto src = internal::RemoveTrailingSlash(childDir);
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
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, s, path.container, path.path_to_file);
    if (path.path_to_file.empty()) {
      auto fileSystemClient = gen2Client_->GetFileSystemClient(path.container);
      auto props = fileSystemClient.GetProperties().Value;
      properties->LastModified = props.LastModified;
      properties->Metadata = props.Metadata;
      properties->ETag = props.ETag;
      properties->FileSize = -1;
      return Status::OK();
    }
    auto props = pathClient_->GetProperties().Value;
    properties->FileSize = props.FileSize;
    properties->LastModified = props.LastModified;
    properties->Metadata = props.Metadata;
    properties->ETag = props.ETag;
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& container, const std::string& path,
                           const std::vector<std::string>& path_to_file_parts) {
    std::vector<std::string> childrenDirs;
    std::vector<std::string> childrenFiles;

    Status st = ListPaths(container, path, &childrenDirs, &childrenFiles);
    if (!st.ok()) {
      return st;
    }
    for (const auto& childFile : childrenFiles) {
      ARROW_ASSIGN_OR_RAISE(auto filePath, AzurePath::FromString(childFile));
      DeleteFile(filePath.container, filePath.path_to_file_parts);
    }
    for (const auto& childDir : childrenDirs) {
      ARROW_ASSIGN_OR_RAISE(auto dirPath, AzurePath::FromString(childDir));
      DeleteDir(dirPath.container, dirPath.path_to_file_parts);
    }
    return Status::OK();
  }

  Result<std::vector<std::string>> ListContainers() {
    auto outcome = gen2Client_->ListFileSystems();
    std::vector<std::string> containers;
    for (auto container : outcome.FileSystems) {
      containers.push_back(container.Name);
    }
    return containers;
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const std::string& s,
                                                         AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (path.empty()) {
      return Status::IOError("Invalid path provided");
    }
    if (!isHierarchicalNamespaceEnabled) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided,"
            " hierarchical namespace not enabled");
      }
    }
    if (!(FileExists(dfs_endpoint_url + path.full_path)).ValueOrDie()) {
      return Status::IOError("Invalid path provided");
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, dfs_endpoint_url + path.full_path, path.container,
        path.path_to_file);

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
        fileClient_, options_, dfs_endpoint_url + path.full_path, path.container,
        path.path_to_file);

    auto ptr = std::make_shared<ObjectInputFile>(pathClient_, fileClient_,
                                                 fs->io_context(), path);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectOutputStream>> OpenOutputStream(
      const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata,
      AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (path.empty() || path.path_to_file.empty()) {
      return Status::IOError("Invalid path provided");
    }
    std::string endpoint_url = dfs_endpoint_url;
    if (!isHierarchicalNamespaceEnabled) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided,"
            " hierarchical namespace not enabled");
      }
      endpoint_url = blob_endpoint_url;
    }
    if (DirExists(dfs_endpoint_url + path.full_path).ValueOrDie()) {
      return Status::IOError("Invalid path provided");
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, endpoint_url + path.full_path, path.container,
        path.path_to_file);

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
        fileClient_, options_, endpoint_url + path.full_path, path.container,
        path.path_to_file);

    if (path.has_parent()) {
      AzurePath parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        if (!ContainerExists(parent_path.container).ValueOrDie()) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        auto exists = DirExists(dfs_endpoint_url + parent_path.full_path);
        if (!(exists.ValueOrDie())) {
          return Status::IOError("Cannot write to file '", path.full_path,
                                 "': parent directory does not exist");
        }
      }
    }
    auto ptr = std::make_shared<ObjectOutputStream>(pathClient_, fileClient_,
                                                    fs->io_context(), path, metadata);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectAppendStream>> OpenAppendStream(
      const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata,
      AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

    if (!isHierarchicalNamespaceEnabled) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided, "
            "hierarchical namespace not enabled");
      }
    }

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, dfs_endpoint_url + s, path.container, path.path_to_file);

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
        fileClient_, options_, dfs_endpoint_url + s, path.container, path.path_to_file);

    auto ptr = std::make_shared<ObjectAppendStream>(pathClient_, fileClient_,
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

    if (!isHierarchicalNamespaceEnabled) {
      if (path.path_to_file_parts.size() > 1) {
        return Status::IOError(
            "Invalid path provided, hierarchical namespace"
            " not enabled");
      }
    }
    if (!(FileExists(dfs_endpoint_url + info.path())).ValueOrDie()) {
      return Status::IOError("Invalid path provided");
    }
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> pathClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakePathClient>(
        pathClient_, options_, dfs_endpoint_url + info.path(), path.container,
        path.path_to_file);

    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> fileClient_;
    InitPathClient<Azure::Storage::Files::DataLake::DataLakeFileClient>(
        fileClient_, options_, dfs_endpoint_url + info.path(), path.container,
        path.path_to_file);

    auto ptr = std::make_shared<ObjectInputFile>(pathClient_, fileClient_,
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
  const auto& azurefs =
      ::arrow::internal::checked_cast<const AzureBlobFileSystem&>(other);
  return options().Equals(azurefs.options());
}

AzureOptions AzureBlobFileSystem::options() const { return impl_->options(); }

Result<FileInfo> AzureBlobFileSystem::GetFileInfo(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  FileInfo info;
  info.set_path(s);

  if (!impl_->isHierarchicalNamespaceEnabled) {
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
                          impl_->FileExists(impl_->dfs_endpoint_url + path.full_path));
    if (file_exists) {
      // "File" object found
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      impl_->GetProperties(impl_->dfs_endpoint_url + path.full_path, &properties);
      FileObjectToInfo(properties, &info);
      return info;
    }
    // Not found => perhaps it's a "directory"
    auto is_dir = impl_->DirExists(impl_->dfs_endpoint_url + path.full_path);
    if (is_dir.ValueOrDie()) {
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
      // std::string url = impl_->gen2Client_->GetUrl();
      Azure::Storage::Files::DataLake::Models::PathProperties properties;
      impl_->GetProperties(impl_->dfs_endpoint_url + container, &properties);
      PathInfoToFileInfo(container, FileType::Directory, -1, properties.LastModified,
                         &info);
      results.push_back(std::move(info));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, container, "", 0, &results));
      }
    }
    return results;
  }

  if (!impl_->isHierarchicalNamespaceEnabled) {
    if (base_path.path_to_file_parts.size() > 1) {
      if (!select.allow_not_found) {
        return Status::IOError(
            "Invalid path provided, hierarchical namespace not"
            " enabled");
      }
      return results;
    }
  }

  if (base_path.path_to_file.empty() &&
      !(impl_->ContainerExists(base_path.container).ValueOrDie())) {
    if (!select.allow_not_found) {
      return Status::IOError("Invalid path provided");
    }
    return results;
  }

  if (impl_->FileExists(impl_->dfs_endpoint_url + base_path.full_path).ValueOrDie()) {
    return Status::IOError("Invalid path provided");
  }

  if (!(base_path.path_to_file.empty()) &&
      !(impl_->DirExists(impl_->dfs_endpoint_url + base_path.full_path).ValueOrDie())) {
    if (!select.allow_not_found) {
      return Status::IOError("Invalid path provided");
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
  if ((impl_->FileExists(impl_->dfs_endpoint_url + path.full_path)).ValueOrDie()) {
    return Status::IOError("Cannot create directory, file exists at path");
  }
  if (path.path_to_file.empty()) {
    // Create container
    return impl_->CreateContainer(path.container);
  }
  // Hierarchical namespace not enabled type storage accounts
  if (!impl_->isHierarchicalNamespaceEnabled) {
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
      AzurePath parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        auto exists = impl_->ContainerExists(parent_path.container);
        if (!(exists.ValueOrDie())) {
          return Status::IOError("Cannot create directory '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        auto exists = impl_->DirExists(impl_->dfs_endpoint_url + parent_path.full_path);
        if (!(exists.ValueOrDie())) {
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
  if ((impl_->FileExists(impl_->dfs_endpoint_url + path.full_path)).ValueOrDie()) {
    return Status::IOError("Cannot delete directory, file exists at path");
  }

  // Hierarchical namespace not enabled type storage accounts
  if (!impl_->isHierarchicalNamespaceEnabled) {
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
    return Status::IOError("Invalid path provided");
  }

  if (path.path_to_file.empty() &&
      !(impl_->ContainerExists(path.container).ValueOrDie())) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided");
  }

  if (impl_->FileExists(impl_->dfs_endpoint_url + path.full_path).ValueOrDie()) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided");
  }

  if (!(path.path_to_file.empty()) &&
      !(impl_->DirExists(impl_->dfs_endpoint_url + path.full_path).ValueOrDie())) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided");
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

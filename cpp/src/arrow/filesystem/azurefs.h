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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"

namespace Azure {
namespace Core {
namespace Credentials {

class TokenCredential;

}  // namespace Credentials
}  // namespace Core
namespace Storage {

class StorageSharedKeyCredential;

}  // namespace Storage
}  // namespace Azure

namespace arrow {
namespace fs {

enum class AzureCredentialsKind : int8_t {
  /// Anonymous access (no credentials used), public
  Anonymous,
  /// Use explicitly-provided access key pair
  StorageCredentials,
  /// Use ServicePrincipleCredentials
  ServicePrincipleCredentials,
  /// Use Sas Token to authenticate
  Sas,
  /// Use Connection String
  ConnectionString
};

/// Options for the AzureFileSystem implementation.
struct ARROW_EXPORT AzureOptions {
  std::string scheme;
  std::string account_dfs_url;
  std::string account_blob_url;
  bool is_azurite = false;
  AzureCredentialsKind credentials_kind = AzureCredentialsKind::Anonymous;

  std::string sas_token;
  std::string connection_string;
  std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>
      storage_credentials_provider;
  std::shared_ptr<Azure::Core::Credentials::TokenCredential>
      service_principle_credentials_provider;

  AzureOptions();

  Result<std::string> GetAccountNameFromConnectionString(
      const std::string& connection_string);

  Status ConfigureAnonymousCredentials(const std::string& account_name);

  Status ConfigureAccountKeyCredentials(const std::string& account_name,
                                        const std::string& account_key);

  Status ConfigureConnectionStringCredentials(const std::string& connection_string);

  Status ConfigureServicePrincipleCredentials(const std::string& account_name,
                                              const std::string& tenant_id,
                                              const std::string& client_id,
                                              const std::string& client_secret);

  Status ConfigureSasCredentials(const std::string& sas_token);

  bool Equals(const AzureOptions& other) const;

  static Result<AzureOptions> FromAnonymous(const std::string& account_name);

  static Result<AzureOptions> FromAccountKey(const std::string& account_name,
                                             const std::string& account_key);

  // https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
  static Result<AzureOptions> FromConnectionString(const std::string& connection_string);

  static Result<AzureOptions> FromServicePrincipleCredential(
      const std::string& account_name, const std::string& tenant_id,
      const std::string& client_id, const std::string& client_secret);

  static Result<AzureOptions> FromSas(const std::string& uri);

  static Result<AzureOptions> FromUri(const ::arrow::internal::Uri& uri,
                                      std::string* out_path = NULLPTR);
  static Result<AzureOptions> FromUri(const std::string& uri,
                                      std::string* out_path = NULLPTR);
};

class ARROW_EXPORT AzureBlobFileSystem : public FileSystem {
 public:
  ~AzureBlobFileSystem() override;

  std::string type_name() const override { return "abfs"; }

  /// Return the original Azure options when constructing the filesystem
  AzureOptions options() const;

  bool Equals(const FileSystem& other) const override;

  /// \cond FALSE
  using FileSystem::GetFileInfo;
  /// \endcond
  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) override;

  /// FileInfoGenerator GetFileInfoGenerator(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path, bool missing_dir_ok = false) override;
  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  static Result<std::shared_ptr<AzureBlobFileSystem>> Make(
      const AzureOptions& options, const io::IOContext& = io::default_io_context());

 protected:
  explicit AzureBlobFileSystem(const AzureOptions& options, const io::IOContext&);

  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow

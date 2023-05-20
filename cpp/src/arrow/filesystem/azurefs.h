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

  bool Equals(const AzureOptions& other) const;
};

/// \brief Azure-backed FileSystem implementation for ABFS and ADLS.
///
/// TODO: GH-18014 Complete the internal implementation
class ARROW_EXPORT AzureFileSystem : public FileSystem {
 public:
  ~AzureFileSystem() override = default;

  std::string type_name() const override { return "abfs"; }

  /// Return the original Azure options when constructing the filesystem
  AzureOptions options() const;

  virtual bool Equals(const FileSystem& other) const override;

  virtual Result<FileInfo> GetFileInfo(const std::string& path) override;

  virtual Result<FileInfoVector> GetFileInfo(const FileSelector& select) override;

  virtual Status CreateDir(const std::string& path, bool recursive = true) override;

  virtual Status DeleteDir(const std::string& path) override;

  virtual Status DeleteDirContents(const std::string& path, bool missing_dir_ok = false) override;

  virtual Status DeleteRootDirContents() override;

  virtual Status DeleteFile(const std::string& path) override;

  virtual Status Move(const std::string& src, const std::string& dest) override;

  virtual Status CopyFile(const std::string& src, const std::string& dest) override;

  virtual Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;

  virtual Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  virtual Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  virtual Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  virtual Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  virtual Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  static Result<std::shared_ptr<AzureFileSystem>> Make(
      const AzureOptions& options, const io::IOContext& = io::default_io_context());

 private:
  explicit AzureFileSystem(const AzureOptions& options, const io::IOContext& io_context);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow

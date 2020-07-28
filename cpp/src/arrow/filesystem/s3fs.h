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

namespace Aws {
namespace Auth {

class AWSCredentialsProvider;

}  // namespace Auth
}  // namespace Aws

namespace arrow {
namespace fs {

extern ARROW_EXPORT const char* kS3DefaultRegion;

/// Options for the S3FileSystem implementation.
struct ARROW_EXPORT S3Options {
  /// AWS region to connect to (default "us-east-1")
  std::string region = kS3DefaultRegion;

  /// If non-empty, override region with a connect string such as "localhost:9000"
  // XXX perhaps instead take a URL like "http://localhost:9000"?
  std::string endpoint_override;
  /// S3 connection transport, default "https"
  std::string scheme = "https";

  /// AWS credentials provider
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;

  /// Whether OutputStream writes will be issued in the background, without blocking.
  bool background_writes = true;

  /// Configure with the default AWS credentials provider chain.
  void ConfigureDefaultCredentials();

  /// Configure with anonymous credentials.  This will only let you access public buckets.
  void ConfigureAnonymousCredentials();

  /// Configure with explicit access and secret key.
  void ConfigureAccessKey(const std::string& access_key, const std::string& secret_key);

  std::string GetAccessKey() const;
  std::string GetSecretKey() const;

  bool Equals(const S3Options& other) const;

  /// \brief Initialize with default credentials provider chain
  ///
  /// This is recommended if you use the standard AWS environment variables
  /// and/or configuration file.
  static S3Options Defaults();
  /// \brief Initialize with anonymous credentials.
  ///
  /// This will only let you access public buckets.
  static S3Options Anonymous();
  /// \brief Initialize with explicit access and secret key
  static S3Options FromAccessKey(const std::string& access_key,
                                 const std::string& secret_key);

  static Result<S3Options> FromUri(const ::arrow::internal::Uri& uri,
                                   std::string* out_path = NULLPTR);
  static Result<S3Options> FromUri(const std::string& uri,
                                   std::string* out_path = NULLPTR);
};

/// S3-backed FileSystem implementation.
///
/// Some implementation notes:
/// - buckets are special and the operations available on them may be limited
///   or more expensive than desired.
class ARROW_EXPORT S3FileSystem : public FileSystem {
 public:
  ~S3FileSystem() override;

  std::string type_name() const override { return "s3"; }
  S3Options options() const;

  bool Equals(const FileSystem& other) const override;

  /// \cond FALSE
  using FileSystem::GetFileInfo;
  /// \endcond
  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path) override;
  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  /// Create a sequential input stream for reading from a S3 object.
  ///
  /// NOTE: Reads from the stream will be synchronous and unbuffered.
  /// You way want to wrap the stream in a BufferedInputStream or use
  /// a custom readahead strategy to avoid idle waits.
  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  /// Create a sequential input stream for reading from a S3 object.
  ///
  /// This override avoids a HEAD request by assuming the FileInfo
  /// contains correct information.
  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  /// Create a random access file for reading from a S3 object.
  ///
  /// See OpenInputStream for performance notes.
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  /// Create a random access file for reading from a S3 object.
  ///
  /// This override avoids a HEAD request by assuming the FileInfo
  /// contains correct information.
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  /// Create a sequential output stream for writing to a S3 object.
  ///
  /// NOTE: Writes to the stream will be buffered.  Depending on
  /// S3Options.background_writes, they can be synchronous or not.
  /// It is recommended to enable background_writes unless you prefer
  /// implementing your own background execution strategy.
  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path) override;

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path) override;

  /// Create a S3FileSystem instance from the given options.
  static Result<std::shared_ptr<S3FileSystem>> Make(const S3Options& options);

 protected:
  explicit S3FileSystem(const S3Options& options);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

enum class S3LogLevel : int8_t { Off, Fatal, Error, Warn, Info, Debug, Trace };

struct ARROW_EXPORT S3GlobalOptions {
  S3LogLevel log_level;
};

/// Initialize the S3 APIs.  It is required to call this function at least once
/// before using S3FileSystem.
ARROW_EXPORT
Status InitializeS3(const S3GlobalOptions& options);

/// Ensure the S3 APIs are initialized, but only if not already done.
/// If necessary, this will call InitializeS3() with some default options.
ARROW_EXPORT
Status EnsureS3Initialized();

/// Shutdown the S3 APIs.
ARROW_EXPORT
Status FinalizeS3();

}  // namespace fs
}  // namespace arrow

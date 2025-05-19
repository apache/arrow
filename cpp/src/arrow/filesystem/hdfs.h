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
#include "arrow/util/uri.h"

namespace arrow::io::internal {
class HdfsReadableFile;
class HdfsOutputStream;

struct HdfsPathInfo;
}  // namespace arrow::io::internal

namespace arrow::fs {

struct HdfsConnectionConfig {
  std::string host;
  int port;
  std::string user;
  std::string kerb_ticket;
  std::unordered_map<std::string, std::string> extra_conf;
};

/// Options for the HDFS implementation.
struct ARROW_EXPORT HdfsOptions {
  HdfsOptions() = default;
  ~HdfsOptions() = default;

  /// Hdfs configuration options, contains host, port, driver
  HdfsConnectionConfig connection_config;

  /// Used by Hdfs OpenWritable Interface.
  int32_t buffer_size = 0;
  int16_t replication = 3;
  int64_t default_block_size = 0;

  void ConfigureEndPoint(std::string host, int port);
  void ConfigureReplication(int16_t replication);
  void ConfigureUser(std::string user_name);
  void ConfigureBufferSize(int32_t buffer_size);
  void ConfigureBlockSize(int64_t default_block_size);
  void ConfigureKerberosTicketCachePath(std::string path);
  void ConfigureExtraConf(std::string key, std::string val);

  bool Equals(const HdfsOptions& other) const;

  static Result<HdfsOptions> FromUri(const ::arrow::util::Uri& uri);
  static Result<HdfsOptions> FromUri(const std::string& uri);
};

/// HDFS-backed FileSystem implementation.
class ARROW_EXPORT HadoopFileSystem : public FileSystem {
 public:
  ~HadoopFileSystem() override;

  std::string type_name() const override { return "hdfs"; }
  HdfsOptions options() const;
  bool Equals(const FileSystem& other) const override;
  Result<std::string> PathFromUri(const std::string& uri_string) const override;

  /// \cond FALSE
  using FileSystem::CreateDir;
  using FileSystem::DeleteDirContents;
  using FileSystem::GetFileInfo;
  using FileSystem::OpenAppendStream;
  using FileSystem::OpenOutputStream;
  /// \endcond

  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive) override;

  Status DeleteDir(const std::string& path) override;

  Status DeleteDirContents(const std::string& path, bool missing_dir_ok) override;

  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status MakeDirectory(const std::string& path);

  bool Exists(const std::string& path);

  Status GetPathInfoStatus(const std::string& path, io::internal::HdfsPathInfo* info);

  Status ListDirectory(const std::string& path,
                       std::vector<io::internal::HdfsPathInfo>* listing);

  // Delete file or directory
  // @param path absolute path to data
  // @param recursive if path is a directory, delete contents as well
  // @returns error status on failure
  Status Delete(const std::string& path, bool recursive = false);

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  // Move file or directory from source path to destination path within the
  // current filesystem
  Status Rename(const std::string& src, const std::string& dst);

  Status Copy(const std::string& src, const std::string& dst);

  Status GetCapacity(int64_t* nbytes);

  Status GetUsed(int64_t* nbytes);

  /// Change
  ///
  /// @param path file path to change
  /// @param owner pass null for no change
  /// @param group pass null for no change
  Status Chown(const std::string& path, const char* owner, const char* group);

  /// Change path permissions
  ///
  /// \param path Absolute path in file system
  /// \param mode Mode bitset
  /// \return Status
  Status Chmod(const std::string& path, int mode);

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) override;
  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata) override;

  /// Create a HdfsFileSystem instance from the given options.
  static Result<std::shared_ptr<HadoopFileSystem>> Make(
      const HdfsOptions& options, const io::IOContext& = io::default_io_context());

  // Open an HDFS file in READ mode. Returns error
  // status if the file is not found.
  //
  // @param path complete file path
  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      std::shared_ptr<arrow::io::internal::HdfsReadableFile>* file);

  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      const io::IOContext& io_context,
                      std::shared_ptr<arrow::io::internal::HdfsReadableFile>* file);

  Status OpenReadable(const std::string& path,
                      std::shared_ptr<arrow::io::internal::HdfsReadableFile>* file);

  Status OpenReadable(const std::string& path, const io::IOContext& io_context,
                      std::shared_ptr<arrow::io::internal::HdfsReadableFile>* file);

  // FileMode::WRITE options
  // @param path complete file path
  // @param buffer_size 0 by default
  // @param replication 0 by default
  // @param default_block_size 0 by default
  Status OpenWritable(const std::string& path, bool append, int32_t buffer_size,
                      int16_t replication, int64_t default_block_size,
                      std::shared_ptr<arrow::io::internal::HdfsOutputStream>* file);

  Status OpenWritable(const std::string& path, bool append,
                      std::shared_ptr<arrow::io::internal::HdfsOutputStream>* file);

 protected:
  HadoopFileSystem(const HdfsOptions& options, const io::IOContext&);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

ARROW_EXPORT Status HaveLibHdfs();

}  // namespace arrow::fs

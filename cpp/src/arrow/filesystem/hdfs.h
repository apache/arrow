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
#include <unordered_map>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace fs {

namespace internal {
class HdfsReadableFile;
class HdfsOutputStream;
}  // namespace internal

enum class HadoopDriver : char { LIBHDFS, LIBHDFS3 };

struct HadoopOptions {
  std::string host;
  int port;
  std::string user;
  std::string kerb_ticket;
  std::unordered_map<std::string, std::string> extra_conf;
  HadoopDriver driver;
  // TODO(bkietz) add replication, buffer_size, ...
};

// TODO(bkietz) delete this in favor of fs::FileType
struct ObjectType {
  using type = FileType;
  static constexpr type FILE = FileType::File;
  static constexpr type DIRECTORY = FileType::Directory;
};

struct HadoopPathInfo {
  // TODO(bkietz) consolidate with FileStats
  ObjectType::type kind;

  std::string name;
  std::string owner;
  std::string group;

  // Access times in UNIX timestamps (seconds)
  int64_t size;
  int64_t block_size;

  int32_t last_modified_time;
  int32_t last_access_time;

  int16_t replication;
  int16_t permissions;
};

class ARROW_EXPORT HadoopFileSystem : public FileSystem {
 public:
  /// Connect to an HDFS cluster given a configuration
  ///
  /// \param config (in): configuration for connecting
  /// \param fs (out): the created client
  /// \returns Status
  static Status Connect(const HadoopOptions& options,
                        std::shared_ptr<HadoopFileSystem>* fs);

  /// Disconnect from cluster
  ///
  /// \returns Status
  Status Disconnect();

  using FileSystem::GetTargetStats;
  Status GetTargetStats(const std::string& path, FileStats* out) override;
  Status GetTargetStats(const Selector& select, std::vector<FileStats>* out) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;

  Status DeleteDirContents(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Status OpenInputStream(const std::string& path,
                         std::shared_ptr<io::InputStream>* out) override;

  Status OpenInputFile(const std::string& path,
                       std::shared_ptr<io::RandomAccessFile>* out) override;

  Status OpenOutputStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) override;

  Status OpenAppendStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) override;

  /// \param path (in): absolute HDFS path
  /// \returns bool, true if the path exists, false if not (or on error)
  bool Exists(const std::string& path);

  /// \param path (in): absolute HDFS path
  /// \param info (out)
  /// \returns Status
  Status GetPathInfo(const std::string& path, HadoopPathInfo* info);

  /// \param nbytes (out): total capacity of the filesystem
  /// \returns Status
  Status GetCapacity(int64_t* nbytes);

  /// \param nbytes (out): total bytes used of the filesystem
  /// \returns Status
  Status GetUsed(int64_t* nbytes);

  /// Change
  ///
  /// \param path file path to change
  /// \param owner pass null for no change
  /// \param group pass null for no change
  Status Chown(const std::string& path, const char* owner, const char* group);

  /// Change path permissions
  ///
  /// \param path Absolute path in file system
  /// \param mode Mode bitset
  /// \return Status
  Status Chmod(const std::string& path, int mode);

  // TODO(wesm): GetWorkingDirectory, SetWorkingDirectory

  /// Open an HDFS file in READ mode. Returns error
  /// status if the file is not found.
  ///
  /// \param path complete file path
  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      std::shared_ptr<io::RandomAccessFile>* file);

  Status OpenReadable(const std::string& path,
                      std::shared_ptr<io::RandomAccessFile>* file);

  /// FileMode::WRITE options
  ///
  /// \param path complete file path
  /// \param buffer_size, 0 for default
  /// \param replication, 0 for default
  /// \param default_block_size, 0 for default
  Status OpenWritable(const std::string& path, bool append, int32_t buffer_size,
                      int16_t replication, int64_t default_block_size,
                      std::shared_ptr<io::OutputStream>* file);

  Status OpenWritable(const std::string& path, bool append,
                      std::shared_ptr<io::OutputStream>* file);

 private:
  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;

  HadoopFileSystem();
  ARROW_DISALLOW_COPY_AND_ASSIGN(HadoopFileSystem);
};

Status ARROW_EXPORT HaveLibHdfs();
Status ARROW_EXPORT HaveLibHdfs3();

}  // namespace fs
}  // namespace arrow

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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/filesystem/filesystem_utils.h"
#ifdef ARROW_HDFS
#include "arrow/filesystem/hdfs.h"
#endif
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/uri.h"

namespace arrow {

namespace fs {

enum class FileSystemType { HDFS, LOCAL, S3, UNKNOWN };

namespace {

class PathInfo {
 public:
  PathInfo() {}
  ~PathInfo() {}

  static Status Make(const std::string& full_path, std::shared_ptr<PathInfo>* path_info) {
    *path_info = std::make_shared<PathInfo>();
    RETURN_NOT_OK((*path_info)->Init(full_path));
    return Status::OK();
  }

  Status Init(const std::string& full_path) {
    RETURN_NOT_OK(ParseURI(full_path));
    return Status::OK();
  }

  FileSystemType GetFileSystemType() { return fs_type_; }

  std::string GetHostName() {
    auto search = options_.find("host_name");
    if (search == options_.end()) {
      return "";
    }
    return search->second;
  }

  int GetHostPort() {
    auto search = options_.find("host_port");
    if (search == options_.end()) {
      return -1;
    }
    std::string port_text = search->second;
    return std::stoi(port_text);
  }

  std::string GetUser() {
    auto search = options_.find("user");
    if (search != options_.end()) {
      return search->second;
    }
    return "";
  }

  bool GetIfUseHdfs3() {
    auto search = options_.find("use_hdfs3");
    if (search != options_.end()) {
      if (search->second.compare("1") == 0) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  int GetRepsNum() {
    auto search = options_.find("replication");
    if (search != options_.end()) {
      if (search->second.empty()) {
        return 3;
      } else {
        return std::stoi(search->second);
      }
    } else {
      return 3;
    }
  }

 private:
  FileSystemType GetFileSystemTypeFromString(const std::string& s) {
    if (s == "hdfs") {
      return FileSystemType::HDFS;
    }
    if (s == "http") {
      return FileSystemType::S3;
    }
    if (s == "https") {
      return FileSystemType::S3;
    }
    if (s == "file") {
      return FileSystemType::LOCAL;
    }
    if (s.empty()) {
      return FileSystemType::LOCAL;
    }
    return FileSystemType::UNKNOWN;
  }

  Status ParseURI(const std::string& s) {
    arrow::internal::Uri uri;
    RETURN_NOT_OK(uri.Parse(s));
    fs_type_ = GetFileSystemTypeFromString(uri.scheme());
    switch (fs_type_) {
      case FileSystemType::HDFS:
        options_.emplace("host_name", uri.host());
        options_.emplace("host_port", uri.port_text());
        RETURN_NOT_OK(ParseOptions(&uri));
        break;
      case FileSystemType::LOCAL:
        RETURN_NOT_OK(ParseOptions(&uri));
        break;
      case FileSystemType::S3:
        return Status::NotImplemented("S3 is not supported yet.");
      default:
        break;
    }
    return Status::OK();
  }

  Status ParseOptions(arrow::internal::Uri* uri) {
    ARROW_ASSIGN_OR_RAISE(auto options, uri->query_items());
    for (auto option : options) {
      options_.emplace(option.first, option.second);
    }
    return Status::OK();
  }

  FileSystemType fs_type_;
  std::unordered_map<std::string, std::string> options_;
};
}  // namespace

Status MakeFileSystem(const std::string& full_path, std::shared_ptr<FileSystem>* fs) {
  std::shared_ptr<PathInfo> path_info;
  RETURN_NOT_OK(PathInfo::Make(full_path, &path_info));
  FileSystemType fs_type = path_info->GetFileSystemType();

  switch (fs_type) {
#ifdef ARROW_HDFS
    case FileSystemType::HDFS: {
      // Init Hdfs FileSystem
      HdfsOptions hdfs_options;
      hdfs_options.ConfigureEndPoint(path_info->GetHostName(), path_info->GetHostPort());
      hdfs_options.ConfigureHdfsDriver(path_info->GetIfUseHdfs3());
      hdfs_options.ConfigureHdfsReplication(path_info->GetRepsNum());
      hdfs_options.ConfigureHdfsUser(path_info->GetUser());

      std::shared_ptr<HadoopFileSystem> hdfs;
      RETURN_NOT_OK(HadoopFileSystem::Make(hdfs_options, &hdfs));

      *fs = hdfs;
    } break;
#endif
    case FileSystemType::LOCAL: {
      auto local_fs = std::make_shared<LocalFileSystem>();
      *fs = local_fs;
    } break;
#ifdef ARROW_S3
    case FileSystemType::S3:
      return Status::NotImplemented("S3 is not supported yet.");
#endif
    default:
      return Status::NotImplemented("This type of filesystem is not supported yet.");
  }

  return Status::OK();
}
}  // namespace fs
}  // namespace arrow

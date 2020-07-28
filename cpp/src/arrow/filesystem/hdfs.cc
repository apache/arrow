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

#include <chrono>
#include <cstring>
#include <unordered_map>
#include <utility>

#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/hdfs.h"
#include "arrow/io/hdfs_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {

using internal::ParseValue;
using internal::Uri;

namespace fs {

using internal::GetAbstractPathParent;
using internal::MakeAbstractPathRelative;
using internal::RemoveLeadingSlash;

class HadoopFileSystem::Impl {
 public:
  explicit Impl(HdfsOptions options) : options_(std::move(options)) {}

  ~Impl() {
    Status st = Close();
    if (!st.ok()) {
      ARROW_LOG(WARNING) << "Failed to disconnect hdfs client: " << st.ToString();
    }
  }

  Status Init() {
    io::internal::LibHdfsShim* driver_shim;
    RETURN_NOT_OK(ConnectLibHdfs(&driver_shim));
    RETURN_NOT_OK(io::HadoopFileSystem::Connect(&options_.connection_config, &client_));
    return Status::OK();
  }

  Status Close() {
    if (client_) {
      RETURN_NOT_OK(client_->Disconnect());
    }
    return Status::OK();
  }

  HdfsOptions options() const { return options_; }

  Result<FileInfo> GetFileInfo(const std::string& path) {
    FileInfo info;
    io::HdfsPathInfo path_info;
    auto status = client_->GetPathInfo(path, &path_info);
    info.set_path(path);
    if (status.IsIOError()) {
      info.set_type(FileType::NotFound);
      return info;
    }

    PathInfoToFileInfo(path_info, &info);
    return info;
  }

  Status StatSelector(const std::string& wd, const std::string& path,
                      const FileSelector& select, int nesting_depth,
                      std::vector<FileInfo>* out) {
    std::vector<io::HdfsPathInfo> children;
    Status st = client_->ListDirectory(path, &children);
    if (!st.ok()) {
      if (select.allow_not_found) {
        ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(path));
        if (info.type() == FileType::NotFound) {
          return Status::OK();
        }
      }
      return st;
    }
    for (const auto& child_path_info : children) {
      // HDFS returns an absolute URI here, need to extract path relative to wd
      Uri uri;
      RETURN_NOT_OK(uri.Parse(child_path_info.name));
      std::string child_path = uri.path();
      if (!wd.empty()) {
        ARROW_ASSIGN_OR_RAISE(child_path, MakeAbstractPathRelative(wd, child_path));
      }

      FileInfo info;
      info.set_path(child_path);
      PathInfoToFileInfo(child_path_info, &info);
      const bool is_dir = info.type() == FileType::Directory;
      out->push_back(std::move(info));
      if (is_dir && select.recursive && nesting_depth < select.max_recursion) {
        RETURN_NOT_OK(StatSelector(wd, child_path, select, nesting_depth + 1, out));
      }
    }
    return Status::OK();
  }

  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) {
    std::vector<FileInfo> results;

    std::string wd;
    if (select.base_dir.empty() || select.base_dir.front() != '/') {
      // Fetch working directory, because we need to trim it from the start
      // of paths returned by ListDirectory as select.base_dir is relative.
      RETURN_NOT_OK(client_->GetWorkingDirectory(&wd));
      Uri wd_uri;
      RETURN_NOT_OK(wd_uri.Parse(wd));
      wd = wd_uri.path();
    }

    ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(select.base_dir));
    if (info.type() == FileType::File) {
      return Status::Invalid(
          "GetFileInfo expects base_dir of selector to be a directory, while '",
          select.base_dir, "' is a file");
    }
    RETURN_NOT_OK(StatSelector(wd, select.base_dir, select, 0, &results));
    return results;
  }

  Status CreateDir(const std::string& path, bool recursive) {
    if (IsDirectory(path)) {
      return Status::OK();
    }
    if (!recursive) {
      const auto parent = GetAbstractPathParent(path).first;
      if (!parent.empty() && !IsDirectory(parent)) {
        return Status::IOError("Cannot create directory '", path,
                               "': parent is not a directory");
      }
    }
    RETURN_NOT_OK(client_->MakeDirectory(path));
    return Status::OK();
  }

  Status DeleteDir(const std::string& path) {
    if (!IsDirectory(path)) {
      return Status::IOError("Cannot delete directory '", path, "': not a directory");
    }
    RETURN_NOT_OK(client_->DeleteDirectory(path));
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& path) {
    std::vector<std::string> file_list;
    RETURN_NOT_OK(client_->GetChildren(path, &file_list));
    for (auto file : file_list) {
      RETURN_NOT_OK(client_->Delete(file, /*recursive=*/true));
    }
    return Status::OK();
  }

  Status DeleteFile(const std::string& path) {
    if (IsDirectory(path)) {
      return Status::IOError("path is a directory");
    }
    RETURN_NOT_OK(client_->Delete(path));
    return Status::OK();
  }

  Status Move(const std::string& src, const std::string& dest) {
    RETURN_NOT_OK(client_->Rename(src, dest));
    return Status::OK();
  }

  Status CopyFile(const std::string& src, const std::string& dest) {
    // TODO implement this (but only if HDFS supports on-server copy)
    return Status::NotImplemented("HadoopFileSystem::CopyFile is not supported yet");
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const std::string& path) {
    std::shared_ptr<io::HdfsReadableFile> file;
    RETURN_NOT_OK(client_->OpenReadable(path, &file));
    return file;
  }

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(const std::string& path) {
    std::shared_ptr<io::HdfsReadableFile> file;
    RETURN_NOT_OK(client_->OpenReadable(path, &file));
    return file;
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(const std::string& path) {
    bool append = false;
    return OpenOutputStreamGeneric(path, append);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(const std::string& path) {
    bool append = true;
    return OpenOutputStreamGeneric(path, append);
  }

 protected:
  HdfsOptions options_;
  std::shared_ptr<::arrow::io::HadoopFileSystem> client_;

  void PathInfoToFileInfo(const io::HdfsPathInfo& info, FileInfo* out) {
    if (info.kind == io::ObjectType::DIRECTORY) {
      out->set_type(FileType::Directory);
      out->set_size(kNoSize);
    } else if (info.kind == io::ObjectType::FILE) {
      out->set_type(FileType::File);
      out->set_size(info.size);
    }
    out->set_mtime(ToTimePoint(info.last_modified_time));
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStreamGeneric(
      const std::string& path, bool append) {
    std::shared_ptr<io::HdfsOutputStream> stream;
    RETURN_NOT_OK(client_->OpenWritable(path, append, options_.buffer_size,
                                        options_.replication, options_.default_block_size,
                                        &stream));
    return stream;
  }

  bool IsDirectory(const std::string& path) {
    io::HdfsPathInfo info;
    Status status = client_->GetPathInfo(path, &info);
    if (!status.ok()) {
      return false;
    }
    if (info.kind == io::ObjectType::DIRECTORY) {
      return true;
    }
    return false;
  }

  TimePoint ToTimePoint(int secs) {
    std::chrono::nanoseconds ns_count(static_cast<int64_t>(secs) * 1000000000);
    return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
  }
};

void HdfsOptions::ConfigureEndPoint(std::string host, int port) {
  connection_config.host = std::move(host);
  connection_config.port = port;
}

void HdfsOptions::ConfigureUser(std::string user_name) {
  connection_config.user = std::move(user_name);
}

void HdfsOptions::ConfigureKerberosTicketCachePath(std::string path) {
  connection_config.kerb_ticket = std::move(path);
}

void HdfsOptions::ConfigureReplication(int16_t replication) {
  this->replication = replication;
}

void HdfsOptions::ConfigureBufferSize(int32_t buffer_size) {
  this->buffer_size = buffer_size;
}

void HdfsOptions::ConfigureBlockSize(int64_t default_block_size) {
  this->default_block_size = default_block_size;
}

void HdfsOptions::ConfigureExtraConf(std::string key, std::string val) {
  connection_config.extra_conf.emplace(std::move(key), std::move(val));
}

bool HdfsOptions::Equals(const HdfsOptions& other) const {
  return (buffer_size == other.buffer_size && replication == other.replication &&
          default_block_size == other.default_block_size &&
          connection_config.host == other.connection_config.host &&
          connection_config.port == other.connection_config.port &&
          connection_config.user == other.connection_config.user &&
          connection_config.kerb_ticket == other.connection_config.kerb_ticket &&
          connection_config.extra_conf == other.connection_config.extra_conf);
}

Result<HdfsOptions> HdfsOptions::FromUri(const Uri& uri) {
  HdfsOptions options;

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  std::string host;
  host = uri.scheme() + "://" + uri.host();

  // configure endpoint
  const auto port = uri.port();
  if (port == -1) {
    // default port will be determined by hdfs FileSystem impl
    options.ConfigureEndPoint(host, 0);
  } else {
    options.ConfigureEndPoint(host, port);
  }

  // configure replication
  auto it = options_map.find("replication");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int16_t replication;
    if (!ParseValue<Int16Type>(v.data(), v.size(), &replication)) {
      return Status::Invalid("Invalid value for option 'replication': '", v, "'");
    }
    options.ConfigureReplication(replication);
    options_map.erase(it);
  }

  // configure buffer_size
  it = options_map.find("buffer_size");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int32_t buffer_size;
    if (!ParseValue<Int32Type>(v.data(), v.size(), &buffer_size)) {
      return Status::Invalid("Invalid value for option 'buffer_size': '", v, "'");
    }
    options.ConfigureBufferSize(buffer_size);
    options_map.erase(it);
  }

  // configure default_block_size
  it = options_map.find("default_block_size");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int64_t default_block_size;
    if (!ParseValue<Int64Type>(v.data(), v.size(), &default_block_size)) {
      return Status::Invalid("Invalid value for option 'default_block_size': '", v, "'");
    }
    options.ConfigureBlockSize(default_block_size);
    options_map.erase(it);
  }

  // configure user
  it = options_map.find("user");
  if (it != options_map.end()) {
    const auto& user = it->second;
    options.ConfigureUser(user);
    options_map.erase(it);
  }

  // configure kerberos
  it = options_map.find("kerb_ticket");
  if (it != options_map.end()) {
    const auto& ticket = it->second;
    options.ConfigureKerberosTicketCachePath(ticket);
    options_map.erase(it);
  }

  // configure other options
  for (const auto& it : options_map) {
    options.ConfigureExtraConf(it.first, it.second);
  }

  return options;
}

Result<HdfsOptions> HdfsOptions::FromUri(const std::string& uri_string) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri);
}

HadoopFileSystem::HadoopFileSystem(const HdfsOptions& options)
    : impl_(new Impl{options}) {}

HadoopFileSystem::~HadoopFileSystem() {}

Result<std::shared_ptr<HadoopFileSystem>> HadoopFileSystem::Make(
    const HdfsOptions& options) {
  std::shared_ptr<HadoopFileSystem> ptr(new HadoopFileSystem(options));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

Result<FileInfo> HadoopFileSystem::GetFileInfo(const std::string& path) {
  return impl_->GetFileInfo(path);
}

HdfsOptions HadoopFileSystem::options() const { return impl_->options(); }

bool HadoopFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& hdfs = ::arrow::internal::checked_cast<const HadoopFileSystem&>(other);
  return options().Equals(hdfs.options());
}

Result<std::vector<FileInfo>> HadoopFileSystem::GetFileInfo(const FileSelector& select) {
  return impl_->GetFileInfo(select);
}

Status HadoopFileSystem::CreateDir(const std::string& path, bool recursive) {
  return impl_->CreateDir(path, recursive);
}

Status HadoopFileSystem::DeleteDir(const std::string& path) {
  return impl_->DeleteDir(path);
}

Status HadoopFileSystem::DeleteDirContents(const std::string& path) {
  if (internal::IsEmptyPath(path)) {
    return internal::InvalidDeleteDirContents(path);
  }
  return impl_->DeleteDirContents(path);
}

Status HadoopFileSystem::DeleteRootDirContents() { return impl_->DeleteDirContents(""); }

Status HadoopFileSystem::DeleteFile(const std::string& path) {
  return impl_->DeleteFile(path);
}

Status HadoopFileSystem::Move(const std::string& src, const std::string& dest) {
  return impl_->Move(src, dest);
}

Status HadoopFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return impl_->CopyFile(src, dest);
}

Result<std::shared_ptr<io::InputStream>> HadoopFileSystem::OpenInputStream(
    const std::string& path) {
  return impl_->OpenInputStream(path);
}

Result<std::shared_ptr<io::RandomAccessFile>> HadoopFileSystem::OpenInputFile(
    const std::string& path) {
  return impl_->OpenInputFile(path);
}

Result<std::shared_ptr<io::OutputStream>> HadoopFileSystem::OpenOutputStream(
    const std::string& path) {
  return impl_->OpenOutputStream(path);
}

Result<std::shared_ptr<io::OutputStream>> HadoopFileSystem::OpenAppendStream(
    const std::string& path) {
  return impl_->OpenAppendStream(path);
}

}  // namespace fs
}  // namespace arrow

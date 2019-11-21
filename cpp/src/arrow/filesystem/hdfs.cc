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
#include "arrow/io/hdfs.h"
#include "arrow/io/hdfs_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"

#ifdef _WIN32
#ifdef DeleteFile
#undef DeleteFile
#endif
#ifdef CopyFile
#undef CopyFile
#endif
#endif

namespace arrow {

using internal::Uri;

namespace fs {

using internal::GetAbstractPathParent;
using internal::MakeAbstractPathRelative;
using internal::RemoveLeadingSlash;

static constexpr int32_t kDefaultHdfsPort = 8020;

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
    if (options_.connection_config.driver == io::HdfsDriver::LIBHDFS3) {
      RETURN_NOT_OK(ConnectLibHdfs3(&driver_shim));
    } else {
      RETURN_NOT_OK(ConnectLibHdfs(&driver_shim));
    }
    RETURN_NOT_OK(io::HadoopFileSystem::Connect(&options_.connection_config, &client_));
    return Status::OK();
  }

  Status Close() {
    if (client_) {
      RETURN_NOT_OK(client_->Disconnect());
    }
    return Status::OK();
  }

  Result<FileStats> GetTargetStats(const std::string& path) {
    FileStats st;
    io::HdfsPathInfo info;
    auto status = client_->GetPathInfo(path, &info);
    st.set_path(path);
    if (status.IsIOError()) {
      st.set_type(FileType::NonExistent);
      return st;
    }

    PathInfoToFileStats(info, &st);
    return st;
  }

  Status StatSelector(const std::string& wd, const std::string& path,
                      const Selector& select, int nesting_depth,
                      std::vector<FileStats>* out) {
    std::vector<io::HdfsPathInfo> children;
    Status st = client_->ListDirectory(path, &children);
    if (!st.ok()) {
      if (select.allow_non_existent) {
        ARROW_ASSIGN_OR_RAISE(auto stat, GetTargetStats(path));
        if (stat.type() == FileType::NonExistent) {
          return Status::OK();
        }
      }
      return st;
    }
    for (const auto& child_info : children) {
      // HDFS returns an absolute URI here, need to extract path relative to wd
      Uri uri;
      RETURN_NOT_OK(uri.Parse(child_info.name));
      std::string child_path = uri.path();
      if (!wd.empty()) {
        ARROW_ASSIGN_OR_RAISE(child_path, MakeAbstractPathRelative(wd, child_path));
      }

      FileStats stat;
      stat.set_path(child_path);
      PathInfoToFileStats(child_info, &stat);
      const bool is_dir = stat.type() == FileType::Directory;
      out->push_back(std::move(stat));
      if (is_dir && select.recursive && nesting_depth < select.max_recursion) {
        RETURN_NOT_OK(StatSelector(wd, child_path, select, nesting_depth + 1, out));
      }
    }
    return Status::OK();
  }

  Result<std::vector<FileStats>> GetTargetStats(const Selector& select) {
    std::vector<FileStats> results;

    std::string wd;
    if (select.base_dir.empty() || select.base_dir.front() != '/') {
      // Fetch working directory, because we need to trim it from the start
      // of paths returned by ListDirectory as select.base_dir is relative.
      RETURN_NOT_OK(client_->GetWorkingDirectory(&wd));
      Uri wd_uri;
      RETURN_NOT_OK(wd_uri.Parse(wd));
      wd = wd_uri.path();
    }

    ARROW_ASSIGN_OR_RAISE(auto stat, GetTargetStats(select.base_dir));
    if (stat.type() == FileType::File) {
      return Status::Invalid(
          "GetTargetStates expects base_dir of selector to be a directory, while '",
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

  void PathInfoToFileStats(const io::HdfsPathInfo& info, FileStats* out) {
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

void HdfsOptions::ConfigureEndPoint(const std::string& host, int port) {
  connection_config.host = host;
  connection_config.port = port;
}

void HdfsOptions::ConfigureHdfsDriver(bool use_hdfs3) {
  if (use_hdfs3) {
    connection_config.driver = ::arrow::io::HdfsDriver::LIBHDFS3;
  } else {
    connection_config.driver = ::arrow::io::HdfsDriver::LIBHDFS;
  }
}

void HdfsOptions::ConfigureHdfsUser(const std::string& user_name) {
  connection_config.user = user_name;
}

void HdfsOptions::ConfigureHdfsReplication(int16_t replication) {
  this->replication = replication;
}

void HdfsOptions::ConfigureHdfsBufferSize(int32_t buffer_size) {
  this->buffer_size = buffer_size;
}

void HdfsOptions::ConfigureHdfsBlockSize(int64_t default_block_size) {
  this->default_block_size = default_block_size;
}

Result<HdfsOptions> HdfsOptions::FromUri(const Uri& uri) {
  HdfsOptions options;

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  const auto port = uri.port();
  if (port == -1) {
    options.ConfigureEndPoint(uri.host(), kDefaultHdfsPort);
  } else {
    options.ConfigureEndPoint(uri.host(), port);
  }

  auto it = options_map.find("use_hdfs3");
  if (it != options_map.end()) {
    const auto& v = it->second;
    if (v == "1") {
      options.ConfigureHdfsDriver(true);
    } else if (v == "0") {
      options.ConfigureHdfsDriver(false);
    } else {
      return Status::Invalid(
          "Invalid value for option 'use_hdfs3' (allowed values are '0' and '1'): '", v,
          "'");
    }
  }
  it = options_map.find("replication");
  if (it != options_map.end()) {
    const auto& v = it->second;
    ::arrow::internal::StringConverter<Int16Type> converter;
    int16_t reps;
    if (!converter(v.data(), v.size(), &reps)) {
      return Status::Invalid("Invalid value for option 'replication': '", v, "'");
    }
    options.ConfigureHdfsReplication(reps);
  }
  it = options_map.find("user");
  if (it != options_map.end()) {
    const auto& v = it->second;
    options.ConfigureHdfsUser(v);
  }
  return options;
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

Result<FileStats> HadoopFileSystem::GetTargetStats(const std::string& path) {
  return impl_->GetTargetStats(path);
}

Result<std::vector<FileStats>> HadoopFileSystem::GetTargetStats(const Selector& select) {
  return impl_->GetTargetStats(select);
}

Status HadoopFileSystem::CreateDir(const std::string& path, bool recursive) {
  return impl_->CreateDir(path, recursive);
}

Status HadoopFileSystem::DeleteDir(const std::string& path) {
  return impl_->DeleteDir(path);
}

Status HadoopFileSystem::DeleteDirContents(const std::string& path) {
  return impl_->DeleteDirContents(path);
}

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

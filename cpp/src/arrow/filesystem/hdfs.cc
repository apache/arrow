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
#include <utility>

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#else
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#endif  // _WIN32

#include <boost/filesystem.hpp>

#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/hdfs.h"
#include "arrow/io/hdfs_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

class HadoopFileSystem::Impl {
 public:
  explicit Impl(HdfsOptions options) : options_(std::move(options)) {}
  ~Impl() { auto status = Close(); }

  Status Init() {
    io::internal::LibHdfsShim* driver_shim;
    if (options_.hdfs_config.driver == io::HdfsDriver::LIBHDFS3) {
      RETURN_NOT_OK(ConnectLibHdfs3(&driver_shim));
    } else {
      RETURN_NOT_OK(ConnectLibHdfs(&driver_shim));
    }
    RETURN_NOT_OK(io::HadoopFileSystem::Connect(&options_.hdfs_config, &client_));
    return Status::OK();
  }

  Status Close() {
    if (client_) {
      RETURN_NOT_OK(client_->Disconnect());
    }
    return Status::OK();
  }

  Status GetTargetStats(const std::string& path, FileStats* out) {
    io::HdfsPathInfo info;
    RETURN_NOT_OK(client_->GetPathInfo(path, &info));

    out->set_path(path);
    if (info.kind == io::ObjectType::DIRECTORY) {
      out->set_type(FileType::Directory);
      out->set_size(kNoSize);
    } else {
      out->set_type(FileType::File);
      out->set_size(info.size);
    }
    out->set_mtime(ToTimePoint(info.last_modified_time));
    return Status::OK();
  }

  Status GetTargetStats(const Selector& select, std::vector<FileStats>* out) {
    std::vector<std::string> file_list;
    RETURN_NOT_OK(client_->GetChildren(select.base_dir, &file_list));
    for (auto file : file_list) {
      FileStats stat;
      RETURN_NOT_OK(GetTargetStats(file, &stat));
      out->push_back(stat);
    }
    return Status::OK();
  }

  Status CreateDir(const std::string& path, bool recursive) {
    if (client_->Exists(path)) {
      return Status::OK();
    }
    RETURN_NOT_OK(client_->MakeDirectory(path));
    return Status::OK();
  }

  Status DeleteDir(const std::string& path) {
    if (!IsDirectory(path)) {
      return Status::IOError("path is not a directory");
    }
    if (!client_->Exists(path)) {
      return Status::OK();
    }
    RETURN_NOT_OK(client_->DeleteDirectory(path));
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& path) {
    if (!IsDirectory(path)) {
      return Status::IOError("path is not a directory");
    }
    std::vector<std::string> file_list;
    RETURN_NOT_OK(client_->GetChildren(path, &file_list));
    for (auto file : file_list) {
      RETURN_NOT_OK(client_->Delete(path + file));
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
    // TODO
    return Status::NotImplemented("HadoopFileSystem::CopyFile is not supported yet");
  }

  Status OpenInputStream(const std::string& path, std::shared_ptr<io::InputStream>* out) {
    // TODO
    return Status::NotImplemented(
        "HadoopFileSystem::OpenInputStream is not supported yet");
  }

  Status OpenInputFile(const std::string& path,
                       std::shared_ptr<io::RandomAccessFile>* out) {
    std::shared_ptr<io::HdfsReadableFile> file;
    RETURN_NOT_OK(client_->OpenReadable(path, &file));
    *out = file;
    return Status::OK();
  }

  Status OpenOutputStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) {
    bool append = false;
    return OpenOutputStreamGeneric(path, append, out);
  }

  Status OpenAppendStream(const std::string& path,
                          std::shared_ptr<io::OutputStream>* out) {
    bool append = true;
    return OpenOutputStreamGeneric(path, append, out);
  }

 protected:
  HdfsOptions options_;
  std::shared_ptr<::arrow::io::HadoopFileSystem> client_;

  Status OpenOutputStreamGeneric(const std::string& path, bool append,
                                 std::shared_ptr<io::OutputStream>* out) {
    std::shared_ptr<io::HdfsOutputStream> stream;
    RETURN_NOT_OK(client_->OpenWritable(path, append, options_.buffer_size,
                                        options_.replication, options_.default_block_size,
                                        &stream));
    *out = stream;
    return Status::OK();
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
  hdfs_config.host = host;
  hdfs_config.port = port;
}

void HdfsOptions::ConfigureHdfsDriver(bool use_hdfs3) {
  if (use_hdfs3) {
    hdfs_config.driver = ::arrow::io::HdfsDriver::LIBHDFS3;
  } else {
    hdfs_config.driver = ::arrow::io::HdfsDriver::LIBHDFS;
  }
}

void HdfsOptions::ConfigureHdfsUser(const std::string& user_name) {
  if (!user_name.empty()) {
    hdfs_config.user = user_name;
  }
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

HadoopFileSystem::HadoopFileSystem(const HdfsOptions& options)
    : impl_(new Impl{options}) {}

HadoopFileSystem::~HadoopFileSystem() {}

Status HadoopFileSystem::Make(const HdfsOptions& options,
                              std::shared_ptr<HadoopFileSystem>* out) {
  std::shared_ptr<HadoopFileSystem> ptr(new HadoopFileSystem(options));
  RETURN_NOT_OK(ptr->impl_->Init());
  *out = std::move(ptr);
  return Status::OK();
}

Status HadoopFileSystem::GetTargetStats(const std::string& path, FileStats* out) {
  return impl_->GetTargetStats(path, out);
}

Status HadoopFileSystem::GetTargetStats(const Selector& select,
                                        std::vector<FileStats>* out) {
  return impl_->GetTargetStats(select, out);
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

Status HadoopFileSystem::OpenInputStream(const std::string& path,
                                         std::shared_ptr<io::InputStream>* out) {
  return impl_->OpenInputStream(path, out);
}

Status HadoopFileSystem::OpenInputFile(const std::string& path,
                                       std::shared_ptr<io::RandomAccessFile>* out) {
  return impl_->OpenInputFile(path, out);
}

Status HadoopFileSystem::OpenOutputStream(const std::string& path,
                                          std::shared_ptr<io::OutputStream>* out) {
  return impl_->OpenOutputStream(path, out);
}

Status HadoopFileSystem::OpenAppendStream(const std::string& path,
                                          std::shared_ptr<io::OutputStream>* out) {
  return impl_->OpenAppendStream(path, out);
}

}  // namespace fs
}  // namespace arrow

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

#include <hdfs.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/hdfs_internal.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/hdfs.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

using std::size_t;

namespace arrow {
namespace fs {

using internal::CopyStream;
using internal::HdfsOutputStream;
using internal::HdfsReadableFile;

namespace {

std::string TranslateErrno(int error_code) {
  std::stringstream ss;
  ss << error_code << " (" << strerror(error_code) << ")";
  if (error_code == 255) {
    // Unknown error can occur if the host is correct but the port is not
    ss << " Please check that you are connecting to the correct HDFS RPC port";
  }
  return ss.str();
}

}  // namespace

#define CHECK_FAILURE(RETURN_VALUE, WHAT)                                               \
  do {                                                                                  \
    if (RETURN_VALUE == -1) {                                                           \
      return Status::IOError("HDFS ", WHAT, " failed, errno: ", TranslateErrno(errno)); \
    }                                                                                   \
  } while (0)

static constexpr int kDefaultHdfsBufferSize = 1 << 16;

// ----------------------------------------------------------------------
// File reading

namespace {

Status GetPathInfoFailed(const std::string& path) {
  std::stringstream ss;
  ss << "Calling GetPathInfo for " << path << " failed. errno: " << TranslateErrno(errno);
  return Status::IOError(ss.str());
}

}  // namespace

// ----------------------------------------------------------------------
// HDFS client

// TODO(wesm): this could throw std::bad_alloc in the course of copying strings
// into the path info object
static void SetPathInfo(const hdfsFileInfo* input, io::HdfsPathInfo* out) {
  out->kind =
      input->mKind == kObjectKindFile ? io::ObjectType::FILE : io::ObjectType::DIRECTORY;
  out->name = std::string(input->mName);
  out->owner = std::string(input->mOwner);
  out->group = std::string(input->mGroup);

  out->last_access_time = static_cast<int32_t>(input->mLastAccess);
  out->last_modified_time = static_cast<int32_t>(input->mLastMod);
  out->size = static_cast<int64_t>(input->mSize);

  out->replication = input->mReplication;
  out->block_size = input->mBlockSize;

  out->permissions = input->mPermissions;
}

// Private implementation
class HadoopFileSystem::Impl {
 public:
  Impl() : driver_(NULLPTR), port_(0), fs_(NULLPTR) {}

  Status Connect(const HadoopOptions& config) {
    if (config.driver == HadoopDriver::LIBHDFS3) {
      RETURN_NOT_OK(ConnectLibHdfs3(&driver_));
    } else {
      RETURN_NOT_OK(ConnectLibHdfs(&driver_));
    }

    // connect to HDFS with the builder object
    hdfsBuilder* builder = driver_->NewBuilder();
    if (!config.host.empty()) {
      driver_->BuilderSetNameNode(builder, config.host.c_str());
    }
    driver_->BuilderSetNameNodePort(builder, static_cast<tPort>(config.port));
    if (!config.user.empty()) {
      driver_->BuilderSetUserName(builder, config.user.c_str());
    }
    if (!config.kerb_ticket.empty()) {
      driver_->BuilderSetKerbTicketCachePath(builder, config.kerb_ticket.c_str());
    }

    for (auto& kv : config.extra_conf) {
      int ret = driver_->BuilderConfSetStr(builder, kv.first.c_str(), kv.second.c_str());
      CHECK_FAILURE(ret, "confsetstr");
    }

    driver_->BuilderSetForceNewInstance(builder);
    fs_ = driver_->BuilderConnect(builder);

    if (fs_ == nullptr) {
      return Status::IOError("HDFS connection failed");
    }
    namenode_host_ = config.host;
    port_ = config.port;
    user_ = config.user;
    kerb_ticket_ = config.kerb_ticket;

    return Status::OK();
  }

  Status MakeDirectory(const std::string& path) {
    int ret = driver_->MakeDirectory(fs_, path.c_str());
    CHECK_FAILURE(ret, "create directory");
    return Status::OK();
  }

  Status Delete(const std::string& path, bool recursive) {
    int ret = driver_->Delete(fs_, path.c_str(), static_cast<int>(recursive));
    CHECK_FAILURE(ret, "delete");
    return Status::OK();
  }

  Status Disconnect() {
    int ret = driver_->Disconnect(fs_);
    CHECK_FAILURE(ret, "hdfsFS::Disconnect");
    return Status::OK();
  }

  bool Exists(const std::string& path) {
    // hdfsExists does not distinguish between RPC failure and the file not
    // existing
    int ret = driver_->Exists(fs_, path.c_str());
    return ret == 0;
  }

  Status GetCapacity(int64_t* nbytes) {
    tOffset ret = driver_->GetCapacity(fs_);
    CHECK_FAILURE(ret, "GetCapacity");
    *nbytes = ret;
    return Status::OK();
  }

  Status GetUsed(int64_t* nbytes) {
    tOffset ret = driver_->GetUsed(fs_);
    CHECK_FAILURE(ret, "GetUsed");
    *nbytes = ret;
    return Status::OK();
  }

  Status GetPathInfo(const std::string& path, io::HdfsPathInfo* info) {
    hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path.c_str());

    if (entry == nullptr) {
      return GetPathInfoFailed(path);
    }

    SetPathInfo(entry, info);
    driver_->FreeFileInfo(entry, 1);

    return Status::OK();
  }

  Status GetChildren(const std::string& path, std::vector<std::string>* listing) {
    std::vector<io::HdfsPathInfo> detailed_listing;
    RETURN_NOT_OK(ListDirectory(path, &detailed_listing));
    for (const auto& info : detailed_listing) {
      listing->push_back(info.name);
    }
    return Status::OK();
  }

  Status ListDirectory(const std::string& path, std::vector<io::HdfsPathInfo>* listing) {
    int num_entries = 0;
    errno = 0;
    hdfsFileInfo* entries = driver_->ListDirectory(fs_, path.c_str(), &num_entries);

    if (entries == nullptr) {
      // If the directory is empty, entries is NULL but errno is 0. Non-zero
      // errno indicates error
      //
      // Note: errno is thread-local
      //
      // XXX(wesm): ARROW-2300; we found with Hadoop 2.6 that libhdfs would set
      // errno 2/ENOENT for empty directories. To be more robust to this we
      // double check this case
      if ((errno == 0) || (errno == ENOENT && Exists(path))) {
        num_entries = 0;
      } else {
        return Status::IOError("HDFS list directory failed, errno: ",
                               TranslateErrno(errno));
      }
    }

    // Allocate additional space for elements
    int vec_offset = static_cast<int>(listing->size());
    listing->resize(vec_offset + num_entries);

    for (int i = 0; i < num_entries; ++i) {
      SetPathInfo(entries + i, &(*listing)[vec_offset + i]);
    }

    // Free libhdfs file info
    driver_->FreeFileInfo(entries, num_entries);

    return Status::OK();
  }

  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      std::shared_ptr<io::RandomAccessFile>* file) {
    hdfsFile handle = driver_->OpenFile(fs_, path.c_str(), O_RDONLY, buffer_size, 0, 0);

    if (handle == nullptr) {
      const char* msg = !Exists(path) ? "HDFS file does not exist: "
                                      : "HDFS path exists, but opening file failed: ";
      return Status::IOError(msg, path);
    }

    *file = std::make_shared<HdfsReadableFile>(path, driver_, fs_, handle, buffer_size);
    return Status::OK();
  }

  Status OpenWritable(const std::string& path, bool append, int32_t buffer_size,
                      int16_t replication, int64_t default_block_size,
                      std::shared_ptr<io::OutputStream>* file) {
    int flags = O_WRONLY;
    if (append) flags |= O_APPEND;

    hdfsFile handle =
        driver_->OpenFile(fs_, path.c_str(), flags, buffer_size, replication,
                          static_cast<tSize>(default_block_size));

    if (handle == nullptr) {
      return Status::IOError("Unable to open file ", path);
    }

    *file = std::make_shared<HdfsOutputStream>(path, driver_, fs_, handle);
    return Status::OK();
  }

  Status Rename(const std::string& src, const std::string& dst) {
    int ret = driver_->Rename(fs_, src.c_str(), dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

  Status Chmod(const std::string& path, int mode) {
    int ret = driver_->Chmod(fs_, path.c_str(), static_cast<short>(mode));  // NOLINT
    CHECK_FAILURE(ret, "Chmod");
    return Status::OK();
  }

  Status Chown(const std::string& path, const char* owner, const char* group) {
    int ret = driver_->Chown(fs_, path.c_str(), owner, group);
    CHECK_FAILURE(ret, "Chown");
    return Status::OK();
  }

 private:
  internal::LibHdfsShim* driver_;

  std::string namenode_host_;
  std::string user_;
  int port_;
  std::string kerb_ticket_;

  hdfsFS fs_;
};

// ----------------------------------------------------------------------
// Public API for HDFSClient

HadoopFileSystem::HadoopFileSystem() { impl_.reset(new Impl()); }

Status HadoopFileSystem::Connect(const HadoopOptions& options,
                                 std::shared_ptr<HadoopFileSystem>* fs) {
  // ctor is private, make_shared will not work
  *fs = std::shared_ptr<HadoopFileSystem>(new HadoopFileSystem());

  RETURN_NOT_OK((*fs)->impl_->Connect(options));
  return Status::OK();
}

Status HadoopFileSystem::Disconnect() { return impl_->Disconnect(); }

Status HadoopFileSystem::GetTargetStats(const std::string& path, FileStats* out) {
  out->set_path(path);
  if (!impl_->Exists(path)) {
    out->set_type(FileType::NonExistent);
    return Status::OK();
  }
  io::HdfsPathInfo info;
  RETURN_NOT_OK(impl_->GetPathInfo(path, &info));
  out->set_size(info.size);
  out->set_type(info.kind == io::ObjectType::FILE ? FileType::File : FileType::Directory);
  return Status::OK();
}

inline Selector SetBaseDir(const Selector& selector, std::string d) {
  Selector out;
  out.base_dir = std::move(d);
  out.allow_non_existent = selector.allow_non_existent;
  out.recursive = selector.recursive;
  out.max_recursion = selector.max_recursion;
  return out;
}

Status HadoopFileSystem::GetTargetStats(const Selector& selector,
                                        std::vector<FileStats>* out) {
  std::vector<std::string> paths;
  RETURN_NOT_OK(impl_->GetChildren(selector.base_dir, &paths));
  std::vector<io::HdfsPathInfo> infos;
  RETURN_NOT_OK(impl_->ListDirectory(selector.base_dir, &infos));
  out->reserve(out->size() + infos.size());
  for (size_t i = 0; i < infos.size(); ++i) {
    out->emplace_back();
    out->back().set_path(paths[i]);
    out->back().set_size(infos[i].size);
    out->back().set_mtime(TimePoint(std::chrono::duration_cast<TimePoint::duration>(
        std::chrono::seconds(infos[i].last_modified_time))));

    if (infos[i].kind == io::ObjectType::FILE) {
      out->back().set_type(FileType::File);
      continue;
    }

    out->back().set_type(FileType::Directory);
    if (selector.recursive) {
      Selector recursive = SetBaseDir(selector, std::move(paths[i]));
      RETURN_NOT_OK(GetTargetStats(recursive, out));
    }
  }
  return Status::OK();
}

Status HadoopFileSystem::CreateDir(const std::string& path, bool recursive) {
  // XXX hdfsCreateDirectory always creates non existent parents.
  // Do we need to emit an error if !recursive and non existent parents will be created?
  return impl_->MakeDirectory(path);
}

Status HadoopFileSystem::DeleteDir(const std::string& path) {
  return impl_->Delete(path, true);
}

Status HadoopFileSystem::DeleteDirContents(const std::string& path) {
  std::vector<io::HdfsPathInfo> detailed_listing;
  RETURN_NOT_OK(impl_->ListDirectory(path, &detailed_listing));
  for (const auto& info : detailed_listing) {
    RETURN_NOT_OK(impl_->Delete(info.name, true));
  }
  return Status::OK();
}

Status HadoopFileSystem::DeleteFile(const std::string& path) {
  return impl_->Delete(path, false);
}

Status HadoopFileSystem::Move(const std::string& src, const std::string& dst) {
  return impl_->Rename(src, dst);
}

Status HadoopFileSystem::CopyFile(const std::string& src, const std::string& dst) {
  std::shared_ptr<io::InputStream> src_file;
  std::shared_ptr<io::OutputStream> dst_file;
  RETURN_NOT_OK(OpenInputStream(src, &src_file));
  RETURN_NOT_OK(OpenOutputStream(dst, &dst_file));
  return CopyStream(src_file, dst_file, kDefaultHdfsBufferSize);
}

Status HadoopFileSystem::OpenInputStream(const std::string& path,
                                         std::shared_ptr<io::InputStream>* file) {
  std::shared_ptr<io::RandomAccessFile> random_file;
  RETURN_NOT_OK(impl_->OpenReadable(path, kDefaultHdfsBufferSize, &random_file));
  *file = std::move(random_file);
  return Status::OK();
}

Status HadoopFileSystem::OpenInputFile(const std::string& path,
                                       std::shared_ptr<io::RandomAccessFile>* file) {
  return impl_->OpenReadable(path, kDefaultHdfsBufferSize, file);
}

Status HadoopFileSystem::OpenOutputStream(const std::string& path,
                                          std::shared_ptr<io::OutputStream>* file) {
  return OpenWritable(path, false, 0, 0, 0, file);
}

Status HadoopFileSystem::OpenAppendStream(const std::string& path,
                                          std::shared_ptr<io::OutputStream>* file) {
  return OpenWritable(path, true, 0, 0, 0, file);
}

bool HadoopFileSystem::Exists(const std::string& path) { return impl_->Exists(path); }

Status HadoopFileSystem::GetCapacity(int64_t* nbytes) {
  return impl_->GetCapacity(nbytes);
}

Status HadoopFileSystem::GetUsed(int64_t* nbytes) { return impl_->GetUsed(nbytes); }

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      std::shared_ptr<io::RandomAccessFile>* file) {
  return impl_->OpenReadable(path, kDefaultHdfsBufferSize, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path, int32_t buffer_size,
                                      std::shared_ptr<io::RandomAccessFile>* file) {
  return impl_->OpenReadable(path, buffer_size, file);
}

Status HadoopFileSystem::OpenWritable(const std::string& path, bool append,
                                      int32_t buffer_size, int16_t replication,
                                      int64_t default_block_size,
                                      std::shared_ptr<io::OutputStream>* file) {
  return impl_->OpenWritable(path, append, buffer_size, replication, default_block_size,
                             file);
}

Status HadoopFileSystem::OpenWritable(const std::string& path, bool append,
                                      std::shared_ptr<io::OutputStream>* file) {
  return OpenWritable(path, append, 0, 0, 0, file);
}

Status HadoopFileSystem::Chmod(const std::string& path, int mode) {
  return impl_->Chmod(path, mode);
}

Status HadoopFileSystem::Chown(const std::string& path, const char* owner,
                               const char* group) {
  return impl_->Chown(path, owner, group);
}

Status HadoopFileSystem::GetPathInfo(const std::string& path, HadoopPathInfo* info) {
  return impl_->GetPathInfo(path, info);
}

constexpr ObjectType::type ObjectType::FILE;
constexpr ObjectType::type ObjectType::DIRECTORY;

// ----------------------------------------------------------------------
// Allow public API users to check whether we are set up correctly

Status HaveLibHdfs() {
  internal::LibHdfsShim* driver;
  return internal::ConnectLibHdfs(&driver);
}

Status HaveLibHdfs3() {
  internal::LibHdfsShim* driver;
  return internal::ConnectLibHdfs3(&driver);
}

}  // namespace fs
}  // namespace arrow

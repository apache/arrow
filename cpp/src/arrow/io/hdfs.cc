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

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/hdfs.h"
#include "arrow/io/hdfs_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

using std::size_t;

namespace arrow {

using internal::IOErrorFromErrno;

namespace io {

#define CHECK_FAILURE(RETURN_VALUE, WHAT)                       \
  do {                                                          \
    if (RETURN_VALUE == -1) {                                   \
      return IOErrorFromErrno(errno, "HDFS ", WHAT, " failed"); \
    }                                                           \
  } while (0)

static constexpr int kDefaultHdfsBufferSize = 1 << 16;

// ----------------------------------------------------------------------
// File reading

class HdfsAnyFileImpl {
 public:
  void set_members(const std::string& path, internal::LibHdfsShim* driver, hdfsFS fs,
                   hdfsFile handle) {
    path_ = path;
    driver_ = driver;
    fs_ = fs;
    file_ = handle;
    is_open_ = true;
  }

  Status Seek(int64_t position) {
    RETURN_NOT_OK(CheckClosed());
    int ret = driver_->Seek(fs_, file_, position);
    CHECK_FAILURE(ret, "seek");
    return Status::OK();
  }

  Result<int64_t> Tell() {
    RETURN_NOT_OK(CheckClosed());
    int64_t ret = driver_->Tell(fs_, file_);
    CHECK_FAILURE(ret, "tell");
    return ret;
  }

  bool is_open() const { return is_open_; }

 protected:
  Status CheckClosed() {
    if (!is_open_) {
      return Status::Invalid("Operation on closed HDFS file");
    }
    return Status::OK();
  }

  std::string path_;

  internal::LibHdfsShim* driver_;

  // For threadsafety
  std::mutex lock_;

  // These are pointers in libhdfs, so OK to copy
  hdfsFS fs_;
  hdfsFile file_;

  bool is_open_;
};

namespace {

Status GetPathInfoFailed(const std::string& path) {
  return IOErrorFromErrno(errno, "Calling GetPathInfo for '", path, "' failed");
}

}  // namespace

// Private implementation for read-only files
class HdfsReadableFile::HdfsReadableFileImpl : public HdfsAnyFileImpl {
 public:
  explicit HdfsReadableFileImpl(MemoryPool* pool) : pool_(pool) {}

  Status Close() {
    if (is_open_) {
      // is_open_ must be set to false in the beginning, because the destructor
      // attempts to close the stream again, and if the first close fails, then
      // the error doesn't get propagated properly and the second close
      // initiated by the destructor raises a segfault
      is_open_ = false;
      int ret = driver_->CloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
    }
    return Status::OK();
  }

  bool closed() const { return !is_open_; }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, uint8_t* buffer) {
    RETURN_NOT_OK(CheckClosed());
    if (!driver_->HasPread()) {
      std::lock_guard<std::mutex> guard(lock_);
      RETURN_NOT_OK(Seek(position));
      return Read(nbytes, buffer);
    }

    constexpr int64_t kMaxBlockSize = std::numeric_limits<int32_t>::max();
    int64_t total_bytes = 0;
    while (nbytes > 0) {
      const auto block_size = static_cast<tSize>(std::min(kMaxBlockSize, nbytes));
      tSize ret =
          driver_->Pread(fs_, file_, static_cast<tOffset>(position), buffer, block_size);
      CHECK_FAILURE(ret, "read");
      DCHECK_LE(ret, block_size);
      if (ret == 0) {
        break;  // EOF
      }
      buffer += ret;
      total_bytes += ret;
      position += ret;
      nbytes -= ret;
    }
    return total_bytes;
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());

    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          ReadAt(position, nbytes, buffer->mutable_data()));
    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
      buffer->ZeroPadding();
    }
    // R build with openSUSE155 requires an explicit shared_ptr construction
    return std::shared_ptr<Buffer>(std::move(buffer));
  }

  Result<int64_t> Read(int64_t nbytes, void* buffer) {
    RETURN_NOT_OK(CheckClosed());

    int64_t total_bytes = 0;
    while (total_bytes < nbytes) {
      tSize ret = driver_->Read(
          fs_, file_, reinterpret_cast<uint8_t*>(buffer) + total_bytes,
          static_cast<tSize>(std::min<int64_t>(buffer_size_, nbytes - total_bytes)));
      CHECK_FAILURE(ret, "read");
      total_bytes += ret;
      if (ret == 0) {
        break;
      }
    }
    return total_bytes;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());

    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
    }
    // R build with openSUSE155 requires an explicit shared_ptr construction
    return std::shared_ptr<Buffer>(std::move(buffer));
  }

  Result<int64_t> GetSize() {
    RETURN_NOT_OK(CheckClosed());

    hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path_.c_str());
    if (entry == nullptr) {
      return GetPathInfoFailed(path_);
    }
    int64_t size = entry->mSize;
    driver_->FreeFileInfo(entry, 1);
    return size;
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

  void set_buffer_size(int32_t buffer_size) { buffer_size_ = buffer_size; }

 private:
  MemoryPool* pool_;
  int32_t buffer_size_;
};

HdfsReadableFile::HdfsReadableFile(const io::IOContext& io_context) {
  impl_.reset(new HdfsReadableFileImpl(io_context.pool()));
}

HdfsReadableFile::~HdfsReadableFile() {
  ARROW_WARN_NOT_OK(impl_->Close(), "Failed to close HdfsReadableFile");
}

Status HdfsReadableFile::Close() { return impl_->Close(); }

bool HdfsReadableFile::closed() const { return impl_->closed(); }

Result<int64_t> HdfsReadableFile::ReadAt(int64_t position, int64_t nbytes, void* buffer) {
  return impl_->ReadAt(position, nbytes, reinterpret_cast<uint8_t*>(buffer));
}

Result<std::shared_ptr<Buffer>> HdfsReadableFile::ReadAt(int64_t position,
                                                         int64_t nbytes) {
  return impl_->ReadAt(position, nbytes);
}

Result<int64_t> HdfsReadableFile::Read(int64_t nbytes, void* buffer) {
  return impl_->Read(nbytes, buffer);
}

Result<std::shared_ptr<Buffer>> HdfsReadableFile::Read(int64_t nbytes) {
  return impl_->Read(nbytes);
}

Result<int64_t> HdfsReadableFile::GetSize() { return impl_->GetSize(); }

Status HdfsReadableFile::Seek(int64_t position) { return impl_->Seek(position); }

Result<int64_t> HdfsReadableFile::Tell() const { return impl_->Tell(); }

// ----------------------------------------------------------------------
// File writing

// Private implementation for writable-only files
class HdfsOutputStream::HdfsOutputStreamImpl : public HdfsAnyFileImpl {
 public:
  HdfsOutputStreamImpl() {}

  Status Close() {
    if (is_open_) {
      // is_open_ must be set to false in the beginning, because the destructor
      // attempts to close the stream again, and if the first close fails, then
      // the error doesn't get propagated properly and the second close
      // initiated by the destructor raises a segfault
      is_open_ = false;
      RETURN_NOT_OK(FlushInternal());
      int ret = driver_->CloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
    }
    return Status::OK();
  }

  bool closed() const { return !is_open_; }

  Status Flush() {
    RETURN_NOT_OK(CheckClosed());

    return FlushInternal();
  }

  Status Write(const uint8_t* buffer, int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());

    constexpr int64_t kMaxBlockSize = std::numeric_limits<int32_t>::max();

    std::lock_guard<std::mutex> guard(lock_);
    while (nbytes > 0) {
      const auto block_size = static_cast<tSize>(std::min(kMaxBlockSize, nbytes));
      tSize ret = driver_->Write(fs_, file_, buffer, block_size);
      CHECK_FAILURE(ret, "Write");
      DCHECK_LE(ret, block_size);
      buffer += ret;
      nbytes -= ret;
    }
    return Status::OK();
  }

 protected:
  Status FlushInternal() {
    int ret = driver_->Flush(fs_, file_);
    CHECK_FAILURE(ret, "Flush");
    return Status::OK();
  }
};

HdfsOutputStream::HdfsOutputStream() { impl_.reset(new HdfsOutputStreamImpl()); }

HdfsOutputStream::~HdfsOutputStream() {
  ARROW_WARN_NOT_OK(impl_->Close(), "Failed to close HdfsOutputStream");
}

Status HdfsOutputStream::Close() { return impl_->Close(); }

bool HdfsOutputStream::closed() const { return impl_->closed(); }

Status HdfsOutputStream::Write(const void* buffer, int64_t nbytes) {
  return impl_->Write(reinterpret_cast<const uint8_t*>(buffer), nbytes);
}

Status HdfsOutputStream::Flush() { return impl_->Flush(); }

Result<int64_t> HdfsOutputStream::Tell() const { return impl_->Tell(); }

// ----------------------------------------------------------------------
// HDFS client

// TODO(wesm): this could throw std::bad_alloc in the course of copying strings
// into the path info object
static void SetPathInfo(const hdfsFileInfo* input, HdfsPathInfo* out) {
  out->kind = input->mKind == kObjectKindFile ? ObjectType::FILE : ObjectType::DIRECTORY;
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
class HadoopFileSystem::HadoopFileSystemImpl {
 public:
  HadoopFileSystemImpl() : driver_(NULLPTR), port_(0), fs_(NULLPTR) {}

  Status Connect(const HdfsConnectionConfig* config) {
    RETURN_NOT_OK(ConnectLibHdfs(&driver_));

    // connect to HDFS with the builder object
    hdfsBuilder* builder = driver_->NewBuilder();
    if (!config->host.empty()) {
      driver_->BuilderSetNameNode(builder, config->host.c_str());
    }
    driver_->BuilderSetNameNodePort(builder, static_cast<tPort>(config->port));
    if (!config->user.empty()) {
      driver_->BuilderSetUserName(builder, config->user.c_str());
    }
    if (!config->kerb_ticket.empty()) {
      driver_->BuilderSetKerbTicketCachePath(builder, config->kerb_ticket.c_str());
    }

    for (const auto& kv : config->extra_conf) {
      int ret = driver_->BuilderConfSetStr(builder, kv.first.c_str(), kv.second.c_str());
      CHECK_FAILURE(ret, "confsetstr");
    }

    driver_->BuilderSetForceNewInstance(builder);
    fs_ = driver_->BuilderConnect(builder);

    if (fs_ == nullptr) {
      return Status::IOError("HDFS connection failed");
    }
    namenode_host_ = config->host;
    port_ = config->port;
    user_ = config->user;
    kerb_ticket_ = config->kerb_ticket;

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

  Status GetWorkingDirectory(std::string* out) {
    char buffer[2048];
    if (driver_->GetWorkingDirectory(fs_, buffer, sizeof(buffer) - 1) == nullptr) {
      return IOErrorFromErrno(errno, "HDFS GetWorkingDirectory failed");
    }
    *out = buffer;
    return Status::OK();
  }

  Status GetPathInfo(const std::string& path, HdfsPathInfo* info) {
    hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path.c_str());

    if (entry == nullptr) {
      return GetPathInfoFailed(path);
    }

    SetPathInfo(entry, info);
    driver_->FreeFileInfo(entry, 1);

    return Status::OK();
  }

  Status Stat(const std::string& path, FileStatistics* stat) {
    HdfsPathInfo info;
    RETURN_NOT_OK(GetPathInfo(path, &info));

    stat->size = info.size;
    stat->kind = info.kind;
    return Status::OK();
  }

  Status GetChildren(const std::string& path, std::vector<std::string>* listing) {
    std::vector<HdfsPathInfo> detailed_listing;
    RETURN_NOT_OK(ListDirectory(path, &detailed_listing));
    for (const auto& info : detailed_listing) {
      listing->push_back(info.name);
    }
    return Status::OK();
  }

  Status ListDirectory(const std::string& path, std::vector<HdfsPathInfo>* listing) {
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
        return IOErrorFromErrno(errno, "HDFS list directory failed");
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
                      const io::IOContext& io_context,
                      std::shared_ptr<HdfsReadableFile>* file) {
    errno = 0;
    hdfsFile handle = driver_->OpenFile(fs_, path.c_str(), O_RDONLY, buffer_size, 0, 0);

    if (handle == nullptr) {
      return IOErrorFromErrno(errno, "Opening HDFS file '", path, "' failed");
    }

    // std::make_shared does not work with private ctors
    *file = std::shared_ptr<HdfsReadableFile>(new HdfsReadableFile(io_context));
    (*file)->impl_->set_members(path, driver_, fs_, handle);
    (*file)->impl_->set_buffer_size(buffer_size);

    return Status::OK();
  }

  Status OpenWritable(const std::string& path, bool append, int32_t buffer_size,
                      int16_t replication, int64_t default_block_size,
                      std::shared_ptr<HdfsOutputStream>* file) {
    int flags = O_WRONLY;
    if (append) flags |= O_APPEND;

    errno = 0;
    hdfsFile handle =
        driver_->OpenFile(fs_, path.c_str(), flags, buffer_size, replication,
                          static_cast<tSize>(default_block_size));

    if (handle == nullptr) {
      return IOErrorFromErrno(errno, "Opening HDFS file '", path, "' failed");
    }

    // std::make_shared does not work with private ctors
    *file = std::shared_ptr<HdfsOutputStream>(new HdfsOutputStream());
    (*file)->impl_->set_members(path, driver_, fs_, handle);

    return Status::OK();
  }

  Status Rename(const std::string& src, const std::string& dst) {
    int ret = driver_->Rename(fs_, src.c_str(), dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

  Status Copy(const std::string& src, const std::string& dst) {
    int ret = driver_->Copy(fs_, src.c_str(), fs_, dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

  Status Move(const std::string& src, const std::string& dst) {
    int ret = driver_->Move(fs_, src.c_str(), fs_, dst.c_str());
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

HadoopFileSystem::HadoopFileSystem() { impl_.reset(new HadoopFileSystemImpl()); }

HadoopFileSystem::~HadoopFileSystem() {}

Status HadoopFileSystem::Connect(const HdfsConnectionConfig* config,
                                 std::shared_ptr<HadoopFileSystem>* fs) {
  // ctor is private, make_shared will not work
  *fs = std::shared_ptr<HadoopFileSystem>(new HadoopFileSystem());

  RETURN_NOT_OK((*fs)->impl_->Connect(config));
  return Status::OK();
}

Status HadoopFileSystem::MakeDirectory(const std::string& path) {
  return impl_->MakeDirectory(path);
}

Status HadoopFileSystem::Delete(const std::string& path, bool recursive) {
  return impl_->Delete(path, recursive);
}

Status HadoopFileSystem::DeleteDirectory(const std::string& path) {
  return Delete(path, true);
}

Status HadoopFileSystem::Disconnect() { return impl_->Disconnect(); }

bool HadoopFileSystem::Exists(const std::string& path) { return impl_->Exists(path); }

Status HadoopFileSystem::GetPathInfo(const std::string& path, HdfsPathInfo* info) {
  return impl_->GetPathInfo(path, info);
}

Status HadoopFileSystem::Stat(const std::string& path, FileStatistics* stat) {
  return impl_->Stat(path, stat);
}

Status HadoopFileSystem::GetCapacity(int64_t* nbytes) {
  return impl_->GetCapacity(nbytes);
}

Status HadoopFileSystem::GetUsed(int64_t* nbytes) { return impl_->GetUsed(nbytes); }

Status HadoopFileSystem::GetWorkingDirectory(std::string* out) {
  return impl_->GetWorkingDirectory(out);
}

Status HadoopFileSystem::GetChildren(const std::string& path,
                                     std::vector<std::string>* listing) {
  return impl_->GetChildren(path, listing);
}

Status HadoopFileSystem::ListDirectory(const std::string& path,
                                       std::vector<HdfsPathInfo>* listing) {
  return impl_->ListDirectory(path, listing);
}

Status HadoopFileSystem::OpenReadable(const std::string& path, int32_t buffer_size,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, buffer_size, io::default_io_context(), file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return OpenReadable(path, kDefaultHdfsBufferSize, io::default_io_context(), file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path, int32_t buffer_size,
                                      const io::IOContext& io_context,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, buffer_size, io_context, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      const io::IOContext& io_context,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return OpenReadable(path, kDefaultHdfsBufferSize, io_context, file);
}

Status HadoopFileSystem::OpenWritable(const std::string& path, bool append,
                                      int32_t buffer_size, int16_t replication,
                                      int64_t default_block_size,
                                      std::shared_ptr<HdfsOutputStream>* file) {
  return impl_->OpenWritable(path, append, buffer_size, replication, default_block_size,
                             file);
}

Status HadoopFileSystem::OpenWritable(const std::string& path, bool append,
                                      std::shared_ptr<HdfsOutputStream>* file) {
  return OpenWritable(path, append, 0, 0, 0, file);
}

Status HadoopFileSystem::Chmod(const std::string& path, int mode) {
  return impl_->Chmod(path, mode);
}

Status HadoopFileSystem::Chown(const std::string& path, const char* owner,
                               const char* group) {
  return impl_->Chown(path, owner, group);
}

Status HadoopFileSystem::Rename(const std::string& src, const std::string& dst) {
  return impl_->Rename(src, dst);
}

Status HadoopFileSystem::Copy(const std::string& src, const std::string& dst) {
  return impl_->Copy(src, dst);
}

Status HadoopFileSystem::Move(const std::string& src, const std::string& dst) {
  return impl_->Move(src, dst);
}

// ----------------------------------------------------------------------
// Allow public API users to check whether we are set up correctly

Status HaveLibHdfs() {
  internal::LibHdfsShim* driver;
  return internal::ConnectLibHdfs(&driver);
}

}  // namespace io
}  // namespace arrow

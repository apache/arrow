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
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <sstream>
#include <string>

#include "arrow/buffer.h"
#include "arrow/io/hdfs-internal.h"
#include "arrow/io/hdfs.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

using std::size_t;

namespace arrow {
namespace io {

#define CHECK_FAILURE(RETURN_VALUE, WHAT)  \
  do {                                     \
    if (RETURN_VALUE == -1) {              \
      std::stringstream ss;                \
      ss << "HDFS: " << WHAT << " failed"; \
      return Status::IOError(ss.str());    \
    }                                      \
  } while (0)

static Status CheckReadResult(int ret) {
  // Check for error on -1 (possibly errno set)

  // ret == 0 at end of file, which is OK
  if (ret == -1) {
    // EOF
    std::stringstream ss;
    ss << "HDFS read failed, errno: " << errno;
    return Status::IOError(ss.str());
  }
  return Status::OK();
}

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
    int ret = driver_->Seek(fs_, file_, position);
    CHECK_FAILURE(ret, "seek");
    return Status::OK();
  }

  Status Tell(int64_t* offset) {
    int64_t ret = driver_->Tell(fs_, file_);
    CHECK_FAILURE(ret, "tell");
    *offset = ret;
    return Status::OK();
  }

  bool is_open() const { return is_open_; }

 protected:
  std::string path_;

  internal::LibHdfsShim* driver_;

  // For threadsafety
  std::mutex lock_;

  // These are pointers in libhdfs, so OK to copy
  hdfsFS fs_;
  hdfsFile file_;

  bool is_open_;
};

// Private implementation for read-only files
class HdfsReadableFile::HdfsReadableFileImpl : public HdfsAnyFileImpl {
 public:
  explicit HdfsReadableFileImpl(MemoryPool* pool) : pool_(pool) {}

  Status Close() {
    if (is_open_) {
      int ret = driver_->CloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
      is_open_ = false;
    }
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
    tSize ret;
    if (driver_->HasPread()) {
      ret = driver_->Pread(fs_, file_, static_cast<tOffset>(position),
                           reinterpret_cast<void*>(buffer), static_cast<tSize>(nbytes));
    } else {
      std::lock_guard<std::mutex> guard(lock_);
      RETURN_NOT_OK(Seek(position));
      return Read(nbytes, bytes_read, buffer);
    }
    RETURN_NOT_OK(CheckReadResult(ret));
    *bytes_read = ret;
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> buffer;
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(ReadAt(position, nbytes, &bytes_read, buffer->mutable_data()));

    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
    }

    *out = buffer;
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
    int64_t total_bytes = 0;
    while (total_bytes < nbytes) {
      tSize ret = driver_->Read(
          fs_, file_, reinterpret_cast<void*>(buffer + total_bytes),
          static_cast<tSize>(std::min<int64_t>(buffer_size_, nbytes - total_bytes)));
      RETURN_NOT_OK(CheckReadResult(ret));
      total_bytes += ret;
      if (ret == 0) {
        break;
      }
    }

    *bytes_read = total_bytes;
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> buffer;
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
    if (bytes_read < nbytes) {
      RETURN_NOT_OK(buffer->Resize(bytes_read));
    }

    *out = buffer;
    return Status::OK();
  }

  Status GetSize(int64_t* size) {
    hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path_.c_str());
    if (entry == nullptr) {
      return Status::IOError("HDFS: GetPathInfo failed");
    }

    *size = entry->mSize;
    driver_->FreeFileInfo(entry, 1);
    return Status::OK();
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

  void set_buffer_size(int32_t buffer_size) { buffer_size_ = buffer_size; }

 private:
  MemoryPool* pool_;
  int32_t buffer_size_;
};

HdfsReadableFile::HdfsReadableFile(MemoryPool* pool) {
  if (pool == nullptr) {
    pool = default_memory_pool();
  }
  impl_.reset(new HdfsReadableFileImpl(pool));
}

HdfsReadableFile::~HdfsReadableFile() { DCHECK(impl_->Close().ok()); }

Status HdfsReadableFile::Close() { return impl_->Close(); }

Status HdfsReadableFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                uint8_t* buffer) {
  return impl_->ReadAt(position, nbytes, bytes_read, buffer);
}

Status HdfsReadableFile::ReadAt(int64_t position, int64_t nbytes,
                                std::shared_ptr<Buffer>* out) {
  return impl_->ReadAt(position, nbytes, out);
}

bool HdfsReadableFile::supports_zero_copy() const { return false; }

Status HdfsReadableFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  return impl_->Read(nbytes, bytes_read, buffer);
}

Status HdfsReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* buffer) {
  return impl_->Read(nbytes, buffer);
}

Status HdfsReadableFile::GetSize(int64_t* size) { return impl_->GetSize(size); }

Status HdfsReadableFile::Seek(int64_t position) { return impl_->Seek(position); }

Status HdfsReadableFile::Tell(int64_t* position) const { return impl_->Tell(position); }

// ----------------------------------------------------------------------
// File writing

// Private implementation for writeable-only files
class HdfsOutputStream::HdfsOutputStreamImpl : public HdfsAnyFileImpl {
 public:
  HdfsOutputStreamImpl() {}

  Status Close() {
    if (is_open_) {
      RETURN_NOT_OK(Flush());
      int ret = driver_->CloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
      is_open_ = false;
    }
    return Status::OK();
  }

  Status Flush() {
    int ret = driver_->Flush(fs_, file_);
    CHECK_FAILURE(ret, "Flush");
    return Status::OK();
  }

  Status Write(const uint8_t* buffer, int64_t nbytes, int64_t* bytes_written) {
    std::lock_guard<std::mutex> guard(lock_);
    tSize ret = driver_->Write(fs_, file_, reinterpret_cast<const void*>(buffer),
                               static_cast<tSize>(nbytes));
    CHECK_FAILURE(ret, "Write");
    *bytes_written = ret;
    return Status::OK();
  }
};

HdfsOutputStream::HdfsOutputStream() { impl_.reset(new HdfsOutputStreamImpl()); }

HdfsOutputStream::~HdfsOutputStream() { DCHECK(impl_->Close().ok()); }

Status HdfsOutputStream::Close() { return impl_->Close(); }

Status HdfsOutputStream::Write(const uint8_t* buffer, int64_t nbytes,
                               int64_t* bytes_read) {
  return impl_->Write(buffer, nbytes, bytes_read);
}

Status HdfsOutputStream::Write(const uint8_t* buffer, int64_t nbytes) {
  int64_t bytes_written_dummy = 0;
  return Write(buffer, nbytes, &bytes_written_dummy);
}

Status HdfsOutputStream::Flush() { return impl_->Flush(); }

Status HdfsOutputStream::Tell(int64_t* position) const { return impl_->Tell(position); }

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
  HadoopFileSystemImpl() {}

  Status Connect(const HdfsConnectionConfig* config) {
    if (config->driver == HdfsDriver::LIBHDFS3) {
      RETURN_NOT_OK(ConnectLibHdfs3(&driver_));
    } else {
      RETURN_NOT_OK(ConnectLibHdfs(&driver_));
    }

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

  Status GetPathInfo(const std::string& path, HdfsPathInfo* info) {
    hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path.c_str());

    if (entry == nullptr) {
      return Status::IOError("HDFS: GetPathInfo failed");
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
    hdfsFileInfo* entries = driver_->ListDirectory(fs_, path.c_str(), &num_entries);

    if (entries == nullptr) {
      // If the directory is empty, entries is NULL but errno is 0. Non-zero
      // errno indicates error
      //
      // Note: errno is thread-locala
      if (errno == 0) {
        num_entries = 0;
      } else {
        return Status::IOError("HDFS: list directory failed");
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
                      std::shared_ptr<HdfsReadableFile>* file) {
    hdfsFile handle = driver_->OpenFile(fs_, path.c_str(), O_RDONLY, buffer_size, 0, 0);

    if (handle == nullptr) {
      std::stringstream ss;
      if (!Exists(path)) {
        ss << "HDFS file does not exist: " << path;
      } else {
        // TODO(wesm): determine other causes of failure
        ss << "HDFS path exists, but opening file failed: " << path;
      }
      return Status::IOError(ss.str());
    }

    // std::make_shared does not work with private ctors
    *file = std::shared_ptr<HdfsReadableFile>(new HdfsReadableFile());
    (*file)->impl_->set_members(path, driver_, fs_, handle);
    (*file)->impl_->set_buffer_size(buffer_size);

    return Status::OK();
  }

  Status OpenWriteable(const std::string& path, bool append, int32_t buffer_size,
                       int16_t replication, int64_t default_block_size,
                       std::shared_ptr<HdfsOutputStream>* file) {
    int flags = O_WRONLY;
    if (append) flags |= O_APPEND;

    hdfsFile handle =
        driver_->OpenFile(fs_, path.c_str(), flags, buffer_size, replication,
                          static_cast<tSize>(default_block_size));

    if (handle == nullptr) {
      // TODO(wesm): determine cause of failure
      std::stringstream ss;
      ss << "Unable to open file " << path;
      return Status::IOError(ss.str());
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
  return impl_->OpenReadable(path, buffer_size, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return OpenReadable(path, kDefaultHdfsBufferSize, file);
}

Status HadoopFileSystem::OpenWriteable(const std::string& path, bool append,
                                       int32_t buffer_size, int16_t replication,
                                       int64_t default_block_size,
                                       std::shared_ptr<HdfsOutputStream>* file) {
  return impl_->OpenWriteable(path, append, buffer_size, replication, default_block_size,
                              file);
}

Status HadoopFileSystem::OpenWriteable(const std::string& path, bool append,
                                       std::shared_ptr<HdfsOutputStream>* file) {
  return OpenWriteable(path, append, 0, 0, 0, file);
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

}  // namespace io
}  // namespace arrow

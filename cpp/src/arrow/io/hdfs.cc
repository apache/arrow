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

#include <cstdint>
#include <sstream>
#include <string>

#include "arrow/io/hdfs.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

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

// ----------------------------------------------------------------------
// File reading

class HdfsAnyFileImpl {
 public:
  void set_members(const std::string& path, hdfsFS fs, hdfsFile handle) {
    path_ = path;
    fs_ = fs;
    file_ = handle;
    is_open_ = true;
  }

  Status Seek(int64_t position) {
    int ret = hdfsSeek(fs_, file_, position);
    CHECK_FAILURE(ret, "seek");
    return Status::OK();
  }

  Status Tell(int64_t* offset) {
    int64_t ret = hdfsTell(fs_, file_);
    CHECK_FAILURE(ret, "tell");
    *offset = ret;
    return Status::OK();
  }

  bool is_open() const { return is_open_; }

 protected:
  std::string path_;

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
      int ret = hdfsCloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
      is_open_ = false;
    }
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
    tSize ret = hdfsPread(fs_, file_, static_cast<tOffset>(position),
        reinterpret_cast<void*>(buffer), nbytes);
    RETURN_NOT_OK(CheckReadResult(ret));
    *bytes_read = ret;
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
    auto buffer = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(buffer->Resize(nbytes));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(ReadAt(position, nbytes, &bytes_read, buffer->mutable_data()));

    if (bytes_read < nbytes) { RETURN_NOT_OK(buffer->Resize(bytes_read)); }

    *out = buffer;
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
    tSize ret = hdfsRead(fs_, file_, reinterpret_cast<void*>(buffer), nbytes);
    RETURN_NOT_OK(CheckReadResult(ret));
    *bytes_read = ret;
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    auto buffer = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(buffer->Resize(nbytes));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));

    if (bytes_read < nbytes) { RETURN_NOT_OK(buffer->Resize(bytes_read)); }

    *out = buffer;
    return Status::OK();
  }

  Status GetSize(int64_t* size) {
    hdfsFileInfo* entry = hdfsGetPathInfo(fs_, path_.c_str());
    if (entry == nullptr) { return Status::IOError("HDFS: GetPathInfo failed"); }

    *size = entry->mSize;
    hdfsFreeFileInfo(entry, 1);
    return Status::OK();
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

 private:
  MemoryPool* pool_;
};

HdfsReadableFile::HdfsReadableFile(MemoryPool* pool) {
  if (pool == nullptr) { pool = default_memory_pool(); }
  impl_.reset(new HdfsReadableFileImpl(pool));
}

HdfsReadableFile::~HdfsReadableFile() {
  impl_->Close();
}

Status HdfsReadableFile::Close() {
  return impl_->Close();
}

Status HdfsReadableFile::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  return impl_->ReadAt(position, nbytes, bytes_read, buffer);
}

Status HdfsReadableFile::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return impl_->ReadAt(position, nbytes, out);
}

bool HdfsReadableFile::supports_zero_copy() const {
  return false;
}

Status HdfsReadableFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) {
  return impl_->Read(nbytes, bytes_read, buffer);
}

Status HdfsReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* buffer) {
  return impl_->Read(nbytes, buffer);
}

Status HdfsReadableFile::GetSize(int64_t* size) {
  return impl_->GetSize(size);
}

Status HdfsReadableFile::Seek(int64_t position) {
  return impl_->Seek(position);
}

Status HdfsReadableFile::Tell(int64_t* position) {
  return impl_->Tell(position);
}

// ----------------------------------------------------------------------
// File writing

// Private implementation for writeable-only files
class HdfsOutputStream::HdfsOutputStreamImpl : public HdfsAnyFileImpl {
 public:
  HdfsOutputStreamImpl() {}

  Status Close() {
    if (is_open_) {
      int ret = hdfsFlush(fs_, file_);
      CHECK_FAILURE(ret, "Flush");
      ret = hdfsCloseFile(fs_, file_);
      CHECK_FAILURE(ret, "CloseFile");
      is_open_ = false;
    }
    return Status::OK();
  }

  Status Write(const uint8_t* buffer, int64_t nbytes, int64_t* bytes_written) {
    tSize ret = hdfsWrite(fs_, file_, reinterpret_cast<const void*>(buffer), nbytes);
    CHECK_FAILURE(ret, "Write");
    *bytes_written = ret;
    return Status::OK();
  }
};

HdfsOutputStream::HdfsOutputStream() {
  impl_.reset(new HdfsOutputStreamImpl());
}

HdfsOutputStream::~HdfsOutputStream() {
  impl_->Close();
}

Status HdfsOutputStream::Close() {
  return impl_->Close();
}

Status HdfsOutputStream::Write(
    const uint8_t* buffer, int64_t nbytes, int64_t* bytes_read) {
  return impl_->Write(buffer, nbytes, bytes_read);
}

Status HdfsOutputStream::Write(const uint8_t* buffer, int64_t nbytes) {
  int64_t bytes_written_dummy = 0;
  return Write(buffer, nbytes, &bytes_written_dummy);
}

Status HdfsOutputStream::Tell(int64_t* position) {
  return impl_->Tell(position);
}

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
class HdfsClient::HdfsClientImpl {
 public:
  HdfsClientImpl() {}

  Status Connect(const HdfsConnectionConfig* config) {
    RETURN_NOT_OK(ConnectLibHdfs());

    // connect to HDFS with the builder object
    hdfsBuilder* builder = hdfsNewBuilder();
    if (!config->host.empty()) { hdfsBuilderSetNameNode(builder, config->host.c_str()); }
    hdfsBuilderSetNameNodePort(builder, config->port);
    if (!config->user.empty()) { hdfsBuilderSetUserName(builder, config->user.c_str()); }
    if (!config->kerb_ticket.empty()) {
      hdfsBuilderSetKerbTicketCachePath(builder, config->kerb_ticket.c_str());
    }
    fs_ = hdfsBuilderConnect(builder);

    if (fs_ == nullptr) { return Status::IOError("HDFS connection failed"); }
    namenode_host_ = config->host;
    port_ = config->port;
    user_ = config->user;
    kerb_ticket_ = config->kerb_ticket;

    return Status::OK();
  }

  Status CreateDirectory(const std::string& path) {
    int ret = hdfsCreateDirectory(fs_, path.c_str());
    CHECK_FAILURE(ret, "create directory");
    return Status::OK();
  }

  Status Delete(const std::string& path, bool recursive) {
    int ret = hdfsDelete(fs_, path.c_str(), static_cast<int>(recursive));
    CHECK_FAILURE(ret, "delete");
    return Status::OK();
  }

  Status Disconnect() {
    int ret = hdfsDisconnect(fs_);
    CHECK_FAILURE(ret, "hdfsFS::Disconnect");
    return Status::OK();
  }

  bool Exists(const std::string& path) {
    // hdfsExists does not distinguish between RPC failure and the file not
    // existing
    int ret = hdfsExists(fs_, path.c_str());
    return ret == 0;
  }

  Status GetCapacity(int64_t* nbytes) {
    tOffset ret = hdfsGetCapacity(fs_);
    CHECK_FAILURE(ret, "GetCapacity");
    *nbytes = ret;
    return Status::OK();
  }

  Status GetUsed(int64_t* nbytes) {
    tOffset ret = hdfsGetUsed(fs_);
    CHECK_FAILURE(ret, "GetUsed");
    *nbytes = ret;
    return Status::OK();
  }

  Status GetPathInfo(const std::string& path, HdfsPathInfo* info) {
    hdfsFileInfo* entry = hdfsGetPathInfo(fs_, path.c_str());

    if (entry == nullptr) { return Status::IOError("HDFS: GetPathInfo failed"); }

    SetPathInfo(entry, info);
    hdfsFreeFileInfo(entry, 1);

    return Status::OK();
  }

  Status ListDirectory(const std::string& path, std::vector<HdfsPathInfo>* listing) {
    int num_entries = 0;
    hdfsFileInfo* entries = hdfsListDirectory(fs_, path.c_str(), &num_entries);

    if (entries == nullptr) {
      // If the directory is empty, entries is NULL but errno is 0. Non-zero
      // errno indicates error
      //
      // Note: errno is thread-locala
      if (errno == 0) { num_entries = 0; }
      { return Status::IOError("HDFS: list directory failed"); }
    }

    // Allocate additional space for elements

    int vec_offset = listing->size();
    listing->resize(vec_offset + num_entries);

    for (int i = 0; i < num_entries; ++i) {
      SetPathInfo(entries + i, &(*listing)[vec_offset + i]);
    }

    // Free libhdfs file info
    hdfsFreeFileInfo(entries, num_entries);

    return Status::OK();
  }

  Status OpenReadable(const std::string& path, std::shared_ptr<HdfsReadableFile>* file) {
    hdfsFile handle = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);

    if (handle == nullptr) {
      // TODO(wesm): determine cause of failure
      std::stringstream ss;
      ss << "Unable to open file " << path;
      return Status::IOError(ss.str());
    }

    // std::make_shared does not work with private ctors
    *file = std::shared_ptr<HdfsReadableFile>(new HdfsReadableFile());
    (*file)->impl_->set_members(path, fs_, handle);

    return Status::OK();
  }

  Status OpenWriteable(const std::string& path, bool append, int32_t buffer_size,
      int16_t replication, int64_t default_block_size,
      std::shared_ptr<HdfsOutputStream>* file) {
    int flags = O_WRONLY;
    if (append) flags |= O_APPEND;

    hdfsFile handle = hdfsOpenFile(
        fs_, path.c_str(), flags, buffer_size, replication, default_block_size);

    if (handle == nullptr) {
      // TODO(wesm): determine cause of failure
      std::stringstream ss;
      ss << "Unable to open file " << path;
      return Status::IOError(ss.str());
    }

    // std::make_shared does not work with private ctors
    *file = std::shared_ptr<HdfsOutputStream>(new HdfsOutputStream());
    (*file)->impl_->set_members(path, fs_, handle);

    return Status::OK();
  }

  Status Rename(const std::string& src, const std::string& dst) {
    int ret = hdfsRename(fs_, src.c_str(), dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

 private:
  std::string namenode_host_;
  std::string user_;
  int port_;
  std::string kerb_ticket_;

  hdfsFS fs_;
};

// ----------------------------------------------------------------------
// Public API for HDFSClient

HdfsClient::HdfsClient() {
  impl_.reset(new HdfsClientImpl());
}

HdfsClient::~HdfsClient() {}

Status HdfsClient::Connect(
    const HdfsConnectionConfig* config, std::shared_ptr<HdfsClient>* fs) {
  // ctor is private, make_shared will not work
  *fs = std::shared_ptr<HdfsClient>(new HdfsClient());

  RETURN_NOT_OK((*fs)->impl_->Connect(config));
  return Status::OK();
}

Status HdfsClient::CreateDirectory(const std::string& path) {
  return impl_->CreateDirectory(path);
}

Status HdfsClient::Delete(const std::string& path, bool recursive) {
  return impl_->Delete(path, recursive);
}

Status HdfsClient::Disconnect() {
  return impl_->Disconnect();
}

bool HdfsClient::Exists(const std::string& path) {
  return impl_->Exists(path);
}

Status HdfsClient::GetPathInfo(const std::string& path, HdfsPathInfo* info) {
  return impl_->GetPathInfo(path, info);
}

Status HdfsClient::GetCapacity(int64_t* nbytes) {
  return impl_->GetCapacity(nbytes);
}

Status HdfsClient::GetUsed(int64_t* nbytes) {
  return impl_->GetUsed(nbytes);
}

Status HdfsClient::ListDirectory(
    const std::string& path, std::vector<HdfsPathInfo>* listing) {
  return impl_->ListDirectory(path, listing);
}

Status HdfsClient::OpenReadable(
    const std::string& path, std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, file);
}

Status HdfsClient::OpenWriteable(const std::string& path, bool append,
    int32_t buffer_size, int16_t replication, int64_t default_block_size,
    std::shared_ptr<HdfsOutputStream>* file) {
  return impl_->OpenWriteable(
      path, append, buffer_size, replication, default_block_size, file);
}

Status HdfsClient::OpenWriteable(
    const std::string& path, bool append, std::shared_ptr<HdfsOutputStream>* file) {
  return OpenWriteable(path, append, 0, 0, 0, file);
}

Status HdfsClient::Rename(const std::string& src, const std::string& dst) {
  return impl_->Rename(src, dst);
}

}  // namespace io
}  // namespace arrow

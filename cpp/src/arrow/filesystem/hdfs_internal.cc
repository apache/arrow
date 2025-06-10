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

// This shim interface to libhdfs (for runtime shared library loading) has been
// adapted from the SFrame project, released under the ASF-compatible 3-clause
// BSD license
//
// Using this required having the $JAVA_HOME and $HADOOP_HOME environment
// variables set, so that libjvm and libhdfs can be located easily

// Copyright (C) 2015 Dato, Inc.
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

#include "arrow/filesystem/hdfs_internal.h"

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <utility>
#include <vector>
#include "arrow/util/basic_decimal.h"

#ifndef _WIN32
#  include <dlfcn.h>
#endif

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging_internal.h"

namespace arrow {

using internal::GetEnvVarNative;
using internal::IOErrorFromErrno;
using internal::PlatformFilename;

namespace fs::internal {

namespace {

#define CHECK_FAILURE(RETURN_VALUE, WHAT)                       \
  do {                                                          \
    if (RETURN_VALUE == -1) {                                   \
      return IOErrorFromErrno(errno, "HDFS ", WHAT, " failed"); \
    }                                                           \
  } while (0)

template <bool Required, typename T>
Status SetSymbol(void* handle, char const* name, T** symbol) {
  if (*symbol != nullptr) return Status::OK();

  auto maybe_symbol = ::arrow::internal::GetSymbolAs<T>(handle, name);
  if (Required || maybe_symbol.ok()) {
    ARROW_ASSIGN_OR_RAISE(*symbol, maybe_symbol);
  }
  return Status::OK();
}

#define GET_SYMBOL_REQUIRED(SHIM, SYMBOL_NAME) \
  RETURN_NOT_OK(SetSymbol<true>(SHIM->handle, #SYMBOL_NAME, &SHIM->SYMBOL_NAME));

#define GET_SYMBOL(SHIM, SYMBOL_NAME) \
  DCHECK_OK(SetSymbol<false>(SHIM->handle, #SYMBOL_NAME, &SHIM->SYMBOL_NAME));

Result<std::vector<PlatformFilename>> MakeFilenameVector(
    const std::vector<std::string>& names) {
  std::vector<PlatformFilename> filenames(names.size());
  for (size_t i = 0; i < names.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(filenames[i], PlatformFilename::FromString(names[i]));
  }
  return filenames;
}

void AppendEnvVarFilename(const char* var_name,
                          std::vector<PlatformFilename>* filenames) {
  auto maybe_env_var = GetEnvVarNative(var_name);
  if (maybe_env_var.ok()) {
    filenames->emplace_back(std::move(*maybe_env_var));
  }
}

void AppendEnvVarFilename(const char* var_name, const char* suffix,
                          std::vector<PlatformFilename>* filenames) {
  auto maybe_env_var = GetEnvVarNative(var_name);
  if (maybe_env_var.ok()) {
    auto maybe_env_var_with_suffix =
        PlatformFilename(std::move(*maybe_env_var)).Join(suffix);
    if (maybe_env_var_with_suffix.ok()) {
      filenames->emplace_back(std::move(*maybe_env_var_with_suffix));
    }
  }
}

void InsertEnvVarFilename(const char* var_name,
                          std::vector<PlatformFilename>* filenames) {
  auto maybe_env_var = GetEnvVarNative(var_name);
  if (maybe_env_var.ok()) {
    filenames->emplace(filenames->begin(), PlatformFilename(std::move(*maybe_env_var)));
  }
}

Result<std::vector<PlatformFilename>> get_potential_libhdfs_paths() {
  std::vector<PlatformFilename> potential_paths;
  std::string file_name;

// OS-specific file name
#ifdef _WIN32
  file_name = "hdfs.dll";
#elif __APPLE__
  file_name = "libhdfs.dylib";
#else
  file_name = "libhdfs.so";
#endif

  // Common paths
  ARROW_ASSIGN_OR_RAISE(auto search_paths, MakeFilenameVector({"", "."}));

  // Path from environment variable
  AppendEnvVarFilename("HADOOP_HOME", "lib/native", &search_paths);
  AppendEnvVarFilename("ARROW_LIBHDFS_DIR", &search_paths);

  // All paths with file name
  for (const auto& path : search_paths) {
    ARROW_ASSIGN_OR_RAISE(auto full_path, path.Join(file_name));
    potential_paths.push_back(std::move(full_path));
  }

  return potential_paths;
}

Result<std::vector<PlatformFilename>> get_potential_libjvm_paths() {
  std::vector<PlatformFilename> potential_paths;

  std::vector<PlatformFilename> search_prefixes;
  std::vector<PlatformFilename> search_suffixes;
  std::string file_name;

// From heuristics
#ifdef _WIN32
  ARROW_ASSIGN_OR_RAISE(search_prefixes, MakeFilenameVector({""}));
  ARROW_ASSIGN_OR_RAISE(search_suffixes,
                        MakeFilenameVector({"/jre/bin/server", "/bin/server"}));
  file_name = "jvm.dll";
#elif __APPLE__
  ARROW_ASSIGN_OR_RAISE(search_prefixes, MakeFilenameVector({""}));
  ARROW_ASSIGN_OR_RAISE(search_suffixes,
                        MakeFilenameVector({"/jre/lib/server", "/lib/server"}));
  file_name = "libjvm.dylib";

// SFrame uses /usr/libexec/java_home to find JAVA_HOME; for now we are
// expecting users to set an environment variable
#else
#  if defined(__aarch64__)
  const std::string prefix_arch{"arm64"};
  const std::string suffix_arch{"aarch64"};
#  else
  const std::string prefix_arch{"amd64"};
  const std::string suffix_arch{"amd64"};
#  endif
  ARROW_ASSIGN_OR_RAISE(
      search_prefixes,
      MakeFilenameVector({
          "/usr/lib/jvm/default-java",        // ubuntu / debian distros
          "/usr/lib/jvm/java",                // rhel6
          "/usr/lib/jvm",                     // centos6
          "/usr/lib64/jvm",                   // opensuse 13
          "/usr/local/lib/jvm/default-java",  // alt ubuntu / debian distros
          "/usr/local/lib/jvm/java",          // alt rhel6
          "/usr/local/lib/jvm",               // alt centos6
          "/usr/local/lib64/jvm",             // alt opensuse 13
          "/usr/local/lib/jvm/java-8-openjdk-" +
              prefix_arch,                               // alt ubuntu / debian distros
          "/usr/lib/jvm/java-8-openjdk-" + prefix_arch,  // alt ubuntu / debian distros
          "/usr/local/lib/jvm/java-7-openjdk-" +
              prefix_arch,                               // alt ubuntu / debian distros
          "/usr/lib/jvm/java-7-openjdk-" + prefix_arch,  // alt ubuntu / debian distros
          "/usr/local/lib/jvm/java-6-openjdk-" +
              prefix_arch,                               // alt ubuntu / debian distros
          "/usr/lib/jvm/java-6-openjdk-" + prefix_arch,  // alt ubuntu / debian distros
          "/usr/lib/jvm/java-7-oracle",                  // alt ubuntu
          "/usr/lib/jvm/java-8-oracle",                  // alt ubuntu
          "/usr/lib/jvm/java-6-oracle",                  // alt ubuntu
          "/usr/local/lib/jvm/java-7-oracle",            // alt ubuntu
          "/usr/local/lib/jvm/java-8-oracle",            // alt ubuntu
          "/usr/local/lib/jvm/java-6-oracle",            // alt ubuntu
          "/usr/lib/jvm/default",                        // alt centos
          "/usr/java/latest"                             // alt centos
      }));
  ARROW_ASSIGN_OR_RAISE(
      search_suffixes,
      MakeFilenameVector({"", "/lib/server", "/jre/lib/" + suffix_arch + "/server",
                          "/lib/" + suffix_arch + "/server"}));
  file_name = "libjvm.so";
#endif

  // From direct environment variable
  InsertEnvVarFilename("JAVA_HOME", &search_prefixes);

  // Generate cross product between search_prefixes, search_suffixes, and file_name
  for (auto& prefix : search_prefixes) {
    for (auto& suffix : search_suffixes) {
      ARROW_ASSIGN_OR_RAISE(auto path, prefix.Join(suffix).Join(file_name));
      potential_paths.push_back(std::move(path));
    }
  }

  return potential_paths;
}

Result<void*> try_dlopen(const std::vector<PlatformFilename>& potential_paths,
                         std::string name) {
  std::string error_message = "Unable to load " + name;

  for (const auto& p : potential_paths) {
    auto maybe_handle = arrow::internal::LoadDynamicLibrary(p);
    if (maybe_handle.ok()) return *maybe_handle;
    error_message += "\n" + maybe_handle.status().message();
  }

  return Status(StatusCode::IOError, std::move(error_message));
}

LibHdfsShim libhdfs_shim;

}  // namespace

Status LibHdfsShim::GetRequiredSymbols() {
  GET_SYMBOL_REQUIRED(this, hdfsNewBuilder);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetNameNode);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetNameNodePort);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetUserName);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetKerbTicketCachePath);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderSetForceNewInstance);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderConfSetStr);
  GET_SYMBOL_REQUIRED(this, hdfsBuilderConnect);
  GET_SYMBOL_REQUIRED(this, hdfsCreateDirectory);
  GET_SYMBOL_REQUIRED(this, hdfsDelete);
  GET_SYMBOL_REQUIRED(this, hdfsDisconnect);
  GET_SYMBOL_REQUIRED(this, hdfsExists);
  GET_SYMBOL_REQUIRED(this, hdfsFreeFileInfo);
  GET_SYMBOL_REQUIRED(this, hdfsGetCapacity);
  GET_SYMBOL_REQUIRED(this, hdfsGetUsed);
  GET_SYMBOL_REQUIRED(this, hdfsGetPathInfo);
  GET_SYMBOL_REQUIRED(this, hdfsListDirectory);
  GET_SYMBOL_REQUIRED(this, hdfsChown);
  GET_SYMBOL_REQUIRED(this, hdfsChmod);

  // File methods
  GET_SYMBOL_REQUIRED(this, hdfsCloseFile);
  GET_SYMBOL_REQUIRED(this, hdfsFlush);
  GET_SYMBOL_REQUIRED(this, hdfsOpenFile);
  GET_SYMBOL_REQUIRED(this, hdfsRead);
  GET_SYMBOL_REQUIRED(this, hdfsSeek);
  GET_SYMBOL_REQUIRED(this, hdfsTell);
  GET_SYMBOL_REQUIRED(this, hdfsWrite);

  return Status::OK();
}

Status ConnectLibHdfs(LibHdfsShim** driver) {
  static std::mutex lock;
  std::lock_guard<std::mutex> guard(lock);

  LibHdfsShim* shim = &libhdfs_shim;

  static bool shim_attempted = false;
  if (!shim_attempted) {
    shim_attempted = true;

    shim->Initialize();

    ARROW_ASSIGN_OR_RAISE(auto libjvm_potential_paths, get_potential_libjvm_paths());
    RETURN_NOT_OK(try_dlopen(libjvm_potential_paths, "libjvm"));

    ARROW_ASSIGN_OR_RAISE(auto libhdfs_potential_paths, get_potential_libhdfs_paths());
    ARROW_ASSIGN_OR_RAISE(shim->handle, try_dlopen(libhdfs_potential_paths, "libhdfs"));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

///////////////////////////////////////////////////////////////////////////
// HDFS thin wrapper methods

hdfsBuilder* LibHdfsShim::NewBuilder() { return this->hdfsNewBuilder(); }

void LibHdfsShim::BuilderSetNameNode(hdfsBuilder* bld, const char* nn) {
  this->hdfsBuilderSetNameNode(bld, nn);
}

void LibHdfsShim::BuilderSetNameNodePort(hdfsBuilder* bld, tPort port) {
  this->hdfsBuilderSetNameNodePort(bld, port);
}

void LibHdfsShim::BuilderSetUserName(hdfsBuilder* bld, const char* userName) {
  this->hdfsBuilderSetUserName(bld, userName);
}

void LibHdfsShim::BuilderSetKerbTicketCachePath(hdfsBuilder* bld,
                                                const char* kerbTicketCachePath) {
  this->hdfsBuilderSetKerbTicketCachePath(bld, kerbTicketCachePath);
}

void LibHdfsShim::BuilderSetForceNewInstance(hdfsBuilder* bld) {
  this->hdfsBuilderSetForceNewInstance(bld);
}

hdfsFS LibHdfsShim::BuilderConnect(hdfsBuilder* bld) {
  return this->hdfsBuilderConnect(bld);
}

int LibHdfsShim::BuilderConfSetStr(hdfsBuilder* bld, const char* key, const char* val) {
  return this->hdfsBuilderConfSetStr(bld, key, val);
}

int LibHdfsShim::Disconnect(hdfsFS fs) { return this->hdfsDisconnect(fs); }

hdfsFile LibHdfsShim::OpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
                               short replication, tSize blocksize) {  // NOLINT
  return this->hdfsOpenFile(fs, path, flags, bufferSize, replication, blocksize);
}

int LibHdfsShim::CloseFile(hdfsFS fs, hdfsFile file) {
  return this->hdfsCloseFile(fs, file);
}

int LibHdfsShim::Exists(hdfsFS fs, const char* path) {
  return this->hdfsExists(fs, path);
}

int LibHdfsShim::Seek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  return this->hdfsSeek(fs, file, desiredPos);
}

tOffset LibHdfsShim::Tell(hdfsFS fs, hdfsFile file) { return this->hdfsTell(fs, file); }

tSize LibHdfsShim::Read(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  return this->hdfsRead(fs, file, buffer, length);
}

bool LibHdfsShim::HasPread() {
  GET_SYMBOL(this, hdfsPread);
  return this->hdfsPread != nullptr;
}

tSize LibHdfsShim::Pread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer,
                         tSize length) {
  GET_SYMBOL(this, hdfsPread);
  DCHECK(this->hdfsPread);
  return this->hdfsPread(fs, file, position, buffer, length);
}

tSize LibHdfsShim::Write(hdfsFS fs, hdfsFile file, const void* buffer, tSize length) {
  return this->hdfsWrite(fs, file, buffer, length);
}

int LibHdfsShim::Flush(hdfsFS fs, hdfsFile file) { return this->hdfsFlush(fs, file); }

int LibHdfsShim::Available(hdfsFS fs, hdfsFile file) {
  GET_SYMBOL(this, hdfsAvailable);
  if (this->hdfsAvailable) {
    return this->hdfsAvailable(fs, file);
  } else {
    return 0;
  }
}

int LibHdfsShim::Copy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsCopy);
  if (this->hdfsCopy) {
    return this->hdfsCopy(srcFS, src, dstFS, dst);
  } else {
    return 0;
  }
}

int LibHdfsShim::Move(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsMove);
  if (this->hdfsMove) {
    return this->hdfsMove(srcFS, src, dstFS, dst);
  } else {
    return 0;
  }
}

int LibHdfsShim::Delete(hdfsFS fs, const char* path, int recursive) {
  return this->hdfsDelete(fs, path, recursive);
}

int LibHdfsShim::Rename(hdfsFS fs, const char* oldPath, const char* newPath) {
  GET_SYMBOL(this, hdfsRename);
  if (this->hdfsRename) {
    return this->hdfsRename(fs, oldPath, newPath);
  } else {
    return 0;
  }
}

char* LibHdfsShim::GetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize) {
  GET_SYMBOL(this, hdfsGetWorkingDirectory);
  if (this->hdfsGetWorkingDirectory) {
    return this->hdfsGetWorkingDirectory(fs, buffer, bufferSize);
  } else {
    return nullptr;
  }
}

int LibHdfsShim::SetWorkingDirectory(hdfsFS fs, const char* path) {
  GET_SYMBOL(this, hdfsSetWorkingDirectory);
  if (this->hdfsSetWorkingDirectory) {
    return this->hdfsSetWorkingDirectory(fs, path);
  } else {
    return 0;
  }
}

int LibHdfsShim::MakeDirectory(hdfsFS fs, const char* path) {
  return this->hdfsCreateDirectory(fs, path);
}

int LibHdfsShim::SetReplication(hdfsFS fs, const char* path, int16_t replication) {
  GET_SYMBOL(this, hdfsSetReplication);
  if (this->hdfsSetReplication) {
    return this->hdfsSetReplication(fs, path, replication);
  } else {
    return 0;
  }
}

hdfsFileInfo* LibHdfsShim::ListDirectory(hdfsFS fs, const char* path, int* numEntries) {
  return this->hdfsListDirectory(fs, path, numEntries);
}

hdfsFileInfo* LibHdfsShim::GetPathInfo(hdfsFS fs, const char* path) {
  return this->hdfsGetPathInfo(fs, path);
}

void LibHdfsShim::FreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries) {
  this->hdfsFreeFileInfo(hdfsFileInfo, numEntries);
}

char*** LibHdfsShim::GetHosts(hdfsFS fs, const char* path, tOffset start,
                              tOffset length) {
  GET_SYMBOL(this, hdfsGetHosts);
  if (this->hdfsGetHosts) {
    return this->hdfsGetHosts(fs, path, start, length);
  } else {
    return nullptr;
  }
}

void LibHdfsShim::FreeHosts(char*** blockHosts) {
  GET_SYMBOL(this, hdfsFreeHosts);
  if (this->hdfsFreeHosts) {
    this->hdfsFreeHosts(blockHosts);
  }
}

tOffset LibHdfsShim::GetDefaultBlockSize(hdfsFS fs) {
  GET_SYMBOL(this, hdfsGetDefaultBlockSize);
  if (this->hdfsGetDefaultBlockSize) {
    return this->hdfsGetDefaultBlockSize(fs);
  } else {
    return 0;
  }
}

tOffset LibHdfsShim::GetCapacity(hdfsFS fs) { return this->hdfsGetCapacity(fs); }

tOffset LibHdfsShim::GetUsed(hdfsFS fs) { return this->hdfsGetUsed(fs); }

int LibHdfsShim::Chown(hdfsFS fs, const char* path, const char* owner,
                       const char* group) {
  return this->hdfsChown(fs, path, owner, group);
}

int LibHdfsShim::Chmod(hdfsFS fs, const char* path, short mode) {  // NOLINT
  return this->hdfsChmod(fs, path, mode);
}

int LibHdfsShim::Utime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  GET_SYMBOL(this, hdfsUtime);
  if (this->hdfsUtime) {
    return this->hdfsUtime(fs, path, mtime, atime);
  } else {
    return 0;
  }
}

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

Result<std::shared_ptr<HdfsReadableFile>> HdfsReadableFile::Make(
    const std::string& path, int32_t buffer_size, const io::IOContext& io_context,
    internal::LibHdfsShim* driver, hdfsFS fs, hdfsFile handle) {
  // Must use `new` due to private constructor
  std::shared_ptr<HdfsReadableFile> file(new HdfsReadableFile(io_context));
  file->impl_->set_members(path, driver, fs, handle);
  file->impl_->set_buffer_size(buffer_size);
  return file;
}

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

Result<std::shared_ptr<HdfsOutputStream>> HdfsOutputStream::Make(const std::string& path,
                                                                 int32_t buffer_size,
                                                                 LibHdfsShim* driver,
                                                                 hdfsFS fs,
                                                                 hdfsFile handle) {
  std::shared_ptr<HdfsOutputStream> file(new HdfsOutputStream());
  file->impl_->set_members(path, driver, fs, handle);
  return file;
}

}  // namespace fs::internal
}  // namespace arrow

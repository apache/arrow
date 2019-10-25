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
#include <vector>

#ifndef _WIN32
#include <dlfcn.h>
#endif

#ifdef ARROW_WITH_BOOST_FILESYSTEM
#include <boost/filesystem.hpp>  // NOLINT
#endif

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {
namespace internal {

#ifdef ARROW_WITH_BOOST_FILESYSTEM

namespace bfs = boost::filesystem;

#ifndef _WIN32
static void* libjvm_handle = NULL;
#else
static HINSTANCE libjvm_handle = NULL;
#endif
/*
 * All the shim pointers
 */

// Helper functions for dlopens
static std::vector<bfs::path> get_potential_libjvm_paths();
static std::vector<bfs::path> get_potential_libhdfs_paths();
static std::vector<bfs::path> get_potential_libhdfs3_paths();
static arrow::Status try_dlopen(std::vector<bfs::path> potential_paths, const char* name,
#ifndef _WIN32
                                void*& out_handle);
#else
                                HINSTANCE& out_handle);
#endif

static std::vector<bfs::path> get_potential_libhdfs_paths() {
  std::vector<bfs::path> libhdfs_potential_paths;
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
  std::vector<bfs::path> search_paths = {bfs::path(""), bfs::path(".")};

  // Path from environment variable
  const char* hadoop_home = std::getenv("HADOOP_HOME");
  if (hadoop_home != nullptr) {
    auto path = bfs::path(hadoop_home) / "lib/native";
    search_paths.push_back(path);
  }

  const char* libhdfs_dir = std::getenv("ARROW_LIBHDFS_DIR");
  if (libhdfs_dir != nullptr) {
    search_paths.push_back(bfs::path(libhdfs_dir));
  }

  // All paths with file name
  for (auto& path : search_paths) {
    libhdfs_potential_paths.push_back(path / file_name);
  }

  return libhdfs_potential_paths;
}

static std::vector<bfs::path> get_potential_libhdfs3_paths() {
  std::vector<bfs::path> potential_paths;
  std::string file_name;

// OS-specific file name
#ifdef _WIN32
  file_name = "hdfs3.dll";
#elif __APPLE__
  file_name = "libhdfs3.dylib";
#else
  file_name = "libhdfs3.so";
#endif

  // Common paths
  std::vector<bfs::path> search_paths = {bfs::path(""), bfs::path(".")};

  const char* libhdfs3_dir = std::getenv("ARROW_LIBHDFS3_DIR");
  if (libhdfs3_dir != nullptr) {
    search_paths.push_back(bfs::path(libhdfs3_dir));
  }

  // All paths with file name
  for (auto& path : search_paths) {
    potential_paths.push_back(path / file_name);
  }

  return potential_paths;
}

static std::vector<bfs::path> get_potential_libjvm_paths() {
  std::vector<bfs::path> libjvm_potential_paths;

  std::vector<bfs::path> search_prefixes;
  std::vector<bfs::path> search_suffixes;
  std::string file_name;

// From heuristics
#ifdef __WIN32
  search_prefixes = {""};
  search_suffixes = {"/jre/bin/server", "/bin/server"};
  file_name = "jvm.dll";
#elif __APPLE__
  search_prefixes = {""};
  search_suffixes = {"", "/jre/lib/server", "/lib/server"};
  file_name = "libjvm.dylib";

// SFrame uses /usr/libexec/java_home to find JAVA_HOME; for now we are
// expecting users to set an environment variable
#else
  search_prefixes = {
      "/usr/lib/jvm/default-java",                // ubuntu / debian distros
      "/usr/lib/jvm/java",                        // rhel6
      "/usr/lib/jvm",                             // centos6
      "/usr/lib64/jvm",                           // opensuse 13
      "/usr/local/lib/jvm/default-java",          // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java",                  // alt rhel6
      "/usr/local/lib/jvm",                       // alt centos6
      "/usr/local/lib64/jvm",                     // alt opensuse 13
      "/usr/local/lib/jvm/java-8-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-8-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java-7-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java-6-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-6-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-8-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-6-oracle",               // alt ubuntu
      "/usr/local/lib/jvm/java-7-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-8-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-6-oracle",         // alt ubuntu
      "/usr/lib/jvm/default",                     // alt centos
      "/usr/java/latest",                         // alt centos
  };
  search_suffixes = {"", "/jre/lib/amd64/server", "/lib/amd64/server"};
  file_name = "libjvm.so";
#endif
  // From direct environment variable
  char* env_value = NULL;
  if ((env_value = getenv("JAVA_HOME")) != NULL) {
    search_prefixes.insert(search_prefixes.begin(), env_value);
  }

  // Generate cross product between search_prefixes, search_suffixes, and file_name
  for (auto& prefix : search_prefixes) {
    for (auto& suffix : search_suffixes) {
      auto path = (bfs::path(prefix) / bfs::path(suffix) / bfs::path(file_name));
      libjvm_potential_paths.push_back(path);
    }
  }

  return libjvm_potential_paths;
}

#ifndef _WIN32
static arrow::Status try_dlopen(std::vector<bfs::path> potential_paths, const char* name,
                                void*& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    out_handle = dlopen(i.native().c_str(), RTLD_NOW | RTLD_LOCAL);

    if (out_handle != NULL) {
      // std::cout << "Loaded " << i << std::endl;
      break;
    } else {
      const char* err_msg = dlerror();
      if (err_msg != NULL) {
        error_messages.push_back(std::string(err_msg));
      } else {
        error_messages.push_back(std::string(" returned NULL"));
      }
    }
  }

  if (out_handle == NULL) {
    return arrow::Status::IOError("Unable to load ", name);
  }

  return arrow::Status::OK();
}

#else
static arrow::Status try_dlopen(std::vector<bfs::path> potential_paths, const char* name,
                                HINSTANCE& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    out_handle = LoadLibrary(i.string().c_str());

    if (out_handle != NULL) {
      break;
    } else {
      // error_messages.push_back(get_last_err_str(GetLastError()));
    }
  }

  if (out_handle == NULL) {
    return arrow::Status::IOError("Unable to load ", name);
  }

  return arrow::Status::OK();
}
#endif  // _WIN32

static inline void* GetLibrarySymbol(void* handle, const char* symbol) {
  if (handle == NULL) return NULL;
#ifndef _WIN32
  return dlsym(handle, symbol);
#else

  void* ret = reinterpret_cast<void*>(
      GetProcAddress(reinterpret_cast<HINSTANCE>(handle), symbol));
  if (ret == NULL) {
    // logstream(LOG_INFO) << "GetProcAddress error: "
    //                     << get_last_err_str(GetLastError()) << std::endl;
  }
  return ret;
#endif
}

#define GET_SYMBOL_REQUIRED(SHIM, SYMBOL_NAME)                         \
  do {                                                                 \
    if (!SHIM->SYMBOL_NAME) {                                          \
      *reinterpret_cast<void**>(&SHIM->SYMBOL_NAME) =                  \
          GetLibrarySymbol(SHIM->handle, "" #SYMBOL_NAME);             \
    }                                                                  \
    if (!SHIM->SYMBOL_NAME)                                            \
      return Status::IOError("Getting symbol " #SYMBOL_NAME "failed"); \
  } while (0)

#define GET_SYMBOL(SHIM, SYMBOL_NAME)                    \
  if (!SHIM->SYMBOL_NAME) {                              \
    *reinterpret_cast<void**>(&SHIM->SYMBOL_NAME) =      \
        GetLibrarySymbol(SHIM->handle, "" #SYMBOL_NAME); \
  }

static LibHdfsShim libhdfs_shim;
static LibHdfsShim libhdfs3_shim;

hdfsBuilder* LibHdfsShim::NewBuilder(void) { return this->hdfsNewBuilder(); }

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

int LibHdfsShim::Disconnect(hdfsFS bfs) { return this->hdfsDisconnect(bfs); }

hdfsFile LibHdfsShim::OpenFile(hdfsFS bfs, const char* path, int flags, int bufferSize,
                               short replication, tSize blocksize) {  // NOLINT
  return this->hdfsOpenFile(bfs, path, flags, bufferSize, replication, blocksize);
}

int LibHdfsShim::CloseFile(hdfsFS bfs, hdfsFile file) {
  return this->hdfsCloseFile(bfs, file);
}

int LibHdfsShim::Exists(hdfsFS bfs, const char* path) {
  return this->hdfsExists(bfs, path);
}

int LibHdfsShim::Seek(hdfsFS bfs, hdfsFile file, tOffset desiredPos) {
  return this->hdfsSeek(bfs, file, desiredPos);
}

tOffset LibHdfsShim::Tell(hdfsFS bfs, hdfsFile file) { return this->hdfsTell(bfs, file); }

tSize LibHdfsShim::Read(hdfsFS bfs, hdfsFile file, void* buffer, tSize length) {
  return this->hdfsRead(bfs, file, buffer, length);
}

bool LibHdfsShim::HasPread() {
  GET_SYMBOL(this, hdfsPread);
  return this->hdfsPread != nullptr;
}

tSize LibHdfsShim::Pread(hdfsFS bfs, hdfsFile file, tOffset position, void* buffer,
                         tSize length) {
  GET_SYMBOL(this, hdfsPread);
  DCHECK(this->hdfsPread);
  return this->hdfsPread(bfs, file, position, buffer, length);
}

tSize LibHdfsShim::Write(hdfsFS bfs, hdfsFile file, const void* buffer, tSize length) {
  return this->hdfsWrite(bfs, file, buffer, length);
}

int LibHdfsShim::Flush(hdfsFS bfs, hdfsFile file) { return this->hdfsFlush(bfs, file); }

int LibHdfsShim::Available(hdfsFS bfs, hdfsFile file) {
  GET_SYMBOL(this, hdfsAvailable);
  if (this->hdfsAvailable)
    return this->hdfsAvailable(bfs, file);
  else
    return 0;
}

int LibHdfsShim::Copy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsCopy);
  if (this->hdfsCopy)
    return this->hdfsCopy(srcFS, src, dstFS, dst);
  else
    return 0;
}

int LibHdfsShim::Move(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(this, hdfsMove);
  if (this->hdfsMove)
    return this->hdfsMove(srcFS, src, dstFS, dst);
  else
    return 0;
}

int LibHdfsShim::Delete(hdfsFS bfs, const char* path, int recursive) {
  return this->hdfsDelete(bfs, path, recursive);
}

int LibHdfsShim::Rename(hdfsFS bfs, const char* oldPath, const char* newPath) {
  GET_SYMBOL(this, hdfsRename);
  if (this->hdfsRename)
    return this->hdfsRename(bfs, oldPath, newPath);
  else
    return 0;
}

char* LibHdfsShim::GetWorkingDirectory(hdfsFS bfs, char* buffer, size_t bufferSize) {
  GET_SYMBOL(this, hdfsGetWorkingDirectory);
  if (this->hdfsGetWorkingDirectory) {
    return this->hdfsGetWorkingDirectory(bfs, buffer, bufferSize);
  } else {
    return NULL;
  }
}

int LibHdfsShim::SetWorkingDirectory(hdfsFS bfs, const char* path) {
  GET_SYMBOL(this, hdfsSetWorkingDirectory);
  if (this->hdfsSetWorkingDirectory) {
    return this->hdfsSetWorkingDirectory(bfs, path);
  } else {
    return 0;
  }
}

int LibHdfsShim::MakeDirectory(hdfsFS bfs, const char* path) {
  return this->hdfsCreateDirectory(bfs, path);
}

int LibHdfsShim::SetReplication(hdfsFS bfs, const char* path, int16_t replication) {
  GET_SYMBOL(this, hdfsSetReplication);
  if (this->hdfsSetReplication) {
    return this->hdfsSetReplication(bfs, path, replication);
  } else {
    return 0;
  }
}

hdfsFileInfo* LibHdfsShim::ListDirectory(hdfsFS bfs, const char* path, int* numEntries) {
  return this->hdfsListDirectory(bfs, path, numEntries);
}

hdfsFileInfo* LibHdfsShim::GetPathInfo(hdfsFS bfs, const char* path) {
  return this->hdfsGetPathInfo(bfs, path);
}

void LibHdfsShim::FreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries) {
  this->hdfsFreeFileInfo(hdfsFileInfo, numEntries);
}

char*** LibHdfsShim::GetHosts(hdfsFS bfs, const char* path, tOffset start,
                              tOffset length) {
  GET_SYMBOL(this, hdfsGetHosts);
  if (this->hdfsGetHosts) {
    return this->hdfsGetHosts(bfs, path, start, length);
  } else {
    return NULL;
  }
}

void LibHdfsShim::FreeHosts(char*** blockHosts) {
  GET_SYMBOL(this, hdfsFreeHosts);
  if (this->hdfsFreeHosts) {
    this->hdfsFreeHosts(blockHosts);
  }
}

tOffset LibHdfsShim::GetDefaultBlockSize(hdfsFS bfs) {
  GET_SYMBOL(this, hdfsGetDefaultBlockSize);
  if (this->hdfsGetDefaultBlockSize) {
    return this->hdfsGetDefaultBlockSize(bfs);
  } else {
    return 0;
  }
}

tOffset LibHdfsShim::GetCapacity(hdfsFS bfs) { return this->hdfsGetCapacity(bfs); }

tOffset LibHdfsShim::GetUsed(hdfsFS bfs) { return this->hdfsGetUsed(bfs); }

int LibHdfsShim::Chown(hdfsFS bfs, const char* path, const char* owner,
                       const char* group) {
  return this->hdfsChown(bfs, path, owner, group);
}

int LibHdfsShim::Chmod(hdfsFS bfs, const char* path, short mode) {  // NOLINT
  return this->hdfsChmod(bfs, path, mode);
}

int LibHdfsShim::Utime(hdfsFS bfs, const char* path, tTime mtime, tTime atime) {
  GET_SYMBOL(this, hdfsUtime);
  if (this->hdfsUtime) {
    return this->hdfsUtime(bfs, path, mtime, atime);
  } else {
    return 0;
  }
}

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

    std::vector<bfs::path> libjvm_potential_paths = get_potential_libjvm_paths();
    RETURN_NOT_OK(try_dlopen(libjvm_potential_paths, "libjvm", libjvm_handle));

    std::vector<bfs::path> libhdfs_potential_paths = get_potential_libhdfs_paths();
    RETURN_NOT_OK(try_dlopen(libhdfs_potential_paths, "libhdfs", shim->handle));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

Status ConnectLibHdfs3(LibHdfsShim** driver) {
  static std::mutex lock;
  std::lock_guard<std::mutex> guard(lock);

  LibHdfsShim* shim = &libhdfs3_shim;

  static bool shim_attempted = false;
  if (!shim_attempted) {
    shim_attempted = true;

    shim->Initialize();

    std::vector<bfs::path> libhdfs3_potential_paths = get_potential_libhdfs3_paths();
    RETURN_NOT_OK(try_dlopen(libhdfs3_potential_paths, "libhdfs3", shim->handle));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs3 failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

#else  // ARROW_WITH_BOOST_FILESYSTEM

Status ConnectLibHdfs(LibHdfsShim** driver) {
  return Status::NotImplemented("ConnectLibHdfs not available in this Arrow build");
}

Status ConnectLibHdfs3(LibHdfsShim** driver) {
  return Status::NotImplemented("ConnectLibHdfs3 not available in this Arrow build");
}

#endif

Status HdfsAnyFileImpl::DoSeek(int64_t position) {
  int ret = driver_->Seek(fs_, file_, position);
  CHECK_FAILURE(ret, "seek");
  return Status::OK();
}

Status HdfsAnyFileImpl::DoTell(int64_t* offset) const {
  int64_t ret = driver_->Tell(fs_, file_);
  CHECK_FAILURE(ret, "tell");
  *offset = ret;
  return Status::OK();
}

HdfsReadableFile::~HdfsReadableFile() { DCHECK_OK(Close()); }

Status HdfsReadableFile::Close() {
  if (is_open_) {
    int ret = driver_->CloseFile(fs_, file_);
    CHECK_FAILURE(ret, "CloseFile");
    is_open_ = false;
  }
  return Status::OK();
}

Status HdfsReadableFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                void* buffer) {
  tSize ret;
  if (driver_->HasPread()) {
    ret = driver_->Pread(fs_, file_, static_cast<tOffset>(position),
                         reinterpret_cast<void*>(buffer), static_cast<tSize>(nbytes));
  } else {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(Seek(position));
    return Read(nbytes, bytes_read, buffer);
  }
  CHECK_FAILURE(ret, "read");
  *bytes_read = ret;
  return Status::OK();
}

Status HdfsReadableFile::ReadAt(int64_t position, int64_t nbytes,
                                std::shared_ptr<Buffer>* out) {
  std::shared_ptr<ResizableBuffer> buffer;
  RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

  int64_t bytes_read = 0;
  RETURN_NOT_OK(ReadAt(position, nbytes, &bytes_read, buffer->mutable_data()));

  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
    buffer->ZeroPadding();
  }

  *out = buffer;
  return Status::OK();
}

Status HdfsReadableFile::Read(int64_t nbytes, int64_t* bytes_read, void* buffer) {
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

  *bytes_read = total_bytes;
  return Status::OK();
}

Status HdfsReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  std::shared_ptr<ResizableBuffer> buffer;
  RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

  int64_t bytes_read = 0;
  RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
  }

  *out = std::move(buffer);
  return Status::OK();
}

inline Status GetPathInfoFailed(const std::string& path) {
  std::stringstream ss;
  ss << "Calling GetPathInfo for " << path << " failed. errno: " << TranslateErrno(errno);
  return Status::IOError(ss.str());
}

Status HdfsReadableFile::GetSize(int64_t* size) {
  hdfsFileInfo* entry = driver_->GetPathInfo(fs_, path_.c_str());
  if (entry == nullptr) {
    return GetPathInfoFailed(path_);
  }

  *size = entry->mSize;
  driver_->FreeFileInfo(entry, 1);
  return Status::OK();
}

// ----------------------------------------------------------------------
// File writing

HdfsOutputStream::~HdfsOutputStream() { DCHECK_OK(Close()); }

Status HdfsOutputStream::Close() {
  if (is_open_) {
    RETURN_NOT_OK(Flush());
    int ret = driver_->CloseFile(fs_, file_);
    CHECK_FAILURE(ret, "CloseFile");
    is_open_ = false;
  }
  return Status::OK();
}

Status HdfsOutputStream::Write(const void* buffer, int64_t nbytes,
                               int64_t* bytes_written) {
  std::lock_guard<std::mutex> guard(lock_);
  tSize ret = driver_->Write(fs_, file_, reinterpret_cast<const void*>(buffer),
                             static_cast<tSize>(nbytes));
  CHECK_FAILURE(ret, "Write");
  *bytes_written = ret;
  return Status::OK();
}

Status HdfsOutputStream::Write(const void* buffer, int64_t nbytes) {
  int64_t bytes_written_dummy = 0;
  return Write(buffer, nbytes, &bytes_written_dummy);
}

Status HdfsOutputStream::Flush() {
  int ret = driver_->Flush(fs_, file_);
  CHECK_FAILURE(ret, "Flush");
  return Status::OK();
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

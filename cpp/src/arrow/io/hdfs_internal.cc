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

#include "arrow/io/hdfs_internal.h"

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <dlfcn.h>
#endif

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::GetEnvVarNative;
using internal::PlatformFilename;
#ifdef _WIN32
using internal::WinErrorMessage;
#endif

namespace io {
namespace internal {

namespace {

void* GetLibrarySymbol(LibraryHandle handle, const char* symbol) {
  if (handle == NULL) return NULL;
#ifndef _WIN32
  return dlsym(handle, symbol);
#else

  void* ret = reinterpret_cast<void*>(GetProcAddress(handle, symbol));
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

LibraryHandle libjvm_handle = nullptr;

// Helper functions for dlopens
Result<std::vector<PlatformFilename>> get_potential_libjvm_paths();
Result<std::vector<PlatformFilename>> get_potential_libhdfs_paths();
Result<std::vector<PlatformFilename>> get_potential_libhdfs3_paths();
Result<LibraryHandle> try_dlopen(const std::vector<PlatformFilename>& potential_paths,
                                 const char* name);

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

Result<std::vector<PlatformFilename>> get_potential_libhdfs3_paths() {
  std::vector<PlatformFilename> potential_paths;
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
  ARROW_ASSIGN_OR_RAISE(auto search_paths, MakeFilenameVector({"", "."}));

  // Path from environment variable
  AppendEnvVarFilename("ARROW_LIBHDFS3_DIR", &search_paths);

  // All paths with file name
  for (auto& path : search_paths) {
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
#ifdef __WIN32
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
  ARROW_ASSIGN_OR_RAISE(
      search_prefixes,
      MakeFilenameVector({
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
      }));
  ARROW_ASSIGN_OR_RAISE(search_suffixes,
                        MakeFilenameVector({"", "/jre/lib/amd64/server",
                                            "/lib/amd64/server", "/lib/server"}));
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

#ifndef _WIN32
Result<LibraryHandle> try_dlopen(const std::vector<PlatformFilename>& potential_paths,
                                 const char* name) {
  std::string error_message = "unknown error";
  LibraryHandle handle;

  for (const auto& p : potential_paths) {
    handle = dlopen(p.ToNative().c_str(), RTLD_NOW | RTLD_LOCAL);

    if (handle != NULL) {
      return handle;
    } else {
      const char* err_msg = dlerror();
      if (err_msg != NULL) {
        error_message = err_msg;
      }
    }
  }

  return Status::IOError("Unable to load ", name, ": ", error_message);
}

#else
Result<LibraryHandle> try_dlopen(const std::vector<PlatformFilename>& potential_paths,
                                 const char* name) {
  std::string error_message;
  LibraryHandle handle;

  for (const auto& p : potential_paths) {
    handle = LoadLibraryW(p.ToNative().c_str());
    if (handle != NULL) {
      return handle;
    } else {
      error_message = WinErrorMessage(GetLastError());
    }
  }

  return Status::IOError("Unable to load ", name, ": ", error_message);
}
#endif  // _WIN32

LibHdfsShim libhdfs_shim;
LibHdfsShim libhdfs3_shim;

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
    ARROW_ASSIGN_OR_RAISE(libjvm_handle, try_dlopen(libjvm_potential_paths, "libjvm"));

    ARROW_ASSIGN_OR_RAISE(auto libhdfs_potential_paths, get_potential_libhdfs_paths());
    ARROW_ASSIGN_OR_RAISE(shim->handle, try_dlopen(libhdfs_potential_paths, "libhdfs"));
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

    ARROW_ASSIGN_OR_RAISE(auto libhdfs3_potential_paths, get_potential_libhdfs3_paths());
    ARROW_ASSIGN_OR_RAISE(shim->handle, try_dlopen(libhdfs3_potential_paths, "libhdfs3"));
  } else if (shim->handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs3 failed");
  }

  *driver = shim;
  return shim->GetRequiredSymbols();
}

///////////////////////////////////////////////////////////////////////////
// HDFS thin wrapper methods

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
  if (this->hdfsAvailable)
    return this->hdfsAvailable(fs, file);
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

int LibHdfsShim::Delete(hdfsFS fs, const char* path, int recursive) {
  return this->hdfsDelete(fs, path, recursive);
}

int LibHdfsShim::Rename(hdfsFS fs, const char* oldPath, const char* newPath) {
  GET_SYMBOL(this, hdfsRename);
  if (this->hdfsRename)
    return this->hdfsRename(fs, oldPath, newPath);
  else
    return 0;
}

char* LibHdfsShim::GetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize) {
  GET_SYMBOL(this, hdfsGetWorkingDirectory);
  if (this->hdfsGetWorkingDirectory) {
    return this->hdfsGetWorkingDirectory(fs, buffer, bufferSize);
  } else {
    return NULL;
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
    return NULL;
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

}  // namespace internal
}  // namespace io
}  // namespace arrow
